package main

import (
	"context"
	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/profile"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"strings"
	"testing"
	"time"
)

func nativeOwnedAuthProjection(t *testing.T) (*auth.PostgreSQLProjectionRepository, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL projection checks")
	}
	admin, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	schema := "auth_projection_" + strings.ReplaceAll(uuidv7.NewString(), "-", "")
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(t.Context(), "CREATE SCHEMA "+identifier); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 3
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, err := admin.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE")
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	migration, err := os.ReadFile("../../migrations/001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), string(migration)); err != nil {
		t.Fatal(err)
	}
	upgrade, err := os.ReadFile("../../migrations/002_auth_projection.sql")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := pool.Exec(t.Context(), string(upgrade)); err != nil {
			t.Fatal(err)
		}
	}

	owned, err := os.ReadFile("../../migrations/004_auth_owned_profile.sql")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := pool.Exec(t.Context(), string(owned)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(t.Context(), "DROP TABLE profile_stats"); err != nil {
		t.Fatal(err)
	}
	return auth.NewPostgreSQLProjectionRepository(pool), pool
}

func ownedAuthSnapshot(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var result string
	if err := pool.QueryRow(t.Context(), `SELECT coalesce(jsonb_agg(to_jsonb(u)-'created_at'-'updated_at' ORDER BY id),'[]'::jsonb)::text FROM auth_user u`).Scan(&result); err != nil {
		t.Fatal(err)
	}
	return result
}
func TestPostgresAuthOwnsProfileFactsWithoutProfileTable(t *testing.T) {
	events, keys := authProfileHistory(t)
	baseline := ""
	for _, parts := range [][]int{{5}, {1, 1, 1, 1, 1}, {2, 3}} {
		repo, pool := nativeOwnedAuthProjection(t)
		offset := 0
		publications := 0
		publisher := authUserInvalidator{publisher: authProfilePublisherFunc(func(ctx context.Context, _ string, _ any) error {
			var count int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM auth_user").Scan(&count); err != nil {
				return err
			}
			if count != 1 {
				t.Fatal("published before commit")
			}
			publications++
			return nil
		})}
		for _, size := range parts {
			if err := auth.ProjectBatch(t.Context(), events[offset:offset+size], repo, keys, authProfileSource(), publisher); err != nil {
				t.Fatal(err)
			}
			offset += size
		}
		snapshot := ownedAuthSnapshot(t, pool)
		if baseline == "" {
			baseline = snapshot
		} else if snapshot != baseline {
			t.Fatal("profile state depends on delivery partition")
		}
		user := events[0].Event.EventID
		if _, err := pool.Exec(t.Context(), `INSERT INTO auth_session(id,token,user_id,expires_at) VALUES('session','session-token',$1,now()+interval '1 hour')`, user); err != nil {
			t.Fatal(err)
		}
		store := auth.NewAuthUserStore(pool)
		byID, err := store.UserByRegisteredID(t.Context(), user)
		if err != nil {
			t.Fatal(err)
		}
		byEither, err := store.UserByIDOrRegisteredID(t.Context(), user)
		if err != nil {
			t.Fatal(err)
		}
		bySession, err := store.UserBySessionToken(t.Context(), "session-token")
		if err != nil {
			t.Fatal(err)
		}
		byEmail, password, err := store.UserByEmailWithPassword(t.Context(), "owned@example.com")
		if err != nil || password == "" {
			t.Fatalf("login read: %v", err)
		}
		for _, read := range []struct{ image, bio, header, name string }{
			{byID.Image, byID.Bio, byID.HeaderImageURL, byID.Name},
			{byEither.Image, byEither.Bio, byEither.HeaderImageURL, byEither.Name},
			{bySession.Image, bySession.Bio, bySession.HeaderImageURL, bySession.Name},
			{byEmail.Image, byEmail.Bio, byEmail.HeaderImageURL, byEmail.Name},
		} {
			if read.image != "/uploads/avatar.png" || read.bio != "Owned bio" || read.header != "/uploads/header.png" || read.name != "Updated name" {
				t.Fatalf("incomplete owning read: %+v", read)
			}
		}
		// Already processed payloads need neither decoders nor subject keys on retry.
		replay := append([]eventstore.ResolvedEvent(nil), events...)
		for i := range replay {
			replay[i].Event.Data = nil
		}
		before := publications
		if err := auth.ProjectBatch(t.Context(), replay, repo, nil, authProfileSource(), publisher); err != nil {
			t.Fatal(err)
		}
		if publications != before+1 || ownedAuthSnapshot(t, pool) != snapshot {
			t.Fatal("replay lost routing or changed rows")
		}
	}
}
func TestPostgresAuthProfileFailureRollsBackOwningState(t *testing.T) {
	events, keys := authProfileHistory(t)
	repo, pool := nativeOwnedAuthProjection(t)
	if _, err := pool.Exec(t.Context(), "ALTER TABLE auth_user ADD CONSTRAINT reject_header CHECK(header_image_url='')"); err != nil {
		t.Fatal(err)
	}
	calls := 0
	publisher := authUserInvalidator{publisher: authProfilePublisherFunc(func(context.Context, string, any) error { calls++; return nil })}
	if err := auth.ProjectBatch(t.Context(), events, repo, keys, authProfileSource(), publisher); err == nil {
		t.Fatal("header SQL failure lost")
	}
	if calls != 0 || ownedAuthSnapshot(t, pool) != "[]" {
		t.Fatal("failed projection committed or published prefix")
	}
}

func TestPostgresAuthProfileClearingPreservesOtherFacts(t *testing.T) {
	events, keys := authProfileHistory(t)
	repo, pool := nativeOwnedAuthProjection(t)
	if err := auth.ProjectBatch(t.Context(), events, repo, keys, authProfileSource(), nil); err != nil {
		t.Fatal(err)
	}
	user := events[0].Event.EventID
	cleared, err := profile.NewProfileBioUpdatedEvent(uuidv7.NewString(), "", time.Now().UTC(), user, keys.key, nil)
	if err != nil {
		t.Fatal(err)
	}
	event := eventstore.ResolvedEvent{Event: cleared, Position: eventstore.Position{Commit: 21, Prepare: 0}}
	if err := auth.ProjectBatch(t.Context(), []eventstore.ResolvedEvent{event}, repo, keys, authProfileSource(), nil); err != nil {
		t.Fatal(err)
	}
	current, err := auth.NewAuthUserStore(pool).UserByRegisteredID(t.Context(), user)
	if err != nil {
		t.Fatal(err)
	}
	if current.Bio != "" || current.HeaderImageURL != "/uploads/header.png" || current.Image != "/uploads/avatar.png" || current.Name != "Updated name" {
		t.Fatal("clearing bio erased other owning facts")
	}
}
