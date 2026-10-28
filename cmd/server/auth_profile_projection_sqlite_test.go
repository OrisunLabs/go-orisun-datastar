package main

import (
	"context"
	"database/sql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/profile"
	"github.com/OrisunLabs/go-orisun-datastar/internal/sqlitetest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"testing"
	"time"
)

func nativeOwnedAuthProjection(t *testing.T) (*auth.SQLiteProjectionRepository, *sql.DB) {
	t.Helper()
	pool := sqlitetest.New(t)
	if _, err := pool.ExecContext(t.Context(), "DROP TABLE profile_stats"); err != nil {
		t.Fatal(err)
	}
	return auth.NewSQLiteProjectionRepository(pool), pool
}

func ownedAuthSnapshot(t *testing.T, pool *sql.DB) string {
	return sqlitetest.Snapshot(t, pool, "auth_user")
}

func TestSQLiteAuthOwnsProfileFactsWithoutProfileTable(t *testing.T) {
	events, keys := authProfileHistory(t)
	baseline := ""
	for _, parts := range [][]int{{5}, {1, 1, 1, 1, 1}, {2, 3}} {
		repo, pool := nativeOwnedAuthProjection(t)
		offset := 0
		publications := 0
		publisher := authUserInvalidator{publisher: authProfilePublisherFunc(func(ctx context.Context, _ string, _ any) error {
			var count int
			if err := pool.QueryRowContext(ctx, "SELECT count(*) FROM auth_user").Scan(&count); err != nil {
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
		if _, err := pool.ExecContext(t.Context(), `INSERT INTO auth_session(id,token,user_id,expires_at) VALUES('session','session-token',$1,datetime('now','+1 hour'))`, user); err != nil {
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
func TestSQLiteAuthProfileFailureRollsBackOwningState(t *testing.T) {
	events, keys := authProfileHistory(t)
	repo, pool := nativeOwnedAuthProjection(t)
	if _, err := pool.ExecContext(t.Context(), "CREATE TRIGGER reject_header BEFORE UPDATE ON auth_user WHEN NEW.header_image_url<>'' BEGIN SELECT RAISE(ABORT,'reject header'); END"); err != nil {
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

func TestSQLiteAuthProfileClearingPreservesOtherFacts(t *testing.T) {
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

func TestSQLiteAuthSkipsErasedProfileSourcesWithoutKeys(t *testing.T) {
	repo, pool := nativeOwnedAuthProjection(t)
	history, _ := authProfileHistory(t)
	user := history[0].Event.EventID
	// The composition fixture intentionally has no profile table: erasure is auth-owned evidence.
	if _, err := pool.ExecContext(t.Context(), "INSERT INTO auth_erased_users(user_id) VALUES ($1)", user); err != nil {
		t.Fatal(err)
	}
	if err := auth.ProjectBatch(t.Context(), history, repo, nil, authProfileSource(), nil); err != nil {
		t.Fatalf("composed profile facts needed erased keys: %v", err)
	}
	for _, e := range history {
		if err := repo.WithinTransaction(t.Context(), func(ctx context.Context) error {
			owner, err := repo.Processed(ctx, e.Position)
			if err != nil {
				return err
			}
			if owner != user {
				t.Fatal("composed occurrence lost")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := pool.QueryRowContext(t.Context(), "SELECT count(*) FROM auth_user").Scan(&count); err != nil || count != 0 {
		t.Fatal("composed profile source resurrected user")
	}
}
