package auth

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func nativeAuthProjection(t *testing.T) (*PostgreSQLProjectionRepository, *pgxpool.Pool) {
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
	return NewPostgreSQLProjectionRepository(pool), pool
}

func authSnapshot(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var snapshot string
	err := pool.QueryRow(t.Context(), `SELECT jsonb_build_object(
 'users',(SELECT coalesce(jsonb_agg(to_jsonb(u)-'created_at'-'updated_at' ORDER BY id),'[]') FROM auth_user u),
 'accounts',(SELECT coalesce(jsonb_agg(to_jsonb(a)-'created_at'-'updated_at' ORDER BY id),'[]') FROM auth_account a),
 'verifications',(SELECT coalesce(jsonb_agg(to_jsonb(v)-'created_at'-'updated_at' ORDER BY id),'[]') FROM auth_verification v))::text`).Scan(&snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
func TestPostgresAuthBatchPartitionsAndReplay(t *testing.T) {
	events, keys := authProjectionHistory(t)
	baseline := ""
	for _, parts := range [][]int{{7}, {1, 1, 1, 1, 1, 1, 1}, {2, 3, 2}} {
		repo, pool := nativeAuthProjection(t)
		offset := 0
		for _, size := range parts {
			if err := ProjectBatch(t.Context(), events[offset:offset+size], repo, keys, ProfileSource{}, nil); err != nil {
				t.Fatal(err)
			}
			offset += size
		}
		snapshot := authSnapshot(t, pool)
		upgrade, err := os.ReadFile("../../migrations/002_auth_projection.sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(t.Context(), string(upgrade)); err != nil {
			t.Fatal(err)
		}
		if authSnapshot(t, pool) != snapshot {
			t.Fatal("migration reapplication changed projected data")
		}

		if baseline == "" {
			baseline = snapshot
		} else if baseline != snapshot {
			t.Fatal("state depends on partition")
		}
		// Known source positions short circuit strict decoding and key lookup.
		replay := append([]eventstore.ResolvedEvent(nil), events...)
		for i := range replay {
			replay[i].Event.Data = nil
		}
		if err := ProjectBatch(t.Context(), replay, repo, nil, ProfileSource{}, nil); err != nil {
			t.Fatal(err)
		}
		if authSnapshot(t, pool) != snapshot {
			t.Fatal("replay changed state")
		}
	}
}
func TestPostgresAuthRollsBackCredentialAndVerificationFailures(t *testing.T) {
	events, keys := authProjectionHistory(t)
	for _, table := range []string{"auth_account", "auth_verification"} {
		t.Run(table, func(t *testing.T) {
			repo, pool := nativeAuthProjection(t)
			if _, err := pool.Exec(t.Context(), "ALTER TABLE "+table+" ADD CONSTRAINT force_failure CHECK(false)"); err != nil {
				t.Fatal(err)
			}
			if err := ProjectBatch(t.Context(), events, repo, keys, ProfileSource{}, nil); err == nil {
				t.Fatal("expected SQL failure")
			}
			var count int
			if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM auth_user").Scan(&count); err != nil || count != 0 {
				t.Fatalf("partial user/evidence: %d %v", count, err)
			}
		})
	}
}
func TestPostgresAuthTransactionBinding(t *testing.T) {
	repo, pool := nativeAuthProjection(t)
	if _, err := repo.Load(t.Context(), "missing"); err == nil {
		t.Fatal("unbound access")
	}
	other := NewPostgreSQLProjectionRepository(pool)
	for _, rollback := range []bool{false, true} {
		var escaped context.Context
		marker := errors.New("rollback")
		err := repo.WithinTransaction(t.Context(), func(ctx context.Context) error {
			escaped = ctx
			if _, err := other.Load(ctx, "missing"); err == nil {
				t.Fatal("foreign access")
			}
			if err := repo.WithinTransaction(ctx, func(context.Context) error { return nil }); err == nil {
				t.Fatal("nested access")
			}
			if rollback {
				return marker
			}
			return nil
		})
		if rollback && !errors.Is(err, marker) {
			t.Fatal(err)
		}
		if !rollback && err != nil {
			t.Fatal(err)
		}
		if _, err := repo.Load(escaped, "missing"); err == nil {
			t.Fatal("escaped access")
		}
	}
}
func TestPostgresAuthMissingRegistrationAndTokenConflict(t *testing.T) {
	events, keys := authProjectionHistory(t)
	repo, pool := nativeAuthProjection(t)
	if err := ProjectBatch(t.Context(), events[1:2], repo, keys, ProfileSource{}, nil); !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatal(err)
	}
	if err := ProjectBatch(t.Context(), events, repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	before := authSnapshot(t, pool)
	conflict := events[2]
	conflict.Position = eventstore.Position{Commit: 11, Prepare: 0}
	conflict.Event.Data = eventstore.MustData(conflict.Event.Data)
	conflict.Event.Data["otpCode"] = "654321"
	if err := ProjectBatch(t.Context(), []eventstore.ResolvedEvent{conflict}, repo, keys, ProfileSource{}, nil); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("token collision: %v", err)
	}
	if authSnapshot(t, pool) != before {
		t.Fatal("conflict changed projection")
	}
}

func TestPostgresAuthOwnWritesAndIsolation(t *testing.T) {
	events, keys := authProjectionHistory(t)
	repo, pool := nativeAuthProjection(t)
	err := repo.WithinTransaction(t.Context(), func(ctx context.Context) error {
		change, err := decodeAuthChange(ctx, events[0], keys, ProfileSource{})
		if err != nil {
			return err
		}
		next, err := applyAuthChange(nil, change, events[0].Position)
		if err != nil {
			return err
		}
		if err := repo.Save(ctx, ProjectionUpdate{State: next, Occurrence: Occurrence{UserID: next.UserID, EventType: UserRegistered, Position: events[0].Position}}); err != nil {
			return err
		}
		loaded, err := repo.Load(ctx, next.UserID)
		if err != nil {
			return err
		}
		if loaded == nil || loaded.Name != next.Name {
			t.Fatal("own writes were not observable")
		}
		owner, err := repo.Processed(ctx, events[0].Position)
		if err != nil {
			return err
		}
		if owner != next.UserID {
			t.Fatal("own evidence was not observable")
		}
		var count int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM auth_user").Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			t.Fatal("pool read saw uncommitted state")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// A pre-migration verified flag has no complete source fact; retain it.
	if _, err := pool.Exec(t.Context(), "UPDATE auth_user SET email_verified=true"); err != nil {
		t.Fatal(err)
	}
	if err := ProjectBatch(t.Context(), events[1:2], repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	var verified bool
	if err := pool.QueryRow(t.Context(), "SELECT email_verified FROM auth_user").Scan(&verified); err != nil || !verified {
		t.Fatalf("legacy verified flag lost: %v", err)
	}
}

func TestPostgresAuthDecryptionFailureRollsBackPrefix(t *testing.T) {
	events, keys := authProjectionHistory(t)
	repo, pool := nativeAuthProjection(t)
	id := events[0].Event.EventID
	wrong, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	events[1].Event = mustAuthEvent(NewUserNameChangedEvent(uuidv7.NewString(), "Wrong key", time.Now().UTC(), id, wrong, nil))
	if err := ProjectBatch(t.Context(), events[:2], repo, keys, ProfileSource{}, nil); err == nil {
		t.Fatal("wrong-key ciphertext accepted")
	}
	var count int
	if err := pool.QueryRow(t.Context(), "SELECT count(*) FROM auth_user").Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial registration: %d %v", count, err)
	}
}

func TestPostgresAuthUnknownPositionsAndConstraints(t *testing.T) {
	repo, pool := nativeAuthProjection(t)
	events, keys := authProjectionHistory(t)
	if err := ProjectBatch(t.Context(), events, repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), "UPDATE auth_user SET last_event_commit_position=NULL,last_event_prepare_position=NULL"); err != nil {
		t.Fatal(err)
	}
	if err := repo.WithinTransaction(t.Context(), func(ctx context.Context) error {
		state, err := repo.Load(ctx, events[0].Event.EventID)
		if err != nil {
			return err
		}
		if state.Position != nil {
			t.Fatal("unknown user position fabricated")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, columns := range []struct{ table, commit, prepare string }{
		{"auth_user", "last_event_commit_position", "last_event_prepare_position"},
	} {
		for _, values := range []string{"0,NULL", "NULL,0", "-1,-1", "-1,0", "0,-1"} {
			sql := "UPDATE " + columns.table + " SET (" + columns.commit + "," + columns.prepare + ")=(" + values + ")"
			if _, err := pool.Exec(t.Context(), sql); err == nil {
				t.Fatalf("invalid pair accepted: %s %s", columns.table, values)
			}
		}
		if _, err := pool.Exec(t.Context(), "UPDATE "+columns.table+" SET ("+columns.commit+","+columns.prepare+")=(0,0)"); err != nil {
			t.Fatal(err)
		}
	}
}
