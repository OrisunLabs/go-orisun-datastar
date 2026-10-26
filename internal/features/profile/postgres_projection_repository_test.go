package profile

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func nativeProfileProjection(t *testing.T) (*PostgreSQLProjectionRepository, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL projection checks")
	}
	admin, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	schema := "profile_projection_" + strings.ReplaceAll(uuidv7.NewString(), "-", "")
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
	migration, err := os.ReadFile("../../../migrations/001_init.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), string(migration)); err != nil {
		t.Fatal(err)
	}
	upgrade, err := os.ReadFile("../../../migrations/003_profile_projection.sql")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := pool.Exec(t.Context(), string(upgrade)); err != nil {
			t.Fatal(err)
		}
	}

	return NewPostgreSQLProjectionRepository(pool), pool
}

func profileSnapshot(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var snapshot string
	if err := pool.QueryRow(t.Context(), `SELECT coalesce(jsonb_agg(to_jsonb(p)-'created_at'-'updated_at' ORDER BY user_id),'[]'::jsonb)::text FROM profile_stats p`).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}
func TestPostgresProfilePartitionsAndReplay(t *testing.T) {
	events, keys := profileProjectionHistory(t)
	baseline := ""
	for _, parts := range [][]int{{8}, {1, 1, 1, 1, 1, 1, 1, 1}, {2, 3, 3}} {
		repo, pool := nativeProfileProjection(t)
		offset := 0
		for _, size := range parts {
			if _, err := ProjectBatch(t.Context(), events[offset:offset+size], repo, keys); err != nil {
				t.Fatal(err)
			}
			offset += size
		}
		snapshot := profileSnapshot(t, pool)
		if baseline == "" {
			baseline = snapshot
		} else if baseline != snapshot {
			t.Fatal("partition-dependent rows/evidence")
		}
		replay := append([]eventstore.ResolvedEvent(nil), events...)
		for i := range replay {
			replay[i].Event.Data = nil
		}
		ids, err := ProjectBatch(t.Context(), replay, repo, nil)
		if err != nil || len(ids) != 2 {
			t.Fatalf("replay routing: %v %v", ids, err)
		}
		if snapshot != profileSnapshot(t, pool) {
			t.Fatal("replay changed state")
		}
		upgrade, err := os.ReadFile("../../../migrations/003_profile_projection.sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(t.Context(), string(upgrade)); err != nil {
			t.Fatal(err)
		}
		if snapshot != profileSnapshot(t, pool) {
			t.Fatal("migration changed rows")
		}
		// An older previously unseen occurrence records evidence without reverting fields.
		historical := events[2]
		historical.Position = eventstore.Position{Commit: 41, Prepare: 99}
		if _, err := ProjectBatch(t.Context(), []eventstore.ResolvedEvent{historical}, repo, keys); err != nil {
			t.Fatal(err)
		}
		var bio string
		if err := pool.QueryRow(t.Context(), "SELECT bio FROM profile_stats WHERE user_id=$1", events[0].Event.EventID).Scan(&bio); err != nil || bio != "" {
			t.Fatalf("historical bio reverted cleared value: %q %v", bio, err)
		}
	}
}
func TestPostgresProfileSQLAndDecryptFailuresRollBack(t *testing.T) {
	events, keys := profileProjectionHistory(t)
	repo, pool := nativeProfileProjection(t)
	if _, err := pool.Exec(t.Context(), "ALTER TABLE profile_stats ADD CONSTRAINT reject_bio CHECK(bio IS NULL)"); err != nil {
		t.Fatal(err)
	}
	if ids, err := ProjectBatch(t.Context(), events, repo, keys); err == nil || len(ids) != 0 {
		t.Fatal("SQL failure lost")
	}
	if snapshot := profileSnapshot(t, pool); snapshot != "[]" {
		t.Fatal("partial registration/evidence survived")
	}
	if _, err := pool.Exec(t.Context(), "ALTER TABLE profile_stats DROP CONSTRAINT reject_bio"); err != nil {
		t.Fatal(err)
	}
	wrong, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	bad, badErr := mustProfileBio(events[0].Event.EventID, wrong)
	events[2].Event = projectionEvent(t, bad, badErr)
	if _, err := ProjectBatch(t.Context(), events, repo, keys); err == nil {
		t.Fatal("wrong-key encryption accepted")
	}
	if snapshot := profileSnapshot(t, pool); snapshot != "[]" {
		t.Fatal("decrypt failure retained prefix")
	}
}
func mustProfileBio(user string, key protectedpii.SubjectDataKey) (eventstore.DomainEvent, error) {
	return NewProfileBioUpdatedEvent(uuidv7.NewString(), "Bad key", testProfileTime(), user, key, nil)
}
func TestPostgresProfileBindingAndOwnWrites(t *testing.T) {
	repo, pool := nativeProfileProjection(t)
	if _, err := repo.LoadProfile(t.Context(), "missing"); err == nil {
		t.Fatal("unbound access")
	}
	other := NewPostgreSQLProjectionRepository(pool)
	events, keys := profileProjectionHistory(t)
	var escaped context.Context
	marker := errors.New("rollback")
	for _, rollback := range []bool{true, false} {
		err := repo.WithinTransaction(t.Context(), func(ctx context.Context) error {
			escaped = ctx
			if _, err := other.LoadProfile(ctx, "missing"); err == nil {
				t.Fatal("foreign access")
			}
			if err := repo.WithinTransaction(ctx, func(context.Context) error { return nil }); err == nil {
				t.Fatal("nested access")
			}
			change, err := decodeProfileChange(ctx, events[0], keys)
			if err != nil {
				return err
			}
			next, err := applyProfile(nil, change)
			if err != nil {
				return err
			}
			if err := repo.SaveProfile(ctx, next, change.identity()); err != nil {
				return err
			}
			own, err := repo.LoadProfile(ctx, next.UserID)
			if err != nil || own == nil {
				return errors.New("own write not observable")
			}
			owner, err := repo.FindProcessedProfile(ctx, events[0].Position)
			if err != nil || owner != next.UserID {
				return errors.New("own evidence not observable")
			}
			var count int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM profile_stats").Scan(&count); err != nil {
				return err
			}
			if count != 0 {
				t.Fatal("uncommitted state visible outside transaction")
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
		if _, err := repo.LoadProfile(escaped, "missing"); err == nil {
			t.Fatal("escaped access")
		}
	}
}
