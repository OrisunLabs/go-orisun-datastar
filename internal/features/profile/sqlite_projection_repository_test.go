package profile

import (
	"context"
	"errors"
	"github.com/OrisunLabs/go-orisun-datastar/internal/appdb"
	"testing"

	"database/sql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/sqlitetest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func nativeProfileProjection(t *testing.T) (*SQLiteProjectionRepository, *sql.DB) {
	t.Helper()
	pool := sqlitetest.New(t)
	return NewSQLiteProjectionRepository(pool), pool
}

func profileSnapshot(t *testing.T, pool *sql.DB) string {
	return sqlitetest.Snapshot(t, pool, "profile_stats")
}

func TestSQLiteProfilePartitionsAndReplay(t *testing.T) {
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
		if err := appdb.Migrate(t.Context(), pool); err != nil {
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
		if err := pool.QueryRowContext(t.Context(), "SELECT bio FROM profile_stats WHERE user_id=$1", events[0].Event.EventID).Scan(&bio); err != nil || bio != "" {
			t.Fatalf("historical bio reverted cleared value: %q %v", bio, err)
		}
	}
}
func TestSQLiteProfileSQLAndDecryptFailuresRollBack(t *testing.T) {
	events, keys := profileProjectionHistory(t)
	repo, pool := nativeProfileProjection(t)
	if _, err := pool.ExecContext(t.Context(), "CREATE TRIGGER reject_bio BEFORE INSERT ON profile_stats WHEN NEW.bio IS NOT NULL BEGIN SELECT RAISE(ABORT,'reject bio'); END"); err != nil {
		t.Fatal(err)
	}
	if ids, err := ProjectBatch(t.Context(), events, repo, keys); err == nil || len(ids) != 0 {
		t.Fatal("SQL failure lost")
	}
	if snapshot := profileSnapshot(t, pool); snapshot != "[]" {
		t.Fatal("partial registration/evidence survived")
	}
	if _, err := pool.ExecContext(t.Context(), "DROP TRIGGER reject_bio"); err != nil {
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
func TestSQLiteProfileBindingAndOwnWrites(t *testing.T) {
	repo, pool := nativeProfileProjection(t)
	if _, err := repo.LoadProfile(t.Context(), "missing"); err == nil {
		t.Fatal("unbound access")
	}
	other := NewSQLiteProjectionRepository(pool)
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
			if err := pool.QueryRowContext(ctx, "SELECT count(*) FROM profile_stats").Scan(&count); err != nil {
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
