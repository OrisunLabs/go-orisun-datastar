package todo

import (
	"context"
	"errors"
	"strings"
	"testing"

	"database/sql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/sqlitetest"
)

func nativeTodoProjection(t *testing.T) (*SQLiteProjectionRepository, *sql.DB) {
	t.Helper()
	pool := sqlitetest.New(t)
	return NewSQLiteProjectionRepository(pool), pool
}

func nativeTodoSnapshot(t *testing.T, pool *sql.DB) string {
	return sqlitetest.Snapshot(t, pool, "todo_items")
}

func TestSQLiteTodoProjectionBatchPartitionEquivalence(t *testing.T) {
	events := todoProjectionHistory(t)
	baseline := ""
	for _, partition := range [][]int{{6}, {1, 1, 1, 1, 1, 1}, {2, 1, 3}} {
		repo, pool := nativeTodoProjection(t)
		offset := 0
		for _, size := range partition {
			if _, err := ProjectBatch(t.Context(), events[offset:offset+size], repo); err != nil {
				t.Fatal(err)
			}
			offset += size
		}
		snapshot := nativeTodoSnapshot(t, pool)
		if baseline == "" {
			baseline = snapshot
		} else if baseline != snapshot {
			t.Fatal("native rows/provenance depend on delivery partition")
		}
		if !strings.Contains(snapshot, `"completed":true`) {
			t.Fatal("native status was not derived from latest lifecycle positions")
		}
		if err := repo.WithinTransaction(t.Context(), func(tc context.Context) error {
			for _, event := range events {
				owner, err := repo.FindProcessed(tc, event.Position)
				if err != nil {
					return err
				}
				if owner == nil {
					t.Fatal("occurrence evidence lost")
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		replay := append([]eventstore.ResolvedEvent(nil), events...)
		for i := range replay {
			replay[i].Event.Data = map[string]any{"bad": "already processed"}
		}
		if _, err := ProjectBatch(t.Context(), replay, repo); err != nil {
			t.Fatal(err)
		}
		if nativeTodoSnapshot(t, pool) != snapshot {
			t.Fatal("historical redelivery repeated native effects")
		}
	}
}
func TestSQLiteTodoProjectionReadsPrecedingOccurrenceState(t *testing.T) {
	repo, pool := nativeTodoProjection(t)
	events := todoProjectionHistory(t)
	if _, err := ProjectBatch(t.Context(), events[:4], repo); err != nil {
		t.Fatal(err)
	}
	snapshot := nativeTodoSnapshot(t, pool)
	if !strings.Contains(snapshot, `"completed":false`) || strings.Contains(snapshot, `"completed_at":null`) || strings.Contains(snapshot, `"reopened_at":null`) {
		t.Fatal("reopening discarded completion facts or left stale derived status")
	}
	if _, err := ProjectBatch(t.Context(), events[4:], repo); err != nil {
		t.Fatal(err)
	}
}
func TestSQLiteTodoProjectionWriteFailureRollsBackWholeBatch(t *testing.T) {
	repo, pool := nativeTodoProjection(t)
	// The second event triggers a native constraint error after the first event
	// has already been saved on this transaction's connection.
	if _, err := pool.ExecContext(t.Context(), "CREATE TRIGGER injected_failure BEFORE UPDATE ON todo_items WHEN NEW.title='Renamed' BEGIN SELECT RAISE(ABORT,'injected failure'); END"); err != nil {
		t.Fatal(err)
	}
	if _, err := ProjectBatch(t.Context(), todoProjectionHistory(t)[:2], repo); err == nil {
		t.Fatal("native write failure ignored")
	}
	if nativeTodoSnapshot(t, pool) != "[]" {
		t.Fatal("prefix state/evidence survived native rollback")
	}
}
func TestSQLiteTodoProjectionTransactionContextsFailClosed(t *testing.T) {
	repo, pool := nativeTodoProjection(t)
	other := NewSQLiteProjectionRepository(pool)
	events := todoProjectionHistory(t)
	facts, err := decodeTodoEvent(events[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Load(t.Context(), facts.todoID); !errors.Is(err, errTodoProjectionTransaction) {
		t.Fatal("unbound read accepted")
	}
	if err := repo.Save(t.Context(), TodoProjectionState{}, TodoProjectionOccurrence{}); !errors.Is(err, errTodoProjectionTransaction) {
		t.Fatal("unbound write accepted")
	}
	var escaped context.Context
	failure := errors.New("abort after native save")
	err = repo.WithinTransaction(t.Context(), func(tc context.Context) error {
		escaped = tc
		if _, err := other.Load(tc, facts.todoID); !errors.Is(err, errTodoProjectionTransaction) {
			t.Fatal("foreign transaction accepted")
		}
		if err := repo.WithinTransaction(tc, func(context.Context) error { return nil }); err == nil {
			t.Fatal("nested transaction accepted")
		}
		occurrence := TodoProjectionOccurrence{TodoID: facts.todoID, UserID: facts.userID, Title: facts.title, At: facts.at, EventType: TodoCreated, Position: events[0].Position}
		next, err := applyTodoProjection(nil, occurrence)
		if err != nil {
			return err
		}
		if err := repo.Save(tc, next, occurrence); err != nil {
			return err
		}
		current, err := repo.Load(tc, facts.todoID)
		if err != nil {
			return err
		}
		if current == nil || current.Title != facts.title {
			t.Fatal("transaction read did not observe its own native write")
		}
		if nativeTodoSnapshot(t, pool) != "[]" {
			t.Fatal("transaction write leaked before commit")
		}
		return failure
	})
	if !errors.Is(err, failure) || nativeTodoSnapshot(t, pool) != "[]" {
		t.Fatal("transaction rollback failed")
	}
	var committed context.Context
	if err := repo.WithinTransaction(t.Context(), func(tc context.Context) error { committed = tc; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Load(committed, facts.todoID); !errors.Is(err, errTodoProjectionTransaction) {
		t.Fatal("post-commit context accepted")
	}

	if _, err := repo.Load(escaped, facts.todoID); !errors.Is(err, errTodoProjectionTransaction) {
		t.Fatal("escaped read accepted")
	}
	if err := repo.Save(escaped, TodoProjectionState{}, TodoProjectionOccurrence{}); !errors.Is(err, errTodoProjectionTransaction) {
		t.Fatal("escaped write accepted")
	}
}
