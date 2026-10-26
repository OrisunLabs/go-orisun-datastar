package todo

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func nativeTodoProjection(t *testing.T) (*PostgreSQLProjectionRepository, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL projection checks")
	}
	admin, err := pgxpool.New(t.Context(), url)
	if err != nil {
		t.Fatal(err)
	}
	schema := "todo_projection_" + strings.ReplaceAll(uuidv7.NewString(), "-", "")
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
	start := strings.Index(string(migration), "CREATE TABLE IF NOT EXISTS todo_items")
	if start < 0 {
		t.Fatal("todo owner schema not found")
	}
	if _, err := pool.Exec(t.Context(), string(migration)[start:]); err != nil {
		t.Fatal(err)
	}
	return NewPostgreSQLProjectionRepository(pool), pool
}
func nativeTodoSnapshot(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var result string
	if err := pool.QueryRow(t.Context(), "SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY todo_id),'[]'::jsonb)::text FROM todo_items t").Scan(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestPostgresTodoProjectionBatchPartitionEquivalence(t *testing.T) {
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
		if !strings.Contains(snapshot, `"completed": true`) {
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
func TestPostgresTodoProjectionReadsPrecedingOccurrenceState(t *testing.T) {
	repo, pool := nativeTodoProjection(t)
	events := todoProjectionHistory(t)
	if _, err := ProjectBatch(t.Context(), events[:4], repo); err != nil {
		t.Fatal(err)
	}
	snapshot := nativeTodoSnapshot(t, pool)
	if !strings.Contains(snapshot, `"completed": false`) || strings.Contains(snapshot, `"completed_at": null`) || strings.Contains(snapshot, `"reopened_at": null`) {
		t.Fatal("reopening discarded completion facts or left stale derived status")
	}
	if _, err := ProjectBatch(t.Context(), events[4:], repo); err != nil {
		t.Fatal(err)
	}
}
func TestPostgresTodoProjectionWriteFailureRollsBackWholeBatch(t *testing.T) {
	repo, pool := nativeTodoProjection(t)
	// The second event triggers a native constraint error after the first event
	// has already been saved on this transaction's connection.
	if _, err := pool.Exec(t.Context(), "ALTER TABLE todo_items ADD CONSTRAINT injected_failure CHECK(title <> 'Renamed')"); err != nil {
		t.Fatal(err)
	}
	if _, err := ProjectBatch(t.Context(), todoProjectionHistory(t)[:2], repo); err == nil {
		t.Fatal("native write failure ignored")
	}
	if nativeTodoSnapshot(t, pool) != "[]" {
		t.Fatal("prefix state/evidence survived native rollback")
	}
}
func TestPostgresTodoProjectionTransactionContextsFailClosed(t *testing.T) {
	repo, pool := nativeTodoProjection(t)
	other := NewPostgreSQLProjectionRepository(pool)
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
