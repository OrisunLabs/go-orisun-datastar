package todo

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func TestCompleteTodosWritesAtomicallyAndRetriesHistoricalOutcomes(t *testing.T) {
	store, user, operations := bulkTodoFixture(t, false)
	command := CompleteTodosCommand{UserID: user, Operations: operations, Metadata: eventstore.SystemCommandMetadata("test", "bulk-complete")}
	if err := CompleteTodosCommandHandler(t.Context(), command, store, store); err != nil {
		t.Fatal(err)
	}
	assertBulkWrite(t, store, TodoCompleted, operations)
	for id := range operations {
		if err := ReopenTodosCommandHandler(t.Context(), ReopenTodosCommand{UserID: user, Operations: map[string]string{id: uuidv7.NewString()}}, store, store); err != nil {
			t.Fatal(err)
		}
		break
	}
	if err := CompleteTodosCommandHandler(t.Context(), command, store, store); err != nil {
		t.Fatal(err)
	}
	if countSaved(t, store, TodoCompleted) != 2 {
		t.Fatal("retry re-applied historical completion")
	}
}

func TestCompleteTodosRejectsForeignMemberWithoutPartialCompletion(t *testing.T) {
	store, user, operations := bulkTodoFixture(t, false)
	foreign := uuidv7.NewString()
	foreignEvent := mustTodoEvent(NewTodoCreatedEvent(foreign, uuidv7.NewString(), "Another user", time.Now(), nil))
	if _, err := store.SaveEventsV2(t.Context(), []eventstore.DomainEvent{foreignEvent}, nil); err != nil {
		t.Fatal(err)
	}
	operations[foreign] = uuidv7.NewString()
	if err := CompleteTodosCommandHandler(t.Context(), CompleteTodosCommand{UserID: user, Operations: operations}, store, store); !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("missing member: %v", err)
	}
	if countSaved(t, store, TodoCompleted) != 0 {
		t.Fatal("invalid inventory partially completed")
	}
}

func TestCompleteTodosRejectsConcurrentDeletionAsOneBatch(t *testing.T) {
	store, user, operations := bulkTodoFixture(t, false)
	reader := &bulkChangeAfterRead{EmbeddedOrisun: store, change: func() {
		for id := range operations {
			if err := DeleteTodosCommandHandler(t.Context(), DeleteTodosCommand{UserID: user, Operations: map[string]string{id: uuidv7.NewString()}}, store, store); err != nil {
				t.Fatal(err)
			}
			break
		}
	}}
	err := CompleteTodosCommandHandler(t.Context(), CompleteTodosCommand{UserID: user, Operations: operations}, store, reader)
	if !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("concurrent batch: %v", err)
	}
	if countSaved(t, store, TodoCompleted) != 0 {
		t.Fatal("concurrent conflict partially completed")
	}
}

func TestCompleteTodosRejectsOperationIDReassignment(t *testing.T) {
	store, user, operations := bulkTodoFixture(t, false)
	command := CompleteTodosCommand{UserID: user, Operations: operations}
	if err := CompleteTodosCommandHandler(t.Context(), command, store, store); err != nil {
		t.Fatal(err)
	}
	changed := map[string]string{}
	for _, operation := range operations {
		changed[uuidv7.NewString()] = operation
		break
	}
	if err := CompleteTodosCommandHandler(t.Context(), CompleteTodosCommand{UserID: user, Operations: changed}, store, store); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("reassigned receipt: %v", err)
	}
}

func TestCompleteTodoPersistsOnceAndEnforcesOwnership(t *testing.T) {
	user, id := uuidv7.NewString(), uuidv7.NewString()
	store := eventstoretest.New(t, mustTodoEvent(NewTodoCreatedEvent(id, user, "Ship starter", time.Now(), nil)))
	command := CompleteTodosCommand{UserID: user, Operations: map[string]string{id: uuidv7.NewString()}, Metadata: eventstore.SystemCommandMetadata("test", "complete")}
	if err := CompleteTodosCommandHandler(t.Context(), command, store, store); err != nil {
		t.Fatal(err)
	}
	if err := CompleteTodosCommandHandler(t.Context(), command, store, store); err != nil {
		t.Fatal(err)
	}

	command.UserID = uuidv7.NewString()
	if err := CompleteTodosCommandHandler(t.Context(), command, store, store); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("ownership: %v", err)
	}
	if got := countSaved(t, store, TodoCompleted); got != 1 {
		t.Fatalf("completions: %d", got)
	}
	events, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, TodoReadModelEventHandlerQuery())
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if _, err := store.GetWriteContext(t.Context(), last.WriteID); err != nil {
		t.Fatal(err)
	}
	if _, exists := last.Event.Metadata["scope_event_ids"]; exists {
		t.Fatal("obsolete metadata persisted")
	}
	if _, exists := last.Event.Metadata["audit"]; !exists {
		t.Fatal("missing command audit")
	}
}

func TestCompleteTodoRejectsConcurrentDeletion(t *testing.T) {
	user, id := uuidv7.NewString(), uuidv7.NewString()
	store := eventstoretest.New(t, mustTodoEvent(NewTodoCreatedEvent(id, user, "Ship starter", time.Now(), nil)))
	reader := &deleteAfterRead{EmbeddedOrisun: store, user: user, todo: id, t: t}
	err := CompleteTodosCommandHandler(t.Context(), CompleteTodosCommand{UserID: user, Operations: map[string]string{id: uuidv7.NewString()}}, store, reader)
	if !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("concurrent completion: %v", err)
	}
	if got := countSaved(t, store, TodoCompleted); got != 0 {
		t.Fatalf("invalid completions: %d", got)
	}
}

type deleteAfterRead struct {
	*eventstore.EmbeddedOrisun
	user, todo string
	t          *testing.T
}

func (r *deleteAfterRead) GetLatestByCriteria(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
	latest, err := r.EmbeddedOrisun.GetLatestByCriteria(ctx, criteria)
	if err != nil {
		return latest, err
	}
	if err := DeleteTodosCommandHandler(ctx, DeleteTodosCommand{UserID: r.user, Operations: map[string]string{r.todo: uuidv7.NewString()}}, r.EmbeddedOrisun, r.EmbeddedOrisun); err != nil {
		r.t.Fatal(err)
	}
	return latest, nil
}
