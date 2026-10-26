package todo

import (
	"context"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

func TestRenameTodoCommandSkipsUnchangedTitle(t *testing.T) {
	t.Parallel()

	store := eventstoretest.New(t,
		mustTodoEvent(NewTodoCreatedEvent("019a0000-0000-7000-8000-000000000002", "019a0000-0000-7000-8000-000000000001", "Ship starter", time.Now(), nil)),
	)

	result, err := RenameTodoCommandHandler(context.Background(), RenameTodoCommand{
		UserID: "019a0000-0000-7000-8000-000000000001",
		TodoID: "019a0000-0000-7000-8000-000000000002",
		Title:  "  Ship starter  ",
	}, store, store)
	if err != nil {
		t.Fatalf("rename todo: %v", err)
	}
	if !result.Skipped {
		t.Fatal("expected unchanged rename to be skipped")
	}
	if got := countSaved(t, store, TodoRenamed); got != 0 {
		t.Fatalf("expected no rename event, got %d", got)
	}
}

func TestBulkTodoActionsAppendExpectedEvents(t *testing.T) {
	t.Parallel()

	readModel := fakeTodoReadModel{todos: []views.Todo{
		{TodoID: "019a0000-0000-7000-8000-000000000003", Title: "Active one"},
		{TodoID: "019a0000-0000-7000-8000-000000000004", Title: "Active two"},
		{TodoID: "019a0000-0000-7000-8000-000000000005", Title: "Done one", Completed: true},
	}}
	store := eventstoretest.New(t,
		mustTodoEvent(NewTodoCreatedEvent("019a0000-0000-7000-8000-000000000003", "019a0000-0000-7000-8000-000000000001", "Active one", time.Now(), nil)),
		mustTodoEvent(NewTodoCreatedEvent("019a0000-0000-7000-8000-000000000004", "019a0000-0000-7000-8000-000000000001", "Active two", time.Now(), nil)),
		mustTodoEvent(NewTodoCreatedEvent("019a0000-0000-7000-8000-000000000005", "019a0000-0000-7000-8000-000000000001", "Done one", time.Now(), nil)),
		mustTodoEvent(NewTodoCompletedEvent("019a0000-0000-7000-8000-000000000006", "019a0000-0000-7000-8000-000000000005", "019a0000-0000-7000-8000-000000000001", time.Now(), nil)),
	)

	if err := CompleteAllActiveTodosCommandHandler(context.Background(), CompleteAllActiveTodosCommand{
		UserID: "019a0000-0000-7000-8000-000000000001",
	}, readModel, store, store); err != nil {
		t.Fatalf("complete active todos: %v", err)
	}
	if got := countSaved(t, store, TodoCompleted); got != 3 {
		t.Fatalf("expected two new completions plus the seeded completion, got %d", got)
	}

	if err := ClearCompletedTodosCommandHandler(context.Background(), ClearCompletedTodosCommand{
		UserID: "019a0000-0000-7000-8000-000000000001",
	}, readModel, store, store); err != nil {
		t.Fatalf("clear completed todos: %v", err)
	}
	if got := countSaved(t, store, TodoDeleted); got != 1 {
		t.Fatalf("expected one deleted event, got %d", got)
	}
}

type fakeTodoReadModel struct {
	todos []views.Todo
	err   error
}

func (m fakeTodoReadModel) List(context.Context, string) ([]views.Todo, error) {
	if m.err != nil {
		return nil, m.err
	}
	return append([]views.Todo(nil), m.todos...), nil
}

func countSaved(t *testing.T, store *eventstore.EmbeddedOrisun, eventType string) int {
	t.Helper()
	events, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: eventType}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return len(events)
}

func mustTodoEvent(event eventstore.DomainEvent, err error) eventstore.DomainEvent {
	if err != nil {
		panic(err)
	}
	return event
}
