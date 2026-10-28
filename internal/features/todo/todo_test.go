package todo

import (
	"context"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func TestRenameTodoCommandSkipsUnchangedTitle(t *testing.T) {
	t.Parallel()

	store := eventstoretest.New(t,
		mustTodoEvent(NewTodoCreatedEvent("019a0000-0000-7000-8000-000000000002", "019a0000-0000-7000-8000-000000000001", "Ship starter", time.Now(), nil)),
	)

	err := RenameTodosCommandHandler(context.Background(), RenameTodosCommand{UserID: "019a0000-0000-7000-8000-000000000001", Operations: map[string]TodoRename{"019a0000-0000-7000-8000-000000000002": {EventID: uuidv7.NewString(), Title: "  Ship starter  ", ExpectedTitle: "Ship starter"}}}, store, store)
	if err != nil {
		t.Fatal(err)
	}
	if got := countSaved(t, store, TodoRenamed); got != 0 {
		t.Fatalf("expected no rename event, got %d", got)
	}
}

func TestBulkTodoActionsAppendExpectedEvents(t *testing.T) {
	t.Parallel()

	store := eventstoretest.New(t,
		mustTodoEvent(NewTodoCreatedEvent("019a0000-0000-7000-8000-000000000003", "019a0000-0000-7000-8000-000000000001", "Active one", time.Now(), nil)),
		mustTodoEvent(NewTodoCreatedEvent("019a0000-0000-7000-8000-000000000004", "019a0000-0000-7000-8000-000000000001", "Active two", time.Now(), nil)),
		mustTodoEvent(NewTodoCreatedEvent("019a0000-0000-7000-8000-000000000005", "019a0000-0000-7000-8000-000000000001", "Done one", time.Now(), nil)),
		mustTodoEvent(NewTodoCompletedEvent("019a0000-0000-7000-8000-000000000006", "019a0000-0000-7000-8000-000000000005", "019a0000-0000-7000-8000-000000000001", time.Now(), nil)),
	)

	if err := CompleteTodosCommandHandler(context.Background(), CompleteTodosCommand{
		UserID:     "019a0000-0000-7000-8000-000000000001",
		Operations: map[string]string{"019a0000-0000-7000-8000-000000000003": "019a0000-0000-7000-8000-000000000007", "019a0000-0000-7000-8000-000000000004": "019a0000-0000-7000-8000-000000000008"},
	}, store, store); err != nil {
		t.Fatalf("complete active todos: %v", err)
	}
	if got := countSaved(t, store, TodoCompleted); got != 3 {
		t.Fatalf("expected two new completions plus the seeded completion, got %d", got)
	}

	if err := DeleteTodosCommandHandler(context.Background(), DeleteTodosCommand{
		UserID:     "019a0000-0000-7000-8000-000000000001",
		Operations: map[string]string{"019a0000-0000-7000-8000-000000000005": "019a0000-0000-7000-8000-000000000009"},
	}, store, store); err != nil {
		t.Fatalf("clear completed todos: %v", err)
	}
	if got := countSaved(t, store, TodoDeleted); got != 1 {
		t.Fatalf("expected one deleted event, got %d", got)
	}
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

func bulkTodoFixture(t *testing.T, completed bool) (*eventstore.EmbeddedOrisun, string, map[string]string) {
	t.Helper()
	user := uuidv7.NewString()
	operations := map[string]string{}
	events := []eventstore.DomainEvent{}
	for range 2 {
		id := uuidv7.NewString()
		operations[id] = uuidv7.NewString()
		events = append(events, mustTodoEvent(NewTodoCreatedEvent(id, user, "Batch todo", time.Now(), nil)))
		if completed {
			events = append(events, mustTodoEvent(NewTodoCompletedEvent(uuidv7.NewString(), id, user, time.Now(), nil)))
		}
	}
	return eventstoretest.New(t, events...), user, operations
}

func assertBulkWrite(t *testing.T, store *eventstore.EmbeddedOrisun, kind string, operations map[string]string) {
	t.Helper()
	events, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: kind}}}}})
	if err != nil {
		t.Fatal(err)
	}
	writeID := ""
	found := 0
	for _, event := range events {
		facts, err := decodeTodoEvent(event.Event)
		if err != nil {
			t.Fatal(err)
		}
		if event.Event.EventID != operations[facts.todoID] {
			continue
		}
		if writeID == "" {
			writeID = event.WriteID
		}
		if writeID == "" || event.WriteID != writeID {
			t.Fatal("bulk members were not appended in one write")
		}
		if _, duplicated := event.Event.Metadata["consistency"]; duplicated {
			t.Fatal("batch observations duplicated into per-event metadata")
		}
		found++
	}
	if found != len(operations) {
		t.Fatalf("bulk outcomes=%d want=%d", found, len(operations))
	}
	context, err := store.GetWriteContext(t.Context(), writeID)
	if err != nil || len(context.Consistency) != 1 || len(context.Consistency[0].Query.Criteria) != 6*len(operations) {
		t.Fatalf("batch observation=%#v error=%v", context, err)
	}
}

type bulkChangeAfterRead struct {
	*eventstore.EmbeddedOrisun
	change func()
}

func (r *bulkChangeAfterRead) GetLatestByCriteria(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
	result, err := r.EmbeddedOrisun.GetLatestByCriteria(ctx, criteria)
	if err == nil {
		r.change()
	}
	return result, err
}
