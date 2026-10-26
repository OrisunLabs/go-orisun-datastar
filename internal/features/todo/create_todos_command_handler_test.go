package todo

import (
	"errors"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func TestCreateTodosUsesOneWriteAndStableCreationIDs(t *testing.T) {
	store := eventstoretest.New(t)
	user := uuidv7.NewString()
	command := CreateTodosCommand{UserID: user, Titles: map[string]string{uuidv7.NewString(): "First", uuidv7.NewString(): "Second"}}
	for range 2 {
		if err := CreateTodosCommandHandler(t.Context(), command, store, store); err != nil {
			t.Fatal(err)
		}
	}
	events := createdTodoFacts(t, store)
	if len(events) != 2 || events[0].WriteID == "" || events[0].WriteID != events[1].WriteID {
		t.Fatal("creation was duplicated or split across writes")
	}
	context, err := store.GetWriteContext(t.Context(), events[0].WriteID)
	if err != nil || len(context.Consistency) != 1 || len(context.Consistency[0].Query.Criteria) != 2 {
		t.Fatalf("creation context=%#v error=%v", context, err)
	}
}
func TestCreateTodosRejectsConflictingIdentityWithoutPartialCreation(t *testing.T) {
	store := eventstoretest.New(t)
	existing := uuidv7.NewString()
	if err := CreateTodosCommandHandler(t.Context(), CreateTodosCommand{UserID: uuidv7.NewString(), Titles: map[string]string{existing: "Another owner"}}, store, store); err != nil {
		t.Fatal(err)
	}
	err := CreateTodosCommandHandler(t.Context(), CreateTodosCommand{UserID: uuidv7.NewString(), Titles: map[string]string{uuidv7.NewString(): "New", existing: "Conflicting"}}, store, store)
	if !errors.Is(err, eventstore.ErrConflict) || len(createdTodoFacts(t, store)) != 1 {
		t.Fatalf("partial creation: %v", err)
	}
}
func TestCreateTodosRejectsConcurrentCollisionAsOneBatch(t *testing.T) {
	store := eventstoretest.New(t)
	user, id := uuidv7.NewString(), uuidv7.NewString()
	reader := &bulkChangeAfterRead{EmbeddedOrisun: store, change: func() {
		if err := CreateTodosCommandHandler(t.Context(), CreateTodosCommand{UserID: user, Titles: map[string]string{id: "Concurrent"}}, store, store); err != nil {
			t.Fatal(err)
		}
	}}
	err := CreateTodosCommandHandler(t.Context(), CreateTodosCommand{UserID: user, Titles: map[string]string{id: "Outer", uuidv7.NewString(): "Sibling"}}, store, reader)
	if !errors.Is(err, eventstore.ErrConflict) || len(createdTodoFacts(t, store)) != 1 {
		t.Fatalf("partial concurrent creation: %v", err)
	}
}
func createdTodoFacts(t *testing.T, store *eventstore.EmbeddedOrisun) []eventstore.ResolvedEvent {
	t.Helper()
	events, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: TodoCreated}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return events
}
