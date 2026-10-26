package todo

import (
	"errors"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func TestDeleteTodosWritesOneBatchAndRetriesDeletedOutcomes(t *testing.T) {
	store, user, operations := bulkTodoFixture(t, true)
	command := DeleteTodosCommand{UserID: user, Operations: operations, RequireCompleted: true}
	for range 2 {
		if err := DeleteTodosCommandHandler(t.Context(), command, store, store); err != nil {
			t.Fatal(err)
		}
	}
	assertBulkWrite(t, store, TodoDeleted, operations)
	if countSaved(t, store, TodoDeleted) != 2 {
		t.Fatal("retry deleted again")
	}
}

func TestDeleteTodosRejectsConcurrentReopening(t *testing.T) {
	store, user, operations := bulkTodoFixture(t, true)
	reader := &bulkChangeAfterRead{EmbeddedOrisun: store, change: func() {
		for id := range operations {
			if err := ReopenTodosCommandHandler(t.Context(), ReopenTodosCommand{UserID: user, Operations: map[string]string{id: uuidv7.NewString()}}, store, store); err != nil {
				t.Fatal(err)
			}
			break
		}
	}}
	err := DeleteTodosCommandHandler(t.Context(), DeleteTodosCommand{UserID: user, Operations: operations, RequireCompleted: true}, store, reader)
	if !errors.Is(err, eventstore.ErrConflict) || countSaved(t, store, TodoDeleted) != 0 {
		t.Fatalf("partial concurrent clearing: %v", err)
	}
}

func TestDeleteTodosValidatesInventoryBeforeEffects(t *testing.T) {
	user := uuidv7.NewString()
	if err := DeleteTodosCommandHandler(t.Context(), DeleteTodosCommand{UserID: user}, nil, nil); err != nil {
		t.Fatal(err)
	}
	duplicate := uuidv7.NewString()
	err := DeleteTodosCommandHandler(t.Context(), DeleteTodosCommand{UserID: user, Operations: map[string]string{uuidv7.NewString(): duplicate, uuidv7.NewString(): duplicate}}, nil, nil)
	if !errors.Is(err, eventstore.ErrInvalidEvent) {
		t.Fatalf("duplicate operation IDs: %v", err)
	}
}
