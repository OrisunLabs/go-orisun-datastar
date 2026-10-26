package todo

import (
	"errors"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

func TestReopenTodosWritesOneBatch(t *testing.T) {
	store, user, operations := bulkTodoFixture(t, true)
	command := ReopenTodosCommand{UserID: user, Operations: operations}
	for range 2 {
		if err := ReopenTodosCommandHandler(t.Context(), command, store, store); err != nil {
			t.Fatal(err)
		}
	}
	assertBulkWrite(t, store, TodoReopened, operations)
	if countSaved(t, store, TodoReopened) != 2 {
		t.Fatal("retry emitted more reopen facts")
	}
}

func TestReopenTodosRejectsStaleSelectionWithoutPartialEffects(t *testing.T) {
	store, user, operations := bulkTodoFixture(t, true)
	for id := range operations {
		if err := ReopenTodosCommandHandler(t.Context(), ReopenTodosCommand{UserID: user, Operations: map[string]string{id: uuidv7.NewString()}}, store, store); err != nil {
			t.Fatal(err)
		}
		break
	}
	err := ReopenTodosCommandHandler(t.Context(), ReopenTodosCommand{UserID: user, Operations: operations}, store, store)
	if !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("stale selection: %v", err)
	}
	if countSaved(t, store, TodoReopened) != 1 {
		t.Fatal("stale selection partially applied")
	}
}

func TestReopenTodosRejectsConcurrentDeletion(t *testing.T) {
	store, user, operations := bulkTodoFixture(t, true)
	reader := &bulkChangeAfterRead{EmbeddedOrisun: store, change: func() {
		for id := range operations {
			if err := DeleteTodosCommandHandler(t.Context(), DeleteTodosCommand{UserID: user, Operations: map[string]string{id: uuidv7.NewString()}}, store, store); err != nil {
				t.Fatal(err)
			}
			break
		}
	}}
	err := ReopenTodosCommandHandler(t.Context(), ReopenTodosCommand{UserID: user, Operations: operations}, store, reader)
	if !errors.Is(err, eventstore.ErrConflict) || countSaved(t, store, TodoReopened) != 0 {
		t.Fatalf("partial concurrent reopen: %v", err)
	}
}
