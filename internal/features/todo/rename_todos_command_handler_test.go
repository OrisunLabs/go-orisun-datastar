package todo

import (
	"errors"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func TestRenameTodosUsesOneWriteAndHistoricalRetryReceipts(t *testing.T) {
	store, user, items := bulkTodoFixture(t, false)
	operations := map[string]TodoRename{}
	ids := map[string]string{}
	for id, operation := range items {
		operations[id] = TodoRename{EventID: operation, Title: "Renamed", ExpectedTitle: "Batch todo"}
		ids[id] = operation
	}
	command := RenameTodosCommand{UserID: user, Operations: operations}
	if err := RenameTodosCommandHandler(t.Context(), command, store, store); err != nil {
		t.Fatal(err)
	}
	assertBulkWrite(t, store, TodoRenamed, ids)
	for id := range items {
		if err := RenameTodosCommandHandler(t.Context(), RenameTodosCommand{UserID: user, Operations: map[string]TodoRename{id: {EventID: uuidv7.NewString(), Title: "Later", ExpectedTitle: "Renamed"}}}, store, store); err != nil {
			t.Fatal(err)
		}
		break
	}
	if err := RenameTodosCommandHandler(t.Context(), command, store, store); err != nil {
		t.Fatal(err)
	}
	if countSaved(t, store, TodoRenamed) != 3 {
		t.Fatal("historical retry renamed again")
	}
}
func TestRenameTodosRejectsStaleExpectedTitleWithoutPartialRenaming(t *testing.T) {
	store, user, items := bulkTodoFixture(t, false)
	operations := map[string]TodoRename{}
	for id, operation := range items {
		operations[id] = TodoRename{EventID: operation, Title: "Renamed", ExpectedTitle: "Batch todo"}
	}
	for id, item := range operations {
		item.ExpectedTitle = "Stale"
		operations[id] = item
		break
	}
	err := RenameTodosCommandHandler(t.Context(), RenameTodosCommand{UserID: user, Operations: operations}, store, store)
	if !errors.Is(err, eventstore.ErrConflict) || countSaved(t, store, TodoRenamed) != 0 {
		t.Fatalf("partial stale rename: %v", err)
	}
}
func TestUnchangedRenameCannotOverwriteInterveningChange(t *testing.T) {
	store, user, items := bulkTodoFixture(t, false)
	for id, operation := range items {
		unchanged := RenameTodosCommand{UserID: user, Operations: map[string]TodoRename{id: {EventID: operation, Title: "Batch todo", ExpectedTitle: "Batch todo"}}}
		if err := RenameTodosCommandHandler(t.Context(), unchanged, store, store); err != nil {
			t.Fatal(err)
		}
		if err := RenameTodosCommandHandler(t.Context(), RenameTodosCommand{UserID: user, Operations: map[string]TodoRename{id: {EventID: uuidv7.NewString(), Title: "Later", ExpectedTitle: "Batch todo"}}}, store, store); err != nil {
			t.Fatal(err)
		}
		if err := RenameTodosCommandHandler(t.Context(), unchanged, store, store); !errors.Is(err, eventstore.ErrConflict) {
			t.Fatalf("unchanged retry overwrote later intent: %v", err)
		}
		break
	}
	if countSaved(t, store, TodoRenamed) != 1 {
		t.Fatal("unchanged rename emitted an event")
	}
}
func TestRenameTodosRejectsConcurrentDeletionWithoutPartialRenaming(t *testing.T) {
	store, user, items := bulkTodoFixture(t, false)
	operations := map[string]TodoRename{}
	for id, operation := range items {
		operations[id] = TodoRename{EventID: operation, Title: "Renamed", ExpectedTitle: "Batch todo"}
	}
	reader := &bulkChangeAfterRead{EmbeddedOrisun: store, change: func() {
		for id := range items {
			if err := DeleteTodosCommandHandler(t.Context(), DeleteTodosCommand{UserID: user, Operations: map[string]string{id: uuidv7.NewString()}}, store, store); err != nil {
				t.Fatal(err)
			}
			break
		}
	}}
	err := RenameTodosCommandHandler(t.Context(), RenameTodosCommand{UserID: user, Operations: operations}, store, reader)
	if !errors.Is(err, eventstore.ErrConflict) || countSaved(t, store, TodoRenamed) != 0 {
		t.Fatalf("partial concurrent rename: %v", err)
	}
}
