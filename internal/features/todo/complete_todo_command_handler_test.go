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

func TestCompleteTodoPersistsOnceAndEnforcesOwnership(t *testing.T) {
	user, id := uuidv7.NewString(), uuidv7.NewString()
	store := eventstoretest.New(t, mustTodoEvent(NewTodoCreatedEvent(id, user, "Ship starter", time.Now(), nil)))
	command := CompleteTodoCommand{UserID: user, TodoID: id, Metadata: eventstore.SystemCommandMetadata("test", "complete")}
	result, err := CompleteTodoCommandHandler(t.Context(), command, store, store)
	if err != nil || result.Skipped {
		t.Fatalf("complete: %#v %v", result, err)
	}
	retry, err := CompleteTodoCommandHandler(t.Context(), command, store, store)
	if err != nil || !retry.Skipped {
		t.Fatalf("repeat: %#v %v", retry, err)
	}
	command.UserID = uuidv7.NewString()
	if _, err := CompleteTodoCommandHandler(t.Context(), command, store, store); !errors.Is(err, eventstore.ErrNotFound) {
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
	_, err := CompleteTodoCommandHandler(t.Context(), CompleteTodoCommand{UserID: user, TodoID: id}, store, reader)
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
	if _, err := DeleteTodoCommandHandler(ctx, DeleteTodoCommand{UserID: r.user, TodoID: r.todo}, r.EmbeddedOrisun, r.EmbeddedOrisun); err != nil {
		r.t.Fatal(err)
	}
	return latest, nil
}
