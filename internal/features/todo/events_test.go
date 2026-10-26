package todo

import (
	"errors"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func TestTodoCommandsRejectMalformedAuthority(t *testing.T) {
	user, id := uuidv7.NewString(), uuidv7.NewString()
	now := time.Now()
	root := mustTodoEvent(NewTodoCreatedEvent(id, user, "Ship starter", now, nil))
	contracts := []struct {
		event              eventstore.DomainEvent
		idField, timeField string
	}{
		{root, TodoCreatedIDField, TodoCreatedAtField},
		{mustTodoEvent(NewTodoRenamedEvent(uuidv7.NewString(), id, user, "Renamed", now, nil)), TodoRenamedIDField, TodoRenamedAtField},
		{mustTodoEvent(NewTodoCompletedEvent(uuidv7.NewString(), id, user, now, nil)), TodoCompletedIDField, TodoCompletedAtField},
		{mustTodoEvent(NewTodoReopenedEvent(uuidv7.NewString(), id, user, now, nil)), TodoReopenedIDField, TodoReopenedAtField},
		{mustTodoEvent(NewTodoDeletedEvent(uuidv7.NewString(), id, user, now, nil)), TodoDeletedIDField, TodoDeletedAtField},
	}
	for _, contract := range contracts {
		mutations := map[string]func(map[string]any){
			"unknown field":         func(data map[string]any) { data["unexpected"] = true },
			"legacy scope":          func(data map[string]any) { data["scope"] = data["scopes"]; delete(data, "scopes") },
			"null scopes":           func(data map[string]any) { data["scopes"] = nil },
			"unknown ancestor":      func(data map[string]any) { data["scopes"] = map[string]any{"otherId": uuidv7.NewString()} },
			"missing user":          func(data map[string]any) { delete(data, "userId") },
			"null user":             func(data map[string]any) { data["userId"] = nil },
			"registration ancestry": func(data map[string]any) { data["scopes"] = map[string]any{"userRegisteredId": user} },
			"missing identity":      func(data map[string]any) { delete(data, contract.idField) },
			"mismatched envelope":   func(data map[string]any) { data[contract.idField] = uuidv7.NewString() },
			"invalid instant":       func(data map[string]any) { data[contract.timeField] = "yesterday" },
			"offset instant":        func(data map[string]any) { data[contract.timeField] = "2026-10-26T10:00:00+01:00" },
			"missing instant":       func(data map[string]any) { delete(data, contract.timeField) },
		}
		if contract.event.EventType == TodoCreated || contract.event.EventType == TodoRenamed {
			mutations["null title"] = func(data map[string]any) { data["title"] = nil }
			mutations["blank title"] = func(data map[string]any) { data["title"] = " " }
		}
		for name, mutate := range mutations {
			t.Run(contract.event.EventType+"/"+name, func(t *testing.T) {
				corrupt := contract.event
				corrupt.Data = eventstore.MustData(corrupt.Data)
				mutate(corrupt.Data)
				events := []eventstore.DomainEvent{corrupt}
				if corrupt.EventType != TodoCreated {
					events = append([]eventstore.DomainEvent{root}, events...)
				}
				store := eventstoretest.New(t, events...)
				if _, err := decodeTodoEvent(corrupt); !errors.Is(err, eventstore.ErrInvalidEvent) {
					t.Fatalf("decoder accepted corrupt event: %v", err)
				}
				// Command validation covers malformed events selected by its exact
				// authority query. Missing routing fields are still rejected by
				// the decoder and the boundary-wide projection subscription.
				scopes, _ := corrupt.Data["scopes"].(map[string]any)
				identityField := TodoCreatedIDField
				identityData := corrupt.Data
				if corrupt.EventType != TodoCreated {
					identityField = TodoScopeIDKey
					identityData = scopes
				}
				if corrupt.Data[TodoUserIDField] == user && identityData[identityField] == id {
					_, err := CompleteTodoCommandHandler(t.Context(), CompleteTodoCommand{UserID: user, TodoID: id}, store, store)
					if !errors.Is(err, eventstore.ErrInvalidEvent) {
						t.Fatalf("got %v, want invalid event", err)
					}
				}

				// The same contract must fail before any projection write or publication.
				handler := &TodoReadModelEventHandler{}
				if err := handler.handle(t.Context(), eventstore.ResolvedEvent{Event: corrupt}); !errors.Is(err, eventstore.ErrInvalidEvent) {
					t.Fatalf("projection accepted malformed event: %v", err)
				}
			})
		}
	}
}

func TestTodoLifecycleUsesCanonicalEventContracts(t *testing.T) {
	user := uuidv7.NewString()
	store := eventstoretest.New(t)
	created, err := CreateTodoCommandHandler(t.Context(), CreateTodoCommand{UserID: user, Title: "  First title  "}, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = RenameTodoCommandHandler(t.Context(), RenameTodoCommand{UserID: user, TodoID: created.TodoID, Title: "Next title"}, store, store); err != nil {
		t.Fatal(err)
	}
	if _, err = CompleteTodoCommandHandler(t.Context(), CompleteTodoCommand{UserID: user, TodoID: created.TodoID}, store, store); err != nil {
		t.Fatal(err)
	}
	if _, err = ReopenTodoCommandHandler(t.Context(), ReopenTodoCommand{UserID: user, TodoID: created.TodoID}, store, store); err != nil {
		t.Fatal(err)
	}
	if _, err = DeleteTodoCommandHandler(t.Context(), DeleteTodoCommand{UserID: user, TodoID: created.TodoID}, store, store); err != nil {
		t.Fatal(err)
	}
	events, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, TodoReadModelEventHandlerQuery())
	if err != nil || len(events) != 5 {
		t.Fatalf("events=%d error=%v", len(events), err)
	}
	for _, event := range events {
		facts, err := decodeTodoEvent(event.Event)
		if err != nil || facts.todoID != created.TodoID || facts.userID != user {
			t.Fatalf("facts=%#v error=%v", facts, err)
		}
		if _, exists := event.Event.Data["scope"]; exists {
			t.Fatal("singular scope persisted")
		}
		if event.Event.Data["userId"] != user {
			t.Fatal("missing cross-domain user reference")
		}
		if _, exists := event.Event.Data["userRegisteredId"]; exists {
			t.Fatal("registration event identity leaked into todo payload")
		}
		scopes, hasScopes := event.Event.Data["scopes"].(map[string]any)
		if event.Event.EventType == TodoCreated {
			if hasScopes {
				t.Fatal("todo root must omit scopes")
			}
		} else if len(scopes) != 1 || scopes[TodoCreatedIDField] != created.TodoID {
			t.Fatalf("unexpected todo ancestry: %#v", scopes)
		}
		if _, exists := event.Event.Data["todoId"]; exists {
			t.Fatal("entity ID duplicated in payload")
		}
	}
	if _, err := CompleteTodoCommandHandler(t.Context(), CompleteTodoCommand{UserID: user, TodoID: created.TodoID}, store, store); !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("deleted todo: %v", err)
	}
}

func TestTodoConstructorsRejectInvalidIdentityAndTime(t *testing.T) {
	id, user := uuidv7.NewString(), uuidv7.NewString()
	for _, bad := range []string{"", "not-a-uuid", "00000000-0000-0000-0000-000000000000"} {
		if _, err := NewTodoCreatedEvent(bad, user, "Title", time.Now(), nil); !errors.Is(err, eventstore.ErrInvalidEvent) {
			t.Fatalf("invalid identity: %v", err)
		}
		if _, err := NewTodoCompletedEvent(id, id, bad, time.Now(), nil); !errors.Is(err, eventstore.ErrInvalidEvent) {
			t.Fatalf("invalid owner: %v", err)
		}
	}
	if _, err := NewTodoCreatedEvent(id, user, "Title", time.Time{}, nil); !errors.Is(err, eventstore.ErrInvalidEvent) {
		t.Fatalf("zero instant: %v", err)
	}
}
