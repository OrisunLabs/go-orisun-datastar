package todo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/google/uuid"
)

const (
	TodoCreated   = "TodoCreated"
	TodoRenamed   = "TodoRenamed"
	TodoCompleted = "TodoCompleted"
	TodoReopened  = "TodoReopened"
	TodoDeleted   = "TodoDeleted"
)

const (
	TodoCreatedIDField   = "todoCreatedId"
	TodoUserIDField      = "userId"
	TodoTitleField       = "title"
	TodoCreatedAtField   = "createdAt"
	TodoRenamedIDField   = "todoRenamedId"
	TodoRenamedAtField   = "renamedAt"
	TodoCompletedIDField = "todoCompletedId"
	TodoCompletedAtField = "completedAt"
	TodoReopenedIDField  = "todoReopenedId"
	TodoReopenedAtField  = "reopenedAt"
	TodoDeletedIDField   = "todoDeletedId"
	TodoDeletedAtField   = "deletedAt"
	TodoScopeIDKey       = TodoCreatedIDField
	TodoScopeIDField     = "scopes." + TodoScopeIDKey
)

type TodoCreatedEvent struct {
	UserID        string `json:"userId"`
	TodoCreatedID string `json:"todoCreatedId"`
	Title         string `json:"title"`
	CreatedAt     string `json:"createdAt"`
}

type TodoRenamedEvent struct {
	UserID        string     `json:"userId"`
	TodoRenamedID string     `json:"todoRenamedId"`
	Title         string     `json:"title"`
	RenamedAt     string     `json:"renamedAt"`
	Scopes        TodoScopes `json:"scopes"`
}

type TodoCompletedEvent struct {
	UserID          string     `json:"userId"`
	TodoCompletedID string     `json:"todoCompletedId"`
	CompletedAt     string     `json:"completedAt"`
	Scopes          TodoScopes `json:"scopes"`
}

type TodoReopenedEvent struct {
	UserID         string     `json:"userId"`
	TodoReopenedID string     `json:"todoReopenedId"`
	ReopenedAt     string     `json:"reopenedAt"`
	Scopes         TodoScopes `json:"scopes"`
}

type TodoDeletedEvent struct {
	UserID        string     `json:"userId"`
	TodoDeletedID string     `json:"todoDeletedId"`
	DeletedAt     string     `json:"deletedAt"`
	Scopes        TodoScopes `json:"scopes"`
}

type TodoScopes struct {
	TodoCreatedID string `json:"todoCreatedId"`
}

func NewTodoCreatedEvent(todoID, userID, title string, createdAt time.Time, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   todoID,
		EventType: TodoCreated,
		Data: eventstore.MustData(TodoCreatedEvent{
			TodoCreatedID: todoID,
			Title:         title,
			CreatedAt:     createdAt.UTC().Format(time.RFC3339Nano),
			UserID:        userID,
		}),
		Metadata: metadata,
	}
	_, err := DecodeTodoCreatedEvent(event)
	return event, err
}

func NewTodoRenamedEvent(todoRenamedID, todoID, userID, title string, renamedAt time.Time, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   todoRenamedID,
		EventType: TodoRenamed,
		Data: eventstore.MustData(TodoRenamedEvent{
			TodoRenamedID: todoRenamedID,
			Title:         title,
			RenamedAt:     renamedAt.UTC().Format(time.RFC3339Nano),
			UserID:        userID,
			Scopes:        todoScope(todoID),
		}),
		Metadata: metadata,
	}
	_, err := DecodeTodoRenamedEvent(event)
	return event, err
}

func NewTodoCompletedEvent(todoCompletedID, todoID, userID string, completedAt time.Time, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   todoCompletedID,
		EventType: TodoCompleted,
		Data: eventstore.MustData(TodoCompletedEvent{
			TodoCompletedID: todoCompletedID,
			CompletedAt:     completedAt.UTC().Format(time.RFC3339Nano),
			UserID:          userID,
			Scopes:          todoScope(todoID),
		}),
		Metadata: metadata,
	}
	_, err := DecodeTodoCompletedEvent(event)
	return event, err
}

func NewTodoReopenedEvent(todoReopenedID, todoID, userID string, reopenedAt time.Time, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   todoReopenedID,
		EventType: TodoReopened,
		Data: eventstore.MustData(TodoReopenedEvent{
			TodoReopenedID: todoReopenedID,
			ReopenedAt:     reopenedAt.UTC().Format(time.RFC3339Nano),
			UserID:         userID,
			Scopes:         todoScope(todoID),
		}),
		Metadata: metadata,
	}
	_, err := DecodeTodoReopenedEvent(event)
	return event, err
}

func NewTodoDeletedEvent(todoDeletedID, todoID, userID string, deletedAt time.Time, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   todoDeletedID,
		EventType: TodoDeleted,
		Data: eventstore.MustData(TodoDeletedEvent{
			TodoDeletedID: todoDeletedID,
			DeletedAt:     deletedAt.UTC().Format(time.RFC3339Nano),
			UserID:        userID,
			Scopes:        todoScope(todoID),
		}),
		Metadata: metadata,
	}
	_, err := DecodeTodoDeletedEvent(event)
	return event, err
}

func todoScope(todoID string) TodoScopes {
	return TodoScopes{TodoCreatedID: todoID}
}

func Channel(userID string) string {
	return "todo." + userID
}

// decodedTodoEvent is the validated event contract used by commands and projections.
// It carries no command decisions or database behavior.
type decodedTodoEvent struct {
	todoID string
	userID string
	title  string
	at     time.Time
}

func decodeTodoEvent(event eventstore.DomainEvent) (decodedTodoEvent, error) {
	switch event.EventType {
	case TodoCreated:
		value, err := DecodeTodoCreatedEvent(event)
		at, _ := time.Parse(time.RFC3339Nano, value.CreatedAt)
		return decodedTodoEvent{value.TodoCreatedID, value.UserID, value.Title, at}, err
	case TodoRenamed:
		value, err := DecodeTodoRenamedEvent(event)
		at, _ := time.Parse(time.RFC3339Nano, value.RenamedAt)
		return decodedTodoEvent{value.Scopes.TodoCreatedID, value.UserID, value.Title, at}, err
	case TodoCompleted:
		value, err := DecodeTodoCompletedEvent(event)
		at, _ := time.Parse(time.RFC3339Nano, value.CompletedAt)
		return decodedTodoEvent{value.Scopes.TodoCreatedID, value.UserID, "", at}, err
	case TodoReopened:
		value, err := DecodeTodoReopenedEvent(event)
		at, _ := time.Parse(time.RFC3339Nano, value.ReopenedAt)
		return decodedTodoEvent{value.Scopes.TodoCreatedID, value.UserID, "", at}, err
	case TodoDeleted:
		value, err := DecodeTodoDeletedEvent(event)
		at, _ := time.Parse(time.RFC3339Nano, value.DeletedAt)
		return decodedTodoEvent{value.Scopes.TodoCreatedID, value.UserID, "", at}, err
	default:
		return decodedTodoEvent{}, fmt.Errorf("%w: unknown todo event type %q", eventstore.ErrInvalidEvent, event.EventType)
	}
}

// decodePayload enforces the exact serialized schema, including required fields,
// scalar types and non-null nested objects. Canonical comparison is safe here:
// todo contracts contain only strings and typed scope objects.
func decodePayload(event eventstore.DomainEvent, eventType string, target any) error {
	if event.EventType != eventType {
		return fmt.Errorf("%w: expected %s", eventstore.ErrInvalidEvent, eventType)
	}
	encoded, err := json.Marshal(event.Data)
	if err != nil {
		return fmt.Errorf("%w: %v", eventstore.ErrInvalidEvent, err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%w: %v", eventstore.ErrInvalidEvent, err)
	}
	canonical := eventstore.MustData(target)
	if !reflect.DeepEqual(canonical, event.Data) {
		return fmt.Errorf("%w: missing, null or noncanonical fields in %s", eventstore.ErrInvalidEvent, eventType)
	}
	if !canonicalID(event.EventID) {
		return fmt.Errorf("%w: invalid event UUID", eventstore.ErrInvalidEvent)
	}
	return nil
}

func canonicalID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}

func validateTodoFacts(event eventstore.DomainEvent, id, user, todo, instant string, title *string) error {
	if id != event.EventID || !canonicalID(id) || !canonicalID(user) || !canonicalID(todo) {
		return fmt.Errorf("%w: invalid todo event identity or ancestry", eventstore.ErrInvalidEvent)
	}
	at, err := time.Parse(time.RFC3339Nano, instant)
	if err != nil || at.IsZero() || at.UTC().Format(time.RFC3339Nano) != instant {
		return fmt.Errorf("%w: todo timestamp must be a canonical UTC instant", eventstore.ErrInvalidEvent)
	}
	if title != nil && (strings.TrimSpace(*title) != *title || len(*title) == 0 || len(*title) > 160) {
		return fmt.Errorf("%w: invalid todo title", eventstore.ErrInvalidEvent)
	}
	return nil
}

func DecodeTodoCreatedEvent(event eventstore.DomainEvent) (TodoCreatedEvent, error) {
	var value TodoCreatedEvent
	if err := decodePayload(event, TodoCreated, &value); err != nil {
		return value, err
	}
	err := validateTodoFacts(event, value.TodoCreatedID, value.UserID, value.TodoCreatedID, value.CreatedAt, &value.Title)
	return value, err
}

func DecodeTodoRenamedEvent(event eventstore.DomainEvent) (TodoRenamedEvent, error) {
	var value TodoRenamedEvent
	if err := decodePayload(event, TodoRenamed, &value); err != nil {
		return value, err
	}
	err := validateTodoFacts(event, value.TodoRenamedID, value.UserID, value.Scopes.TodoCreatedID, value.RenamedAt, &value.Title)
	return value, err
}

func DecodeTodoCompletedEvent(event eventstore.DomainEvent) (TodoCompletedEvent, error) {
	var value TodoCompletedEvent
	if err := decodePayload(event, TodoCompleted, &value); err != nil {
		return value, err
	}
	err := validateTodoFacts(event, value.TodoCompletedID, value.UserID, value.Scopes.TodoCreatedID, value.CompletedAt, nil)
	return value, err
}

func DecodeTodoReopenedEvent(event eventstore.DomainEvent) (TodoReopenedEvent, error) {
	var value TodoReopenedEvent
	if err := decodePayload(event, TodoReopened, &value); err != nil {
		return value, err
	}
	err := validateTodoFacts(event, value.TodoReopenedID, value.UserID, value.Scopes.TodoCreatedID, value.ReopenedAt, nil)
	return value, err
}

func DecodeTodoDeletedEvent(event eventstore.DomainEvent) (TodoDeletedEvent, error) {
	var value TodoDeletedEvent
	if err := decodePayload(event, TodoDeleted, &value); err != nil {
		return value, err
	}
	err := validateTodoFacts(event, value.TodoDeletedID, value.UserID, value.Scopes.TodoCreatedID, value.DeletedAt, nil)
	return value, err
}
