package todo

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

const TodoReadModelEventHandlerName = "todo_read_model_event_handler"

type TodoReadModelReader interface {
	List(ctx context.Context, userID string) ([]views.Todo, error)
}

type TodoReadModelWriter interface {
	InsertCreatedTodo(ctx context.Context, event TodoCreatedProjection) error
	RenameTodo(ctx context.Context, event TodoRenamedProjection) error
	CompleteTodo(ctx context.Context, event TodoCompletedProjection) error
	ReopenTodo(ctx context.Context, event TodoReopenedProjection) error
	DeleteTodo(ctx context.Context, event TodoDeletedProjection) error
}

type TodoCreatedProjection struct {
	Position  eventstore.Position
	TodoID    string
	UserID    string
	Title     string
	CreatedAt time.Time
}

type TodoRenamedProjection struct {
	Position  eventstore.Position
	TodoID    string
	Title     string
	RenamedAt time.Time
}

type TodoCompletedProjection struct {
	Position    eventstore.Position
	TodoID      string
	CompletedAt time.Time
}

type TodoReopenedProjection struct {
	Position   eventstore.Position
	TodoID     string
	ReopenedAt time.Time
}

type TodoDeletedProjection struct {
	Position  eventstore.Position
	TodoID    string
	DeletedAt time.Time
}

type TodoReadModelEventHandler struct {
	global    *eventstore.GlobalEventHandler
	readModel TodoReadModelWriter
	publisher eventstore.Publisher
}

func NewTodoReadModelEventHandler(subscriber eventstore.Subscriber, checkpointer eventstore.Checkpointer, readModel TodoReadModelWriter, publisher eventstore.Publisher, logger *slog.Logger) (*TodoReadModelEventHandler, error) {
	handler := &TodoReadModelEventHandler{readModel: readModel, publisher: publisher}
	global, err := eventstore.NewGlobalEventHandler(eventstore.GlobalEventHandlerConfig{
		Subscriber:      subscriber,
		Checkpointer:    checkpointer,
		Name:            TodoReadModelEventHandlerName,
		Query:           TodoReadModelEventHandlerQuery(),
		Logger:          logger,
		MaxEventRetries: -1,
		HandleEvent:     handler.handle,
	})
	if err != nil {
		return nil, err
	}
	handler.global = global
	return handler, nil
}

func (h *TodoReadModelEventHandler) StartSubscribing(ctx context.Context) error {
	return h.global.StartSubscribing(ctx)
}

func (h *TodoReadModelEventHandler) StopSubscribing() {
	h.global.StopSubscribing()
}

func TodoReadModelEventHandlerQuery() eventstore.Query {
	criteria := make([]eventstore.Criterion, 0, 5)
	for _, eventType := range []string{TodoCreated, TodoRenamed, TodoCompleted, TodoReopened, TodoDeleted} {
		criteria = append(criteria, eventstore.Criterion{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: eventType}}})
	}
	return eventstore.Query{Criteria: criteria}
}

func (h *TodoReadModelEventHandler) handle(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	facts, err := decodeTodoEvent(resolved.Event)
	if err != nil {
		return err
	}
	todoID, userID := facts.todoID, facts.userID

	switch resolved.Event.EventType {
	case TodoCreated:
		if err := h.readModel.InsertCreatedTodo(ctx, TodoCreatedProjection{
			Position:  resolved.Position,
			TodoID:    todoID,
			UserID:    userID,
			Title:     facts.title,
			CreatedAt: facts.at,
		}); err != nil {
			return err
		}
	case TodoRenamed:
		if err := h.readModel.RenameTodo(ctx, TodoRenamedProjection{
			Position:  resolved.Position,
			TodoID:    todoID,
			Title:     facts.title,
			RenamedAt: facts.at,
		}); err != nil {
			return err
		}
	case TodoCompleted:
		if err := h.readModel.CompleteTodo(ctx, TodoCompletedProjection{
			Position:    resolved.Position,
			TodoID:      todoID,
			CompletedAt: facts.at,
		}); err != nil {
			return err
		}
	case TodoReopened:
		if err := h.readModel.ReopenTodo(ctx, TodoReopenedProjection{
			Position:   resolved.Position,
			TodoID:     todoID,
			ReopenedAt: facts.at,
		}); err != nil {
			return err
		}
	case TodoDeleted:
		if err := h.readModel.DeleteTodo(ctx, TodoDeletedProjection{
			Position:  resolved.Position,
			TodoID:    todoID,
			DeletedAt: facts.at,
		}); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unhandled todo read model event type %q", resolved.Event.EventType)
	}

	return h.publisher.Publish(ctx, Channel(userID), map[string]string{"userId": userID})
}
