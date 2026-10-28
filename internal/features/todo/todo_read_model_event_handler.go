package todo

import (
	"context"
	"log/slog"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

type TodoReadModelEventHandler struct {
	global     *eventstore.GlobalEventHandler
	repository ProjectionRepository
	publisher  eventstore.Publisher
}

func NewTodoReadModelEventHandler(subscriber eventstore.Subscriber, checkpointer eventstore.Checkpointer, repository ProjectionRepository, publisher eventstore.Publisher, logger *slog.Logger) (*TodoReadModelEventHandler, error) {
	handler := &TodoReadModelEventHandler{repository: repository, publisher: publisher}
	global, err := eventstore.NewGlobalEventHandler(eventstore.GlobalEventHandlerConfig{Subscriber: subscriber, Checkpointer: checkpointer, Name: TodoReadModelEventHandlerName, Query: TodoReadModelEventHandlerQuery(), Logger: logger, MaxEventRetries: -1, HandleEvent: handler.handle})
	if err != nil {
		return nil, err
	}
	handler.global = global
	return handler, nil
}
func (h *TodoReadModelEventHandler) StartSubscribing(ctx context.Context) error {
	return h.global.StartSubscribing(ctx)
}
func (h *TodoReadModelEventHandler) StopSubscribing() { h.global.StopSubscribing() }
func TodoReadModelEventHandlerQuery() eventstore.Query {
	criteria := []eventstore.Criterion{}
	for _, kind := range []string{TodoCreated, TodoRenamed, TodoCompleted, TodoReopened, TodoDeleted} {
		criteria = append(criteria, eventstore.Criterion{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: kind}}})
	}
	return eventstore.Query{Criteria: criteria}
}
func (h *TodoReadModelEventHandler) handle(ctx context.Context, event eventstore.ResolvedEvent) error {
	return h.ProjectBatch(ctx, []eventstore.ResolvedEvent{event})
}
func (h *TodoReadModelEventHandler) ProjectBatch(ctx context.Context, events []eventstore.ResolvedEvent) error {
	users, err := ProjectBatch(ctx, events, h.repository)
	if err != nil {
		return err
	}
	for _, id := range users {
		if err := h.publisher.Publish(ctx, Channel(id), map[string]string{"userId": id}); err != nil {
			return err
		}
	}
	return nil
}
