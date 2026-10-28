package profile

import (
	"context"
	"log/slog"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

const ProfileReadModelEventHandlerName = "profile_read_model_event_handler"

type ReadModelEventHandler struct {
	global     *eventstore.GlobalEventHandler
	repository ProjectionRepository
	publisher  eventstore.Publisher
	keys       SubjectDataKeyReader
}

func NewReadModelEventHandler(subscriber eventstore.Subscriber, checkpointer eventstore.Checkpointer, repository ProjectionRepository, publisher eventstore.Publisher, keys SubjectDataKeyReader, logger *slog.Logger) (*ReadModelEventHandler, error) {
	handler := &ReadModelEventHandler{repository: repository, publisher: publisher, keys: keys}
	global, err := eventstore.NewGlobalEventHandler(eventstore.GlobalEventHandlerConfig{
		Subscriber:      subscriber,
		Checkpointer:    checkpointer,
		Name:            ProfileReadModelEventHandlerName,
		Query:           readModelEventHandlerQuery(),
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

func (h *ReadModelEventHandler) StartSubscribing(ctx context.Context) error {
	return h.global.StartSubscribing(ctx)
}

func (h *ReadModelEventHandler) StopSubscribing() {
	h.global.StopSubscribing()
}

func (h *ReadModelEventHandler) handle(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	users, err := ProjectBatch(ctx, []eventstore.ResolvedEvent{resolved}, h.repository, h.keys)
	if err != nil {
		return err
	}
	for _, userID := range users {
		if err := h.publisher.Publish(ctx, Channel(userID), map[string]string{"userId": userID}); err != nil {
			return err
		}
	}
	return nil
}

func readModelEventHandlerQuery() eventstore.Query {
	eventTypes := []string{
		userRegistered,
		userNameChanged,
		ProfileBioUpdated,
		ProfileImageUploaded,
		ProfileHeaderImageUploaded,
	}
	criteria := make([]eventstore.Criterion, 0, len(eventTypes))
	for _, eventType := range eventTypes {
		criteria = append(criteria, eventstore.Criterion{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: eventType}}})
	}
	return eventstore.Query{Criteria: criteria}
}
