package profile

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

const ProfileReadModelEventHandlerName = "profile_read_model_event_handler"

type ReadModelEventHandler struct {
	global    *eventstore.GlobalEventHandler
	readModel *ReadModel
	publisher eventstore.Publisher
	keys      SubjectDataKeyReader
}

func NewReadModelEventHandler(subscriber eventstore.Subscriber, checkpointer eventstore.Checkpointer, readModel *ReadModel, publisher eventstore.Publisher, keys SubjectDataKeyReader, logger *slog.Logger) (*ReadModelEventHandler, error) {
	handler := &ReadModelEventHandler{readModel: readModel, publisher: publisher, keys: keys}
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
	var userID string
	switch resolved.Event.EventType {
	case userRegistered:
		if _, err := auth.DecodeUserRegisteredEvent(resolved.Event); err != nil {
			return err
		}
		userID, _ = resolved.Event.Data[auth.UserRegisteredIDField].(string)
		if err := h.readModel.UpsertRegisteredUser(ctx, resolved, h.keys); err != nil {
			return err
		}
	case userNameChanged:
		if _, err := auth.DecodeUserNameChangedEvent(resolved.Event); err != nil {
			return err
		}
		userID, _ = eventstore.Scopes(resolved.Event.Data)[auth.ScopeUserRegisteredIDKey].(string)
		if err := h.readModel.UpdateName(ctx, resolved, h.keys); err != nil {
			return err
		}
	case ProfileBioUpdated:
		var err error
		userID, err = profileEventUserID(resolved.Event)
		if err != nil {
			return err
		}
		if err := h.readModel.UpdateBio(ctx, resolved, h.keys); err != nil {
			return err
		}
	case ProfileImageUploaded:
		var err error
		userID, err = profileEventUserID(resolved.Event)
		if err != nil {
			return err
		}
		if err := h.readModel.UpdateImage(ctx, resolved); err != nil {
			return err
		}
	case ProfileHeaderImageUploaded:
		var err error
		userID, err = profileEventUserID(resolved.Event)
		if err != nil {
			return err
		}
		if err := h.readModel.UpdateHeaderImage(ctx, resolved); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unhandled profile read model event type %q", resolved.Event.EventType)
	}
	return h.publisher.Publish(ctx, Channel(userID), map[string]string{"userId": userID})
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
