package auth

import (
	"context"
	"log/slog"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

const RegistrationCompletionEventHandlerName = "registration_completion_event_handler"

type RegistrationCompletionEventHandler struct {
	global    *eventstore.GlobalEventHandler
	saver     eventstore.SaverV2
	retriever eventstore.LatestRetriever
}

func NewRegistrationCompletionEventHandler(subscriber eventstore.Subscriber, checkpointer eventstore.Checkpointer, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, logger *slog.Logger) (*RegistrationCompletionEventHandler, error) {
	handler := &RegistrationCompletionEventHandler{saver: saver, retriever: retriever}
	global, err := eventstore.NewGlobalEventHandler(eventstore.GlobalEventHandlerConfig{Subscriber: subscriber, Checkpointer: checkpointer, Name: RegistrationCompletionEventHandlerName, Query: eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}}}}}, Logger: logger, MaxEventRetries: -1, HandleEvent: handler.handle})
	if err != nil {
		return nil, err
	}
	handler.global = global
	return handler, nil
}
func (h *RegistrationCompletionEventHandler) StartSubscribing(ctx context.Context) error {
	return h.global.StartSubscribing(ctx)
}
func (h *RegistrationCompletionEventHandler) StopSubscribing() { h.global.StopSubscribing() }
func (h *RegistrationCompletionEventHandler) handle(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	value, err := DecodeUserRegisteredEvent(resolved.Event)
	if err != nil {
		return err
	}
	return CompleteUserRegistrationCommandHandler(ctx, CompleteUserRegistrationCommand{UserRegisteredID: value.UserRegisteredID, Metadata: eventstore.EventHandlerCommandMetadata(RegistrationCompletionEventHandlerName, resolved)}, h.saver, h.retriever)
}
