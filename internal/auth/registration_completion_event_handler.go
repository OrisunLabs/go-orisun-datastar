package auth

import (
	"context"
	"errors"
	"log/slog"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

const RegistrationCompletionEventHandlerName = "registration_completion_event_handler"

type RegistrationCompletionEventHandler struct {
	global    *eventstore.GlobalEventHandler
	saver     eventstore.SaverV2
	retriever eventstore.LatestRetriever
	keys      SubjectPiiKeyPort
}

func NewRegistrationCompletionEventHandler(subscriber eventstore.Subscriber, checkpointer eventstore.Checkpointer, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, keys SubjectPiiKeyPort, logger *slog.Logger) (*RegistrationCompletionEventHandler, error) {
	handler := &RegistrationCompletionEventHandler{saver: saver, retriever: retriever, keys: keys}
	global, err := eventstore.NewGlobalEventHandler(eventstore.GlobalEventHandlerConfig{Subscriber: subscriber, Checkpointer: checkpointer, Name: RegistrationCompletionEventHandlerName, Query: eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPValidated}}}}}, Logger: logger, MaxEventRetries: -1, HandleEvent: handler.handle})
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
	value, err := DecodeEmailVerificationOTPValidatedEvent(resolved.Event)
	if err != nil {
		return err
	}
	err = FinalizeUserRegistration(ctx, value.Scopes.UserRegistrationStartedID, eventstore.EventHandlerCommandMetadata(RegistrationCompletionEventHandlerName, resolved), h.saver, h.retriever, h.keys)
	if errors.Is(err, ErrRegistrationIdentityUnavailable) {
		return nil
	}
	return err
}
