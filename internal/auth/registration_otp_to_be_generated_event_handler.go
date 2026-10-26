package auth

import (
	"context"
	"log/slog"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

const RegistrationOTPToBeGeneratedEventHandlerName = "todo_registration_otp_to_be_generated_event_handler"

type RegistrationOTPToBeGeneratedEventHandler struct {
	global    *eventstore.GlobalEventHandler
	saver     eventstore.SaverV2
	retriever eventstore.Retriever
}

func NewRegistrationOTPToBeGeneratedEventHandler(subscriber eventstore.Subscriber, checkpointer eventstore.Checkpointer, saver eventstore.SaverV2, retriever eventstore.Retriever, logger *slog.Logger) (*RegistrationOTPToBeGeneratedEventHandler, error) {
	handler := &RegistrationOTPToBeGeneratedEventHandler{saver: saver, retriever: retriever}
	global, err := eventstore.NewGlobalEventHandler(eventstore.GlobalEventHandlerConfig{
		Subscriber:      subscriber,
		Checkpointer:    checkpointer,
		Name:            RegistrationOTPToBeGeneratedEventHandlerName,
		Query:           userRegisteredEventHandlerQuery(),
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

func (h *RegistrationOTPToBeGeneratedEventHandler) StartSubscribing(ctx context.Context) error {
	return h.global.StartSubscribing(ctx)
}

func (h *RegistrationOTPToBeGeneratedEventHandler) StopSubscribing() {
	h.global.StopSubscribing()
}

func (h *RegistrationOTPToBeGeneratedEventHandler) handle(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	if resolved.Event.EventType != UserRegistered {
		return nil
	}
	if err := ValidateEvent(resolved.Event); err != nil {
		return err
	}
	value, err := DecodeUserRegisteredEvent(resolved.Event)
	if err != nil {
		return err
	}
	user := views.User{UserRegisteredID: value.UserRegisteredID}

	_, err = GenerateEmailVerificationOTPCommandHandler(ctx, GenerateEmailVerificationOTPCommand{
		User:     user,
		Metadata: eventstore.EventHandlerCommandMetadata(RegistrationOTPToBeGeneratedEventHandlerName, resolved),
	}, h.saver, h.retriever)
	return err
}

func userRegisteredEventHandlerQuery() eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}}}}}
}
