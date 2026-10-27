package auth

import (
	"context"
	"log/slog"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

const RegistrationExecutionEventHandlerName = "registration_execution_event_handler"

type RegistrationExecutionEventHandler struct {
	global    *eventstore.GlobalEventHandler
	saver     eventstore.SaverV2
	retriever eventstore.LatestRetriever
	keys      SubjectPiiKeyReader
}

func NewRegistrationExecutionEventHandler(subscriber eventstore.Subscriber, checkpointer eventstore.Checkpointer, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, keys SubjectPiiKeyReader, logger *slog.Logger) (*RegistrationExecutionEventHandler, error) {
	h := &RegistrationExecutionEventHandler{saver: saver, retriever: retriever, keys: keys}
	g, err := eventstore.NewGlobalEventHandler(eventstore.GlobalEventHandlerConfig{Subscriber: subscriber, Checkpointer: checkpointer, Name: RegistrationExecutionEventHandlerName, Query: eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPValidated}}}}}, Logger: logger, MaxEventRetries: -1, HandleEvent: h.handle})
	if err != nil {
		return nil, err
	}
	h.global = g
	return h, nil
}
func (h *RegistrationExecutionEventHandler) StartSubscribing(ctx context.Context) error {
	return h.global.StartSubscribing(ctx)
}
func (h *RegistrationExecutionEventHandler) StopSubscribing() { h.global.StopSubscribing() }
func (h *RegistrationExecutionEventHandler) handle(ctx context.Context, e eventstore.ResolvedEvent) error {
	v, err := DecodeEmailVerificationOTPValidatedEvent(e.Event)
	if err != nil {
		return err
	}
	// Resolve immutable source input only; workflow state never selects commands.
	query := eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationStarted}, {Key: UserRegistrationStartedIDField, Value: v.Scopes.UserRegistrationStartedID}}}}}
	latest, err := h.retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return err
	}
	facts := eventstore.EventsFromLatest(latest.Results)
	if len(facts) != 1 {
		return eventstore.ErrNotFound
	}
	started, err := DecodeUserRegistrationStartedEvent(facts[0].Event)
	if err != nil {
		return err
	}
	if started.UserRegistrationStartedID != v.Scopes.UserRegistrationStartedID || !e.Position.After(facts[0].Position) {
		return eventstore.ErrInvalidEvent
	}
	at, _ := time.Parse(time.RFC3339Nano, v.ValidatedAt)
	_, err = RegisterUserCommandHandler(ctx, RegisterUserCommand{UserRegisteredID: started.UserID, RegistrationDetails: started.RegistrationDetails, EmailVerifiedAt: at, Metadata: eventstore.EventHandlerCommandMetadata(RegistrationExecutionEventHandlerName, e)}, h.saver, h.retriever, h.keys)
	return err
}
