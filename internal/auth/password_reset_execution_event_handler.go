package auth

import (
	"context"
	"log/slog"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

const PasswordResetExecutionEventHandlerName = "password_reset_execution_event_handler"

type PasswordResetExecutionEventHandler struct {
	global    *eventstore.GlobalEventHandler
	saver     eventstore.SaverV2
	retriever eventstore.LatestRetriever
}

func NewPasswordResetExecutionEventHandler(subscriber eventstore.Subscriber, checkpointer eventstore.Checkpointer, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, logger *slog.Logger) (*PasswordResetExecutionEventHandler, error) {
	handler := &PasswordResetExecutionEventHandler{saver: saver, retriever: retriever}
	global, err := eventstore.NewGlobalEventHandler(eventstore.GlobalEventHandlerConfig{Subscriber: subscriber, Checkpointer: checkpointer, Name: PasswordResetExecutionEventHandlerName, Query: eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetAuthorized}}}}}, Logger: logger, MaxEventRetries: -1, HandleEvent: handler.handle})
	if err != nil {
		return nil, err
	}
	handler.global = global
	return handler, nil
}
func (h *PasswordResetExecutionEventHandler) StartSubscribing(ctx context.Context) error {
	return h.global.StartSubscribing(ctx)
}
func (h *PasswordResetExecutionEventHandler) StopSubscribing() { h.global.StopSubscribing() }
func (h *PasswordResetExecutionEventHandler) handle(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	value, err := DecodePasswordResetAuthorizedEvent(resolved.Event)
	if err != nil {
		return err
	}
	err = SetPasswordCommandHandler(ctx, SetPasswordCommand{PasswordChangedID: value.OperationID, UserRegisteredID: value.Scopes.UserRegisteredID, PasswordHash: value.PasswordHash, Metadata: eventstore.EventHandlerCommandMetadata(PasswordResetExecutionEventHandlerName, resolved)}, h.saver, h.retriever)
	return err
}
