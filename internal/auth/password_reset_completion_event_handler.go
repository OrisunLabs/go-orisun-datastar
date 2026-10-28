package auth

import (
	"context"
	"log/slog"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

const PasswordResetCompletionEventHandlerName = "password_reset_completion_event_handler"

type PasswordResetCompletionEventHandler struct {
	global    *eventstore.GlobalEventHandler
	saver     eventstore.SaverV2
	retriever eventstore.LatestRetriever
}

func NewPasswordResetCompletionEventHandler(
	subscriber eventstore.Subscriber,
	checkpointer eventstore.Checkpointer,
	saver eventstore.SaverV2,
	retriever eventstore.LatestRetriever,
	logger *slog.Logger,
) (*PasswordResetCompletionEventHandler, error) {
	handler := &PasswordResetCompletionEventHandler{saver: saver, retriever: retriever}
	global, err := eventstore.NewGlobalEventHandler(eventstore.GlobalEventHandlerConfig{Subscriber: subscriber, Checkpointer: checkpointer, Name: PasswordResetCompletionEventHandlerName, Query: eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordChanged}}}}}, Logger: logger, MaxEventRetries: -1, HandleEvent: handler.handle})
	if err != nil {
		return nil, err
	}
	handler.global = global
	return handler, nil
}
func (h *PasswordResetCompletionEventHandler) StartSubscribing(ctx context.Context) error {
	return h.global.StartSubscribing(ctx)
}
func (h *PasswordResetCompletionEventHandler) StopSubscribing() { h.global.StopSubscribing() }
func (h *PasswordResetCompletionEventHandler) handle(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	value, err := DecodePasswordChangedEvent(resolved.Event)
	if err != nil {
		return err
	}
	err = CompletePasswordResetCommandHandler(ctx, CompletePasswordResetCommand{PasswordChangedID: value.PasswordChangedID, Metadata: eventstore.EventHandlerCommandMetadata(PasswordResetCompletionEventHandlerName, resolved)}, h.saver, h.retriever)
	return err
}
