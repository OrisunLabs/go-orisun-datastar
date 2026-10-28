package auth

import (
	"context"
	"log/slog"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

const AccountDeletionCompletionEventHandlerName = "account_deletion_completion_event_handler"

type AccountDeletionCompletionEventHandler struct {
	global    *eventstore.GlobalEventHandler
	saver     eventstore.SaverV2
	retriever eventstore.LatestRetriever
}

func NewAccountDeletionCompletionEventHandler(
	subscriber eventstore.Subscriber,
	checkpointer eventstore.Checkpointer,
	saver eventstore.SaverV2,
	retriever eventstore.LatestRetriever,
	logger *slog.Logger,
) (*AccountDeletionCompletionEventHandler, error) {
	handler := &AccountDeletionCompletionEventHandler{saver: saver, retriever: retriever}
	global, err := eventstore.NewGlobalEventHandler(eventstore.GlobalEventHandlerConfig{Subscriber: subscriber, Checkpointer: checkpointer, Name: AccountDeletionCompletionEventHandlerName, Query: eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeleted}}}}}, Logger: logger, MaxEventRetries: -1, HandleEvent: handler.handle})
	if err != nil {
		return nil, err
	}
	handler.global = global
	return handler, nil
}
func (h *AccountDeletionCompletionEventHandler) StartSubscribing(ctx context.Context) error {
	return h.global.StartSubscribing(ctx)
}
func (h *AccountDeletionCompletionEventHandler) StopSubscribing() { h.global.StopSubscribing() }
func (h *AccountDeletionCompletionEventHandler) handle(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	value, err := DecodeAccountDeletedEvent(resolved.Event)
	if err != nil {
		return err
	}
	err = CompleteAccountDeletionCommandHandler(ctx, CompleteAccountDeletionCommand{AccountDeletedID: value.AccountDeletedID, Metadata: eventstore.EventHandlerCommandMetadata(AccountDeletionCompletionEventHandlerName, resolved)}, h.saver, h.retriever)
	return err
}
