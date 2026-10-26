package auth

import (
	"context"
	"log/slog"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

const AccountDeletionEventHandlerName = "account_deletion_event_handler"

type AccountDeletionEventHandler struct {
	global *eventstore.GlobalEventHandler
	data   AccountDataDeletionPort
	keys   AccountPiiKeyPort
	saver  eventstore.SaverV2
	events eventstore.Retriever
}

func NewAccountDeletionEventHandler(subscriber eventstore.Subscriber, checkpointer eventstore.Checkpointer, saver eventstore.SaverV2, retriever eventstore.Retriever, data AccountDataDeletionPort, keys AccountPiiKeyPort, logger *slog.Logger) (*AccountDeletionEventHandler, error) {
	handler := &AccountDeletionEventHandler{data: data, keys: keys, saver: saver, events: retriever}
	global, err := eventstore.NewGlobalEventHandler(eventstore.GlobalEventHandlerConfig{
		Subscriber:      subscriber,
		Checkpointer:    checkpointer,
		Name:            AccountDeletionEventHandlerName,
		Query:           accountDeletionEventHandlerQuery(),
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

func (h *AccountDeletionEventHandler) StartSubscribing(ctx context.Context) error {
	return h.global.StartSubscribing(ctx)
}

func (h *AccountDeletionEventHandler) StopSubscribing() {
	h.global.StopSubscribing()
}

func (h *AccountDeletionEventHandler) handle(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	if resolved.Event.EventType != AccountDeletionRequested {
		return nil
	}
	if err := ValidateEvent(resolved.Event); err != nil {
		return err
	}
	requestID, _ := resolved.Event.Data[AccountDeletionRequestedIDField].(string)
	authUserID, _ := resolved.Event.Data[AccountDeletionAuthUserIDField].(string)
	userRegisteredID, _ := eventstore.Scopes(resolved.Event.Data)[ScopeUserRegisteredIDKey].(string)
	return CompleteAccountDeletionCommandHandler(ctx, CompleteAccountDeletionCommand{
		AccountDeletionRequestedID: requestID,
		UserRegisteredID:           userRegisteredID,
		AuthUserID:                 authUserID,
		Metadata:                   eventstore.EventHandlerCommandMetadata(AccountDeletionEventHandlerName, resolved),
	}, h.data, h.keys, h.saver, h.events)
}

func accountDeletionEventHandlerQuery() eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: AccountDeletionRequested},
	}}}}
}
