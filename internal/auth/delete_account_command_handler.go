package auth

import (
	"context"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

// DeleteAccountCommand is complete trusted input, independent of deletion requests.
type DeleteAccountCommand struct {
	AccountDeletedID, UserRegisteredID, AuthUserID string
	Metadata                                       CommandMetadata
}

func DeleteAccountCommandHandler(ctx context.Context, command DeleteAccountCommand, data AccountDataDeletionPort, keys AccountPiiKeyPort, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadDeleteAccountContext(ctx, command, retriever)
	if err != nil {
		return err
	}
	if model.applied {
		return nil
	}
	event, err := NewAccountDeletedEvent(command.AccountDeletedID, time.Now(), command.AuthUserID, command.UserRegisteredID, nil)
	if err != nil {
		return err
	}
	if err := data.DeleteAccountData(ctx, command.UserRegisteredID); err != nil {
		return err
	}
	if err := keys.DestroySubjectKey(ctx, command.UserRegisteredID); err != nil {
		return err
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}})
	return err
}

type deleteAccountContext struct {
	applied  bool
	query    eventstore.Query
	position eventstore.Position
}

func loadDeleteAccountContext(ctx context.Context, command DeleteAccountCommand, retriever eventstore.LatestRetriever) (*deleteAccountContext, error) {
	if !validAuthID(command.AccountDeletedID) || !validAuthID(command.UserRegisteredID) || !validAuthID(command.AuthUserID) || command.AccountDeletedID == command.UserRegisteredID || command.AccountDeletedID == command.AuthUserID {
		return nil, eventstore.ErrInvalidEvent
	}
	query := deleteAccountQuery(command)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model := &deleteAccountContext{query: query, position: latest.ContextPosition}
	registered := false
	for _, e := range eventstore.EventsFromLatest(latest.Results) {
		switch e.Event.EventType {
		case UserRegistered:
			v, err := DecodeUserRegisteredEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.UserRegisteredID != command.UserRegisteredID {
				return nil, eventstore.ErrInvalidEvent
			}
			registered = true
		case AccountDeleted:
			v, err := DecodeAccountDeletedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.AccountDeletedID != command.AccountDeletedID || v.Scopes.UserRegisteredID != command.UserRegisteredID || v.AuthUserID != command.AuthUserID {
				return nil, eventstore.ErrConflict
			}
			model.applied = true
		default:
			return nil, eventstore.ErrInvalidEvent
		}
	}
	if !registered {
		return nil, eventstore.ErrNotFound
	}
	return model, nil
}
func deleteAccountQuery(command DeleteAccountCommand) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}, {Key: UserRegisteredIDField, Value: command.UserRegisteredID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeleted}, {Key: "eventId", Value: command.AccountDeletedID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeleted}, {Key: ScopeUserRegisteredIDField, Value: command.UserRegisteredID}}},
	}}
}
