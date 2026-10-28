package auth

import (
	"context"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

// ReadAccountDeletionStatus is read-side workflow evidence; it never executes cleanup.
func ReadAccountDeletionStatus(ctx context.Context, id string, reader eventstore.LatestRetriever) (string, error) {
	if !validAuthID(id) {
		return "", eventstore.ErrInvalidEvent
	}
	first, err := reader.GetLatestByCriteria(ctx, []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeletionRequested}, {Key: "eventId", Value: id}}}})
	if err != nil {
		return "", err
	}
	var request *AccountDeletionRequestedEvent
	var requestPosition eventstore.Position
	for _, e := range eventstore.EventsFromLatest(first.Results) {
		v, err := DecodeAccountDeletionRequestedEvent(e.Event)
		if err != nil {
			return "", err
		}
		if v.AccountDeletionRequestedID != id {
			return "", eventstore.ErrInvalidEvent
		}
		request = &v
		requestPosition = e.Position
	}
	if request == nil {
		return "", eventstore.ErrNotFound
	}
	r := request
	latest, err := reader.GetLatestByCriteria(ctx, []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeleted}, {Key: "eventId", Value: r.OperationID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeletionCompleted}, {Key: "scopes.accountDeletedId", Value: r.OperationID}}},
	})
	if err != nil {
		return "", err
	}
	var effect *AccountDeletedEvent
	var receipt *AccountDeletionCompletedEvent
	var effectPosition, receiptPosition eventstore.Position
	for _, e := range eventstore.EventsFromLatest(latest.Results) {
		switch e.Event.EventType {
		case AccountDeleted:
			v, err := DecodeAccountDeletedEvent(e.Event)
			if err != nil {
				return "", err
			}
			if v.AccountDeletedID != r.OperationID || v.AuthUserID != r.AuthUserID || v.Scopes.UserRegisteredID != r.Scopes.UserRegisteredID || !e.Position.After(requestPosition) {
				return "", eventstore.ErrInvalidEvent
			}
			effect = &v
			effectPosition = e.Position
		case AccountDeletionCompleted:
			v, err := DecodeAccountDeletionCompletedEvent(e.Event)
			if err != nil {
				return "", err
			}
			if v.Scopes.AccountDeletionRequestedID != id || v.Scopes.AccountDeletedID != r.OperationID || v.AuthUserID != r.AuthUserID || v.Scopes.UserRegisteredID != r.Scopes.UserRegisteredID {
				return "", eventstore.ErrInvalidEvent
			}
			receipt = &v
			receiptPosition = e.Position
		default:
			return "", eventstore.ErrInvalidEvent
		}
	}
	if receipt != nil {
		if effect == nil || !receiptPosition.After(effectPosition) {
			return "", eventstore.ErrInvalidEvent
		}
		return "complete", nil
	}
	return "processing", nil
}
