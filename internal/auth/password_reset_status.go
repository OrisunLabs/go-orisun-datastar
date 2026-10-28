package auth

import (
	"context"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

// PasswordResetStatus contains read-side progress, never command authority or protected input.
type PasswordResetStatus struct {
	UserID         string
	Stage          string
	EffectPosition eventstore.Position
}

func ReadPasswordResetStatus(ctx context.Context, id string, reader eventstore.LatestRetriever) (PasswordResetStatus, error) {
	empty := PasswordResetStatus{}
	if !validAuthID(id) {
		return empty, eventstore.ErrInvalidEvent
	}
	first, err := reader.GetLatestByCriteria(ctx, []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetAuthorized}, {Key: "eventId", Value: id}}}})
	if err != nil {
		return empty, err
	}
	var authorization *PasswordResetAuthorizedEvent
	var authorizedPosition eventstore.Position
	for _, e := range eventstore.EventsFromLatest(first.Results) {
		v, err := DecodePasswordResetAuthorizedEvent(e.Event)
		if err != nil {
			return empty, err
		}
		if v.PasswordResetAuthorizedID != id {
			return empty, eventstore.ErrInvalidEvent
		}
		authorization = &v
		authorizedPosition = e.Position
	}
	if authorization == nil {
		return empty, eventstore.ErrNotFound
	}
	a := authorization
	latest, err := reader.GetLatestByCriteria(ctx, []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordChanged}, {Key: "eventId", Value: a.OperationID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetCompleted}, {Key: "scopes.passwordResetAuthorizedId", Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeleted}, {Key: ScopeUserRegisteredIDField, Value: a.Scopes.UserRegisteredID}}},
	})
	if err != nil {
		return empty, err
	}
	var effect *PasswordChangedEvent
	var receipt *PasswordResetCompletedEvent
	var effectPosition, receiptPosition eventstore.Position
	deleted := false
	for _, e := range eventstore.EventsFromLatest(latest.Results) {
		switch e.Event.EventType {
		case PasswordChanged:
			v, err := DecodePasswordChangedEvent(e.Event)
			if err != nil {
				return empty, err
			}
			if v.PasswordChangedID != a.OperationID || v.Scopes.UserRegisteredID != a.Scopes.UserRegisteredID || v.PasswordHash != a.PasswordHash || !e.Position.After(authorizedPosition) {
				return empty, eventstore.ErrInvalidEvent
			}
			effect = &v
			effectPosition = e.Position
		case PasswordResetCompleted:
			v, err := DecodePasswordResetCompletedEvent(e.Event)
			if err != nil {
				return empty, err
			}
			if v.Scopes.PasswordResetAuthorizedID != id || v.Scopes.PasswordResetRequestedID != a.Scopes.PasswordResetRequestedID || v.Scopes.UserRegisteredID != a.Scopes.UserRegisteredID || v.Scopes.PasswordChangedID != a.OperationID {
				return empty, eventstore.ErrInvalidEvent
			}
			receipt = &v
			receiptPosition = e.Position
		case AccountDeleted:
			v, err := DecodeAccountDeletedEvent(e.Event)
			if err != nil {
				return empty, err
			}
			if v.Scopes.UserRegisteredID != a.Scopes.UserRegisteredID || !e.Position.After(authorizedPosition) {
				return empty, eventstore.ErrInvalidEvent
			}
			deleted = true
		default:
			return empty, eventstore.ErrInvalidEvent
		}
	}
	if receipt != nil && (effect == nil || !receiptPosition.After(effectPosition)) {
		return empty, eventstore.ErrInvalidEvent
	}
	stage := "processing"
	if receipt != nil {
		stage = "complete"
	}
	if deleted {
		stage = "unavailable"
	}
	return PasswordResetStatus{UserID: a.Scopes.UserRegisteredID, Stage: stage, EffectPosition: effectPosition}, nil
}
