package auth

import (
	"context"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

type CompletePasswordResetCommand struct {
	PasswordChangedID string
	Metadata          CommandMetadata
}

func CompletePasswordResetCommandHandler(ctx context.Context, command CompletePasswordResetCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	m, err := loadCompletePasswordResetContext(ctx, command.PasswordChangedID, retriever)
	if err != nil {
		return err
	}
	if m.completed {
		return nil
	}
	a := m.authorization
	event, err := NewPasswordResetCompletedEvent(uuidv7.NewString(), time.Now(), a.Scopes.PasswordResetRequestedID, a.Scopes.UserRegisteredID, a.PasswordResetAuthorizedID, a.OperationID, nil)
	if err != nil {
		return err
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, m.observations)
	return err
}

type completePasswordResetContext struct {
	authorization PasswordResetAuthorizedEvent
	completed     bool
	observations  []eventstore.ConsistencyObservation
}

func loadCompletePasswordResetContext(ctx context.Context, id string, retriever eventstore.LatestRetriever) (*completePasswordResetContext, error) {
	if !validAuthID(id) {
		return nil, eventstore.ErrInvalidEvent
	}
	query := completePasswordResetEffectQuery(id)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	m := &completePasswordResetContext{observations: []eventstore.ConsistencyObservation{{Query: query, Position: latest.ContextPosition}}}
	var receipt *PasswordResetCompletedEvent
	var changed PasswordChangedEvent
	var authorizedPosition, completedPosition, changedPosition eventstore.Position
	for _, e := range eventstore.EventsFromLatest(latest.Results) {
		switch e.Event.EventType {
		case PasswordResetAuthorized:
			v, err := DecodePasswordResetAuthorizedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.OperationID != id {
				return nil, eventstore.ErrInvalidEvent
			}
			m.authorization = v
			authorizedPosition = e.Position
		case PasswordResetCompleted:
			v, err := DecodePasswordResetCompletedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			receipt = &v
			completedPosition = e.Position
		case PasswordChanged:
			v, err := DecodePasswordChangedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.PasswordChangedID != id {
				return nil, eventstore.ErrInvalidEvent
			}
			changed = v
			changedPosition = e.Position
		default:
			return nil, eventstore.ErrInvalidEvent
		}
	}
	// Ordinary domain changes have no reset workflow to complete.
	if m.authorization.PasswordResetAuthorizedID == "" {
		if receipt != nil {
			return nil, eventstore.ErrInvalidEvent
		}
		if changed.PasswordChangedID == "" {
			return nil, eventstore.ErrNotFound
		}
		m.completed = true
		return m, nil
	}

	a := m.authorization
	if receipt != nil {
		if !completedPosition.After(authorizedPosition) || receipt.Scopes.PasswordResetAuthorizedID != a.PasswordResetAuthorizedID || receipt.Scopes.PasswordChangedID != a.OperationID || receipt.Scopes.PasswordResetRequestedID != a.Scopes.PasswordResetRequestedID || receipt.Scopes.UserRegisteredID != a.Scopes.UserRegisteredID {
			return nil, eventstore.ErrInvalidEvent
		}
		m.completed = true
		return m, nil
	}
	if changed.PasswordChangedID == "" {
		return nil, eventstore.ErrNotFound
	}
	if !changedPosition.After(authorizedPosition) || changed.Scopes.UserRegisteredID != a.Scopes.UserRegisteredID || changed.PasswordHash != a.PasswordHash {
		return nil, eventstore.ErrConflict
	}

	return m, nil
}
func completePasswordResetEffectQuery(id string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetAuthorized}, {Key: "operationId", Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordChanged}, {Key: "eventId", Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetCompleted}, {Key: "scopes.passwordChangedId", Value: id}}},
	}}
}
