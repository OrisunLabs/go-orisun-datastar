package auth

import (
	"context"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"golang.org/x/crypto/bcrypt"
)

// SetPasswordCommand is trusted backend input, independent of any reset workflow.
type SetPasswordCommand struct {
	PasswordChangedID, UserRegisteredID, PasswordHash string
	Metadata                                          CommandMetadata
}

func SetPasswordCommandHandler(ctx context.Context, command SetPasswordCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	if !validAuthID(command.PasswordChangedID) || !validAuthID(command.UserRegisteredID) {
		return eventstore.ErrInvalidEvent
	}
	if _, err := bcrypt.Cost([]byte(command.PasswordHash)); err != nil {
		return eventstore.ErrInvalidEvent
	}
	m, err := loadSetPasswordContext(ctx, command, retriever)
	if err != nil {
		return err
	}
	if m.applied {
		return nil
	}
	if !m.registered {
		return eventstore.ErrNotFound
	}
	event, err := NewPasswordChangedEvent(command.PasswordChangedID, time.Now(), command.UserRegisteredID, command.PasswordHash, nil)
	if err != nil {
		return err
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: m.query, Position: m.position}})
	return err
}

type setPasswordContext struct {
	registered, applied bool
	query               eventstore.Query
	position            eventstore.Position
}

func loadSetPasswordContext(ctx context.Context, command SetPasswordCommand, retriever eventstore.LatestRetriever) (*setPasswordContext, error) {
	query := setPasswordQuery(command)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	m := &setPasswordContext{query: query, position: latest.ContextPosition}
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
			m.registered = true
		case PasswordChanged:
			v, err := DecodePasswordChangedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.PasswordChangedID == command.PasswordChangedID {
				if v.Scopes.UserRegisteredID != command.UserRegisteredID || v.PasswordHash != command.PasswordHash {
					return nil, eventstore.ErrConflict
				}
				m.applied = true
			} else if v.Scopes.UserRegisteredID != command.UserRegisteredID {
				return nil, eventstore.ErrInvalidEvent
			}
		default:
			return nil, eventstore.ErrInvalidEvent
		}
	}
	return m, nil
}
func setPasswordQuery(command SetPasswordCommand) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}, {Key: UserRegisteredIDField, Value: command.UserRegisteredID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordChanged}, {Key: "eventId", Value: command.PasswordChangedID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordChanged}, {Key: ScopeUserRegisteredIDField, Value: command.UserRegisteredID}}},
	}}
}
