package auth

import (
	"context"
	"errors"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"golang.org/x/crypto/bcrypt"
)

type ChangePasswordCommand struct {
	UserRegisteredID string
	CurrentPassword  string
	NewPassword      string
	Metadata         CommandMetadata
}

func ChangePasswordCommandHandler(ctx context.Context, command ChangePasswordCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	if len(command.NewPassword) < 6 {
		return errors.New("password must be at least 6 characters")
	}
	model, err := loadChangePasswordContext(ctx, command, retriever)
	if err != nil {
		return err
	}
	if !model.userExists {
		return errors.New("registered user event not found")
	}

	if err := bcrypt.CompareHashAndPassword([]byte(model.passwordHash), []byte(command.CurrentPassword)); err != nil {
		return errors.New("current password is incorrect")
	}
	newHash, err := bcrypt.GenerateFromPassword([]byte(command.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	eventID := uuidv7.NewString()
	event, err := NewPasswordChangedEvent(eventID, time.Now(), command.UserRegisteredID, string(newHash), nil)
	if err != nil {
		return err
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}})
	return err
}

type changePasswordContext struct {
	userRegisteredID   string
	passwordHash       string
	passwordPosition   *eventstore.Position
	userExists         bool
	deleted            bool
	registeredPosition eventstore.Position
	position           eventstore.Position
	query              eventstore.Query
}

func loadChangePasswordContext(ctx context.Context, command ChangePasswordCommand, retriever eventstore.LatestRetriever) (*changePasswordContext, error) {
	if !validAuthID(command.UserRegisteredID) {
		return nil, eventstore.ErrInvalidEvent
	}
	userQuery := changePasswordUserRegisteredQuery(command.UserRegisteredID)
	passwordQuery := changePasswordPasswordChangedByUserQuery(command.UserRegisteredID)
	query := changePasswordCombineQueries(userQuery, passwordQuery, changePasswordAccountDeletedQuery(command.UserRegisteredID))
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	events := eventstore.EventsFromLatest(latest.Results)
	model := &changePasswordContext{userRegisteredID: command.UserRegisteredID, position: latest.ContextPosition, query: query}
	for _, resolved := range events {
		if err := model.handle(resolved); err != nil {
			return nil, err
		}
	}
	if model.deleted {
		return nil, eventstore.ErrNotFound
	}
	if model.userExists && model.passwordPosition != nil && !model.passwordPosition.After(model.registeredPosition) {
		return nil, eventstore.ErrInvalidEvent
	}
	return model, nil
}

func (m *changePasswordContext) handle(resolved eventstore.ResolvedEvent) error {
	var hash string
	switch resolved.Event.EventType {
	case UserRegistered:
		value, err := DecodeUserRegisteredEvent(resolved.Event)
		if err != nil {
			return err
		}
		if value.UserRegisteredID != m.userRegisteredID {
			return eventstore.ErrInvalidEvent
		}
		m.userExists = true
		m.registeredPosition = resolved.Position
		if m.passwordPosition == nil {
			m.passwordHash = value.PasswordHash
		}
		return nil
	case AccountDeleted:
		value, err := DecodeAccountDeletedEvent(resolved.Event)
		if err != nil {
			return err
		}
		if value.Scopes.UserRegisteredID != m.userRegisteredID {
			return eventstore.ErrInvalidEvent
		}
		m.deleted = true
		return nil
	case PasswordChanged:
		value, err := DecodePasswordChangedEvent(resolved.Event)
		if err != nil {
			return err
		}
		if value.Scopes.UserRegisteredID != m.userRegisteredID {
			return eventstore.ErrInvalidEvent
		}
		hash = value.PasswordHash

	default:
		return eventstore.ErrInvalidEvent
	}
	if m.passwordPosition == nil || resolved.Position.After(*m.passwordPosition) {
		position := resolved.Position
		m.passwordPosition = &position
		m.passwordHash = hash
	}
	return nil
}

func changePasswordCombineQueries(queries ...eventstore.Query) eventstore.Query {
	combined := eventstore.Query{}
	for _, query := range queries {
		combined.Criteria = append(combined.Criteria, query.Criteria...)
	}
	return combined
}

func changePasswordPasswordChangedByUserQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: PasswordChanged},
		{Key: ScopeUserRegisteredIDField, Value: userRegisteredID},
	}}}}
}

func changePasswordUserRegisteredQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: UserRegistered},
		{Key: UserRegisteredIDField, Value: userRegisteredID},
	}}}}
}

func changePasswordAccountDeletedQuery(userID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeleted}, {Key: ScopeUserRegisteredIDField, Value: userID}}}}}
}
