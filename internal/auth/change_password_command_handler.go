package auth

import (
	"context"
	"errors"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
	"golang.org/x/crypto/bcrypt"
)

type ChangePasswordCommand struct {
	User            views.User
	CurrentPassword string
	NewPassword     string
	Metadata        CommandMetadata
}

func ChangePasswordCommandHandler(ctx context.Context, command ChangePasswordCommand, credentials PasswordCredentialReader, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	if len(command.NewPassword) < 6 {
		return errors.New("password must be at least 6 characters")
	}
	credentialUser, _, err := credentials.UserByEmailWithPassword(ctx, command.User.Email)
	if err != nil {
		return err
	}
	if credentialUser.UserRegisteredID != command.User.UserRegisteredID {
		return errors.New("password credential does not belong to registered user")
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
	event, err := NewPasswordChangedEvent(eventID, time.Now(), command.User.UserRegisteredID, string(newHash), nil)
	if err != nil {
		return err
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}})
	return err
}

type changePasswordContext struct {
	userRegisteredID string
	passwordHash     string
	passwordPosition *eventstore.Position
	userExists       bool
	position         eventstore.Position
	events           []eventstore.ResolvedEvent
	query            eventstore.Query
}

func loadChangePasswordContext(ctx context.Context, command ChangePasswordCommand, retriever eventstore.LatestRetriever) (*changePasswordContext, error) {
	userQuery := changePasswordUserRegisteredQuery(command.User.UserRegisteredID)
	passwordQuery := changePasswordPasswordChangedByUserQuery(command.User.UserRegisteredID)
	query := changePasswordCombineQueries(userQuery, passwordQuery, changePasswordPasswordResetCompletedByUserQuery(command.User.UserRegisteredID))
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	events := eventstore.EventsFromLatest(latest.Results)
	model := &changePasswordContext{userRegisteredID: command.User.UserRegisteredID, position: latest.ContextPosition, events: events, query: query}
	for _, resolved := range events {
		if err := model.handle(resolved); err != nil {
			return nil, err
		}
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
		if m.passwordPosition == nil {
			m.passwordHash = value.PasswordHash
		}
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
	case PasswordResetCompleted:
		value, err := DecodePasswordResetCompletedEvent(resolved.Event)
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

func changePasswordPasswordResetCompletedByUserQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: PasswordResetCompleted},
		{Key: ScopeUserRegisteredIDField, Value: userRegisteredID},
	}}}}
}
