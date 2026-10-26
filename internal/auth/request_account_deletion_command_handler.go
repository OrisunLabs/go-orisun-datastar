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

type RequestAccountDeletionCommand struct {
	User     views.User
	Password string
	Metadata CommandMetadata
}

func RequestAccountDeletionCommandHandler(ctx context.Context, command RequestAccountDeletionCommand, credentials PasswordCredentialReader, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) (string, error) {
	if err := commandlimits.Assert(command); err != nil {
		return "", err
	}
	if command.User.UserRegisteredID == "" {
		return "", errors.New("user is required")
	}
	credentialUser, _, err := credentials.UserByEmailWithPassword(ctx, command.User.Email)
	if err != nil {
		return "", errors.New("invalid password")
	}
	if credentialUser.UserRegisteredID != command.User.UserRegisteredID || credentialUser.ID != command.User.ID {
		return "", errors.New("password credential does not belong to account")
	}
	model, err := loadAccountDeletionRequestContext(ctx, command.User.UserRegisteredID, retriever)
	if err != nil {
		return "", err
	}
	if !model.userExists {
		return "", errors.New("registered user event not found")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(model.passwordHash), []byte(command.Password)); err != nil {
		return "", errors.New("invalid password")
	}
	if model.deleted || model.requestedID != "" {
		return model.requestedID, nil
	}
	requestID := uuidv7.NewString()
	event, err := NewAccountDeletionRequestedEvent(requestID, time.Now(), command.User.ID, command.User.UserRegisteredID, nil)
	if err != nil {
		return "", err
	}
	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}}); err != nil {
		return "", err
	}
	return requestID, nil
}

type accountDeletionRequestContext struct {
	userExists       bool
	passwordHash     string
	passwordPosition *eventstore.Position
	requestedID      string
	deleted          bool
	position         eventstore.Position
	events           []eventstore.ResolvedEvent
	query            eventstore.Query
}

func loadAccountDeletionRequestContext(ctx context.Context, userRegisteredID string, retriever eventstore.LatestRetriever) (*accountDeletionRequestContext, error) {
	query := requestAccountDeletionCombineQueries(requestAccountDeletionUserRegisteredQuery(userRegisteredID), requestAccountDeletionAccountDeletionRequestedByUserQuery(userRegisteredID), requestAccountDeletionAccountDeletedByUserQuery(userRegisteredID), requestAccountDeletionPasswordChangedByUserQuery(userRegisteredID), requestAccountDeletionPasswordResetCompletedByUserQuery(userRegisteredID))
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model := &accountDeletionRequestContext{position: latest.ContextPosition, events: eventstore.EventsFromLatest(latest.Results), query: query}
	for _, resolved := range model.events {
		switch resolved.Event.EventType {
		case UserRegistered:
			value, err := DecodeUserRegisteredEvent(resolved.Event)
			if err != nil {
				return nil, err
			}
			if value.UserRegisteredID != userRegisteredID {
				return nil, eventstore.ErrInvalidEvent
			}
			model.userExists = true
			if model.passwordPosition == nil {
				model.passwordHash = value.PasswordHash
			}
		case PasswordChanged:
			value, err := DecodePasswordChangedEvent(resolved.Event)
			if err != nil {
				return nil, err
			}
			if value.Scopes.UserRegisteredID != userRegisteredID {
				return nil, eventstore.ErrInvalidEvent
			}
			if model.passwordPosition == nil || resolved.Position.After(*model.passwordPosition) {
				position := resolved.Position
				model.passwordPosition = &position
				model.passwordHash = value.PasswordHash
			}
		case PasswordResetCompleted:
			value, err := DecodePasswordResetCompletedEvent(resolved.Event)
			if err != nil {
				return nil, err
			}
			if value.Scopes.UserRegisteredID != userRegisteredID {
				return nil, eventstore.ErrInvalidEvent
			}
			if model.passwordPosition == nil || resolved.Position.After(*model.passwordPosition) {
				position := resolved.Position
				model.passwordPosition = &position
				model.passwordHash = value.PasswordHash
			}
		case AccountDeletionRequested:
			value, err := DecodeAccountDeletionRequestedEvent(resolved.Event)
			if err != nil {
				return nil, err
			}
			if value.Scopes.UserRegisteredID != userRegisteredID {
				return nil, eventstore.ErrInvalidEvent
			}
			model.requestedID = value.AccountDeletionRequestedID
		case AccountDeleted:
			value, err := DecodeAccountDeletedEvent(resolved.Event)
			if err != nil {
				return nil, err
			}
			if value.Scopes.UserRegisteredID != userRegisteredID {
				return nil, eventstore.ErrInvalidEvent
			}
			model.deleted = true
		default:
			return nil, eventstore.ErrInvalidEvent
		}
	}
	return model, nil
}

func requestAccountDeletionAccountDeletedByUserQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: AccountDeleted},
		{Key: ScopeUserRegisteredIDField, Value: userRegisteredID},
	}}}}
}

func requestAccountDeletionAccountDeletionRequestedByUserQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: AccountDeletionRequested},
		{Key: ScopeUserRegisteredIDField, Value: userRegisteredID},
	}}}}
}

func requestAccountDeletionCombineQueries(queries ...eventstore.Query) eventstore.Query {
	combined := eventstore.Query{}
	for _, query := range queries {
		combined.Criteria = append(combined.Criteria, query.Criteria...)
	}
	return combined
}

func requestAccountDeletionUserRegisteredQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: UserRegistered},
		{Key: UserRegisteredIDField, Value: userRegisteredID},
	}}}}
}

func requestAccountDeletionPasswordChangedByUserQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: PasswordChanged},
		{Key: ScopeUserRegisteredIDField, Value: userRegisteredID},
	}}}}
}

func requestAccountDeletionPasswordResetCompletedByUserQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: PasswordResetCompleted},
		{Key: ScopeUserRegisteredIDField, Value: userRegisteredID},
	}}}}
}
