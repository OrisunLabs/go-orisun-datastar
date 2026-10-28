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

type RequestAccountDeletionCommand struct {
	UserRegisteredID string
	AuthUserID       string
	Password         string
	Metadata         CommandMetadata
}

func RequestAccountDeletionCommandHandler(ctx context.Context, command RequestAccountDeletionCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) (string, error) {
	if err := commandlimits.Assert(command); err != nil {
		return "", err
	}
	model, err := loadAccountDeletionRequestContext(ctx, command, retriever)
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
	requestID, operationID := uuidv7.NewString(), uuidv7.NewString()
	collision, err := loadRequestAccountDeletionCollisionContext(ctx, requestID, operationID, retriever)
	if err != nil {
		return "", err
	}
	event, err := NewAccountDeletionRequestedEvent(requestID, time.Now(), command.AuthUserID, command.UserRegisteredID, operationID, nil)
	if err != nil {
		return "", err
	}
	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}, collision}); err != nil {
		return "", err
	}
	return requestID, nil
}

type accountDeletionRequestContext struct {
	userExists          bool
	passwordHash        string
	passwordPosition    *eventstore.Position
	registeredPosition  eventstore.Position
	requestedID         string
	requestedAuthUserID string
	operationID         string
	deletedOperationID  string
	deletedAuthUserID   string
	deleted             bool
	position            eventstore.Position
	query               eventstore.Query
}

func loadAccountDeletionRequestContext(ctx context.Context, command RequestAccountDeletionCommand, retriever eventstore.LatestRetriever) (*accountDeletionRequestContext, error) {
	userRegisteredID := command.UserRegisteredID
	if !validAuthID(userRegisteredID) || !validAuthID(command.AuthUserID) {
		return nil, eventstore.ErrInvalidEvent
	}
	query := requestAccountDeletionCombineQueries(requestAccountDeletionUserRegisteredQuery(userRegisteredID), requestAccountDeletionAccountDeletionRequestedByUserQuery(userRegisteredID), requestAccountDeletionAccountDeletedByUserQuery(userRegisteredID), requestAccountDeletionPasswordChangedByUserQuery(userRegisteredID))
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model := &accountDeletionRequestContext{position: latest.ContextPosition, query: query}
	for _, resolved := range eventstore.EventsFromLatest(latest.Results) {
		switch resolved.Event.EventType {
		case UserRegistered:
			value, err := DecodeUserRegisteredEvent(resolved.Event)
			if err != nil {
				return nil, err
			}
			// Accepted registration is the identity used for both auth_user.id and user_registered_id.
			if value.UserRegisteredID != userRegisteredID || value.UserRegisteredID != command.AuthUserID {
				return nil, eventstore.ErrInvalidEvent
			}
			model.userExists = true
			model.registeredPosition = resolved.Position
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

		case AccountDeletionRequested:
			value, err := DecodeAccountDeletionRequestedEvent(resolved.Event)
			if err != nil {
				return nil, err
			}
			if value.Scopes.UserRegisteredID != userRegisteredID {
				return nil, eventstore.ErrInvalidEvent
			}
			model.requestedID = value.AccountDeletionRequestedID
			model.requestedAuthUserID = value.AuthUserID
			model.operationID = value.OperationID
		case AccountDeleted:
			value, err := DecodeAccountDeletedEvent(resolved.Event)
			if err != nil {
				return nil, err
			}
			if value.Scopes.UserRegisteredID != userRegisteredID {
				return nil, eventstore.ErrInvalidEvent
			}
			model.deletedOperationID = value.AccountDeletedID
			model.deletedAuthUserID = value.AuthUserID
			model.deleted = true
		default:
			return nil, eventstore.ErrInvalidEvent
		}
	}
	if model.userExists && model.passwordPosition != nil && !model.passwordPosition.After(model.registeredPosition) {
		return nil, eventstore.ErrInvalidEvent
	}
	if model.requestedID != "" && model.requestedAuthUserID != command.AuthUserID {
		return nil, eventstore.ErrConflict
	}
	if model.deleted && (model.deletedOperationID != model.operationID || model.deletedAuthUserID != model.requestedAuthUserID) {
		return nil, errors.New("account deleted scope does not match request context")
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

func loadRequestAccountDeletionCollisionContext(ctx context.Context, requestID, operationID string, retriever eventstore.LatestRetriever) (eventstore.ConsistencyObservation, error) {
	query := requestAccountDeletionCollisionQuery(requestID, operationID)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return eventstore.ConsistencyObservation{}, err
	}
	if len(eventstore.EventsFromLatest(latest.Results)) != 0 {
		return eventstore.ConsistencyObservation{}, eventstore.ErrConflict
	}
	return eventstore.ConsistencyObservation{Query: query, Position: latest.ContextPosition}, nil
}
func requestAccountDeletionCollisionQuery(requestID, operationID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeletionRequested}, {Key: AccountDeletionRequestedIDField, Value: requestID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeletionRequested}, {Key: "operationId", Value: operationID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeleted}, {Key: "eventId", Value: operationID}}},
	}}
}
