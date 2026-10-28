package auth

import (
	"context"
	"errors"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"golang.org/x/crypto/bcrypt"
)

type ResetPasswordCommand struct {
	Token, Password string
	Metadata        CommandMetadata
}
type PasswordResetReader interface {
	PasswordResetByToken(context.Context, string) (PasswordResetVerification, error)
}

// AuthorizePasswordResetCommandHandler authorizes immutable input. It does not change credentials.
func AuthorizePasswordResetCommandHandler(ctx context.Context, command ResetPasswordCommand, resets PasswordResetReader, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) (string, error) {
	if err := commandlimits.Assert(command); err != nil {
		return "", err
	}
	if len(command.Password) < 6 {
		return "", errors.New("password must be at least 6 characters")
	}
	verification, err := resets.PasswordResetByToken(ctx, command.Token)
	if err != nil {
		return "", err
	}
	model, err := loadResetPasswordContext(ctx, verification.ID, verification.UserID, retriever)
	if err != nil {
		return "", err
	}
	if model.request.PasswordResetRequestedID == "" || !model.registered {
		return "", errors.New("password reset request event not found")
	}
	if model.request.Scopes.UserRegisteredID != verification.UserID {
		return "", errors.New("password reset request does not belong to registered user")
	}
	if model.request.ResetTokenHash != protectedpii.FromEnv().SensitiveBlindIndex(PasswordResetRequestedTokenField, command.Token) {
		return "", errors.New("invalid password reset token")
	}
	if model.deleted {
		return "", eventstore.ErrNotFound
	}
	if model.completed {
		return "", errors.New("password reset already completed")
	}
	if model.authorization != nil {
		if bcrypt.CompareHashAndPassword([]byte(model.authorization.PasswordHash), []byte(command.Password)) != nil {
			return "", eventstore.ErrConflict
		}
		return model.authorization.PasswordResetAuthorizedID, nil
	}
	expiry, _ := time.Parse(time.RFC3339Nano, model.request.ExpiresAt)
	if !time.Now().Before(expiry) {
		return "", errors.New("password reset token expired")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(command.Password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	id, operationID := uuidv7.NewString(), uuidv7.NewString()
	collision, err := loadResetPasswordCollisionContext(ctx, id, operationID, retriever)
	if err != nil {
		return "", err
	}
	event, err := NewPasswordResetAuthorizedEvent(id, operationID, verification.ID, verification.UserID, string(hash), time.Now(), nil)
	if err != nil {
		return "", err
	}
	_, err = eventstore.SaveCommandEventsV2(
		ctx,
		saver,
		command.Metadata,
		[]eventstore.DomainEvent{event},
		[]eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}, collision},
	)
	if err != nil {
		return "", err
	}
	return id, nil
}

type resetPasswordContext struct {
	request                                                                      PasswordResetRequestedEvent
	authorization                                                                *PasswordResetAuthorizedEvent
	registered, completed                                                        bool
	deleted                                                                      bool
	registeredPosition, requestedPosition, authorizedPosition, completedPosition eventstore.Position
	query                                                                        eventstore.Query
	position                                                                     eventstore.Position
}

func loadResetPasswordContext(ctx context.Context, requestID, userID string, retriever eventstore.LatestRetriever) (*resetPasswordContext, error) {
	if !validAuthID(requestID) || !validAuthID(userID) {
		return nil, eventstore.ErrInvalidEvent
	}
	query := resetPasswordWorkflowQuery(requestID, userID)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	m := &resetPasswordContext{query: query, position: latest.ContextPosition}
	var completion *PasswordResetCompletedEvent
	for _, e := range eventstore.EventsFromLatest(latest.Results) {
		switch e.Event.EventType {
		case UserRegistered:
			v, err := DecodeUserRegisteredEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.UserRegisteredID != userID {
				return nil, eventstore.ErrInvalidEvent
			}
			m.registered = true
			m.registeredPosition = e.Position
		case PasswordResetRequested:
			v, err := DecodePasswordResetRequestedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.PasswordResetRequestedID != requestID || v.Scopes.UserRegisteredID != userID {
				return nil, eventstore.ErrInvalidEvent
			}
			m.request = v
			m.requestedPosition = e.Position
		case PasswordResetAuthorized:
			v, err := DecodePasswordResetAuthorizedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.Scopes.PasswordResetRequestedID != requestID || v.Scopes.UserRegisteredID != userID {
				return nil, eventstore.ErrInvalidEvent
			}
			m.authorization = &v
			m.authorizedPosition = e.Position
		case AccountDeleted:
			v, err := DecodeAccountDeletedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.Scopes.UserRegisteredID != userID {
				return nil, eventstore.ErrInvalidEvent
			}
			m.deleted = true
		case PasswordResetCompleted:
			v, err := DecodePasswordResetCompletedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			completion = &v
			m.completedPosition = e.Position
		default:
			return nil, eventstore.ErrInvalidEvent
		}
	}
	if m.registered && m.request.PasswordResetRequestedID != "" && !m.requestedPosition.After(m.registeredPosition) {
		return nil, eventstore.ErrInvalidEvent
	}
	if m.authorization != nil && (m.request.PasswordResetRequestedID == "" || !m.authorizedPosition.After(m.requestedPosition)) {
		return nil, eventstore.ErrInvalidEvent
	}
	if completion != nil {
		if !m.completedPosition.After(m.authorizedPosition) || completion.Scopes.PasswordResetRequestedID != requestID || completion.Scopes.UserRegisteredID != userID || m.authorization == nil || completion.Scopes.PasswordResetAuthorizedID != m.authorization.PasswordResetAuthorizedID || completion.Scopes.PasswordChangedID != m.authorization.OperationID {
			return nil, errors.New("password reset completion scope does not match request context")
		}
		m.completed = true
	}
	return m, nil
}
func resetPasswordWorkflowQuery(requestID, userID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}, {Key: UserRegisteredIDField, Value: userID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeleted}, {Key: ScopeUserRegisteredIDField, Value: userID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetRequested}, {Key: PasswordResetRequestedIDField, Value: requestID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetAuthorized}, {Key: ScopePasswordResetRequestedIDField, Value: requestID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetCompleted}, {Key: ScopePasswordResetRequestedIDField, Value: requestID}}},
	}}
}
func resetPasswordOperationCollisionQuery(id, operationID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetAuthorized}, {Key: PasswordResetAuthorizedIDField, Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetAuthorized}, {Key: "operationId", Value: operationID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordChanged}, {Key: "eventId", Value: operationID}}},
	}}
}

func loadResetPasswordCollisionContext(ctx context.Context, id, operationID string, retriever eventstore.LatestRetriever) (eventstore.ConsistencyObservation, error) {
	query := resetPasswordOperationCollisionQuery(id, operationID)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return eventstore.ConsistencyObservation{}, err
	}
	if len(eventstore.EventsFromLatest(latest.Results)) != 0 {
		return eventstore.ConsistencyObservation{}, eventstore.ErrConflict
	}
	return eventstore.ConsistencyObservation{Query: query, Position: latest.ContextPosition}, nil
}
