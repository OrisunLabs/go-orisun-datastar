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

type ResetPasswordCommand struct {
	Token    string
	Password string
	Metadata CommandMetadata
}

type PasswordResetReader interface {
	PasswordResetByToken(ctx context.Context, token string) (PasswordResetVerification, error)
}

func ResetPasswordCommandHandler(ctx context.Context, command ResetPasswordCommand, resets PasswordResetReader, users AuthUserByIDReader, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	if len(command.Password) < 6 {
		return errors.New("password must be at least 6 characters")
	}
	verification, err := resets.PasswordResetByToken(ctx, command.Token)
	if err != nil {
		return err
	}
	user, err := users.UserByIDOrRegisteredID(ctx, verification.UserID)
	if err != nil {
		return err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(command.Password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	model, err := loadResetPasswordContext(ctx, verification.ID, retriever)
	if err != nil {
		return err
	}
	if !model.requestExists {
		return errors.New("password reset request event not found")
	}
	if model.alreadyCompleted {
		return errors.New("password reset already completed")
	}

	if model.userRegisteredID != user.UserRegisteredID {
		return errors.New("password reset request does not belong to registered user")
	}
	eventID := uuidv7.NewString()
	event, err := NewPasswordResetCompletedEvent(eventID, time.Now(), model.passwordResetRequestedID, model.userRegisteredID, string(hash), nil)
	if err != nil {
		return err
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}})
	return err
}

type resetPasswordContext struct {
	passwordResetRequestedID  string
	userRegisteredID          string
	completedRequestID        string
	completedUserRegisteredID string
	requestExists             bool
	alreadyCompleted          bool
	position                  eventstore.Position
	events                    []eventstore.ResolvedEvent
	query                     eventstore.Query
}

func loadResetPasswordContext(ctx context.Context, passwordResetRequestedID string, retriever eventstore.LatestRetriever) (*resetPasswordContext, error) {
	requestedQuery := resetPasswordPasswordResetRequestedQuery(passwordResetRequestedID)
	completedQuery := resetPasswordPasswordResetCompletedQuery(passwordResetRequestedID)
	query := resetPasswordCombineQueries(requestedQuery, completedQuery)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	events := eventstore.EventsFromLatest(latest.Results)
	model := &resetPasswordContext{passwordResetRequestedID: passwordResetRequestedID, position: latest.ContextPosition, events: events, query: query}
	for _, resolved := range events {
		if err := model.handle(resolved); err != nil {
			return nil, err
		}
	}
	if model.alreadyCompleted && (model.completedRequestID != model.passwordResetRequestedID || model.completedUserRegisteredID != model.userRegisteredID) {
		return nil, errors.New("password reset completion scope does not match request context")
	}
	return model, nil
}

func (m *resetPasswordContext) handle(resolved eventstore.ResolvedEvent) error {
	switch resolved.Event.EventType {
	case PasswordResetRequested:
		requested, err := DecodePasswordResetRequestedEvent(resolved.Event)
		if err != nil {
			return err
		}
		if requested.PasswordResetRequestedID != m.passwordResetRequestedID {
			return errors.New("password reset request does not match context")
		}
		m.userRegisteredID = requested.Scopes.UserRegisteredID
		m.requestExists = true
	case PasswordResetCompleted:
		completed, err := DecodePasswordResetCompletedEvent(resolved.Event)
		if err != nil {
			return err
		}
		m.completedRequestID = completed.Scopes.PasswordResetRequestedID
		m.completedUserRegisteredID = completed.Scopes.UserRegisteredID
		m.alreadyCompleted = true
	default:
		return eventstore.ErrInvalidEvent
	}
	return nil
}

func resetPasswordCombineQueries(queries ...eventstore.Query) eventstore.Query {
	combined := eventstore.Query{}
	for _, query := range queries {
		combined.Criteria = append(combined.Criteria, query.Criteria...)
	}
	return combined
}

func resetPasswordPasswordResetCompletedQuery(requestID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: PasswordResetCompleted},
		{Key: ScopePasswordResetRequestedIDField, Value: requestID},
	}}}}
}

func resetPasswordPasswordResetRequestedQuery(requestID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: PasswordResetRequested},
		{Key: PasswordResetRequestedIDField, Value: requestID},
	}}}}
}
