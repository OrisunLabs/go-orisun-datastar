package auth

import (
	"context"
	"strings"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

type RequestPasswordResetCommand struct {
	EmailAddress string
	Metadata     CommandMetadata
}

type RequestPasswordResetResult struct {
	PasswordResetRequestedID string
	Token                    string
	ExpiresAt                time.Time
}

type PasswordResetUserReader interface {
	UserByEmailWithPassword(ctx context.Context, emailAddress string) (views.User, string, error)
}

func RequestPasswordResetCommandHandler(ctx context.Context, command RequestPasswordResetCommand, users PasswordResetUserReader, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, keys SubjectPiiKeyPort) (RequestPasswordResetResult, error) {
	if err := commandlimits.Assert(command); err != nil {
		return RequestPasswordResetResult{}, err
	}
	user, _, err := users.UserByEmailWithPassword(ctx, strings.ToLower(strings.TrimSpace(command.EmailAddress)))
	if err != nil {
		return RequestPasswordResetResult{}, nil
	}
	return requestPasswordReset(ctx, user, command.Metadata, saver, retriever, keys)
}

func requestPasswordReset(ctx context.Context, user views.User, metadata CommandMetadata, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, keys SubjectPiiKeyPort) (RequestPasswordResetResult, error) {
	model, err := loadRequestPasswordResetContext(ctx, user, retriever)
	if err != nil {
		return RequestPasswordResetResult{}, err
	}

	token, err := randomToken(32)
	if err != nil {
		return RequestPasswordResetResult{}, err
	}
	requestID := uuidv7.NewString()
	expiresAt := time.Now().Add(30 * time.Minute)
	subjectKey, ok, err := keys.GetSubjectDataKey(ctx, user.UserRegisteredID)
	if err != nil {
		return RequestPasswordResetResult{}, err
	}
	if !ok {
		return RequestPasswordResetResult{}, eventstore.ErrNotFound
	}
	event, err := NewPasswordResetRequestedEvent(requestID, user.Email, token, expiresAt, user.UserRegisteredID, subjectKey, nil)
	if err != nil {
		return RequestPasswordResetResult{}, err
	}
	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}}); err != nil {
		return RequestPasswordResetResult{}, err
	}
	return RequestPasswordResetResult{PasswordResetRequestedID: requestID, Token: token, ExpiresAt: expiresAt}, nil
}

type requestPasswordResetContext struct {
	position eventstore.Position
	events   []eventstore.ResolvedEvent
	query    eventstore.Query
}

func loadRequestPasswordResetContext(ctx context.Context, user views.User, retriever eventstore.LatestRetriever) (*requestPasswordResetContext, error) {
	query := requestPasswordResetUserRegisteredQuery(user.UserRegisteredID)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	events := eventstore.EventsFromLatest(latest.Results)
	for _, checked := range events {
		if err := ValidateEvent(checked.Event); err != nil {
			return nil, err
		}
	}
	model := &requestPasswordResetContext{position: latest.ContextPosition, events: events, query: query}
	return model, nil
}

func requestPasswordResetUserRegisteredQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: UserRegistered},
		{Key: UserRegisteredIDField, Value: userRegisteredID},
	}}}}
}
