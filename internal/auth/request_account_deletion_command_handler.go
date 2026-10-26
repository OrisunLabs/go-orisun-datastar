package auth

import (
	"context"
	"errors"
	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
	"golang.org/x/crypto/bcrypt"
	"time"
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
	_, hash, err := credentials.UserByEmailWithPassword(ctx, command.User.Email)
	if err != nil {
		return "", errors.New("invalid password")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(command.Password)); err != nil {
		return "", errors.New("invalid password")
	}

	model, err := loadAccountDeletionRequestContext(ctx, command.User.UserRegisteredID, retriever)
	if err != nil {
		return "", err
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
	requestedID string
	deleted     bool
	position    eventstore.Position
	events      []eventstore.ResolvedEvent
	query       eventstore.Query
}

func loadAccountDeletionRequestContext(ctx context.Context, userRegisteredID string, retriever eventstore.LatestRetriever) (*accountDeletionRequestContext, error) {
	query := requestAccountDeletionCombineQueries(requestAccountDeletionUserRegisteredQuery(userRegisteredID), requestAccountDeletionAccountDeletionRequestedByUserQuery(userRegisteredID), requestAccountDeletionAccountDeletedByUserQuery(userRegisteredID))
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model := &accountDeletionRequestContext{position: latest.ContextPosition, events: eventstore.EventsFromLatest(latest.Results), query: query}
	for _, resolved := range model.events {
		if err := ValidateEvent(resolved.Event); err != nil {
			return nil, err
		}
		switch resolved.Event.EventType {
		case AccountDeletionRequested:
			model.requestedID, _ = resolved.Event.Data[AccountDeletionRequestedIDField].(string)
		case AccountDeleted:
			model.deleted = true
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
