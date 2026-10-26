package auth

import (
	"context"
	"errors"
	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"time"
)

type CompleteAccountDeletionCommand struct {
	AccountDeletionRequestedID string
	UserRegisteredID           string
	AuthUserID                 string
	Metadata                   CommandMetadata
}

func CompleteAccountDeletionCommandHandler(ctx context.Context, command CompleteAccountDeletionCommand, data AccountDataDeletionPort, keys AccountPiiKeyPort, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadAccountDeletionCompletionContext(ctx, command.AccountDeletionRequestedID, command.UserRegisteredID, retriever)
	if err != nil {
		return err
	}
	if !model.requested {
		return errors.New("account deletion request event not found")
	}
	if model.userRegisteredID != command.UserRegisteredID || model.authUserID != command.AuthUserID {
		return errors.New("account deletion request does not match command identity")
	}
	if model.deleted {
		return nil
	}
	deletedID := uuidv7.NewString()
	event, err := NewAccountDeletedEvent(deletedID, time.Now(), model.requestedID, model.authUserID, model.userRegisteredID, nil)
	if err != nil {
		return err
	}
	if err := data.DeleteAccountData(ctx, model.userRegisteredID); err != nil {
		return err
	}
	if err := keys.DestroySubjectKey(ctx, model.userRegisteredID); err != nil {
		return err
	}

	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}})
	return err
}

type accountDeletionCompletionContext struct {
	requestedID             string
	userRegisteredID        string
	authUserID              string
	deletedRequestID        string
	deletedUserRegisteredID string
	deletedAuthUserID       string
	requested               bool
	deleted                 bool
	position                eventstore.Position
	events                  []eventstore.ResolvedEvent
	query                   eventstore.Query
}

func loadAccountDeletionCompletionContext(ctx context.Context, requestID, userRegisteredID string, retriever eventstore.LatestRetriever) (*accountDeletionCompletionContext, error) {
	query := completeAccountDeletionCombineQueries(completeAccountDeletionAccountDeletionRequestedQuery(requestID), completeAccountDeletionAccountDeletedByRequestQuery(requestID))
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model := &accountDeletionCompletionContext{requestedID: requestID, userRegisteredID: userRegisteredID, position: latest.ContextPosition, events: eventstore.EventsFromLatest(latest.Results), query: query}
	for _, resolved := range model.events {
		switch resolved.Event.EventType {
		case AccountDeletionRequested:
			requested, err := DecodeAccountDeletionRequestedEvent(resolved.Event)
			if err != nil {
				return nil, err
			}
			if requested.AccountDeletionRequestedID != requestID || requested.Scopes.UserRegisteredID != userRegisteredID {
				return nil, errors.New("account deletion request does not match completion context")
			}
			model.requestedID = requested.AccountDeletionRequestedID
			model.userRegisteredID = requested.Scopes.UserRegisteredID
			model.authUserID = requested.AuthUserID
			model.requested = true
		case AccountDeleted:
			deleted, err := DecodeAccountDeletedEvent(resolved.Event)
			if err != nil {
				return nil, err
			}
			model.deletedRequestID = deleted.Scopes.AccountDeletionRequestedID
			model.deletedUserRegisteredID = deleted.Scopes.UserRegisteredID
			model.deletedAuthUserID = deleted.AuthUserID
			model.deleted = true
		default:
			return nil, eventstore.ErrInvalidEvent
		}
	}
	if model.deleted && (model.deletedRequestID != model.requestedID || model.deletedUserRegisteredID != model.userRegisteredID || model.deletedAuthUserID != model.authUserID) {
		return nil, errors.New("account deleted scope does not match completion context")
	}
	return model, nil
}

func completeAccountDeletionAccountDeletedByRequestQuery(requestID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: AccountDeleted},
		{Key: "scopes.accountDeletionRequestedId", Value: requestID},
	}}}}
}

func completeAccountDeletionAccountDeletionRequestedQuery(requestID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: AccountDeletionRequested},
		{Key: AccountDeletionRequestedIDField, Value: requestID},
	}}}}
}

func completeAccountDeletionCombineQueries(queries ...eventstore.Query) eventstore.Query {
	combined := eventstore.Query{}
	for _, query := range queries {
		combined.Criteria = append(combined.Criteria, query.Criteria...)
	}
	return combined
}
