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
	if model.deleted {
		return nil
	}
	deletedID := uuidv7.NewString()
	event, err := NewAccountDeletedEvent(deletedID, time.Now(), command.AccountDeletionRequestedID, command.AuthUserID, command.UserRegisteredID, nil)
	if err != nil {
		return err
	}
	if err := data.DeleteAccountData(ctx, command.UserRegisteredID); err != nil {
		return err
	}
	if err := keys.DestroySubjectKey(ctx, command.UserRegisteredID); err != nil {
		return err
	}

	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}})
	return err
}

type accountDeletionCompletionContext struct {
	requested bool
	deleted   bool
	position  eventstore.Position
	events    []eventstore.ResolvedEvent
	query     eventstore.Query
}

func loadAccountDeletionCompletionContext(ctx context.Context, requestID, userRegisteredID string, retriever eventstore.LatestRetriever) (*accountDeletionCompletionContext, error) {
	query := completeAccountDeletionCombineQueries(completeAccountDeletionAccountDeletionRequestedByUserQuery(userRegisteredID), completeAccountDeletionAccountDeletedByRequestQuery(requestID))
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model := &accountDeletionCompletionContext{position: latest.ContextPosition, events: eventstore.EventsFromLatest(latest.Results), query: query}
	for _, resolved := range model.events {
		if err := ValidateEvent(resolved.Event); err != nil {
			return nil, err
		}
		switch resolved.Event.EventType {
		case AccountDeletionRequested:
			id, _ := resolved.Event.Data[AccountDeletionRequestedIDField].(string)
			model.requested = id == requestID
		case AccountDeleted:
			model.deleted = true
		}
	}
	return model, nil
}

func completeAccountDeletionAccountDeletedByRequestQuery(requestID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: AccountDeleted},
		{Key: "scopes.accountDeletionRequestedId", Value: requestID},
	}}}}
}

func completeAccountDeletionAccountDeletionRequestedByUserQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: AccountDeletionRequested},
		{Key: ScopeUserRegisteredIDField, Value: userRegisteredID},
	}}}}
}

func completeAccountDeletionCombineQueries(queries ...eventstore.Query) eventstore.Query {
	combined := eventstore.Query{}
	for _, query := range queries {
		combined.Criteria = append(combined.Criteria, query.Criteria...)
	}
	return combined
}
