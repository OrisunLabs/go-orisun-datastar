package auth

import (
	"context"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

type CompleteAccountDeletionCommand struct {
	AccountDeletedID string
	Metadata         CommandMetadata
}

func CompleteAccountDeletionCommandHandler(ctx context.Context, command CompleteAccountDeletionCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadCompleteAccountDeletionContext(ctx, command.AccountDeletedID, retriever)
	if err != nil {
		return err
	}
	if model.completed {
		return nil
	}
	r := model.request
	event, err := NewAccountDeletionCompletedEvent(uuidv7.NewString(), time.Now(), r.AccountDeletionRequestedID, r.OperationID, r.AuthUserID, r.Scopes.UserRegisteredID, nil)
	if err != nil {
		return err
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}})
	return err
}

type completeAccountDeletionContext struct {
	request   AccountDeletionRequestedEvent
	completed bool
	query     eventstore.Query
	position  eventstore.Position
}

func loadCompleteAccountDeletionContext(ctx context.Context, id string, retriever eventstore.LatestRetriever) (*completeAccountDeletionContext, error) {
	if !validAuthID(id) {
		return nil, eventstore.ErrInvalidEvent
	}
	query := completeAccountDeletionQuery(id)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model := &completeAccountDeletionContext{query: query, position: latest.ContextPosition}
	var effect AccountDeletedEvent
	var receipt *AccountDeletionCompletedEvent
	var requestPosition, effectPosition, receiptPosition eventstore.Position
	for _, e := range eventstore.EventsFromLatest(latest.Results) {
		switch e.Event.EventType {
		case AccountDeletionRequested:
			v, err := DecodeAccountDeletionRequestedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.OperationID != id {
				return nil, eventstore.ErrInvalidEvent
			}
			model.request = v
			requestPosition = e.Position
		case AccountDeleted:
			v, err := DecodeAccountDeletedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.AccountDeletedID != id {
				return nil, eventstore.ErrInvalidEvent
			}
			effect = v
			effectPosition = e.Position
		case AccountDeletionCompleted:
			v, err := DecodeAccountDeletionCompletedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			receipt = &v
			receiptPosition = e.Position
		default:
			return nil, eventstore.ErrInvalidEvent
		}
	}
	if effect.AccountDeletedID == "" {
		return nil, eventstore.ErrNotFound
	}
	r := model.request
	if r.AccountDeletionRequestedID == "" {
		if receipt != nil {
			return nil, eventstore.ErrInvalidEvent
		}
		model.completed = true
		return model, nil
	}
	if !effectPosition.After(requestPosition) || effect.Scopes.UserRegisteredID != r.Scopes.UserRegisteredID || effect.AuthUserID != r.AuthUserID {
		return nil, eventstore.ErrConflict
	}
	if receipt != nil {
		if !receiptPosition.After(effectPosition) || receipt.Scopes.AccountDeletedID != id || receipt.Scopes.AccountDeletionRequestedID != r.AccountDeletionRequestedID || receipt.Scopes.UserRegisteredID != r.Scopes.UserRegisteredID || receipt.AuthUserID != r.AuthUserID {
			return nil, eventstore.ErrInvalidEvent
		}
		model.completed = true
	}
	return model, nil
}
func completeAccountDeletionQuery(id string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeletionRequested}, {Key: "operationId", Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeleted}, {Key: "eventId", Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeletionCompleted}, {Key: "scopes.accountDeletedId", Value: id}}},
	}}
}
