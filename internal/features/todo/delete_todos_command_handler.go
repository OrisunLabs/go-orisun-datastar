package todo

import (
	"context"
	"sort"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

// DeleteTodosCommand carries the complete caller-owned selection. Each map key
// is a todo creation ID and each value is a fresh operation/event ID retained
// unchanged when retrying this intent.
type DeleteTodosCommand struct {
	UserID           string
	RequireCompleted bool
	Operations       map[string]string
	Metadata         CommandMetadata
}

func DeleteTodosCommandHandler(ctx context.Context, command DeleteTodosCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadDeleteTodosContext(ctx, command, retriever)
	if err != nil {
		return err
	}
	if len(model.events) == 0 {
		return nil
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, model.events, model.consistency)
	return err
}

type deleteTodosContext struct {
	events      []eventstore.DomainEvent
	consistency []eventstore.ConsistencyObservation
}
type deleteTodosItem struct {
	exists    bool
	completed bool
	deleted   bool
	applied   bool
}

func loadDeleteTodosContext(ctx context.Context, command DeleteTodosCommand, retriever eventstore.LatestRetriever) (*deleteTodosContext, error) {
	if !canonicalID(command.UserID) {
		return nil, eventstore.ErrInvalidEvent
	}
	ids := make([]string, 0, len(command.Operations))
	ownerByOperation := make(map[string]string, len(command.Operations))
	states := make(map[string]*deleteTodosItem, len(command.Operations))
	for id, operation := range command.Operations {
		if !canonicalID(id) || !canonicalID(operation) {
			return nil, eventstore.ErrInvalidEvent
		}
		if _, duplicate := ownerByOperation[operation]; duplicate {
			return nil, eventstore.ErrInvalidEvent
		}
		ownerByOperation[operation] = id
		states[id] = &deleteTodosItem{}
		ids = append(ids, id)
	}
	model := &deleteTodosContext{}
	if len(ids) == 0 {
		return model, nil
	}
	sort.Strings(ids)
	query := deleteTodosQuery(command.UserID, ids, command.Operations)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model.consistency = []eventstore.ConsistencyObservation{{Query: query, Position: latest.ContextPosition}}
	// Receipt criteria use the envelope ID without an owner filter so a reused
	// operation ID for another todo/user cannot masquerade as an absent outcome.
	for _, result := range latest.Results {
		operation := ""
		for _, tag := range result.Criterion.Tags {
			if tag.Key == "eventId" {
				operation = tag.Value
			}
		}
		if operation == "" || result.Event == nil {
			continue
		}
		id := ownerByOperation[operation]
		facts, err := decodeTodoEvent(result.Event.Event)
		if err != nil {
			return nil, err
		}
		if result.Event.Event.EventType != TodoDeleted || result.Event.Event.EventID != operation || facts.todoID != id || facts.userID != command.UserID {
			return nil, eventstore.ErrConflict
		}
		states[id].applied = true
	}
	// Fold only the authority criteria, excluding historical retry receipts.
	history := make([]eventstore.LatestCriterionResult, 0, len(latest.Results))
	for _, result := range latest.Results {
		receipt := false
		for _, tag := range result.Criterion.Tags {
			if tag.Key == "eventId" {
				receipt = true
			}
		}
		if !receipt {
			history = append(history, result)
		}
	}
	for _, resolved := range eventstore.EventsFromLatest(history) {
		facts, err := decodeTodoEvent(resolved.Event)
		if err != nil {
			return nil, err
		}
		state, ok := states[facts.todoID]
		if !ok || facts.userID != command.UserID {
			return nil, eventstore.ErrInvalidEvent
		}
		switch resolved.Event.EventType {
		case TodoCreated:
			state.exists = true
		case TodoCompleted:
			state.completed = true
		case TodoReopened:
			state.completed = false
		case TodoDeleted:
			state.deleted = true
		}
	}
	now := time.Now()
	for _, id := range ids {
		state := states[id]
		if state.applied {
			continue
		}
		if !state.exists || state.deleted {
			return nil, eventstore.ErrNotFound
		}
		if command.RequireCompleted && !state.completed {
			return nil, eventstore.ErrConflict
		}
		event, err := NewTodoDeletedEvent(command.Operations[id], id, command.UserID, now, nil)
		if err != nil {
			return nil, err
		}
		model.events = append(model.events, event)
	}
	return model, nil
}

func deleteTodosQuery(userID string, ids []string, operations map[string]string) eventstore.Query {
	query := eventstore.Query{}
	for _, id := range ids {
		for _, kind := range []string{TodoCreated, TodoRenamed, TodoCompleted, TodoReopened, TodoDeleted} {
			identity := TodoScopeIDField
			if kind == TodoCreated {
				identity = TodoCreatedIDField
			}
			query.Criteria = append(query.Criteria, eventstore.Criterion{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: kind}, {Key: identity, Value: id}, {Key: TodoUserIDField, Value: userID}}})
		}
		query.Criteria = append(query.Criteria, eventstore.Criterion{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: TodoDeleted}, {Key: "eventId", Value: operations[id]}}})
	}
	return query
}
