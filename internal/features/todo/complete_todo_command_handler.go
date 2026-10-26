package todo

import (
	"context"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

type CompleteTodoCommand struct {
	UserID   string
	TodoID   string
	Metadata CommandMetadata
}

type CompleteTodoResult struct {
	TodoCompletedID string
	Skipped         bool
}

func CompleteTodoCommandHandler(ctx context.Context, command CompleteTodoCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) (CompleteTodoResult, error) {
	if err := commandlimits.Assert(command); err != nil {
		return CompleteTodoResult{}, err
	}
	model, err := loadCompleteTodoContext(ctx, retriever, command.TodoID, command.UserID)
	if err != nil {
		return CompleteTodoResult{}, err
	}
	if err := model.requireActive(); err != nil {
		return CompleteTodoResult{}, err
	}
	if model.completed {
		return CompleteTodoResult{Skipped: true}, nil
	}

	eventID := uuidv7.NewString()
	event, err := NewTodoCompletedEvent(eventID, command.TodoID, command.UserID, time.Now(), nil)
	if err != nil {
		return CompleteTodoResult{}, err
	}

	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}}); err != nil {
		return CompleteTodoResult{}, err
	}
	return CompleteTodoResult{TodoCompletedID: eventID}, nil
}

type completeTodoContext struct {
	exists    bool
	deleted   bool
	completed bool
	position  eventstore.Position
	query     eventstore.Query
}

func loadCompleteTodoContext(ctx context.Context, retriever eventstore.LatestRetriever, todoID, userID string) (*completeTodoContext, error) {
	if !canonicalID(todoID) || !canonicalID(userID) {
		return nil, eventstore.ErrInvalidEvent
	}
	query := completeTodoStreamQuery(todoID, userID)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	events := eventstore.EventsFromLatest(latest.Results)

	model := &completeTodoContext{position: latest.ContextPosition, query: query}
	for _, event := range events {
		facts, err := decodeTodoEvent(event.Event)
		if err != nil {
			return nil, err
		}
		if facts.todoID != todoID || facts.userID != userID {
			return nil, eventstore.ErrInvalidEvent
		}
		model.handle(event, facts)
	}
	return model, nil
}

func (m *completeTodoContext) requireActive() error {
	if !m.exists || m.deleted {
		return eventstore.ErrNotFound
	}
	return nil
}

func (m *completeTodoContext) handle(resolved eventstore.ResolvedEvent, facts decodedTodoEvent) {
	switch resolved.Event.EventType {
	case TodoCreated:
		m.exists = true
		m.deleted = false
		m.completed = false
	case TodoCompleted:
		m.completed = true
	case TodoReopened:
		m.completed = false
	case TodoDeleted:
		m.deleted = true
	}
}

func completeTodoStreamQuery(todoID, userID string) eventstore.Query {
	criteria := make([]eventstore.Criterion, 0, 5)
	for _, eventType := range []string{TodoCreated, TodoRenamed, TodoCompleted, TodoReopened, TodoDeleted} {
		idField := TodoScopeIDField
		if eventType == TodoCreated {
			idField = TodoCreatedIDField
		}
		criteria = append(criteria, eventstore.Criterion{Tags: []eventstore.Tag{
			{Key: eventstore.EventTypeField, Value: eventType},
			{Key: idField, Value: todoID},
			{Key: TodoUserIDField, Value: userID},
		}})
	}
	return eventstore.Query{Criteria: criteria}
}
