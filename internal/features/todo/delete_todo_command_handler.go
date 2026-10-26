package todo

import (
	"context"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

type DeleteTodoCommand struct {
	UserID   string
	TodoID   string
	Metadata CommandMetadata
}

type DeleteTodoResult struct {
	TodoDeletedID string
}

func DeleteTodoCommandHandler(ctx context.Context, command DeleteTodoCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) (DeleteTodoResult, error) {
	if err := commandlimits.Assert(command); err != nil {
		return DeleteTodoResult{}, err
	}
	model, err := loadDeleteTodoContext(ctx, retriever, command.TodoID, command.UserID)
	if err != nil {
		return DeleteTodoResult{}, err
	}
	if err := model.requireActive(); err != nil {
		return DeleteTodoResult{}, err
	}

	eventID := uuidv7.NewString()
	event, err := NewTodoDeletedEvent(eventID, command.TodoID, command.UserID, time.Now(), nil)
	if err != nil {
		return DeleteTodoResult{}, err
	}

	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}}); err != nil {
		return DeleteTodoResult{}, err
	}
	return DeleteTodoResult{TodoDeletedID: eventID}, nil
}

type deleteTodoContext struct {
	exists   bool
	deleted  bool
	position eventstore.Position
	query    eventstore.Query
}

func loadDeleteTodoContext(ctx context.Context, retriever eventstore.LatestRetriever, todoID, userID string) (*deleteTodoContext, error) {
	if !canonicalID(todoID) || !canonicalID(userID) {
		return nil, eventstore.ErrInvalidEvent
	}
	query := deleteTodoStreamQuery(todoID, userID)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	events := eventstore.EventsFromLatest(latest.Results)

	model := &deleteTodoContext{position: latest.ContextPosition, query: query}
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

func (m *deleteTodoContext) requireActive() error {
	if !m.exists || m.deleted {
		return eventstore.ErrNotFound
	}
	return nil
}

func (m *deleteTodoContext) handle(resolved eventstore.ResolvedEvent, facts decodedTodoEvent) {
	switch resolved.Event.EventType {
	case TodoCreated:
		m.exists = true
		m.deleted = false
	case TodoDeleted:
		m.deleted = true
	}
}

func deleteTodoStreamQuery(todoID, userID string) eventstore.Query {
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
