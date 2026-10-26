package todo

import (
	"context"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

type CreateTodoCommand struct {
	UserID   string
	Title    string
	Metadata CommandMetadata
}

type CreateTodoResult struct {
	TodoID string
}

func CreateTodoCommandHandler(ctx context.Context, command CreateTodoCommand, saver eventstore.SaverV2) (CreateTodoResult, error) {
	if err := commandlimits.Assert(command); err != nil {
		return CreateTodoResult{}, err
	}
	model, err := newCreateTodoContext(command)
	if err != nil {
		return CreateTodoResult{}, err
	}

	event, err := NewTodoCreatedEvent(model.todoID, command.UserID, model.title, time.Now(), nil)
	if err != nil {
		return CreateTodoResult{}, err
	}

	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: eventstore.NoEventPosition}}); err != nil {
		return CreateTodoResult{}, err
	}
	return CreateTodoResult{TodoID: model.todoID}, nil
}

type createTodoContext struct {
	todoID string
	title  string
	query  eventstore.Query
}

func newCreateTodoContext(command CreateTodoCommand) (*createTodoContext, error) {
	title, err := validateTitle(command.Title)
	if err != nil {
		return nil, err
	}
	todoID := uuidv7.NewString()
	return &createTodoContext{
		todoID: todoID,
		title:  title,
		query:  createTodoStreamQuery(todoID, command.UserID),
	}, nil
}

func createTodoStreamQuery(todoID, userID string) eventstore.Query {
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
