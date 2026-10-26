package todo

import (
	"context"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

type RenameTodoCommand struct {
	UserID   string
	TodoID   string
	Title    string
	Metadata CommandMetadata
}

type RenameTodoResult struct {
	TodoRenamedID string
	Skipped       bool
}

func RenameTodoCommandHandler(ctx context.Context, command RenameTodoCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) (RenameTodoResult, error) {
	if err := commandlimits.Assert(command); err != nil {
		return RenameTodoResult{}, err
	}
	title, err := validateTitle(command.Title)
	if err != nil {
		return RenameTodoResult{}, err
	}

	model, err := loadRenameTodoContext(ctx, retriever, command.TodoID, command.UserID)
	if err != nil {
		return RenameTodoResult{}, err
	}
	if err := model.requireActive(); err != nil {
		return RenameTodoResult{}, err
	}
	if model.title == title {
		return RenameTodoResult{Skipped: true}, nil
	}

	eventID := uuidv7.NewString()
	event, err := NewTodoRenamedEvent(eventID, command.TodoID, command.UserID, title, time.Now(), nil)
	if err != nil {
		return RenameTodoResult{}, err
	}

	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}}); err != nil {
		return RenameTodoResult{}, err
	}
	return RenameTodoResult{TodoRenamedID: eventID}, nil
}

type renameTodoContext struct {
	exists   bool
	deleted  bool
	title    string
	position eventstore.Position
	query    eventstore.Query
}

func loadRenameTodoContext(ctx context.Context, retriever eventstore.LatestRetriever, todoID, userID string) (*renameTodoContext, error) {
	if !canonicalID(todoID) || !canonicalID(userID) {
		return nil, eventstore.ErrInvalidEvent
	}
	query := renameTodoStreamQuery(todoID, userID)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	events := eventstore.EventsFromLatest(latest.Results)

	model := &renameTodoContext{position: latest.ContextPosition, query: query}
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

func (m *renameTodoContext) requireActive() error {
	if !m.exists || m.deleted {
		return eventstore.ErrNotFound
	}
	return nil
}

func (m *renameTodoContext) handle(resolved eventstore.ResolvedEvent, facts decodedTodoEvent) {
	switch resolved.Event.EventType {
	case TodoCreated:
		m.exists = true
		m.deleted = false
		m.title = facts.title
	case TodoRenamed:
		m.title = facts.title
	case TodoDeleted:
		m.deleted = true
	}
}

func renameTodoStreamQuery(todoID, userID string) eventstore.Query {
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
