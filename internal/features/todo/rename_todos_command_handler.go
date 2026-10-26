package todo

import (
	"context"
	"sort"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

// RenameTodosCommand carries the complete caller-owned selection. Each map key
// is a todo creation ID; each value retains its operation ID, expected title
// and desired title unchanged when retrying this intent.
type RenameTodosCommand struct {
	UserID     string
	Operations map[string]TodoRename
	Metadata   CommandMetadata
}

func RenameTodosCommandHandler(ctx context.Context, command RenameTodosCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadRenameTodosContext(ctx, command, retriever)
	if err != nil {
		return err
	}
	if len(model.events) == 0 {
		return nil
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, model.events, model.consistency)
	return err
}

// TodoRename freezes the caller's expected title so an unchanged rename cannot
// later overwrite a different title when the same intent is retried.
type TodoRename struct {
	EventID       string
	Title         string
	ExpectedTitle string
}

type renameTodosContext struct {
	events      []eventstore.DomainEvent
	consistency []eventstore.ConsistencyObservation
}
type renameTodosItem struct {
	exists  bool
	title   string
	deleted bool
	applied bool
}

func loadRenameTodosContext(ctx context.Context, command RenameTodosCommand, retriever eventstore.LatestRetriever) (*renameTodosContext, error) {
	if !canonicalID(command.UserID) {
		return nil, eventstore.ErrInvalidEvent
	}
	ids := make([]string, 0, len(command.Operations))
	ownerByOperation := make(map[string]string, len(command.Operations))
	states := make(map[string]*renameTodosItem, len(command.Operations))
	for id, item := range command.Operations {
		operation := item.EventID
		if _, err := validateTitle(item.Title); err != nil {
			return nil, err
		}
		if _, err := validateTitle(item.ExpectedTitle); err != nil {
			return nil, err
		}
		if !canonicalID(id) || !canonicalID(operation) {
			return nil, eventstore.ErrInvalidEvent
		}
		if _, duplicate := ownerByOperation[operation]; duplicate {
			return nil, eventstore.ErrInvalidEvent
		}
		ownerByOperation[operation] = id
		states[id] = &renameTodosItem{}
		ids = append(ids, id)
	}
	model := &renameTodosContext{}
	if len(ids) == 0 {
		return model, nil
	}
	sort.Strings(ids)
	query := renameTodosQuery(command.UserID, ids, command.Operations)
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
		if result.Event.Event.EventType != TodoRenamed || result.Event.Event.EventID != operation || facts.todoID != id || facts.userID != command.UserID {
			return nil, eventstore.ErrConflict
		}
		title, _ := validateTitle(command.Operations[id].Title)
		if facts.title != title {
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
			state.title = facts.title
		case TodoRenamed:
			state.title = facts.title
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
		item := command.Operations[id]
		title, _ := validateTitle(item.Title)
		expected, _ := validateTitle(item.ExpectedTitle)
		if state.title != expected {
			return nil, eventstore.ErrConflict
		}
		if state.title == title {
			continue
		}
		event, err := NewTodoRenamedEvent(item.EventID, id, command.UserID, title, now, nil)
		if err != nil {
			return nil, err
		}
		model.events = append(model.events, event)
	}
	return model, nil
}

func renameTodosQuery(userID string, ids []string, operations map[string]TodoRename) eventstore.Query {
	query := eventstore.Query{}
	for _, id := range ids {
		for _, kind := range []string{TodoCreated, TodoRenamed, TodoCompleted, TodoReopened, TodoDeleted} {
			identity := TodoScopeIDField
			if kind == TodoCreated {
				identity = TodoCreatedIDField
			}
			query.Criteria = append(query.Criteria, eventstore.Criterion{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: kind}, {Key: identity, Value: id}, {Key: TodoUserIDField, Value: userID}}})
		}
		query.Criteria = append(query.Criteria, eventstore.Criterion{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: TodoRenamed}, {Key: "eventId", Value: operations[id].EventID}}})
	}
	return query
}
