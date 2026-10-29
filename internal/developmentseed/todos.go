package developmentseed

import (
	"context"
	"errors"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/todo"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

var titles = [4]string{"Explore your workspace", "Verify an email address", "Rename this sample todo", "Clear completed work"}

const renamedTitle = "Watch live updates"

var kinds = []string{todo.TodoCreated, todo.TodoRenamed, todo.TodoCompleted, todo.TodoReopened, todo.TodoDeleted}

func plan(index int) []string {
	steps := []string{todo.TodoCreated}
	if index == 2 {
		steps = append(steps, todo.TodoRenamed)
	}
	if index > 0 {
		steps = append(steps, todo.TodoCompleted)
	}
	if index == 2 {
		steps = append(steps, todo.TodoReopened)
	}
	if index == 3 {
		steps = append(steps, todo.TodoDeleted)
	}
	return steps
}

type todoFacts map[string]eventstore.ResolvedEvent

func seedTodoFacts(ctx context.Context, s Store, spec userSpec, userID string) ([]todoFacts, error) {
	var criteria []eventstore.Criterion
	for _, id := range spec.todoIDs {
		for _, kind := range kinds {
			field := todo.TodoScopeIDField
			if kind == todo.TodoCreated {
				field = todo.TodoCreatedIDField
			}
			criteria = append(criteria, criterion(kind, field, id))
		}
	}
	facts, err := latest(ctx, s, criteria...)
	if err != nil {
		return nil, err
	}
	states := make([]todoFacts, len(spec.todoIDs))
	for i := range states {
		states[i] = todoFacts{}
	}
	for _, fact := range facts {
		var owner, id, title string
		switch fact.Event.EventType {
		case todo.TodoCreated:
			v, e := todo.DecodeTodoCreatedEvent(fact.Event)
			if e != nil {
				return nil, e
			}
			owner, id, title = v.UserID, v.TodoCreatedID, v.Title
		case todo.TodoRenamed:
			v, e := todo.DecodeTodoRenamedEvent(fact.Event)
			if e != nil {
				return nil, e
			}
			owner, id, title = v.UserID, v.Scopes.TodoCreatedID, v.Title
		case todo.TodoCompleted:
			v, e := todo.DecodeTodoCompletedEvent(fact.Event)
			if e != nil {
				return nil, e
			}
			owner, id = v.UserID, v.Scopes.TodoCreatedID
		case todo.TodoReopened:
			v, e := todo.DecodeTodoReopenedEvent(fact.Event)
			if e != nil {
				return nil, e
			}
			owner, id = v.UserID, v.Scopes.TodoCreatedID
		case todo.TodoDeleted:
			v, e := todo.DecodeTodoDeletedEvent(fact.Event)
			if e != nil {
				return nil, e
			}
			owner, id = v.UserID, v.Scopes.TodoCreatedID
		default:
			return nil, eventstore.ErrInvalidEvent
		}
		if owner != userID || !owned(fact.Event, spec.key, fact.Event.EventType) {
			return nil, errors.New("todo has different ownership or was edited")
		}
		index := -1
		for i, candidate := range spec.todoIDs {
			if id == candidate {
				index = i
				break
			}
		}
		if index < 0 {
			return nil, eventstore.ErrInvalidEvent
		}
		if fact.Event.EventType == todo.TodoCreated && title != titles[index] || fact.Event.EventType == todo.TodoRenamed && (index != 2 || title != renamedTitle) {
			return nil, errors.New("seed todo terms differ")
		}
		states[index][fact.Event.EventType] = fact
	}
	for i, state := range states {
		missing := false
		position := eventstore.NoEventPosition
		allowed := map[string]bool{}
		for _, kind := range plan(i) {
			allowed[kind] = true
			fact, ok := state[kind]
			if !ok {
				missing = true
				continue
			}
			if missing || !fact.Position.After(position) {
				return nil, errors.New("seed todo has incomplete or reordered ancestry")
			}
			position = fact.Position
		}
		for kind := range state {
			if !allowed[kind] {
				return nil, errors.New("seed todo lifecycle was changed")
			}
		}
	}
	return states, nil
}
func seedTodos(ctx context.Context, s Store, spec userSpec, userID string) ([]Todo, error) {
	states, err := seedTodoFacts(ctx, s, spec, userID)
	if err != nil {
		return nil, err
	}
	creates := map[string]string{}
	for i, id := range spec.todoIDs {
		if _, ok := states[i][todo.TodoCreated]; !ok {
			creates[id] = titles[i]
		}
	}
	if len(creates) > 0 {
		if err := todo.CreateTodosCommandHandler(ctx, todo.CreateTodosCommand{UserID: userID, Titles: creates, Metadata: metadata(spec.key, todo.TodoCreated)}, s, s); err != nil {
			return nil, err
		}
	}
	if _, ok := states[2][todo.TodoRenamed]; !ok {
		c := todo.RenameTodosCommand{UserID: userID, Operations: map[string]todo.TodoRename{spec.todoIDs[2]: {EventID: uuidv7.NewString(), Title: renamedTitle, ExpectedTitle: titles[2]}}, Metadata: metadata(spec.key, todo.TodoRenamed)}
		if err := todo.RenameTodosCommandHandler(ctx, c, s, s); err != nil {
			return nil, err
		}
	}
	completes := map[string]string{}
	for i, id := range spec.todoIDs {
		if i > 0 {
			if _, ok := states[i][todo.TodoCompleted]; !ok {
				completes[id] = uuidv7.NewString()
			}
		}
	}
	if len(completes) > 0 {
		if err := todo.CompleteTodosCommandHandler(ctx, todo.CompleteTodosCommand{UserID: userID, Operations: completes, Metadata: metadata(spec.key, todo.TodoCompleted)}, s, s); err != nil {
			return nil, err
		}
	}
	if _, ok := states[2][todo.TodoReopened]; !ok {
		if err := todo.ReopenTodosCommandHandler(ctx, todo.ReopenTodosCommand{UserID: userID, Operations: map[string]string{spec.todoIDs[2]: uuidv7.NewString()}, Metadata: metadata(spec.key, todo.TodoReopened)}, s, s); err != nil {
			return nil, err
		}
	}
	if _, ok := states[3][todo.TodoDeleted]; !ok {
		if err := todo.DeleteTodosCommandHandler(ctx, todo.DeleteTodosCommand{UserID: userID, Operations: map[string]string{spec.todoIDs[3]: uuidv7.NewString()}, Metadata: metadata(spec.key, todo.TodoDeleted)}, s, s); err != nil {
			return nil, err
		}
	}
	states, err = seedTodoFacts(ctx, s, spec, userID)
	if err != nil {
		return nil, err
	}
	out := make([]Todo, 0, len(spec.todoIDs))
	for i, id := range spec.todoIDs {
		steps := plan(i)
		if len(states[i]) != len(steps) {
			return nil, errors.New("seed todo history incomplete")
		}
		title := titles[i]
		if i == 2 {
			title = renamedTitle
		}
		out = append(out, Todo{ID: id, Title: title, Completed: i == 1 || i == 3, Deleted: i == 3, Position: states[i][steps[len(steps)-1]].Position})
	}
	return out, nil
}
