package todo

import (
	"context"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

// ProjectBatch folds one ordered source sequence inside one owner transaction.
// Each occurrence observes all state/evidence written by its predecessor.
func ProjectBatch(ctx context.Context, events []eventstore.ResolvedEvent, repository ProjectionRepository) ([]string, error) {
	for i, event := range events {
		if event.Position.Commit < 0 || event.Position.Prepare < 0 || (i > 0 && !event.Position.After(events[i-1].Position)) {
			return nil, eventstore.ErrInvalidEvent
		}
	}
	if len(events) == 0 {
		return nil, nil
	}
	users := []string{}
	seen := map[string]bool{}
	err := repository.WithinTransaction(ctx, func(txCtx context.Context) error {
		for _, event := range events {
			processed, err := repository.FindProcessed(txCtx, event.Position)
			if err != nil {
				return err
			}
			userID := ""
			if processed != nil {
				userID = processed.UserID
			} else {
				facts, err := decodeTodoEvent(event.Event)
				if err != nil {
					return err
				}
				occurrence := TodoProjectionOccurrence{TodoID: facts.todoID, UserID: facts.userID, EventType: event.Event.EventType, Title: facts.title, At: facts.at, Position: event.Position}
				current, err := repository.Load(txCtx, facts.todoID)
				if err != nil {
					return err
				}
				next, err := applyTodoProjection(current, occurrence)
				if err != nil {
					return err
				}
				if err := repository.Save(txCtx, next, occurrence); err != nil {
					return err
				}
				userID = next.UserID
			}
			if !seen[userID] {
				users = append(users, userID)
				seen[userID] = true
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return users, nil
}

func applyTodoProjection(current *TodoProjectionState, event TodoProjectionOccurrence) (TodoProjectionState, error) {
	if current == nil {
		if event.EventType != TodoCreated {
			return TodoProjectionState{}, eventstore.ErrNotFound
		}
		return TodoProjectionState{TodoID: event.TodoID, UserID: event.UserID, Title: event.Title, CreatedAt: event.At, UpdatedAt: event.At, Position: event.Position}, nil
	}
	next := *current
	if next.TodoID != event.TodoID || next.UserID != event.UserID || !event.Position.After(next.Position) || next.DeletedAt != nil {
		return TodoProjectionState{}, eventstore.ErrInvalidEvent
	}
	switch event.EventType {
	case TodoRenamed:
		next.Title = event.Title
	case TodoCompleted:
		if next.Completed() {
			return TodoProjectionState{}, eventstore.ErrInvalidEvent
		}
		next.Completion = &TodoLifecycleFact{At: event.At, Position: event.Position}
	case TodoReopened:
		if !next.Completed() {
			return TodoProjectionState{}, eventstore.ErrInvalidEvent
		}
		next.Reopening = &TodoLifecycleFact{At: event.At, Position: event.Position}
	case TodoDeleted:
		at := event.At
		next.DeletedAt = &at
	default:
		return TodoProjectionState{}, eventstore.ErrInvalidEvent
	}
	next.UpdatedAt = event.At
	next.Position = event.Position
	return next, nil
}
