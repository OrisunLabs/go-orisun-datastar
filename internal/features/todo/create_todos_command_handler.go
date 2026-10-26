package todo

import (
	"context"
	"sort"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

// Titles maps caller-allocated creation IDs to titles. Reuse the exact map
// when retrying; one-item creation uses this same command.
type CreateTodosCommand struct {
	UserID   string
	Titles   map[string]string
	Metadata CommandMetadata
}

func CreateTodosCommandHandler(ctx context.Context, command CreateTodosCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadCreateTodosContext(ctx, command, retriever)
	if err != nil {
		return err
	}
	if len(model.events) == 0 {
		return nil
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, model.events, model.consistency)
	return err
}

type createTodosContext struct {
	events      []eventstore.DomainEvent
	consistency []eventstore.ConsistencyObservation
}

func loadCreateTodosContext(ctx context.Context, command CreateTodosCommand, retriever eventstore.LatestRetriever) (*createTodosContext, error) {
	if !canonicalID(command.UserID) {
		return nil, eventstore.ErrInvalidEvent
	}
	ids := make([]string, 0, len(command.Titles))
	titles := make(map[string]string, len(command.Titles))
	for id, title := range command.Titles {
		if !canonicalID(id) {
			return nil, eventstore.ErrInvalidEvent
		}
		normalized, err := validateTitle(title)
		if err != nil {
			return nil, err
		}
		titles[id] = normalized
		ids = append(ids, id)
	}
	model := &createTodosContext{}
	if len(ids) == 0 {
		return model, nil
	}
	sort.Strings(ids)
	query := createTodosQuery(ids)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model.consistency = []eventstore.ConsistencyObservation{{Query: query, Position: latest.ContextPosition}}
	existing := map[string]bool{}
	for _, result := range latest.Results {
		if result.Event == nil {
			continue
		}
		value, err := DecodeTodoCreatedEvent(result.Event.Event)
		if err != nil {
			return nil, err
		}
		title, selected := titles[value.TodoCreatedID]
		if !selected || value.UserID != command.UserID || value.Title != title {
			return nil, eventstore.ErrConflict
		}
		existing[value.TodoCreatedID] = true
	}
	now := time.Now()
	for _, id := range ids {
		if existing[id] {
			continue
		}
		event, err := NewTodoCreatedEvent(id, command.UserID, titles[id], now, nil)
		if err != nil {
			return nil, err
		}
		model.events = append(model.events, event)
	}
	return model, nil
}
func createTodosQuery(ids []string) eventstore.Query {
	query := eventstore.Query{}
	for _, id := range ids {
		query.Criteria = append(query.Criteria, eventstore.Criterion{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: TodoCreated}, {Key: "eventId", Value: id}}})
	}
	return query
}
