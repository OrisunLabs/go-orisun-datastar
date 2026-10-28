package todo

import (
	"context"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

const TodoReadModelEventHandlerName = "todo_read_model_event_handler"

type TodoReadModelReader interface {
	List(context.Context, string) ([]views.Todo, error)
}

// ProjectionRepository operations require the active context supplied by this
// owner. Driver handles and generated query types stay inside its adapter.
type ProjectionRepository interface {
	WithinTransaction(context.Context, func(context.Context) error) error
	FindProcessed(context.Context, eventstore.Position) (*TodoProjectionOwner, error)
	Load(context.Context, string) (*TodoProjectionState, error)
	Save(context.Context, TodoProjectionState, TodoProjectionOccurrence) error
}
type TodoProjectionOwner struct{ TodoID, UserID string }
type TodoLifecycleFact struct {
	At       time.Time
	Position eventstore.Position
}
type TodoProjectionState struct {
	TodoID, UserID, Title string
	CreatedAt, UpdatedAt  time.Time
	Completion, Reopening *TodoLifecycleFact
	DeletedAt             *time.Time
	Position              eventstore.Position
}

func (s TodoProjectionState) Completed() bool {
	return s.Completion != nil && (s.Reopening == nil || s.Completion.Position.After(s.Reopening.Position))
}

type TodoProjectionOccurrence struct {
	TodoID, UserID, EventType, Title string
	At                               time.Time
	Position                         eventstore.Position
}
type TodoProjectionReceipt struct {
	EventType string              `json:"eventType"`
	Position  eventstore.Position `json:"position"`
}
