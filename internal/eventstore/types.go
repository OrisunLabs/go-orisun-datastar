package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
)

var (
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("event position conflict")
	ErrInvalidEvent = errors.New("invalid event")
)

type Position struct {
	Commit  int64 `json:"commitPosition"`
	Prepare int64 `json:"preparePosition"`
}

var (
	NoEventPosition   = Position{Commit: -1, Prepare: -1}
	LastEventPosition = Position{Commit: 1<<63 - 1, Prepare: 1<<63 - 1}
)

func (p Position) After(other Position) bool {
	if p.Commit != other.Commit {
		return p.Commit > other.Commit
	}
	return p.Prepare > other.Prepare
}

const EventTypeField = "eventType"

type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type Criterion struct {
	Tags []Tag `json:"tags"`
}

type Query struct {
	Criteria []Criterion `json:"criteria"`
}

type DomainEvent struct {
	EventID   string         `json:"eventId"`
	EventType string         `json:"eventType"`
	Data      map[string]any `json:"data"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

type ResolvedEvent struct {
	WriteID  string      `json:"writeId,omitempty"`
	Position Position    `json:"position"`
	Event    DomainEvent `json:"event"`
}

type LatestCriterionResult struct {
	Criterion Criterion      `json:"criterion"`
	Event     *ResolvedEvent `json:"event,omitempty"`
}

type LatestByCriteriaResult struct {
	Results         []LatestCriterionResult `json:"results"`
	ContextPosition Position                `json:"contextPosition"`
}

type WriteResult struct {
	Position Position `json:"position"`
}

// WriteContext contains the observations persisted and checked by Orisun.
type WriteContext struct {
	WriteID     string
	Consistency []ConsistencyObservation
}

// ConsistencyObservation preserves one exact authoritative read for atomic CCC.
type ConsistencyObservation struct {
	Query    Query    `json:"query"`
	Position Position `json:"position"`
}

type SaverV2 interface {
	SaveEventsV2(context.Context, []DomainEvent, []ConsistencyObservation) (WriteResult, error)
}

// LatestRetriever exposes bounded authority reads to command handlers.
type LatestRetriever interface {
	GetLatestByCriteria(context.Context, []Criterion) (LatestByCriteriaResult, error)
}

type Retriever interface {
	LatestRetriever
	GetEvents(context.Context, Position, int, Direction, Query) ([]ResolvedEvent, error)
}

type Subscriber interface {
	SubscribeToEvents(ctx context.Context, subscriberName string, after Position, query Query, handle func(context.Context, ResolvedEvent) error) error
}

type Store interface {
	SaverV2
	Retriever
	Subscriber
}

type Direction string

const (
	Forward  Direction = "forward"
	Backward Direction = "backward"
)

type Checkpointer interface {
	GetCheckpoint(ctx context.Context, name string) (Position, bool, error)
	UpdateCheckpoint(ctx context.Context, name string, position Position) error
}

type Publisher interface {
	Publish(ctx context.Context, subject string, data any) error
}

type MessageSubscription interface {
	Close() error
}

type MessageSubscriber interface {
	Subscribe(ctx context.Context, subject string, handle func(context.Context, []byte)) (MessageSubscription, error)
}

// Scopes returns plural event ancestry. Entity references belong in ordinary
// payload fields when they cross domain boundaries.
func Scopes(data map[string]any) map[string]any {
	scopes, _ := data["scopes"].(map[string]any)
	return scopes
}

func MustJSON(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func MustData(v any) map[string]any {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	out := map[string]any{}
	if err := json.Unmarshal(data, &out); err != nil {
		panic(err)
	}
	return out
}

func EventsFromLatest(results []LatestCriterionResult) []ResolvedEvent {
	events := make([]ResolvedEvent, 0, len(results))
	seen := make(map[Position]struct{}, len(results))
	for _, result := range results {
		if result.Event == nil {
			continue
		}
		if _, duplicate := seen[result.Event.Position]; duplicate {
			continue
		}
		seen[result.Event.Position] = struct{}{}
		events = append(events, *result.Event)
	}
	sort.SliceStable(events, func(i, j int) bool {
		left := events[i].Position
		right := events[j].Position
		if left.Commit != right.Commit {
			return left.Commit < right.Commit
		}
		return left.Prepare < right.Prepare
	})
	return events
}
