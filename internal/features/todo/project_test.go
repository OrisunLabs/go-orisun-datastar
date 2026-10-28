package todo

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

// Behavior fake implements the consumer transaction contract; native SQL tests
// separately establish actual transaction enlistment, rollback and constraints.
type projectionTestRepository struct {
	rows           map[string]TodoProjectionState
	receipts       map[eventstore.Position]TodoProjectionOwner
	commits, saves int
	inTransaction  bool
	failAt         int
}

func (r *projectionTestRepository) WithinTransaction(ctx context.Context, block func(context.Context) error) error {
	rows := map[string]TodoProjectionState{}
	for id, row := range r.rows {
		rows[id] = row
	}
	receipts := map[eventstore.Position]TodoProjectionOwner{}
	for position, owner := range r.receipts {
		receipts[position] = owner
	}
	previousRows, previousReceipts := r.rows, r.receipts
	r.rows, r.receipts = rows, receipts
	r.inTransaction = true
	err := block(ctx)
	r.inTransaction = false
	if err != nil {
		r.rows, r.receipts = previousRows, previousReceipts
		return err
	}
	r.commits++
	return nil
}
func (r *projectionTestRepository) FindProcessed(_ context.Context, p eventstore.Position) (*TodoProjectionOwner, error) {
	if !r.inTransaction {
		return nil, errors.New("unbound fake read")
	}
	owner, ok := r.receipts[p]
	if !ok {
		return nil, nil
	}
	return &owner, nil
}
func (r *projectionTestRepository) Load(_ context.Context, id string) (*TodoProjectionState, error) {
	if !r.inTransaction {
		return nil, errors.New("unbound fake load")
	}
	row, ok := r.rows[id]
	if !ok {
		return nil, nil
	}
	return &row, nil
}
func (r *projectionTestRepository) Save(_ context.Context, state TodoProjectionState, event TodoProjectionOccurrence) error {
	if !r.inTransaction {
		return errors.New("unbound fake save")
	}
	r.saves++
	if r.failAt > 0 && r.saves == r.failAt {
		return errors.New("injected write failure")
	}
	r.rows[state.TodoID] = state
	r.receipts[event.Position] = TodoProjectionOwner{TodoID: state.TodoID, UserID: state.UserID}
	return nil
}
func todoProjectionHistory(t *testing.T) []eventstore.ResolvedEvent {
	t.Helper()
	user, id := uuidv7.NewString(), uuidv7.NewString()
	at := time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)
	// Deliberately share the complete/recomplete event UUID: occurrence identity
	// must include the full source position, never just event identity/type.
	completeID := uuidv7.NewString()
	events := []eventstore.DomainEvent{
		mustTodoEvent(NewTodoCreatedEvent(id, user, "Initial", at, nil)),
		mustTodoEvent(NewTodoRenamedEvent(uuidv7.NewString(), id, user, "Renamed", at, nil)),
		mustTodoEvent(NewTodoCompletedEvent(completeID, id, user, at, nil)),
		mustTodoEvent(NewTodoReopenedEvent(uuidv7.NewString(), id, user, at, nil)),
		mustTodoEvent(NewTodoCompletedEvent(completeID, id, user, at, nil)),
		mustTodoEvent(NewTodoDeletedEvent(uuidv7.NewString(), id, user, at, nil)),
	}
	result := []eventstore.ResolvedEvent{}
	for i, event := range events {
		result = append(result, eventstore.ResolvedEvent{Event: event, Position: eventstore.Position{Commit: 1, Prepare: int64(i + 1)}})
	}
	return result
}
func TestTodoProjectionPartitionsReadTheirOwnUncommittedState(t *testing.T) {
	events := todoProjectionHistory(t)
	var expected map[string]TodoProjectionState
	for _, partitions := range [][]int{{6}, {1, 1, 1, 1, 1, 1}, {2, 1, 3}} {
		repo := &projectionTestRepository{}
		offset := 0
		for _, size := range partitions {
			if _, err := ProjectBatch(t.Context(), events[offset:offset+size], repo); err != nil {
				t.Fatal(err)
			}
			offset += size
		}
		if expected == nil {
			expected = repo.rows
		} else if !reflect.DeepEqual(expected, repo.rows) {
			t.Fatal("partition-dependent projection state")
		}
		for _, row := range repo.rows {
			if !row.Completed() || row.Completion == nil || row.Reopening == nil || row.DeletedAt == nil || row.Title != "Renamed" {
				t.Fatalf("lost lifecycle facts: %#v", row)
			}
		}
		if len(repo.receipts) != 6 {
			t.Fatal("stored occurrences collapsed by UUID")
		}
	}
}
func TestTodoProjectionFailureRollsBackAndNeverPublishes(t *testing.T) {
	repo := &projectionTestRepository{failAt: 3}
	publisher := &projectionTestPublisher{repository: repo}
	handler := &TodoReadModelEventHandler{repository: repo, publisher: publisher}
	if err := handler.ProjectBatch(t.Context(), todoProjectionHistory(t)); err == nil {
		t.Fatal("injected failure ignored")
	}
	if len(repo.rows) != 0 || len(repo.receipts) != 0 || publisher.calls != 0 {
		t.Fatal("projection prefix or notification escaped rollback")
	}
}
func TestTodoProjectionPublicationRetryUsesPersistedEvidence(t *testing.T) {
	repo := &projectionTestRepository{}
	publisher := &projectionTestPublisher{repository: repo, fail: true}
	handler := &TodoReadModelEventHandler{repository: repo, publisher: publisher}
	events := todoProjectionHistory(t)
	if err := handler.ProjectBatch(t.Context(), events); err == nil {
		t.Fatal("publication failure ignored")
	}
	saves := repo.saves
	publisher.fail = false
	// Replay must recover routing from stored occurrence evidence, without
	// requiring the already-applied historical payload to decode again.
	for i := range events {
		events[i].Event.Data = map[string]any{"invalid": "historical payload"}
	}
	if err := handler.ProjectBatch(t.Context(), events); err != nil {
		t.Fatal(err)
	}
	if repo.saves != saves || publisher.calls != 2 {
		t.Fatal("publication retry repeated effects or lost invalidation")
	}
}

type projectionTestPublisher struct {
	repository *projectionTestRepository
	calls      int
	fail       bool
}

func (p *projectionTestPublisher) Publish(_ context.Context, _ string, data any) error {
	if p.repository.inTransaction || p.repository.commits == 0 {
		return errors.New("published before commit")
	}
	payload, ok := data.(map[string]string)
	if !ok || len(payload) != 1 || payload["userId"] == "" {
		return fmt.Errorf("notification carries more than routing identity")
	}
	p.calls++
	if p.fail {
		return errors.New("injected publish failure")
	}
	return nil
}

func TestTodoProjectionRejectsUnorderedInputBeforeTransaction(t *testing.T) {
	events := todoProjectionHistory(t)
	repo := &projectionTestRepository{}
	if _, err := ProjectBatch(t.Context(), []eventstore.ResolvedEvent{events[1], events[0]}, repo); !errors.Is(err, eventstore.ErrInvalidEvent) {
		t.Fatal("out-of-order source accepted")
	}
	if repo.commits != 0 || repo.saves != 0 {
		t.Fatal("source-order failure touched persistence")
	}
}
