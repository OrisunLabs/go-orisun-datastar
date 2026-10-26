package todo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgreSQLProjectionRepository struct{ pool *pgxpool.Pool }

func NewPostgreSQLProjectionRepository(pool *pgxpool.Pool) *PostgreSQLProjectionRepository {
	return &PostgreSQLProjectionRepository{pool: pool}
}

type todoProjectionTransactionKey struct{}
type todoProjectionTransaction struct {
	owner   *PostgreSQLProjectionRepository
	queries *dbsql.Queries
	active  atomic.Bool
}

var errTodoProjectionTransaction = errors.New("todo projection requires an active owner transaction context")

func (r *PostgreSQLProjectionRepository) WithinTransaction(ctx context.Context, block func(context.Context) error) error {
	if ctx.Value(todoProjectionTransactionKey{}) != nil {
		return errors.New("nested todo projection transaction")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	binding := &todoProjectionTransaction{owner: r, queries: dbsql.New(r.pool).WithTx(tx)}
	binding.active.Store(true)
	defer binding.active.Store(false)
	txCtx := context.WithValue(ctx, todoProjectionTransactionKey{}, binding)
	if err := binding.queries.LockTodoProjection(txCtx); err != nil {
		return err
	}
	if err := block(txCtx); err != nil {
		return err
	}
	binding.active.Store(false)
	return tx.Commit(ctx)
}
func (r *PostgreSQLProjectionRepository) transactionQueries(ctx context.Context) (*dbsql.Queries, error) {
	binding, ok := ctx.Value(todoProjectionTransactionKey{}).(*todoProjectionTransaction)
	if !ok || binding.owner != r || !binding.active.Load() {
		return nil, errTodoProjectionTransaction
	}
	return binding.queries, nil
}
func (r *PostgreSQLProjectionRepository) FindProcessed(ctx context.Context, position eventstore.Position) (*TodoProjectionOwner, error) {
	query, err := r.transactionQueries(ctx)
	if err != nil {
		return nil, err
	}
	row, err := query.FindProcessedTodoProjection(ctx, dbsql.FindProcessedTodoProjectionParams{CommitPosition: position.Commit, PreparePosition: position.Prepare, PositionKey: fmt.Sprintf("%d/%d", position.Commit, position.Prepare)})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &TodoProjectionOwner{TodoID: row.TodoID, UserID: row.UserID}, nil
}
func (r *PostgreSQLProjectionRepository) Load(ctx context.Context, id string) (*TodoProjectionState, error) {
	query, err := r.transactionQueries(ctx)
	if err != nil {
		return nil, err
	}
	row, err := query.LoadTodoProjection(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	state := &TodoProjectionState{TodoID: row.TodoID, UserID: row.UserID, Title: row.Title, CreatedAt: row.CreatedAt.Time, UpdatedAt: row.UpdatedAt.Time, Position: eventstore.Position{Commit: row.LastEventCommitPosition, Prepare: row.LastEventPreparePosition}}
	state.Completion, err = decodeTodoLifecycleFact(row.CompletedAt, row.CompletedCommitPosition, row.CompletedPreparePosition)
	if err != nil {
		return nil, err
	}
	state.Reopening, err = decodeTodoLifecycleFact(row.ReopenedAt, row.ReopenedCommitPosition, row.ReopenedPreparePosition)
	if err != nil {
		return nil, err
	}
	if row.DeletedAt.Valid {
		at := row.DeletedAt.Time
		state.DeletedAt = &at
	}
	return state, nil
}
func (r *PostgreSQLProjectionRepository) Save(ctx context.Context, state TodoProjectionState, event TodoProjectionOccurrence) error {
	query, err := r.transactionQueries(ctx)
	if err != nil {
		return err
	}
	if state.TodoID != event.TodoID || state.UserID != event.UserID || state.Position != event.Position || event.EventType == "" || state.Position.Commit < 0 || state.Position.Prepare < 0 {
		return eventstore.ErrInvalidEvent
	}
	receipt, err := json.Marshal(map[string]TodoProjectionReceipt{fmt.Sprintf("%d/%d", event.Position.Commit, event.Position.Prepare): {EventType: event.EventType, Position: event.Position}})
	if err != nil {
		return err
	}
	params := dbsql.SaveTodoProjectionParams{TodoID: state.TodoID, UserID: state.UserID, Title: state.Title, CreatedAt: todoProjectionTime(state.CreatedAt), UpdatedAt: todoProjectionTime(state.UpdatedAt), LastEventCommitPosition: state.Position.Commit, LastEventPreparePosition: state.Position.Prepare, EventPositions: receipt}
	if state.DeletedAt != nil {
		params.DeletedAt = todoProjectionTime(*state.DeletedAt)
	}
	if state.Completion != nil {
		params.CompletedAt = todoProjectionTime(state.Completion.At)
		params.CompletedCommitPosition = &state.Completion.Position.Commit
		params.CompletedPreparePosition = &state.Completion.Position.Prepare
	}
	if state.Reopening != nil {
		params.ReopenedAt = todoProjectionTime(state.Reopening.At)
		params.ReopenedCommitPosition = &state.Reopening.Position.Commit
		params.ReopenedPreparePosition = &state.Reopening.Position.Prepare
	}
	count, err := query.SaveTodoProjection(ctx, params)
	if err != nil {
		return err
	}
	if count != 1 {
		return eventstore.ErrConflict
	}
	return nil
}
func todoProjectionTime(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value, Valid: true}
}
func decodeTodoLifecycleFact(at pgtype.Timestamptz, commit, prepare *int64) (*TodoLifecycleFact, error) {
	if !at.Valid && commit == nil && prepare == nil {
		return nil, nil
	}
	if !at.Valid || commit == nil || prepare == nil || *commit < 0 || *prepare < 0 {
		return nil, eventstore.ErrInvalidEvent
	}
	return &TodoLifecycleFact{At: at.Time, Position: eventstore.Position{Commit: *commit, Prepare: *prepare}}, nil
}
