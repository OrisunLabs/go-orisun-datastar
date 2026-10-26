package profile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"sync/atomic"
)

type PostgreSQLProjectionRepository struct{ pool *pgxpool.Pool }

func NewPostgreSQLProjectionRepository(pool *pgxpool.Pool) *PostgreSQLProjectionRepository {
	return &PostgreSQLProjectionRepository{pool}
}

type profileTransactionKey struct{}
type profileTransaction struct {
	owner   *PostgreSQLProjectionRepository
	queries *dbsql.Queries
	active  atomic.Bool
}

func (r *PostgreSQLProjectionRepository) WithinTransaction(ctx context.Context, block func(context.Context) error) error {
	if ctx.Value(profileTransactionKey{}) != nil {
		return errors.New("nested profile projection transaction")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	binding := &profileTransaction{owner: r, queries: dbsql.New(r.pool).WithTx(tx)}
	binding.active.Store(true)
	defer func() { binding.active.Store(false) }()
	txCtx := context.WithValue(ctx, profileTransactionKey{}, binding)
	if err := binding.queries.LockProfileProjection(txCtx); err != nil {
		return err
	}
	if err := block(txCtx); err != nil {
		return err
	}
	binding.active.Store(false)
	return tx.Commit(ctx)
}
func (r *PostgreSQLProjectionRepository) queries(ctx context.Context) (*dbsql.Queries, error) {
	b, ok := ctx.Value(profileTransactionKey{}).(*profileTransaction)
	if !ok || b.owner != r || !b.active.Load() {
		return nil, errors.New("profile projection requires active owner transaction context")
	}
	return b.queries, nil
}
func (r *PostgreSQLProjectionRepository) FindProcessedProfile(ctx context.Context, p eventstore.Position) (string, error) {
	q, err := r.queries(ctx)
	if err != nil {
		return "", err
	}
	id, err := q.FindProcessedProfile(ctx, fmt.Sprintf("%d/%d", p.Commit, p.Prepare))
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return id, err
}
func (r *PostgreSQLProjectionRepository) LoadProfile(ctx context.Context, id string) (*ProfileState, error) {
	q, err := r.queries(ctx)
	if err != nil {
		return nil, err
	}
	row, err := q.LoadProfileProjection(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &ProfileState{UserID: row.UserID, Name: row.Name, Username: row.Username, Email: row.Email, Bio: row.Bio, ImageURL: row.Image, HeaderImageURL: row.HeaderImageUrl, Position: eventstore.Position{Commit: row.LastEventCommitPosition, Prepare: row.LastEventPreparePosition}}, nil
}
func (r *PostgreSQLProjectionRepository) SaveProfile(ctx context.Context, s ProfileState, o ProfileUpdateIdentity) error {
	q, err := r.queries(ctx)
	if err != nil {
		return err
	}
	if s.UserID == "" || s.Position.Commit < 0 || s.Position.Prepare < 0 || o.UserID != s.UserID || o.EventType == "" || profilePositionBefore(s.Position, o.Position) {
		return errors.New("invalid profile occurrence")
	}
	receipt, err := profileReceiptJSON(o)
	if err != nil {
		return err
	}
	n, err := q.SaveProfileProjection(ctx, dbsql.SaveProfileProjectionParams{UserID: s.UserID, Name: s.Name, Username: s.Username, Email: s.Email, Bio: s.Bio, Image: s.ImageURL, HeaderImageUrl: s.HeaderImageURL, LastEventCommitPosition: s.Position.Commit, LastEventPreparePosition: s.Position.Prepare, EventPositions: receipt})
	if err != nil {
		return err
	}
	if n != 1 {
		return eventstore.ErrConflict
	}
	return nil
}
func profileReceiptJSON(identity ProfileUpdateIdentity) ([]byte, error) {
	if identity.Position.Commit < 0 || identity.Position.Prepare < 0 {
		return nil, eventstore.ErrInvalidEvent
	}
	return json.Marshal(map[string]ProcessedEventReceipt{fmt.Sprintf("%d/%d", identity.Position.Commit, identity.Position.Prepare): {EventType: identity.EventType, Position: identity.Position}})
}
