package profile

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"sync/atomic"
)

type SQLiteProjectionRepository struct{ pool *sql.DB }

func NewSQLiteProjectionRepository(pool *sql.DB) *SQLiteProjectionRepository {
	return &SQLiteProjectionRepository{pool}
}

type profileTransactionKey struct{}
type profileTransaction struct {
	owner   *SQLiteProjectionRepository
	queries *dbsql.Queries
	active  atomic.Bool
}

func (r *SQLiteProjectionRepository) WithinTransaction(ctx context.Context, block func(context.Context) error) error {
	if ctx.Value(profileTransactionKey{}) != nil {
		return errors.New("nested profile projection transaction")
	}
	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	binding := &profileTransaction{owner: r, queries: dbsql.New(r.pool).WithTx(tx)}
	binding.active.Store(true)
	defer func() { binding.active.Store(false) }()
	txCtx := context.WithValue(ctx, profileTransactionKey{}, binding)
	if err := block(txCtx); err != nil {
		return err
	}
	binding.active.Store(false)
	return tx.Commit()
}
func (r *SQLiteProjectionRepository) queries(ctx context.Context) (*dbsql.Queries, error) {
	b, ok := ctx.Value(profileTransactionKey{}).(*profileTransaction)
	if !ok || b.owner != r || !b.active.Load() {
		return nil, errors.New("profile projection requires active owner transaction context")
	}
	return b.queries, nil
}
func (r *SQLiteProjectionRepository) FindProcessedProfile(ctx context.Context, p eventstore.Position) (string, error) {
	q, err := r.queries(ctx)
	if err != nil {
		return "", err
	}
	key := fmt.Sprintf("%d/%d", p.Commit, p.Prepare)
	id, err := q.FindProcessedProfile(ctx, &key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id, err
}
func (r *SQLiteProjectionRepository) LoadProfile(ctx context.Context, id string) (*ProfileState, error) {
	q, err := r.queries(ctx)
	if err != nil {
		return nil, err
	}
	row, err := q.LoadProfileProjection(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &ProfileState{UserID: row.UserID, Name: row.Name, Username: row.Username, Email: row.Email, Bio: row.Bio, ImageURL: row.Image, HeaderImageURL: row.HeaderImageUrl, Position: eventstore.Position{Commit: row.LastEventCommitPosition, Prepare: row.LastEventPreparePosition}}, nil
}
func (r *SQLiteProjectionRepository) SaveProfile(ctx context.Context, s ProfileState, o ProfileUpdateIdentity) error {
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
