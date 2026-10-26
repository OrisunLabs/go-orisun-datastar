package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgreSQLProjectionRepository struct{ pool *pgxpool.Pool }

func NewPostgreSQLProjectionRepository(pool *pgxpool.Pool) *PostgreSQLProjectionRepository {
	return &PostgreSQLProjectionRepository{pool}
}

type authTxKey struct{}
type authTx struct {
	owner   *PostgreSQLProjectionRepository
	queries *dbsql.Queries
	active  atomic.Bool
}

func (r *PostgreSQLProjectionRepository) WithinTransaction(ctx context.Context, block func(context.Context) error) error {
	if ctx.Value(authTxKey{}) != nil {
		return errors.New("nested authentication projection transaction")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	binding := &authTx{owner: r, queries: dbsql.New(r.pool).WithTx(tx)}
	binding.active.Store(true)
	defer func() { binding.active.Store(false) }()
	tc := context.WithValue(ctx, authTxKey{}, binding)
	if err := binding.queries.LockAuthProjection(tc); err != nil {
		return err
	}
	if err := block(tc); err != nil {
		return err
	}
	binding.active.Store(false)
	return tx.Commit(ctx)
}
func (r *PostgreSQLProjectionRepository) queries(ctx context.Context) (*dbsql.Queries, error) {
	b, ok := ctx.Value(authTxKey{}).(*authTx)
	if !ok || b.owner != r || !b.active.Load() {
		return nil, errors.New("authentication projection requires active owner transaction")
	}
	return b.queries, nil
}
func (r *PostgreSQLProjectionRepository) Processed(ctx context.Context, pos eventstore.Position) (string, error) {
	q, err := r.queries(ctx)
	if err != nil {
		return "", err
	}
	id, err := q.AuthOccurrenceOwner(ctx, fmt.Sprintf("%d/%d", pos.Commit, pos.Prepare))
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return id, err
}
func (r *PostgreSQLProjectionRepository) Load(ctx context.Context, id string) (*AuthState, error) {
	q, err := r.queries(ctx)
	if err != nil {
		return nil, err
	}
	v, err := q.LoadAuthProjection(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		_, ownerErr := q.AuthCredentialOwner(ctx, id+":credential")
		if ownerErr == nil {
			return nil, eventstore.ErrConflict
		}
		if !errors.Is(ownerErr, pgx.ErrNoRows) {
			return nil, ownerErr
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if v.UserRegisteredID != id || v.CredentialID == nil || *v.CredentialID != id+":credential" || v.ProviderID == nil || *v.ProviderID != "credential" || v.UserID == nil || *v.UserID != id || v.AccountID == nil || *v.AccountID != v.Email {
		return nil, eventstore.ErrConflict
	}
	s := &AuthState{UserID: id, Name: v.Name, Username: v.Username, Email: v.Email, PasswordHash: v.Password, Image: v.Image, Bio: v.Bio, HeaderImage: v.HeaderImageUrl, EmailVerified: v.EmailVerified}
	s.Position, err = authProjectionPosition(v.LastEventCommitPosition, v.LastEventPreparePosition)
	if err != nil {
		return nil, err
	}
	if v.EmailValidatedID != nil {
		s.Verification = &EmailValidationFact{*v.EmailValidatedID, *v.EmailValidationOtpID, v.EmailValidatedAt.Time.UTC()}
	}
	return s, nil
}
func (r *PostgreSQLProjectionRepository) LoadVerification(ctx context.Context, id string) (*VerificationFact, error) {
	q, err := r.queries(ctx)
	if err != nil {
		return nil, err
	}
	v, err := q.LoadAuthVerificationInput(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	purpose, user, ok := strings.Cut(v.Identifier, ":")
	if !ok {
		return nil, eventstore.ErrInvalidEvent
	}
	return &VerificationFact{v.ID, user, purpose, v.Value, v.ExpiresAt.Time.UTC()}, nil
}
func (r *PostgreSQLProjectionRepository) Save(ctx context.Context, update ProjectionUpdate) error {
	s, o, fact := update.State, update.Occurrence, update.Verification
	q, err := r.queries(ctx)
	if err != nil {
		return err
	}
	if s.UserID == "" || s.UserID != o.UserID || o.EventType == "" || o.Position.Commit < 0 || o.Position.Prepare < 0 || s.Position == nil || s.Position.Commit < 0 || s.Position.Prepare < 0 || o.Position.After(*s.Position) {
		return eventstore.ErrInvalidEvent
	}
	// Persist only event type and full position as replay evidence.
	evidence, err := json.Marshal(map[string]any{fmt.Sprintf("%d/%d", o.Position.Commit, o.Position.Prepare): struct {
		EventType string              `json:"eventType"`
		Position  eventstore.Position `json:"position"`
	}{o.EventType, o.Position}})
	if err != nil {
		return err
	}
	input := dbsql.SaveAuthProjectionParams{ID: s.UserID, Name: s.Name, Username: stringPtr(s.Username), Email: s.Email, Image: stringPtr(s.Image), Bio: s.Bio, HeaderImageUrl: s.HeaderImage, EmailVerified: s.EmailVerified, EventPositions: evidence, LastEventCommitPosition: &s.Position.Commit, LastEventPreparePosition: &s.Position.Prepare}
	if s.Verification != nil {
		input.EmailValidatedID = &s.Verification.ID
		input.EmailValidationOtpID = &s.Verification.OTPID
		input.EmailValidatedAt = pgtype.Timestamptz{Time: s.Verification.At, Valid: true}
	}
	if err := q.SaveAuthProjection(ctx, input); err != nil {
		return err
	}
	if err := q.SaveAuthCredential(ctx, dbsql.SaveAuthCredentialParams{ID: s.UserID + ":credential", AccountID: s.Email, UserID: s.UserID, Password: stringPtr(s.PasswordHash)}); err != nil {
		return err
	}
	if fact != nil {
		return q.SaveAuthVerificationInput(ctx, dbsql.SaveAuthVerificationInputParams{ID: fact.ID, Identifier: fact.Purpose + ":" + fact.UserID, Value: fact.ValueHash, ExpiresAt: pgTime(fact.ExpiresAt)})
	}
	return nil
}

// A missing position is unknown; a known position must be complete and nonnegative.
func authProjectionPosition(commit, prepare *int64) (*eventstore.Position, error) {
	if commit == nil && prepare == nil {
		return nil, nil
	}
	if commit == nil || prepare == nil || *commit < 0 || *prepare < 0 {
		return nil, eventstore.ErrInvalidEvent
	}
	return &eventstore.Position{Commit: *commit, Prepare: *prepare}, nil
}
