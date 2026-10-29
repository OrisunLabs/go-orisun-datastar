package profile

import (
	"context"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

type ReadModel struct {
	queries *dbsql.Queries
}

func NewReadModel(db *pgxpool.Pool) *ReadModel {
	return &ReadModel{queries: dbsql.New(db)}
}

func (m *ReadModel) User(ctx context.Context, userRegisteredID string) (views.User, error) {
	row, err := m.queries.ProfileUser(ctx, userRegisteredID)
	if err != nil {
		return views.User{}, err
	}
	return views.User{
		ID:               row.UserID,
		UserRegisteredID: row.UserID,
		Name:             row.Name,
		Username:         row.Username,
		Email:            row.Email,
		Image:            row.Image,
		Bio:              row.Bio,
		HeaderImageURL:   row.HeaderImageUrl,
	}, nil
}

// ProjectionReached reports the owning read model's full-position watermark.
func (s *ReadModel) ProjectionReached(ctx context.Context, id string, p eventstore.Position) (bool, error) {
	if p.Commit < 0 || p.Prepare < 0 {
		return false, eventstore.ErrInvalidEvent
	}
	return s.queries.ProfileProjectionReached(ctx, dbsql.ProfileProjectionReachedParams{ID: id, CommitPosition: p.Commit, PreparePosition: p.Prepare})
}
