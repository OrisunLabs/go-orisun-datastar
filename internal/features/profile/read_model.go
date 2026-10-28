package profile

import (
	"context"
	"database/sql"

	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

type ReadModel struct {
	queries *dbsql.Queries
}

func NewReadModel(db *sql.DB) *ReadModel {
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
