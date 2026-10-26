package todo

import (
	"context"

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

func (m *ReadModel) List(ctx context.Context, userID string) ([]views.Todo, error) {
	rows, err := m.queries.ListTodos(ctx, userID)
	if err != nil {
		return nil, err
	}

	todos := make([]views.Todo, 0, len(rows))
	for _, row := range rows {
		todos = append(todos, views.Todo{
			TodoID:    row.TodoID,
			Title:     row.Title,
			Completed: row.Completed,
			CreatedAt: row.CreatedAt.Time,
			UpdatedAt: row.UpdatedAt.Time,
		})
	}
	return todos, nil
}
