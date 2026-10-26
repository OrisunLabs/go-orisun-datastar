package todo

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
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

func (m *ReadModel) InsertCreatedTodo(ctx context.Context, event TodoCreatedProjection) error {
	return m.queries.InsertCreatedTodo(ctx, dbsql.InsertCreatedTodoParams{
		TodoID:                   event.TodoID,
		UserID:                   event.UserID,
		Title:                    event.Title,
		LastEventCommitPosition:  event.Position.Commit,
		LastEventPreparePosition: event.Position.Prepare,
		CreatedAt:                pgTime(event.CreatedAt),
	})
}

func (m *ReadModel) RenameTodo(ctx context.Context, event TodoRenamedProjection) error {
	return m.queries.RenameTodo(ctx, dbsql.RenameTodoParams{
		Title:                    event.Title,
		LastEventCommitPosition:  event.Position.Commit,
		LastEventPreparePosition: event.Position.Prepare,
		UpdatedAt:                pgTime(event.RenamedAt),
		TodoID:                   event.TodoID,
	})
}

func (m *ReadModel) CompleteTodo(ctx context.Context, event TodoCompletedProjection) error {
	return m.queries.CompleteTodo(ctx, dbsql.CompleteTodoParams{
		CompletedAt:              pgTime(event.CompletedAt),
		LastEventCommitPosition:  event.Position.Commit,
		LastEventPreparePosition: event.Position.Prepare,
		TodoID:                   event.TodoID,
	})
}

func (m *ReadModel) ReopenTodo(ctx context.Context, event TodoReopenedProjection) error {
	return m.queries.ReopenTodo(ctx, dbsql.ReopenTodoParams{
		LastEventCommitPosition:  event.Position.Commit,
		LastEventPreparePosition: event.Position.Prepare,
		UpdatedAt:                pgTime(event.ReopenedAt),
		TodoID:                   event.TodoID,
	})
}

func (m *ReadModel) DeleteTodo(ctx context.Context, event TodoDeletedProjection) error {
	return m.queries.DeleteTodo(ctx, dbsql.DeleteTodoParams{
		DeletedAt:                pgTime(event.DeletedAt),
		LastEventCommitPosition:  event.Position.Commit,
		LastEventPreparePosition: event.Position.Prepare,
		TodoID:                   event.TodoID,
	})
}

func pgTime(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value, Valid: true}
}
