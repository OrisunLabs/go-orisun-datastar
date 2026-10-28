package eventstore

import (
	"context"
	"database/sql"
	"errors"

	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

type SQLiteCheckpointer struct {
	queries *dbsql.Queries
}

func NewSQLiteCheckpointer(db *sql.DB) *SQLiteCheckpointer {
	return &SQLiteCheckpointer{queries: dbsql.New(db)}
}

func (c *SQLiteCheckpointer) GetCheckpoint(ctx context.Context, name string) (Position, bool, error) {
	row, err := c.queries.GetEventHandlerCheckpoint(ctx, name)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return NoEventPosition, false, nil
		}
		return NoEventPosition, false, err
	}
	return Position{Commit: row.CommitPosition, Prepare: row.PreparePosition}, true, nil
}

func (c *SQLiteCheckpointer) UpdateCheckpoint(ctx context.Context, name string, position Position) error {
	return c.queries.UpsertEventHandlerCheckpoint(ctx, dbsql.UpsertEventHandlerCheckpointParams{
		ID:              uuidv7.NewString(),
		Name:            name,
		CommitPosition:  position.Commit,
		PreparePosition: position.Prepare,
	})
}
