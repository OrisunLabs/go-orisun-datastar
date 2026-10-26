package eventstore

import (
	"context"
	"errors"

	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresCheckpointer struct {
	queries *dbsql.Queries
}

func NewPostgresCheckpointer(db *pgxpool.Pool) *PostgresCheckpointer {
	return &PostgresCheckpointer{queries: dbsql.New(db)}
}

func (c *PostgresCheckpointer) GetCheckpoint(ctx context.Context, name string) (Position, bool, error) {
	row, err := c.queries.GetEventHandlerCheckpoint(ctx, name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return NoEventPosition, false, nil
		}
		return NoEventPosition, false, err
	}
	return Position{Commit: row.CommitPosition, Prepare: row.PreparePosition}, true, nil
}

func (c *PostgresCheckpointer) UpdateCheckpoint(ctx context.Context, name string, position Position) error {
	return c.queries.UpsertEventHandlerCheckpoint(ctx, dbsql.UpsertEventHandlerCheckpointParams{
		ID:              uuidv7.NewString(),
		Name:            name,
		CommitPosition:  position.Commit,
		PreparePosition: position.Prepare,
	})
}
