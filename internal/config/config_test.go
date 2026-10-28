package config

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"testing"
)

func TestPostgresURLPreservesReservedCredentials(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("POSTGRES_USER", "user@team")
	t.Setenv("POSTGRES_PASSWORD", "password@1:/?#%")
	t.Setenv("POSTGRES_HOST", "localhost")
	t.Setenv("POSTGRES_PORT", "5434")
	t.Setenv("POSTGRES_DB", "starter")
	t.Setenv("POSTGRES_USE_SSL", "false")
	parsed, err := pgxpool.ParseConfig(Load().PostgresURL)
	if err != nil {
		t.Fatal("generated connection URL could not be parsed")
	}
	c := parsed.ConnConfig
	if c.User != "user@team" || c.Password != "password@1:/?#%" || c.Host != "localhost" || c.Port != 5434 || c.Database != "starter" {
		t.Fatal("connection URL changed credentials or destination")
	}
}
func TestExplicitDatabaseURLPreserved(t *testing.T) {
	const supplied = "postgres://user:encoded%40password@localhost:5434/custom?sslmode=require"
	t.Setenv("DATABASE_URL", supplied)
	if Load().PostgresURL != supplied {
		t.Fatal("explicit database URL changed")
	}
}
