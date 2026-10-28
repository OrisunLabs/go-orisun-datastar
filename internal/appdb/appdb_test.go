package appdb_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/appdb"
)

func TestSQLitePoolSettingsAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "space ?#", "app.sqlite")
	db, err := appdb.Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	var connections []*sql.Conn
	for range 3 {
		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
		var foreignKeys, timeout int
		var mode string
		if err := conn.QueryRowContext(t.Context(), "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRowContext(t.Context(), "PRAGMA busy_timeout").Scan(&timeout); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRowContext(t.Context(), "PRAGMA journal_mode").Scan(&mode); err != nil {
			t.Fatal(err)
		}
		if foreignKeys != 1 || timeout != 5000 || mode != "wal" {
			t.Fatal("connection lost pool safety settings", foreignKeys, timeout, mode)
		}
	}
	for _, conn := range connections {
		conn.Close()
	}
	if err := appdb.Migrate(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), "INSERT INTO projector_checkpoint(id,name,commit_position,prepare_position) VALUES ('checkpoint','fixture',8,3)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("SQLite path was not preserved", err)
	}
	reopened, err := appdb.Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := appdb.Migrate(t.Context(), reopened); err != nil {
		t.Fatal(err)
	}
	var commit, prepare int64
	if err := reopened.QueryRowContext(t.Context(), "SELECT commit_position,prepare_position FROM projector_checkpoint WHERE name='fixture'").Scan(&commit, &prepare); err != nil || commit != 8 || prepare != 3 {
		t.Fatal("checkpoint did not survive reopen", commit, prepare, err)
	}
}

func TestSQLiteRelativePath(t *testing.T) {
	t.Chdir(t.TempDir())
	db, err := appdb.Open(t.Context(), filepath.Join("data", "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := appdb.Migrate(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join("data", "app.sqlite")); err != nil {
		t.Fatal("relative database path changed", err)
	}
}
