// Package sqlitetest provides isolated real SQLite read-model databases.
package sqlitetest

import (
	"database/sql"
	"encoding/json"
	"github.com/OrisunLabs/go-orisun-datastar/internal/appdb"
	"path/filepath"
	"testing"
)

func New(t testing.TB) *sql.DB {
	t.Helper()
	db, err := appdb.Open(t.Context(), filepath.Join(t.TempDir(), "app.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := appdb.Migrate(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	return db
}

// Snapshot compares complete persisted state while excluding local write clocks.
func Snapshot(t testing.TB, db *sql.DB, tables ...string) string {
	t.Helper()
	out := map[string]any{}
	for _, table := range tables {
		// Names originate only from test literals, never application input.
		rows, err := db.QueryContext(t.Context(), "SELECT * FROM "+table+" ORDER BY 1")
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		values := []map[string]any{}
		for rows.Next() {
			raw := make([]any, len(columns))
			dest := make([]any, len(columns))
			for i := range raw {
				dest[i] = &raw[i]
			}
			if err := rows.Scan(dest...); err != nil {
				t.Fatal(err)
			}
			value := map[string]any{}
			for i, key := range columns {
				if key == "created_at" || key == "updated_at" {
					continue
				}
				if b, ok := raw[i].([]byte); ok {
					raw[i] = string(b)
				}
				if key == "completed" || key == "email_verified" || key == "banned" {
					if n, ok := raw[i].(int64); ok {
						raw[i] = n != 0
					}
				}
				value[key] = raw[i]
			}
			values = append(values, value)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		out[table] = values
	}
	var value any = out
	if len(tables) == 1 {
		value = out[tables[0]]
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
