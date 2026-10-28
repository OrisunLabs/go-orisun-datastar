package appdb

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/OrisunLabs/go-orisun-datastar/migrations"
	"io/fs"
	_ "modernc.org/sqlite"
	"net/url"
	"os"
	"path/filepath"
	"sort"
)

// Open serializes writers before authority reads. Every pooled connection has
// foreign keys and a bounded busy timeout; WAL allows readers during writes.
func Open(ctx context.Context, filename string) (*sql.DB, error) {
	filename, err := filepath.Abs(filename)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: filename}
	q := url.Values{"_pragma": {"foreign_keys(1)", "busy_timeout(5000)", "journal_mode(WAL)"}, "_txlock": {"immediate"}, "_time_format": {"sqlite"}}
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	if err = db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}
func Migrate(ctx context.Context, db *sql.DB) error {
	files, err := fs.Glob(migrations.Files, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)
	for _, name := range files {
		data, err := migrations.Files.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err = db.ExecContext(ctx, string(data)); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return nil
}
