package config

import "testing"

func TestSQLitePathsAreConfigurable(t *testing.T) {
	t.Setenv("SQLITE_PATH", "data/custom app.sqlite")
	t.Setenv("ORISUN_SQLITE_DIR", "data/custom events")
	c := Load()
	if c.SQLitePath != "data/custom app.sqlite" || c.OrisunSQLiteDir != "data/custom events" {
		t.Fatal(c)
	}
}
