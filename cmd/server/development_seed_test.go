package main

import (
	"io"
	"log/slog"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/config"
)

func TestSeedProductionAndMixedFlagsRejectedBeforeDatabaseAccess(t *testing.T) {
	for _, tc := range []struct {
		name    string
		dev     bool
		options runOptions
	}{{"production", false, runOptions{seedOnly: true}}, {"migrate", true, runOptions{seedOnly: true, migrateOnly: true}}, {"reset", true, runOptions{seedOnly: true, resetReadModels: true}}} {
		t.Run(tc.name, func(t *testing.T) {
			err := run(t.Context(), func() {}, config.Config{DevelopmentCookie: tc.dev}, tc.options, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err == nil {
				t.Fatal("unsafe seeding flags accepted")
			}
			if tc.name == "production" && err.Error() != "development seeding is disabled in production" {
				t.Fatal("production guard ran after database access", err)
			}
		})
	}
}
func TestSeedStartsOnlyProjectionFactories(t *testing.T) {
	all := eventHandlerFactories(nil, nil, config.Config{}, appComponents{}, nil)
	selected := projectionFactories(all)
	if len(selected) != 3 {
		t.Fatal("seed projection inventory changed", len(selected))
	}
	want := map[string]bool{"auth user projection": true, "profile read model": true, "todo read model": true}
	for _, f := range selected {
		if !want[f.name] {
			t.Fatal("seed enabled unrelated workflow", f.name)
		}
	}
	// Normal startup still includes every workflow factory.
	if len(all) <= len(selected) {
		t.Fatal("normal workflow handlers missing")
	}
}
