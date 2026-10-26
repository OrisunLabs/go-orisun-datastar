// Package eventstoretest supplies real, isolated Orisun storage for tests.
package eventstoretest

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/OrisunLabs/Orisun/boundary"
	"github.com/OrisunLabs/Orisun/config"
	"github.com/OrisunLabs/Orisun/logging"
	"github.com/OrisunLabs/Orisun/orisun"
	"github.com/OrisunLabs/Orisun/sqlite"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

// Orisun's logger uses a process-global singleton without synchronized setup.
var fixtureLogger = sync.OnceValues(func() (logging.Logger, error) {
	return logging.ZapLogger("error")
})

// New creates a private SQLite database in RAM. It uses Orisun's actual query
// engine and atomic consistency checks without starting NATS or PostgreSQL.
// The fixture has no subscriptions; reaction tests deliver events explicitly.
func New(t testing.TB, events ...eventstore.DomainEvent) *eventstore.EmbeddedOrisun {
	t.Helper()
	return newSQLiteStore(t, events...)
}

func newSQLiteStore(t testing.TB, events ...eventstore.DomainEvent) *eventstore.EmbeddedOrisun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logger, err := fixtureLogger()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := sqlite.InitializeSqliteDatabaseRuntimeWithLockProvider(ctx,
		config.SqliteConfig{InMemory: true}, config.AdminConfig{Boundary: "test_admin"}, &localLock{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	definition := boundary.Definition{Name: "test_boundary", Placement: boundary.Placement{Backend: "sqlite", Namespace: "test_boundary"}}
	if err := runtime.ProvisionBoundary(ctx, definition); err != nil {
		t.Fatal(err)
	}
	if err := runtime.InstallBoundary(ctx, definition); err != nil {
		t.Fatal(err)
	}
	client, err := orisun.NewOrisunServer(ctx, runtime.SaveEvents, runtime.GetEvents, runtime.LockProvider, nil, logger)
	if err != nil {
		t.Fatal(err)
	}
	store := eventstore.NewOrisunBoundaryStore(client, definition.Name)

	if len(events) > 0 {
		if _, err := store.SaveEventsV2(ctx, events, nil); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

// This fixture never starts subscriptions, the only operation requiring a
// distributed lock. Fail closed if one is accidentally attempted.
type localLock struct{}

func (*localLock) AcquireLock(context.Context, string) (orisun.LockLease, error) {
	return nil, errors.New("subscriptions are not supported by the SQLite command fixture")
}
