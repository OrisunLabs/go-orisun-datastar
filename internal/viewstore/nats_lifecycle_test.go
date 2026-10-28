package viewstore

import (
	"context"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestNATSKVWatcherDisconnectAndReconnect(t *testing.T) {
	server, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go server.Start()
	t.Cleanup(server.Shutdown)
	if !server.ReadyForConnections(2 * time.Second) {
		t.Fatal("test NATS server did not start")
	}
	conn, err := nats.Connect(server.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	store, err := NewNATSStore(conn, "LIFECYCLE_TEST", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	watcher, err := store.Watch(ctx, "state", WatchOptions{IgnoreDeletes: true})
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Stop()
	read := func(watcher Watcher, want string) {
		t.Helper()
		select {
		case entry, ok := <-watcher.Updates():
			if !ok || entry.Operation != OperationPut || entry.Value != want {
				t.Fatalf("unexpected KV update: %+v", entry)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("missing KV update")
		}
	}
	if err := store.Put(ctx, "state", "first"); err != nil {
		t.Fatal(err)
	}
	read(watcher, "first")
	if err := store.Delete(ctx, "state"); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(ctx, "state", "latest"); err != nil {
		t.Fatal(err)
	}
	read(watcher, "latest")
	cancel()
	awaitWatcherClosed(t, watcher)
	reconnect, err := store.Watch(t.Context(), "state", WatchOptions{IgnoreDeletes: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reconnect.Stop()
	read(reconnect, "latest")
	if err := reconnect.Stop(); err != nil {
		t.Fatal(err)
	}
	awaitWatcherClosed(t, reconnect)
}
