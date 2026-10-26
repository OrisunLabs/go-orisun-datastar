package viewstore

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func awaitWatcherClosed(t *testing.T, watcher Watcher) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case _, ok := <-watcher.Updates():
			if !ok {
				return
			}
		case <-deadline.C:
			t.Fatal("watcher did not close")
		}
	}
}
func TestMemoryWatcherStopAndCancellation(t *testing.T) {
	for _, stop := range []bool{true, false} {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		store := NewMemoryStore()
		watcher, err := store.Watch(ctx, "state", WatchOptions{IgnoreDeletes: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Put(ctx, "state", "current"); err != nil {
			t.Fatal(err)
		}
		if stop {
			if err := watcher.Stop(); err != nil {
				t.Fatal(err)
			}
		} else {
			cancel()
		}
		awaitWatcherClosed(t, watcher)
		if err := watcher.Stop(); err != nil {
			t.Fatal(err)
		}
		store.mu.RLock()
		count := len(store.watchers)
		store.mu.RUnlock()
		if count != 0 {
			t.Fatal("watcher registration retained")
		}
	}
}
func TestMemoryWatcherConcurrentPutAndStop(t *testing.T) {
	store := NewMemoryStore()
	watcher, err := store.Watch(t.Context(), "state", WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 4 {
		group.Go(func() {
			for range 100 {
				if err := store.Put(t.Context(), "state", "current"); err != nil {
					t.Error(err)
				}
			}
		})
	}
	if err := watcher.Stop(); err != nil {
		t.Fatal(err)
	}
	group.Wait()
	awaitWatcherClosed(t, watcher)
}

type fakeKeyWatcher struct {
	updates chan nats.KeyValueEntry
	stopped chan struct{}
	once    sync.Once
}

func (w *fakeKeyWatcher) Error() <-chan error                { return nil }
func (w *fakeKeyWatcher) Context() context.Context           { return context.Background() }
func (w *fakeKeyWatcher) Updates() <-chan nats.KeyValueEntry { return w.updates }
func (w *fakeKeyWatcher) Stop() error                        { w.once.Do(func() { close(w.stopped) }); return nil }

type fakeKVEntry struct {
	nats.KeyValueEntry
	forwarding chan struct{}
}

func (e fakeKVEntry) Key() string              { close(e.forwarding); return "state" }
func (fakeKVEntry) Value() []byte              { return []byte(`{"current":true}`) }
func (fakeKVEntry) Operation() nats.KeyValueOp { return nats.KeyValuePut }
func TestNATSWatcherStopsWhileConsumerIsBlocked(t *testing.T) {
	for _, stop := range []bool{true, false} {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		source := &fakeKeyWatcher{updates: make(chan nats.KeyValueEntry, 2), stopped: make(chan struct{})}
		watcher := &natsWatcher{ctx: ctx, watcher: source, updates: make(chan Entry), done: make(chan struct{})}
		// An unbuffered output models an SSE consumer that cannot accept another update.
		source.updates <- nil
		forwarding := make(chan struct{})
		source.updates <- fakeKVEntry{forwarding: forwarding}
		exited := make(chan struct{})
		go func() { watcher.run(); close(exited) }()
		select {
		case <-forwarding:
		case <-time.After(2 * time.Second):
			t.Fatal("source was not forwarded")
		}
		if stop {
			if err := watcher.Stop(); err != nil {
				t.Fatal(err)
			}
		} else {
			cancel()
		}
		select {
		case <-exited:
		case <-time.After(2 * time.Second):
			t.Fatal("blocked forwarding leaked")
		}
		select {
		case <-source.stopped:
		default:
			t.Fatal("underlying subscription retained")
		}
		awaitWatcherClosed(t, watcher)
	}
}
