package httpui

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/viewstore"
	"github.com/starfederation/datastar-go/datastar"
)

type lifecycleWatcher struct {
	updates chan viewstore.Entry
	stops   atomic.Int32
}

func (w *lifecycleWatcher) Updates() <-chan viewstore.Entry { return w.updates }
func (w *lifecycleWatcher) Stop() error                     { w.stops.Add(1); return nil }

type lifecycleSubscription struct{ closes atomic.Int32 }

func (s *lifecycleSubscription) Close() error { s.closes.Add(1); return nil }

type lifecycleStore struct {
	viewstore.Store
	watcher  *lifecycleWatcher
	watchErr error
	ctx      context.Context
}

func (s *lifecycleStore) Watch(ctx context.Context, _ string, options viewstore.WatchOptions) (viewstore.Watcher, error) {
	if !options.IgnoreDeletes {
		return nil, errors.New("watch must ignore deletes")
	}
	s.ctx = ctx
	return s.watcher, s.watchErr
}
func TestViewStoreStreamCleansUpEveryExit(t *testing.T) {
	failure := errors.New("failure")
	for _, phase := range []string{"initial refresh", "watch", "subscribe", "catchup refresh", "decode", "patch", "watch ended", "disconnect"} {
		t.Run(phase, func(t *testing.T) {
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			watcher := &lifecycleWatcher{updates: make(chan viewstore.Entry, 1)}
			subscription := &lifecycleSubscription{}
			store := &lifecycleStore{watcher: watcher}
			if phase == "watch" {
				store.watchErr = failure
			}
			refreshes := 0
			config := viewStoreFatMorphConfig[map[string]string]{
				Key: "state", Store: store,
				Refresh: func(ctx context.Context) error {
					refreshes++
					if phase == "initial refresh" || phase == "catchup refresh" && refreshes == 2 {
						return failure
					}
					return nil
				},
				Subscribe: func(ctx context.Context, notify func()) (closeableSubscription, error) {
					if phase == "subscribe" {
						return nil, failure
					}
					if phase == "disconnect" {
						cancel()
					}
					return subscription, nil
				},
				Patch: func(*datastar.ServerSentEventGenerator, map[string]string) error { return failure },
			}
			switch phase {
			case "decode":
				watcher.updates <- viewstore.Entry{Value: "invalid JSON"}
			case "patch":
				watcher.updates <- viewstore.Entry{Value: `{"current":"state"}`}
			case "watch ended":
				close(watcher.updates)
			}
			done := make(chan error, 1)
			go func() { done <- streamViewStoreFatMorph(parent, nil, config) }()
			select {
			case err := <-done:
				if phase == "disconnect" || phase == "watch ended" {
					if err != nil {
						t.Fatal(err)
					}
				} else if err == nil {
					t.Fatal("error lost")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("stream did not exit")
			}
			wantWatch := int32(1)
			if phase == "initial refresh" || phase == "watch" {
				wantWatch = 0
			}
			if watcher.stops.Load() != wantWatch {
				t.Fatal("watcher cleanup mismatch")
			}
			wantSub := int32(1)
			if phase == "initial refresh" || phase == "watch" || phase == "subscribe" {
				wantSub = 0
			}
			if subscription.closes.Load() != wantSub {
				t.Fatal("subscription cleanup mismatch")
			}
			if store.ctx != nil && store.ctx.Err() == nil {
				t.Fatal("stream child context retained")
			}
			if phase != "disconnect" && parent.Err() != nil {
				t.Fatal("stream cancelled caller context")
			}
		})
	}
}
func TestViewStoreStreamCatchupAndSerializedPatching(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store := viewstore.NewMemoryStore()
	var revision atomic.Int64
	revision.Store(1)
	registered := make(chan func(), 1)
	patches := make(chan int64, 16)
	closed := &lifecycleSubscription{}
	var writing atomic.Bool
	done := make(chan error, 1)
	go func() {
		done <- streamViewStoreFatMorph(ctx, nil, viewStoreFatMorphConfig[struct{ Revision int64 }]{
			Key: "state", Store: store,
			Refresh: func(ctx context.Context) error {
				return viewstore.PutState(ctx, store, "state", struct{ Revision int64 }{revision.Load()})
			},
			Subscribe: func(_ context.Context, notify func()) (closeableSubscription, error) {
				revision.Store(2)
				registered <- notify
				return closed, nil
			},
			Patch: func(_ *datastar.ServerSentEventGenerator, state struct{ Revision int64 }) error {
				if !writing.CompareAndSwap(false, true) {
					t.Error("concurrent SSE writes")
				}
				defer writing.Store(false)
				patches <- state.Revision
				return nil
			},
		})
	}()
	notify := <-registered
	deadline := time.After(2 * time.Second)
	for {
		select {
		case value := <-patches:
			if value == 2 {
				goto caughtUp
			}
		case <-deadline:
			t.Fatal("subscription setup gap lost latest state")
		}
	}
caughtUp:
	revision.Store(3)
	for range 100 {
		notify()
	}
	deadline = time.After(2 * time.Second)
	for {
		select {
		case value := <-patches:
			if value == 3 {
				goto refreshed
			}
		case <-deadline:
			t.Fatal("notification did not refresh projection")
		}
	}
refreshed:
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("disconnect did not exit")
	}
	if closed.closes.Load() != 1 {
		t.Fatal("subscription retained")
	}
}

// Verify the SDK path handles a failed client write through the same cleanup path.
type failedStreamWriter struct{ *httptest.ResponseRecorder }

func (w failedStreamWriter) Write([]byte) (int, error) { return 0, errors.New("client disconnected") }
func TestViewStoreStreamWriteFailure(t *testing.T) {
	request := httptest.NewRequest("GET", "/stream", nil)
	sse := newSSE(failedStreamWriter{httptest.NewRecorder()}, request)
	store := viewstore.NewMemoryStore()
	subscription := &lifecycleSubscription{}
	var child context.Context
	err := streamViewStoreFatMorph(request.Context(), sse, viewStoreFatMorphConfig[map[string]string]{
		Key: "state", Store: store,
		Refresh: func(ctx context.Context) error {
			child = ctx
			return viewstore.PutState(ctx, store, "state", map[string]string{"value": "current"})
		},
		Subscribe: func(context.Context, func()) (closeableSubscription, error) { return subscription, nil },
		Patch: func(sse *datastar.ServerSentEventGenerator, state map[string]string) error {
			return sse.MarshalAndPatchSignals(state)
		},
	})
	if err == nil || child.Err() == nil || subscription.closes.Load() != 1 {
		t.Fatal("failed client write leaked stream resources")
	}
}
