package viewstore

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func queuedWatcherEntries(watcher Watcher) []Entry {
	var entries []Entry
	for {
		select {
		case entry, ok := <-watcher.Updates():
			if !ok {
				return entries
			}
			entries = append(entries, entry)
		default:
			return entries
		}
	}
}
func TestMemoryWatcherOverflowRetainsLatestState(t *testing.T) {
	store := NewMemoryStore()
	if err := store.Put(t.Context(), "state", "cached"); err != nil {
		t.Fatal(err)
	}
	slow, err := store.Watch(t.Context(), "state", WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Stop()
	fast, err := store.Watch(t.Context(), "state", WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Stop()
	if entry := <-fast.Updates(); entry.Value != "cached" {
		t.Fatal("missing initial state")
	}
	for i := 0; i < 100; i++ {
		value := fmt.Sprint(i)
		if err := store.Put(t.Context(), "state", value); err != nil {
			t.Fatal(err)
		}
		select {
		case entry := <-fast.Updates():
			if entry.Value != value {
				t.Fatal("slow watcher interfered with fast watcher")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("fast watcher blocked")
		}
	}
	queued := queuedWatcherEntries(slow)
	if len(queued) == 0 {
		t.Fatal("slow watcher lost all updates")
	}
	current, ok, err := store.Get(t.Context(), "state")
	if err != nil || !ok {
		t.Fatal(err)
	}
	last := queued[len(queued)-1]
	if last.Value != current || last.Operation != OperationPut || last.Key != "state" {
		t.Fatalf("store=%s, last queued entry=%+v", current, last)
	}
	previous := -1
	for _, entry := range queued {
		revision, err := strconv.Atoi(entry.Value)
		if err != nil || revision <= previous {
			t.Fatal("retained snapshots were reordered")
		}
		previous = revision
	}
}
func TestMemoryWatcherOverflowRetainsFinalDelete(t *testing.T) {
	for _, ignore := range []bool{false, true} {
		t.Run(fmt.Sprintf("ignoreDeletes=%t", ignore), func(t *testing.T) {
			store := NewMemoryStore()
			watcher, err := store.Watch(t.Context(), "state", WatchOptions{IgnoreDeletes: ignore})
			if err != nil {
				t.Fatal(err)
			}
			defer watcher.Stop()
			for i := 0; i < 100; i++ {
				if err := store.Put(t.Context(), "state", fmt.Sprint(i)); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Delete(t.Context(), "state"); err != nil {
				t.Fatal(err)
			}
			queued := queuedWatcherEntries(watcher)
			if len(queued) == 0 {
				t.Fatal("missing final state")
			}
			last := queued[len(queued)-1]
			if ignore {
				if last.Operation != OperationPut || last.Value != "99" {
					t.Fatalf("latest retained put lost: %+v", last)
				}
			} else if last.Operation != OperationDelete || last.Value != "" {
				t.Fatalf("final deletion lost: %+v", last)
			}
			if _, ok, err := store.Get(t.Context(), "state"); err != nil || ok {
				t.Fatal("delete did not remove stored state")
			}
		})
	}
}
