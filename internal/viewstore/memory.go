package viewstore

import (
	"context"
	"sync"
)

type MemoryStore struct {
	mu       sync.RWMutex
	values   map[string]string
	watchers map[string]map[*memoryWatcher]struct{}
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		values:   map[string]string{},
		watchers: map[string]map[*memoryWatcher]struct{}{},
	}
}

func (s *MemoryStore) Get(_ context.Context, key string) (string, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.values[key]
	return value, ok, nil
}

func (s *MemoryStore) Put(ctx context.Context, key string, value string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.values[key] = value
	s.broadcastLocked(Entry{Key: key, Value: value, Operation: OperationPut})
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.values, key)
	s.broadcastLocked(Entry{Key: key, Operation: OperationDelete})
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Watch(ctx context.Context, key string, opts WatchOptions) (Watcher, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w := &memoryWatcher{
		store:         s,
		key:           key,
		ignoreDeletes: opts.IgnoreDeletes,
		updates:       make(chan Entry, 16),
		done:          make(chan struct{}),
	}

	s.mu.Lock()
	if s.watchers[key] == nil {
		s.watchers[key] = map[*memoryWatcher]struct{}{}
	}
	s.watchers[key][w] = struct{}{}
	if value, ok := s.values[key]; ok {
		w.sendLocked(Entry{Key: key, Value: value, Operation: OperationPut})
	}
	s.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
			_ = w.Stop()
		case <-w.done:
		}
	}()

	return w, nil
}

func (s *MemoryStore) broadcastLocked(entry Entry) {
	for watcher := range s.watchers[entry.Key] {
		if watcher.ignoreDeletes && entry.Operation != OperationPut {
			continue
		}
		watcher.sendLocked(entry)
	}
}

type memoryWatcher struct {
	store         *MemoryStore
	key           string
	ignoreDeletes bool
	updates       chan Entry
	done          chan struct{}
	once          sync.Once
}

func (w *memoryWatcher) Updates() <-chan Entry {
	return w.updates
}

func (w *memoryWatcher) Stop() error {
	w.once.Do(func() {
		w.store.mu.Lock()
		delete(w.store.watchers[w.key], w)
		if len(w.store.watchers[w.key]) == 0 {
			delete(w.store.watchers, w.key)
		}
		w.store.mu.Unlock()
		close(w.done)
		close(w.updates)
	})
	return nil
}

func (w *memoryWatcher) sendLocked(entry Entry) {
	select {
	case <-w.done:
	case w.updates <- entry:
	default:
	}
}
