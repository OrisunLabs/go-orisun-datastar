package viewstore

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type NATSStore struct {
	kv nats.KeyValue
}

func NewNATSStore(conn *nats.Conn, bucket string, ttl time.Duration) (*NATSStore, error) {
	js, err := conn.JetStream()
	if err != nil {
		return nil, err
	}
	kv, err := js.KeyValue(bucket)
	if err != nil {
		kv, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:  bucket,
			History: 1,
			TTL:     ttl,
			Storage: nats.MemoryStorage,
		})
		if err != nil {
			return nil, err
		}
	}
	return &NATSStore{kv: kv}, nil
}

func (s *NATSStore) Get(ctx context.Context, key string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	entry, err := s.kv.Get(key)
	if errors.Is(err, nats.ErrKeyNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	switch entry.Operation() {
	case nats.KeyValueDelete, nats.KeyValuePurge:
		return "", false, nil
	default:
		return string(entry.Value()), true, nil
	}
}

func (s *NATSStore) Put(ctx context.Context, key string, value string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := s.kv.PutString(key, value)
	return err
}

func (s *NATSStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.kv.Delete(key)
}

func (s *NATSStore) Watch(ctx context.Context, key string, opts WatchOptions) (Watcher, error) {
	watchOpts := []nats.WatchOpt{nats.Context(ctx)}
	if opts.IgnoreDeletes {
		watchOpts = append(watchOpts, nats.IgnoreDeletes())
	}
	watcher, err := s.kv.Watch(key, watchOpts...)
	if err != nil {
		return nil, err
	}
	out := &natsWatcher{
		ctx:     ctx,
		watcher: watcher,
		updates: make(chan Entry, 16),
		done:    make(chan struct{}),
	}
	go out.run()
	return out, nil
}

type natsWatcher struct {
	ctx     context.Context
	watcher nats.KeyWatcher
	updates chan Entry
	done    chan struct{}
	once    sync.Once
	err     error
}

func (w *natsWatcher) Updates() <-chan Entry {
	return w.updates
}

func (w *natsWatcher) Stop() error {
	w.once.Do(func() {
		close(w.done)
		w.err = w.watcher.Stop()
	})
	return w.err
}

func (w *natsWatcher) run() {
	defer close(w.updates)
	defer w.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-w.ctx.Done():
			return
		case entry, ok := <-w.watcher.Updates():
			if !ok {
				return
			}
			if entry == nil {
				continue
			}
			select {
			case w.updates <- Entry{Key: entry.Key(), Value: string(entry.Value()), Operation: operationFromNATS(entry.Operation())}:
			case <-w.done:
				return
			case <-w.ctx.Done():
				return
			}
		}
	}
}

func operationFromNATS(op nats.KeyValueOp) Operation {
	switch op {
	case nats.KeyValueDelete:
		return OperationDelete
	case nats.KeyValuePurge:
		return OperationPurge
	default:
		return OperationPut
	}
}
