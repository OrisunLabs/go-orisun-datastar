package viewstore

import (
	"context"
	"encoding/json"
	"strings"
)

type Operation string

const (
	OperationPut    Operation = "PUT"
	OperationDelete Operation = "DEL"
	OperationPurge  Operation = "PURGE"
)

type Entry struct {
	Key       string
	Value     string
	Operation Operation
}

func (e Entry) JSON(target any) error {
	return json.Unmarshal([]byte(e.Value), target)
}

type WatchOptions struct {
	IgnoreDeletes bool
}

type Watcher interface {
	Updates() <-chan Entry
	Stop() error
}

type Store interface {
	Get(ctx context.Context, key string) (string, bool, error)
	Put(ctx context.Context, key string, value string) error
	Delete(ctx context.Context, key string) error
	Watch(ctx context.Context, key string, opts WatchOptions) (Watcher, error)
}

func PutState[T any](ctx context.Context, store Store, key string, state T) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return store.Put(ctx, key, string(data))
}

func GetState[T any](ctx context.Context, store Store, key string) (T, bool, error) {
	var zero T
	value, ok, err := store.Get(ctx, key)
	if err != nil || !ok {
		return zero, ok, err
	}
	if err := json.Unmarshal([]byte(value), &zero); err != nil {
		return zero, false, err
	}
	return zero, true, nil
}

func TodoListKey(sessionID, userRegisteredID string) string {
	return viewKey("todo-list", sessionID, userRegisteredID)
}

func ProfileKey(sessionID, userRegisteredID string) string {
	return viewKey("profile", sessionID, userRegisteredID)
}

func OpsMetricsKey() string {
	return viewKey("ops", "event-handlers")
}

func viewKey(parts ...string) string {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		part = strings.ReplaceAll(part, "/", "_")
		if part != "" {
			clean = append(clean, part)
		}
	}
	return strings.Join(clean, "//")
}

func RegistrationProgressKey(sessionID, registrationID string) string {
	return viewKey("registration-progress", sessionID, registrationID)
}

func PasswordResetProgressKey(sessionID, authorizationID string) string {
	return viewKey("password-reset-progress", sessionID, authorizationID)
}

func AccountDeletionProgressKey(sessionID, requestID string) string {
	return viewKey("account-deletion-progress", sessionID, requestID)
}
