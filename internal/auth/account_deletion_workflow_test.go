package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func deletionWorkflowFixture(t *testing.T) (*eventstore.EmbeddedOrisun, RequestAccountDeletionCommand) {
	t.Helper()
	owner := uuidv7.NewString()
	store := eventstoretest.New(t, registeredAuthEvent(t, owner))
	return store, RequestAccountDeletionCommand{UserRegisteredID: owner, AuthUserID: owner, Password: "secret123"}
}
func TestAccountDeletionSeparateReactionsAndStableOperation(t *testing.T) {
	store, command := deletionWorkflowFixture(t)
	id, err := RequestAccountDeletionCommandHandler(t.Context(), command, store, store)
	if err != nil {
		t.Fatal(err)
	}
	requests := resetOutcomeFacts(t, store, AccountDeletionRequested)
	request, err := DecodeAccountDeletionRequestedEvent(requests[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	if request.OperationID == id || request.OperationID == command.UserRegisteredID {
		t.Fatal("workflow reused authority identity for domain operation")
	}
	retry, err := RequestAccountDeletionCommandHandler(t.Context(), command, store, store)
	if err != nil || retry != id {
		t.Fatalf("request retry replaced operation: %v", err)
	}
	if len(resetOutcomeFacts(t, store, AccountDeleted)) != 0 {
		t.Fatal("request executed deletion")
	}
	writeContext, err := store.GetWriteContext(t.Context(), requests[0].WriteID)
	if err != nil {
		t.Fatal(err)
	}
	if len(writeContext.Consistency) != 2 || len(writeContext.Consistency[0].Query.Criteria) != 4 || len(writeContext.Consistency[1].Query.Criteria) != 3 {
		t.Fatal("independent request/collision observations were not retained")
	}
	effects := &deletionIdentityEffects{}
	execution := &AccountDeletionEventHandler{data: effects, keys: effects, saver: store, events: store}
	for range 2 {
		if err := execution.handle(t.Context(), requests[0]); err != nil {
			t.Fatal(err)
		}
	}
	deleted := resetOutcomeFacts(t, store, AccountDeleted)
	if len(deleted) != 1 || deleted[0].Event.EventID != request.OperationID || len(effects.deleted) != 1 || len(effects.destroyed) != 1 || len(resetOutcomeFacts(t, store, AccountDeletionCompleted)) != 0 {
		t.Fatal("execution did not retain its single-command boundary")
	}
	if len(eventstore.Scopes(deleted[0].Event.Data)) != 1 {
		t.Fatal("domain deletion contains workflow ancestry")
	}
	completion := &AccountDeletionCompletionEventHandler{saver: store, retriever: store}
	for range 2 {
		if err := completion.handle(t.Context(), deleted[0]); err != nil {
			t.Fatal(err)
		}
	}
	receipts := resetOutcomeFacts(t, store, AccountDeletionCompleted)
	if len(receipts) != 1 || len(effects.deleted) != 1 || len(effects.destroyed) != 1 {
		t.Fatal("completion repeated cleanup or appended duplicate receipt")
	}
	receipt, err := DecodeAccountDeletionCompletedEvent(receipts[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Scopes.AccountDeletionRequestedID != id || receipt.Scopes.AccountDeletedID != request.OperationID {
		t.Fatal("completion did not name exact authorization and effect")
	}
}
func TestAccountDeletionExecutionRetriesCleanupFailure(t *testing.T) {
	store, command := deletionWorkflowFixture(t)
	if _, err := RequestAccountDeletionCommandHandler(t.Context(), command, store, store); err != nil {
		t.Fatal(err)
	}
	source := resetOutcomeFacts(t, store, AccountDeletionRequested)[0]
	effects := &deletionFailureEffects{fail: "key"}
	execution := &AccountDeletionEventHandler{data: effects, keys: effects, saver: store, events: store}
	if err := execution.handle(t.Context(), source); err == nil {
		t.Fatal("key failure ignored")
	}
	if len(resetOutcomeFacts(t, store, AccountDeleted)) != 0 || len(resetOutcomeFacts(t, store, AccountDeletionCompleted)) != 0 {
		t.Fatal("failed cleanup emitted success")
	}
	effects.fail = ""
	if err := execution.handle(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	if len(resetOutcomeFacts(t, store, AccountDeleted)) != 1 {
		t.Fatal("cleanup retry lost frozen operation")
	}
}
func TestConcurrentDeletionRequestsKeepOneFrozenOperation(t *testing.T) {
	store, command := deletionWorkflowFixture(t)
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	retriever := resetLatestFunc(func(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
		latest, err := store.GetLatestByCriteria(ctx, criteria)
		if err != nil {
			return latest, err
		}
		authority := false
		for _, c := range criteria {
			for _, tag := range c.Tags {
				if tag.Key == eventstore.EventTypeField && tag.Value == UserRegistered {
					authority = true
				}
			}
		}
		if authority {
			ready <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return latest, ctx.Err()
			}
		}
		return latest, nil
	})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := RequestAccountDeletionCommandHandler(t.Context(), command, store, retriever)
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatal("requests did not reach authority")
		}
	}
	unblock()
	successes, conflicts := 0, 0
	for range 2 {
		select {
		case err := <-results:
			if err == nil {
				successes++
			} else if errors.Is(err, eventstore.ErrConflict) {
				conflicts++
			} else {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("requests did not finish")
		}
	}
	requests := resetOutcomeFacts(t, store, AccountDeletionRequested)
	if successes != 1 || conflicts != 1 || len(requests) != 1 {
		t.Fatal("concurrent requests allocated multiple committed operations")
	}
	id, err := RequestAccountDeletionCommandHandler(t.Context(), command, store, store)
	if err != nil || id != requests[0].Event.EventID {
		t.Fatalf("retry did not recover accepted request: %v", err)
	}
}
