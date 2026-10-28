package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func TestRemainingAuthMutationsObserveAccountDeletion(t *testing.T) {
	for _, intent := range []string{"assign password", "rename"} {
		t.Run(intent, func(t *testing.T) {
			for _, scenario := range []string{"active", "deleted", "deleted unchanged name", "foreign deletion", "malformed deletion", "deletion before registration"} {
				t.Run(scenario, func(t *testing.T) {
					root, keys := nameCommandFixture(t, "Alice", "Example")
					facts := []eventstore.DomainEvent{root}
					rejected := scenario != "active" && scenario != "foreign deletion"
					if scenario != "active" {
						owner := keys.owner
						if scenario == "foreign deletion" {
							owner = uuidv7.NewString()
						}
						deleted := mustAuthEvent(NewAccountDeletedEvent(uuidv7.NewString(), time.Now(), owner, owner, nil))
						if scenario == "malformed deletion" {
							deleted.Data = eventstore.MustData(deleted.Data)
							deleted.Data["extra"] = true
						}
						facts = append(facts, deleted)
						if scenario == "deletion before registration" {
							facts = []eventstore.DomainEvent{deleted, root}
						}
					}
					store := eventstoretest.New(t, facts...)
					kind := PasswordChanged
					expectedCriteria := 4
					var err error
					if intent == "assign password" {
						err = SetPasswordCommandHandler(t.Context(), SetPasswordCommand{PasswordChangedID: uuidv7.NewString(), UserRegisteredID: keys.owner, PasswordHash: testAuthHash(t)}, store, store)
					} else {
						kind = UserNameChanged
						expectedCriteria = 3
						name := "New Name"
						if scenario == "deleted unchanged name" {
							name = "Alice Example"
						}
						_, err = UpdateUserNameCommandHandler(t.Context(), UpdateUserNameCommand{UserRegisteredID: keys.owner, Name: name}, store, store, keys)
					}
					if rejected && err == nil {
						t.Fatal("deleted or invalid account accepted mutation")
					}
					if !rejected && err != nil {
						t.Fatal(err)
					}
					saved := resetOutcomeFacts(t, store, kind)
					if rejected {
						if len(saved) != 0 || keys.calls != 0 {
							t.Fatal("deletion reached key access or persisted mutation")
						}
						return
					}
					if len(saved) != 1 {
						t.Fatal("active account mutation was not saved")
					}
					writeContext, err := store.GetWriteContext(t.Context(), saved[0].WriteID)
					if err != nil || len(writeContext.Consistency) != 1 || len(writeContext.Consistency[0].Query.Criteria) != expectedCriteria {
						t.Fatal("mutation lost combined deletion observation")
					}
					found := false
					for _, c := range writeContext.Consistency[0].Query.Criteria {
						for _, tag := range c.Tags {
							if tag.Key == eventstore.EventTypeField && tag.Value == AccountDeleted {
								found = true
							}
						}
					}
					if !found {
						t.Fatal("persisted consistency guard omitted deletion")
					}
				})
			}
		})
	}
}
func TestRemainingAuthMutationsConflictWithConcurrentDeletion(t *testing.T) {
	for _, intent := range []string{"assign password", "rename"} {
		t.Run(intent, func(t *testing.T) {
			root, keys := nameCommandFixture(t, "Alice", "Example")
			store := eventstoretest.New(t, root)
			read := false
			retriever := resetLatestFunc(func(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
				if read {
					t.Fatal("authority reloaded instead of guarding original read")
				}
				read = true
				latest, err := store.GetLatestByCriteria(ctx, criteria)
				if err != nil {
					return latest, err
				}
				deleted := mustAuthEvent(NewAccountDeletedEvent(uuidv7.NewString(), time.Now(), keys.owner, keys.owner, nil))
				_, err = store.SaveEventsV2(ctx, []eventstore.DomainEvent{deleted}, nil)
				return latest, err
			})
			var err error
			if intent == "assign password" {
				err = SetPasswordCommandHandler(t.Context(), SetPasswordCommand{PasswordChangedID: uuidv7.NewString(), UserRegisteredID: keys.owner, PasswordHash: testAuthHash(t)}, store, retriever)
			} else {
				_, err = UpdateUserNameCommandHandler(t.Context(), UpdateUserNameCommand{UserRegisteredID: keys.owner, Name: "New Name"}, store, retriever, keys)
			}
			if !errors.Is(err, eventstore.ErrConflict) {
				t.Fatalf("concurrent deletion did not invalidate append: %v", err)
			}
			if len(resetOutcomeFacts(t, store, PasswordChanged)) != 0 || len(resetOutcomeFacts(t, store, UserNameChanged)) != 0 {
				t.Fatal("mutation saved after conflicting deletion")
			}
		})
	}
}
func TestPasswordAssignmentHistoricalRetryAfterDeletion(t *testing.T) {
	owner, operation := uuidv7.NewString(), uuidv7.NewString()
	hash := testAuthHash(t)
	root := registeredAuthEvent(t, owner)
	applied := mustAuthEvent(NewPasswordChangedEvent(operation, time.Now(), owner, hash, nil))
	newer := mustAuthEvent(NewPasswordChangedEvent(uuidv7.NewString(), time.Now(), owner, testAuthHash(t), nil))
	deleted := mustAuthEvent(NewAccountDeletedEvent(uuidv7.NewString(), time.Now(), owner, owner, nil))
	store := eventstoretest.New(t, root, applied, newer, deleted)
	command := SetPasswordCommand{PasswordChangedID: operation, UserRegisteredID: owner, PasswordHash: hash}
	for range 2 {
		if err := SetPasswordCommandHandler(t.Context(), command, store, store); err != nil {
			t.Fatalf("historical receipt was rejected after deletion: %v", err)
		}
	}
	if len(resetOutcomeFacts(t, store, PasswordChanged)) != 2 {
		t.Fatal("historical retry reapplied password")
	}
	command.PasswordHash = testAuthHash(t)
	if err := SetPasswordCommandHandler(t.Context(), command, store, store); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("deletion bypassed content-bound operation identity: %v", err)
	}
	// A matching fact recorded after deletion cannot count as a valid historical effect.
	invalid := eventstoretest.New(t, root, deleted, applied)
	command.PasswordHash = hash
	if err := SetPasswordCommandHandler(t.Context(), command, invalid, invalid); !errors.Is(err, eventstore.ErrInvalidEvent) {
		t.Fatalf("accepted an effect recorded after deletion: %v", err)
	}
}
