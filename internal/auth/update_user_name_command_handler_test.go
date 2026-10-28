package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

// This port deliberately offers no key creation or destruction capability.
type nameReadKeys struct {
	key     protectedpii.SubjectDataKey
	owner   string
	calls   int
	missing bool
}

func (k *nameReadKeys) GetSubjectDataKey(_ context.Context, id string) (protectedpii.SubjectDataKey, bool, error) {
	k.calls++
	return k.key, !k.missing && id == k.owner, nil
}
func nameCommandFixture(t *testing.T, first, last string) (eventstore.DomainEvent, *nameReadKeys) {
	t.Helper()
	owner := uuidv7.NewString()
	key, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	root := mustAuthEvent(NewUserRegisteredEvent(owner, "name-user", "name@example.com", first, last, 1990, testAuthHash(t), time.Unix(1, 0).UTC(), time.Unix(1, 0).UTC(), key, nil))
	return root, &nameReadKeys{key: key, owner: owner}
}
func TestNameCommandUsesEffectiveNameAndSkipsUnchangedInput(t *testing.T) {
	for _, scenario := range []string{"registration full name", "registration username fallback", "latest name"} {
		t.Run(scenario, func(t *testing.T) {
			first, last, current := "Alice", "Example", "Alice Example"
			if scenario == "registration username fallback" {
				first, last, current = "", "", "name-user"
			}
			root, keys := nameCommandFixture(t, first, last)
			facts := []eventstore.DomainEvent{root}
			if scenario == "latest name" {
				current = "Current Name"
				facts = append(facts, mustAuthEvent(NewUserNameChangedEvent(uuidv7.NewString(), current, time.Now(), keys.owner, keys.key, nil)))
			}
			store := eventstoretest.New(t, facts...)
			before := len(resetOutcomeFacts(t, store, UserNameChanged))
			command := UpdateUserNameCommand{UserRegisteredID: keys.owner, Name: "  " + current + "  "}
			result, err := UpdateUserNameCommandHandler(t.Context(), command, store, store, keys)
			if err != nil || !result.Skipped || result.Name != current {
				t.Fatalf("unchanged authority not recognized: %+v %v", result, err)
			}
			if len(resetOutcomeFacts(t, store, UserNameChanged)) != before {
				t.Fatal("unchanged name appended event")
			}
			command.Name = "  New Name  "
			result, err = UpdateUserNameCommandHandler(t.Context(), command, store, store, keys)
			if err != nil || result.Skipped || result.Name != "New Name" {
				t.Fatalf("rename failed: %+v %v", result, err)
			}
			saved := resetOutcomeFacts(t, store, UserNameChanged)
			event, err := DecodeUserNameChangedEvent(saved[len(saved)-1].Event)
			if err != nil {
				t.Fatal(err)
			}
			name, err := protectedpii.FromEnv().DecryptWithDataKey(event.Name, keys.key)
			if err != nil || name != "New Name" || event.Scopes.UserRegisteredID != keys.owner {
				t.Fatal("rename lost normalized input or owner")
			}
			result, err = UpdateUserNameCommandHandler(t.Context(), command, store, store, keys)
			if err != nil || !result.Skipped || len(resetOutcomeFacts(t, store, UserNameChanged)) != before+1 {
				t.Fatal("retry duplicated rename")
			}
			writeContext, err := store.GetWriteContext(t.Context(), saved[len(saved)-1].WriteID)
			if err != nil || len(writeContext.Consistency) != 1 || len(writeContext.Consistency[0].Query.Criteria) != 3 {
				t.Fatal("rename lost exact combined observation")
			}
		})
	}
}
func TestNameCommandRejectsInvalidAuthorityBeforeKeyAccess(t *testing.T) {
	for _, scenario := range []string{"missing registration", "invalid ID", "blank name", "malformed registration", "malformed name", "name before registration"} {
		t.Run(scenario, func(t *testing.T) {
			root, keys := nameCommandFixture(t, "Alice", "Example")
			changed := mustAuthEvent(NewUserNameChangedEvent(uuidv7.NewString(), "Old Name", time.Now(), keys.owner, keys.key, nil))
			facts := []eventstore.DomainEvent{root}
			command := UpdateUserNameCommand{UserRegisteredID: keys.owner, Name: "New Name"}
			switch scenario {
			case "missing registration":
				facts = []eventstore.DomainEvent{changed}
			case "invalid ID":
				command.UserRegisteredID = "invalid"
			case "blank name":
				command.Name = "  "
			case "malformed registration":
				root.Data = eventstore.MustData(root.Data)
				root.Data["extra"] = true
				facts = []eventstore.DomainEvent{root}
			case "malformed name":
				changed.Data = eventstore.MustData(changed.Data)
				changed.Data["extra"] = true
				facts = append(facts, changed)
			case "name before registration":
				facts = []eventstore.DomainEvent{changed, root}
			}
			store := eventstoretest.New(t, facts...)
			before := len(resetOutcomeFacts(t, store, UserNameChanged))
			if _, err := UpdateUserNameCommandHandler(t.Context(), command, store, store, keys); err == nil {
				t.Fatal("invalid authority accepted")
			}
			if keys.calls != 0 || len(resetOutcomeFacts(t, store, UserNameChanged)) != before {
				t.Fatal("invalid authority reached key access or persistence")
			}
		})
	}
}
func TestNameCommandBindsReturnedFactsToRequestedUser(t *testing.T) {
	for _, kind := range []string{UserRegistered, UserNameChanged} {
		t.Run(kind, func(t *testing.T) {
			root, keys := nameCommandFixture(t, "Alice", "Example")
			foreign := uuidv7.NewString()
			var source eventstore.DomainEvent
			if kind == UserRegistered {
				source = mustAuthEvent(NewUserRegisteredEvent(foreign, "foreign-user", "foreign@example.com", "Foreign", "User", 1990, testAuthHash(t), time.Unix(1, 0).UTC(), time.Unix(1, 0).UTC(), keys.key, nil))
			} else {
				source = mustAuthEvent(NewUserNameChangedEvent(uuidv7.NewString(), "Foreign Name", time.Now(), foreign, keys.key, nil))
			}
			store := eventstoretest.New(t, root, source)
			var foreignOccurrence eventstore.ResolvedEvent
			for _, e := range resetOutcomeFacts(t, store, kind) {
				if e.Event.EventID == source.EventID {
					foreignOccurrence = e
				}
			}
			retriever := resetLatestFunc(func(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
				latest, err := store.GetLatestByCriteria(ctx, criteria)
				if err != nil {
					return latest, err
				}
				index := 0
				if kind == UserNameChanged {
					index = 1
				}
				latest.Results[index].Event = &foreignOccurrence
				return latest, nil
			})
			before := len(resetOutcomeFacts(t, store, UserNameChanged))
			if _, err := UpdateUserNameCommandHandler(t.Context(), UpdateUserNameCommand{UserRegisteredID: keys.owner, Name: "New Name"}, store, retriever, keys); !errors.Is(err, eventstore.ErrInvalidEvent) {
				t.Fatalf("foreign authority accepted: %v", err)
			}
			if keys.calls != 0 || len(resetOutcomeFacts(t, store, UserNameChanged)) != before {
				t.Fatal("foreign context reached effects")
			}
		})
	}
}
func TestNameCommandRejectsMismatchedPlaintextCommitment(t *testing.T) {
	root, keys := nameCommandFixture(t, "Alice", "Example")
	changed := mustAuthEvent(NewUserNameChangedEvent(uuidv7.NewString(), "Old Name", time.Now(), keys.owner, keys.key, nil))
	changed.Data = eventstore.MustData(changed.Data)
	wrongHash := protectedpii.FromEnv().BlindIndex(UserNameChangedNameField, "Different Name")
	changed.Data["name"].(map[string]any)["blindIndex"] = wrongHash
	changed.Data[UserNameChangedNameHashField] = wrongHash
	store := eventstoretest.New(t, root, changed)
	if _, err := UpdateUserNameCommandHandler(t.Context(), UpdateUserNameCommand{UserRegisteredID: keys.owner, Name: "New Name"}, store, store, keys); !errors.Is(err, eventstore.ErrInvalidEvent) {
		t.Fatalf("inconsistent plaintext commitment accepted: %v", err)
	}
	if len(resetOutcomeFacts(t, store, UserNameChanged)) != 1 {
		t.Fatal("invalid plaintext produced rename")
	}
}
func TestNameCommandConflictsWithConcurrentRename(t *testing.T) {
	root, keys := nameCommandFixture(t, "Alice", "Example")
	store := eventstoretest.New(t, root)
	read := false
	retriever := resetLatestFunc(func(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
		if read {
			t.Fatal("authority reloaded instead of preserving observation")
		}
		read = true
		latest, err := store.GetLatestByCriteria(ctx, criteria)
		if err != nil {
			return latest, err
		}
		racing := mustAuthEvent(NewUserNameChangedEvent(uuidv7.NewString(), "Racing Name", time.Now(), keys.owner, keys.key, nil))
		_, err = store.SaveEventsV2(ctx, []eventstore.DomainEvent{racing}, nil)
		return latest, err
	})
	if _, err := UpdateUserNameCommandHandler(t.Context(), UpdateUserNameCommand{UserRegisteredID: keys.owner, Name: "New Name"}, store, retriever, keys); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("stale context accepted: %v", err)
	}
	if len(resetOutcomeFacts(t, store, UserNameChanged)) != 1 {
		t.Fatal("conflicting rename persisted")
	}
}
