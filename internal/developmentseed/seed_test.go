package developmentseed

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/profile"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/todo"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

type memoryKeys map[string]protectedpii.SubjectDataKey

func (k memoryKeys) GetOrCreateSubjectDataKey(ctx context.Context, id string) (protectedpii.SubjectDataKey, error) {
	if key, ok := k[id]; ok {
		return key, nil
	}
	key, err := protectedpii.GenerateSubjectDataKey()
	if err == nil {
		k[id] = key
	}
	return key, err
}
func (k memoryKeys) GetSubjectDataKey(ctx context.Context, id string) (protectedpii.SubjectDataKey, bool, error) {
	key, ok := k[id]
	return key, ok, nil
}
func (k memoryKeys) DestroySubjectKey(ctx context.Context, id string) error {
	delete(k, id)
	return nil
}
func allFacts(t *testing.T, s eventstore.Retriever) []eventstore.ResolvedEvent {
	t.Helper()
	facts, err := s.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, eventstore.Query{})
	if err != nil {
		t.Fatal(err)
	}
	return facts
}
func TestSeedCreatesPublicHistoriesAndRerunsWithoutWrites(t *testing.T) {
	s := eventstoretest.New(t)
	keys := memoryKeys{}
	first, err := Seed(t.Context(), s, keys)
	if err != nil {
		t.Fatal(err)
	}
	if first.Version != DatasetVersion || len(first.Users) != 2 || first.Users[0].ID == first.Users[1].ID {
		t.Fatal(first)
	}
	facts := allFacts(t, s)
	counts := map[string]int{}
	for _, fact := range facts {
		counts[fact.Event.EventType]++
		seed, ok := fact.Event.Metadata["seed"].(map[string]any)
		if !ok || seed["dataset"] != dataset {
			t.Fatal("missing seed ownership", fact.Event.Metadata)
		}
		if audit, ok := fact.Event.Metadata["audit"].(map[string]any); !ok || audit["source"] != "system" {
			t.Fatal("missing command audit")
		}
		guard, err := s.GetWriteContext(t.Context(), fact.WriteID)
		if err != nil || len(guard.Consistency) == 0 {
			t.Fatal("missing persisted command guards", guard, err)
		}
		for _, observation := range guard.Consistency {
			for _, c := range observation.Query.Criteria {
				typed := false
				for _, tag := range c.Tags {
					typed = typed || tag.Key == eventstore.EventTypeField
				}
				if !typed {
					t.Fatal("untyped consistency criterion")
				}
			}
		}
	}
	want := map[string]int{auth.UserRegistrationStarted: 2, auth.EmailVerificationOTPGenerated: 2, auth.EmailVerificationOTPValidated: 2, auth.UserRegistered: 2, auth.UserRegistrationCompleted: 2, profile.ProfileBioUpdated: 2, todo.TodoCreated: 8, todo.TodoRenamed: 2, todo.TodoCompleted: 6, todo.TodoReopened: 2, todo.TodoDeleted: 2}
	if !reflect.DeepEqual(counts, want) {
		t.Fatal(counts)
	}
	second, err := Seed(t.Context(), s, keys)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || len(allFacts(t, s)) != len(facts) {
		t.Fatal("rerun changed identities/history")
	}
	for _, user := range first.Users {
		if len(user.Todos) != 4 || user.Todos[0].Completed || !user.Todos[1].Completed || user.Todos[2].Completed || !user.Todos[3].Deleted {
			t.Fatal(user.Todos)
		}
	}
}

type interruptStore struct {
	Store
	kind string
}

func (s interruptStore) SaveEventsV2(ctx context.Context, events []eventstore.DomainEvent, guards []eventstore.ConsistencyObservation) (eventstore.WriteResult, error) {
	for _, e := range events {
		if e.EventType == s.kind {
			return eventstore.WriteResult{}, errors.New("interrupted seed")
		}
	}
	return s.Store.SaveEventsV2(ctx, events, guards)
}
func TestSeedResumesIncompleteRegistrationAndTodoHistory(t *testing.T) {
	for _, kind := range []string{auth.UserRegistered, todo.TodoReopened} {
		t.Run(kind, func(t *testing.T) {
			s := eventstoretest.New(t)
			keys := memoryKeys{}
			if _, err := Seed(t.Context(), interruptStore{s, kind}, keys); err == nil {
				t.Fatal("expected interruption")
			}
			before := allFacts(t, s)
			result, err := Seed(t.Context(), s, keys)
			if err != nil {
				t.Fatal(err)
			}
			after := allFacts(t, s)
			for i := range before {
				if before[i].Event.EventID != after[i].Event.EventID {
					t.Fatal("partial history replaced")
				}
			}
			if len(result.Users) != 2 || len(after) != 32 {
				t.Fatal("partial run duplicated facts", len(after))
			}
		})
	}
}
func TestSeedRefusesForeignIdentityWithoutAppending(t *testing.T) {
	s := eventstoretest.New(t)
	keys := memoryKeys{}
	spec := users[0]
	if _, err := auth.StartUserRegistrationCommandHandler(t.Context(), auth.StartUserRegistrationCommand{Username: spec.username, Email: spec.email, FirstName: spec.first, LastName: spec.last, YearOfBirth: 1990, Password: DemoPassword}, s, s, keys); err != nil {
		t.Fatal(err)
	}
	before := len(allFacts(t, s))
	if _, err := Seed(t.Context(), s, keys); err == nil {
		t.Fatal("foreign identity adopted")
	}
	if len(allFacts(t, s)) != before {
		t.Fatal("rejection appended history")
	}
}
func TestSeedPreservesUserEditsAndMissingKeys(t *testing.T) {
	for _, edit := range []string{"password", "name", "bio", "todo", "missing key", "deleted"} {
		t.Run(edit, func(t *testing.T) {
			s := eventstoretest.New(t)
			keys := memoryKeys{}
			result, err := Seed(t.Context(), s, keys)
			if err != nil {
				t.Fatal(err)
			}
			u := result.Users[0]
			switch edit {
			case "password":
				err = auth.ChangePasswordCommandHandler(t.Context(), auth.ChangePasswordCommand{UserRegisteredID: u.ID, CurrentPassword: DemoPassword, NewPassword: "MyEditedPassword123"}, s, s)
			case "name":
				_, err = auth.UpdateUserNameCommandHandler(t.Context(), auth.UpdateUserNameCommand{UserRegisteredID: u.ID, Name: "Edited Display Name"}, s, s, keys)
			case "deleted":
				retained := keys[u.ID]
				err = auth.DeleteAccountCommandHandler(t.Context(), auth.DeleteAccountCommand{AccountDeletedID: uuidv7.NewString(), UserRegisteredID: u.ID, AuthUserID: u.ID}, noAccountData{}, keys, s, s)
				// An effective deletion must also defeat a stale external key cache.
				keys[u.ID] = retained
			case "bio":
				err = profile.UpdateProfileBioCommandHandler(t.Context(), profile.UpdateProfileBioCommand{UserID: u.ID, Bio: "My own biography"}, s, s, keys)
			case "todo":
				err = todo.RenameTodosCommandHandler(t.Context(), todo.RenameTodosCommand{UserID: u.ID, Operations: map[string]todo.TodoRename{u.Todos[0].ID: {EventID: uuidv7.NewString(), ExpectedTitle: u.Todos[0].Title, Title: "My own title"}}}, s, s)
			case "missing key":
				delete(keys, u.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			before := len(allFacts(t, s))
			if _, err := Seed(t.Context(), s, keys); err == nil {
				t.Fatal("seed restored edited data")
			}
			if len(allFacts(t, s)) != before {
				t.Fatal("rejected seed appended history")
			}
		})
	}
}

type noAccountData struct{}

func (noAccountData) DeleteAccountData(context.Context, string) error { return nil }
