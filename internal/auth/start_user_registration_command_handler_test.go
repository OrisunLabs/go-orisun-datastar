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

func startCandidateIDs(t *testing.T, criteria []eventstore.Criterion) (string, string) {
	t.Helper()
	if len(criteria) != 5 {
		t.Fatalf("want one five-criterion authority read; got %d", len(criteria))
	}
	var registrationID, userID string
	for _, c := range criteria {
		kind, id := "", ""
		for _, tag := range c.Tags {
			if tag.Key == eventstore.EventTypeField {
				kind = tag.Value
			}
			if tag.Key == "eventId" {
				id = tag.Value
			}
		}
		if id != "" && kind == UserRegistrationStarted {
			registrationID = id
		}
		if id != "" && kind == UserRegistered {
			userID = id
		}
	}
	if !validAuthID(registrationID) || !validAuthID(userID) || registrationID == userID {
		t.Fatal("fresh identities were not allocated before the authority read")
	}
	return registrationID, userID
}
func startCollisionFact(t *testing.T, kind, registrationID, userID string) eventstore.DomainEvent {
	t.Helper()
	key, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	hash := testAuthHash(t)
	switch kind {
	case "pending identity":
		return mustAuthEvent(NewUserRegistrationStartedEvent(registrationID, uuidv7.NewString(), "other-user", "other@example.com", "Other", "User", 1990, hash, time.Now(), key, nil))
	case "accepted user identity":
		return mustAuthEvent(NewUserRegisteredEvent(userID, "other-user", "other@example.com", "Other", "User", 1990, hash, time.Unix(1, 0).UTC(), time.Unix(1, 0).UTC(), key, nil))
	case "planned user identity":
		return mustAuthEvent(NewUserRegistrationStartedEvent(uuidv7.NewString(), userID, "other-user", "other@example.com", "Other", "User", 1990, hash, time.Now(), key, nil))
	default:
		t.Fatal("unknown collision kind")
		return eventstore.DomainEvent{}
	}
}
func TestRegistrationStartPersistsItsActualCombinedObservation(t *testing.T) {
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	var observed eventstore.ConsistencyObservation
	reads := 0
	retriever := resetLatestFunc(func(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
		reads++
		if reads != 1 {
			t.Fatal("known criteria were split across reads")
		}
		startCandidateIDs(t, criteria)
		latest, err := store.GetLatestByCriteria(ctx, criteria)
		observed = eventstore.ConsistencyObservation{Query: eventstore.Query{Criteria: criteria}, Position: latest.ContextPosition}
		return latest, err
	})
	result, err := StartUserRegistrationCommandHandler(t.Context(), StartUserRegistrationCommand{Username: "new-user", Email: "new@example.com", Password: "secret123"}, store, retriever, keys)
	if err != nil {
		t.Fatal(err)
	}
	facts := resetOutcomeFacts(t, store, UserRegistrationStarted)
	if len(facts) != 1 || reads != 1 {
		t.Fatal("start did not save from a single authority read")
	}
	registrationID, userID := startCandidateIDs(t, observed.Query.Criteria)
	if result.UserRegistrationStartedID != registrationID || result.UserID != userID {
		t.Fatal("saved identities differ from observed collision inventory")
	}
	writeContext, err := store.GetWriteContext(t.Context(), facts[0].WriteID)
	if err != nil || len(writeContext.Consistency) != 1 || !equalObservations(writeContext.Consistency, []eventstore.ConsistencyObservation{observed}) {
		t.Fatalf("persisted consistency differs from actual read: %+v %v", writeContext, err)
	}
}
func TestRegistrationStartRejectsExistingCollisionsBeforeKeyCreation(t *testing.T) {
	for _, kind := range []string{"pending identity", "accepted user identity", "planned user identity"} {
		t.Run(kind, func(t *testing.T) {
			store := eventstoretest.New(t)
			keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
			retriever := resetLatestFunc(func(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
				registrationID, userID := startCandidateIDs(t, criteria)
				collision := startCollisionFact(t, kind, registrationID, userID)
				if _, err := store.SaveEventsV2(ctx, []eventstore.DomainEvent{collision}, nil); err != nil {
					return eventstore.LatestByCriteriaResult{}, err
				}
				return store.GetLatestByCriteria(ctx, criteria)
			})
			result, err := StartUserRegistrationCommandHandler(t.Context(), StartUserRegistrationCommand{Username: "new-user", Email: "new@example.com", Password: "secret123"}, store, retriever, keys)
			if !errors.Is(err, eventstore.ErrConflict) || result.UserID != "" || result.UserRegistrationStartedID != "" || len(keys.keys) != 0 {
				t.Fatalf("existing collision reached key creation or start persistence: %v", err)
			}
			expected := 1
			if kind == "accepted user identity" {
				expected = 0
			}
			if len(resetOutcomeFacts(t, store, UserRegistrationStarted)) != expected {
				t.Fatal("collision appended a new proposal")
			}
		})
	}
}
func TestRegistrationStartGuardsLateCollisions(t *testing.T) {
	for _, kind := range []string{"pending identity", "accepted user identity", "planned user identity"} {
		t.Run(kind, func(t *testing.T) {
			store := eventstoretest.New(t)
			keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
			reads := 0
			retriever := resetLatestFunc(func(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
				reads++
				if reads != 1 {
					t.Fatal("snapshot reloaded instead of guarded")
				}
				registrationID, userID := startCandidateIDs(t, criteria)
				latest, err := store.GetLatestByCriteria(ctx, criteria)
				if err != nil {
					return latest, err
				}
				_, err = store.SaveEventsV2(ctx, []eventstore.DomainEvent{startCollisionFact(t, kind, registrationID, userID)}, nil)
				return latest, err
			})
			result, err := StartUserRegistrationCommandHandler(t.Context(), StartUserRegistrationCommand{Username: "new-user", Email: "new@example.com", Password: "secret123"}, store, retriever, keys)
			if !errors.Is(err, eventstore.ErrConflict) || result.UserID != "" || result.UserRegistrationStartedID != "" {
				t.Fatalf("late collision did not invalidate append: %v", err)
			}
			expected := 1
			if kind == "accepted user identity" {
				expected = 0
			}
			if len(resetOutcomeFacts(t, store, UserRegistrationStarted)) != expected {
				t.Fatal("start persisted after colliding write")
			}
		})
	}
}

func TestRegistrationStartGuardsAcceptedUsernameAndEmail(t *testing.T) {
	for _, field := range []string{"username", "email"} {
		for _, phase := range []string{"existing", "after read"} {
			t.Run(field+"/"+phase, func(t *testing.T) {
				key, err := protectedpii.GenerateSubjectDataKey()
				if err != nil {
					t.Fatal(err)
				}
				username, email := "other-user", "other@example.com"
				if field == "username" {
					username = "new-user"
				} else {
					email = "new@example.com"
				}
				accepted := mustAuthEvent(NewUserRegisteredEvent(uuidv7.NewString(), username, email, "Other", "User", 1990, testAuthHash(t), time.Unix(1, 0).UTC(), time.Unix(1, 0).UTC(), key, nil))
				store := eventstoretest.New(t)
				if phase == "existing" {
					if _, err := store.SaveEventsV2(t.Context(), []eventstore.DomainEvent{accepted}, nil); err != nil {
						t.Fatal(err)
					}
				}
				keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
				reads := 0
				retriever := resetLatestFunc(func(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
					reads++
					if reads != 1 {
						t.Fatal("admission snapshot reloaded")
					}
					startCandidateIDs(t, criteria)
					latest, err := store.GetLatestByCriteria(ctx, criteria)
					if err != nil {
						return latest, err
					}
					if phase == "after read" {
						_, err = store.SaveEventsV2(ctx, []eventstore.DomainEvent{accepted}, nil)
					}
					return latest, err
				})
				result, err := StartUserRegistrationCommandHandler(t.Context(), StartUserRegistrationCommand{Username: "new-user", Email: "new@example.com", Password: "secret123"}, store, retriever, keys)
				if err == nil || result.UserID != "" || len(resetOutcomeFacts(t, store, UserRegistrationStarted)) != 0 {
					t.Fatal("accepted identity was bypassed")
				}
				if phase == "existing" && len(keys.keys) != 0 {
					t.Fatal("duplicate accepted identity reached key creation")
				}
				if phase == "after read" && !errors.Is(err, eventstore.ErrConflict) {
					t.Fatalf("accepted-identity race did not invalidate append: %v", err)
				}
			})
		}
	}
}
