package auth

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func TestDeleteAccountIndependentAuthorityAndIdempotency(t *testing.T) {
	for _, scenario := range []string{"fresh", "missing registration", "invalid identity", "known operation", "foreign historical owner", "foreign historical auth user", "other prior deletion", "malformed registration", "malformed deletion"} {
		t.Run(scenario, func(t *testing.T) {
			owner, operation := uuidv7.NewString(), uuidv7.NewString()
			command := DeleteAccountCommand{AccountDeletedID: operation, UserRegisteredID: owner, AuthUserID: owner}
			facts := []eventstore.DomainEvent{registeredAuthEvent(t, owner)}
			reject, already := false, false
			switch scenario {
			case "missing registration":
				facts = nil
				reject = true
			case "invalid identity":
				command.AccountDeletedID = owner
				reject = true
			case "malformed registration":
				facts[0].Data = eventstore.MustData(facts[0].Data)
				facts[0].Data["extra"] = true
				reject = true
			case "known operation", "foreign historical owner", "foreign historical auth user", "other prior deletion", "malformed deletion":
				id, userID, authID := operation, owner, owner
				already = true
				if scenario == "foreign historical owner" {
					userID = uuidv7.NewString()
					reject = true
				}
				if scenario == "foreign historical auth user" {
					authID = uuidv7.NewString()
					reject = true
				}
				if scenario == "other prior deletion" {
					id = uuidv7.NewString()
					reject = true
				}
				effect := mustAuthEvent(NewAccountDeletedEvent(id, time.Now(), authID, userID, nil))
				if scenario == "malformed deletion" {
					effect.Data = eventstore.MustData(effect.Data)
					effect.Data["extra"] = true
					reject = true
				}
				facts = append(facts, effect)
			}
			store := eventstoretest.New(t, facts...)
			effects := &deletionIdentityEffects{}
			before := len(resetOutcomeFacts(t, store, AccountDeleted))
			err := DeleteAccountCommandHandler(t.Context(), command, effects, effects, store, store)
			if reject && err == nil {
				t.Fatal("invalid deletion authority accepted")
			}
			if !reject && err != nil {
				t.Fatal(err)
			}
			expected := 0
			if !reject && !already {
				expected = 1
			}
			if len(effects.deleted) != expected || len(effects.destroyed) != expected || len(resetOutcomeFacts(t, store, AccountDeleted)) != before+expected {
				t.Fatal("rejected or historical command repeated effects")
			}
			if expected == 1 {
				event := resetOutcomeFacts(t, store, AccountDeleted)[0].Event
				scopes := eventstore.Scopes(event.Data)
				if _, ok := scopes[AccountDeletionRequestedIDField]; ok {
					t.Fatal("domain fact contains workflow ancestry")
				}
				if effects.deleted[0] != owner || effects.destroyed[0] != owner {
					t.Fatal("destruction targeted wrong owner")
				}
				if err := DeleteAccountCommandHandler(t.Context(), command, effects, effects, store, store); err != nil {
					t.Fatal(err)
				}
				if len(effects.deleted) != 1 || len(effects.destroyed) != 1 {
					t.Fatal("retry repeated cleanup")
				}
			}
		})
	}
}

type deletionFailureEffects struct {
	order []string
	fail  string
}

func (e *deletionFailureEffects) DeleteAccountData(context.Context, string) error {
	e.order = append(e.order, "data")
	if e.fail == "data" {
		return errors.New("data failure")
	}
	return nil
}
func (e *deletionFailureEffects) DestroySubjectKey(context.Context, string) error {
	e.order = append(e.order, "key")
	if e.fail == "key" {
		return errors.New("key failure")
	}
	return nil
}

type deletionFailSaver struct {
	effects *deletionFailureEffects
	store   eventstore.SaverV2
	fail    bool
}

func (s *deletionFailSaver) SaveEventsV2(ctx context.Context, events []eventstore.DomainEvent, observations []eventstore.ConsistencyObservation) (eventstore.WriteResult, error) {
	s.effects.order = append(s.effects.order, "save")
	if s.fail {
		return eventstore.WriteResult{}, errors.New("save failure")
	}
	return s.store.SaveEventsV2(ctx, events, observations)
}
func TestDeleteAccountPreservesCleanupOrderAndRetriesFailures(t *testing.T) {
	for _, stage := range []string{"data", "key", "save"} {
		t.Run(stage, func(t *testing.T) {
			owner := uuidv7.NewString()
			store := eventstoretest.New(t, registeredAuthEvent(t, owner))
			command := DeleteAccountCommand{AccountDeletedID: uuidv7.NewString(), UserRegisteredID: owner, AuthUserID: owner}
			effects := &deletionFailureEffects{fail: stage}
			saver := &deletionFailSaver{effects: effects, store: store, fail: stage == "save"}
			if err := DeleteAccountCommandHandler(t.Context(), command, effects, effects, saver, store); err == nil {
				t.Fatal("failure ignored")
			}
			expected := []string{"data"}
			if stage == "key" {
				expected = append(expected, "key")
			}
			if stage == "save" {
				expected = append(expected, "key", "save")
			}
			if !slices.Equal(effects.order, expected) || len(resetOutcomeFacts(t, store, AccountDeleted)) != 0 {
				t.Fatalf("wrong failure ordering: %v", effects.order)
			}
			effects.order = nil
			effects.fail = ""
			saver.fail = false
			if err := DeleteAccountCommandHandler(t.Context(), command, effects, effects, saver, store); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(effects.order, []string{"data", "key", "save"}) || len(resetOutcomeFacts(t, store, AccountDeleted)) != 1 {
				t.Fatal("retry did not complete in order")
			}
		})
	}
}
