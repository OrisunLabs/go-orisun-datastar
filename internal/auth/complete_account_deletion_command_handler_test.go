package auth

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

type deletionIdentityEffects struct{ deleted, destroyed []string }

func (e *deletionIdentityEffects) DeleteAccountData(_ context.Context, id string) error {
	e.deleted = append(e.deleted, id)
	return nil
}
func (e *deletionIdentityEffects) DestroySubjectKey(_ context.Context, id string) error {
	e.destroyed = append(e.destroyed, id)
	return nil
}
func deletionCompletionFacts(t *testing.T, store eventstore.Retriever) []eventstore.ResolvedEvent {
	t.Helper()
	facts, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeleted}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return facts
}
func TestDeletionCompletionBindsRequestIdentityBeforeEffects(t *testing.T) {
	for _, scenario := range []string{"matching identity", "foreign command auth user", "foreign command registered user", "missing request", "foreign completion auth user", "foreign completion registered user", "already completed", "already completed with foreign command", "later request exists", "malformed request"} {
		t.Run(scenario, func(t *testing.T) {
			owner, foreign, request := uuidv7.NewString(), uuidv7.NewString(), uuidv7.NewString()
			requested := mustAuthEvent(NewAccountDeletionRequestedEvent(request, time.Now(), owner, owner, nil))
			facts := []eventstore.DomainEvent{requested}
			command := CompleteAccountDeletionCommand{AccountDeletionRequestedID: request, UserRegisteredID: owner, AuthUserID: owner}
			reject, alreadyCompleted := false, false
			switch scenario {
			case "foreign command auth user":
				command.AuthUserID = foreign
				reject = true
			case "foreign command registered user":
				command.UserRegisteredID = foreign
				reject = true
			case "missing request":
				facts = nil
				reject = true
			case "foreign completion auth user", "foreign completion registered user", "already completed", "already completed with foreign command":
				authID, userID := owner, owner
				if scenario == "foreign completion auth user" {
					authID = foreign
					reject = true
				}
				if scenario == "foreign completion registered user" {
					userID = foreign
					reject = true
				}
				if scenario == "already completed with foreign command" {
					command.AuthUserID = foreign
					reject = true
				}
				facts = append(facts, mustAuthEvent(NewAccountDeletedEvent(uuidv7.NewString(), time.Now(), request, authID, userID, nil)))
				alreadyCompleted = true
			case "later request exists":
				facts = append(facts, mustAuthEvent(NewAccountDeletionRequestedEvent(uuidv7.NewString(), time.Now(), foreign, owner, nil)))
			case "malformed request":
				facts[0].Data = eventstore.MustData(facts[0].Data)
				facts[0].Data["extra"] = true
				reject = true
			}
			store := eventstoretest.New(t, facts...)
			before := len(deletionCompletionFacts(t, store))
			effects := &deletionIdentityEffects{}
			err := CompleteAccountDeletionCommandHandler(t.Context(), command, effects, effects, store, store)
			if reject && err == nil {
				t.Fatal("mismatched deletion authority accepted")
			}
			if !reject && err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(scenario, "foreign completion") && err != nil && !strings.Contains(err.Error(), "scope does not match") {
				t.Fatalf("completion ancestry was not validated: %v", err)
			}
			expectedEffects := 1
			if reject || alreadyCompleted {
				expectedEffects = 0
			}
			if len(effects.deleted) != expectedEffects || len(effects.destroyed) != expectedEffects {
				t.Fatalf("unexpected destructive effects: %+v", effects)
			}
			completed := deletionCompletionFacts(t, store)
			if len(completed) != before+expectedEffects {
				t.Fatal("rejected or repeated command saved a completion")
			}
			if expectedEffects == 1 {
				if effects.deleted[0] != owner || effects.destroyed[0] != owner {
					t.Fatal("side effects targeted a different owner")
				}
				value, err := DecodeAccountDeletedEvent(completed[len(completed)-1].Event)
				if err != nil {
					t.Fatal(err)
				}
				if value.AuthUserID != owner || value.Scopes.UserRegisteredID != owner || value.Scopes.AccountDeletionRequestedID != request {
					t.Fatal("completion differs from request identity")
				}
				// Exact delivery retries must not repeat destruction or append another fact.
				if err := CompleteAccountDeletionCommandHandler(t.Context(), command, effects, effects, store, store); err != nil {
					t.Fatal(err)
				}
				if len(effects.deleted) != 1 || len(effects.destroyed) != 1 || len(deletionCompletionFacts(t, store)) != before+1 {
					t.Fatal("retry repeated destructive effects")
				}
			}
		})
	}
}
