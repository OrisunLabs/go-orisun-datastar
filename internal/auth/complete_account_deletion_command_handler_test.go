package auth

import (
	"context"
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

func TestDeletionCompletionProvesExactEffectiveFact(t *testing.T) {
	for _, scenario := range []string{"matching", "direct deletion", "missing effect", "earlier effect", "foreign effect owner", "foreign effect auth user", "already completed", "foreign receipt request", "foreign receipt user", "foreign receipt auth user", "receipt without request", "later unrelated request"} {
		t.Run(scenario, func(t *testing.T) {
			owner, foreign, request, operation := uuidv7.NewString(), uuidv7.NewString(), uuidv7.NewString(), uuidv7.NewString()
			proposal := mustAuthEvent(NewAccountDeletionRequestedEvent(request, time.Now(), owner, owner, operation, nil))
			authID, userID := owner, owner
			if scenario == "foreign effect owner" {
				userID = foreign
			}
			if scenario == "foreign effect auth user" {
				authID = foreign
			}
			effect := mustAuthEvent(NewAccountDeletedEvent(operation, time.Now(), authID, userID, nil))
			facts := []eventstore.DomainEvent{proposal, effect}
			reject := false
			already := false
			switch scenario {
			case "direct deletion":
				facts = []eventstore.DomainEvent{effect}
			case "missing effect":
				facts = []eventstore.DomainEvent{proposal}
				reject = true
			case "earlier effect":
				facts = []eventstore.DomainEvent{effect, proposal}
				reject = true
			case "foreign effect owner", "foreign effect auth user":
				reject = true
			case "already completed", "foreign receipt request", "foreign receipt user", "foreign receipt auth user", "receipt without request":
				receiptRequest, receiptUser, receiptAuth := request, owner, owner
				if scenario == "foreign receipt request" {
					receiptRequest = foreign
					reject = true
				}
				if scenario == "foreign receipt user" {
					receiptUser = foreign
					reject = true
				}
				if scenario == "foreign receipt auth user" {
					receiptAuth = foreign
					reject = true
				}
				receipt := mustAuthEvent(NewAccountDeletionCompletedEvent(uuidv7.NewString(), time.Now(), receiptRequest, operation, receiptAuth, receiptUser, nil))
				facts = append(facts, receipt)
				already = true
				if scenario == "receipt without request" {
					facts = []eventstore.DomainEvent{effect, receipt}
					reject = true
				}
			case "later unrelated request":
				facts = append(facts, mustAuthEvent(NewAccountDeletionRequestedEvent(uuidv7.NewString(), time.Now(), foreign, owner, uuidv7.NewString(), nil)))
			}
			store := eventstoretest.New(t, facts...)
			before := len(resetOutcomeFacts(t, store, AccountDeletionCompleted))
			command := CompleteAccountDeletionCommand{AccountDeletedID: operation}
			err := CompleteAccountDeletionCommandHandler(t.Context(), command, store, store)
			if reject && err == nil {
				t.Fatal("invalid effect or ancestry completed workflow")
			}
			if !reject && err != nil {
				t.Fatal(err)
			}
			expected := before
			if !reject && !already && scenario != "direct deletion" {
				expected++
			}
			if len(resetOutcomeFacts(t, store, AccountDeletionCompleted)) != expected {
				t.Fatal("unexpected completion count")
			}
			if !reject {
				if err := CompleteAccountDeletionCommandHandler(t.Context(), command, store, store); err != nil {
					t.Fatal(err)
				}
				if len(resetOutcomeFacts(t, store, AccountDeletionCompleted)) != expected {
					t.Fatal("retry duplicated completion")
				}
			}
		})
	}
}
