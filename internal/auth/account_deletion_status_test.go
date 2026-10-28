package auth

import (
	"errors"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"testing"
	"time"
)

func TestAccountDeletionStatusBindsOrderedRequestEffectAndCompletion(t *testing.T) {
	for _, scenario := range []string{"requested", "effective", "complete", "missing effect", "wrong effect owner", "wrong receipt request", "wrong receipt owner", "receipt before effect", "effect before request"} {
		t.Run(scenario, func(t *testing.T) {
			user, request, operation := uuidv7.NewString(), uuidv7.NewString(), uuidv7.NewString()
			requested := mustAuthEvent(NewAccountDeletionRequestedEvent(request, time.Now(), user, user, operation, nil))
			effectOwner := user
			if scenario == "wrong effect owner" {
				effectOwner = uuidv7.NewString()
			}
			effect := mustAuthEvent(NewAccountDeletedEvent(operation, time.Now(), effectOwner, effectOwner, nil))
			receiptRequest, receiptOwner := request, user
			if scenario == "wrong receipt request" {
				receiptRequest = uuidv7.NewString()
			}
			if scenario == "wrong receipt owner" {
				receiptOwner = uuidv7.NewString()
			}
			receipt := mustAuthEvent(NewAccountDeletionCompletedEvent(uuidv7.NewString(), time.Now(), receiptRequest, operation, receiptOwner, receiptOwner, nil))
			facts := []eventstore.DomainEvent{requested}
			switch scenario {
			case "requested":
			case "effective":
				facts = append(facts, effect)
			case "missing effect":
				facts = append(facts, receipt)
			case "receipt before effect":
				facts = append(facts, receipt, effect)
			case "effect before request":
				facts = []eventstore.DomainEvent{effect, requested, receipt}
			default:
				facts = append(facts, effect, receipt)
			}
			store := eventstoretest.New(t, facts...)
			stage, err := ReadAccountDeletionStatus(t.Context(), request, store)
			switch scenario {
			case "requested", "effective":
				if err != nil || stage != "processing" {
					t.Fatalf("premature completion: %s %v", stage, err)
				}
			case "complete":
				if err != nil || stage != "complete" {
					t.Fatalf("completion missing: %s %v", stage, err)
				}
			default:
				if !errors.Is(err, eventstore.ErrInvalidEvent) {
					t.Fatalf("invalid authority accepted: %s %v", stage, err)
				}
			}
			for _, id := range []string{"invalid", uuidv7.NewString()} {
				if _, err := ReadAccountDeletionStatus(t.Context(), id, store); err == nil {
					t.Fatal("missing/invalid identity accepted")
				}
			}
		})
	}
}

func TestAccountDeletionStatusWaitsThroughCleanupFailure(t *testing.T) {
	store, command := deletionWorkflowFixture(t)
	id, err := RequestAccountDeletionCommandHandler(t.Context(), command, store, store)
	if err != nil {
		t.Fatal(err)
	}
	request, err := DecodeAccountDeletionRequestedEvent(resetOutcomeFacts(t, store, AccountDeletionRequested)[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	effects := &deletionFailureEffects{fail: "key"}
	cmd := DeleteAccountCommand{AccountDeletedID: request.OperationID, UserRegisteredID: command.UserRegisteredID, AuthUserID: command.AuthUserID}
	if err := DeleteAccountCommandHandler(t.Context(), cmd, effects, effects, store, store); err == nil {
		t.Fatal("expected cleanup failure")
	}
	check := func(want string) {
		t.Helper()
		stage, err := ReadAccountDeletionStatus(t.Context(), id, store)
		if err != nil || stage != want {
			t.Fatalf("stage %s: %s %v", want, stage, err)
		}
	}
	check("processing")
	effects.fail = ""
	if err := DeleteAccountCommandHandler(t.Context(), cmd, effects, effects, store, store); err != nil {
		t.Fatal(err)
	}
	check("processing")
	if err := CompleteAccountDeletionCommandHandler(t.Context(), CompleteAccountDeletionCommand{AccountDeletedID: request.OperationID}, store, store); err != nil {
		t.Fatal(err)
	}
	check("complete")
}
