package auth

import (
	"errors"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func TestPasswordResetStatusRejectsUnboundOrUnorderedReceipts(t *testing.T) {
	for _, scenario := range []string{"valid", "missing effect", "wrong owner", "receipt before effect", "deleted"} {
		t.Run(scenario, func(t *testing.T) {
			user, request, authorization, operation := uuidv7.NewString(), uuidv7.NewString(), uuidv7.NewString(), uuidv7.NewString()
			hash := testAuthHash(t)
			a := mustAuthEvent(NewPasswordResetAuthorizedEvent(authorization, operation, request, user, hash, time.Now(), nil))
			effectOwner := user
			if scenario == "wrong owner" {
				effectOwner = uuidv7.NewString()
			}
			effect := mustAuthEvent(NewPasswordChangedEvent(operation, time.Now(), effectOwner, hash, nil))
			receipt := mustAuthEvent(NewPasswordResetCompletedEvent(uuidv7.NewString(), time.Now(), request, user, authorization, operation, nil))
			facts := []eventstore.DomainEvent{a}
			if scenario == "receipt before effect" {
				facts = append(facts, receipt, effect)
			} else {
				if scenario != "missing effect" {
					facts = append(facts, effect)
				}
				facts = append(facts, receipt)
			}
			if scenario == "deleted" {
				facts = append(facts, mustAuthEvent(NewAccountDeletedEvent(uuidv7.NewString(), time.Now(), user, user, nil)))
			}
			store := eventstoretest.New(t, facts...)
			status, err := ReadPasswordResetStatus(t.Context(), authorization, store)
			if scenario == "valid" || scenario == "deleted" {
				want := "complete"
				if scenario == "deleted" {
					want = "unavailable"
				}
				if err != nil || status.Stage != want {
					t.Fatalf("%+v %v", status, err)
				}
			} else if !errors.Is(err, eventstore.ErrInvalidEvent) {
				t.Fatalf("invalid receipt accepted: %+v %v", status, err)
			}
			if _, err := ReadPasswordResetStatus(t.Context(), uuidv7.NewString(), store); !errors.Is(err, eventstore.ErrNotFound) {
				t.Fatal("missing authorization accepted")
			}
		})
	}
}
