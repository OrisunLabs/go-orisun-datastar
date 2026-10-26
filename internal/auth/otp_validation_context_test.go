package auth

import (
	"strings"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func TestOTPValidationBindsUserAndGeneration(t *testing.T) {
	for _, scenario := range []string{"valid", "already validated", "foreign validation owner", "previous OTP validated", "missing registration", "missing OTP", "incorrect code", "expired OTP", "malformed generation"} {
		t.Run(scenario, func(t *testing.T) {
			owner, foreign, otp := uuidv7.NewString(), uuidv7.NewString(), uuidv7.NewString()
			expires := time.Now().Add(time.Hour)
			if scenario == "expired OTP" {
				expires = time.Now().Add(-time.Hour)
			}
			facts := []eventstore.DomainEvent{}
			if scenario != "missing registration" {
				facts = append(facts, registrationStartedAuthEvent(t, owner))
			}
			if scenario == "previous OTP validated" {
				previous := uuidv7.NewString()
				facts = append(facts, mustAuthEvent(NewEmailVerificationOTPGeneratedEvent(previous, "654321", expires, owner, nil)), mustAuthEvent(NewEmailVerificationOTPValidatedEvent(uuidv7.NewString(), time.Now(), previous, owner, nil)))
			}
			if scenario != "missing OTP" {
				generated := mustAuthEvent(NewEmailVerificationOTPGeneratedEvent(otp, "123456", expires, owner, nil))
				if scenario == "malformed generation" {
					generated.Data = eventstore.MustData(generated.Data)
					generated.Data["extra"] = true
				}
				facts = append(facts, generated)
			}
			if scenario == "already validated" || scenario == "foreign validation owner" {
				validationOwner := owner
				if scenario == "foreign validation owner" {
					validationOwner = foreign
				}
				facts = append(facts, mustAuthEvent(NewEmailVerificationOTPValidatedEvent(uuidv7.NewString(), time.Now(), otp, validationOwner, nil)))
			}
			store := eventstoretest.New(t, facts...)
			before := len(resetOutcomeFacts(t, store, EmailVerificationOTPValidated))
			code := "123456"
			if scenario == "incorrect code" {
				code = "000000"
			}
			err := ValidateEmailVerificationOTPCommandHandler(t.Context(), ValidateEmailVerificationOTPCommand{UserRegistrationStartedID: owner, Code: code}, store, store)
			accepted := scenario == "valid" || scenario == "previous OTP validated" || scenario == "already validated"
			if accepted && err != nil {
				t.Fatal(err)
			}
			if !accepted && err == nil {
				t.Fatal("invalid OTP context accepted")
			}
			if scenario == "foreign validation owner" && !strings.Contains(err.Error(), "scope does not match") {
				t.Fatalf("foreign validation was mistaken for a legitimate prior validation: %v", err)
			}
			after := resetOutcomeFacts(t, store, EmailVerificationOTPValidated)
			expected := before
			if accepted && scenario != "already validated" {
				expected++
			}
			if len(after) != expected {
				t.Fatal("rejected validation appended an outcome")
			}
			if accepted {
				value, err := DecodeEmailVerificationOTPValidatedEvent(after[len(after)-1].Event)
				if err != nil {
					t.Fatal(err)
				}
				if value.Scopes.UserRegistrationStartedID != owner || value.Scopes.EmailVerificationOTPGeneratedID != otp {
					t.Fatal("validation did not reference the selected owner's latest OTP")
				}
			}
		})
	}
}
