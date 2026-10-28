package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

type emailReplayKeys struct {
	SubjectPiiKeyReader
	calls int
	err   error
}

func (k *emailReplayKeys) GetSubjectDataKey(ctx context.Context, id string) (protectedpii.SubjectDataKey, bool, error) {
	k.calls++
	if k.err != nil {
		return protectedpii.SubjectDataKey{}, false, k.err
	}
	return k.SubjectPiiKeyReader.GetSubjectDataKey(ctx, id)
}
func TestRecordedEmailReplayDoesNotNeedSubjectKey(t *testing.T) {
	for _, kind := range []string{"OTP", "reset"} {
		for _, scenario := range []string{"erased key", "unavailable key service", "unsent erased key", "malformed source", "malformed receipt"} {
			t.Run(kind+"/"+scenario, func(t *testing.T) {
				store, user, keys := emailCommandFixture(t)
				subjectID := user.UserRegisteredID
				if kind == "OTP" {
					var registration StartUserRegistrationResult
					store, registration, keys = pendingEmailFixture(t)
					user.UserRegisteredID = registration.UserRegistrationStartedID
					subjectID = registration.UserID
				}
				sender := &commandTestEmailSender{}
				var invoke func(SubjectPiiKeyReader) error
				sourceType, outcomeType := EmailVerificationOTPGenerated, EmailVerificationOTPSent
				if kind == "OTP" {
					generated, err := GenerateEmailVerificationOTPCommandHandler(t.Context(), GenerateEmailVerificationOTPCommand{UserRegistrationStartedID: user.UserRegisteredID}, store, store)
					if err != nil {
						t.Fatal(err)
					}
					command := SendEmailValidationOTPCommand{UserRegistrationStartedID: user.UserRegisteredID, EmailVerificationOTPGeneratedID: generated.EmailVerificationOTPGeneratedID}
					invoke = func(keys SubjectPiiKeyReader) error {
						return SendEmailValidationOTPCommandHandler(t.Context(), command, store, store, sender, keys)
					}
				} else {
					sourceType, outcomeType = PasswordResetRequested, PasswordResetEmailSent
					requested, err := RequestPasswordResetCommandHandler(t.Context(), RequestPasswordResetCommand{EmailAddress: user.Email}, store, store, keys)
					if err != nil {
						t.Fatal(err)
					}
					command := SendPasswordResetEmailCommand{PasswordResetRequestedID: requested.PasswordResetRequestedID}
					invoke = func(keys SubjectPiiKeyReader) error {
						return SendPasswordResetEmailCommandHandler(t.Context(), command, store, store, sender, "https://app.example.com", keys)
					}
				}
				if scenario != "unsent erased key" {
					if err := invoke(keys); err != nil {
						t.Fatal(err)
					}
				}
				before := len(resetOutcomeFacts(t, store, outcomeType))
				sent := len(sender.messages)
				if scenario == "malformed source" || scenario == "malformed receipt" {
					eventType := sourceType
					if scenario == "malformed receipt" {
						eventType = outcomeType
					}
					facts := resetOutcomeFacts(t, store, eventType)
					corrupt := facts[0].Event
					corrupt.Data = eventstore.MustData(corrupt.Data)
					corrupt.Data["extra"] = true
					if _, err := store.SaveEventsV2(t.Context(), []eventstore.DomainEvent{corrupt}, nil); err != nil {
						t.Fatal(err)
					}
					before = len(resetOutcomeFacts(t, store, outcomeType))
				}
				if err := keys.DestroySubjectKey(t.Context(), subjectID); err != nil {
					t.Fatal(err)
				}
				replayKeys := &emailReplayKeys{SubjectPiiKeyReader: keys}
				if scenario == "unavailable key service" {
					replayKeys.err = errors.New("key service unavailable")
				}
				err := invoke(replayKeys)
				rejected := scenario == "unsent erased key" || scenario == "malformed source" || scenario == "malformed receipt"
				if rejected && err == nil {
					t.Fatal("unsent or malformed source accepted")
				}
				if !rejected && err != nil {
					t.Fatalf("recorded send still needs erased PII: %v", err)
				}
				if scenario == "unsent erased key" {
					if !errors.Is(err, eventstore.ErrNotFound) || replayKeys.calls != 1 {
						t.Fatal("unsent email did not require subject key")
					}
				} else if replayKeys.calls != 0 {
					t.Fatal("recorded or invalid send accessed subject key")
				}
				if len(sender.messages) != sent || len(resetOutcomeFacts(t, store, outcomeType)) != before {
					t.Fatal("replay sent another email or appended another receipt")
				}
			})
		}
	}
}
func TestOTPEmailRejectsDifferentUserBeforeSendingOrSkipping(t *testing.T) {
	for _, alreadySent := range []bool{false, true} {
		t.Run(map[bool]string{false: "unsent", true: "recorded"}[alreadySent], func(t *testing.T) {
			store, registration, keys := pendingEmailFixture(t)
			generated, err := GenerateEmailVerificationOTPCommandHandler(t.Context(), GenerateEmailVerificationOTPCommand{UserRegistrationStartedID: registration.UserRegistrationStartedID}, store, store)
			if err != nil {
				t.Fatal(err)
			}
			command := SendEmailValidationOTPCommand{UserRegistrationStartedID: registration.UserRegistrationStartedID, EmailVerificationOTPGeneratedID: generated.EmailVerificationOTPGeneratedID}
			sender := &commandTestEmailSender{}
			if alreadySent {
				if err := SendEmailValidationOTPCommandHandler(t.Context(), command, store, store, sender, keys); err != nil {
					t.Fatal(err)
				}
			}
			other, err := StartUserRegistrationCommandHandler(t.Context(), StartUserRegistrationCommand{Username: "other-user", Email: "other@example.com", Password: "secret123"}, store, store, keys)
			if err != nil {
				t.Fatal(err)
			}
			command.UserRegistrationStartedID = other.UserRegistrationStartedID
			before, sent := len(resetOutcomeFacts(t, store, EmailVerificationOTPSent)), len(sender.messages)
			reader := &emailReplayKeys{SubjectPiiKeyReader: keys}
			if err := SendEmailValidationOTPCommandHandler(t.Context(), command, store, store, sender, reader); err == nil {
				t.Fatal("OTP generation belonging to a different user accepted")
			}
			if reader.calls != 0 || len(sender.messages) != sent || len(resetOutcomeFacts(t, store, EmailVerificationOTPSent)) != before {
				t.Fatal("foreign OTP caused effects or key access")
			}
		})
	}
}
