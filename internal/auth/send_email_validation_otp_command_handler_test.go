package auth

import (
	"context"
	"errors"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"strings"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/email"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

func TestSendEmailValidationOTPOwnsContextAndSkipsRedelivery(t *testing.T) {
	store, registration, keys := pendingEmailFixture(t)
	generated, err := GenerateEmailVerificationOTPCommandHandler(t.Context(), GenerateEmailVerificationOTPCommand{UserRegistrationStartedID: registration.UserRegistrationStartedID}, store, store)
	if err != nil {
		t.Fatal(err)
	}
	sender := &commandTestEmailSender{}
	command := SendEmailValidationOTPCommand{UserRegistrationStartedID: registration.UserRegistrationStartedID, EmailVerificationOTPGeneratedID: generated.EmailVerificationOTPGeneratedID}
	for range 2 {
		if err := SendEmailValidationOTPCommandHandler(t.Context(), command, store, store, sender, keys); err != nil {
			t.Fatal(err)
		}
	}
	if len(sender.messages) != 1 || sender.messages[0].To != "pending@example.com" || !strings.Contains(sender.messages[0].Body, generated.Code) {
		t.Fatal("OTP redelivery or context changed")
	}
	occurrences, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPSent}, {Key: ScopeEmailVerificationOTPGeneratedIDField, Value: generated.EmailVerificationOTPGeneratedID}}}}})
	if err != nil || len(occurrences) != 1 {
		t.Fatalf("sent outcomes=%d error=%v", len(occurrences), err)
	}
	context, err := store.GetWriteContext(t.Context(), occurrences[0].WriteID)
	if err != nil || len(context.Consistency) != 1 || len(context.Consistency[0].Query.Criteria) != 3 {
		t.Fatalf("OTP observations=%#v error=%v", context, err)
	}
}

func emailCommandFixture(t *testing.T) (*eventstore.EmbeddedOrisun, views.User, *authTestKeys) {
	t.Helper()
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	registered, err := RegisterUserCommandHandler(t.Context(), RegisterUserCommand{Username: "email-user", Email: "email@example.com", UserRegisteredID: uuidv7.NewString(), PasswordHash: testAuthHash(t), EmailVerifiedAt: time.Unix(1, 0).UTC()}, store, store, keys)
	if err != nil {
		t.Fatal(err)
	}
	return store, views.User{UserRegisteredID: registered.UserRegisteredID, Email: registered.Email}, keys
}

type commandTestEmailSender struct {
	messages []email.Message
	fail     bool
}

func (s *commandTestEmailSender) Send(_ context.Context, message email.Message) error {
	if s.fail {
		return errors.New("injected email failure")
	}
	s.messages = append(s.messages, message)
	return nil
}

func pendingEmailFixture(t *testing.T) (*eventstore.EmbeddedOrisun, StartUserRegistrationResult, *authTestKeys) {
	t.Helper()
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	registration, err := StartUserRegistrationCommandHandler(t.Context(), StartUserRegistrationCommand{Username: "pending-user", Email: "pending@example.com", Password: "secret123"}, store, store, keys)
	if err != nil {
		t.Fatal(err)
	}
	return store, registration, keys
}
