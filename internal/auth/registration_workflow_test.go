package auth

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"golang.org/x/crypto/bcrypt"
)

func stagedRegistration(t *testing.T, store *eventstore.EmbeddedOrisun, keys *authTestKeys) StartUserRegistrationResult {
	t.Helper()
	result, err := StartUserRegistrationCommandHandler(t.Context(), StartUserRegistrationCommand{Username: "staged-user", Email: "staged@example.com", Password: "secret123", FirstName: "Staged", LastName: "User", YearOfBirth: 1990}, store, store, keys)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func verifyStagedRegistration(t *testing.T, store *eventstore.EmbeddedOrisun, result StartUserRegistrationResult) {
	t.Helper()
	generated, err := GenerateEmailVerificationOTPCommandHandler(t.Context(), GenerateEmailVerificationOTPCommand{UserRegistrationStartedID: result.UserRegistrationStartedID}, store, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateEmailVerificationOTPCommandHandler(t.Context(), ValidateEmailVerificationOTPCommand{UserRegistrationStartedID: result.UserRegistrationStartedID, Code: generated.Code}, store, store); err != nil {
		t.Fatal(err)
	}
}
func TestRegistrationWorkflowProducesIndependentAcceptedFact(t *testing.T) {
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	result := stagedRegistration(t, store, keys)
	if result.UserID == result.UserRegistrationStartedID {
		t.Fatal("workflow and user identities were reused")
	}
	if len(resetOutcomeFacts(t, store, UserRegistered)) != 0 {
		t.Fatal("starting registration created an accepted user")
	}
	started := resetOutcomeFacts(t, store, UserRegistrationStarted)[0]
	generation := &RegistrationOTPToBeGeneratedEventHandler{saver: store, retriever: store}
	if err := generation.handle(t.Context(), started); err != nil {
		t.Fatal(err)
	}
	if err := generation.handle(t.Context(), started); err != nil {
		t.Fatal(err)
	}
	generated := resetOutcomeFacts(t, store, EmailVerificationOTPGenerated)
	if len(generated) != 1 {
		t.Fatal("registration replay duplicated OTP")
	}
	value, err := DecodeEmailVerificationOTPGeneratedEvent(generated[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	if value.Scopes.UserRegistrationStartedID != result.UserRegistrationStartedID {
		t.Fatal("OTP refers to a user instead of pending registration")
	}
	sender := &commandTestEmailSender{}
	delivery := &EmailValidationOTPToBeSentEventHandler{saver: store, retriever: store, sender: sender, keys: keys}
	for range 2 {
		if err := delivery.handle(t.Context(), generated[0]); err != nil {
			t.Fatal(err)
		}
	}
	if len(sender.messages) != 1 || sender.messages[0].To != "staged@example.com" {
		t.Fatal("pending registration email delivery failed")
	}
	command := ValidateEmailVerificationOTPCommand{UserRegistrationStartedID: result.UserRegistrationStartedID, Code: "invalid"}
	if err := ValidateEmailVerificationOTPCommandHandler(t.Context(), command, store, store); err == nil {
		t.Fatal("wrong OTP accepted")
	}
	if err := FinalizeUserRegistration(t.Context(), result.UserRegistrationStartedID, nil, store, store, keys); !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("unverified workflow could finalize: %v", err)
	}
	command.Code = value.OTPCode
	for range 2 {
		if err := ValidateEmailVerificationOTPCommandHandler(t.Context(), command, store, store); err != nil {
			t.Fatal(err)
		}
	}
	validated := resetOutcomeFacts(t, store, EmailVerificationOTPValidated)
	if len(validated) != 1 || len(resetOutcomeFacts(t, store, UserRegistered)) != 0 {
		t.Fatal("OTP validation prematurely created domain state or duplicated validation")
	}
	reaction := &RegistrationCompletionEventHandler{saver: store, retriever: store, keys: keys}
	if err := reaction.handle(t.Context(), validated[0]); err != nil {
		t.Fatal(err)
	}
	accepted := resetOutcomeFacts(t, store, UserRegistered)
	if len(accepted) != 1 || len(resetOutcomeFacts(t, store, UserRegistrationCompleted)) != 1 {
		t.Fatal("missing accepted fact/workflow completion")
	}
	registered, err := DecodeUserRegisteredEvent(accepted[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	confirmation, err := DecodeEmailVerificationOTPValidatedEvent(validated[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	if registered.UserRegisteredID != result.UserID || registered.EmailVerifiedAt != confirmation.ValidatedAt || registered.RegisteredAt == "" {
		t.Fatal("accepted identity or verification fact missing")
	}
	if _, exists := accepted[0].Event.Data["scopes"]; exists {
		t.Fatal("accepted user depends on workflow ancestry")
	}
	projection := &authProjectionFake{states: map[string]AuthState{}, receipts: map[eventstore.Position]string{}, facts: map[string]VerificationFact{}}
	if err := ProjectBatch(t.Context(), accepted, projection, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	user := projection.states[result.UserID]
	if !user.EmailVerified || user.Verification == nil || user.Name != "Staged User" || user.Email != "staged@example.com" || user.Username != "staged-user" {
		t.Fatal("accepted fact alone cannot reconstruct user")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte("secret123")); err != nil {
		t.Fatal("password missing from accepted fact")
	}
	if err := keys.DestroySubjectKey(t.Context(), result.UserID); err != nil {
		t.Fatal(err)
	}
	if err := reaction.handle(t.Context(), validated[0]); err != nil {
		t.Fatalf("completed workflow needed erased key on retry: %v", err)
	}
	if len(resetOutcomeFacts(t, store, UserRegistered)) != 1 || len(resetOutcomeFacts(t, store, UserRegistrationCompleted)) != 1 {
		t.Fatal("retry duplicated final facts")
	}
}
func TestPendingRegistrationsDoNotReserveDomainIdentity(t *testing.T) {
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	first, second := stagedRegistration(t, store, keys), stagedRegistration(t, store, keys)
	verifyStagedRegistration(t, store, first)
	verifyStagedRegistration(t, store, second)
	if err := FinalizeUserRegistration(t.Context(), first.UserRegistrationStartedID, nil, store, store, keys); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := FinalizeUserRegistration(t.Context(), second.UserRegistrationStartedID, nil, store, store, keys); !errors.Is(err, ErrRegistrationIdentityUnavailable) {
			t.Fatal(err)
		}
	}
	failed := resetOutcomeFacts(t, store, UserRegistrationFailed)
	if len(failed) != 1 || len(resetOutcomeFacts(t, store, UserRegistered)) != 1 || len(resetOutcomeFacts(t, store, UserRegistrationCompleted)) != 1 {
		t.Fatal("identity collision created a second user or repeated workflow failure")
	}
	reaction := &RegistrationCompletionEventHandler{saver: store, retriever: store, keys: keys}
	for _, validated := range resetOutcomeFacts(t, store, EmailVerificationOTPValidated) {
		if err := reaction.handle(t.Context(), validated); err != nil {
			t.Fatal("terminal workflow failure blocks reaction checkpoint")
		}
	}
}
func TestRegisterUserCommandIsIndependentAndContentBound(t *testing.T) {
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	command := RegisterUserCommand{UserRegisteredID: uuidv7.NewString(), Username: "direct-user", Email: "direct@example.com", PasswordHash: testAuthHash(t), EmailVerifiedAt: time.Unix(1, 0).UTC()}
	for range 2 {
		if _, err := RegisterUserCommandHandler(t.Context(), command, store, store, keys); err != nil {
			t.Fatal(err)
		}
	}
	if len(resetOutcomeFacts(t, store, UserRegistrationStarted)) != 0 || len(resetOutcomeFacts(t, store, EmailVerificationOTPValidated)) != 0 || len(resetOutcomeFacts(t, store, UserRegistered)) != 1 {
		t.Fatal("domain command acquired workflow dependencies")
	}
	command.FirstName = "Different"
	if _, err := RegisterUserCommandHandler(t.Context(), command, store, store, keys); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("identity accepted different terms: %v", err)
	}
	command.EmailVerifiedAt = time.Time{}
	if _, err := RegisterUserCommandHandler(t.Context(), command, store, store, keys); err == nil {
		t.Fatal("unverified accepted user allowed")
	}
}

func TestRegistrationWorkflowDoesNotAdoptEarlierDomainFact(t *testing.T) {
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	result := stagedRegistration(t, store, keys)
	started, err := DecodeUserRegistrationStartedEvent(resetOutcomeFacts(t, store, UserRegistrationStarted)[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	generated, err := GenerateEmailVerificationOTPCommandHandler(t.Context(), GenerateEmailVerificationOTPCommand{UserRegistrationStartedID: result.UserRegistrationStartedID}, store, store)
	if err != nil {
		t.Fatal(err)
	}
	verifiedAt := time.Unix(1, 0).UTC()
	_, err = RegisterUserCommandHandler(t.Context(), RegisterUserCommand{UserRegisteredID: result.UserID, Username: "staged-user", Email: "staged@example.com", FirstName: "Staged", LastName: "User", YearOfBirth: 1990, PasswordHash: started.PasswordHash, EmailVerifiedAt: verifiedAt}, store, store, keys)
	if err != nil {
		t.Fatal(err)
	}
	validation := mustAuthEvent(NewEmailVerificationOTPValidatedEvent(uuidv7.NewString(), verifiedAt, generated.EmailVerificationOTPGeneratedID, result.UserRegistrationStartedID, nil))
	if _, err := store.SaveEventsV2(t.Context(), []eventstore.DomainEvent{validation}, nil); err != nil {
		t.Fatal(err)
	}
	if err := CompleteUserRegistrationCommandHandler(t.Context(), CompleteUserRegistrationCommand{UserRegistrationStartedID: result.UserRegistrationStartedID}, store, store); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("earlier effective fact was adopted: %v", err)
	}
	if err := FinalizeUserRegistration(t.Context(), result.UserRegistrationStartedID, nil, store, store, keys); !errors.Is(err, ErrRegistrationIdentityUnavailable) {
		t.Fatal(err)
	}
	if len(resetOutcomeFacts(t, store, UserRegistrationCompleted)) != 0 || len(resetOutcomeFacts(t, store, UserRegistrationFailed)) != 1 {
		t.Fatal("workflow falsely claimed an earlier direct registration")
	}
}

func TestConcurrentRegistrationFinalizationKeepsOneEffectiveIdentity(t *testing.T) {
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	registrations := []StartUserRegistrationResult{stagedRegistration(t, store, keys), stagedRegistration(t, store, keys)}
	for _, registration := range registrations {
		verifyStagedRegistration(t, store, registration)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var reads atomic.Int32
	retriever := resetLatestFunc(func(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
		latest, err := store.GetLatestByCriteria(ctx, criteria)
		if err != nil {
			return latest, err
		}
		domain := len(criteria) == 3
		for _, criterion := range criteria {
			hasType := false
			for _, tag := range criterion.Tags {
				if tag.Key == eventstore.EventTypeField && tag.Value == UserRegistered {
					hasType = true
				}
			}
			domain = domain && hasType
		}
		if domain && reads.Add(1) <= 2 {
			ready <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return latest, ctx.Err()
			}
		}
		return latest, nil
	})
	outcomes := make(chan error, 2)
	for _, registration := range registrations {
		go func(id string) { outcomes <- FinalizeUserRegistration(ctx, id, nil, store, retriever, keys) }(registration.UserRegistrationStartedID)
	}
	for range 2 {
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatal("domain admissions were not concurrent")
		}
	}
	unblock()
	success := 0
	for range 2 {
		select {
		case err := <-outcomes:
			if err == nil {
				success++
			} else if !errors.Is(err, eventstore.ErrConflict) && !errors.Is(err, ErrRegistrationIdentityUnavailable) {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("finalization did not finish")
		}
	}
	if success != 1 {
		t.Fatal("concurrent workflows admitted more than one identity")
	}
	for _, registration := range registrations {
		err := FinalizeUserRegistration(t.Context(), registration.UserRegistrationStartedID, nil, store, store, keys)
		if err != nil && !errors.Is(err, ErrRegistrationIdentityUnavailable) {
			t.Fatal(err)
		}
	}
	if len(resetOutcomeFacts(t, store, UserRegistered)) != 1 || len(resetOutcomeFacts(t, store, UserRegistrationCompleted)) != 1 || len(resetOutcomeFacts(t, store, UserRegistrationFailed)) != 1 {
		t.Fatal("racing finalizers duplicated or lost terminal facts")
	}
}
