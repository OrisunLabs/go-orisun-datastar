package auth

import (
	"errors"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func TestRegistrationStatusTracksSeparateStagesWithoutKeys(t *testing.T) {
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	pending := stagedRegistration(t, store, keys)
	expect := func(stage RegistrationStage) {
		t.Helper()
		state, err := ReadRegistrationStatus(t.Context(), pending.UserRegistrationStartedID, store)
		if err != nil || state.Stage != stage || state.UserID != pending.UserID {
			t.Fatalf("status=%+v err=%v; want %s", state, err, stage)
		}
	}
	expect(RegistrationVerifying)
	verifyStagedRegistration(t, store, pending)
	expect(RegistrationProcessing)
	executeRegistrationForTest(t, store, keys)
	expect(RegistrationProcessing)
	completeRegistrationForTest(t, store)
	expect(RegistrationComplete)
	if err := DeleteAccountCommandHandler(t.Context(), DeleteAccountCommand{AccountDeletedID: uuidv7.NewString(), UserRegisteredID: pending.UserID, AuthUserID: pending.UserID}, &deletionIdentityEffects{}, keys, store, store); err != nil {
		t.Fatal(err)
	}
	expect(RegistrationUnavailable)
}
func TestRegistrationStatusRejectsForeignCompletionAndReportsFailure(t *testing.T) {
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	first := stagedRegistration(t, store, keys)
	second := stagedRegistration(t, store, keys)
	verifyStagedRegistration(t, store, first)
	verifyStagedRegistration(t, store, second)
	source := resetOutcomeFacts(t, store, EmailVerificationOTPValidated)[0]
	execution := &RegistrationExecutionEventHandler{saver: store, retriever: store, keys: keys}
	if err := execution.handle(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	completeRegistrationForTest(t, store)
	if err := FailUserRegistrationCommandHandler(t.Context(), FailUserRegistrationCommand{UserRegistrationStartedID: second.UserRegistrationStartedID}, store, store); err != nil {
		t.Fatal(err)
	}
	state, err := ReadRegistrationStatus(t.Context(), second.UserRegistrationStartedID, store)
	if err != nil || state.Stage != RegistrationFailed {
		t.Fatalf("failed proposal not reported: %+v %v", state, err)
	}
	validation, err := DecodeEmailVerificationOTPValidatedEvent(source.Event)
	if err != nil {
		t.Fatal(err)
	}
	invalid := mustAuthEvent(NewUserRegistrationCompletedEvent(uuidv7.NewString(), first.UserRegistrationStartedID, validation.EmailVerificationOTPValidatedID, second.UserID, time.Now(), nil))
	if _, err := store.SaveEventsV2(t.Context(), []eventstore.DomainEvent{invalid}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRegistrationStatus(t.Context(), first.UserRegistrationStartedID, store); !errors.Is(err, eventstore.ErrInvalidEvent) {
		t.Fatalf("foreign accepted user drove completion: %v", err)
	}
}
