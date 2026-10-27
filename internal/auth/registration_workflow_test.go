package auth

import (
	"context"
	"errors"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"sync"
	"testing"
	"time"
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

func executeRegistrationForTest(t *testing.T, store *eventstore.EmbeddedOrisun, keys SubjectPiiKeyReader) {
	t.Helper()
	execute := &RegistrationExecutionEventHandler{saver: store, retriever: store, keys: keys}
	for _, e := range resetOutcomeFacts(t, store, EmailVerificationOTPValidated) {
		if err := execute.handle(t.Context(), e); err != nil {
			t.Fatal(err)
		}
	}
}
func completeRegistrationForTest(t *testing.T, store *eventstore.EmbeddedOrisun) {
	t.Helper()
	complete := &RegistrationCompletionEventHandler{saver: store, retriever: store}
	for _, e := range resetOutcomeFacts(t, store, UserRegistered) {
		if err := complete.handle(t.Context(), e); err != nil {
			t.Fatal(err)
		}
	}
}
func TestRegistrationReactionsHaveSeparateResponsibilities(t *testing.T) {
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	pending := stagedRegistration(t, store, keys)
	verifyStagedRegistration(t, store, pending)
	if len(resetOutcomeFacts(t, store, UserRegistered)) != 0 {
		t.Fatal("validation executed registration")
	}
	executeRegistrationForTest(t, store, keys)
	if len(resetOutcomeFacts(t, store, UserRegistered)) != 1 || len(resetOutcomeFacts(t, store, UserRegistrationCompleted)) != 0 {
		t.Fatal("execution also completed workflow")
	}
	completeRegistrationForTest(t, store)
	accepted := resetOutcomeFacts(t, store, UserRegistered)
	if len(resetOutcomeFacts(t, store, UserRegistrationCompleted)) != 1 {
		t.Fatal("domain effect did not complete workflow")
	}
	if _, ok := accepted[0].Event.Data["scopes"]; ok {
		t.Fatal("accepted fact depends on workflow")
	}
	projection := &authProjectionFake{states: map[string]AuthState{}, receipts: map[eventstore.Position]string{}, facts: map[string]VerificationFact{}}
	if err := ProjectBatch(t.Context(), accepted, projection, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	if user := projection.states[pending.UserID]; !user.EmailVerified || user.Email != "staged@example.com" || user.Name != "Staged User" {
		t.Fatal("accepted fact not self-contained")
	}
	if err := keys.DestroySubjectKey(t.Context(), pending.UserID); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		executeRegistrationForTest(t, store, keys)
		completeRegistrationForTest(t, store)
	}
	if len(resetOutcomeFacts(t, store, UserRegistered)) != 1 || len(resetOutcomeFacts(t, store, UserRegistrationCompleted)) != 1 {
		t.Fatal("reactions duplicated outcomes")
	}
}
func directRegistrationCommand(t *testing.T, id, username, email string, keys *authTestKeys) RegisterUserCommand {
	t.Helper()
	key, err := keys.GetOrCreateSubjectDataKey(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	details, err := protectRegistrationDetails(username, email, "", "", 0, testAuthHash(t), key)
	if err != nil {
		t.Fatal(err)
	}
	return RegisterUserCommand{UserRegisteredID: id, RegistrationDetails: details, EmailVerifiedAt: time.Unix(1, 0).UTC()}
}
func TestRegisterUserCommandIsIndependentAndContentBound(t *testing.T) {
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	command := directRegistrationCommand(t, uuidv7.NewString(), "direct-user", "direct@example.com", keys)
	for range 2 {
		if _, err := RegisterUserCommandHandler(t.Context(), command, store, store, keys); err != nil {
			t.Fatal(err)
		}
	}
	// Completion is invoked for every domain fact; direct registrations are no-ops.
	completeRegistrationForTest(t, store)
	if len(resetOutcomeFacts(t, store, UserRegistrationCompleted)) != 0 {
		t.Fatal("direct effect invented workflow")
	}
	command.YearOfBirth = 1990
	if _, err := RegisterUserCommandHandler(t.Context(), command, store, store, keys); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("operation not content-bound: %v", err)
	}
	command.EmailVerifiedAt = time.Time{}
	if _, err := RegisterUserCommandHandler(t.Context(), command, store, store, keys); err == nil {
		t.Fatal("accepted missing verification")
	}
}
func TestCompetingRegistrationDoesNotDispatchAnotherCommand(t *testing.T) {
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	first := stagedRegistration(t, store, keys)
	second := stagedRegistration(t, store, keys)
	verifyStagedRegistration(t, store, first)
	verifyStagedRegistration(t, store, second)
	events := resetOutcomeFacts(t, store, EmailVerificationOTPValidated)
	execution := &RegistrationExecutionEventHandler{saver: store, retriever: store, keys: keys}
	if err := execution.handle(t.Context(), events[0]); err != nil {
		t.Fatal(err)
	}
	if err := execution.handle(t.Context(), events[1]); !errors.Is(err, ErrRegistrationIdentityUnavailable) {
		t.Fatalf("expected identity conflict: %v", err)
	}
	if len(resetOutcomeFacts(t, store, UserRegistrationFailed)) != 0 {
		t.Fatal("reaction selected failure command")
	}
	completeRegistrationForTest(t, store)
	if len(resetOutcomeFacts(t, store, UserRegistrationCompleted)) != 1 {
		t.Fatal("failed attempt falsely completed")
	}
}

func TestRegistrationCompletionRejectsEarlierEffect(t *testing.T) {
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	pending := stagedRegistration(t, store, keys)
	started, err := DecodeUserRegistrationStartedEvent(resetOutcomeFacts(t, store, UserRegistrationStarted)[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	if _, err := RegisterUserCommandHandler(t.Context(), RegisterUserCommand{UserRegisteredID: pending.UserID, RegistrationDetails: started.RegistrationDetails, EmailVerifiedAt: at}, store, store, keys); err != nil {
		t.Fatal(err)
	}
	// Source position, rather than matching wall-clock values, proves ordering.
	validation := mustAuthEvent(NewEmailVerificationOTPValidatedEvent(uuidv7.NewString(), at, uuidv7.NewString(), pending.UserRegistrationStartedID, nil))
	if _, err := store.SaveEventsV2(t.Context(), []eventstore.DomainEvent{validation}, nil); err != nil {
		t.Fatal(err)
	}
	if err := CompleteUserRegistrationCommandHandler(t.Context(), CompleteUserRegistrationCommand{UserRegisteredID: pending.UserID}, store, store); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("adopted earlier accepted fact: %v", err)
	}
	if len(resetOutcomeFacts(t, store, UserRegistrationCompleted)) != 0 {
		t.Fatal("false completion")
	}
}

func TestRegistrationExecutionConcurrentIdentityGuard(t *testing.T) {
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	first := stagedRegistration(t, store, keys)
	second := stagedRegistration(t, store, keys)
	verifyStagedRegistration(t, store, first)
	verifyStagedRegistration(t, store, second)
	ready := make(chan struct{}, 2)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	retriever := resetLatestFunc(func(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
		latest, err := store.GetLatestByCriteria(ctx, criteria)
		if err != nil {
			return latest, err
		}
		effective := false
		for _, c := range criteria {
			for _, tag := range c.Tags {
				if tag.Key == UserRegisteredUsernameHashField {
					effective = true
				}
			}
		}
		if effective {
			ready <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return latest, ctx.Err()
			}
		}
		return latest, nil
	})
	reaction := &RegistrationExecutionEventHandler{saver: store, retriever: retriever, keys: keys}
	results := make(chan error, 2)
	for _, e := range resetOutcomeFacts(t, store, EmailVerificationOTPValidated) {
		go func(e eventstore.ResolvedEvent) { results <- reaction.handle(t.Context(), e) }(e)
	}
	for range 2 {
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatal("executions did not reach effective authority")
		}
	}
	unblock()
	successes, conflicts := 0, 0
	for range 2 {
		select {
		case err := <-results:
			if err == nil {
				successes++
			} else if errors.Is(err, eventstore.ErrConflict) {
				conflicts++
			} else {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("execution did not finish")
		}
	}
	if successes != 1 || conflicts != 1 || len(resetOutcomeFacts(t, store, UserRegistered)) != 1 {
		t.Fatal("concurrent execution duplicated accepted identity")
	}
	completeRegistrationForTest(t, store)
	if len(resetOutcomeFacts(t, store, UserRegistrationCompleted)) != 1 {
		t.Fatal("accepted effect did not complete its workflow")
	}
}
