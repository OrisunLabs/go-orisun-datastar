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

func resetWorkflowFixture(t *testing.T) (*eventstore.EmbeddedOrisun, string, string, resetTokenCandidate) {
	t.Helper()
	owner, request := uuidv7.NewString(), uuidv7.NewString()
	key, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	requested := mustAuthEvent(NewPasswordResetRequestedEvent(request, "reset@example.com", "token", time.Now().Add(time.Hour), owner, key, nil))
	store := eventstoretest.New(t, registeredAuthEvent(t, owner), requested)
	return store, owner, request, resetTokenCandidate{PasswordResetVerification{ID: request, UserID: owner}}
}
func TestPasswordResetSeparatesAuthorizationEffectAndCompletion(t *testing.T) {
	store, owner, request, resets := resetWorkflowFixture(t)
	command := ResetPasswordCommand{Token: "token", Password: "updated123"}
	id, err := AuthorizePasswordResetCommandHandler(t.Context(), command, resets, store, store)
	if err != nil {
		t.Fatal(err)
	}
	authorized := resetOutcomeFacts(t, store, PasswordResetAuthorized)
	a, err := DecodePasswordResetAuthorizedEvent(authorized[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	if a.OperationID == id || a.OperationID == request || len(resetOutcomeFacts(t, store, PasswordChanged)) != 0 || len(resetOutcomeFacts(t, store, PasswordResetCompleted)) != 0 {
		t.Fatal("authorization applied domain change")
	}
	retry, err := AuthorizePasswordResetCommandHandler(t.Context(), command, resets, store, store)
	if err != nil || retry != id {
		t.Fatalf("authorization retry changed frozen operation: %v", err)
	}
	command.Password = "different123"
	if _, err := AuthorizePasswordResetCommandHandler(t.Context(), command, resets, store, store); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("authorization replaced password: %v", err)
	}
	if err := CompletePasswordResetCommandHandler(t.Context(), CompletePasswordResetCommand{PasswordChangedID: a.OperationID}, store, store); !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("completed without effect: %v", err)
	}
	// Execute with complete direct input; no workflow dependency or read-model access.
	effect := SetPasswordCommand{PasswordChangedID: a.OperationID, UserRegisteredID: owner, PasswordHash: a.PasswordHash}
	if err := SetPasswordCommandHandler(t.Context(), effect, store, store); err != nil {
		t.Fatal(err)
	}
	changed := resetOutcomeFacts(t, store, PasswordChanged)
	input, err := decodeAuthChange(t.Context(), changed[0], nil, ProfileSource{})
	if err != nil {
		t.Fatal(err)
	}
	if input.(PasswordInput).PasswordHash != a.PasswordHash {
		t.Fatal("effective fact omitted accepted hash")
	}
	// A later ordinary assignment must survive recovery of the interrupted workflow.
	newerHash := authorityPasswordHash(t, "newer-secret")
	if err := SetPasswordCommandHandler(t.Context(), SetPasswordCommand{PasswordChangedID: uuidv7.NewString(), UserRegisteredID: owner, PasswordHash: newerHash}, store, store); err != nil {
		t.Fatal(err)
	}
	reaction := &PasswordResetCompletionEventHandler{saver: store, retriever: store}
	for range 2 {
		if err := reaction.handle(t.Context(), changed[0]); err != nil {
			t.Fatal(err)
		}
	}
	receipts := resetOutcomeFacts(t, store, PasswordResetCompleted)
	if len(receipts) != 1 || len(resetOutcomeFacts(t, store, PasswordChanged)) != 2 {
		t.Fatal("recovery reapplied historical password or duplicated completion")
	}
	receipt, err := DecodePasswordResetCompletedEvent(receipts[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Scopes.PasswordChangedID != a.OperationID || receipt.Scopes.PasswordResetAuthorizedID != id {
		t.Fatal("completion lost exact effect ancestry")
	}
	if _, ok := receipts[0].Event.Data["passwordHash"]; ok {
		t.Fatal("workflow completion contains credential state")
	}
	model, err := loadChangePasswordContext(t.Context(), ChangePasswordCommand{UserRegisteredID: owner}, store)
	if err != nil || model.passwordHash != newerHash {
		t.Fatalf("workflow completion changed effective authority: %v", err)
	}
	effect.PasswordHash = newerHash
	if err := SetPasswordCommandHandler(t.Context(), effect, store, store); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("operation identity not content-bound: %v", err)
	}
}
func TestResetAuthorizationChecksAuthoritativeTokenAndExpiry(t *testing.T) {
	for _, scenario := range []string{"wrong token", "expired source"} {
		t.Run(scenario, func(t *testing.T) {
			store, owner, request, resets := resetWorkflowFixture(t)
			token := "wrong"
			if scenario == "expired source" {
				token = "token"
				key, _ := protectedpii.GenerateSubjectDataKey()
				store = eventstoretest.New(t, registeredAuthEvent(t, owner), mustAuthEvent(NewPasswordResetRequestedEvent(request, "reset@example.com", token, time.Now().Add(-time.Hour), owner, key, nil)))
			}
			if _, err := AuthorizePasswordResetCommandHandler(t.Context(), ResetPasswordCommand{Token: token, Password: "updated123"}, resets, store, store); err == nil {
				t.Fatal("projection candidate bypassed token authority")
			}
			if len(resetOutcomeFacts(t, store, PasswordResetAuthorized)) != 0 {
				t.Fatal("invalid authority saved authorization")
			}
		})
	}
}
func TestResetCompletionRejectsEarlierOrDifferentEffectiveFact(t *testing.T) {
	for _, scenario := range []string{"earlier", "wrong hash", "wrong owner"} {
		t.Run(scenario, func(t *testing.T) {
			owner, request, id, operation := uuidv7.NewString(), uuidv7.NewString(), uuidv7.NewString(), uuidv7.NewString()
			hash := testAuthHash(t)
			authorization := mustAuthEvent(NewPasswordResetAuthorizedEvent(id, operation, request, owner, hash, time.Now(), nil))
			effectOwner, effectHash := owner, hash
			if scenario == "wrong owner" {
				effectOwner = uuidv7.NewString()
			}
			if scenario == "wrong hash" {
				effectHash = authorityPasswordHash(t, "other123")
			}
			effect := mustAuthEvent(NewPasswordChangedEvent(operation, time.Now(), effectOwner, effectHash, nil))
			facts := []eventstore.DomainEvent{authorization, effect}
			if scenario == "earlier" {
				facts = []eventstore.DomainEvent{effect, authorization}
			}
			store := eventstoretest.New(t, facts...)
			if err := CompletePasswordResetCommandHandler(t.Context(), CompletePasswordResetCommand{PasswordChangedID: operation}, store, store); !errors.Is(err, eventstore.ErrConflict) {
				t.Fatalf("invalid effect adopted: %v", err)
			}
			if len(resetOutcomeFacts(t, store, PasswordResetCompleted)) != 0 {
				t.Fatal("false workflow completion")
			}
		})
	}
}
func TestSetPasswordWorksWithoutResetWorkflow(t *testing.T) {
	owner := uuidv7.NewString()
	store := eventstoretest.New(t, registeredAuthEvent(t, owner))
	command := SetPasswordCommand{PasswordChangedID: uuidv7.NewString(), UserRegisteredID: owner, PasswordHash: testAuthHash(t)}
	for range 2 {
		if err := SetPasswordCommandHandler(t.Context(), command, store, store); err != nil {
			t.Fatal(err)
		}
	}
	if len(resetOutcomeFacts(t, store, PasswordChanged)) != 1 {
		t.Fatal("direct assignment retry duplicated effect")
	}
}

func TestPasswordResetExecutionDoesNotCompleteWorkflow(t *testing.T) {
	store, _, _, resets := resetWorkflowFixture(t)
	if _, err := AuthorizePasswordResetCommandHandler(t.Context(), ResetPasswordCommand{Token: "token", Password: "updated123"}, resets, store, store); err != nil {
		t.Fatal(err)
	}
	authorization := resetOutcomeFacts(t, store, PasswordResetAuthorized)[0]
	execution := &PasswordResetExecutionEventHandler{saver: store, retriever: store}
	for range 2 {
		if err := execution.handle(t.Context(), authorization); err != nil {
			t.Fatal(err)
		}
	}
	if len(resetOutcomeFacts(t, store, PasswordChanged)) != 1 || len(resetOutcomeFacts(t, store, PasswordResetCompleted)) != 0 {
		t.Fatal("execution also selected workflow completion")
	}
	completion := &PasswordResetCompletionEventHandler{saver: store, retriever: store}
	for range 2 {
		if err := completion.handle(t.Context(), resetOutcomeFacts(t, store, PasswordChanged)[0]); err != nil {
			t.Fatal(err)
		}
	}
	if len(resetOutcomeFacts(t, store, PasswordResetCompleted)) != 1 {
		t.Fatal("completion duplicated receipt")
	}
}
func TestDirectPasswordChangeHasNoResetCompletion(t *testing.T) {
	owner := uuidv7.NewString()
	store := eventstoretest.New(t, registeredAuthEvent(t, owner))
	command := SetPasswordCommand{PasswordChangedID: uuidv7.NewString(), UserRegisteredID: owner, PasswordHash: testAuthHash(t)}
	if err := SetPasswordCommandHandler(t.Context(), command, store, store); err != nil {
		t.Fatal(err)
	}
	completion := &PasswordResetCompletionEventHandler{saver: store, retriever: store}
	if err := completion.handle(t.Context(), resetOutcomeFacts(t, store, PasswordChanged)[0]); err != nil {
		t.Fatal(err)
	}
	if len(resetOutcomeFacts(t, store, PasswordResetCompleted)) != 0 {
		t.Fatal("direct change invented workflow")
	}
}
