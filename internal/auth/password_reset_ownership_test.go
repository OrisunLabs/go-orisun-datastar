package auth

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"golang.org/x/crypto/bcrypt"
)

type resetTokenCandidate struct{ verification PasswordResetVerification }

func (r resetTokenCandidate) PasswordResetByToken(context.Context, string) (PasswordResetVerification, error) {
	return r.verification, nil
}
func resetOutcomeFacts(t *testing.T, store eventstore.Retriever, eventType string) []eventstore.ResolvedEvent {
	t.Helper()
	facts, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: eventType}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return facts
}
func TestResetPasswordBindsRequestAndCompletionOwner(t *testing.T) {
	for _, scenario := range []string{"matching owner", "foreign request owner", "missing request", "already completed", "foreign completion owner"} {
		t.Run(scenario, func(t *testing.T) {
			owner, foreign, request := uuidv7.NewString(), uuidv7.NewString(), uuidv7.NewString()
			key, err := protectedpii.GenerateSubjectDataKey()
			if err != nil {
				t.Fatal(err)
			}
			requested := mustAuthEvent(NewPasswordResetRequestedEvent(request, "reset@example.com", "token", time.Now().Add(time.Hour), owner, key, nil))
			facts := []eventstore.DomainEvent{registeredAuthEvent(t, owner), requested}
			verification := PasswordResetVerification{ID: request, UserID: owner}
			reject := false
			switch scenario {
			case "foreign request owner":
				verification.UserID = foreign
				reject = true
			case "missing request":
				facts = nil
				reject = true
			case "already completed", "foreign completion owner":
				completedOwner := owner
				if scenario == "foreign completion owner" {
					completedOwner = foreign
				}
				authorizationID, operationID := uuidv7.NewString(), uuidv7.NewString()
				facts = append(facts, mustAuthEvent(NewPasswordResetAuthorizedEvent(authorizationID, operationID, request, owner, testAuthHash(t), time.Now(), nil)), mustAuthEvent(NewPasswordResetCompletedEvent(uuidv7.NewString(), time.Now(), request, completedOwner, authorizationID, operationID, nil)))
				reject = true
			}
			store := eventstoretest.New(t, facts...)
			before := len(resetOutcomeFacts(t, store, PasswordResetAuthorized))
			_, err = AuthorizePasswordResetCommandHandler(t.Context(), ResetPasswordCommand{Token: "token", Password: "updated123"}, resetTokenCandidate{verification}, store, store)
			if reject && err == nil {
				t.Fatal("invalid reset authority accepted")
			}
			if !reject && err != nil {
				t.Fatal(err)
			}
			if scenario == "foreign completion owner" && err != nil && !strings.Contains(err.Error(), "scope does not match") {
				t.Fatalf("completion ancestry not checked: %v", err)
			}
			after := resetOutcomeFacts(t, store, PasswordResetAuthorized)
			expected := before
			if !reject {
				expected++
			}
			if len(after) != expected {
				t.Fatal("rejected reset appended a completion")
			}
			if !reject {
				completed, err := DecodePasswordResetAuthorizedEvent(after[0].Event)
				if err != nil {
					t.Fatal(err)
				}
				if completed.Scopes.UserRegisteredID != owner || completed.Scopes.PasswordResetRequestedID != request {
					t.Fatal("saved completion owner differs from request")
				}
				if err := bcrypt.CompareHashAndPassword([]byte(completed.PasswordHash), []byte("updated123")); err != nil {
					t.Fatal("accepted password differs from saved password")
				}
			}
		})
	}
}

type resetLatestFunc func(context.Context, []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error)

func (f resetLatestFunc) GetLatestByCriteria(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
	return f(ctx, criteria)
}
func TestResetPasswordConcurrentCompletionIsAtomic(t *testing.T) {
	owner, request := uuidv7.NewString(), uuidv7.NewString()
	key, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	store := eventstoretest.New(t, registeredAuthEvent(t, owner), mustAuthEvent(NewPasswordResetRequestedEvent(request, "reset@example.com", "token", time.Now().Add(time.Hour), owner, key, nil)))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
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
		if !hasResetRequestCriterion(criteria) {
			return latest, nil
		}
		ready <- struct{}{}
		select {
		case <-release:
			return latest, nil
		case <-ctx.Done():
			return latest, ctx.Err()
		}
	})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := AuthorizePasswordResetCommandHandler(ctx, ResetPasswordCommand{Token: "token", Password: "updated123"}, resetTokenCandidate{PasswordResetVerification{ID: request, UserID: owner}}, store, retriever)
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-ready:
		case <-time.After(5 * time.Second):
			t.Fatal("commands did not reach concurrent authority reads")
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
			t.Fatal("commands did not finish")
		}
	}
	if successes != 1 || conflicts != 1 || len(resetOutcomeFacts(t, store, PasswordResetAuthorized)) != 1 {
		t.Fatal("concurrent reset appended more than one completion")
	}
}

func hasResetRequestCriterion(criteria []eventstore.Criterion) bool {
	for _, c := range criteria {
		for _, tag := range c.Tags {
			if tag.Key == eventstore.EventTypeField && tag.Value == PasswordResetRequested {
				return true
			}
		}
	}
	return false
}

func TestResetAuthorizationRejectsStaleDiscoveryAndInvalidOrdering(t *testing.T) {
	for _, scenario := range []string{"missing registration", "invalid discovered user", "invalid discovered request", "deleted account", "foreign deletion", "request before registration", "authorization before request"} {
		t.Run(scenario, func(t *testing.T) {
			store, owner, request, candidate := resetWorkflowFixture(t)
			requested := resetOutcomeFacts(t, store, PasswordResetRequested)[0].Event
			root := resetOutcomeFacts(t, store, UserRegistered)[0].Event
			reject := true
			switch scenario {
			case "missing registration":
				store = eventstoretest.New(t, requested)
			case "invalid discovered user":
				candidate.verification.UserID = "invalid"
			case "invalid discovered request":
				candidate.verification.ID = "invalid"
			case "deleted account", "foreign deletion":
				deletedOwner := owner
				if scenario == "foreign deletion" {
					deletedOwner = uuidv7.NewString()
					reject = false
				}
				deleted := mustAuthEvent(NewAccountDeletedEvent(uuidv7.NewString(), time.Now(), deletedOwner, deletedOwner, nil))
				if _, err := store.SaveEventsV2(t.Context(), []eventstore.DomainEvent{deleted}, nil); err != nil {
					t.Fatal(err)
				}
			case "request before registration":
				store = eventstoretest.New(t, requested, root)
			case "authorization before request":
				authorized := mustAuthEvent(NewPasswordResetAuthorizedEvent(uuidv7.NewString(), uuidv7.NewString(), request, owner, testAuthHash(t), time.Now(), nil))
				store = eventstoretest.New(t, root, authorized, requested)
			}
			before := len(resetOutcomeFacts(t, store, PasswordResetAuthorized))
			id, err := AuthorizePasswordResetCommandHandler(t.Context(), ResetPasswordCommand{Token: "token", Password: "updated123"}, candidate, store, store)
			if reject && (err == nil || id != "") {
				t.Fatal("stale or invalid discovery accepted")
			}
			if !reject && (err != nil || id == "") {
				t.Fatalf("unrelated deletion blocked authorization: %v", err)
			}
			expected := before
			if !reject {
				expected++
			}
			if len(resetOutcomeFacts(t, store, PasswordResetAuthorized)) != expected {
				t.Fatal("invalid authority appended authorization")
			}
		})
	}
}
func TestResetAuthorizationConflictsWithConcurrentDeletion(t *testing.T) {
	store, owner, _, candidate := resetWorkflowFixture(t)
	read := false
	retriever := resetLatestFunc(func(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
		latest, err := store.GetLatestByCriteria(ctx, criteria)
		if err != nil {
			return latest, err
		}
		if hasResetRequestCriterion(criteria) {
			if read {
				t.Fatal("workflow authority reloaded instead of guarding observation")
			}
			read = true
			deleted := mustAuthEvent(NewAccountDeletedEvent(uuidv7.NewString(), time.Now(), owner, owner, nil))
			_, err = store.SaveEventsV2(ctx, []eventstore.DomainEvent{deleted}, nil)
		}
		return latest, err
	})
	id, err := AuthorizePasswordResetCommandHandler(t.Context(), ResetPasswordCommand{Token: "token", Password: "updated123"}, candidate, store, retriever)
	if !errors.Is(err, eventstore.ErrConflict) || id != "" || len(resetOutcomeFacts(t, store, PasswordResetAuthorized)) != 0 {
		t.Fatalf("deletion did not invalidate authorization: id=%s err=%v", id, err)
	}
}
