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
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
	"golang.org/x/crypto/bcrypt"
)

type resetCandidateUser struct {
	user views.User
	err  error
}

func (r resetCandidateUser) UserByIDOrRegisteredID(context.Context, string) (views.User, error) {
	return r.user, r.err
}
func (r resetCandidateUser) UserByEmailWithPassword(context.Context, string) (views.User, string, error) {
	return r.user, "", r.err
}

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
	for _, scenario := range []string{"matching owner", "foreign request owner", "foreign lookup user", "missing request", "already completed", "foreign completion owner"} {
		t.Run(scenario, func(t *testing.T) {
			owner, foreign, request := uuidv7.NewString(), uuidv7.NewString(), uuidv7.NewString()
			key, err := protectedpii.GenerateSubjectDataKey()
			if err != nil {
				t.Fatal(err)
			}
			requested := mustAuthEvent(NewPasswordResetRequestedEvent(request, "reset@example.com", "token", time.Now().Add(time.Hour), owner, key, nil))
			facts := []eventstore.DomainEvent{registeredAuthEvent(t, owner), requested}
			verification := PasswordResetVerification{ID: request, UserID: owner}
			user := views.User{ID: owner, UserRegisteredID: owner}
			reject := false
			switch scenario {
			case "foreign request owner":
				verification.UserID = foreign
				user = views.User{ID: foreign, UserRegisteredID: foreign}
				reject = true
			case "foreign lookup user":
				user = views.User{ID: foreign, UserRegisteredID: foreign}
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
			_, err = AuthorizePasswordResetCommandHandler(t.Context(), ResetPasswordCommand{Token: "token", Password: "updated123"}, resetTokenCandidate{verification}, resetCandidateUser{user: user}, store, store)
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
func TestRequestPasswordResetRequiresRegistration(t *testing.T) {
	for _, scenario := range []string{"registered", "missing registration", "unknown user", "lookup failure"} {
		t.Run(scenario, func(t *testing.T) {
			owner := uuidv7.NewString()
			key, err := protectedpii.GenerateSubjectDataKey()
			if err != nil {
				t.Fatal(err)
			}
			user := views.User{ID: owner, UserRegisteredID: owner, Email: "reset@example.com"}
			var facts []eventstore.DomainEvent
			if scenario == "registered" {
				facts = append(facts, mustAuthEvent(NewUserRegisteredEvent(owner, "reset-user", user.Email, "Reset", "User", 1990, testAuthHash(t), time.Unix(1, 0).UTC(), time.Unix(1, 0).UTC(), key, nil)))
			}
			users := resetCandidateUser{user: user}
			if scenario == "unknown user" || scenario == "lookup failure" {
				users.err = errors.New("user lookup failed")
			}
			store := eventstoretest.New(t, facts...)
			result, err := RequestPasswordResetCommandHandler(t.Context(), RequestPasswordResetCommand{EmailAddress: user.Email}, users, store, store, &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{owner: key}})
			if scenario == "missing registration" {
				if err == nil {
					t.Fatal("reset request accepted without registered authority")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			saved := resetOutcomeFacts(t, store, PasswordResetRequested)
			if scenario == "registered" {
				if len(saved) != 1 || result.PasswordResetRequestedID == "" {
					t.Fatal("registered reset did not save")
				}
			} else {
				if len(saved) != 0 || result.PasswordResetRequestedID != "" || result.Token != "" || !result.ExpiresAt.IsZero() {
					t.Fatal("unregistered reset returned or saved an outcome")
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
			_, err := AuthorizePasswordResetCommandHandler(ctx, ResetPasswordCommand{Token: "token", Password: "updated123"}, resetTokenCandidate{PasswordResetVerification{ID: request, UserID: owner}}, resetCandidateUser{user: views.User{ID: owner, UserRegisteredID: owner}}, store, retriever)
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
