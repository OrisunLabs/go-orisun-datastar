package auth

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
	"golang.org/x/crypto/bcrypt"
)

func authorityPasswordHash(t *testing.T, password string) string {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(hash)
}
func TestPasswordMutationsUseAuthoritativeCredential(t *testing.T) {
	for _, intent := range []string{"change", "delete"} {
		t.Run(intent, func(t *testing.T) {
			for _, scenario := range []string{"registration", "stale after change", "latest after change", "stale after reset", "latest after reset", "change after reset", "reset after change", "foreign change", "missing registration", "malformed reset"} {
				t.Run(scenario, func(t *testing.T) {
					owner, foreign := uuidv7.NewString(), uuidv7.NewString()
					user := views.User{ID: owner, UserRegisteredID: owner, Email: "test@example.com"}
					root := registeredAuthEvent(t, owner)
					changed := mustAuthEvent(NewPasswordChangedEvent(uuidv7.NewString(), time.Now(), owner, authorityPasswordHash(t, "changed-secret"), nil))
					reset := mustAuthEvent(NewPasswordResetCompletedEvent(uuidv7.NewString(), time.Now(), uuidv7.NewString(), owner, authorityPasswordHash(t, "reset-secret"), nil))
					facts := []eventstore.DomainEvent{root}
					password := "secret123"
					reject := false
					switch scenario {
					case "stale after change":
						facts = append(facts, changed)
						reject = true
					case "latest after change":
						facts = append(facts, changed)
						password = "changed-secret"
					case "stale after reset":
						facts = append(facts, reset)
						reject = true
					case "latest after reset":
						facts = append(facts, reset)
						password = "reset-secret"
					case "change after reset":
						facts = append(facts, reset, changed)
						password = "changed-secret"
					case "reset after change":
						facts = append(facts, changed, reset)
						password = "reset-secret"
					case "foreign change":
						facts = append(facts, mustAuthEvent(NewPasswordChangedEvent(uuidv7.NewString(), time.Now(), foreign, authorityPasswordHash(t, "foreign-secret"), nil)))
					case "missing registration":
						facts = []eventstore.DomainEvent{changed}
						reject = true
					case "malformed reset":
						reset.Data = eventstore.MustData(reset.Data)
						reset.Data["extra"] = true
						facts = append(facts, reset)
						reject = true
					}
					store := eventstoretest.New(t, facts...)
					credentials := ownershipCredentials{user: user, hash: testAuthHash(t)} // Deliberately lagging projection.
					eventType := PasswordChanged
					if intent == "delete" {
						eventType = AccountDeletionRequested
					}
					before := len(resetOutcomeFacts(t, store, eventType))
					var err error
					if intent == "change" {
						err = ChangePasswordCommandHandler(t.Context(), ChangePasswordCommand{User: user, CurrentPassword: password, NewPassword: "updated123"}, credentials, store, store)
					} else {
						_, err = RequestAccountDeletionCommandHandler(t.Context(), RequestAccountDeletionCommand{User: user, Password: password}, credentials, store, store)
					}
					if reject && err == nil {
						t.Fatal("stale or missing password authority accepted")
					}
					if !reject && err != nil {
						t.Fatal(err)
					}
					if strings.HasPrefix(scenario, "stale") && err != nil && !strings.Contains(err.Error(), "password") {
						t.Fatalf("unexpected rejection: %v", err)
					}
					expected := before
					if !reject {
						expected++
					}
					if len(resetOutcomeFacts(t, store, eventType)) != expected {
						t.Fatal("incorrect mutation count")
					}
				})
			}
		})
	}
}
func TestPasswordMutationsConflictWithConcurrentCredentialChange(t *testing.T) {
	for _, intent := range []string{"change", "delete"} {
		for _, source := range []string{PasswordChanged, PasswordResetCompleted} {
			t.Run(intent+"/"+source, func(t *testing.T) {
				owner := uuidv7.NewString()
				user := views.User{ID: owner, UserRegisteredID: owner, Email: "test@example.com"}
				store := eventstoretest.New(t, registeredAuthEvent(t, owner))
				hash := authorityPasswordHash(t, "racing-secret")
				var racing eventstore.DomainEvent
				if source == PasswordChanged {
					racing = mustAuthEvent(NewPasswordChangedEvent(uuidv7.NewString(), time.Now(), owner, hash, nil))
				} else {
					racing = mustAuthEvent(NewPasswordResetCompletedEvent(uuidv7.NewString(), time.Now(), uuidv7.NewString(), owner, hash, nil))
				}
				read := false
				retriever := resetLatestFunc(func(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
					latest, err := store.GetLatestByCriteria(ctx, criteria)
					if err != nil {
						return latest, err
					}
					if read {
						t.Fatal("authority was reloaded instead of guarding the accepted observation")
					}
					read = true
					_, err = store.SaveEventsV2(ctx, []eventstore.DomainEvent{racing}, nil)
					return latest, err
				})
				credentials := ownershipCredentials{user: user, hash: testAuthHash(t)}
				var err error
				eventType := PasswordChanged
				if intent == "change" {
					err = ChangePasswordCommandHandler(t.Context(), ChangePasswordCommand{User: user, CurrentPassword: "secret123", NewPassword: "updated123"}, credentials, store, retriever)
				} else {
					eventType = AccountDeletionRequested
					_, err = RequestAccountDeletionCommandHandler(t.Context(), RequestAccountDeletionCommand{User: user, Password: "secret123"}, credentials, store, retriever)
				}
				if !errors.Is(err, eventstore.ErrConflict) {
					t.Fatalf("concurrent credential change did not invalidate append: %v", err)
				}
				expected := 0
				if eventType == source {
					expected = 1
				}
				if len(resetOutcomeFacts(t, store, eventType)) != expected {
					t.Fatal("conflicting mutation was persisted")
				}
			})
		}
	}
}
