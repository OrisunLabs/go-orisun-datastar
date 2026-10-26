package auth

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
	"golang.org/x/crypto/bcrypt"
)

type ownershipCredentials struct {
	user views.User
	hash string
	err  error
}

func (c ownershipCredentials) UserByEmailWithPassword(context.Context, string) (views.User, string, error) {
	return c.user, c.hash, c.err
}

func TestCredentialOwnershipBeforePasswordAndDeletionCommands(t *testing.T) {
	for _, intent := range []string{"password change", "deletion request"} {
		t.Run(intent, func(t *testing.T) {
			for _, scenario := range []string{"matching owner", "foreign registered user", "foreign auth user", "missing owner", "incorrect password", "lookup failure"} {
				t.Run(scenario, func(t *testing.T) {
					target, foreign := uuidv7.NewString(), uuidv7.NewString()
					user := views.User{ID: target, UserRegisteredID: target, Email: "test@example.com"}
					credentials := ownershipCredentials{user: user, hash: testAuthHash(t)}
					password := "secret123"
					reject := false
					ownerMismatch := false
					switch scenario {
					case "foreign registered user":
						credentials.user.UserRegisteredID = foreign
						reject = true
						ownerMismatch = true
					case "foreign auth user":
						credentials.user.ID = foreign
						reject = intent == "deletion request"
						ownerMismatch = reject
					case "missing owner":
						credentials.user = views.User{}
						reject = true
						ownerMismatch = true
					case "incorrect password":
						password = "incorrect"
						reject = true
					case "lookup failure":
						credentials.err = errors.New("lookup unavailable")
						reject = true
					}
					store := eventstoretest.New(t, registeredAuthEvent(t, target))
					eventType := PasswordChanged
					var err error
					if intent == "password change" {
						err = ChangePasswordCommandHandler(t.Context(), ChangePasswordCommand{User: user, CurrentPassword: password, NewPassword: "updated123"}, credentials, store, store)
					} else {
						eventType = AccountDeletionRequested
						var requestID string
						requestID, err = RequestAccountDeletionCommandHandler(t.Context(), RequestAccountDeletionCommand{User: user, Password: password}, credentials, store, store)
						if reject && requestID != "" {
							t.Fatal("rejected deletion returned an identity")
						}
					}
					if reject && err == nil {
						t.Fatal("invalid credentials accepted")
					}
					if !reject && err != nil {
						t.Fatal(err)
					}
					if ownerMismatch && err != nil && !strings.Contains(err.Error(), "does not belong") {
						t.Fatalf("ownership was not explicitly checked: %v", err)
					}
					facts, readErr := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: eventType}, {Key: ScopeUserRegisteredIDField, Value: target}}}}})
					if readErr != nil {
						t.Fatal(readErr)
					}
					expected := 1
					if reject {
						expected = 0
					}
					if len(facts) != expected {
						t.Fatalf("saved %d facts, want %d", len(facts), expected)
					}
					if !reject && intent == "password change" {
						changed, err := DecodePasswordChangedEvent(facts[0].Event)
						if err != nil {
							t.Fatal(err)
						}
						if err := bcrypt.CompareHashAndPassword([]byte(changed.PasswordHash), []byte("updated123")); err != nil {
							t.Fatal("saved password differs from accepted command")
						}
					}
					if !reject && intent == "deletion request" {
						requested, err := DecodeAccountDeletionRequestedEvent(facts[0].Event)
						if err != nil {
							t.Fatal(err)
						}
						if requested.AuthUserID != target || requested.Scopes.UserRegisteredID != target {
							t.Fatal("saved deletion has wrong owner")
						}
					}
				})
			}
		})
	}
}
