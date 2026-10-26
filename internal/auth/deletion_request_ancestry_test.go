package auth

import (
	"strings"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

func TestDeletionRequestValidatesCompletionAncestry(t *testing.T) {
	for _, scenario := range []string{"new request", "pending request", "matching completion", "different request", "different auth user", "completion without request"} {
		t.Run(scenario, func(t *testing.T) {
			owner, foreign, request := uuidv7.NewString(), uuidv7.NewString(), uuidv7.NewString()
			facts := []eventstore.DomainEvent{registeredAuthEvent(t, owner)}
			reject := scenario == "different request" || scenario == "different auth user" || scenario == "completion without request"
			if scenario != "new request" && scenario != "completion without request" {
				facts = append(facts, mustAuthEvent(NewAccountDeletionRequestedEvent(request, time.Now(), owner, owner, nil)))
			}
			if scenario != "new request" && scenario != "pending request" {
				completedRequest, authID := request, owner
				if scenario == "different request" {
					completedRequest = uuidv7.NewString()
				}
				if scenario == "different auth user" {
					authID = foreign
				}
				facts = append(facts, mustAuthEvent(NewAccountDeletedEvent(uuidv7.NewString(), time.Now(), completedRequest, authID, owner, nil)))
			}
			store := eventstoretest.New(t, facts...)
			user := views.User{ID: owner, UserRegisteredID: owner, Email: "test@example.com"}
			credentials := ownershipCredentials{user: user, hash: testAuthHash(t)}
			before := len(resetOutcomeFacts(t, store, AccountDeletionRequested))
			result, err := RequestAccountDeletionCommandHandler(t.Context(), RequestAccountDeletionCommand{User: user, Password: "secret123"}, credentials, store, store)
			if reject {
				if err == nil || result != "" {
					t.Fatal("inconsistent completion accepted as an idempotent retry")
				}
				if !strings.Contains(err.Error(), "scope does not match") {
					t.Fatalf("completion relationship not checked: %v", err)
				}
			} else {
				if err != nil || result == "" {
					t.Fatalf("valid request rejected: %v", err)
				}
				if scenario != "new request" && result != request {
					t.Fatal("retry returned a different request identity")
				}
			}
			expected := before
			if scenario == "new request" {
				expected++
			}
			if len(resetOutcomeFacts(t, store, AccountDeletionRequested)) != expected {
				t.Fatal("unexpected additional request event")
			}
			if !reject {
				repeated, err := RequestAccountDeletionCommandHandler(t.Context(), RequestAccountDeletionCommand{User: user, Password: "secret123"}, credentials, store, store)
				if err != nil || repeated != result || len(resetOutcomeFacts(t, store, AccountDeletionRequested)) != expected {
					t.Fatal("valid retry changed outcome")
				}
			}
		})
	}
}
