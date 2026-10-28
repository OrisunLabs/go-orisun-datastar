package auth

import (
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func TestDeletionRequestUsesAuthenticatedIDsAndEventPassword(t *testing.T) {
	for _, scenario := range []string{"matching owner", "foreign registered user", "foreign auth user", "missing auth user", "invalid registered user", "incorrect password", "missing registration", "latest password", "stale password", "password before registration", "foreign pending auth user"} {
		t.Run(scenario, func(t *testing.T) {
			owner, foreign := uuidv7.NewString(), uuidv7.NewString()
			command := RequestAccountDeletionCommand{UserRegisteredID: owner, AuthUserID: owner, Password: "secret123"}
			root := registeredAuthEvent(t, owner)
			facts := []eventstore.DomainEvent{root}
			reject := false
			switch scenario {
			case "foreign registered user":
				command.UserRegisteredID = foreign
				reject = true
			case "foreign auth user":
				command.AuthUserID = foreign
				reject = true
			case "missing auth user":
				command.AuthUserID = ""
				reject = true
			case "invalid registered user":
				command.UserRegisteredID = "invalid"
				reject = true
			case "incorrect password":
				command.Password = "wrong"
				reject = true
			case "missing registration":
				facts = nil
				reject = true
			case "latest password", "stale password", "password before registration":
				changed := mustAuthEvent(NewPasswordChangedEvent(uuidv7.NewString(), time.Now(), owner, authorityPasswordHash(t, "current-secret"), nil))
				facts = append(facts, changed)
				if scenario == "latest password" {
					command.Password = "current-secret"
				} else {
					reject = true
				}
				if scenario == "password before registration" {
					facts = []eventstore.DomainEvent{changed, root}
					command.Password = "current-secret"
				}
			case "foreign pending auth user":
				facts = append(facts, mustAuthEvent(NewAccountDeletionRequestedEvent(uuidv7.NewString(), time.Now(), foreign, owner, uuidv7.NewString(), nil)))
				reject = true
			}
			store := eventstoretest.New(t, facts...)
			before := len(resetOutcomeFacts(t, store, AccountDeletionRequested))
			id, err := RequestAccountDeletionCommandHandler(t.Context(), command, store, store)
			if reject && (err == nil || id != "") {
				t.Fatal("invalid ownership or password accepted")
			}
			if !reject && (err != nil || id == "") {
				t.Fatalf("accepted identity rejected: %v", err)
			}
			expected := before
			if !reject {
				expected++
			}
			requests := resetOutcomeFacts(t, store, AccountDeletionRequested)
			if len(requests) != expected {
				t.Fatal("rejected command appended request")
			}
			if !reject {
				saved, err := DecodeAccountDeletionRequestedEvent(requests[len(requests)-1].Event)
				if err != nil {
					t.Fatal(err)
				}
				if saved.AuthUserID != owner || saved.Scopes.UserRegisteredID != owner {
					t.Fatal("request lost authenticated owner")
				}
				if len(resetOutcomeFacts(t, store, AccountDeleted)) != 0 {
					t.Fatal("request performed domain deletion")
				}
			}
		})
	}
}
