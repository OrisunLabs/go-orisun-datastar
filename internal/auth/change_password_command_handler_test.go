package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"golang.org/x/crypto/bcrypt"
)

func TestPasswordChangeUsesEventAuthorityWithoutCredentialProjection(t *testing.T) {
	for _, scenario := range []string{"accepted", "wrong password", "short password", "invalid identity", "missing registration", "deleted account", "foreign deletion", "malformed deletion", "password before registration"} {
		t.Run(scenario, func(t *testing.T) {
			owner := uuidv7.NewString()
			root := registeredAuthEvent(t, owner)
			command := ChangePasswordCommand{UserRegisteredID: owner, CurrentPassword: "secret123", NewPassword: "updated123"}
			facts := []eventstore.DomainEvent{root}
			reject := false
			switch scenario {
			case "wrong password":
				command.CurrentPassword = "incorrect"
				reject = true
			case "short password":
				command.NewPassword = "short"
				reject = true
			case "invalid identity":
				command.UserRegisteredID = "invalid"
				reject = true
			case "missing registration":
				facts = nil
				reject = true
			case "deleted account", "foreign deletion", "malformed deletion":
				deletedOwner := owner
				if scenario == "foreign deletion" {
					deletedOwner = uuidv7.NewString()
				} else {
					reject = true
				}
				deleted := mustAuthEvent(NewAccountDeletedEvent(uuidv7.NewString(), time.Now(), deletedOwner, deletedOwner, nil))
				if scenario == "malformed deletion" {
					deleted.Data = eventstore.MustData(deleted.Data)
					deleted.Data["extra"] = true
				}
				facts = append(facts, deleted)
			case "password before registration":
				facts = []eventstore.DomainEvent{mustAuthEvent(NewPasswordChangedEvent(uuidv7.NewString(), time.Now(), owner, testAuthHash(t), nil)), root}
				reject = true
			}
			store := eventstoretest.New(t, facts...)
			before := len(resetOutcomeFacts(t, store, PasswordChanged))
			err := ChangePasswordCommandHandler(t.Context(), command, store, store)
			if reject && err == nil {
				t.Fatal("invalid authority accepted")
			}
			if !reject && err != nil {
				t.Fatal(err)
			}
			expected := before
			if !reject {
				expected++
			}
			saved := resetOutcomeFacts(t, store, PasswordChanged)
			if len(saved) != expected {
				t.Fatal("rejected change saved password")
			}
			if !reject {
				value, err := DecodePasswordChangedEvent(saved[len(saved)-1].Event)
				if err != nil {
					t.Fatal(err)
				}
				if value.Scopes.UserRegisteredID != owner || bcrypt.CompareHashAndPassword([]byte(value.PasswordHash), []byte(command.NewPassword)) != nil {
					t.Fatal("saved wrong password or owner")
				}
				writeContext, err := store.GetWriteContext(t.Context(), saved[len(saved)-1].WriteID)
				if err != nil || len(writeContext.Consistency) != 1 || len(writeContext.Consistency[0].Query.Criteria) != 3 {
					t.Fatal("active account authority not guarded with password")
				}
			}
		})
	}
}
func TestPasswordChangeConflictsWithConcurrentAccountDeletion(t *testing.T) {
	owner := uuidv7.NewString()
	store := eventstoretest.New(t, registeredAuthEvent(t, owner))
	read := false
	retriever := resetLatestFunc(func(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
		if read {
			t.Fatal("authority reloaded instead of guarding original observation")
		}
		read = true
		latest, err := store.GetLatestByCriteria(ctx, criteria)
		if err != nil {
			return latest, err
		}
		deleted := mustAuthEvent(NewAccountDeletedEvent(uuidv7.NewString(), time.Now(), owner, owner, nil))
		_, err = store.SaveEventsV2(ctx, []eventstore.DomainEvent{deleted}, nil)
		return latest, err
	})
	err := ChangePasswordCommandHandler(t.Context(), ChangePasswordCommand{UserRegisteredID: owner, CurrentPassword: "secret123", NewPassword: "updated123"}, store, retriever)
	if !errors.Is(err, eventstore.ErrConflict) || len(resetOutcomeFacts(t, store, PasswordChanged)) != 0 {
		t.Fatalf("concurrent deletion did not invalidate password append: %v", err)
	}
}
