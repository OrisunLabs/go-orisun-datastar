package auth

import (
	"context"
	"errors"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"strings"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

func TestPasswordResetProjectsDecryptedTokenAndRejectsTampering(t *testing.T) {
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	store := eventstoretest.New(t)
	registered, err := RegisterUserCommandHandler(t.Context(), directRegistrationCommand(t, uuidv7.NewString(), "reset-user", "reset@example.com", keys), store, store, keys)
	if err != nil {
		t.Fatal(err)
	}
	result, err := RequestPasswordResetCommandHandler(t.Context(), RequestPasswordResetCommand{EmailAddress: "reset@example.com"}, store, store, keys)
	if err != nil {
		t.Fatal(err)
	}
	events, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetRequested}, {Key: PasswordResetRequestedIDField, Value: result.PasswordResetRequestedID}}}}})
	if err != nil || len(events) != 1 {
		t.Fatalf("reset facts=%d error=%v", len(events), err)
	}
	change, err := decodeAuthChange(t.Context(), events[0], keys, ProfileSource{})
	if err != nil {
		t.Fatal(err)
	}
	projected := change.(VerificationFact)
	if projected.ValueHash != verificationValueHash("password-reset-token", result.Token) || projected.UserID != registered.UserRegisteredID {
		t.Fatal("projected token differs from accepted command")
	}
	// A validly encoded ciphertext with an invalid authentication tag must
	// produce an error before persisting another verification row.
	tampered := events[0]
	tampered.Event.Data = eventstore.MustData(tampered.Event.Data)
	token := tampered.Event.Data[PasswordResetRequestedTokenField].(map[string]any)
	ciphertext := token["ciphertext"].(string)
	replacement := "A"
	if strings.HasPrefix(ciphertext, "A") {
		replacement = "B"
	}
	token["ciphertext"] = replacement + ciphertext[1:]
	if _, err := decodeAuthChange(t.Context(), tampered, keys, ProfileSource{}); err == nil {
		t.Fatal("tampered ciphertext accepted")
	}

}

type authTestKeys struct {
	keys map[string]protectedpii.SubjectDataKey
}

func (k *authTestKeys) GetOrCreateSubjectDataKey(_ context.Context, id string) (protectedpii.SubjectDataKey, error) {
	if key, ok := k.keys[id]; ok {
		return key, nil
	}
	key, err := protectedpii.GenerateSubjectDataKey()
	if err == nil {
		k.keys[id] = key
	}
	return key, err
}
func (k *authTestKeys) GetSubjectDataKey(_ context.Context, id string) (protectedpii.SubjectDataKey, bool, error) {
	key, ok := k.keys[id]
	return key, ok, nil
}
func (k *authTestKeys) DestroySubjectKey(_ context.Context, id string) error {
	delete(k.keys, id)
	return nil
}

type resetRequestKeyReader struct {
	key     protectedpii.SubjectDataKey
	owner   string
	reads   int
	missing bool
}

func (k *resetRequestKeyReader) GetSubjectDataKey(_ context.Context, id string) (protectedpii.SubjectDataKey, bool, error) {
	k.reads++
	return k.key, !k.missing && id == k.owner, nil
}
func resetRequestFixture(t *testing.T) (eventstore.DomainEvent, *resetRequestKeyReader) {
	t.Helper()
	owner := uuidv7.NewString()
	key, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	root := mustAuthEvent(NewUserRegisteredEvent(owner, "reset-user", "reset@example.com", "Reset", "User", 1990, testAuthHash(t), time.Unix(1, 0).UTC(), time.Unix(1, 0).UTC(), key, nil))
	return root, &resetRequestKeyReader{key: key, owner: owner}
}
func TestResetRequestDiscoversRecipientFromAcceptedEvents(t *testing.T) {
	for _, scenario := range []string{"registered", "normalized email", "unknown email", "missing registration", "pending registration", "deleted account", "missing key", "malformed registration", "recipient commitment mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			root, keys := resetRequestFixture(t)
			facts := []eventstore.DomainEvent{root}
			email := "reset@example.com"
			accepted := false
			reject := false
			switch scenario {
			case "registered":
				accepted = true
			case "normalized email":
				email = "  RESET@EXAMPLE.COM  "
				accepted = true
			case "unknown email":
				email = "unknown@example.com"
			case "missing registration":
				facts = nil
			case "pending registration":
				facts = []eventstore.DomainEvent{mustAuthEvent(NewUserRegistrationStartedEvent(uuidv7.NewString(), keys.owner, "reset-user", email, "Reset", "User", 1990, testAuthHash(t), time.Now(), keys.key, nil))}
			case "deleted account":
				facts = append(facts, mustAuthEvent(NewAccountDeletedEvent(uuidv7.NewString(), time.Now(), keys.owner, keys.owner, nil)))
			case "missing key":
				keys.missing = true
				reject = true
			case "malformed registration":
				root.Data = eventstore.MustData(root.Data)
				root.Data["extra"] = true
				facts[0] = root
				reject = true
			case "recipient commitment mismatch":
				wrong, err := protectedpii.FromEnv().ProtectWithDataKey("other@example.com", UserRegisteredEmailField, keys.key)
				if err != nil {
					t.Fatal(err)
				}
				wrong.BlindIndex = protectedpii.FromEnv().BlindIndex(UserRegisteredEmailField, email)
				root.Data = eventstore.MustData(root.Data)
				root.Data[UserRegisteredEmailField] = eventstore.MustData(wrong)
				facts[0] = root
				reject = true
			}
			store := eventstoretest.New(t, facts...)
			result, err := RequestPasswordResetCommandHandler(t.Context(), RequestPasswordResetCommand{EmailAddress: email}, store, store, keys)
			if reject && err == nil {
				t.Fatal("invalid authority accepted")
			}
			if !reject && err != nil {
				t.Fatal(err)
			}
			saved := resetOutcomeFacts(t, store, PasswordResetRequested)
			if !accepted {
				if len(saved) != 0 || result.PasswordResetRequestedID != "" || result.Token != "" || !result.ExpiresAt.IsZero() {
					t.Fatal("ineligible request emitted reset authority")
				}
				if !reject && keys.reads != 0 {
					t.Fatal("unknown/pending/deleted user reached key access")
				}
				return
			}
			if len(saved) != 1 || result.Token == "" || result.PasswordResetRequestedID == "" {
				t.Fatal("accepted registration did not produce reset")
			}
			request, err := DecodePasswordResetRequestedEvent(saved[0].Event)
			if err != nil {
				t.Fatal(err)
			}
			recipient, err := protectedpii.FromEnv().DecryptWithDataKey(request.Email, keys.key)
			if err != nil || recipient != "reset@example.com" || request.Scopes.UserRegisteredID != keys.owner {
				t.Fatal("recipient or owner not drawn from authoritative registration")
			}
			context, err := store.GetWriteContext(t.Context(), saved[0].WriteID)
			if err != nil || len(context.Consistency) != 2 || len(context.Consistency[0].Query.Criteria) != 1 || len(context.Consistency[1].Query.Criteria) != 1 || context.Consistency[1].Position != eventstore.NoEventPosition {
				t.Fatal("dependent observations not preserved")
			}
		})
	}
}
func TestResetRequestGuardsDeletionAcrossDependentReads(t *testing.T) {
	for _, stage := range []string{"after registration read", "after deletion read"} {
		t.Run(stage, func(t *testing.T) {
			root, keys := resetRequestFixture(t)
			store := eventstoretest.New(t, root)
			reads := 0
			retriever := resetLatestFunc(func(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
				reads++
				if reads > 2 {
					t.Fatal("authority reloaded instead of retaining observations")
				}
				latest, err := store.GetLatestByCriteria(ctx, criteria)
				if err != nil {
					return latest, err
				}
				if stage == "after registration read" && reads == 1 || stage == "after deletion read" && reads == 2 {
					deleted := mustAuthEvent(NewAccountDeletedEvent(uuidv7.NewString(), time.Now(), keys.owner, keys.owner, nil))
					_, err = store.SaveEventsV2(ctx, []eventstore.DomainEvent{deleted}, nil)
				}
				return latest, err
			})
			result, err := RequestPasswordResetCommandHandler(t.Context(), RequestPasswordResetCommand{EmailAddress: "reset@example.com"}, store, retriever, keys)
			if stage == "after registration read" && (err != nil || keys.reads != 0) {
				t.Fatalf("visible deletion did not stop key access: %v", err)
			}
			if stage == "after deletion read" && !errors.Is(err, eventstore.ErrConflict) {
				t.Fatalf("late deletion did not invalidate append: %v", err)
			}
			if len(resetOutcomeFacts(t, store, PasswordResetRequested)) != 0 || result.Token != "" {
				t.Fatal("deleted account received reset authority")
			}
		})
	}
}
