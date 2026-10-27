package auth

import (
	"context"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"strings"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

func TestPasswordResetProjectsDecryptedTokenAndRejectsTampering(t *testing.T) {
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	store := eventstoretest.New(t)
	registered, err := RegisterUserCommandHandler(t.Context(), directRegistrationCommand(t, uuidv7.NewString(), "reset-user", "reset@example.com", keys), store, store, keys)
	if err != nil {
		t.Fatal(err)
	}
	users := resetTestUser{user: views.User{UserRegisteredID: registered.UserRegisteredID, Email: "reset@example.com"}}
	result, err := RequestPasswordResetCommandHandler(t.Context(), RequestPasswordResetCommand{EmailAddress: "reset@example.com"}, users, store, store, keys)
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

type resetTestUser struct{ user views.User }

func (u resetTestUser) UserByEmailWithPassword(context.Context, string) (views.User, string, error) {
	return u.user, "", nil
}
