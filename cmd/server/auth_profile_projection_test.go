package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/profile"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"golang.org/x/crypto/bcrypt"
)

type authProfileKeys struct {
	key   protectedpii.SubjectDataKey
	found bool
	err   error
}

func (k authProfileKeys) GetSubjectDataKey(context.Context, string) (protectedpii.SubjectDataKey, bool, error) {
	return k.key, k.found, k.err
}
func mustAuthProfileEvent(t *testing.T, e eventstore.DomainEvent, err error) eventstore.DomainEvent {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func authProfileHistory(t *testing.T) ([]eventstore.ResolvedEvent, authProfileKeys) {
	t.Helper()
	user := uuidv7.NewString()
	key, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	must := func(e eventstore.DomainEvent, err error) eventstore.DomainEvent {
		return mustAuthProfileEvent(t, e, err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("secret123"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	facts := []eventstore.DomainEvent{
		must(auth.NewUserRegisteredEvent(user, "owned-profile", "owned@example.com", "Owned", "Profile", 1990, string(hash), key, nil)),
		must(profile.NewProfileImageUploadedEvent(uuidv7.NewString(), "/uploads/avatar.png", now, user, nil)),
		must(profile.NewProfileBioUpdatedEvent(uuidv7.NewString(), "Owned bio", now, user, key, nil)),
		must(profile.NewProfileHeaderImageUploadedEvent(uuidv7.NewString(), "/uploads/header.png", now, user, nil)),
		must(auth.NewUserNameChangedEvent(uuidv7.NewString(), "Updated name", now, user, key, nil)),
	}
	result := make([]eventstore.ResolvedEvent, len(facts))
	for i, event := range facts {
		result[i] = eventstore.ResolvedEvent{Event: event, Position: eventstore.Position{Commit: 20, Prepare: int64(i)}}
	}
	return result, authProfileKeys{key: key, found: true}
}
func TestAuthProfileSourceUsesOwningContracts(t *testing.T) {
	history, keys := authProfileHistory(t)
	source := authProfileSource()
	user := history[0].Event.EventID
	expected := []auth.AuthChange{auth.ImageInput{UserID: user, URL: "/uploads/avatar.png"}, auth.BioInput{UserID: user, Bio: "Owned bio"}, auth.HeaderImageInput{UserID: user, URL: "/uploads/header.png"}}
	for i, event := range history[1:4] {
		change, err := source.Decode(t.Context(), event.Event, keys)
		if err != nil || !reflect.DeepEqual(change, expected[i]) {
			t.Fatalf("mapping: %#v %v", change, err)
		}
		corrupt := event.Event
		corrupt.Data = eventstore.MustData(event.Event.Data)
		corrupt.Data["scopes"] = map[string]any{"userRegisteredId": user}
		if _, err := source.Decode(t.Context(), corrupt, keys); err == nil {
			t.Fatal("legacy auth ancestry accepted")
		}
	}
	for _, keys := range []authProfileKeys{{found: false}, {found: true, err: errors.New("key unavailable")}, {found: true}} {
		if _, err := source.Decode(t.Context(), history[2].Event, keys); err == nil {
			t.Fatal("bio decoded without valid subject key")
		}
	}
}

type authProfilePublisherFunc func(context.Context, string, any) error

func (f authProfilePublisherFunc) Publish(ctx context.Context, subject string, data any) error {
	return f(ctx, subject, data)
}
func TestAuthProfileInvalidationIsRoutingOnly(t *testing.T) {
	publisher := authUserInvalidator{publisher: authProfilePublisherFunc(func(_ context.Context, subject string, data any) error {
		if subject != profile.Channel("user") || !reflect.DeepEqual(data, map[string]string{"userId": "user"}) {
			t.Fatal("notification includes more than routing")
		}
		return nil
	})}
	if err := publisher.InvalidateAuthUser(t.Context(), "user"); err != nil {
		t.Fatal(err)
	}
}
