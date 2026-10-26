package profile

import (
	"context"
	"errors"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

func TestBioUpdateRoundTripUnchangedAndClearing(t *testing.T) {
	store, user, keys := profileFixture(t)
	command := UpdateProfileBioCommand{UserID: user, Bio: "  Hello profile  "}
	if err := UpdateProfileBioCommandHandler(t.Context(), command, store, store, keys); err != nil {
		t.Fatal(err)
	}
	if err := UpdateProfileBioCommandHandler(t.Context(), command, store, store, keys); err != nil {
		t.Fatal(err)
	}
	events := profileFacts(t, store)
	if len(events) != 1 {
		t.Fatalf("unchanged bio emitted another fact: %d", len(events))
	}
	bio, err := DecodeProfileBioUpdatedEvent(events[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	if bio.UserID != user {
		t.Fatal("missing cross-domain user reference")
	}
	value, err := decryptProfileBio(bio, keys.key)
	if err != nil || value != "Hello profile" {
		t.Fatalf("decrypted bio=%q error=%v", value, err)
	}
	command.Bio = ""
	if err := UpdateProfileBioCommandHandler(t.Context(), command, store, store, keys); err != nil {
		t.Fatal(err)
	}
	events = profileFacts(t, store)
	cleared, err := DecodeProfileBioUpdatedEvent(events[1].Event)
	if err != nil {
		t.Fatal(err)
	}
	value, err = decryptProfileBio(cleared, keys.key)
	if err != nil || value != "" {
		t.Fatalf("cleared bio=%q error=%v", value, err)
	}
}

func TestBioUpdateRejectsTamperedEncryptedHistory(t *testing.T) {
	store, user, keys := profileFixture(t)
	event, err := NewProfileBioUpdatedEvent("019a0000-0000-7000-8000-000000000001", "Before", testProfileTime(), user, keys.key, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Preserve valid serialization while invalidating the GCM authentication tag.
	protected := event.Data[ProfileBioUpdatedBioField].(map[string]any)
	ciphertext := protected["ciphertext"].(string)
	prefix := "A"
	if ciphertext[:1] == prefix {
		prefix = "B"
	}
	protected["ciphertext"] = prefix + ciphertext[1:]
	if _, err := store.SaveEventsV2(t.Context(), []eventstore.DomainEvent{event}, nil); err != nil {
		t.Fatal(err)
	}
	err = UpdateProfileBioCommandHandler(t.Context(), UpdateProfileBioCommand{UserID: user, Bio: "After"}, store, store, keys)
	if err == nil {
		t.Fatal("tampered bio accepted")
	}
	if len(profileFacts(t, store)) != 1 {
		t.Fatal("appended after invalid authority")
	}
}

func TestBioUpdateRejectsConcurrentChange(t *testing.T) {
	store, user, keys := profileFixture(t)
	reader := &profileConcurrentBioReader{EmbeddedOrisun: store, t: t, user: user, keys: keys}
	err := UpdateProfileBioCommandHandler(t.Context(), UpdateProfileBioCommand{UserID: user, Bio: "Stale"}, store, reader, keys)
	if !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("stale update: %v", err)
	}
	if len(profileFacts(t, store)) != 1 {
		t.Fatal("conflicting bio persisted")
	}
}

type profileConcurrentBioReader struct {
	*eventstore.EmbeddedOrisun
	t    *testing.T
	user string
	keys *profileTestKeys
}

func (r *profileConcurrentBioReader) GetLatestByCriteria(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
	latest, err := r.EmbeddedOrisun.GetLatestByCriteria(ctx, criteria)
	if err != nil {
		return latest, err
	}
	if err := UpdateProfileBioCommandHandler(ctx, UpdateProfileBioCommand{UserID: r.user, Bio: "Concurrent"}, r.EmbeddedOrisun, r.EmbeddedOrisun, r.keys); err != nil {
		r.t.Fatal(err)
	}
	return latest, nil
}

func profileFixture(t *testing.T) (*eventstore.EmbeddedOrisun, string, *profileTestKeys) {
	t.Helper()
	store := eventstoretest.New(t)
	key, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	keys := &profileTestKeys{key: key}
	user, err := auth.RegisterUserCommandHandler(t.Context(), auth.RegisterUserCommand{Username: "profile-user", Email: "profile@example.com", Password: "secret123"}, store, store, keys)
	if err != nil {
		t.Fatal(err)
	}
	return store, user.UserRegisteredID, keys
}
func profileFacts(t *testing.T, store *eventstore.EmbeddedOrisun) []eventstore.ResolvedEvent {
	t.Helper()
	query := eventstore.Query{}
	for _, kind := range []string{ProfileBioUpdated, ProfileImageUploaded, ProfileHeaderImageUploaded} {
		query.Criteria = append(query.Criteria, eventstore.Criterion{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: kind}}})
	}
	facts, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, query)
	if err != nil {
		t.Fatal(err)
	}
	return facts
}

type profileTestKeys struct {
	key protectedpii.SubjectDataKey
	id  string
}

func (k *profileTestKeys) GetOrCreateSubjectDataKey(_ context.Context, id string) (protectedpii.SubjectDataKey, error) {
	k.id = id
	return k.key, nil
}
func (k *profileTestKeys) GetSubjectDataKey(_ context.Context, id string) (protectedpii.SubjectDataKey, bool, error) {
	return k.key, id == k.id, nil
}
func (k *profileTestKeys) DestroySubjectKey(context.Context, string) error { return nil }
