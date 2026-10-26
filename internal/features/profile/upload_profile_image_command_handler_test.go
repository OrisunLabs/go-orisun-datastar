package profile

import (
	"context"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func testProfileTime() time.Time { return time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC) }
func TestProfileUploadsUseUserEntityReference(t *testing.T) {
	store, user, _ := profileFixture(t)
	storage := &profileTestObjects{url: "/static/uploads/example.png"}
	for _, header := range []bool{false, true} {
		result, err := UploadProfileImageCommandHandler(t.Context(), UploadProfileImageCommand{UserID: user, Data: []byte("image"), ContentType: "image/png", Header: header}, store, store, storage)
		if err != nil || result.URL != storage.url {
			t.Fatalf("upload: %#v %v", result, err)
		}
	}
	events := profileFacts(t, store)
	if len(events) != 2 || storage.puts != 2 {
		t.Fatalf("events=%d uploads=%d", len(events), storage.puts)
	}
	for _, event := range events {
		id, err := profileEventUserID(event.Event)
		if err != nil || id != user {
			t.Fatalf("owner=%s error=%v", id, err)
		}
		if _, ok := event.Event.Data["scope"]; ok {
			t.Fatal("singular auth ancestry persisted")
		}
		if _, ok := event.Event.Data["scopes"]; ok {
			t.Fatal("auth ancestry persisted")
		}
		if _, ok := event.Event.Data["userRegisteredId"]; ok {
			t.Fatal("auth event identity leaked into payload")
		}
	}
}
func TestProfileUploadRejectsInvalidURLBeforeStorageWrite(t *testing.T) {
	store, user, _ := profileFixture(t)
	storage := &profileTestObjects{url: "javascript:alert(1)"}
	_, err := UploadProfileImageCommandHandler(t.Context(), UploadProfileImageCommand{UserID: user, Data: []byte("image"), ContentType: "image/png"}, store, store, storage)
	if err == nil || storage.puts != 0 || len(profileFacts(t, store)) != 0 {
		t.Fatal("invalid URL caused effects")
	}
}
func TestProfileConsumersRejectMalformedContracts(t *testing.T) {
	_, user, keys := profileFixture(t)
	bio, err := NewProfileBioUpdatedEvent(uuidv7.NewString(), "", testProfileTime(), user, keys.key, nil)
	if err != nil {
		t.Fatal(err)
	}
	avatar, err := NewProfileImageUploadedEvent(uuidv7.NewString(), "/uploads/a.png", testProfileTime(), user, nil)
	if err != nil {
		t.Fatal(err)
	}
	header, err := NewProfileHeaderImageUploadedEvent(uuidv7.NewString(), "https://cdn.example.com/header.png", testProfileTime(), user, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []eventstore.DomainEvent{bio, avatar, header} {
		mutations := map[string]func(map[string]any){
			"legacy ancestry": func(data map[string]any) { data["scopes"] = map[string]any{"userRegisteredId": user} },
			"unknown":         func(data map[string]any) { data["extra"] = true },
			"invalid owner":   func(data map[string]any) { data["userId"] = "invalid" },
		}
		for field := range event.Data {
			mutations["missing "+field] = func(data map[string]any) { delete(data, field) }
			mutations["null "+field] = func(data map[string]any) { data[field] = nil }
		}
		for name, mutate := range mutations {
			t.Run(event.EventType+"/"+name, func(t *testing.T) {
				corrupt := event
				corrupt.Data = eventstore.MustData(event.Data)
				mutate(corrupt.Data)
				if _, err := profileEventUserID(corrupt); err == nil {
					t.Fatal("invalid contract accepted")
				}
				handler := &ReadModelEventHandler{}
				if err := handler.handle(t.Context(), eventstore.ResolvedEvent{Event: corrupt}); err == nil {
					t.Fatal("invalid contract reached projection")
				}
				if event.EventType == ProfileImageUploaded {
					bridge := &ProfileImageUploadedAuthUserEventHandler{}
					if err := bridge.handle(t.Context(), eventstore.ResolvedEvent{Event: corrupt}); err == nil {
						t.Fatal("invalid contract reached auth bridge")
					}
				}
			})
		}
	}
}

type profileTestObjects struct {
	url  string
	puts int
}

func (s *profileTestObjects) PublicURL(string) string { return s.url }
func (s *profileTestObjects) PutObject(context.Context, string, []byte, string) error {
	s.puts++
	return nil
}
