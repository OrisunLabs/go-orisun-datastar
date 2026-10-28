package httpui

import (
	"context"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
	"golang.org/x/crypto/bcrypt"
)

func TestHTTPNameCommandUsesAuthenticatedIdentity(t *testing.T) {
	owner, foreign := uuidv7.NewString(), uuidv7.NewString()
	keys := &signupKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	ownerKey, err := keys.GetOrCreateSubjectDataKey(t.Context(), owner)
	if err != nil {
		t.Fatal(err)
	}
	foreignKey, err := keys.GetOrCreateSubjectDataKey(t.Context(), foreign)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("secret123"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1, 0).UTC()
	root, err := auth.NewUserRegisteredEvent(owner, "owner-user", "owner@example.com", "Owner", "User", 1990, string(hash), at, at, ownerKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	other, err := auth.NewUserRegisteredEvent(foreign, "foreign-user", "foreign@example.com", "Foreign", "User", 1990, string(hash), at, at, foreignKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	store := eventstoretest.New(t, root, other)
	app := Server{EventSaver: store, EventRetriever: store, PIIKeys: keys}
	for range 2 {
		form := url.Values{"name": {"  Owner Updated  "}, "userId": {foreign}, "userRegisteredId": {foreign}}
		request := httptest.NewRequest("POST", "/profile/settings/name", strings.NewReader(form.Encode()))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		// Unrelated/stale view fields are not domain command input.
		request = request.WithContext(context.WithValue(request.Context(), userKey, views.User{ID: owner, UserRegisteredID: owner, Email: "stale@example.com", Name: "Stale Name"}))
		response := httptest.NewRecorder()
		app.updateName(response, request)
		if response.Header().Get("Content-Type") != "text/event-stream" || !strings.Contains(response.Body.String(), "/profile/settings") {
			t.Fatalf("name action failed: %s", response.Body.String())
		}
	}
	events := signupFacts(t, store, auth.UserNameChanged)
	if len(events) != 1 {
		t.Fatal("retry duplicated name event")
	}
	changed, err := auth.DecodeUserNameChangedEvent(events[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	name, err := protectedpii.FromEnv().DecryptWithDataKey(changed.Name, ownerKey)
	if err != nil || name != "Owner Updated" || changed.Scopes.UserRegisteredID != owner {
		t.Fatal("form identity or stale view state controlled rename")
	}
}
