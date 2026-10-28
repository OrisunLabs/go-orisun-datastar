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

func TestHTTPPasswordChangeUsesSessionIdentityAndEventPassword(t *testing.T) {
	owner, foreign := uuidv7.NewString(), uuidv7.NewString()
	key, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("secret123"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1, 0).UTC()
	root, err := auth.NewUserRegisteredEvent(owner, "owner-user", "owner@example.com", "Owner", "User", 1990, string(hash), at, at, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	store := eventstoretest.New(t, root)
	// No SQL credential reader is wired: this action needs only event authority.
	app := Server{EventSaver: store, EventRetriever: store}
	form := url.Values{"currentPassword": {"secret123"}, "newPassword": {"updated123"}, "userId": {foreign}, "userRegisteredId": {foreign}, "email": {"foreign@example.com"}}
	request := httptest.NewRequest("POST", "/profile/settings/password", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request = request.WithContext(context.WithValue(request.Context(), userKey, views.User{ID: owner, UserRegisteredID: owner, Email: "stale@example.com"}))
	response := httptest.NewRecorder()
	app.changePassword(response, request)
	if response.Header().Get("Content-Type") != "text/event-stream" || !strings.Contains(response.Body.String(), "/profile/settings") {
		t.Fatalf("password action failed: %s", response.Body.String())
	}
	events := signupFacts(t, store, auth.PasswordChanged)
	if len(events) != 1 {
		t.Fatal("password command did not save")
	}
	changed, err := auth.DecodePasswordChangedEvent(events[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Scopes.UserRegisteredID != owner || bcrypt.CompareHashAndPassword([]byte(changed.PasswordHash), []byte("updated123")) != nil {
		t.Fatal("form identity or stale projection controlled password change")
	}
}
