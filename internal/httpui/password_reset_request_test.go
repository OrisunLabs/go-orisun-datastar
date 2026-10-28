package httpui

import (
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"golang.org/x/crypto/bcrypt"
)

func TestHTTPResetRequestUsesEventsAndKeepsGenericAcknowledgement(t *testing.T) {
	owner := uuidv7.NewString()
	keys := &signupKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	key, err := keys.GetOrCreateSubjectDataKey(t.Context(), owner)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("secret123"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1, 0).UTC()
	root, err := auth.NewUserRegisteredEvent(owner, "reset-user", "reset@example.com", "Reset", "User", 1990, string(hash), at, at, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	store := eventstoretest.New(t, root)
	// No user, credential or verification projection is wired.
	app := Server{EventSaver: store, EventRetriever: store, PIIKeys: keys}
	submit := func(email string) *httptest.ResponseRecorder {
		request := httptest.NewRequest("POST", "/forgot-password", strings.NewReader(url.Values{"email": {email}}.Encode()))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response := httptest.NewRecorder()
		app.forgotPassword(response, request)
		if response.Header().Get("Content-Type") != "text/event-stream" || !strings.Contains(response.Body.String(), "/login") {
			t.Fatal("reset request did not acknowledge over SSE")
		}
		return response
	}
	accepted := submit("  RESET@EXAMPLE.COM  ")
	if len(signupFacts(t, store, auth.PasswordResetRequested)) != 1 {
		t.Fatal("event-authoritative reset depended on SQL projection")
	}
	for _, email := range []string{"unknown@example.com", ""} {
		unknown := submit(email)
		if accepted.Body.String() != unknown.Body.String() {
			t.Fatal("public reset response revealed registration status")
		}
		if len(signupFacts(t, store, auth.PasswordResetRequested)) != 1 {
			t.Fatal("unknown email saved reset")
		}
	}
	saved, err := auth.DecodePasswordResetRequestedEvent(signupFacts(t, store, auth.PasswordResetRequested)[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	recipient, err := protectedpii.FromEnv().DecryptWithDataKey(saved.Email, key)
	if err != nil || recipient != "reset@example.com" || saved.Scopes.UserRegisteredID != owner {
		t.Fatal("reset did not use accepted recipient and owner")
	}
}
