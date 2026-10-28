package httpui

import (
	"bufio"
	"context"
	"github.com/OrisunLabs/go-orisun-datastar/internal/viewstore"
	"github.com/go-chi/chi/v5"
	"io"
	"net/http"
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

type deletionProgressEffects struct {
	dataCalls, keyCalls int
	owner               string
}

func (e *deletionProgressEffects) DeleteAccountData(_ context.Context, id string) error {
	if id != e.owner {
		panic("foreign cleanup")
	}
	e.dataCalls++
	return nil
}
func (e *deletionProgressEffects) DestroySubjectKey(_ context.Context, id string) error {
	if id != e.owner {
		panic("foreign key destruction")
	}
	e.keyCalls++
	return nil
}

func TestHTTPDeletionRequestUsesSessionIDsAndEventPassword(t *testing.T) {
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
	app := Server{EventSaver: store, EventRetriever: store, Sessions: auth.NewSessionManager(nil, nil, false), ViewStore: viewstore.NewMemoryStore()}
	form := url.Values{"password": {"secret123"}, "authUserId": {foreign}, "userId": {foreign}, "userRegisteredId": {foreign}, "email": {"foreign@example.com"}}
	request := httptest.NewRequest("POST", "/settings/delete-account", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request = request.WithContext(context.WithValue(request.Context(), userKey, views.User{ID: owner, UserRegisteredID: owner, Email: "stale@example.com"}))
	response := httptest.NewRecorder()
	app.deleteAccount(response, request)
	if response.Header().Get("Content-Type") != "text/event-stream" || !strings.Contains(response.Body.String(), "/account-deletion/") {
		t.Fatalf("deletion request failed: %s", response.Body.String())
	}
	events := signupFacts(t, store, auth.AccountDeletionRequested)
	if len(events) != 1 {
		t.Fatal("deletion request did not save")
	}
	changed, err := auth.DecodeAccountDeletionRequestedEvent(events[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Scopes.UserRegisteredID != owner || changed.AuthUserID != owner {
		t.Fatal("form identity or stale projection controlled deletion request")
	}
	if len(signupFacts(t, store, auth.AccountDeleted)) != 0 {
		t.Fatal("HTTP executed deletion instead of requesting it")
	}
	cleared := false
	for _, cookie := range response.Result().Cookies() {
		if cookie.Name == app.Sessions.SessionCookieName() && cookie.MaxAge < 0 {
			cleared = true
		}
	}
	if !cleared {
		t.Fatal("request did not sign out immediately")
	}
	effects := &deletionProgressEffects{owner: owner}
	router := chi.NewRouter()
	// Progress routes remain reachable after deletion removes the authenticated session.
	app.authRoutes(router)
	server := httptest.NewServer(router)
	defer server.Close()
	server.Client().Transport = &http.Transport{DisableCompression: true}
	path := "/account-deletion/" + changed.AccountDeletionRequestedID
	assertPage := func(want string) {
		t.Helper()
		response, err := server.Client().Get(server.URL + path + "/processing")
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		if response.StatusCode != http.StatusOK || !strings.Contains(string(body), want) {
			t.Fatalf("processing page: %d %s", response.StatusCode, body)
		}
	}
	assertPage("We're deleting your account")
	connect := func() (*http.Response, *bufio.Reader) {
		t.Helper()
		response, err := server.Client().Get(server.URL + path + "/stream")
		if err != nil {
			t.Fatal(err)
		}
		return response, bufio.NewReader(response.Body)
	}
	stream, reader := connect()
	readStreamUntil(t, reader, "We're deleting your account")
	stream.Body.Close()
	if effects.dataCalls != 0 || effects.keyCalls != 0 {
		t.Fatal("progress executed cleanup")
	}
	if err := auth.DeleteAccountCommandHandler(t.Context(), auth.DeleteAccountCommand{AccountDeletedID: changed.OperationID, UserRegisteredID: owner, AuthUserID: owner}, effects, effects, store, store); err != nil {
		t.Fatal(err)
	}
	assertPage("We're deleting your account")
	stream, reader = connect()
	defer stream.Body.Close()
	readStreamUntil(t, reader, "We're deleting your account")
	if err := auth.CompleteAccountDeletionCommandHandler(t.Context(), auth.CompleteAccountDeletionCommand{AccountDeletedID: changed.OperationID}, store, store); err != nil {
		t.Fatal(err)
	}
	readStreamUntil(t, reader, "Your account has been deleted")
	remaining, err := io.ReadAll(reader)
	if err != nil || strings.TrimSpace(string(remaining)) != "" {
		t.Fatalf("terminal stream did not close cleanly: %s %v", remaining, err)
	}
	stream.Body.Close()
	assertPage("Your account has been deleted")
	stream, reader = connect()
	readStreamUntil(t, reader, "Your account has been deleted")
	stream.Body.Close()
	if effects.dataCalls != 1 || effects.keyCalls != 1 {
		t.Fatal("progress repeated cleanup")
	}
	for _, suffix := range []string{"processing", "stream"} {
		out := httptest.NewRecorder()
		router.ServeHTTP(out, httptest.NewRequest("GET", "/account-deletion/invalid/"+suffix, nil))
		if suffix == "processing" && out.Code != http.StatusNotFound {
			t.Fatal("invalid page accepted")
		}
		if suffix == "stream" && (out.Header().Get("Content-Type") != "text/event-stream" || !strings.Contains(out.Body.String(), "Account deletion is unavailable")) {
			t.Fatal("invalid identity did not return safe SSE")
		}
	}

}
