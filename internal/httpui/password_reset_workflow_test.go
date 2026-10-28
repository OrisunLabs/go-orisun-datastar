package httpui

import (
	"bufio"
	"context"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/viewstore"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/go-chi/chi/v5"
	"golang.org/x/crypto/bcrypt"
)

type resetHTTPIdentity struct{ owner, request string }

func (r resetHTTPIdentity) PasswordResetByToken(context.Context, string) (auth.PasswordResetVerification, error) {
	return auth.PasswordResetVerification{ID: r.request, UserID: r.owner}, nil
}

type resetReadiness struct {
	ready    atomic.Bool
	reads    atomic.Int64
	position atomic.Pointer[eventstore.Position]
}

func (s *resetReadiness) PasswordResetReady(_ context.Context, _ string, p eventstore.Position) (bool, error) {
	s.reads.Add(1)
	s.position.Store(&p)
	return s.ready.Load(), nil
}

func TestHTTPPasswordResetAppliesIndependentPasswordFact(t *testing.T) {
	owner, request := uuidv7.NewString(), uuidv7.NewString()
	key, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte("old-secret"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	root, err := auth.NewUserRegisteredEvent(owner, "reset-user", "reset@example.com", "Reset", "User", 1990, string(hash), time.Unix(1, 0).UTC(), time.Unix(1, 0).UTC(), key, nil)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := auth.NewPasswordResetRequestedEvent(request, "reset@example.com", "token", time.Now().Add(time.Hour), owner, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	store := eventstoretest.New(t, root, proposal)
	identity := resetHTTPIdentity{owner, request}
	app := Server{EventSaver: store, EventRetriever: store, Verifications: identity}
	submit := func(token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/reset-password/"+token, strings.NewReader(url.Values{"password": {"updated123"}}.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		route := chi.NewRouteContext()
		route.URLParams.Add("token", token)
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, route))
		out := httptest.NewRecorder()
		app.resetPassword(out, req)
		if out.Header().Get("Content-Type") != "text/event-stream" {
			t.Fatal("reset did not return SSE")
		}
		return out
	}
	submit("wrong-token")
	if len(signupFacts(t, store, auth.PasswordChanged)) != 0 {
		t.Fatal("invalid token changed password")
	}
	out := submit("token")
	if !strings.Contains(out.Body.String(), "/password-reset/") {
		t.Fatalf("reset did not redirect: %s", out.Body.String())
	}
	if len(signupFacts(t, store, auth.PasswordResetAuthorized)) != 1 || len(signupFacts(t, store, auth.PasswordChanged)) != 0 || len(signupFacts(t, store, auth.PasswordResetCompleted)) != 0 {
		t.Fatal("HTTP executed beyond authorization")
	}
	authorization, err := auth.DecodePasswordResetAuthorizedEvent(signupFacts(t, store, auth.PasswordResetAuthorized)[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	readiness := &resetReadiness{}
	app.PasswordResetReadiness = readiness
	app.ViewStore = viewstore.NewMemoryStore()
	router := chi.NewRouter()
	app.authRoutes(router)
	server := httptest.NewServer(router)
	server.Client().Transport = &http.Transport{DisableCompression: true}
	defer server.Close()
	path := "/password-reset/" + authorization.PasswordResetAuthorizedID
	assertStage := func(want string) {
		t.Helper()
		status, err := auth.ReadPasswordResetStatus(t.Context(), authorization.PasswordResetAuthorizedID, store)
		if err != nil || status.Stage != want {
			t.Fatalf("stage %s: %+v %v", want, status, err)
		}
	}
	assertStage("processing")
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
	assertPage("updating your password")
	if readiness.reads.Load() != 0 {
		t.Fatal("read credentials before workflow completion")
	}
	if err := auth.SetPasswordCommandHandler(t.Context(), auth.SetPasswordCommand{PasswordChangedID: authorization.OperationID, UserRegisteredID: owner, PasswordHash: authorization.PasswordHash}, store, store); err != nil {
		t.Fatal(err)
	}
	if len(signupFacts(t, store, auth.PasswordResetCompleted)) != 0 {
		t.Fatal("domain command completed workflow")
	}
	assertStage("processing")
	assertPage("updating your password")
	if err := auth.CompletePasswordResetCommandHandler(t.Context(), auth.CompletePasswordResetCommand{PasswordChangedID: authorization.OperationID}, store, store); err != nil {
		t.Fatal(err)
	}
	assertStage("complete")
	assertPage("updating your password")
	waiting, err := server.Client().Get(server.URL + path + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(waiting.Body)
	readStreamUntil(t, reader, "updating your password")
	waiting.Body.Close()
	// Reconnect while still processing, then let the ticker catch projected readiness.
	waiting, err = server.Client().Get(server.URL + path + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer waiting.Body.Close()
	reader = bufio.NewReader(waiting.Body)
	readStreamUntil(t, reader, "updating your password")
	readiness.ready.Store(true)
	readStreamUntil(t, reader, "Your password has been reset")
	waiting.Body.Close()
	assertPage("Your password has been reset")
	effectPosition := signupFacts(t, store, auth.PasswordChanged)[0].Position
	if readiness.position.Load() == nil || *readiness.position.Load() != effectPosition {
		t.Fatal("readiness did not use effect position")
	}
	response, err := server.Client().Get(server.URL + path + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || !strings.Contains(string(body), "Your password has been reset") || strings.Contains(string(body), "event: datastar-execute-script") {
		t.Fatalf("completion stream: %s %v", body, err)
	}
	for _, kind := range []string{auth.PasswordResetAuthorized, auth.PasswordChanged, auth.PasswordResetCompleted} {
		if len(signupFacts(t, store, kind)) != 1 {
			t.Fatalf("reset missing %s", kind)
		}
	}
	effect, err := auth.DecodePasswordChangedEvent(signupFacts(t, store, auth.PasswordChanged)[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	if bcrypt.CompareHashAndPassword([]byte(effect.PasswordHash), []byte("updated123")) != nil {
		t.Fatal("reset applied wrong password")
	}
	submit("token")
	if len(signupFacts(t, store, auth.PasswordChanged)) != 1 {
		t.Fatal("completed token applied a second password change")
	}

	deleted, err := auth.NewAccountDeletedEvent(uuidv7.NewString(), time.Now(), owner, owner, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveEventsV2(t.Context(), []eventstore.DomainEvent{deleted}, nil); err != nil {
		t.Fatal(err)
	}
	assertStage("unavailable")
	assertPage("This password reset is no longer available")
	invalid := httptest.NewRecorder()
	router.ServeHTTP(invalid, httptest.NewRequest("GET", "/password-reset/invalid/stream", nil))
	if invalid.Header().Get("Content-Type") != "text/event-stream" || !strings.Contains(invalid.Body.String(), "Password reset is unavailable") {
		t.Fatal("invalid progress did not return safe SSE")
	}
}
