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
	"github.com/go-chi/chi/v5"
	"golang.org/x/crypto/bcrypt"
)

type resetHTTPIdentity struct{ owner, request string }

func (r resetHTTPIdentity) PasswordResetByToken(context.Context, string) (auth.PasswordResetVerification, error) {
	return auth.PasswordResetVerification{ID: r.request, UserID: r.owner}, nil
}
func (r resetHTTPIdentity) UserByIDOrRegisteredID(context.Context, string) (views.User, error) {
	return views.User{ID: r.owner, UserRegisteredID: r.owner}, nil
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
	app := Server{EventSaver: store, EventRetriever: store, Verifications: identity, AuthUsers: identity}
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
	if !strings.Contains(out.Body.String(), "/login") {
		t.Fatalf("reset did not redirect: %s", out.Body.String())
	}
	if len(signupFacts(t, store, auth.PasswordResetAuthorized)) != 1 || len(signupFacts(t, store, auth.PasswordChanged)) != 0 || len(signupFacts(t, store, auth.PasswordResetCompleted)) != 0 {
		t.Fatal("HTTP executed beyond authorization")
	}
	authorization, err := auth.DecodePasswordResetAuthorizedEvent(signupFacts(t, store, auth.PasswordResetAuthorized)[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.SetPasswordCommandHandler(t.Context(), auth.SetPasswordCommand{PasswordChangedID: authorization.OperationID, UserRegisteredID: owner, PasswordHash: authorization.PasswordHash}, store, store); err != nil {
		t.Fatal(err)
	}
	if len(signupFacts(t, store, auth.PasswordResetCompleted)) != 0 {
		t.Fatal("domain command completed workflow")
	}
	if err := auth.CompletePasswordResetCommandHandler(t.Context(), auth.CompletePasswordResetCommand{PasswordChangedID: authorization.OperationID}, store, store); err != nil {
		t.Fatal(err)
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
}
