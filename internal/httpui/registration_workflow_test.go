package httpui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/go-chi/chi/v5"
)

type signupKeys struct {
	keys map[string]protectedpii.SubjectDataKey
}

func (k *signupKeys) GetOrCreateSubjectDataKey(_ context.Context, id string) (protectedpii.SubjectDataKey, error) {
	if v, ok := k.keys[id]; ok {
		return v, nil
	}
	v, err := protectedpii.GenerateSubjectDataKey()
	if err == nil {
		k.keys[id] = v
	}
	return v, err
}
func (k *signupKeys) GetSubjectDataKey(_ context.Context, id string) (protectedpii.SubjectDataKey, bool, error) {
	v, ok := k.keys[id]
	return v, ok, nil
}
func (k *signupKeys) DestroySubjectKey(_ context.Context, id string) error {
	delete(k.keys, id)
	return nil
}

func signupFacts(t *testing.T, store eventstore.Retriever, kind string) []eventstore.ResolvedEvent {
	t.Helper()
	facts, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: kind}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return facts
}
func TestHTTPRegistrationUsesPendingIdentityUntilOTPValidated(t *testing.T) {
	store := eventstoretest.New(t)
	keys := &signupKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	app := Server{EventSaver: store, EventRetriever: store, PIIKeys: keys}
	submit := func(handler http.HandlerFunc, path string, values url.Values, registrationID string) *httptest.ResponseRecorder {
		request := httptest.NewRequest("POST", path, strings.NewReader(values.Encode()))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if registrationID != "" {
			route := chi.NewRouteContext()
			route.URLParams.Add("registrationID", registrationID)
			request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, route))
		}
		response := httptest.NewRecorder()
		handler(response, request)
		if response.Header().Get("Content-Type") != "text/event-stream" {
			t.Fatal("onboarding action did not return SSE")
		}
		return response
	}
	response := submit(app.register, "/register", url.Values{"username": {"http-pending"}, "email": {"http@example.com"}, "password": {"secret123"}}, "")
	pending := signupFacts(t, store, auth.UserRegistrationStarted)
	if len(pending) != 1 || len(signupFacts(t, store, auth.UserRegistered)) != 0 {
		t.Fatal("signup created accepted user")
	}
	id := pending[0].Event.EventID
	if !strings.Contains(response.Body.String(), "/register/"+id+"/validate-email") {
		t.Fatal("signup did not redirect using workflow identity")
	}
	submit(app.sendOTP, "/register/"+id+"/send-email-validation-otp", nil, id)
	generated := signupFacts(t, store, auth.EmailVerificationOTPGenerated)
	if len(generated) != 1 {
		t.Fatal("resend depended on auth user projection")
	}
	value, err := auth.DecodeEmailVerificationOTPGeneratedEvent(generated[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	submit(app.validateEmail, "/register/"+id+"/validate-email", url.Values{"otp": {"wrong"}}, id)
	if len(signupFacts(t, store, auth.UserRegistered)) != 0 {
		t.Fatal("wrong OTP registered a user")
	}
	response = submit(app.validateEmail, "/register/"+id+"/validate-email", url.Values{"otp": {value.OTPCode}}, id)
	if !strings.Contains(response.Body.String(), "/register/"+id+"/processing") || len(signupFacts(t, store, auth.EmailVerificationOTPValidated)) != 1 || len(signupFacts(t, store, auth.UserRegistered)) != 0 || len(signupFacts(t, store, auth.UserRegistrationCompleted)) != 0 {
		t.Fatalf("HTTP executed beyond validation: %s", response.Body.String())
	}
	started, err := auth.DecodeUserRegistrationStartedEvent(pending[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	validated, err := auth.DecodeEmailVerificationOTPValidatedEvent(signupFacts(t, store, auth.EmailVerificationOTPValidated)[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	verifiedAt, _ := time.Parse(time.RFC3339Nano, validated.ValidatedAt)
	if _, err := auth.RegisterUserCommandHandler(t.Context(), auth.RegisterUserCommand{UserRegisteredID: started.UserID, RegistrationDetails: started.RegistrationDetails, EmailVerifiedAt: verifiedAt}, store, store, keys); err != nil {
		t.Fatal(err)
	}
	if err := auth.CompleteUserRegistrationCommandHandler(t.Context(), auth.CompleteUserRegistrationCommand{UserRegisteredID: started.UserID}, store, store); err != nil {
		t.Fatal(err)
	}

	submit(app.validateEmail, "/register/"+id+"/validate-email", url.Values{"otp": {value.OTPCode}}, id)
	if len(signupFacts(t, store, auth.UserRegistered)) != 1 || len(signupFacts(t, store, auth.UserRegistrationCompleted)) != 1 {
		t.Fatal("HTTP retry duplicated registration")
	}
}
