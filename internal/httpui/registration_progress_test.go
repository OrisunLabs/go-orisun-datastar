package httpui

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/viewstore"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type registrationReadyState struct {
	ready  atomic.Bool
	reads  atomic.Int64
	userID string
}

func (s *registrationReadyState) RegistrationReady(_ context.Context, id string) (bool, error) {
	if id != s.userID {
		panic("readiness queried another user")
	}
	s.reads.Add(1)
	return s.ready.Load(), nil
}
func pendingHTTPRegistration(t *testing.T) (*eventstore.EmbeddedOrisun, *signupKeys, auth.StartUserRegistrationResult) {
	t.Helper()
	store := eventstoretest.New(t)
	keys := &signupKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	pending, err := auth.StartUserRegistrationCommandHandler(t.Context(), auth.StartUserRegistrationCommand{Username: "progress-user", Email: "progress@example.com", Password: "secret123"}, store, store, keys)
	if err != nil {
		t.Fatal(err)
	}
	code, err := auth.GenerateEmailVerificationOTPCommandHandler(t.Context(), auth.GenerateEmailVerificationOTPCommand{UserRegistrationStartedID: pending.UserRegistrationStartedID}, store, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.ValidateEmailVerificationOTPCommandHandler(t.Context(), auth.ValidateEmailVerificationOTPCommand{UserRegistrationStartedID: pending.UserRegistrationStartedID, Code: code.Code}, store, store); err != nil {
		t.Fatal(err)
	}
	return store, keys, pending
}
func acceptHTTPRegistration(t *testing.T, store *eventstore.EmbeddedOrisun, keys *signupKeys) {
	t.Helper()
	started, err := auth.DecodeUserRegistrationStartedEvent(signupFacts(t, store, auth.UserRegistrationStarted)[0].Event)
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
}
func TestRegistrationProcessingWaitsForCompletionAndLoginProjection(t *testing.T) {
	for _, scenario := range []string{"projection delay", "completion after projection notification", "reconnect"} {
		t.Run(scenario, func(t *testing.T) {
			store, keys, pending := pendingHTTPRegistration(t)
			readiness := &registrationReadyState{userID: pending.UserID}
			subscriber := streamSubscriber{registered: make(chan *streamSubscription, 4)}
			states := viewstore.NewMemoryStore()
			app := Server{EventSaver: store, EventRetriever: store, RegistrationReadiness: readiness, ViewStore: states, Subscriber: subscriber}
			router := chi.NewRouter()
			app.authRoutes(router)
			server := httptest.NewServer(router)
			defer server.Close()
			client := server.Client()
			client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
			path := "/register/" + pending.UserRegistrationStartedID
			page, err := client.Get(server.URL + path + "/processing")
			if err != nil {
				t.Fatal(err)
			}
			html, err := io.ReadAll(page.Body)
			page.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if page.StatusCode != 200 || !strings.Contains(string(html), "We're creating your account") || !strings.Contains(string(html), "requestCancellation") {
				t.Fatalf("missing processing page: %s", html)
			}
			if strings.Contains(string(html), "type=\"submit\"") {
				t.Fatal("processing page contains another mutation form")
			}
			cookies := page.Cookies()
			if len(cookies) != 1 {
				t.Fatal("anonymous progress view has no session cookie")
			}
			state, ok, err := viewstore.GetState[registrationProgressState](t.Context(), states, viewstore.RegistrationProgressKey(cookies[0].Value, pending.UserRegistrationStartedID))
			if err != nil || !ok || state.Stage != "processing" {
				t.Fatal("page did not initialize per-session view state")
			}
			connect := func() (*http.Response, *bufio.Reader, *streamSubscription, context.CancelFunc) {
				ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
				req, err := http.NewRequestWithContext(ctx, "GET", server.URL+path+"/stream", nil)
				if err != nil {
					t.Fatal(err)
				}
				req.AddCookie(cookies[0])
				req.Header.Set("Accept-Encoding", "identity")
				response, err := client.Do(req)
				if err != nil {
					cancel()
					t.Fatal(err)
				}
				select {
				case sub := <-subscriber.registered:
					return response, bufio.NewReader(response.Body), sub, cancel
				case <-ctx.Done():
					cancel()
					response.Body.Close()
					t.Fatal("progress stream did not subscribe")
				}
				return nil, nil, nil, nil
			}
			response, reader, sub, cancel := connect()
			defer cancel()
			defer response.Body.Close()
			readStreamUntil(t, reader, "We're creating your account")
			if readiness.reads.Load() != 0 {
				t.Fatal("projection readiness used as workflow completion")
			}
			acceptHTTPRegistration(t, store, keys)
			if scenario == "reconnect" {
				cancel()
				response.Body.Close()
				select {
				case <-sub.closed:
				case <-time.After(time.Second):
					t.Fatal("disconnect retained subscription")
				}
			}
			if scenario == "completion after projection notification" {
				readiness.ready.Store(true)
				sub.notify()
				before, err := client.Get(server.URL + path + "/processing")
				if err != nil {
					t.Fatal(err)
				}
				before.Body.Close()
				if before.StatusCode != 200 || readiness.reads.Load() != 0 {
					t.Fatal("projection alone prematurely redirected")
				}
			}
			if err := auth.CompleteUserRegistrationCommandHandler(t.Context(), auth.CompleteUserRegistrationCommand{UserRegisteredID: pending.UserID}, store, store); err != nil {
				t.Fatal(err)
			}
			if scenario == "projection delay" {
				before, err := client.Get(server.URL + path + "/processing")
				if err != nil {
					t.Fatal(err)
				}
				before.Body.Close()
				if before.StatusCode != 200 {
					t.Fatal("workflow completion ignored projection lag")
				}
				readiness.ready.Store(true)
				sub.notify()
			}
			if scenario == "reconnect" {
				readiness.ready.Store(true)
				response, reader, sub, cancel = connect()
				defer cancel()
				defer response.Body.Close()
			}
			// No completion notification is sent in the completion-after-projection case: its periodic refresh must recover it.
			readStreamUntil(t, reader, "window.location.href = \"/login\"")
			select {
			case <-sub.closed:
			case <-time.After(time.Second):
				t.Fatal("terminal redirect retained subscription")
			}
			done, err := client.Get(server.URL + path + "/processing")
			if err != nil {
				t.Fatal(err)
			}
			done.Body.Close()
			if done.StatusCode != http.StatusSeeOther || done.Header.Get("Location") != "/login" {
				t.Fatal("finished page did not recover completion before connection")
			}
		})
	}
}

func TestRegistrationProgressFailureAndInvalidIdentity(t *testing.T) {
	store, _, pending := pendingHTTPRegistration(t)
	failure, err := auth.NewUserRegistrationFailedEvent(uuid.NewString(), pending.UserRegistrationStartedID, time.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SaveEventsV2(t.Context(), []eventstore.DomainEvent{failure}, nil); err != nil {
		t.Fatal(err)
	}
	subscriber := streamSubscriber{registered: make(chan *streamSubscription, 1)}
	app := Server{EventRetriever: store, ViewStore: viewstore.NewMemoryStore(), Subscriber: subscriber}
	router := chi.NewRouter()
	app.authRoutes(router)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest("GET", "/register/"+pending.UserRegistrationStartedID+"/stream", nil))
	if response.Header().Get("Content-Type") != "text/event-stream" || !strings.Contains(response.Body.String(), "Start again") || strings.Contains(response.Body.String(), "window.location.href") {
		t.Fatalf("failure did not terminate without redirect: %s", response.Body.String())
	}
	sub := <-subscriber.registered
	select {
	case <-sub.closed:
	default:
		t.Fatal("failed workflow retained subscription")
	}
	response = httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest("GET", "/register/invalid/stream", nil))
	if response.Header().Get("Content-Type") != "text/event-stream" || !strings.Contains(response.Body.String(), "Registration is unavailable") {
		t.Fatal("invalid stream identity did not return safe SSE error")
	}
}
