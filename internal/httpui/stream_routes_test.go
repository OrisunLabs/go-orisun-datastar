package httpui

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
	"github.com/OrisunLabs/go-orisun-datastar/internal/viewstore"
	"github.com/go-chi/chi/v5"
)

type streamSessions struct{ SessionManager }

func (streamSessions) SessionCookieName() string { return "session" }

type streamTodos struct{ version *atomic.Int64 }

func (s streamTodos) List(context.Context, string) ([]views.Todo, error) {
	return []views.Todo{{TodoID: "todo", Title: fmt.Sprintf("projected-%d", s.version.Load())}}, nil
}

type streamProfiles struct{ version *atomic.Int64 }

func (s streamProfiles) User(context.Context, string) (views.User, error) {
	return views.User{UserRegisteredID: "user", Name: fmt.Sprintf("projected-%d", s.version.Load()), Username: "user"}, nil
}

type streamSubscription struct {
	notify func()
	closed chan struct{}
	once   sync.Once
}

func (s *streamSubscription) Close() error { s.once.Do(func() { close(s.closed) }); return nil }

type streamSubscriber struct{ registered chan *streamSubscription }

func (s streamSubscriber) Subscribe(ctx context.Context, _ string, callback func(context.Context, []byte)) (eventstore.MessageSubscription, error) {
	sub := &streamSubscription{notify: func() { callback(ctx, []byte(`{"userId":"user"}`)) }, closed: make(chan struct{})}
	s.registered <- sub
	return sub, nil
}
func readStreamUntil(t *testing.T, reader *bufio.Reader, expected string) {
	t.Helper()
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("stream ended before %q: %v", expected, err)
		}
		if strings.Contains(line, expected) {
			return
		}
	}
}
func TestReadStreamsRefreshAndReconnect(t *testing.T) {
	for _, route := range []string{"todo", "profile"} {
		t.Run(route, func(t *testing.T) {
			var version atomic.Int64
			version.Store(1)
			subscriber := streamSubscriber{registered: make(chan *streamSubscription, 2)}
			app := Server{Sessions: streamSessions{}, Todos: streamTodos{&version}, Profiles: streamProfiles{&version}, Subscriber: subscriber, ViewStore: viewstore.NewMemoryStore()}
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r = r.WithContext(context.WithValue(r.Context(), userKey, views.User{UserRegisteredID: "user", EmailVerified: true}))
				if route == "todo" {
					app.todosStream(w, r)
				} else {
					app.profileStream(w, r)
				}
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			connect := func() (*http.Response, *bufio.Reader, *streamSubscription, context.CancelFunc) {
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				request, err := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Accept-Encoding", "identity")
				response, err := server.Client().Do(request)
				if err != nil {
					cancel()
					t.Fatal(err)
				}
				if response.StatusCode != 200 || response.Header.Get("Content-Type") != "text/event-stream" {
					t.Fatal("invalid stream response")
				}
				select {
				case sub := <-subscriber.registered:
					return response, bufio.NewReader(response.Body), sub, cancel
				case <-ctx.Done():
					cancel()
					response.Body.Close()
					t.Fatal("stream did not subscribe")
				}
				return nil, nil, nil, nil
			}
			response, reader, sub, cancel := connect()
			defer cancel()
			defer response.Body.Close()
			readStreamUntil(t, reader, "projected-1")
			version.Store(2)
			sub.notify()
			readStreamUntil(t, reader, "projected-2")
			cancel()
			response.Body.Close()
			select {
			case <-sub.closed:
			case <-time.After(2 * time.Second):
				t.Fatal("disconnect retained subscription")
			}
			version.Store(3)
			response2, reader2, sub2, cancel2 := connect()
			defer cancel2()
			defer response2.Body.Close()
			readStreamUntil(t, reader2, "projected-3")
			cancel2()
			response2.Body.Close()
			select {
			case <-sub2.closed:
			case <-time.After(2 * time.Second):
				t.Fatal("reconnect retained subscription")
			}
		})
	}
}
func TestReloadStreamDisconnectAndPing(t *testing.T) {
	router := chi.NewRouter()
	setupReload(router)
	ended := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		router.ServeHTTP(w, r)
		if r.URL.Path == "/reload" {
			ended <- struct{}{}
		}
	}))
	defer server.Close()
	for _, ping := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		request, err := http.NewRequestWithContext(ctx, "GET", server.URL+"/reload", nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Accept-Encoding", "identity")
		response, err := server.Client().Do(request)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if ping {
			result, err := server.Client().Get(server.URL + "/hotreload")
			if err != nil {
				t.Fatal(err)
			}
			result.Body.Close()
			readStreamUntil(t, bufio.NewReader(response.Body), "window.location.reload()")
		}
		cancel()
		response.Body.Close()
		select {
		case <-ended:
		case <-time.After(2 * time.Second):
			t.Fatal("reload stream retained request")
		}
	}
}

func TestOpsStreamDisconnect(t *testing.T) {
	app := Server{ViewStore: viewstore.NewMemoryStore()}
	ended := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { app.opsStream(w, r); close(ended) }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "GET", server.URL+"/ops/stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Accept-Encoding", "identity")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	readStreamUntil(t, bufio.NewReader(response.Body), "ops-metrics")
	cancel()
	response.Body.Close()
	select {
	case <-ended:
	case <-time.After(2 * time.Second):
		t.Fatal("ops stream did not stop")
	}
}
