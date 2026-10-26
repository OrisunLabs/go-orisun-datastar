package httpui

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/profile"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/todo"
	"github.com/OrisunLabs/go-orisun-datastar/internal/resources"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
	"github.com/OrisunLabs/go-orisun-datastar/internal/viewstore"
)

type contextKey string

const userKey contextKey = "user"

type MessageSubscriber interface {
	Subscribe(ctx context.Context, subject string, handle func(context.Context, []byte)) (eventstore.MessageSubscription, error)
}

type SessionManager interface {
	Login(ctx context.Context, emailAddress, password string) (views.User, string, error)
	Logout(ctx context.Context, token string) error
	CurrentUser(ctx context.Context, r *http.Request) (views.User, bool, error)
	SetSessionCookie(w http.ResponseWriter, token string)
	ClearSessionCookie(w http.ResponseWriter)
	SessionCookieName() string
}

type AuthUserReader interface {
	UserByIDOrRegisteredID(ctx context.Context, id string) (views.User, error)
}

type ProfileReader interface {
	User(ctx context.Context, userRegisteredID string) (views.User, error)
}

type VerificationStore interface {
	auth.PasswordResetReader
}

type Server struct {
	Sessions            SessionManager
	AuthUsers           AuthUserReader
	PIIKeys             auth.SubjectPiiKeyPort
	PasswordCredentials auth.PasswordCredentialReader
	Verifications       VerificationStore
	Todos               todo.TodoReadModelReader
	Profiles            ProfileReader
	EventSaver          eventstore.SaverV2
	EventRetriever      eventstore.Retriever
	ProfileStorage      profile.ObjectStore
	Subscriber          MessageSubscriber
	ViewStore           viewstore.Store
	Development         bool
	Logger              *slog.Logger
}

func (s Server) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(requestLogging(s.Logger))
	r.Use(middleware.Recoverer)
	r.Use(middleware.Compress(5,
		"text/html",
		"text/css",
		"text/plain",
		"text/javascript",
		"application/javascript",
		"application/json",
		"image/svg+xml",
	))

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "timestamp": time.Now().UTC().Format(time.RFC3339)})
	})
	if s.Development {
		setupReload(r)
	}
	s.monitoringRoutes(r)
	r.Handle("/static/*", resources.Handler())
	r.Get("/", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/todos", http.StatusFound) })

	s.authRoutes(r)

	r.Group(func(r chi.Router) {
		r.Use(s.requireVerifiedEmail)
		s.todoRoutes(r)
		s.profileRoutes(r)
	})

	return r
}

func render(component interface {
	Render(context.Context, io.Writer) error
}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_ = component.Render(r.Context(), w)
	}
}

func (s Server) requireVerifiedEmail(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok, err := s.Sessions.CurrentUser(r.Context(), r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Redirect(w, r, "/login", http.StatusFound)
			return
		}
		if !user.EmailVerified {
			http.Redirect(w, r, "/register/"+user.ID+"/validate-email", http.StatusFound)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), userKey, user)))
	})
}

func currentUser(r *http.Request) views.User {
	user, _ := r.Context().Value(userKey).(views.User)
	return user
}

func (s Server) sessionID(r *http.Request) string {
	cookie, err := r.Cookie(s.Sessions.SessionCookieName())
	if err == nil && cookie.Value != "" {
		return cookie.Value
	}
	return currentUser(r).UserRegisteredID
}
