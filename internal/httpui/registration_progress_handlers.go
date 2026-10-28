package httpui

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/starfederation/datastar-go/datastar"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/profile"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
	"github.com/OrisunLabs/go-orisun-datastar/internal/viewstore"
)

type RegistrationReadinessReader interface {
	RegistrationReady(context.Context, string) (bool, error)
}
type registrationProgressState struct {
	Stage string `json:"stage"`
}

var errRegistrationStreamFinished = errors.New("registration stream finished")

func (s Server) registrationProgressState(ctx context.Context, id string) (registrationProgressState, string, error) {
	status, err := auth.ReadRegistrationStatus(ctx, id, s.EventRetriever)
	if err != nil {
		return registrationProgressState{}, "", err
	}
	stage := string(status.Stage)
	if status.Stage == auth.RegistrationComplete {
		ready, err := s.RegistrationReadiness.RegistrationReady(ctx, status.UserID)
		if err != nil {
			return registrationProgressState{}, "", err
		}
		stage = "processing"
		if ready {
			stage = "ready"
		}
	}
	return registrationProgressState{Stage: stage}, status.UserID, nil
}
func (s Server) registrationProcessingPage(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "registrationID")
	state, _, err := s.registrationProgressState(r.Context(), id)
	if err != nil {
		if errors.Is(err, eventstore.ErrNotFound) || errors.Is(err, eventstore.ErrInvalidEvent) {
			http.Error(w, "Registration is unavailable.", http.StatusNotFound)
		} else {
			http.Error(w, "Could not check registration progress. Please refresh.", http.StatusServiceUnavailable)
		}
		return
	}
	if state.Stage == "verifying" {
		http.Redirect(w, r, "/register/"+id+"/validate-email", http.StatusSeeOther)
		return
	}
	if state.Stage == "ready" {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	key := viewstore.RegistrationProgressKey(registrationViewSessionID(w, r), id)
	if err := viewstore.PutState(r.Context(), s.ViewStore, key, state); err != nil {
		http.Error(w, "Could not load registration progress. Please refresh.", http.StatusServiceUnavailable)
		return
	}
	_ = views.RegistrationProcessing(id, state.Stage).Render(r.Context(), w)
}
func (s Server) registrationProgressStream(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "registrationID")
	_, userID, err := s.registrationProgressState(r.Context(), id)
	if err != nil {
		writeSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error {
			return alert(sse, "Registration is unavailable. Please refresh.")
		})
		return
	}
	key := viewstore.RegistrationProgressKey(registrationViewSessionID(w, r), id)
	sse := newSSE(w, r)
	err = streamViewStoreFatMorph[registrationProgressState](r.Context(), sse, viewStoreFatMorphConfig[registrationProgressState]{
		Key: key, Store: s.ViewStore,
		Refresh: func(ctx context.Context) error {
			state, _, err := s.registrationProgressState(ctx, id)
			if err != nil {
				return err
			}
			return viewstore.PutState(ctx, s.ViewStore, key, state)
		},
		Subscribe: func(ctx context.Context, notify func()) (closeableSubscription, error) {
			subscription, err := s.Subscriber.Subscribe(ctx, profile.Channel(userID), func(context.Context, []byte) { notify() })
			if err != nil {
				return nil, err
			}
			// Workflow completion can follow the projection notification; reconnect/poll refreshes recover that gap.
			return &registrationProgressSubscription{notification: subscription, ticker: newTickerSubscription(ctx, 2*time.Second, notify)}, nil
		},
		Patch: func(sse *datastar.ServerSentEventGenerator, state registrationProgressState) error {
			if state.Stage == "ready" {
				if err := sse.Redirect("/login"); err != nil {
					return err
				}
				return errRegistrationStreamFinished
			}
			if state.Stage == "verifying" {
				if err := sse.Redirect("/register/" + id + "/validate-email"); err != nil {
					return err
				}
				return errRegistrationStreamFinished
			}
			if err := sse.PatchElementTempl(views.RegistrationProgressPanel(id, state.Stage), datastar.WithSelectorID("registration-status")); err != nil {
				return err
			}
			if state.Stage == "failed" || state.Stage == "unavailable" {
				return errRegistrationStreamFinished
			}
			return nil
		},
	})
	if err != nil && !errors.Is(err, errRegistrationStreamFinished) {
		_ = alert(sse, "Could not check registration progress. Please refresh.")
	}
}

type registrationProgressSubscription struct {
	notification closeableSubscription
	ticker       closeableSubscription
}

func (s *registrationProgressSubscription) Close() error {
	_ = s.ticker.Close()
	return s.notification.Close()
}

func registrationViewSessionID(w http.ResponseWriter, r *http.Request) string {
	return authProgressViewSessionID(w, r, "registration_view", "/register")
}

func authProgressViewSessionID(w http.ResponseWriter, r *http.Request, name, path string) string {
	if cookie, err := r.Cookie(name); err == nil {
		if id, err := uuid.Parse(cookie.Value); err == nil && id != uuid.Nil && id.String() == cookie.Value {
			return cookie.Value
		}
	}
	id := uuidv7.NewString()
	http.SetCookie(w, &http.Cookie{Name: name, Value: id, Path: path, MaxAge: 3600, HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteLaxMode})
	return id
}
