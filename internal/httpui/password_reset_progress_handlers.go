package httpui

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
	"github.com/OrisunLabs/go-orisun-datastar/internal/viewstore"
	"github.com/go-chi/chi/v5"
	"github.com/starfederation/datastar-go/datastar"
)

type PasswordResetReadinessReader interface {
	PasswordResetReady(context.Context, string, eventstore.Position) (bool, error)
}
type passwordResetProgressState struct {
	Stage string `json:"stage"`
}

var errPasswordResetStreamFinished = errors.New("password reset stream finished")

func (s Server) passwordResetProgressState(ctx context.Context, id string) (passwordResetProgressState, error) {
	status, err := auth.ReadPasswordResetStatus(ctx, id, s.EventRetriever)
	if err != nil {
		return passwordResetProgressState{}, err
	}
	stage := status.Stage
	if stage == "complete" {
		ready, err := s.PasswordResetReadiness.PasswordResetReady(ctx, status.UserID, status.EffectPosition)
		if err != nil {
			return passwordResetProgressState{}, err
		}
		stage = "processing"
		if ready {
			stage = "ready"
		}
	}
	return passwordResetProgressState{Stage: stage}, nil
}
func (s Server) passwordResetProcessingPage(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "authorizationID")
	state, err := s.passwordResetProgressState(r.Context(), id)
	if err != nil {
		code := http.StatusServiceUnavailable
		if errors.Is(err, eventstore.ErrNotFound) || errors.Is(err, eventstore.ErrInvalidEvent) {
			code = http.StatusNotFound
		}
		http.Error(w, "Password reset is unavailable. Please try again.", code)
		return
	}
	key := viewstore.PasswordResetProgressKey(authProgressViewSessionID(w, r, "password_reset_view", "/password-reset"), id)
	if err := viewstore.PutState(r.Context(), s.ViewStore, key, state); err != nil {
		http.Error(w, "Could not load password reset progress. Please refresh.", http.StatusServiceUnavailable)
		return
	}
	_ = views.PasswordResetProcessing(id, state.Stage).Render(r.Context(), w)
}
func (s Server) passwordResetProgressStream(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "authorizationID")
	if _, err := s.passwordResetProgressState(r.Context(), id); err != nil {
		writeSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error {
			return alert(sse, "Password reset is unavailable. Please refresh.")
		})
		return
	}
	key := viewstore.PasswordResetProgressKey(authProgressViewSessionID(w, r, "password_reset_view", "/password-reset"), id)
	sse := newSSE(w, r)
	err := streamViewStoreFatMorph[passwordResetProgressState](r.Context(), sse, viewStoreFatMorphConfig[passwordResetProgressState]{
		Key: key, Store: s.ViewStore,
		Refresh: func(ctx context.Context) error {
			state, err := s.passwordResetProgressState(ctx, id)
			if err != nil {
				return err
			}
			return viewstore.PutState(ctx, s.ViewStore, key, state)
		},
		// Completion and projection can arrive in either order. Poll current state on reconnect and while waiting.
		Subscribe: func(ctx context.Context, notify func()) (closeableSubscription, error) {
			return newTickerSubscription(ctx, 2*time.Second, notify), nil
		},
		Patch: func(sse *datastar.ServerSentEventGenerator, state passwordResetProgressState) error {
			if err := sse.PatchElementTempl(views.PasswordResetProgressPanel(id, state.Stage), datastar.WithSelectorID("password-reset-status")); err != nil {
				return err
			}
			if state.Stage == "ready" || state.Stage == "unavailable" {
				return errPasswordResetStreamFinished
			}
			return nil
		},
	})
	if err != nil && !errors.Is(err, errPasswordResetStreamFinished) {
		_ = alert(sse, "Could not check password reset progress. Please refresh.")
	}
}
