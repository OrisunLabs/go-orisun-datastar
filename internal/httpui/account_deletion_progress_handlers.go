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

type accountDeletionProgressState struct {
	Stage string `json:"stage"`
}

var errAccountDeletionStreamFinished = errors.New("account deletion stream finished")

func (s Server) accountDeletionProgressState(ctx context.Context, id string) (accountDeletionProgressState, error) {
	stage, err := auth.ReadAccountDeletionStatus(ctx, id, s.EventRetriever)
	if err != nil {
		return accountDeletionProgressState{}, err
	}
	return accountDeletionProgressState{Stage: stage}, nil
}

func (s Server) accountDeletionProcessingPage(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "requestID")
	state, err := s.accountDeletionProgressState(r.Context(), id)
	if err != nil {
		code := http.StatusServiceUnavailable
		if errors.Is(err, eventstore.ErrNotFound) || errors.Is(err, eventstore.ErrInvalidEvent) {
			code = http.StatusNotFound
		}
		http.Error(w, "Account deletion is unavailable. Please try again.", code)
		return
	}
	key := viewstore.AccountDeletionProgressKey(authProgressViewSessionID(w, r, "account_deletion_view", "/account-deletion"), id)
	if err := viewstore.PutState(r.Context(), s.ViewStore, key, state); err != nil {
		http.Error(w, "Could not load account deletion progress. Please refresh.", http.StatusServiceUnavailable)
		return
	}
	_ = views.AccountDeletionProcessing(id, state.Stage).Render(r.Context(), w)
}
func (s Server) accountDeletionProgressStream(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "requestID")
	if _, err := s.accountDeletionProgressState(r.Context(), id); err != nil {
		writeSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error {
			return alert(sse, "Account deletion is unavailable. Please refresh.")
		})
		return
	}
	key := viewstore.AccountDeletionProgressKey(authProgressViewSessionID(w, r, "account_deletion_view", "/account-deletion"), id)
	sse := newSSE(w, r)
	err := streamViewStoreFatMorph[accountDeletionProgressState](r.Context(), sse, viewStoreFatMorphConfig[accountDeletionProgressState]{
		Key: key, Store: s.ViewStore,
		Refresh: func(ctx context.Context) error {
			state, err := s.accountDeletionProgressState(ctx, id)
			if err != nil {
				return err
			}
			return viewstore.PutState(ctx, s.ViewStore, key, state)
		},
		// Recover background cleanup/completion progress on reconnect and while waiting.
		Subscribe: func(ctx context.Context, notify func()) (closeableSubscription, error) {
			return newTickerSubscription(ctx, 2*time.Second, notify), nil
		},
		Patch: func(sse *datastar.ServerSentEventGenerator, state accountDeletionProgressState) error {
			if err := sse.PatchElementTempl(views.AccountDeletionProgressPanel(id, state.Stage), datastar.WithSelectorID("account-deletion-status")); err != nil {
				return err
			}
			if state.Stage == "complete" {
				return errAccountDeletionStreamFinished
			}
			return nil
		},
	})
	if err != nil && !errors.Is(err, errAccountDeletionStreamFinished) {
		_ = alert(sse, "Could not check account deletion progress. Please refresh.")
	}
}
