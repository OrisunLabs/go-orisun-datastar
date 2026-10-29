package developmentseed

import (
	"context"
	"errors"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"testing"
	"time"
)

type readinessReader struct {
	ready bool
	err   error
}

func (r readinessReader) RegistrationReady(context.Context, string) (bool, error) {
	return r.ready, r.err
}
func (r readinessReader) ProjectionReached(context.Context, string, eventstore.Position) (bool, error) {
	return r.ready, r.err
}
func TestReadinessRequiresAllOwnersAndHonorsCancellation(t *testing.T) {
	result := Result{Users: []User{{ID: "user", Position: eventstore.Position{Commit: 8, Prepare: 3}, Todos: []Todo{{ID: "todo", Position: eventstore.Position{Commit: 9, Prepare: 4}, Deleted: true}}}}}
	for _, lagging := range []string{"auth", "profile", "todo"} {
		t.Run(lagging, func(t *testing.T) {
			readers := Readers{Auth: readinessReader{ready: lagging != "auth"}, Profiles: readinessReader{ready: lagging != "profile"}, Todos: readinessReader{ready: lagging != "todo"}}
			ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
			defer cancel()
			if err := WaitForProjections(ctx, result, readers); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
		})
	}
	if err := WaitForProjections(t.Context(), result, Readers{Auth: readinessReader{ready: true}, Profiles: readinessReader{ready: true}, Todos: readinessReader{ready: true}}); err != nil {
		t.Fatal(err)
	}
	if err := WaitForProjections(t.Context(), result, Readers{}); err == nil {
		t.Fatal("missing readers accepted")
	}
}
