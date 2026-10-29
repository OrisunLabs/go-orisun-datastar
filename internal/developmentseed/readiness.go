package developmentseed

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

// Each feature owns its readiness query; the seeder never reads projection SQL.
type ProjectionReader interface {
	ProjectionReached(context.Context, string, eventstore.Position) (bool, error)
}
type AuthReader interface {
	ProjectionReader
	RegistrationReady(context.Context, string) (bool, error)
}
type Readers struct {
	Auth            AuthReader
	Profiles, Todos ProjectionReader
}

func WaitForProjections(ctx context.Context, result Result, readers Readers) error {
	if readers.Auth == nil || readers.Profiles == nil || readers.Todos == nil {
		return errors.New("seed projection readers are required")
	}
	timer := time.NewTicker(50 * time.Millisecond)
	defer timer.Stop()
	var last error
	for {
		ready, err := projectionsReady(ctx, result, readers)
		if err == nil && ready {
			return nil
		}
		last = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for seed projections: %w (last read: %v)", ctx.Err(), last)
		case <-timer.C:
		}
	}
}
func projectionsReady(ctx context.Context, result Result, r Readers) (bool, error) {
	for _, u := range result.Users {
		ready, err := r.Auth.RegistrationReady(ctx, u.ID)
		if err != nil || !ready {
			return false, err
		}
		for _, reader := range []ProjectionReader{r.Auth, r.Profiles} {
			ready, err = reader.ProjectionReached(ctx, u.ID, u.Position)
			if err != nil || !ready {
				return false, err
			}
		}
		for _, todo := range u.Todos {
			ready, err = r.Todos.ProjectionReached(ctx, todo.ID, todo.Position)
			if err != nil || !ready {
				return false, err
			}
		}
	}
	return true, nil
}
