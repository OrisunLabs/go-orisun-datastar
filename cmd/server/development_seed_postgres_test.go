package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/developmentseed"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/profile"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/todo"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

func TestNativeSeedReadinessRequiresEveryFinalProjection(t *testing.T) {
	authRepo, pool := nativeOwnedAuthProjection(t)
	// Restore the profile owner table that the auth-isolation fixture removes.
	for _, name := range []string{"001_init.sql", "003_profile_projection.sql"} {
		schema, err := os.ReadFile("../../migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(t.Context(), string(schema)); err != nil {
			t.Fatal(err)
		}
	}
	store := eventstoretest.New(t)
	keys := auth.NewSubjectPiiKeyStore(pool, protectedpii.FromEnv())
	result, err := developmentseed.Seed(t.Context(), store, keys)
	if err != nil {
		t.Fatal(err)
	}
	readers := developmentseed.Readers{Auth: auth.NewAuthUserStore(pool), Profiles: profile.NewReadModel(pool), Todos: todo.NewReadModel(pool)}
	checkNotReady := func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
		defer cancel()
		if err := developmentseed.WaitForProjections(ctx, result, readers); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("seed announced readiness before projection", err)
		}
	}
	checkNotReady()
	facts, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, eventstore.Query{})
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.ProjectBatch(t.Context(), facts, authRepo, keys, authProfileSource(), nil); err != nil {
		t.Fatal(err)
	}
	checkNotReady()
	var profileFacts, todoFacts []eventstore.ResolvedEvent
	for _, f := range facts {
		switch f.Event.EventType {
		case auth.UserRegistered, profile.ProfileBioUpdated:
			profileFacts = append(profileFacts, f)
		case todo.TodoCreated, todo.TodoRenamed, todo.TodoCompleted, todo.TodoReopened, todo.TodoDeleted:
			todoFacts = append(todoFacts, f)
		}
	}
	if _, err := profile.ProjectBatch(t.Context(), profileFacts, profile.NewPostgreSQLProjectionRepository(pool), keys); err != nil {
		t.Fatal(err)
	}
	// Creation alone can have the same visible status as a later reopened todo.
	cut := 0
	for cut < len(todoFacts) && todoFacts[cut].Event.EventType == todo.TodoCreated {
		cut++
	}
	todoRepo := todo.NewPostgreSQLProjectionRepository(pool)
	if _, err := todo.ProjectBatch(t.Context(), todoFacts[:cut], todoRepo); err != nil {
		t.Fatal(err)
	}
	checkNotReady()
	if _, err := todo.ProjectBatch(t.Context(), todoFacts[cut:], todoRepo); err != nil {
		t.Fatal(err)
	}
	if err := developmentseed.WaitForProjections(t.Context(), result, readers); err != nil {
		t.Fatal(err)
	}
	for _, u := range result.Users {
		if _, _, err := auth.NewSessionManager(pool, auth.NewAuthUserStore(pool), false).Login(t.Context(), u.Email, developmentseed.DemoPassword); err != nil {
			t.Fatal("seeded user cannot sign in", err)
		}
		rows, err := todo.NewReadModel(pool).List(t.Context(), u.ID)
		if err != nil || len(rows) != 3 {
			t.Fatal("wrong visible todo inventory", rows, err)
		}
	}
}
