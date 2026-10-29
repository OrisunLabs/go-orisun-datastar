// Package developmentseed composes opt-in demo histories through public commands.
package developmentseed

import (
	"context"
	"errors"
	"fmt"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/profile"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

const DatasetVersion = "1"
const DemoPassword = "StarterDemo123!"
const dataset = "starter-demo"

type Store interface {
	eventstore.SaverV2
	eventstore.LatestRetriever
}
type Result struct {
	Version string
	Users   []User
}
type User struct {
	ID, Username, Email, Name, Bio string
	Position                       eventstore.Position
	Todos                          []Todo
}
type Todo struct {
	ID, Title          string
	Completed, Deleted bool
	Position           eventstore.Position
}
type userSpec struct {
	key, username, email, first, last, bio string
	todoIDs                                [4]string
}

var users = []userSpec{
	{"alex", "alex.demo", "alex@example.test", "Alex", "Morgan", "Exploring event-sourced commands and live updates.", [4]string{"01940000-0000-7000-8000-000000000101", "01940000-0000-7000-8000-000000000102", "01940000-0000-7000-8000-000000000103", "01940000-0000-7000-8000-000000000104"}},
	{"sam", "sam.demo", "sam@example.test", "Sam", "Taylor", "A second workspace to demonstrate ownership and isolation.", [4]string{"01940000-0000-7000-8000-000000000201", "01940000-0000-7000-8000-000000000202", "01940000-0000-7000-8000-000000000203", "01940000-0000-7000-8000-000000000204"}},
}

func metadata(key, step string) eventstore.CommandMetadata {
	m := eventstore.SystemCommandMetadata("development-seed", step)
	m["seed"] = map[string]any{"dataset": dataset, "version": DatasetVersion, "key": key, "step": step}
	return m
}
func owned(e eventstore.DomainEvent, key, step string) bool {
	m, ok := e.Metadata["seed"].(map[string]any)
	return ok && m["dataset"] == dataset && m["version"] == DatasetVersion && m["key"] == key && m["step"] == step
}
func criterion(kind, field, value string) eventstore.Criterion {
	return eventstore.Criterion{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: kind}, {Key: field, Value: value}}}
}
func latest(ctx context.Context, s eventstore.LatestRetriever, criteria ...eventstore.Criterion) ([]eventstore.ResolvedEvent, error) {
	result, err := s.GetLatestByCriteria(ctx, criteria)
	if err != nil {
		return nil, err
	}
	return eventstore.EventsFromLatest(result.Results), nil
}
func exact(ctx context.Context, s eventstore.LatestRetriever, kind, id string) (eventstore.ResolvedEvent, error) {
	facts, err := latest(ctx, s, criterion(kind, "eventId", id))
	if err != nil {
		return eventstore.ResolvedEvent{}, err
	}
	if len(facts) != 1 {
		return eventstore.ResolvedEvent{}, eventstore.ErrNotFound
	}
	return facts[0], nil
}

// Seed never writes SQL, fabricates events, or resets existing data. Commands
// allocate workflow identities; the dataset reserves canonical UUIDv7 todo IDs.
func Seed(ctx context.Context, s Store, keys auth.SubjectPiiKeyPort) (Result, error) {
	out := Result{Version: DatasetVersion}
	for _, spec := range users {
		u, err := seedUser(ctx, s, keys, spec)
		if err != nil {
			return out, fmt.Errorf("seed %s: %w", spec.key, err)
		}
		if err := seedBio(ctx, s, keys, spec, &u); err != nil {
			return out, fmt.Errorf("seed %s profile: %w", spec.key, err)
		}
		u.Todos, err = seedTodos(ctx, s, spec, u.ID)
		if err != nil {
			return out, fmt.Errorf("seed %s todos: %w", spec.key, err)
		}
		out.Users = append(out.Users, u)
	}
	return out, nil
}
func seedBio(ctx context.Context, s Store, keys auth.SubjectPiiKeyReader, spec userSpec, u *User) error {
	facts, err := latest(ctx, s, criterion(profile.ProfileBioUpdated, profile.ProfileUserIDField, u.ID))
	if err != nil {
		return err
	}
	if len(facts) == 0 {
		if err := profile.UpdateProfileBioCommandHandler(ctx, profile.UpdateProfileBioCommand{UserID: u.ID, Bio: spec.bio, Metadata: metadata(spec.key, "profile-bio")}, s, s, keys); err != nil {
			return err
		}
		facts, err = latest(ctx, s, criterion(profile.ProfileBioUpdated, profile.ProfileUserIDField, u.ID))
		if err != nil {
			return err
		}
	}
	if len(facts) != 1 || !owned(facts[0].Event, spec.key, "profile-bio") {
		return errors.New("profile has different seed ownership")
	}
	v, err := profile.DecodeProfileBioUpdatedEvent(facts[0].Event)
	if err != nil {
		return err
	}
	key, ok, err := keys.GetSubjectDataKey(ctx, u.ID)
	if err != nil {
		return err
	}
	if !ok {
		return eventstore.ErrNotFound
	}
	bio, err := protectedpii.FromEnv().DecryptWithDataKey(v.Bio, key)
	if err != nil {
		return err
	}
	if v.UserID != u.ID || bio != spec.bio || v.BioHash != protectedpii.FromEnv().BlindIndex(profile.ProfileBioUpdatedBioField, spec.bio) {
		return errors.New("seed profile terms differ")
	}
	u.Position = facts[0].Position
	return nil
}
