package profile

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"golang.org/x/crypto/bcrypt"
)

type profileProjectionKeys map[string]protectedpii.SubjectDataKey

func (k profileProjectionKeys) GetSubjectDataKey(_ context.Context, id string) (protectedpii.SubjectDataKey, bool, error) {
	key, ok := k[id]
	return key, ok, nil
}
func projectionEvent(t *testing.T, event eventstore.DomainEvent, err error) eventstore.DomainEvent {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return event
}
func profileProjectionHistory(t *testing.T) ([]eventstore.ResolvedEvent, profileProjectionKeys) {
	t.Helper()
	user, other := uuidv7.NewString(), uuidv7.NewString()
	key, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	otherKey, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	keys := profileProjectionKeys{user: key, other: otherKey}
	hash, err := bcrypt.GenerateFromPassword([]byte("secret123"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	must := func(event eventstore.DomainEvent, err error) eventstore.DomainEvent {
		return projectionEvent(t, event, err)
	}
	facts := []eventstore.DomainEvent{
		must(auth.NewUserRegisteredEvent(user, "alice", "alice@example.com", "Alice", "Test", 1990, string(hash), key, nil)),
		must(auth.NewUserRegisteredEvent(other, "bobby", "bob@example.com", "Bob", "Test", 1990, string(hash), otherKey, nil)),
		must(NewProfileBioUpdatedEvent(uuidv7.NewString(), "First bio", testProfileTime(), user, key, nil)),
		must(auth.NewUserNameChangedEvent(uuidv7.NewString(), "Bob Changed", testProfileTime(), other, otherKey, nil)),
		must(NewProfileImageUploadedEvent(uuidv7.NewString(), "/uploads/avatar.png", testProfileTime(), user, nil)),
		must(NewProfileHeaderImageUploadedEvent(uuidv7.NewString(), "/uploads/header.png", testProfileTime(), user, nil)),
		must(NewProfileBioUpdatedEvent(uuidv7.NewString(), "", testProfileTime(), user, key, nil)),
	}
	// The same event ID at a distinct source position is a distinct occurrence.
	facts = append(facts, facts[4])
	events := make([]eventstore.ResolvedEvent, len(facts))
	for i, fact := range facts {
		events[i] = eventstore.ResolvedEvent{Position: eventstore.Position{Commit: 42, Prepare: int64(i)}, Event: fact}
	}
	return events, keys
}

type profileProjectionFake struct {
	rows          map[string]ProfileState
	receipts      map[eventstore.Position]string
	committed     bool
	saves, failAt int
}

func (r *profileProjectionFake) WithinTransaction(ctx context.Context, block func(context.Context) error) error {
	rows, receipts := maps.Clone(r.rows), maps.Clone(r.receipts)
	r.committed = false
	if err := block(ctx); err != nil {
		r.rows, r.receipts = rows, receipts
		return err
	}
	r.committed = true
	return nil
}
func (r *profileProjectionFake) FindProcessedProfile(_ context.Context, p eventstore.Position) (string, error) {
	return r.receipts[p], nil
}
func (r *profileProjectionFake) LoadProfile(_ context.Context, id string) (*ProfileState, error) {
	s, ok := r.rows[id]
	if !ok {
		return nil, nil
	}
	return &s, nil
}
func (r *profileProjectionFake) SaveProfile(_ context.Context, s ProfileState, o ProfileUpdateIdentity) error {
	r.saves++
	if r.saves == r.failAt {
		return errors.New("save failed")
	}
	r.rows[s.UserID] = s
	r.receipts[o.Position] = s.UserID
	return nil
}
func newProfileProjectionFake() *profileProjectionFake {
	return &profileProjectionFake{rows: map[string]ProfileState{}, receipts: map[eventstore.Position]string{}}
}
func TestProfileBatchPartitions(t *testing.T) {
	events, keys := profileProjectionHistory(t)
	var baseline *profileProjectionFake
	for _, parts := range [][]int{{8}, {1, 1, 1, 1, 1, 1, 1, 1}, {2, 3, 3}} {
		repo := newProfileProjectionFake()
		offset := 0
		for _, size := range parts {
			ids, err := ProjectBatch(t.Context(), events[offset:offset+size], repo, keys)
			if err != nil {
				t.Fatal(err)
			}
			if len(ids) == 0 {
				t.Fatal("missing routing")
			}
			offset += size
		}
		if baseline == nil {
			baseline = repo
		} else if !reflect.DeepEqual(baseline.rows, repo.rows) || !reflect.DeepEqual(baseline.receipts, repo.receipts) {
			t.Fatal("partition-dependent state")
		}
		user := events[0].Event.EventID
		s := repo.rows[user]
		if s.Bio == nil || *s.Bio != "" || s.ImageURL == nil || s.HeaderImageURL == nil {
			t.Fatal("field update lost another field or cleared value")
		}
	}
}

type profilePublisherFunc func(context.Context, string, any) error

func (f profilePublisherFunc) Publish(ctx context.Context, subject string, payload any) error {
	return f(ctx, subject, payload)
}
func TestProfilePublicationRetryAndRollback(t *testing.T) {
	events, keys := profileProjectionHistory(t)
	repo := newProfileProjectionFake()
	calls := 0
	publisher := profilePublisherFunc(func(_ context.Context, subject string, payload any) error {
		if !repo.committed {
			t.Fatal("published before commit")
		}
		calls++
		user := events[0].Event.EventID
		if subject != Channel(user) || !reflect.DeepEqual(payload, map[string]string{"userId": user}) {
			t.Fatal("unexpected notification")
		}
		return errors.New("offline")
	})
	handler := &ReadModelEventHandler{repository: repo, keys: keys, publisher: publisher}
	repo.failAt = 1
	if err := handler.handle(t.Context(), events[0]); err == nil {
		t.Fatal("save failure lost")
	}
	if calls != 0 || len(repo.rows) != 0 || len(repo.receipts) != 0 {
		t.Fatal("partial commit/publication")
	}
	repo.failAt = 0
	if err := handler.handle(t.Context(), events[0]); err == nil || calls != 1 {
		t.Fatal("publication failure lost")
	}
	saved := repo.saves
	replay := events[0]
	replay.Event.Data = nil
	handler.keys = nil
	handler.publisher = profilePublisherFunc(func(context.Context, string, any) error { calls++; return nil })
	if err := handler.handle(t.Context(), replay); err != nil {
		t.Fatal(err)
	}
	if repo.saves != saved || calls != 2 {
		t.Fatal("retry repeated effects or lost notification")
	}
	repo = newProfileProjectionFake()
	repo.failAt = 3
	if ids, err := ProjectBatch(t.Context(), events, repo, keys); err == nil || len(ids) != 0 || len(repo.rows) != 0 || len(repo.receipts) != 0 {
		t.Fatal("failed batch retained prefix")
	}
	if _, err := ProjectBatch(t.Context(), []eventstore.ResolvedEvent{events[1], events[0]}, repo, keys); !errors.Is(err, eventstore.ErrInvalidEvent) {
		t.Fatal("unordered input accepted")
	}
}

func TestProfileSparseAndHistoricalState(t *testing.T) {
	events, keys := profileProjectionHistory(t)
	repo := newProfileProjectionFake()
	// A field-only input retains the existing sparse-profile behavior.
	if _, err := ProjectBatch(t.Context(), events[4:5], repo, keys); err != nil {
		t.Fatal(err)
	}
	if _, err := ProjectBatch(t.Context(), events[5:7], repo, keys); err != nil {
		t.Fatal(err)
	}
	user := events[0].Event.EventID
	state := repo.rows[user]
	if state.Name != nil || state.ImageURL == nil || state.HeaderImageURL == nil || state.Bio == nil || *state.Bio != "" {
		t.Fatal("sparse field preservation failed")
	}
	old := events[2]
	old.Position = eventstore.Position{Commit: 41, Prepare: 0}
	if _, err := ProjectBatch(t.Context(), []eventstore.ResolvedEvent{old}, repo, keys); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(repo.rows[user], state) || repo.receipts[old.Position] != user {
		t.Fatal("historical occurrence changed state or lost evidence")
	}
}
