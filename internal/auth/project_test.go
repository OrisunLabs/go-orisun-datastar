package auth

import (
	"context"
	"errors"
	"maps"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func authProjectionHistory(t *testing.T) ([]eventstore.ResolvedEvent, *authTestKeys) {
	t.Helper()
	id := uuidv7.NewString()
	key, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{id: key}}
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	reset := uuidv7.NewString()
	hash := testAuthHash(t)
	facts := []eventstore.DomainEvent{
		mustAuthEvent(NewUserRegisteredEvent(id, "alice", "alice@example.com", "Alice", "Test", 1990, hash, time.Unix(1, 0).UTC(), time.Unix(1, 0).UTC(), key, nil)),
		mustAuthEvent(NewUserNameChangedEvent(uuidv7.NewString(), "Alice Changed", at, id, key, nil)),
		mustAuthEvent(NewPasswordResetRequestedEvent(reset, "alice@example.com", "secret-token", at.Add(time.Hour), id, key, nil)),
		mustAuthEvent(NewPasswordChangedEvent(uuidv7.NewString(), at, id, hash, nil)),
		mustAuthEvent(NewPasswordChangedEvent(uuidv7.NewString(), at, id, hash, nil)),
	}
	events := make([]eventstore.ResolvedEvent, len(facts))
	for i, fact := range facts {
		events[i] = eventstore.ResolvedEvent{Position: eventstore.Position{Commit: 10, Prepare: int64(i)}, Event: fact}
	}
	return events, keys
}

func TestAuthProjectionFoldRetainsMutableStateOnHistoricalRegistration(t *testing.T) {
	events, keys := authProjectionHistory(t)
	var state *AuthState
	for _, event := range events {
		change, err := decodeAuthChange(t.Context(), event, keys, ProfileSource{})
		if err != nil {
			t.Fatal(err)
		}
		next, err := applyAuthChange(state, change, event.Position)
		if err != nil {
			t.Fatal(err)
		}
		state = &next
	}
	root, err := decodeAuthChange(t.Context(), events[0], keys, ProfileSource{})
	if err != nil {
		t.Fatal(err)
	}
	next, err := applyAuthChange(state, root, events[0].Position)
	if err != nil {
		t.Fatal(err)
	}
	if next.Name != "Alice Changed" || !next.EmailVerified || next.Position != state.Position {
		t.Fatal("registration reset mutable state")
	}
	if _, err := applyAuthChange(nil, NameInput{UserID: state.UserID, Name: "Missing"}, events[0].Position); !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatal(err)
	}
	conflict := root.(RegistrationInput)
	conflict.Email = "other@example.com"
	if _, err := applyAuthChange(state, conflict, events[0].Position); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatal(err)
	}
}

type authProjectionFake struct {
	states              map[string]AuthState
	receipts            map[eventstore.Position]string
	facts               map[string]VerificationFact
	committed, failSave bool
	saves               int
}

func (r *authProjectionFake) WithinTransaction(ctx context.Context, block func(context.Context) error) error {
	r.committed = false
	states, receipts, facts := maps.Clone(r.states), maps.Clone(r.receipts), maps.Clone(r.facts)
	if err := block(ctx); err != nil {
		r.states, r.receipts, r.facts = states, receipts, facts
		return err
	}
	r.committed = true
	return nil
}
func (r *authProjectionFake) Processed(_ context.Context, p eventstore.Position) (string, error) {
	return r.receipts[p], nil
}
func (r *authProjectionFake) Load(_ context.Context, id string) (*AuthState, error) {
	v, ok := r.states[id]
	if !ok {
		return nil, nil
	}
	return &v, nil
}
func (r *authProjectionFake) LoadVerification(_ context.Context, id string) (*VerificationFact, error) {
	v, ok := r.facts[id]
	if !ok {
		return nil, nil
	}
	return &v, nil
}
func (r *authProjectionFake) Save(_ context.Context, u ProjectionUpdate) error {
	r.saves++
	if r.failSave && r.saves == 2 {
		return errors.New("save failed")
	}
	r.states[u.State.UserID] = u.State
	r.receipts[u.Occurrence.Position] = u.State.UserID
	if u.Verification != nil {
		r.facts[u.Verification.ID] = *u.Verification
	}
	return nil
}

type authPublisherFunc func(context.Context, string) error

func (f authPublisherFunc) InvalidateAuthUser(ctx context.Context, id string) error {
	return f(ctx, id)
}
func TestAuthBatchRollbackAndPublicationRetry(t *testing.T) {
	events, keys := authProjectionHistory(t)
	repo := &authProjectionFake{states: map[string]AuthState{}, receipts: map[eventstore.Position]string{}, facts: map[string]VerificationFact{}, failSave: true}
	calls := 0
	publisher := authPublisherFunc(func(context.Context, string) error {
		if !repo.committed {
			t.Fatal("published before commit")
		}
		calls++
		return errors.New("offline")
	})
	if err := ProjectBatch(t.Context(), events, repo, keys, ProfileSource{}, publisher); err == nil {
		t.Fatal("save failure lost")
	}
	if calls != 0 || len(repo.states) != 0 || len(repo.receipts) != 0 {
		t.Fatal("partial commit or publication")
	}
	repo.failSave = false
	if err := ProjectBatch(t.Context(), events, repo, keys, ProfileSource{}, publisher); err == nil || calls != 1 {
		t.Fatal("publication failure lost")
	}
	saved := repo.saves
	for i := range events {
		events[i].Event.Data = nil
	}
	publisher = authPublisherFunc(func(context.Context, string) error {
		if !repo.committed {
			t.Fatal("published before commit")
		}
		calls++
		return nil
	})
	if err := ProjectBatch(t.Context(), events, repo, nil, ProfileSource{}, publisher); err != nil {
		t.Fatal(err)
	}
	if repo.saves != saved || calls != 2 {
		t.Fatal("retry repeated writes or lost routing")
	}
	if err := ProjectBatch(t.Context(), []eventstore.ResolvedEvent{events[1], events[0]}, repo, nil, ProfileSource{}, publisher); !errors.Is(err, eventstore.ErrInvalidEvent) {
		t.Fatal("unordered batch accepted")
	}
}

func TestAuthUnknownPositionIsExplicit(t *testing.T) {
	zero := int64(0)
	negative := int64(-1)
	position, err := authProjectionPosition(nil, nil)
	if err != nil || position != nil {
		t.Fatal("unknown position must be nil")
	}
	position, err = authProjectionPosition(&zero, &zero)
	if err != nil || position == nil || *position != (eventstore.Position{}) {
		t.Fatal("zero is a valid known position")
	}
	for _, pair := range [][2]*int64{{&zero, nil}, {nil, &zero}, {&negative, &zero}, {&zero, &negative}} {
		if _, err := authProjectionPosition(pair[0], pair[1]); !errors.Is(err, eventstore.ErrInvalidEvent) {
			t.Fatal("invalid position accepted")
		}
	}
	state := &AuthState{UserID: "user", Name: "Legacy", EmailVerified: true}
	next, err := applyAuthChange(state, NameInput{UserID: "user", Name: "Updated"}, eventstore.Position{})
	if err != nil || next.Position == nil || *next.Position != (eventstore.Position{}) || !next.EmailVerified {
		t.Fatal("unknown watermark did not advance to known zero")
	}
}

func TestAuthSubscriptionIncludesInjectedProfileTypes(t *testing.T) {
	source := ProfileSource{EventTypes: []string{"ProfileImageUploaded", "ProfileBioUpdated", "ProfileHeaderImageUploaded"}}
	query := authUserProjectionEventHandlerQuery(source)
	if len(query.Criteria) != 7 {
		t.Fatal("merged subscription missing event types")
	}
	for _, criterion := range query.Criteria {
		if len(criterion.Tags) != 1 || criterion.Tags[0].Key != eventstore.EventTypeField {
			t.Fatal("subscription changed shape")
		}
	}
}
