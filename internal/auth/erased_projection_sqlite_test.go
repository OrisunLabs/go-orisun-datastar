package auth

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

type erasureGuardKeys struct {
	reader SubjectPiiKeyReader
	erased string
	reads  int
}

func (k *erasureGuardKeys) GetSubjectDataKey(ctx context.Context, id string) (protectedpii.SubjectDataKey, bool, error) {
	if id == k.erased {
		k.reads++
		return protectedpii.SubjectDataKey{}, false, errors.New("erased key read")
	}
	return k.reader.GetSubjectDataKey(ctx, id)
}
func TestSQLiteErasedAuthSkipsDelayedEventsAndContinuesOtherUsers(t *testing.T) {
	repo, pool := nativeAuthProjection(t)
	keys := NewSubjectPiiKeyStore(pool, protectedpii.FromEnv())
	owner, active := uuidv7.NewString(), uuidv7.NewString()
	key, err := keys.GetOrCreateSubjectDataKey(t.Context(), owner)
	if err != nil {
		t.Fatal(err)
	}
	activeKey, err := keys.GetOrCreateSubjectDataKey(t.Context(), active)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1, 0).UTC()
	hash := testAuthHash(t)
	root := mustAuthEvent(NewUserRegisteredEvent(owner, "erased", "erased@example.com", "Erased", "User", 1990, hash, at, at, key, nil))
	activeRoot := mustAuthEvent(NewUserRegisteredEvent(active, "active", "active@example.com", "Active", "User", 1990, hash, at, at, activeKey, nil))
	name := mustAuthEvent(NewUserNameChangedEvent(uuidv7.NewString(), "Old name", time.Now(), owner, key, nil))
	reset := mustAuthEvent(NewPasswordResetRequestedEvent(uuidv7.NewString(), "erased@example.com", "old-token", time.Now().Add(time.Hour), owner, key, nil))
	password := mustAuthEvent(NewPasswordChangedEvent(uuidv7.NewString(), time.Now(), owner, hash, nil))
	store := eventstoretest.New(t, root, name, activeRoot, reset, password)
	events, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 10, eventstore.Forward, eventstore.Query{})
	if err != nil || len(events) != 5 {
		t.Fatalf("fixture: %d %v", len(events), err)
	}
	if err := ProjectBatch(t.Context(), events[:1], repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	cleanup := NewAccountDataDeletionStore(pool, nil)
	if err := cleanup.DeleteAccountData(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
	if err := keys.DestroySubjectKey(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
	// Repeated cleanup must retain the marker and prior occurrence evidence.
	if err := cleanup.DeleteAccountData(t.Context(), owner); err != nil {
		t.Fatal(err)
	}
	guarded := &erasureGuardKeys{reader: keys, erased: owner}
	publications := 0
	offline := authPublisherFunc(func(context.Context, string) error { publications++; return errors.New("notification offline") })
	if err := ProjectBatch(t.Context(), events, repo, guarded, ProfileSource{}, offline); err == nil {
		t.Fatal("publication error lost")
	}
	if guarded.reads != 0 || publications == 0 {
		t.Fatal("erased account needed its key or lost retry routing")
	}
	var erasedRows, activeRows, tokens int
	if err := pool.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM auth_user WHERE id=$1), (SELECT count(*) FROM auth_user WHERE id=$2), (SELECT count(*) FROM auth_verification WHERE identifier='password-reset:'||$1)`, owner, active).Scan(&erasedRows, &activeRows, &tokens); err != nil {
		t.Fatal(err)
	}
	if erasedRows != 0 || activeRows != 1 || tokens != 0 {
		t.Fatal("erased user resurrected or active user stalled")
	}
	// The actual commit/prepare occurrences survive deletion and notification retry.
	if err := repo.WithinTransaction(t.Context(), func(ctx context.Context) error {
		for _, e := range events {
			got, err := repo.Processed(ctx, e.Position)
			if err != nil {
				return err
			}
			if got == "" {
				t.Fatal("missing durable occurrence")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i := range events {
		events[i].Event.Data = nil
	}
	online := authPublisherFunc(func(context.Context, string) error { return nil })
	if err := ProjectBatch(t.Context(), events, repo, nil, ProfileSource{}, online); err != nil {
		t.Fatalf("known occurrences reread payload/keys: %v", err)
	}
	// Missing keys alone are not erasure evidence for an active account.
	fresh := mustAuthEvent(NewUserNameChangedEvent(uuidv7.NewString(), "Still active", time.Now(), active, activeKey, nil))
	pos := events[len(events)-1].Position
	pos.Prepare++
	if err := ProjectBatch(t.Context(), []eventstore.ResolvedEvent{{Event: fresh, Position: pos}}, repo, &erasureGuardKeys{reader: keys, erased: active}, ProfileSource{}, nil); err == nil {
		t.Fatal("missing active key silently skipped")
	}
}

func TestSQLiteAuthErasureAndProjectionShareLockAndRollback(t *testing.T) {
	repo, pool := nativeAuthProjection(t)
	history, keys := authProjectionHistory(t)
	user := history[0].Event.EventID
	if err := ProjectBatch(t.Context(), history[:1], repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	// A cleanup failure must roll back both the marker and row removal.
	if _, err := pool.ExecContext(t.Context(), `CREATE TRIGGER fail_auth_delete BEFORE DELETE ON auth_user BEGIN SELECT RAISE(ABORT, 'injected cleanup failure'); END`); err != nil {
		t.Fatal(err)
	}
	cleanup := NewAccountDataDeletionStore(pool, nil)
	if err := cleanup.DeleteAccountData(t.Context(), user); err == nil {
		t.Fatal("cleanup failure ignored")
	}
	var erased bool
	if err := repo.WithinTransaction(t.Context(), func(ctx context.Context) error { var err error; erased, err = repo.Erased(ctx, user); return err }); err != nil || erased {
		t.Fatalf("failed cleanup committed marker: %v %v", erased, err)
	}
	var credentials int
	if err := pool.QueryRowContext(t.Context(), "SELECT count(*) FROM auth_account WHERE user_id=$1", user).Scan(&credentials); err != nil || credentials != 1 {
		t.Fatal("failed cleanup committed credential removal")
	}
	if _, err := pool.ExecContext(t.Context(), "DROP TRIGGER fail_auth_delete"); err != nil {
		t.Fatal(err)
	}
	lock, err := pool.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() { close(started); done <- cleanup.DeleteAccountData(t.Context(), user) }()
	<-started
	select {
	case err := <-done:
		t.Fatalf("cleanup bypassed projection lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not finish after lock release")
	}
	// Even direct repository saves under the lock cannot restore an erased row.
	if err := repo.WithinTransaction(t.Context(), func(ctx context.Context) error {
		change, err := decodeAuthChange(ctx, history[0], keys, ProfileSource{})
		if err != nil {
			return err
		}
		state, err := applyAuthChange(nil, change, history[0].Position)
		if err != nil {
			return err
		}
		return repo.Save(ctx, ProjectionUpdate{State: state, Occurrence: Occurrence{UserID: user, EventType: UserRegistered, Position: history[0].Position}})
	}); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("direct save resurrected account: %v", err)
	}
}

func TestSQLiteAuthErasureBeforeRegistrationAndSkippedReceiptRollback(t *testing.T) {
	repo, pool := nativeAuthProjection(t)
	history, _ := authProjectionHistory(t)
	user := history[0].Event.EventID
	if err := NewAccountDataDeletionStore(pool, nil).DeleteAccountData(t.Context(), user); err != nil {
		t.Fatal(err)
	}
	// Unknown malformed events fail closed even for an erased account; skipped evidence rolls back with the batch.
	malformed := history[1]
	malformed.Event.Data = eventstore.MustData(history[1].Event.Data)
	malformed.Event.Data["unknown"] = true
	if err := ProjectBatch(t.Context(), []eventstore.ResolvedEvent{history[0], malformed}, repo, nil, ProfileSource{}, nil); !errors.Is(err, eventstore.ErrInvalidEvent) {
		t.Fatalf("malformed erased event accepted: %v", err)
	}
	if err := repo.WithinTransaction(t.Context(), func(ctx context.Context) error {
		owner, err := repo.Processed(ctx, history[0].Position)
		if err != nil {
			return err
		}
		if owner != "" {
			t.Fatal("rolled-back skip receipt persisted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// No decrypted registration, credential or token is needed to checkpoint this account's complete history.
	if err := ProjectBatch(t.Context(), history, repo, nil, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	var users, credentials, tokens int
	if err := pool.QueryRowContext(t.Context(), "SELECT (SELECT count(*) FROM auth_user),(SELECT count(*) FROM auth_account),(SELECT count(*) FROM auth_verification)").Scan(&users, &credentials, &tokens); err != nil || users+credentials+tokens != 0 {
		t.Fatal("unseen erased registration created private data")
	}
	// Reuse of an event UUID at a new full position is still a distinct delivery.
	repeated := history[1]
	repeated.Position = history[len(history)-1].Position
	repeated.Position.Prepare++
	if err := ProjectBatch(t.Context(), []eventstore.ResolvedEvent{repeated}, repo, nil, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.WithinTransaction(t.Context(), func(ctx context.Context) error {
		owner, err := repo.Processed(ctx, repeated.Position)
		if err != nil {
			return err
		}
		if owner != user {
			t.Fatal("new occurrence deduplicated by event UUID")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
