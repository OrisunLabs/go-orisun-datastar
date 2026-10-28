package auth

import (
	"context"
	"errors"
	"github.com/OrisunLabs/go-orisun-datastar/internal/appdb"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"

	"database/sql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/sqlitetest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

func nativeAuthProjection(t *testing.T) (*SQLiteProjectionRepository, *sql.DB) {
	t.Helper()
	pool := sqlitetest.New(t)
	return NewSQLiteProjectionRepository(pool), pool
}

func authSnapshot(t *testing.T, pool *sql.DB) string {
	return sqlitetest.Snapshot(t, pool, "auth_user", "auth_account", "auth_verification")
}

func TestSQLiteAuthBatchPartitionsAndReplay(t *testing.T) {
	events, keys := authProjectionHistory(t)
	baseline := ""
	for _, parts := range [][]int{{5}, {1, 1, 1, 1, 1}, {2, 1, 2}} {
		repo, pool := nativeAuthProjection(t)
		offset := 0
		for _, size := range parts {
			if err := ProjectBatch(t.Context(), events[offset:offset+size], repo, keys, ProfileSource{}, nil); err != nil {
				t.Fatal(err)
			}
			offset += size
		}
		snapshot := authSnapshot(t, pool)
		if err := appdb.Migrate(t.Context(), pool); err != nil {
			t.Fatal(err)
		}
		if authSnapshot(t, pool) != snapshot {
			t.Fatal("migration reapplication changed projected data")
		}

		if baseline == "" {
			baseline = snapshot
		} else if baseline != snapshot {
			t.Fatal("state depends on partition")
		}
		// Known source positions short circuit strict decoding and key lookup.
		replay := append([]eventstore.ResolvedEvent(nil), events...)
		for i := range replay {
			replay[i].Event.Data = nil
		}
		if err := ProjectBatch(t.Context(), replay, repo, nil, ProfileSource{}, nil); err != nil {
			t.Fatal(err)
		}
		if authSnapshot(t, pool) != snapshot {
			t.Fatal("replay changed state")
		}
	}
}
func TestSQLiteAuthRollsBackCredentialAndVerificationFailures(t *testing.T) {
	events, keys := authProjectionHistory(t)
	for _, table := range []string{"auth_account", "auth_verification"} {
		t.Run(table, func(t *testing.T) {
			repo, pool := nativeAuthProjection(t)
			if _, err := pool.ExecContext(t.Context(), "CREATE TRIGGER force_failure BEFORE INSERT ON "+table+" BEGIN SELECT RAISE(ABORT,'forced failure'); END"); err != nil {
				t.Fatal(err)
			}
			if err := ProjectBatch(t.Context(), events, repo, keys, ProfileSource{}, nil); err == nil {
				t.Fatal("expected SQL failure")
			}
			var count int
			if err := pool.QueryRowContext(t.Context(), "SELECT count(*) FROM auth_user").Scan(&count); err != nil || count != 0 {
				t.Fatalf("partial user/evidence: %d %v", count, err)
			}
		})
	}
}
func TestSQLiteAuthTransactionBinding(t *testing.T) {
	repo, pool := nativeAuthProjection(t)
	if _, err := repo.Load(t.Context(), "missing"); err == nil {
		t.Fatal("unbound access")
	}
	other := NewSQLiteProjectionRepository(pool)
	for _, rollback := range []bool{false, true} {
		var escaped context.Context
		marker := errors.New("rollback")
		err := repo.WithinTransaction(t.Context(), func(ctx context.Context) error {
			escaped = ctx
			if _, err := other.Load(ctx, "missing"); err == nil {
				t.Fatal("foreign access")
			}
			if err := repo.WithinTransaction(ctx, func(context.Context) error { return nil }); err == nil {
				t.Fatal("nested access")
			}
			if rollback {
				return marker
			}
			return nil
		})
		if rollback && !errors.Is(err, marker) {
			t.Fatal(err)
		}
		if !rollback && err != nil {
			t.Fatal(err)
		}
		if _, err := repo.Load(escaped, "missing"); err == nil {
			t.Fatal("escaped access")
		}
	}
}
func TestSQLiteAuthMissingRegistrationAndTokenConflict(t *testing.T) {
	events, keys := authProjectionHistory(t)
	repo, pool := nativeAuthProjection(t)
	if err := ProjectBatch(t.Context(), events[1:2], repo, keys, ProfileSource{}, nil); !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatal(err)
	}
	if err := ProjectBatch(t.Context(), events, repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	before := authSnapshot(t, pool)
	conflict := events[2]
	conflict.Position = eventstore.Position{Commit: 11, Prepare: 0}
	original, err := DecodePasswordResetRequestedEvent(conflict.Event)
	if err != nil {
		t.Fatal(err)
	}
	expires, _ := time.Parse(time.RFC3339Nano, original.ExpiresAt)
	conflict.Event = mustAuthEvent(NewPasswordResetRequestedEvent(original.PasswordResetRequestedID, "alice@example.com", "different-token", expires, original.Scopes.UserRegisteredID, keys.keys[original.Scopes.UserRegisteredID], nil))
	if err := ProjectBatch(t.Context(), []eventstore.ResolvedEvent{conflict}, repo, keys, ProfileSource{}, nil); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("token collision: %v", err)
	}
	if authSnapshot(t, pool) != before {
		t.Fatal("conflict changed projection")
	}
}

func TestSQLiteAuthOwnWritesAndIsolation(t *testing.T) {
	events, keys := authProjectionHistory(t)
	repo, pool := nativeAuthProjection(t)
	err := repo.WithinTransaction(t.Context(), func(ctx context.Context) error {
		change, err := decodeAuthChange(ctx, events[0], keys, ProfileSource{})
		if err != nil {
			return err
		}
		next, err := applyAuthChange(nil, change, events[0].Position)
		if err != nil {
			return err
		}
		if err := repo.Save(ctx, ProjectionUpdate{State: next, Occurrence: Occurrence{UserID: next.UserID, EventType: UserRegistered, Position: events[0].Position}}); err != nil {
			return err
		}
		loaded, err := repo.Load(ctx, next.UserID)
		if err != nil {
			return err
		}
		if loaded == nil || loaded.Name != next.Name {
			t.Fatal("own writes were not observable")
		}
		owner, err := repo.Processed(ctx, events[0].Position)
		if err != nil {
			return err
		}
		if owner != next.UserID {
			t.Fatal("own evidence was not observable")
		}
		var count int
		if err := pool.QueryRowContext(ctx, "SELECT count(*) FROM auth_user").Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			t.Fatal("pool read saw uncommitted state")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Later domain changes retain the accepted verification fact.
	var before time.Time
	if err := pool.QueryRowContext(t.Context(), "SELECT email_verified_at FROM auth_user").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := ProjectBatch(t.Context(), events[1:2], repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	var verified bool
	var after time.Time
	if err := pool.QueryRowContext(t.Context(), "SELECT email_verified,email_verified_at FROM auth_user").Scan(&verified, &after); err != nil || !verified || !after.Equal(before) {
		t.Fatalf("accepted verification changed: %v", err)
	}

}

func TestSQLiteAuthDecryptionFailureRollsBackPrefix(t *testing.T) {
	events, keys := authProjectionHistory(t)
	repo, pool := nativeAuthProjection(t)
	id := events[0].Event.EventID
	wrong, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	events[1].Event = mustAuthEvent(NewUserNameChangedEvent(uuidv7.NewString(), "Wrong key", time.Now().UTC(), id, wrong, nil))
	if err := ProjectBatch(t.Context(), events[:2], repo, keys, ProfileSource{}, nil); err == nil {
		t.Fatal("wrong-key ciphertext accepted")
	}
	var count int
	if err := pool.QueryRowContext(t.Context(), "SELECT count(*) FROM auth_user").Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial registration: %d %v", count, err)
	}
}

func TestSQLiteAuthUnknownPositionsAndConstraints(t *testing.T) {
	repo, pool := nativeAuthProjection(t)
	events, keys := authProjectionHistory(t)
	if err := ProjectBatch(t.Context(), events, repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.ExecContext(t.Context(), "UPDATE auth_user SET last_event_commit_position=NULL,last_event_prepare_position=NULL"); err != nil {
		t.Fatal(err)
	}
	if err := repo.WithinTransaction(t.Context(), func(ctx context.Context) error {
		state, err := repo.Load(ctx, events[0].Event.EventID)
		if err != nil {
			return err
		}
		if state.Position != nil {
			t.Fatal("unknown user position fabricated")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, columns := range []struct{ table, commit, prepare string }{
		{"auth_user", "last_event_commit_position", "last_event_prepare_position"},
	} {
		for _, values := range []string{"0,NULL", "NULL,0", "-1,-1", "-1,0", "0,-1"} {
			sql := "UPDATE " + columns.table + " SET (" + columns.commit + "," + columns.prepare + ")=(" + values + ")"
			if _, err := pool.ExecContext(t.Context(), sql); err == nil {
				t.Fatalf("invalid pair accepted: %s %s", columns.table, values)
			}
		}
		if _, err := pool.ExecContext(t.Context(), "UPDATE "+columns.table+" SET ("+columns.commit+","+columns.prepare+")=(0,0)"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSQLiteRegistrationProjectsOnlyAcceptedUser(t *testing.T) {
	repo, pool := nativeAuthProjection(t)
	store := eventstoretest.New(t)
	keys := &authTestKeys{keys: map[string]protectedpii.SubjectDataKey{}}
	pending := stagedRegistration(t, store, keys)
	verifyStagedRegistration(t, store, pending)
	for _, kind := range []string{UserRegistrationStarted, EmailVerificationOTPGenerated, EmailVerificationOTPValidated} {
		if err := ProjectBatch(t.Context(), resetOutcomeFacts(t, store, kind), repo, keys, ProfileSource{}, nil); err != nil {
			t.Fatal(err)
		}
	}
	var users, accounts int
	if err := pool.QueryRowContext(t.Context(), "SELECT (SELECT count(*) FROM auth_user),(SELECT count(*) FROM auth_account)").Scan(&users, &accounts); err != nil || users != 0 || accounts != 0 {
		t.Fatal("pending workflow created login state")
	}
	sessions := NewSessionManager(pool, NewAuthUserStore(pool), false)
	if _, _, err := sessions.Login(t.Context(), "staged@example.com", "secret123"); err == nil {
		t.Fatal("pending registration could log in")
	}
	executeRegistrationForTest(t, store, keys)
	completeRegistrationForTest(t, store)
	accepted := resetOutcomeFacts(t, store, UserRegistered)
	// No workflow history is delivered to this fresh domain projection.
	if err := ProjectBatch(t.Context(), accepted, repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	user, token, err := sessions.Login(t.Context(), "staged@example.com", "secret123")
	if err != nil || token == "" || !user.EmailVerified || user.UserRegisteredID != pending.UserID {
		t.Fatalf("accepted user could not log in: %+v %v", user, err)
	}
	if err := ProjectBatch(t.Context(), accepted, repo, nil, ProfileSource{}, nil); err != nil {
		t.Fatalf("accepted replay required workflow or decryption: %v", err)
	}
}

func TestSQLitePasswordResetProjectsOnlyEffectivePasswordChange(t *testing.T) {
	repo, pool := nativeAuthProjection(t)
	history, keys := authProjectionHistory(t)
	root := history[0].Event
	registered, err := DecodeUserRegisteredEvent(root)
	if err != nil {
		t.Fatal(err)
	}
	owner := registered.UserRegisteredID
	request := uuidv7.NewString()
	requested := mustAuthEvent(NewPasswordResetRequestedEvent(request, "alice@example.com", "token", time.Now().Add(time.Hour), owner, keys.keys[owner], nil))
	store := eventstoretest.New(t, root, requested)
	if err := ProjectBatch(t.Context(), resetOutcomeFacts(t, store, UserRegistered), repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	sessions := NewSessionManager(pool, NewAuthUserStore(pool), false)
	if _, _, err := sessions.Login(t.Context(), "alice@example.com", "secret123"); err != nil {
		t.Fatal(err)
	}
	_, err = AuthorizePasswordResetCommandHandler(t.Context(), ResetPasswordCommand{Token: "token", Password: "updated123"}, resetTokenCandidate{PasswordResetVerification{ID: request, UserID: owner}}, store, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := ProjectBatch(t.Context(), resetOutcomeFacts(t, store, PasswordResetAuthorized), repo, nil, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sessions.Login(t.Context(), "alice@example.com", "updated123"); err == nil {
		t.Fatal("authorization changed credentials")
	}
	execution := &PasswordResetExecutionEventHandler{saver: store, retriever: store}
	if err := execution.handle(t.Context(), resetOutcomeFacts(t, store, PasswordResetAuthorized)[0]); err != nil {
		t.Fatal(err)
	}
	completion := &PasswordResetCompletionEventHandler{saver: store, retriever: store}
	if err := completion.handle(t.Context(), resetOutcomeFacts(t, store, PasswordChanged)[0]); err != nil {
		t.Fatal(err)
	}

	// Deliver completion before the effect to prove it cannot mutate credentials.
	if err := ProjectBatch(t.Context(), resetOutcomeFacts(t, store, PasswordResetCompleted), repo, nil, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sessions.Login(t.Context(), "alice@example.com", "secret123"); err != nil {
		t.Fatal("workflow completion changed credentials")
	}
	effects := resetOutcomeFacts(t, store, PasswordChanged)
	if err := ProjectBatch(t.Context(), effects, repo, nil, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sessions.Login(t.Context(), "alice@example.com", "updated123"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := sessions.Login(t.Context(), "alice@example.com", "secret123"); err == nil {
		t.Fatal("old password still accepted")
	}
	if err := ProjectBatch(t.Context(), effects, repo, nil, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestSQLiteDeletionSeparatesRequestCleanupAndCompletion(t *testing.T) {
	repo, pool := nativeAuthProjection(t)
	keys := NewSubjectPiiKeyStore(pool, protectedpii.FromEnv())
	owner, foreign := uuidv7.NewString(), uuidv7.NewString()
	ownerKey, err := keys.GetOrCreateSubjectDataKey(t.Context(), owner)
	if err != nil {
		t.Fatal(err)
	}
	foreignKey, err := keys.GetOrCreateSubjectDataKey(t.Context(), foreign)
	if err != nil {
		t.Fatal(err)
	}
	hash := testAuthHash(t)
	at := time.Unix(1, 0).UTC()
	root := mustAuthEvent(NewUserRegisteredEvent(owner, "deleting-user", "deleting@example.com", "Deleting", "User", 1990, hash, at, at, ownerKey, nil))
	other := mustAuthEvent(NewUserRegisteredEvent(foreign, "retained-user", "retained@example.com", "Retained", "User", 1990, hash, at, at, foreignKey, nil))
	store := eventstoretest.New(t, root, other)
	if err := ProjectBatch(t.Context(), resetOutcomeFacts(t, store, UserRegistered), repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	users := NewAuthUserStore(pool)
	sessions := NewSessionManager(pool, users, false)
	user, _, err := sessions.Login(t.Context(), "deleting@example.com", "secret123")
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"INSERT INTO auth_verification(id,identifier,value,expires_at) VALUES ('deletion-token','password-reset:'||$1,'token',datetime('now','+1 hour'))",
		"INSERT INTO profile_stats(user_id,last_event_commit_position,last_event_prepare_position) VALUES ($1,0,0)",
		"INSERT INTO todo_items(todo_id,user_id,title,last_event_commit_position,last_event_prepare_position,created_at,updated_at) VALUES ('deletion-todo',$1,'Delete me',0,0,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
	} {
		if _, err := pool.ExecContext(t.Context(), statement, owner); err != nil {
			t.Fatal(err)
		}
	}
	snapshot := func() []int {
		t.Helper()
		counts := make([]int, 7)
		err := pool.QueryRowContext(t.Context(), `SELECT
   (SELECT count(*) FROM auth_user WHERE user_registered_id=$1),
   (SELECT count(*) FROM auth_account WHERE user_id=$1),
   (SELECT count(*) FROM auth_session WHERE user_id=$1),
   (SELECT count(*) FROM auth_verification WHERE identifier='password-reset:'||$1),
   (SELECT count(*) FROM profile_stats WHERE user_id=$1),
   (SELECT count(*) FROM todo_items WHERE user_id=$1),
   (SELECT count(*) FROM subject_pii_keys WHERE subject_id=$1)`, owner).Scan(&counts[0], &counts[1], &counts[2], &counts[3], &counts[4], &counts[5], &counts[6])
		if err != nil {
			t.Fatal(err)
		}
		return counts
	}
	if _, err := RequestAccountDeletionCommandHandler(t.Context(), RequestAccountDeletionCommand{UserRegisteredID: user.UserRegisteredID, AuthUserID: user.ID, Password: "secret123"}, store, store); err != nil {
		t.Fatal(err)
	}
	for _, count := range snapshot() {
		if count != 1 {
			t.Fatal("request modified data or keys")
		}
	}
	execution := &AccountDeletionEventHandler{data: NewAccountDataDeletionStore(pool, nil), keys: keys, saver: store, events: store}
	source := resetOutcomeFacts(t, store, AccountDeletionRequested)[0]
	if err := execution.handle(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	for _, count := range snapshot() {
		if count != 0 {
			t.Fatal("domain cleanup retained owned rows or subject key")
		}
	}
	if len(resetOutcomeFacts(t, store, AccountDeletionCompleted)) != 0 {
		t.Fatal("execution completed workflow")
	}
	if _, _, err := sessions.Login(t.Context(), "deleting@example.com", "secret123"); err == nil {
		t.Fatal("deleted account still logs in")
	}
	completion := &AccountDeletionCompletionEventHandler{saver: store, retriever: store}
	for range 2 {
		if err := execution.handle(t.Context(), source); err != nil {
			t.Fatal(err)
		}
		if err := completion.handle(t.Context(), resetOutcomeFacts(t, store, AccountDeleted)[0]); err != nil {
			t.Fatal(err)
		}
	}
	if len(resetOutcomeFacts(t, store, AccountDeleted)) != 1 || len(resetOutcomeFacts(t, store, AccountDeletionCompleted)) != 1 {
		t.Fatal("replay duplicated outcomes")
	}
	if _, _, err := sessions.Login(t.Context(), "retained@example.com", "secret123"); err != nil {
		t.Fatal("deletion affected unrelated account")
	}
	if _, ok, err := keys.GetSubjectDataKey(t.Context(), foreign); err != nil || !ok {
		t.Fatal("deletion destroyed unrelated subject key")
	}
}

func TestSQLiteResetAuthorizationNeedsOnlyTokenDiscovery(t *testing.T) {
	repo, pool := nativeAuthProjection(t)
	history, keys := authProjectionHistory(t)
	store := eventstoretest.New(t, history[0].Event)
	if err := ProjectBatch(t.Context(), resetOutcomeFacts(t, store, UserRegistered), repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	requested, err := RequestPasswordResetCommandHandler(t.Context(), RequestPasswordResetCommand{EmailAddress: "alice@example.com"}, store, store, keys)
	if err != nil {
		t.Fatal(err)
	}
	if err := ProjectBatch(t.Context(), resetOutcomeFacts(t, store, PasswordResetRequested), repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	// Only this isolated test schema is changed. Token discovery must not join or read user projection rows.
	if _, err := pool.ExecContext(t.Context(), "DROP TABLE auth_user"); err != nil {
		t.Fatal(err)
	}
	id, err := AuthorizePasswordResetCommandHandler(t.Context(), ResetPasswordCommand{Token: requested.Token, Password: "updated123"}, NewVerificationStore(pool), store, store)
	if err != nil || id == "" {
		t.Fatalf("authorization depended on full user projection: %v", err)
	}
	authorized := resetOutcomeFacts(t, store, PasswordResetAuthorized)
	if len(authorized) != 1 || len(resetOutcomeFacts(t, store, PasswordChanged)) != 0 {
		t.Fatal("authorization bypassed its stage boundary")
	}
	writeContext, err := store.GetWriteContext(t.Context(), authorized[0].WriteID)
	if err != nil || len(writeContext.Consistency) != 2 || len(writeContext.Consistency[0].Query.Criteria) != 5 {
		t.Fatal("workflow/deletion and collision observations not retained")
	}
}

func TestSQLiteRegistrationReadinessRequiresVerifiedUserAndCredential(t *testing.T) {
	repo, pool := nativeAuthProjection(t)
	history, keys := authProjectionHistory(t)
	root := history[0].Event
	registered, err := DecodeUserRegisteredEvent(root)
	if err != nil {
		t.Fatal(err)
	}
	users := NewAuthUserStore(pool)
	ready, err := users.RegistrationReady(t.Context(), registered.UserRegisteredID)
	if err != nil || ready {
		t.Fatal("missing projection reported ready")
	}
	if err := ProjectBatch(t.Context(), history[:1], repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	ready, err = users.RegistrationReady(t.Context(), registered.UserRegisteredID)
	if err != nil || !ready {
		t.Fatalf("verified credential projection not ready: %v", err)
	}
	if _, err := pool.ExecContext(t.Context(), "UPDATE auth_user SET email_verified=false,email_verified_at=NULL WHERE id=$1", registered.UserRegisteredID); err != nil {
		t.Fatal(err)
	}
	ready, err = users.RegistrationReady(t.Context(), registered.UserRegisteredID)
	if err != nil || ready {
		t.Fatal("unverified user reported ready")
	}
	if _, err := pool.ExecContext(t.Context(), "UPDATE auth_user SET email_verified=true,email_verified_at=CURRENT_TIMESTAMP WHERE id=$1", registered.UserRegisteredID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.ExecContext(t.Context(), "DELETE FROM auth_account WHERE user_id=$1", registered.UserRegisteredID); err != nil {
		t.Fatal(err)
	}
	ready, err = users.RegistrationReady(t.Context(), registered.UserRegisteredID)
	if err != nil || ready {
		t.Fatal("user without credential reported ready")
	}
}

func TestSQLitePasswordResetReadinessUsesFullEffectPosition(t *testing.T) {
	repo, pool := nativeAuthProjection(t)
	history, keys := authProjectionHistory(t)
	root, err := DecodeUserRegisteredEvent(history[0].Event)
	if err != nil {
		t.Fatal(err)
	}
	users := NewAuthUserStore(pool)
	check := func(p eventstore.Position, want bool) {
		t.Helper()
		got, err := users.PasswordResetReady(t.Context(), root.UserRegisteredID, p)
		if err != nil || got != want {
			t.Fatalf("readiness at %+v: %v %v", p, got, err)
		}
	}
	check(history[0].Position, false)
	if err := ProjectBatch(t.Context(), history[:1], repo, keys, ProfileSource{}, nil); err != nil {
		t.Fatal(err)
	}
	check(history[0].Position, true)
	later := history[0].Position
	later.Prepare++
	check(later, false)
	later = history[0].Position
	later.Commit++
	check(later, false)
	if _, err := pool.ExecContext(t.Context(), "DELETE FROM auth_account WHERE user_id=$1", root.UserRegisteredID); err != nil {
		t.Fatal(err)
	}
	check(history[0].Position, false)
}
