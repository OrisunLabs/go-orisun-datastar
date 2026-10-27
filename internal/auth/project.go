package auth

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

// ProfileSource translates owning profile contracts at composition without a reverse package dependency.
type ProfileSource struct {
	EventTypes []string
	Decode     func(context.Context, eventstore.DomainEvent, SubjectPiiKeyReader) (AuthChange, error)
}

// ProjectBatch applies an ordered delivery inside one owner transaction.
// Invalidation runs only after commit; known positions retain retry routing.
func ProjectBatch(ctx context.Context, events []eventstore.ResolvedEvent, repository ProjectionRepository, keys SubjectPiiKeyReader, source ProfileSource, publisher UserChangePublisher) error {
	for i, event := range events {
		if event.Position.Commit < 0 || event.Position.Prepare < 0 || (i > 0 && !event.Position.After(events[i-1].Position)) {
			return eventstore.ErrInvalidEvent
		}
	}
	if len(events) == 0 {
		return nil
	}
	affected := map[string]bool{}
	err := repository.WithinTransaction(ctx, func(txCtx context.Context) error {
		for _, resolved := range events {
			if !authProjectionType(resolved.Event.EventType, source) {
				continue
			}
			owner, err := repository.Processed(txCtx, resolved.Position)
			if err != nil {
				return err
			}
			if owner != "" {
				affected[owner] = true
				continue
			}
			change, err := decodeAuthChange(txCtx, resolved, keys, source)
			if err != nil {
				return err
			}
			state, err := repository.Load(txCtx, change.AuthUserID())
			if err != nil {
				return err
			}
			var verification *VerificationFact
			if fact, ok := change.(VerificationFact); ok {
				existing, err := repository.LoadVerification(txCtx, fact.ID)
				if err != nil {
					return err
				}
				if existing != nil && (existing.UserID != fact.UserID || existing.Purpose != fact.Purpose || existing.ValueHash != fact.ValueHash || !existing.ExpiresAt.Truncate(time.Microsecond).Equal(fact.ExpiresAt.Truncate(time.Microsecond))) {
					return eventstore.ErrConflict
				}
				if existing == nil {
					verification = &fact
				}
			}
			next, err := applyAuthChange(state, change, resolved.Position)
			if err != nil {
				return err
			}
			occurrence := Occurrence{UserID: change.AuthUserID(), EventType: resolved.Event.EventType, Position: resolved.Position}
			if err := repository.Save(txCtx, ProjectionUpdate{State: next, Occurrence: occurrence, Verification: verification}); err != nil {
				return err
			}
			affected[next.UserID] = true
		}
		return nil
	})
	if err != nil {
		return err
	}
	if publisher != nil {
		for id := range affected {
			if err := publisher.InvalidateAuthUser(ctx, id); err != nil {
				return err
			}
		}
	}
	return nil
}
func authProjectionType(kind string, source ProfileSource) bool {
	switch kind {
	case UserRegistered, UserNameChanged, PasswordResetRequested, PasswordChanged:
		return true
	}
	return slices.Contains(source.EventTypes, kind)
}
func decryptAuthValue(ctx context.Context, keys SubjectPiiKeyReader, user string, value protectedpii.Value) (string, error) {
	key, ok, err := keys.GetSubjectDataKey(ctx, user)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", eventstore.ErrNotFound
	}
	return protectedpii.FromEnv().DecryptWithDataKey(value, key)
}
func decodeAuthChange(ctx context.Context, resolved eventstore.ResolvedEvent, keys SubjectPiiKeyReader, source ProfileSource) (AuthChange, error) {
	e := resolved.Event
	switch e.EventType {
	case UserRegistered:
		v, err := DecodeUserRegisteredEvent(e)
		if err != nil {
			return nil, err
		}
		key, ok, err := keys.GetSubjectDataKey(ctx, v.UserRegisteredID)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, eventstore.ErrNotFound
		}
		protector := protectedpii.FromEnv()
		first, err := protector.DecryptWithDataKey(v.FirstName, key)
		if err != nil {
			return nil, err
		}
		last, err := protector.DecryptWithDataKey(v.LastName, key)
		if err != nil {
			return nil, err
		}
		username, err := protector.DecryptWithDataKey(v.Username, key)
		if err != nil {
			return nil, err
		}
		email, err := protector.DecryptWithDataKey(v.Email, key)
		if err != nil {
			return nil, err
		}
		name := strings.TrimSpace(first + " " + last)
		if name == "" {
			name = username
		}
		verifiedAt, _ := time.Parse(time.RFC3339Nano, v.EmailVerifiedAt)
		return RegistrationInput{UserID: v.UserRegisteredID, Name: name, Username: username, Email: email, PasswordHash: v.PasswordHash, EmailVerifiedAt: verifiedAt}, nil
	case UserNameChanged:
		v, err := DecodeUserNameChangedEvent(e)
		if err != nil {
			return nil, err
		}
		name, err := decryptAuthValue(ctx, keys, v.Scopes.UserRegisteredID, v.Name)
		if err == nil && name == "" {
			err = eventstore.ErrInvalidEvent
		}
		return NameInput{v.Scopes.UserRegisteredID, name}, err
	case PasswordChanged:
		v, err := DecodePasswordChangedEvent(e)
		if err != nil {
			return nil, err
		}
		return PasswordInput{v.Scopes.UserRegisteredID, v.PasswordHash}, nil
	case PasswordResetRequested:
		v, err := DecodePasswordResetRequestedEvent(e)
		if err != nil {
			return nil, err
		}
		token, err := decryptAuthValue(ctx, keys, v.Scopes.UserRegisteredID, v.ResetToken)
		if err != nil {
			return nil, err
		}
		if token == "" || protectedpii.FromEnv().SensitiveBlindIndex(PasswordResetRequestedTokenField, token) != v.ResetTokenHash {
			return nil, eventstore.ErrInvalidEvent
		}
		at, _ := time.Parse(time.RFC3339Nano, v.ExpiresAt)
		return VerificationFact{v.PasswordResetRequestedID, v.Scopes.UserRegisteredID, "password-reset", verificationValueHash("password-reset-token", token), at}, nil
	default:
		if source.Decode != nil {
			return source.Decode(ctx, e, keys)
		}
		return nil, eventstore.ErrInvalidEvent
	}
}
func applyAuthChange(state *AuthState, change AuthChange, pos eventstore.Position) (AuthState, error) {
	if root, ok := change.(RegistrationInput); ok {
		if state == nil {
			return AuthState{UserID: root.UserID, Name: root.Name, Username: root.Username, Email: root.Email, PasswordHash: root.PasswordHash, EmailVerified: true, Verification: &EmailValidationFact{At: root.EmailVerifiedAt}, Position: &pos}, nil
		}
		if state.UserID != root.UserID || state.Email != root.Email || state.Username != root.Username {
			return AuthState{}, eventstore.ErrConflict
		}
	} else if state == nil {
		return AuthState{}, fmt.Errorf("authentication input precedes registration: %w", eventstore.ErrNotFound)
	}
	next := *state
	if state.Position != nil && !pos.After(*state.Position) {
		return next, nil
	}
	switch c := change.(type) {
	case RegistrationInput: // registration replay retains subsequent facts
	case NameInput:
		if strings.TrimSpace(c.Name) != "" {
			next.Name = c.Name
		}
	case PasswordInput:
		next.PasswordHash = c.PasswordHash
	case VerificationFact:
	case ImageInput:
		next.Image = c.URL
	case BioInput:
		next.Bio = c.Bio
	case HeaderImageInput:
		next.HeaderImage = c.URL
	default:
		return AuthState{}, eventstore.ErrInvalidEvent
	}
	next.Position = &pos
	return next, nil
}
