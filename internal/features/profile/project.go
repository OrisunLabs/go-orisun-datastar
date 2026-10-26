package profile

import (
	"context"
	"fmt"
	"strings"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

const (
	userRegistered  = "UserRegistered"
	userNameChanged = "UserNameChanged"
)

func decodeProfileChange(ctx context.Context, resolved eventstore.ResolvedEvent, keys SubjectDataKeyReader) (ProfileChange, error) {
	if resolved.Position.Commit < 0 || resolved.Position.Prepare < 0 {
		return nil, eventstore.ErrInvalidEvent
	}

	identity := ProfileUpdateIdentity{EventType: resolved.Event.EventType, Position: resolved.Position}
	switch resolved.Event.EventType {
	case userRegistered:
		return projectRegistration(ctx, resolved, identity, keys)
	case userNameChanged:
		return projectName(ctx, resolved, identity, keys)
	case ProfileBioUpdated:
		return projectBio(ctx, resolved, identity, keys)
	case ProfileImageUploaded:
		return projectImage(ctx, resolved, identity)
	case ProfileHeaderImageUploaded:
		return projectHeaderImage(ctx, resolved, identity)
	default:
		return nil, fmt.Errorf("unhandled profile read model event type %q", resolved.Event.EventType)
	}
}
func projectRegistration(ctx context.Context, resolved eventstore.ResolvedEvent, identity ProfileUpdateIdentity, keys SubjectDataKeyReader) (ProfileChange, error) {
	registered, err := auth.DecodeUserRegisteredEvent(resolved.Event)
	if err != nil {
		return nil, err
	}
	protector := protectedpii.FromEnv()
	key, ok, err := keys.GetSubjectDataKey(ctx, registered.UserRegisteredID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, eventstore.ErrNotFound
	}
	username, err := protector.DecryptWithDataKey(registered.Username, key)
	if err != nil {
		return nil, err
	}
	emailAddress, err := protector.DecryptWithDataKey(registered.Email, key)
	if err != nil {
		return nil, err
	}
	firstName, err := protector.DecryptWithDataKey(registered.FirstName, key)
	if err != nil {
		return nil, err
	}
	lastName, err := protector.DecryptWithDataKey(registered.LastName, key)
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(firstName + " " + lastName)
	identity.UserID = registered.UserRegisteredID
	return RegistrationUpdate{ProfileUpdateIdentity: identity, Name: name, Username: username, Email: emailAddress}, nil
}

func projectName(ctx context.Context, resolved eventstore.ResolvedEvent, identity ProfileUpdateIdentity, keys SubjectDataKeyReader) (ProfileChange, error) {
	changed, err := auth.DecodeUserNameChangedEvent(resolved.Event)
	if err != nil {
		return nil, err
	}
	subjectKey, ok, err := keys.GetSubjectDataKey(ctx, changed.Scopes.UserRegisteredID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, eventstore.ErrNotFound
	}
	name, err := protectedpii.FromEnv().DecryptWithDataKey(changed.Name, subjectKey)
	if err != nil {
		return nil, err
	}
	identity.UserID = changed.Scopes.UserRegisteredID
	return NameUpdate{ProfileUpdateIdentity: identity, Name: name}, nil
}

func projectBio(ctx context.Context, resolved eventstore.ResolvedEvent, identity ProfileUpdateIdentity, keys SubjectDataKeyReader) (ProfileChange, error) {
	updated, err := DecodeProfileBioUpdatedEvent(resolved.Event)
	if err != nil {
		return nil, err
	}
	subjectKey, ok, err := keys.GetSubjectDataKey(ctx, updated.UserID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, eventstore.ErrNotFound
	}
	bio, err := decryptProfileBio(updated, subjectKey)
	if err != nil {
		return nil, err
	}
	identity.UserID = updated.UserID
	return BioUpdate{ProfileUpdateIdentity: identity, Bio: bio}, nil
}

func projectImage(ctx context.Context, resolved eventstore.ResolvedEvent, identity ProfileUpdateIdentity) (ProfileChange, error) {
	uploaded, err := DecodeProfileImageUploadedEvent(resolved.Event)
	if err != nil {
		return nil, err
	}
	identity.UserID = uploaded.UserID
	return ImageUpdate{ProfileUpdateIdentity: identity, ImageURL: uploaded.ImageURL}, nil
}

func projectHeaderImage(ctx context.Context, resolved eventstore.ResolvedEvent, identity ProfileUpdateIdentity) (ProfileChange, error) {
	uploaded, err := DecodeProfileHeaderImageUploadedEvent(resolved.Event)
	if err != nil {
		return nil, err
	}
	identity.UserID = uploaded.UserID
	return HeaderImageUpdate{ProfileUpdateIdentity: identity, ImageURL: uploaded.ImageURL}, nil
}

// ProjectBatch persists each occurrence in source order before advancing.
// Returned user IDs may be published only after this transaction commits.
func ProjectBatch(ctx context.Context, events []eventstore.ResolvedEvent, repo ProjectionRepository, keys SubjectDataKeyReader) ([]string, error) {
	for i, e := range events {
		if e.Position.Commit < 0 || e.Position.Prepare < 0 {
			return nil, eventstore.ErrInvalidEvent
		}
		if i > 0 && !profilePositionBefore(events[i-1].Position, e.Position) {
			return nil, fmt.Errorf("profile requires strictly ordered positions: %w", eventstore.ErrInvalidEvent)
		}
	}
	if len(events) == 0 {
		return nil, nil
	}
	ids := []string{}
	seen := map[string]bool{}
	err := repo.WithinTransaction(ctx, func(txCtx context.Context) error {
		for _, e := range events {
			id, err := repo.FindProcessedProfile(txCtx, e.Position)
			if err != nil {
				return err
			}
			if id == "" {
				change, err := decodeProfileChange(txCtx, e, keys)
				if err != nil {
					return err
				}
				identity := change.identity()
				id = identity.UserID
				current, err := repo.LoadProfile(txCtx, id)
				if err != nil {
					return err
				}
				next, err := applyProfile(current, change)
				if err != nil {
					return err
				}
				if err := repo.SaveProfile(txCtx, next, identity); err != nil {
					return err
				}
			}
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ids, nil
}
func profilePositionBefore(a, b eventstore.Position) bool {
	return a.Commit < b.Commit || (a.Commit == b.Commit && a.Prepare < b.Prepare)
}
func applyProfile(current *ProfileState, change ProfileChange) (ProfileState, error) {
	identity := change.identity()
	next := ProfileState{UserID: identity.UserID, Position: identity.Position}
	if current != nil {
		next = *current
		if !profilePositionBefore(current.Position, identity.Position) {
			return next, nil
		}
		next.Position = identity.Position
	}
	switch c := change.(type) {
	case RegistrationUpdate:
		next.Name = &c.Name
		next.Username = &c.Username
		next.Email = &c.Email
	case NameUpdate:
		next.Name = &c.Name
	case BioUpdate:
		next.Bio = &c.Bio
	case ImageUpdate:
		next.ImageURL = &c.ImageURL
	case HeaderImageUpdate:
		next.HeaderImageURL = &c.ImageURL
	default:
		return ProfileState{}, fmt.Errorf("unknown profile input %T", change)
	}
	return next, nil
}
