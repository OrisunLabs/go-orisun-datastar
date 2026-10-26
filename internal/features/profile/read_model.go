package profile

import (
	"context"
	"strings"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

const (
	userRegistered  = "UserRegistered"
	userNameChanged = "UserNameChanged"
)

type ReadModel struct {
	queries *dbsql.Queries
}

func NewReadModel(db *pgxpool.Pool) *ReadModel {
	return &ReadModel{queries: dbsql.New(db)}
}

func (m *ReadModel) User(ctx context.Context, userRegisteredID string) (views.User, error) {
	row, err := m.queries.ProfileUser(ctx, userRegisteredID)
	if err != nil {
		return views.User{}, err
	}
	return views.User{
		ID:               row.UserID,
		UserRegisteredID: row.UserID,
		Name:             row.Name,
		Username:         row.Username,
		Email:            row.Email,
		Image:            row.Image,
		Bio:              row.Bio,
		HeaderImageURL:   row.HeaderImageUrl,
	}, nil
}

func (m *ReadModel) UpsertRegisteredUser(ctx context.Context, resolved eventstore.ResolvedEvent, keys SubjectDataKeyReader) error {
	if _, err := auth.DecodeUserRegisteredEvent(resolved.Event); err != nil {
		return err
	}
	data := resolved.Event.Data
	userRegisteredID, _ := data[auth.UserRegisteredIDField].(string)
	protector := protectedpii.FromEnv()
	subjectKey, ok, err := keys.GetSubjectDataKey(ctx, userRegisteredID)
	if err != nil {
		return err
	}
	if !ok {
		return eventstore.ErrNotFound
	}
	username, err := protectedpii.DecryptEventStringWithDataKey(protector, subjectKey, data, auth.UserRegisteredUsernameField)
	if err != nil {
		return err
	}
	emailAddress, err := protectedpii.DecryptEventStringWithDataKey(protector, subjectKey, data, auth.UserRegisteredEmailField)
	if err != nil {
		return err
	}
	firstName, err := protectedpii.DecryptEventStringWithDataKey(protector, subjectKey, data, auth.UserRegisteredFirstNameField)
	if err != nil {
		return err
	}
	lastName, err := protectedpii.DecryptEventStringWithDataKey(protector, subjectKey, data, auth.UserRegisteredLastNameField)
	if err != nil {
		return err
	}
	name := strings.TrimSpace(firstName + " " + lastName)
	return m.queries.UpsertRegisteredProfileUser(ctx, dbsql.UpsertRegisteredProfileUserParams{
		UserID:                   userRegisteredID,
		Name:                     stringPtr(name),
		Username:                 stringPtr(username),
		Email:                    stringPtr(emailAddress),
		LastEventCommitPosition:  resolved.Position.Commit,
		LastEventPreparePosition: resolved.Position.Prepare,
	})
}

func (m *ReadModel) UpdateName(ctx context.Context, resolved eventstore.ResolvedEvent, keys SubjectDataKeyReader) error {
	if _, err := auth.DecodeUserNameChangedEvent(resolved.Event); err != nil {
		return err
	}
	userRegisteredID, _ := eventstore.Scopes(resolved.Event.Data)[auth.ScopeUserRegisteredIDKey].(string)
	subjectKey, ok, err := keys.GetSubjectDataKey(ctx, userRegisteredID)
	if err != nil {
		return err
	}
	if !ok {
		return eventstore.ErrNotFound
	}
	name, err := protectedpii.DecryptEventStringWithDataKey(protectedpii.FromEnv(), subjectKey, resolved.Event.Data, auth.UserNameChangedNameField)
	if err != nil {
		return err
	}
	return m.queries.UpsertProfileName(ctx, dbsql.UpsertProfileNameParams{
		UserID:                   userRegisteredID,
		Name:                     stringPtr(name),
		LastEventCommitPosition:  resolved.Position.Commit,
		LastEventPreparePosition: resolved.Position.Prepare,
	})
}

func (m *ReadModel) UpdateBio(ctx context.Context, resolved eventstore.ResolvedEvent, keys SubjectDataKeyReader) error {
	value, err := DecodeProfileBioUpdatedEvent(resolved.Event)
	if err != nil {
		return err
	}
	key, ok, err := keys.GetSubjectDataKey(ctx, value.UserID)
	if err != nil {
		return err
	}
	if !ok {
		return eventstore.ErrNotFound
	}
	bio, err := decryptProfileBio(value, key)
	if err != nil {
		return err
	}
	return m.queries.UpsertProfileBio(ctx, dbsql.UpsertProfileBioParams{UserID: value.UserID, Bio: stringPtr(bio), LastEventCommitPosition: resolved.Position.Commit, LastEventPreparePosition: resolved.Position.Prepare})
}
func (m *ReadModel) UpdateImage(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	value, err := DecodeProfileImageUploadedEvent(resolved.Event)
	if err != nil {
		return err
	}
	return m.queries.UpsertProfileImage(ctx, dbsql.UpsertProfileImageParams{UserID: value.UserID, Image: stringPtr(value.ImageURL), LastEventCommitPosition: resolved.Position.Commit, LastEventPreparePosition: resolved.Position.Prepare})
}
func (m *ReadModel) UpdateHeaderImage(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	value, err := DecodeProfileHeaderImageUploadedEvent(resolved.Event)
	if err != nil {
		return err
	}
	return m.queries.UpsertProfileHeaderImage(ctx, dbsql.UpsertProfileHeaderImageParams{UserID: value.UserID, HeaderImageUrl: stringPtr(value.ImageURL), LastEventCommitPosition: resolved.Position.Commit, LastEventPreparePosition: resolved.Position.Prepare})
}

func stringPtr(value string) *string {
	return &value
}
