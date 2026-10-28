package auth

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

type UpdateUserNameCommand struct {
	UserRegisteredID string
	Name             string
	Metadata         CommandMetadata
}

type UpdateUserNameResult struct {
	Name    string
	Skipped bool
}

func UpdateUserNameCommandHandler(ctx context.Context, command UpdateUserNameCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, keys SubjectPiiKeyReader) (UpdateUserNameResult, error) {
	if err := commandlimits.Assert(command); err != nil {
		return UpdateUserNameResult{}, err
	}
	model, err := loadUpdateUserNameContext(ctx, command, retriever, keys)
	if err != nil {
		return UpdateUserNameResult{}, err
	}
	if model.name == model.nextName {
		return UpdateUserNameResult{Name: model.nextName, Skipped: true}, nil
	}
	eventID := uuidv7.NewString()
	event, err := NewUserNameChangedEvent(eventID, model.nextName, time.Now(), command.UserRegisteredID, model.subjectKey, nil)
	if err != nil {
		return UpdateUserNameResult{}, err
	}
	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}}); err != nil {
		return UpdateUserNameResult{}, err
	}
	return UpdateUserNameResult{Name: model.nextName}, nil
}

type updateUserNameContext struct {
	name, nextName string
	subjectKey     protectedpii.SubjectDataKey
	position       eventstore.Position
	query          eventstore.Query
}

func loadUpdateUserNameContext(ctx context.Context, command UpdateUserNameCommand, retriever eventstore.LatestRetriever, keys SubjectPiiKeyReader) (*updateUserNameContext, error) {
	if !validAuthID(command.UserRegisteredID) {
		return nil, eventstore.ErrInvalidEvent
	}
	name := strings.TrimSpace(command.Name)
	if name == "" {
		return nil, errors.New("name is required")
	}
	query := updateUserNameCombineQueries(updateUserNameUserRegisteredQuery(command.UserRegisteredID), updateUserNameUserNameChangedByUserQuery(command.UserRegisteredID), updateUserNameAccountDeletedQuery(command.UserRegisteredID))
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	var root *UserRegisteredEvent
	var changed *UserNameChangedEvent
	var rootPosition, changedPosition, deletedPosition eventstore.Position
	deleted := false
	for _, e := range eventstore.EventsFromLatest(latest.Results) {
		switch e.Event.EventType {
		case UserRegistered:
			v, err := DecodeUserRegisteredEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.UserRegisteredID != command.UserRegisteredID {
				return nil, eventstore.ErrInvalidEvent
			}
			root = &v
			rootPosition = e.Position
		case AccountDeleted:
			v, err := DecodeAccountDeletedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.Scopes.UserRegisteredID != command.UserRegisteredID {
				return nil, eventstore.ErrInvalidEvent
			}
			deleted = true
			deletedPosition = e.Position
		case UserNameChanged:
			v, err := DecodeUserNameChangedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.Scopes.UserRegisteredID != command.UserRegisteredID {
				return nil, eventstore.ErrInvalidEvent
			}
			if changed == nil || e.Position.After(changedPosition) {
				changed = &v
				changedPosition = e.Position
			}
		default:
			return nil, eventstore.ErrInvalidEvent
		}
	}
	if root == nil {
		return nil, eventstore.ErrNotFound
	}
	if changed != nil && !changedPosition.After(rootPosition) {
		return nil, eventstore.ErrInvalidEvent
	}
	if deleted {
		if !deletedPosition.After(rootPosition) {
			return nil, eventstore.ErrInvalidEvent
		}
		return nil, eventstore.ErrNotFound
	}
	key, ok, err := keys.GetSubjectDataKey(ctx, command.UserRegisteredID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, eventstore.ErrNotFound
	}
	model := &updateUserNameContext{nextName: name, subjectKey: key, query: query, position: latest.ContextPosition}
	if changed != nil {
		model.name, err = decryptUpdateUserNameValue(changed.Name, UserNameChangedNameField, key)
		if err == nil && model.name == "" {
			err = eventstore.ErrInvalidEvent
		}
		return model, err
	}
	first, err := decryptUpdateUserNameValue(root.FirstName, UserRegisteredFirstNameField, key)
	if err != nil {
		return nil, err
	}
	last, err := decryptUpdateUserNameValue(root.LastName, UserRegisteredLastNameField, key)
	if err != nil {
		return nil, err
	}
	model.name = strings.TrimSpace(first + " " + last)
	if model.name == "" {
		model.name, err = decryptUpdateUserNameValue(root.Username, UserRegisteredUsernameField, key)
		if err != nil {
			return nil, err
		}
		if len(model.name) < 4 {
			return nil, eventstore.ErrInvalidEvent
		}
	}
	return model, nil
}
func decryptUpdateUserNameValue(value protectedpii.Value, field string, key protectedpii.SubjectDataKey) (string, error) {
	protector := protectedpii.FromEnv()
	plain, err := protector.DecryptWithDataKey(value, key)
	if err != nil {
		return "", err
	}
	if plain != strings.TrimSpace(plain) || protector.BlindIndex(field, plain) != value.BlindIndex {
		return "", eventstore.ErrInvalidEvent
	}
	return plain, nil
}

func updateUserNameCombineQueries(queries ...eventstore.Query) eventstore.Query {
	combined := eventstore.Query{}
	for _, query := range queries {
		combined.Criteria = append(combined.Criteria, query.Criteria...)
	}
	return combined
}

func updateUserNameUserNameChangedByUserQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: UserNameChanged},
		{Key: ScopeUserRegisteredIDField, Value: userRegisteredID},
	}}}}
}

func updateUserNameUserRegisteredQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: UserRegistered},
		{Key: UserRegisteredIDField, Value: userRegisteredID},
	}}}}
}

func updateUserNameAccountDeletedQuery(userID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeleted}, {Key: ScopeUserRegisteredIDField, Value: userID}}}}}
}
