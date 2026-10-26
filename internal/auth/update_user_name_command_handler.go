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
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

type UpdateUserNameCommand struct {
	User     views.User
	Name     string
	Metadata CommandMetadata
}

type UpdateUserNameResult struct {
	Name    string
	Skipped bool
}

func UpdateUserNameCommandHandler(ctx context.Context, command UpdateUserNameCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, keys SubjectPiiKeyPort) (UpdateUserNameResult, error) {
	if err := commandlimits.Assert(command); err != nil {
		return UpdateUserNameResult{}, err
	}
	model, err := loadUpdateUserNameContext(ctx, command, retriever, keys)
	if err != nil {
		return UpdateUserNameResult{}, err
	}
	if !model.userExists {
		return UpdateUserNameResult{}, errors.New("registered user event not found")
	}
	if model.name == model.nextName {
		return UpdateUserNameResult{Name: model.nextName, Skipped: true}, nil
	}
	eventID := uuidv7.NewString()
	event, err := NewUserNameChangedEvent(eventID, model.nextName, time.Now(), command.User.UserRegisteredID, model.subjectKey, nil)
	if err != nil {
		return UpdateUserNameResult{}, err
	}
	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}}); err != nil {
		return UpdateUserNameResult{}, err
	}
	return UpdateUserNameResult{Name: model.nextName}, nil
}

type updateUserNameContext struct {
	userExists bool
	name       string
	nextName   string
	subjectKey protectedpii.SubjectDataKey
	position   eventstore.Position
	events     []eventstore.ResolvedEvent
	query      eventstore.Query
}

func loadUpdateUserNameContext(ctx context.Context, command UpdateUserNameCommand, retriever eventstore.LatestRetriever, keys SubjectPiiKeyPort) (*updateUserNameContext, error) {
	name := strings.TrimSpace(command.Name)
	if name == "" {
		return nil, errors.New("name is required")
	}
	userQuery := updateUserNameUserRegisteredQuery(command.User.UserRegisteredID)
	nameQuery := updateUserNameUserNameChangedByUserQuery(command.User.UserRegisteredID)
	query := updateUserNameCombineQueries(userQuery, nameQuery)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	events := eventstore.EventsFromLatest(latest.Results)
	for _, checked := range events {
		if err := ValidateEvent(checked.Event); err != nil {
			return nil, err
		}
	}
	subjectKey, ok, err := keys.GetSubjectDataKey(ctx, command.User.UserRegisteredID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, eventstore.ErrNotFound
	}
	model := &updateUserNameContext{nextName: name, subjectKey: subjectKey, position: latest.ContextPosition, events: events, query: query}
	for _, event := range events {
		if err := model.handle(event); err != nil {
			return nil, err
		}
	}
	return model, nil
}

func (m *updateUserNameContext) handle(resolved eventstore.ResolvedEvent) error {
	switch resolved.Event.EventType {
	case UserRegistered:
		m.userExists = true
		protector := protectedpii.FromEnv()
		firstName, err := protectedpii.DecryptEventStringWithDataKey(protector, m.subjectKey, resolved.Event.Data, UserRegisteredFirstNameField)
		if err != nil {
			return err
		}
		lastName, err := protectedpii.DecryptEventStringWithDataKey(protector, m.subjectKey, resolved.Event.Data, UserRegisteredLastNameField)
		if err != nil {
			return err
		}
		username, err := protectedpii.DecryptEventStringWithDataKey(protector, m.subjectKey, resolved.Event.Data, UserRegisteredUsernameField)
		if err != nil {
			return err
		}
		m.name = strings.TrimSpace(firstName + " " + lastName)
		if m.name == "" {
			m.name = username
		}
	case UserNameChanged:
		decryptedname, err := protectedpii.DecryptEventStringWithDataKey(protectedpii.FromEnv(), m.subjectKey, resolved.Event.Data, UserNameChangedNameField)
		if err != nil {
			return err
		}
		m.name = decryptedname
	}
	return nil
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
