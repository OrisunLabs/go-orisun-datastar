package profile

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

type UpdateProfileBioCommand struct {
	UserID   string
	Bio      string
	Metadata eventstore.CommandMetadata
}

func UpdateProfileBioCommandHandler(ctx context.Context, command UpdateProfileBioCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, keys SubjectDataKeyReader) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadUpdateProfileBioContext(ctx, command, retriever, keys)
	if err != nil {
		return err
	}
	if model.bio == model.nextBio {
		return nil
	}
	event, err := NewProfileBioUpdatedEvent(model.eventID, model.nextBio, time.Now(), command.UserID, model.subjectKey, nil)
	if err != nil {
		return err
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}})
	return err
}

type updateProfileBioContext struct {
	userExists bool
	bio        string
	nextBio    string
	subjectKey protectedpii.SubjectDataKey
	eventID    string
	position   eventstore.Position
	events     []eventstore.ResolvedEvent
	query      eventstore.Query
}

func loadUpdateProfileBioContext(ctx context.Context, command UpdateProfileBioCommand, retriever eventstore.LatestRetriever, keys SubjectDataKeyReader) (*updateProfileBioContext, error) {
	if !validProfileID(command.UserID) {
		return nil, eventstore.ErrInvalidEvent
	}
	bio := strings.TrimSpace(command.Bio)
	if len(bio) > 280 {
		return nil, errors.New("bio must be 280 characters or fewer")
	}
	userQuery := updateProfileBioRegisteredUserQuery(command.UserID)
	bioQuery := updateProfileBioProfileUserEventQuery(ProfileBioUpdated, command.UserID)
	query := updateProfileBioCombineQueries(userQuery, bioQuery)
	subjectKey, ok, err := keys.GetSubjectDataKey(ctx, command.UserID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, eventstore.ErrNotFound
	}
	model := &updateProfileBioContext{
		nextBio:    bio,
		subjectKey: subjectKey,
		eventID:    uuidv7.NewString(),
		position:   eventstore.NoEventPosition,
		query:      query,
	}
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model.events = eventstore.EventsFromLatest(latest.Results)
	model.position = latest.ContextPosition
	for _, event := range model.events {
		if event.Event.EventType == auth.UserRegistered {
			if _, err := auth.DecodeUserRegisteredEvent(event.Event); err != nil {
				return nil, err
			}
		}
		if event.Event.EventType != auth.UserRegistered {
			userID, err := profileEventUserID(event.Event)
			if err != nil {
				return nil, err
			}
			if userID != command.UserID {
				return nil, eventstore.ErrInvalidEvent
			}
		}
		if err := model.handle(event); err != nil {
			return nil, err
		}
	}
	if !model.userExists {
		return nil, eventstore.ErrNotFound
	}
	return model, nil
}

func (m *updateProfileBioContext) handle(resolved eventstore.ResolvedEvent) error {
	switch resolved.Event.EventType {
	case "UserRegistered":
		m.userExists = true
	case ProfileBioUpdated:
		value, err := DecodeProfileBioUpdatedEvent(resolved.Event)
		if err != nil {
			return err
		}
		bio, err := decryptProfileBio(value, m.subjectKey)
		if err != nil {
			return err
		}
		m.bio = bio
	}
	return nil
}

func updateProfileBioCombineQueries(queries ...eventstore.Query) eventstore.Query {
	combined := eventstore.Query{}
	for _, query := range queries {
		combined.Criteria = append(combined.Criteria, query.Criteria...)
	}
	return combined
}

func updateProfileBioProfileUserEventQuery(eventType, userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: eventType},
		{Key: ProfileUserIDField, Value: userRegisteredID},
	}}}}
}

func updateProfileBioRegisteredUserQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: auth.UserRegistered},
		{Key: auth.UserRegisteredIDField, Value: userRegisteredID},
	}}}}
}
