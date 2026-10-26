package profile

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

type UploadProfileImageCommand struct {
	UserID      string
	Data        []byte
	ContentType string
	Header      bool
	Metadata    eventstore.CommandMetadata
}

type UploadProfileImageResult struct {
	URL string
}

func UploadProfileImageCommandHandler(ctx context.Context, command UploadProfileImageCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, storage ObjectStore) (UploadProfileImageResult, error) {
	if err := commandlimits.Assert(command); err != nil {
		return UploadProfileImageResult{}, err
	}
	model, err := loadUploadProfileImageContext(ctx, command, retriever)
	if err != nil {
		return UploadProfileImageResult{}, err
	}
	url := storage.PublicURL(model.key)
	var event eventstore.DomainEvent
	if command.Header {
		event, err = NewProfileHeaderImageUploadedEvent(model.eventID, url, time.Now(), command.UserID, nil)
	} else {
		event, err = NewProfileImageUploadedEvent(model.eventID, url, time.Now(), command.UserID, nil)
	}
	if err != nil {
		return UploadProfileImageResult{}, err
	}
	if err := storage.PutObject(ctx, model.key, command.Data, command.ContentType); err != nil {
		return UploadProfileImageResult{}, err
	}

	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}}); err != nil {
		return UploadProfileImageResult{}, err
	}
	return UploadProfileImageResult{URL: url}, nil
}

type uploadProfileImageContext struct {
	userExists bool
	key        string
	eventID    string
	position   eventstore.Position
	events     []eventstore.ResolvedEvent
	query      eventstore.Query
}

func loadUploadProfileImageContext(ctx context.Context, command UploadProfileImageCommand, retriever eventstore.LatestRetriever) (*uploadProfileImageContext, error) {
	if !validProfileID(command.UserID) {
		return nil, eventstore.ErrInvalidEvent
	}
	if len(command.Data) == 0 {
		return nil, errors.New("missing image")
	}
	if len(command.Data) > 5*1024*1024 {
		return nil, errors.New("image must be 5MB or smaller")
	}
	ext := extension(command.ContentType)
	kind := "avatar"
	eventType := ProfileImageUploaded
	if command.Header {
		kind = "header"
		eventType = ProfileHeaderImageUploaded
	}
	userQuery := uploadProfileImageRegisteredUserQuery(command.UserID)
	imageQuery := uploadProfileImageProfileUserEventQuery(eventType, command.UserID)
	query := uploadProfileImageCombineQueries(userQuery, imageQuery)
	key := filepath.ToSlash(fmt.Sprintf("profiles/%s/%s-%s.%s", command.UserID, kind, uuidv7.NewString(), ext))
	eventID := uuidv7.NewString()
	model := &uploadProfileImageContext{
		key:      key,
		eventID:  eventID,
		position: eventstore.NoEventPosition,
		query:    query,
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
		model.handle(event)
	}
	if !model.userExists {
		return nil, eventstore.ErrNotFound
	}
	return model, nil
}

func (m *uploadProfileImageContext) handle(resolved eventstore.ResolvedEvent) {
	switch resolved.Event.EventType {
	case "UserRegistered":
		m.userExists = true
	}
}

func uploadProfileImageCombineQueries(queries ...eventstore.Query) eventstore.Query {
	combined := eventstore.Query{}
	for _, query := range queries {
		combined.Criteria = append(combined.Criteria, query.Criteria...)
	}
	return combined
}

func uploadProfileImageProfileUserEventQuery(eventType, userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: eventType},
		{Key: ProfileUserIDField, Value: userRegisteredID},
	}}}}
}

func uploadProfileImageRegisteredUserQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: auth.UserRegistered},
		{Key: auth.UserRegisteredIDField, Value: userRegisteredID},
	}}}}
}
