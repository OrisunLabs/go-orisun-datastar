package auth

import (
	"context"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

type RecordLoginAttemptCommand struct {
	AttemptedIdentifier string
	IPAddress           string
	UserRegisteredID    string
	Succeeded           bool
	Metadata            CommandMetadata
}

func RecordLoginAttemptCommandHandler(ctx context.Context, command RecordLoginAttemptCommand, saver eventstore.SaverV2) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := newRecordLoginAttemptContext(command)
	if err != nil {
		return err
	}

	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{model.event}, model.consistency)
	return err
}

func loginAttemptRecordedQuery(eventID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: LoginAttemptRecorded},
		{Key: LoginAttemptRecordedIDField, Value: eventID},
	}}}}
}

type recordLoginAttemptContext struct {
	event       eventstore.DomainEvent
	consistency []eventstore.ConsistencyObservation
}

func newRecordLoginAttemptContext(command RecordLoginAttemptCommand) (*recordLoginAttemptContext, error) {
	eventID := uuidv7.NewString()
	event, err := NewLoginAttemptRecordedEvent(eventID, time.Now(), command.AttemptedIdentifier, command.IPAddress, command.UserRegisteredID, command.Succeeded, nil)
	if err != nil {
		return nil, err
	}
	return &recordLoginAttemptContext{event: event, consistency: []eventstore.ConsistencyObservation{{Query: loginAttemptRecordedQuery(eventID), Position: eventstore.NoEventPosition}}}, nil
}
