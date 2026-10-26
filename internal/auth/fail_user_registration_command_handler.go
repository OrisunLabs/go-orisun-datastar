package auth

import (
	"context"
	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"time"
)

type FailUserRegistrationCommand struct {
	UserRegistrationStartedID string
	Metadata                  CommandMetadata
}

func FailUserRegistrationCommandHandler(ctx context.Context, command FailUserRegistrationCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadFailUserRegistrationContext(ctx, command, retriever)
	if err != nil {
		return err
	}
	if model.failed {
		return nil
	}
	event, err := NewUserRegistrationFailedEvent(uuidv7.NewString(), command.UserRegistrationStartedID, time.Now(), nil)
	if err != nil {
		return err
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, model.consistency)
	return err
}

type failUserRegistrationContext struct {
	failed      bool
	consistency []eventstore.ConsistencyObservation
}

func loadFailUserRegistrationContext(ctx context.Context, command FailUserRegistrationCommand, retriever eventstore.LatestRetriever) (*failUserRegistrationContext, error) {
	id := command.UserRegistrationStartedID
	if !validAuthID(id) {
		return nil, eventstore.ErrInvalidEvent
	}
	query := failUserRegistrationWorkflowQuery(id)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model := &failUserRegistrationContext{consistency: []eventstore.ConsistencyObservation{{Query: query, Position: latest.ContextPosition}}}
	var start UserRegistrationStartedEvent
	var validation EmailVerificationOTPValidatedEvent
	var startedPosition, validatedPosition eventstore.Position
	validated := false
	completed := false
	for _, e := range eventstore.EventsFromLatest(latest.Results) {
		switch e.Event.EventType {
		case UserRegistrationStarted:
			v, err := DecodeUserRegistrationStartedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			start = v
			startedPosition = e.Position
		case EmailVerificationOTPValidated:
			v, err := DecodeEmailVerificationOTPValidatedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.Scopes.UserRegistrationStartedID != id {
				return nil, eventstore.ErrInvalidEvent
			}
			validation = v
			validatedPosition = e.Position
			validated = true
		case UserRegistrationFailed:
			v, err := DecodeUserRegistrationFailedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.Scopes.UserRegistrationStartedID != id {
				return nil, eventstore.ErrInvalidEvent
			}
			model.failed = true
		case UserRegistrationCompleted:
			if _, err := DecodeUserRegistrationCompletedEvent(e.Event); err != nil {
				return nil, err
			}
			completed = true
		default:
			return nil, eventstore.ErrInvalidEvent
		}
	}
	if start.UserRegistrationStartedID != id || !validated {
		return nil, eventstore.ErrNotFound
	}
	if !validatedPosition.After(startedPosition) {
		return nil, eventstore.ErrInvalidEvent
	}
	if completed {
		return nil, eventstore.ErrConflict
	}
	if model.failed {
		return model, nil
	}
	effective := failUserRegistrationEffectiveQuery(start)
	loaded, err := retriever.GetLatestByCriteria(ctx, effective.Criteria)
	if err != nil {
		return nil, err
	}
	model.consistency = append(model.consistency, eventstore.ConsistencyObservation{Query: effective, Position: loaded.ContextPosition})
	competing := false
	for _, e := range eventstore.EventsFromLatest(loaded.Results) {
		v, err := DecodeUserRegisteredEvent(e.Event)
		if err != nil {
			return nil, err
		}
		if v.UserRegisteredID == start.UserID {
			if e.Position.After(validatedPosition) && registrationAcceptanceMatches(start, validation, v) {
				return nil, eventstore.ErrConflict
			}
			competing = true
		}
		if v.UsernameHash == start.UsernameHash || v.EmailHash == start.EmailHash {
			competing = true
		}
	}
	if !competing {
		return nil, eventstore.ErrConflict
	}
	return model, nil
}
func failUserRegistrationWorkflowQuery(id string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationStarted}, {Key: UserRegistrationStartedIDField, Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPValidated}, {Key: ScopeUserRegistrationStartedIDField, Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationCompleted}, {Key: ScopeUserRegistrationStartedIDField, Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationFailed}, {Key: ScopeUserRegistrationStartedIDField, Value: id}}},
	}}
}
func failUserRegistrationEffectiveQuery(start UserRegistrationStartedEvent) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}, {Key: UserRegisteredIDField, Value: start.UserID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}, {Key: UserRegisteredUsernameHashField, Value: start.UsernameHash}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}, {Key: UserRegisteredEmailHashField, Value: start.EmailHash}}},
	}}
}
