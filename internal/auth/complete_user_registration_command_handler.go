package auth

import (
	"context"
	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"time"
)

type CompleteUserRegistrationCommand struct {
	UserRegistrationStartedID string
	Metadata                  CommandMetadata
}

func CompleteUserRegistrationCommandHandler(ctx context.Context, command CompleteUserRegistrationCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadCompleteUserRegistrationContext(ctx, command, retriever)
	if err != nil {
		return err
	}
	if model.completed {
		return nil
	}
	event, err := NewUserRegistrationCompletedEvent(uuidv7.NewString(), model.started.UserRegistrationStartedID, model.validated.EmailVerificationOTPValidatedID, model.started.UserID, time.Now(), nil)
	if err != nil {
		return err
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, model.consistency)
	return err
}

type completeUserRegistrationContext struct {
	startedPosition, validatedPosition eventstore.Position
	started                            UserRegistrationStartedEvent
	validated                          EmailVerificationOTPValidatedEvent
	completed                          bool
	consistency                        []eventstore.ConsistencyObservation
}

func loadCompleteUserRegistrationContext(ctx context.Context, command CompleteUserRegistrationCommand, retriever eventstore.LatestRetriever) (*completeUserRegistrationContext, error) {
	id := command.UserRegistrationStartedID
	if !validAuthID(id) {
		return nil, eventstore.ErrInvalidEvent
	}
	query := completeUserRegistrationWorkflowQuery(id)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model := &completeUserRegistrationContext{consistency: []eventstore.ConsistencyObservation{{Query: query, Position: latest.ContextPosition}}}
	var receipt *UserRegistrationCompletedEvent
	var completedPosition eventstore.Position
	failed := false
	for _, e := range eventstore.EventsFromLatest(latest.Results) {
		switch e.Event.EventType {
		case UserRegistrationStarted:
			v, err := DecodeUserRegistrationStartedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			model.started = v
			model.startedPosition = e.Position
		case EmailVerificationOTPValidated:
			v, err := DecodeEmailVerificationOTPValidatedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			model.validated = v
			model.validatedPosition = e.Position
		case UserRegistrationCompleted:
			v, err := DecodeUserRegistrationCompletedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			receipt = &v
			completedPosition = e.Position
		case UserRegistrationFailed:
			if _, err := DecodeUserRegistrationFailedEvent(e.Event); err != nil {
				return nil, err
			}
			failed = true
		default:
			return nil, eventstore.ErrInvalidEvent
		}
	}
	if model.started.UserRegistrationStartedID != id || model.validated.Scopes.UserRegistrationStartedID != id {
		return nil, eventstore.ErrNotFound
	}
	if !model.validatedPosition.After(model.startedPosition) {
		return nil, eventstore.ErrInvalidEvent
	}
	if failed {
		if receipt != nil {
			return nil, eventstore.ErrInvalidEvent
		}
		return nil, ErrRegistrationIdentityUnavailable
	}
	if receipt != nil {
		if !completedPosition.After(model.validatedPosition) || receipt.Scopes.UserRegistrationStartedID != id || receipt.Scopes.UserRegisteredID != model.started.UserID || receipt.Scopes.EmailVerificationOTPValidatedID != model.validated.EmailVerificationOTPValidatedID {
			return nil, eventstore.ErrInvalidEvent
		}
		model.completed = true
		return model, nil
	}
	effective := completeUserRegistrationEffectiveQuery(model.started.UserID)
	loaded, err := retriever.GetLatestByCriteria(ctx, effective.Criteria)
	if err != nil {
		return nil, err
	}
	model.consistency = append(model.consistency, eventstore.ConsistencyObservation{Query: effective, Position: loaded.ContextPosition})
	facts := eventstore.EventsFromLatest(loaded.Results)
	if len(facts) != 1 {
		return nil, eventstore.ErrNotFound
	}
	accepted, err := DecodeUserRegisteredEvent(facts[0].Event)
	if err != nil {
		return nil, err
	}
	if !facts[0].Position.After(model.validatedPosition) || !registrationAcceptanceMatches(model.started, model.validated, accepted) {
		return nil, eventstore.ErrConflict
	}
	return model, nil
}
func completeUserRegistrationWorkflowQuery(id string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationStarted}, {Key: UserRegistrationStartedIDField, Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPValidated}, {Key: ScopeUserRegistrationStartedIDField, Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationCompleted}, {Key: ScopeUserRegistrationStartedIDField, Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationFailed}, {Key: ScopeUserRegistrationStartedIDField, Value: id}}},
	}}
}
func completeUserRegistrationEffectiveQuery(id string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}, {Key: UserRegisteredIDField, Value: id}}}}}
}
func registrationAcceptanceMatches(start UserRegistrationStartedEvent, validated EmailVerificationOTPValidatedEvent, accepted UserRegisteredEvent) bool {
	return accepted.UserRegisteredID == start.UserID && accepted.UsernameHash == start.UsernameHash && accepted.EmailHash == start.EmailHash && accepted.FirstName.BlindIndex == start.FirstName.BlindIndex && accepted.LastName.BlindIndex == start.LastName.BlindIndex && accepted.YearOfBirth == start.YearOfBirth && accepted.PasswordHash == start.PasswordHash && accepted.EmailVerifiedAt == validated.ValidatedAt
}
