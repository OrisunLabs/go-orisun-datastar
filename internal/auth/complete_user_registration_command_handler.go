package auth

import (
	"context"
	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"time"
)

type CompleteUserRegistrationCommand struct {
	UserRegisteredID string
	Metadata         CommandMetadata
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
	userID := command.UserRegisteredID
	if !validAuthID(userID) {
		return nil, eventstore.ErrInvalidEvent
	}
	sourceQuery := completeUserRegistrationSourceQuery(userID)
	sources, err := retriever.GetLatestByCriteria(ctx, sourceQuery.Criteria)
	if err != nil {
		return nil, err
	}
	var id string
	var acceptedFact *eventstore.ResolvedEvent
	for _, e := range eventstore.EventsFromLatest(sources.Results) {
		switch e.Event.EventType {
		case UserRegistrationStarted:
			v, err := DecodeUserRegistrationStartedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.UserID != userID {
				return nil, eventstore.ErrInvalidEvent
			}
			id = v.UserRegistrationStartedID
		case UserRegistered:
			v, err := DecodeUserRegisteredEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.UserRegisteredID != userID {
				return nil, eventstore.ErrInvalidEvent
			}
			copy := e
			acceptedFact = &copy
		default:
			return nil, eventstore.ErrInvalidEvent
		}
	}
	if acceptedFact == nil {
		return nil, eventstore.ErrNotFound
	}

	if id == "" {
		return &completeUserRegistrationContext{completed: true}, nil
	}
	if !validAuthID(id) {
		return nil, eventstore.ErrInvalidEvent
	}
	query := completeUserRegistrationWorkflowQuery(id)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model := &completeUserRegistrationContext{consistency: []eventstore.ConsistencyObservation{{Query: sourceQuery, Position: sources.ContextPosition}, {Query: query, Position: latest.ContextPosition}}}
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
	if model.started.UserRegistrationStartedID != id || model.started.UserID != userID {
		return nil, eventstore.ErrInvalidEvent
	}
	if model.validated.EmailVerificationOTPValidatedID == "" {
		model.completed = true
		return model, nil
	}
	if model.validated.Scopes.UserRegistrationStartedID != id {
		return nil, eventstore.ErrInvalidEvent
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
	accepted, err := DecodeUserRegisteredEvent(acceptedFact.Event)
	if err != nil {
		return nil, err
	}
	if !acceptedFact.Position.After(model.validatedPosition) || !registrationAcceptanceMatches(model.started, model.validated, accepted) {
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
func registrationAcceptanceMatches(start UserRegistrationStartedEvent, validated EmailVerificationOTPValidatedEvent, accepted UserRegisteredEvent) bool {
	return accepted.UserRegisteredID == start.UserID && accepted.UsernameHash == start.UsernameHash && accepted.EmailHash == start.EmailHash && accepted.FirstName.BlindIndex == start.FirstName.BlindIndex && accepted.LastName.BlindIndex == start.LastName.BlindIndex && accepted.YearOfBirth == start.YearOfBirth && accepted.PasswordHash == start.PasswordHash && accepted.EmailVerifiedAt == validated.ValidatedAt
}

func completeUserRegistrationSourceQuery(userID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationStarted}, {Key: "userId", Value: userID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}, {Key: UserRegisteredIDField, Value: userID}}},
	}}
}
