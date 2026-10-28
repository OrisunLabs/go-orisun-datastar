package auth

import (
	"context"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

type RegistrationStage string

const (
	RegistrationVerifying   RegistrationStage = "verifying"
	RegistrationProcessing  RegistrationStage = "processing"
	RegistrationComplete    RegistrationStage = "complete"
	RegistrationFailed      RegistrationStage = "failed"
	RegistrationUnavailable RegistrationStage = "unavailable"
)

type RegistrationStatus struct {
	UserID string
	Stage  RegistrationStage
}

// ReadRegistrationStatus is read-side workflow evidence, never command authority.
func ReadRegistrationStatus(ctx context.Context, id string, reader eventstore.LatestRetriever) (RegistrationStatus, error) {
	empty := RegistrationStatus{}
	if !validAuthID(id) {
		return empty, eventstore.ErrInvalidEvent
	}
	query := registrationStatusQuery(id)
	latest, err := reader.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return empty, err
	}
	var started *UserRegistrationStartedEvent
	var validation *EmailVerificationOTPValidatedEvent
	var completed *UserRegistrationCompletedEvent
	var failed *UserRegistrationFailedEvent
	var startedPosition, validatedPosition, completedPosition, failedPosition eventstore.Position
	for _, e := range eventstore.EventsFromLatest(latest.Results) {
		switch e.Event.EventType {
		case UserRegistrationStarted:
			v, err := DecodeUserRegistrationStartedEvent(e.Event)
			if err != nil {
				return empty, err
			}
			if v.UserRegistrationStartedID != id {
				return empty, eventstore.ErrInvalidEvent
			}
			started = &v
			startedPosition = e.Position
		case EmailVerificationOTPValidated:
			v, err := DecodeEmailVerificationOTPValidatedEvent(e.Event)
			if err != nil {
				return empty, err
			}
			if v.Scopes.UserRegistrationStartedID != id {
				return empty, eventstore.ErrInvalidEvent
			}
			validation = &v
			validatedPosition = e.Position
		case UserRegistrationCompleted:
			v, err := DecodeUserRegistrationCompletedEvent(e.Event)
			if err != nil {
				return empty, err
			}
			if v.Scopes.UserRegistrationStartedID != id {
				return empty, eventstore.ErrInvalidEvent
			}
			completed = &v
			completedPosition = e.Position
		case UserRegistrationFailed:
			v, err := DecodeUserRegistrationFailedEvent(e.Event)
			if err != nil {
				return empty, err
			}
			if v.Scopes.UserRegistrationStartedID != id {
				return empty, eventstore.ErrInvalidEvent
			}
			failed = &v
			failedPosition = e.Position
		default:
			return empty, eventstore.ErrInvalidEvent
		}
	}
	if started == nil {
		return empty, eventstore.ErrNotFound
	}
	if validation != nil && !validatedPosition.After(startedPosition) {
		return empty, eventstore.ErrInvalidEvent
	}
	if completed != nil && (validation == nil || failed != nil || !completedPosition.After(validatedPosition) || completed.Scopes.UserRegisteredID != started.UserID || completed.Scopes.EmailVerificationOTPValidatedID != validation.EmailVerificationOTPValidatedID) {
		return empty, eventstore.ErrInvalidEvent
	}
	if failed != nil && (validation == nil || !failedPosition.After(validatedPosition)) {
		return empty, eventstore.ErrInvalidEvent
	}
	deletionQuery := registrationStatusDeletionQuery(started.UserID)
	deletion, err := reader.GetLatestByCriteria(ctx, deletionQuery.Criteria)
	if err != nil {
		return empty, err
	}
	for _, e := range eventstore.EventsFromLatest(deletion.Results) {
		v, err := DecodeAccountDeletedEvent(e.Event)
		if err != nil {
			return empty, err
		}
		if v.Scopes.UserRegisteredID != started.UserID || !e.Position.After(startedPosition) {
			return empty, eventstore.ErrInvalidEvent
		}
		return RegistrationStatus{UserID: started.UserID, Stage: RegistrationUnavailable}, nil
	}
	stage := RegistrationVerifying
	if validation != nil {
		stage = RegistrationProcessing
	}
	if failed != nil {
		stage = RegistrationFailed
	}
	if completed != nil {
		stage = RegistrationComplete
	}
	return RegistrationStatus{UserID: started.UserID, Stage: stage}, nil
}
func registrationStatusQuery(id string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationStarted}, {Key: UserRegistrationStartedIDField, Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPValidated}, {Key: ScopeUserRegistrationStartedIDField, Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationCompleted}, {Key: ScopeUserRegistrationStartedIDField, Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationFailed}, {Key: ScopeUserRegistrationStartedIDField, Value: id}}},
	}}
}
func registrationStatusDeletionQuery(userID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeleted}, {Key: ScopeUserRegisteredIDField, Value: userID}}}}}
}
