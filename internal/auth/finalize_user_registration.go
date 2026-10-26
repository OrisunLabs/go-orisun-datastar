package auth

import (
	"context"
	"errors"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

// FinalizeUserRegistration translates frozen workflow input into an ordinary
// domain command. It never saves events itself.
func FinalizeUserRegistration(ctx context.Context, id string, metadata CommandMetadata, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, keys SubjectPiiKeyPort) error {
	start, validated, completed, failed, err := loadRegistrationFinalization(ctx, id, retriever)
	if err != nil {
		return err
	}
	if completed {
		return nil
	}
	if failed {
		return ErrRegistrationIdentityUnavailable
	}
	key, ok, err := keys.GetSubjectDataKey(ctx, start.UserID)
	if err != nil {
		return err
	}
	if !ok {
		return eventstore.ErrNotFound
	}
	protector := protectedpii.FromEnv()
	plain := make([]string, 4)
	values := []protectedpii.Value{start.Username, start.Email, start.FirstName, start.LastName}
	fields := []string{UserRegisteredUsernameField, UserRegisteredEmailField, UserRegisteredFirstNameField, UserRegisteredLastNameField}
	for i, value := range values {
		plain[i], err = protector.DecryptWithDataKey(value, key)
		if err != nil {
			return err
		}
		if protector.BlindIndex(fields[i], plain[i]) != value.BlindIndex {
			return eventstore.ErrInvalidEvent
		}
	}
	verifiedAt, _ := time.Parse(time.RFC3339Nano, validated.ValidatedAt)
	_, err = RegisterUserCommandHandler(ctx, RegisterUserCommand{UserRegisteredID: start.UserID, Username: plain[0], Email: plain[1], FirstName: plain[2], LastName: plain[3], YearOfBirth: start.YearOfBirth, PasswordHash: start.PasswordHash, EmailVerifiedAt: verifiedAt, Metadata: metadata}, saver, retriever, keys)
	if errors.Is(err, ErrRegistrationIdentityUnavailable) || errors.Is(err, eventstore.ErrConflict) {
		if failure := FailUserRegistrationCommandHandler(ctx, FailUserRegistrationCommand{UserRegistrationStartedID: id, Metadata: metadata}, saver, retriever); failure != nil {
			if errors.Is(failure, eventstore.ErrConflict) {
				return err
			}
			return failure
		}
		return ErrRegistrationIdentityUnavailable
	}
	if err != nil {
		return err
	}
	err = CompleteUserRegistrationCommandHandler(ctx, CompleteUserRegistrationCommand{UserRegistrationStartedID: id, Metadata: metadata}, saver, retriever)
	if errors.Is(err, eventstore.ErrConflict) {
		if failure := FailUserRegistrationCommandHandler(ctx, FailUserRegistrationCommand{UserRegistrationStartedID: id, Metadata: metadata}, saver, retriever); failure == nil {
			return ErrRegistrationIdentityUnavailable
		} else if !errors.Is(failure, eventstore.ErrConflict) {
			return failure
		}
	}
	return err
}
func loadRegistrationFinalization(ctx context.Context, id string, retriever eventstore.LatestRetriever) (UserRegistrationStartedEvent, EmailVerificationOTPValidatedEvent, bool, bool, error) {
	var start UserRegistrationStartedEvent
	var validated EmailVerificationOTPValidatedEvent
	var receipt *UserRegistrationCompletedEvent
	var startedPosition, validatedPosition, completedPosition eventstore.Position
	failed := false
	query := registrationFinalizationQuery(id)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return start, validated, false, false, err
	}
	for _, e := range eventstore.EventsFromLatest(latest.Results) {
		switch e.Event.EventType {
		case UserRegistrationStarted:
			v, err := DecodeUserRegistrationStartedEvent(e.Event)
			if err != nil {
				return start, validated, false, false, err
			}
			start = v
			startedPosition = e.Position
		case EmailVerificationOTPValidated:
			v, err := DecodeEmailVerificationOTPValidatedEvent(e.Event)
			if err != nil {
				return start, validated, false, false, err
			}
			validated = v
			validatedPosition = e.Position
		case UserRegistrationCompleted:
			v, err := DecodeUserRegistrationCompletedEvent(e.Event)
			if err != nil {
				return start, validated, false, false, err
			}
			receipt = &v
			completedPosition = e.Position
		case UserRegistrationFailed:
			v, err := DecodeUserRegistrationFailedEvent(e.Event)
			if err != nil {
				return start, validated, false, false, err
			}
			if v.Scopes.UserRegistrationStartedID != id {
				return start, validated, false, false, eventstore.ErrInvalidEvent
			}
			failed = true
		default:
			return start, validated, false, false, eventstore.ErrInvalidEvent
		}
	}
	if start.UserRegistrationStartedID != id || validated.Scopes.UserRegistrationStartedID != id {
		return start, validated, false, false, eventstore.ErrNotFound
	}
	if !validatedPosition.After(startedPosition) {
		return start, validated, false, false, eventstore.ErrInvalidEvent
	}
	if receipt != nil && (!completedPosition.After(validatedPosition) || failed || receipt.Scopes.UserRegistrationStartedID != id || receipt.Scopes.UserRegisteredID != start.UserID || receipt.Scopes.EmailVerificationOTPValidatedID != validated.EmailVerificationOTPValidatedID) {
		return start, validated, false, false, eventstore.ErrInvalidEvent
	}
	return start, validated, receipt != nil, failed, nil
}
func registrationFinalizationQuery(id string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationStarted}, {Key: UserRegistrationStartedIDField, Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPValidated}, {Key: ScopeUserRegistrationStartedIDField, Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationCompleted}, {Key: ScopeUserRegistrationStartedIDField, Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationFailed}, {Key: ScopeUserRegistrationStartedIDField, Value: id}}},
	}}
}
