package auth

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

type ValidateEmailVerificationOTPCommand struct {
	User     views.User
	Code     string
	Metadata CommandMetadata
}

func ValidateEmailVerificationOTPCommandHandler(ctx context.Context, command ValidateEmailVerificationOTPCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadValidateEmailVerificationOTPContext(ctx, command, retriever)
	if err != nil {
		return err
	}
	if !model.userExists {
		return errors.New("registered user event not found")
	}
	if model.otpID == "" {
		return errors.New("no verification code found")
	}
	if model.code != strings.TrimSpace(command.Code) || !model.expiresAt.After(time.Now()) {
		return errors.New("invalid or expired verification code")
	}
	if model.alreadyValidated {
		return errors.New("verification code already validated")
	}

	validationID := uuidv7.NewString()
	event, err := NewEmailVerificationOTPValidatedEvent(validationID, time.Now(), model.otpID, model.userRegisteredID, nil)
	if err != nil {
		return err
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, model.consistency)
	return err
}

type validateEmailVerificationOTPContext struct {
	userRegisteredID string
	userExists       bool
	otpID            string
	code             string
	expiresAt        time.Time
	alreadyValidated bool
	consistency      []eventstore.ConsistencyObservation
}

type latestOTP struct {
	id        string
	code      string
	expiresAt time.Time
	position  eventstore.Position
}

func latestEmailVerificationOTP(events []eventstore.ResolvedEvent, userRegisteredID string) (latestOTP, error) {
	var otp latestOTP
	for _, resolved := range events {
		if resolved.Event.EventType != EmailVerificationOTPGenerated {
			continue
		}
		generated, err := DecodeEmailVerificationOTPGeneratedEvent(resolved.Event)
		if err != nil {
			return latestOTP{}, err
		}
		if generated.Scopes.UserRegisteredID != userRegisteredID {
			return latestOTP{}, errors.New("email verification generation scope does not match context")
		}
		expiresAt, err := time.Parse(time.RFC3339Nano, generated.ExpiresAt)
		if err != nil {
			return latestOTP{}, err
		}
		if otp.id == "" || resolved.Position.After(otp.position) {
			otp = latestOTP{id: generated.EmailVerificationOTPGeneratedID, code: generated.OTPCode, expiresAt: expiresAt, position: resolved.Position}
		}
	}
	return otp, nil
}

func loadValidateEmailVerificationOTPContext(ctx context.Context, command ValidateEmailVerificationOTPCommand, retriever eventstore.LatestRetriever) (*validateEmailVerificationOTPContext, error) {
	generatedQuery := validateEmailVerificationOTPEmailVerificationOTPGeneratedByUserQuery(command.User.UserRegisteredID)
	userQuery := validateEmailVerificationOTPUserRegisteredQuery(command.User.UserRegisteredID)

	query := validateEmailVerificationOTPCombineQueries(generatedQuery, userQuery)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	events := eventstore.EventsFromLatest(latest.Results)
	otp, err := latestEmailVerificationOTP(events, command.User.UserRegisteredID)
	if err != nil {
		return nil, err
	}
	model := &validateEmailVerificationOTPContext{
		userRegisteredID: command.User.UserRegisteredID, otpID: otp.id, code: otp.code, expiresAt: otp.expiresAt,
		consistency: []eventstore.ConsistencyObservation{{Query: query, Position: latest.ContextPosition}},
	}
	for _, resolved := range events {
		if err := model.handle(resolved); err != nil {
			return nil, err
		}
	}
	// Only the validation criterion depends on the first response. Preserve both
	// observations; a concurrent replacement OTP invalidates the first guard.
	if otp.id != "" {
		validationQuery := validateEmailVerificationOTPEmailVerificationOTPValidatedQuery(otp.id)
		validation, err := retriever.GetLatestByCriteria(ctx, validationQuery.Criteria)
		if err != nil {
			return nil, err
		}
		model.consistency = append(model.consistency, eventstore.ConsistencyObservation{Query: validationQuery, Position: validation.ContextPosition})
		for _, resolved := range eventstore.EventsFromLatest(validation.Results) {
			if resolved.Event.EventType != EmailVerificationOTPValidated {
				return nil, eventstore.ErrInvalidEvent
			}
			if err := model.handle(resolved); err != nil {
				return nil, err
			}
		}
	}

	return model, nil
}

func (m *validateEmailVerificationOTPContext) handle(resolved eventstore.ResolvedEvent) error {
	switch resolved.Event.EventType {
	case UserRegistered:
		registered, err := DecodeUserRegisteredEvent(resolved.Event)
		if err != nil {
			return err
		}
		if registered.UserRegisteredID != m.userRegisteredID {
			return errors.New("registered user does not match verification context")
		}
		m.userExists = true
	case EmailVerificationOTPGenerated:
		generated, err := DecodeEmailVerificationOTPGeneratedEvent(resolved.Event)
		if err != nil {
			return err
		}
		if generated.EmailVerificationOTPGeneratedID != m.otpID || generated.Scopes.UserRegisteredID != m.userRegisteredID {
			return errors.New("email verification generation scope does not match context")
		}
	case EmailVerificationOTPValidated:
		validated, err := DecodeEmailVerificationOTPValidatedEvent(resolved.Event)
		if err != nil {
			return err
		}
		if validated.Scopes.UserRegisteredID != m.userRegisteredID || validated.Scopes.EmailVerificationOTPGeneratedID != m.otpID {
			return errors.New("email verification validation scope does not match context")
		}
		m.alreadyValidated = true
	default:
		return eventstore.ErrInvalidEvent
	}
	return nil
}

func validateEmailVerificationOTPCombineQueries(queries ...eventstore.Query) eventstore.Query {
	combined := eventstore.Query{}
	for _, query := range queries {
		combined.Criteria = append(combined.Criteria, query.Criteria...)
	}
	return combined
}

func validateEmailVerificationOTPEmailVerificationOTPGeneratedByUserQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: EmailVerificationOTPGenerated},
		{Key: ScopeUserRegisteredIDField, Value: userRegisteredID},
	}}}}
}

func validateEmailVerificationOTPEmailVerificationOTPValidatedQuery(otpID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: EmailVerificationOTPValidated},
		{Key: ScopeEmailVerificationOTPGeneratedIDField, Value: otpID},
	}}}}
}

func validateEmailVerificationOTPUserRegisteredQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: UserRegistered},
		{Key: UserRegisteredIDField, Value: userRegisteredID},
	}}}}
}
