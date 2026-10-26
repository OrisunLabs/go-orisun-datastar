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
	event, err := NewEmailVerificationOTPValidatedEvent(validationID, time.Now(), model.otpID, command.User.UserRegisteredID, nil)
	if err != nil {
		return err
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, model.consistency)
	return err
}

type validateEmailVerificationOTPContext struct {
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

func latestEmailVerificationOTP(events []eventstore.ResolvedEvent) latestOTP {
	otp := latestOTP{position: eventstore.NoEventPosition}
	for _, resolved := range events {
		if !resolved.Position.After(otp.position) {
			continue
		}
		expiresAt, _ := resolved.Event.Data[EmailVerificationOTPExpiresAtField].(string)
		parsed, err := time.Parse(time.RFC3339, expiresAt)
		if err != nil {
			continue
		}
		otp.id, _ = resolved.Event.Data[EmailVerificationOTPGeneratedIDField].(string)
		otp.code, _ = resolved.Event.Data[EmailVerificationOTPCodeField].(string)
		otp.expiresAt = parsed
		otp.position = resolved.Position
	}
	return otp
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
	for _, checked := range events {
		if err := ValidateEvent(checked.Event); err != nil {
			return nil, err
		}
	}
	otp := latestEmailVerificationOTP(events)
	model := &validateEmailVerificationOTPContext{
		otpID: otp.id, code: otp.code, expiresAt: otp.expiresAt,
		consistency: []eventstore.ConsistencyObservation{{Query: query, Position: latest.ContextPosition}},
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
		for _, checked := range eventstore.EventsFromLatest(validation.Results) {
			if err := ValidateEvent(checked.Event); err != nil {
				return nil, err
			}
			events = append(events, checked)
		}
	}

	for _, event := range events {
		model.handle(event)
	}
	return model, nil
}

func (m *validateEmailVerificationOTPContext) handle(resolved eventstore.ResolvedEvent) {
	switch resolved.Event.EventType {
	case UserRegistered:
		m.userExists = true
	case EmailVerificationOTPValidated:
		m.alreadyValidated = true
	}
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
