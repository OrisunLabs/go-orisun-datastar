package auth

import (
	"context"
	"errors"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"

	"github.com/OrisunLabs/go-orisun-datastar/internal/email"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

type SendEmailValidationOTPCommand struct {
	UserRegistrationStartedID       string
	EmailVerificationOTPGeneratedID string
	Metadata                        CommandMetadata
}

type sendEmailValidationOTPContext struct {
	otpID, code, email string
	alreadySent        bool
	position           eventstore.Position
	query              eventstore.Query
}

func SendEmailValidationOTPCommandHandler(ctx context.Context, command SendEmailValidationOTPCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, sender EmailSender, keys SubjectPiiKeyReader) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadSendEmailValidationOTPContext(ctx, command, retriever, keys)
	if err != nil {
		return err
	}
	if model.alreadySent {
		return nil
	}
	if model.otpID == "" {
		return errors.New("email verification OTP generation event not found")
	}
	if model.email == "" {
		return errors.New("registration start event not found")
	}

	if err := sender.Send(ctx, email.Message{
		To:    model.email,
		Title: "Verify your email",
		Body:  "Your verification code is <strong>" + model.code + "</strong>.",
	}); err != nil {
		return err
	}

	id := uuidv7.NewString()
	sent, err := NewEmailVerificationOTPSentEvent(id, time.Now(), command.EmailVerificationOTPGeneratedID, nil)
	if err != nil {
		return err
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{sent}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}})
	return err
}

func sendEmailValidationOTPCombineQueries(queries ...eventstore.Query) eventstore.Query {
	combined := eventstore.Query{}
	for _, query := range queries {
		combined.Criteria = append(combined.Criteria, query.Criteria...)
	}
	return combined
}

func sendEmailValidationOTPEmailVerificationOTPGeneratedQuery(otpID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: EmailVerificationOTPGenerated},
		{Key: EmailVerificationOTPGeneratedIDField, Value: otpID},
	}}}}
}

func sendEmailValidationOTPEmailVerificationOTPSentQuery(otpID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: EmailVerificationOTPSent},
		{Key: ScopeEmailVerificationOTPGeneratedIDField, Value: otpID},
	}}}}
}

func sendEmailValidationOTPUserRegistrationStartedQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: UserRegistrationStarted},
		{Key: UserRegistrationStartedIDField, Value: userRegisteredID},
	}}}}
}

func loadSendEmailValidationOTPContext(ctx context.Context, command SendEmailValidationOTPCommand, retriever eventstore.LatestRetriever, keys SubjectPiiKeyReader) (*sendEmailValidationOTPContext, error) {
	generatedQuery := sendEmailValidationOTPEmailVerificationOTPGeneratedQuery(command.EmailVerificationOTPGeneratedID)
	userQuery := sendEmailValidationOTPUserRegistrationStartedQuery(command.UserRegistrationStartedID)
	sentQuery := sendEmailValidationOTPEmailVerificationOTPSentQuery(command.EmailVerificationOTPGeneratedID)
	query := sendEmailValidationOTPCombineQueries(generatedQuery, userQuery, sentQuery)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}

	model := &sendEmailValidationOTPContext{position: latest.ContextPosition, query: query}
	var registered *UserRegistrationStartedEvent
	for _, resolved := range eventstore.EventsFromLatest(latest.Results) {
		switch resolved.Event.EventType {
		case UserRegistrationStarted:
			value, err := DecodeUserRegistrationStartedEvent(resolved.Event)
			if err != nil {
				return nil, err
			}
			if value.UserRegistrationStartedID != command.UserRegistrationStartedID {
				return nil, errors.New("registration start does not match OTP email context")
			}
			registered = &value
		case EmailVerificationOTPGenerated:
			value, err := DecodeEmailVerificationOTPGeneratedEvent(resolved.Event)
			if err != nil {
				return nil, err
			}
			if value.EmailVerificationOTPGeneratedID != command.EmailVerificationOTPGeneratedID || value.Scopes.UserRegistrationStartedID != command.UserRegistrationStartedID {
				return nil, errors.New("email verification OTP does not belong to registration")
			}
			model.otpID = value.EmailVerificationOTPGeneratedID
			model.code = value.OTPCode
		case EmailVerificationOTPSent:
			value, err := DecodeEmailVerificationOTPSentEvent(resolved.Event)
			if err != nil {
				return nil, err
			}
			if value.Scopes.EmailVerificationOTPGeneratedID != command.EmailVerificationOTPGeneratedID {
				return nil, errors.New("email verification sent scope does not match generation context")
			}
			model.alreadySent = true
		default:
			return nil, eventstore.ErrInvalidEvent
		}
	}
	if model.alreadySent {
		if model.otpID == "" {
			return nil, errors.New("email verification OTP generation event not found")
		}
		return model, nil
	}
	if model.otpID == "" || registered == nil {
		return model, nil
	}
	key, ok, err := keys.GetSubjectDataKey(ctx, registered.UserID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, eventstore.ErrNotFound
	}
	model.email, err = protectedpii.FromEnv().DecryptWithDataKey(registered.Email, key)
	if err != nil {
		return nil, err
	}
	return model, nil
}
