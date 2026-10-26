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
	UserRegisteredID                string
	EmailVerificationOTPGeneratedID string
	Metadata                        CommandMetadata
}

type sendEmailValidationOTPContext struct {
	otpID       string
	code        string
	expiresAt   string
	email       string
	subjectKey  protectedpii.SubjectDataKey
	alreadySent bool
	position    eventstore.Position
	events      []eventstore.ResolvedEvent
	query       eventstore.Query
}

func SendEmailValidationOTPCommandHandler(ctx context.Context, command SendEmailValidationOTPCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, sender EmailSender, keys SubjectPiiKeyPort) error {
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
		return errors.New("registered user event not found")
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

func (m *sendEmailValidationOTPContext) handle(resolved eventstore.ResolvedEvent) error {
	switch resolved.Event.EventType {
	case EmailVerificationOTPGenerated:
		m.otpID, _ = resolved.Event.Data[EmailVerificationOTPGeneratedIDField].(string)
		m.code, _ = resolved.Event.Data[EmailVerificationOTPCodeField].(string)
		m.expiresAt, _ = resolved.Event.Data[EmailVerificationOTPExpiresAtField].(string)
	case UserRegistered:
		decryptedemail, err := protectedpii.DecryptEventStringWithDataKey(protectedpii.FromEnv(), m.subjectKey, resolved.Event.Data, UserRegisteredEmailField)
		if err != nil {
			return err
		}
		m.email = decryptedemail
	case EmailVerificationOTPSent:
		m.alreadySent = true
	}
	return nil
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

func sendEmailValidationOTPUserRegisteredQuery(userRegisteredID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: UserRegistered},
		{Key: UserRegisteredIDField, Value: userRegisteredID},
	}}}}
}

func loadSendEmailValidationOTPContext(ctx context.Context, command SendEmailValidationOTPCommand, retriever eventstore.LatestRetriever, keys SubjectPiiKeyPort) (*sendEmailValidationOTPContext, error) {
	generatedQuery := sendEmailValidationOTPEmailVerificationOTPGeneratedQuery(command.EmailVerificationOTPGeneratedID)
	userQuery := sendEmailValidationOTPUserRegisteredQuery(command.UserRegisteredID)
	sentQuery := sendEmailValidationOTPEmailVerificationOTPSentQuery(command.EmailVerificationOTPGeneratedID)
	query := sendEmailValidationOTPCombineQueries(generatedQuery, userQuery, sentQuery)
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
	model := &sendEmailValidationOTPContext{
		position: latest.ContextPosition,
		events:   events,
		query:    query,
	}
	subjectKey, ok, err := keys.GetSubjectDataKey(ctx, command.UserRegisteredID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, eventstore.ErrNotFound
	}
	model.subjectKey = subjectKey
	for _, resolved := range model.events {
		if err := model.handle(resolved); err != nil {
			return nil, err
		}
	}

	return model, nil
}
