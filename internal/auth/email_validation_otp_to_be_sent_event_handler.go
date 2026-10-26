package auth

import (
	"context"
	"log/slog"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

const EmailValidationOTPToBeSentEventHandlerName = "todo_email_validation_otp_to_be_sent_event_handler"

type EmailValidationOTPToBeSentEventHandler struct {
	global    *eventstore.GlobalEventHandler
	retriever eventstore.Retriever
	saver     eventstore.SaverV2
	sender    EmailSender
	keys      SubjectPiiKeyReader
}

func NewEmailValidationOTPToBeSentEventHandler(subscriber eventstore.Subscriber, checkpointer eventstore.Checkpointer, retriever eventstore.Retriever, saver eventstore.SaverV2, sender EmailSender, keys SubjectPiiKeyReader, logger *slog.Logger) (*EmailValidationOTPToBeSentEventHandler, error) {
	handler := &EmailValidationOTPToBeSentEventHandler{retriever: retriever, saver: saver, sender: sender, keys: keys}
	global, err := eventstore.NewGlobalEventHandler(eventstore.GlobalEventHandlerConfig{
		Subscriber:      subscriber,
		Checkpointer:    checkpointer,
		Name:            EmailValidationOTPToBeSentEventHandlerName,
		Query:           emailValidationOTPToBeSentEventHandlerQuery(),
		Logger:          logger,
		MaxEventRetries: -1,
		HandleEvent:     handler.handle,
	})
	if err != nil {
		return nil, err
	}
	handler.global = global
	return handler, nil
}

func (h *EmailValidationOTPToBeSentEventHandler) StartSubscribing(ctx context.Context) error {
	return h.global.StartSubscribing(ctx)
}

func (h *EmailValidationOTPToBeSentEventHandler) StopSubscribing() {
	h.global.StopSubscribing()
}

func (h *EmailValidationOTPToBeSentEventHandler) handle(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	if resolved.Event.EventType != EmailVerificationOTPGenerated {
		return nil
	}
	if err := ValidateEvent(resolved.Event); err != nil {
		return err
	}
	otpID, _ := resolved.Event.Data[EmailVerificationOTPGeneratedIDField].(string)
	userRegisteredID, _ := eventstore.Scopes(resolved.Event.Data)[ScopeUserRegisteredIDKey].(string)
	return SendEmailValidationOTPCommandHandler(ctx, SendEmailValidationOTPCommand{
		UserRegisteredID:                userRegisteredID,
		EmailVerificationOTPGeneratedID: otpID,
		Metadata:                        eventstore.EventHandlerCommandMetadata(EmailValidationOTPToBeSentEventHandlerName, resolved),
	}, h.saver, h.retriever, h.sender, h.keys)
}

func emailValidationOTPToBeSentEventHandlerQuery() eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPGenerated}}}}}
}
