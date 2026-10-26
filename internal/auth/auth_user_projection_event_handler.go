package auth

import (
	"context"
	"log/slog"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

const AuthUserProjectionEventHandlerName = "auth_user_projection_event_handler"

type AuthUserProjectionWriter interface {
	CreateRegisteredUserAccount(ctx context.Context, registered RegisterUserResult) error
	MarkEmailVerified(ctx context.Context, userRegisteredID string) error
	UpdateNameByRegisteredID(ctx context.Context, userRegisteredID, name string) error
	UpdatePasswordByRegisteredID(ctx context.Context, userRegisteredID, passwordHash string) error
}

type AuthVerificationProjectionWriter interface {
	CreateEmailVerificationOTP(ctx context.Context, userRegisteredID, otpID, code string, expiresAt time.Time) error
	CreatePasswordReset(ctx context.Context, userRegisteredID, requestID, token string, expiresAt time.Time) error
}

type AuthUserProjectionEventHandler struct {
	global        *eventstore.GlobalEventHandler
	writer        AuthUserProjectionWriter
	verifications AuthVerificationProjectionWriter
	keys          SubjectPiiKeyPort
}

func NewAuthUserProjectionEventHandler(
	subscriber eventstore.Subscriber,
	checkpointer eventstore.Checkpointer,
	writer AuthUserProjectionWriter,
	verifications AuthVerificationProjectionWriter,
	keys SubjectPiiKeyPort,
	logger *slog.Logger) (*AuthUserProjectionEventHandler, error) {
	handler := &AuthUserProjectionEventHandler{writer: writer, verifications: verifications, keys: keys}
	global, err := eventstore.NewGlobalEventHandler(eventstore.GlobalEventHandlerConfig{
		Subscriber:      subscriber,
		Checkpointer:    checkpointer,
		Name:            AuthUserProjectionEventHandlerName,
		Query:           authUserProjectionEventHandlerQuery(),
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

func (h *AuthUserProjectionEventHandler) StartSubscribing(ctx context.Context) error {
	return h.global.StartSubscribing(ctx)
}

func (h *AuthUserProjectionEventHandler) StopSubscribing() {
	h.global.StopSubscribing()
}

func (h *AuthUserProjectionEventHandler) handle(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	if err := ValidateEvent(resolved.Event); err != nil {
		return err
	}
	switch resolved.Event.EventType {
	case UserRegistered:
		return h.handleUserRegistered(ctx, resolved)
	case UserNameChanged:
		return h.handleUserNameChanged(ctx, resolved)
	case EmailVerificationOTPGenerated:
		return h.handleEmailVerificationOTPGenerated(ctx, resolved)
	case EmailVerificationOTPValidated:
		return h.handleEmailVerificationOTPValidated(ctx, resolved)
	case PasswordResetRequested:
		return h.handlePasswordResetRequested(ctx, resolved)
	case PasswordResetCompleted, PasswordChanged:
		return h.handlePasswordUpdated(ctx, resolved)
	default:
		return nil
	}
}

func (h *AuthUserProjectionEventHandler) handleUserRegistered(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	value, err := DecodeUserRegisteredEvent(resolved.Event)
	if err != nil {
		return err
	}
	subjectKey, ok, err := h.keys.GetSubjectDataKey(ctx, value.UserRegisteredID)
	if err != nil {
		return err
	}
	if !ok {
		return eventstore.ErrNotFound
	}
	protector := protectedpii.FromEnv()
	username, err := protector.DecryptWithDataKey(value.Username, subjectKey)
	if err != nil {
		return err
	}
	email, err := protector.DecryptWithDataKey(value.Email, subjectKey)
	if err != nil {
		return err
	}
	first, err := protector.DecryptWithDataKey(value.FirstName, subjectKey)
	if err != nil {
		return err
	}
	last, err := protector.DecryptWithDataKey(value.LastName, subjectKey)
	if err != nil {
		return err
	}
	return h.writer.CreateRegisteredUserAccount(ctx, RegisterUserResult{UserRegisteredID: value.UserRegisteredID, Username: username, Email: email, FirstName: first, LastName: last, PasswordHash: value.PasswordHash})
}

func (h *AuthUserProjectionEventHandler) handleUserNameChanged(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	value, err := DecodeUserNameChangedEvent(resolved.Event)
	if err != nil {
		return err
	}
	key, ok, err := h.keys.GetSubjectDataKey(ctx, value.Scopes.UserRegisteredID)
	if err != nil {
		return err
	}
	if !ok {
		return eventstore.ErrNotFound
	}
	name, err := protectedpii.FromEnv().DecryptWithDataKey(value.Name, key)
	if err != nil {
		return err
	}
	if name == "" {
		return eventstore.ErrInvalidEvent
	}
	return h.writer.UpdateNameByRegisteredID(ctx, value.Scopes.UserRegisteredID, name)
}

func (h *AuthUserProjectionEventHandler) handleEmailVerificationOTPGenerated(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	value, err := DecodeEmailVerificationOTPGeneratedEvent(resolved.Event)
	if err != nil {
		return err
	}
	expires, _ := time.Parse(time.RFC3339Nano, value.ExpiresAt)
	return h.verifications.CreateEmailVerificationOTP(ctx, value.Scopes.UserRegisteredID, value.EmailVerificationOTPGeneratedID, value.OTPCode, expires)
}

func (h *AuthUserProjectionEventHandler) handleEmailVerificationOTPValidated(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	value, err := DecodeEmailVerificationOTPValidatedEvent(resolved.Event)
	if err != nil {
		return err
	}
	return h.writer.MarkEmailVerified(ctx, value.Scopes.UserRegisteredID)
}

func (h *AuthUserProjectionEventHandler) handlePasswordResetRequested(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	value, err := DecodePasswordResetRequestedEvent(resolved.Event)
	if err != nil {
		return err
	}
	key, ok, err := h.keys.GetSubjectDataKey(ctx, value.Scopes.UserRegisteredID)
	if err != nil {
		return err
	}
	if !ok {
		return eventstore.ErrNotFound
	}
	token, err := protectedpii.FromEnv().DecryptWithDataKey(value.ResetToken, key)
	if err != nil {
		return err
	}
	if token == "" || protectedpii.FromEnv().SensitiveBlindIndex(PasswordResetRequestedTokenField, token) != value.ResetTokenHash {
		return eventstore.ErrInvalidEvent
	}
	expires, _ := time.Parse(time.RFC3339Nano, value.ExpiresAt)
	return h.verifications.CreatePasswordReset(ctx, value.Scopes.UserRegisteredID, value.PasswordResetRequestedID, token, expires)
}

func (h *AuthUserProjectionEventHandler) handlePasswordUpdated(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	if resolved.Event.EventType == PasswordResetCompleted {
		value, err := DecodePasswordResetCompletedEvent(resolved.Event)
		if err != nil {
			return err
		}
		return h.writer.UpdatePasswordByRegisteredID(ctx, value.Scopes.UserRegisteredID, value.PasswordHash)
	}
	value, err := DecodePasswordChangedEvent(resolved.Event)
	if err != nil {
		return err
	}
	return h.writer.UpdatePasswordByRegisteredID(ctx, value.Scopes.UserRegisteredID, value.PasswordHash)
}

func authUserProjectionEventHandlerQuery() eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserNameChanged}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPGenerated}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPValidated}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetRequested}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetCompleted}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordChanged}}},
	}}
}

func stringValue(value any) string {
	v, _ := value.(string)
	return v
}
