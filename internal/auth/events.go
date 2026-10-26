package auth

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	"reflect"
	"strings"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

const (
	UserRegistered                = "UserRegistered"
	UserNameChanged               = "UserNameChanged"
	EmailVerificationOTPGenerated = "EmailVerificationOTPGenerated"
	EmailVerificationOTPValidated = "EmailVerificationOTPValidated"
	EmailVerificationOTPSent      = "EmailVerificationOTPSent"
	PasswordResetRequested        = "PasswordResetRequested"
	PasswordResetEmailSent        = "PasswordResetEmailSent"
	PasswordResetCompleted        = "PasswordResetCompleted"
	PasswordChanged               = "PasswordChanged"
)

const (
	UserRegisteredIDField                     = "userRegisteredId"
	UserRegisteredUsernameField               = "username"
	UserRegisteredUsernameHashField           = "usernameHash"
	UserRegisteredEmailField                  = "email"
	UserRegisteredEmailHashField              = "emailHash"
	UserRegisteredFirstNameField              = "firstName"
	UserRegisteredLastNameField               = "lastName"
	UserRegisteredYearOfBirthField            = "yearOfBirth"
	UserRegisteredPasswordHashField           = "passwordHash"
	UserNameChangedIDField                    = "userNameChangedId"
	UserNameChangedNameField                  = "name"
	UserNameChangedNameHashField              = "nameHash"
	UserNameChangedChangedAtField             = "changedAt"
	EmailVerificationOTPGeneratedIDField      = "emailVerificationOTPGeneratedId"
	EmailVerificationOTPCodeField             = "otpCode"
	EmailVerificationOTPExpiresAtField        = "expiresAt"
	EmailVerificationOTPValidatedIDField      = "emailVerificationOTPValidatedId"
	EmailVerificationOTPValidatedAtField      = "validatedAt"
	EmailVerificationOTPSentIDField           = "emailVerificationOTPSentId"
	EmailVerificationOTPSentAtField           = "sentAt"
	PasswordResetRequestedIDField             = "passwordResetRequestedId"
	PasswordResetRequestedEmailField          = "email"
	PasswordResetRequestedEmailHashField      = "emailHash"
	PasswordResetRequestedTokenField          = "resetToken"
	PasswordResetRequestedTokenHashField      = "resetTokenHash"
	PasswordResetRequestedExpiresAtField      = "expiresAt"
	PasswordResetEmailSentIDField             = "passwordResetEmailSentId"
	PasswordResetEmailSentAtField             = "sentAt"
	PasswordResetCompletedIDField             = "passwordResetCompletedId"
	PasswordResetCompletedResetAtField        = "resetAt"
	PasswordResetCompletedPasswordHashField   = "passwordHash"
	PasswordChangedIDField                    = "passwordChangedId"
	PasswordChangedAtField                    = "changedAt"
	PasswordChangedPasswordHashField          = "passwordHash"
	ScopeUserRegisteredIDKey                  = UserRegisteredIDField
	ScopeEmailVerificationOTPGeneratedIDKey   = EmailVerificationOTPGeneratedIDField
	ScopePasswordResetRequestedIDKey          = PasswordResetRequestedIDField
	ScopeUserRegisteredIDField                = "scopes." + ScopeUserRegisteredIDKey
	ScopeEmailVerificationOTPGeneratedIDField = "scopes." + ScopeEmailVerificationOTPGeneratedIDKey
	ScopePasswordResetRequestedIDField        = "scopes." + ScopePasswordResetRequestedIDKey
	AccountDeletionRequested                  = "AccountDeletionRequested"
	AccountDeleted                            = "AccountDeleted"
	AccountDeletionRequestedIDField           = "accountDeletionRequestedId"
	AccountDeletionAuthUserIDField            = "authUserId"
	AccountDeletedIDField                     = "accountDeletedId"
	AccountDeletionRequestedAtField           = "requestedAt"
	AccountDeletedAtField                     = "deletedAt"
	LoginAttemptRecorded                      = "LoginAttemptRecorded"
	LoginAttemptRecordedIDField               = "loginAttemptRecordedId"
	LoginAttemptIdentifierHashField           = "attemptedIdentifierHash"
	LoginAttemptIPAddressHashField            = "ipAddressHash"
	LoginAttemptUserRegisteredIDField         = "user.userRegisteredId"
)

type RegistrationDetails struct {
	Username     protectedpii.Value `json:"username"`
	UsernameHash string             `json:"usernameHash"`
	Email        protectedpii.Value `json:"email"`
	EmailHash    string             `json:"emailHash"`
	FirstName    protectedpii.Value `json:"firstName"`
	LastName     protectedpii.Value `json:"lastName"`
	YearOfBirth  int                `json:"yearOfBirth"`
	PasswordHash string             `json:"passwordHash"`
}
type UserRegisteredEvent struct {
	UserRegisteredID string `json:"userRegisteredId"`
	RegisteredAt     string `json:"registeredAt"`
	EmailVerifiedAt  string `json:"emailVerifiedAt"`
	RegistrationDetails
}

type UserNameChangedEvent struct {
	UserNameChangedID string              `json:"userNameChangedId"`
	Name              protectedpii.Value  `json:"name"`
	NameHash          string              `json:"nameHash"`
	ChangedAt         string              `json:"changedAt"`
	Scopes            UserRegisteredScope `json:"scopes"`
}

type EmailVerificationOTPGeneratedEvent struct {
	EmailVerificationOTPGeneratedID string                       `json:"emailVerificationOTPGeneratedId"`
	OTPCode                         string                       `json:"otpCode"`
	ExpiresAt                       string                       `json:"expiresAt"`
	Scopes                          UserRegistrationStartedScope `json:"scopes"`
}

type EmailVerificationOTPValidatedEvent struct {
	EmailVerificationOTPValidatedID string                             `json:"emailVerificationOTPValidatedId"`
	ValidatedAt                     string                             `json:"validatedAt"`
	Scopes                          EmailVerificationOTPValidatedScope `json:"scopes"`
}

type EmailVerificationOTPSentEvent struct {
	EmailVerificationOTPSentID string                             `json:"emailVerificationOTPSentId"`
	SentAt                     string                             `json:"sentAt"`
	Scopes                     EmailVerificationOTPGeneratedScope `json:"scopes"`
}

type PasswordResetRequestedEvent struct {
	PasswordResetRequestedID string              `json:"passwordResetRequestedId"`
	Email                    protectedpii.Value  `json:"email"`
	EmailHash                string              `json:"emailHash"`
	ResetToken               protectedpii.Value  `json:"resetToken"`
	ResetTokenHash           string              `json:"resetTokenHash"`
	ExpiresAt                string              `json:"expiresAt"`
	Scopes                   UserRegisteredScope `json:"scopes"`
}

type PasswordResetEmailSentEvent struct {
	PasswordResetEmailSentID string                      `json:"passwordResetEmailSentId"`
	SentAt                   string                      `json:"sentAt"`
	Scopes                   PasswordResetRequestedScope `json:"scopes"`
}

type PasswordResetCompletedEvent struct {
	PasswordResetCompletedID string                      `json:"passwordResetCompletedId"`
	ResetAt                  string                      `json:"resetAt"`
	PasswordHash             string                      `json:"passwordHash"`
	Scopes                   PasswordResetCompletedScope `json:"scopes"`
}

type PasswordChangedEvent struct {
	PasswordChangedID string              `json:"passwordChangedId"`
	ChangedAt         string              `json:"changedAt"`
	PasswordHash      string              `json:"passwordHash"`
	Scopes            UserRegisteredScope `json:"scopes"`
}

type AccountDeletionRequestedEvent struct {
	AccountDeletionRequestedID string              `json:"accountDeletionRequestedId"`
	RequestedAt                string              `json:"requestedAt"`
	AuthUserID                 string              `json:"authUserId"`
	Scopes                     UserRegisteredScope `json:"scopes"`
}

type AccountDeletedEvent struct {
	AccountDeletedID string               `json:"accountDeletedId"`
	DeletedAt        string               `json:"deletedAt"`
	AuthUserID       string               `json:"authUserId"`
	Scopes           AccountDeletionScope `json:"scopes"`
}

type LoginAttemptRecordedEvent struct {
	LoginAttemptRecordedID  string             `json:"loginAttemptRecordedId"`
	AttemptedIdentifier     protectedpii.Value `json:"attemptedIdentifier"`
	AttemptedIdentifierHash string             `json:"attemptedIdentifierHash"`
	IPAddress               protectedpii.Value `json:"ipAddress"`
	IPAddressHash           string             `json:"ipAddressHash"`
	User                    LoginAttemptUser   `json:"user"`
	Succeeded               bool               `json:"succeeded"`
	RecordedAt              string             `json:"recordedAt"`
}

// LoginAttemptUser explicitly distinguishes anonymous from identified attempts.
type LoginAttemptUser struct {
	Kind             string `json:"kind"`
	UserRegisteredID string `json:"userRegisteredId,omitempty"`
}

func loginAttemptUser(id string) LoginAttemptUser {
	if id == "" {
		return LoginAttemptUser{Kind: "anonymous"}
	}
	return LoginAttemptUser{Kind: "registered", UserRegisteredID: id}
}

type UserRegisteredScope struct {
	UserRegisteredID string `json:"userRegisteredId"`
}

type EmailVerificationOTPGeneratedScope struct {
	EmailVerificationOTPGeneratedID string `json:"emailVerificationOTPGeneratedId"`
}

type EmailVerificationOTPValidatedScope struct {
	EmailVerificationOTPGeneratedID string `json:"emailVerificationOTPGeneratedId"`
	UserRegistrationStartedID       string `json:"userRegistrationStartedId"`
}

type PasswordResetRequestedScope struct {
	PasswordResetRequestedID string `json:"passwordResetRequestedId"`
}

type PasswordResetCompletedScope struct {
	PasswordResetRequestedID string `json:"passwordResetRequestedId"`
	UserRegisteredID         string `json:"userRegisteredId"`
}

type AccountDeletionScope struct {
	AccountDeletionRequestedID string `json:"accountDeletionRequestedId"`
	UserRegisteredID           string `json:"userRegisteredId"`
}

func protectRegistrationDetails(username, emailAddress, firstName, lastName string, yearOfBirth int, passwordHash string, subjectKey protectedpii.SubjectDataKey) (RegistrationDetails, error) {
	if len(strings.TrimSpace(username)) < 4 || username != strings.TrimSpace(username) || emailAddress == "" || emailAddress != strings.ToLower(strings.TrimSpace(emailAddress)) || firstName != strings.TrimSpace(firstName) || lastName != strings.TrimSpace(lastName) {
		return RegistrationDetails{}, eventstore.ErrInvalidEvent
	}
	protector := protectedpii.FromEnv()
	protected0, err := protector.ProtectWithDataKey(username, UserRegisteredUsernameField, subjectKey)
	if err != nil {
		return RegistrationDetails{}, err
	}
	protected1, err := protector.ProtectWithDataKey(emailAddress, UserRegisteredEmailField, subjectKey)
	if err != nil {
		return RegistrationDetails{}, err
	}
	protected2, err := protector.ProtectWithDataKey(firstName, UserRegisteredFirstNameField, subjectKey)
	if err != nil {
		return RegistrationDetails{}, err
	}
	protected3, err := protector.ProtectWithDataKey(lastName, UserRegisteredLastNameField, subjectKey)
	if err != nil {
		return RegistrationDetails{}, err
	}
	return RegistrationDetails{Username: protected0, UsernameHash: protector.BlindIndex(UserRegisteredUsernameField, username), Email: protected1, EmailHash: protector.BlindIndex(UserRegisteredEmailField, emailAddress), FirstName: protected2, LastName: protected3, YearOfBirth: yearOfBirth, PasswordHash: passwordHash}, nil
}
func NewUserRegisteredEvent(userRegisteredID, username, emailAddress, firstName, lastName string, yearOfBirth int, passwordHash string, registeredAt, emailVerifiedAt time.Time, subjectKey protectedpii.SubjectDataKey, metadata map[string]any) (eventstore.DomainEvent, error) {
	details, err := protectRegistrationDetails(username, emailAddress, firstName, lastName, yearOfBirth, passwordHash, subjectKey)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	event := eventstore.DomainEvent{EventID: userRegisteredID, EventType: UserRegistered, Data: eventstore.MustData(UserRegisteredEvent{UserRegisteredID: userRegisteredID, RegisteredAt: formatEventTime(registeredAt), EmailVerifiedAt: formatEventTime(emailVerifiedAt), RegistrationDetails: details}), Metadata: metadata}
	_, err = DecodeUserRegisteredEvent(event)
	return event, err
}

func NewUserNameChangedEvent(userNameChangedID, name string, changedAt time.Time, userRegisteredID string, subjectKey protectedpii.SubjectDataKey, metadata map[string]any) (eventstore.DomainEvent, error) {
	if strings.TrimSpace(name) == "" || name != strings.TrimSpace(name) {
		return eventstore.DomainEvent{}, eventstore.ErrInvalidEvent
	}
	protector := protectedpii.FromEnv()
	protected0, err := protector.ProtectWithDataKey(name, UserNameChangedNameField, subjectKey)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	event := eventstore.DomainEvent{
		EventID:   userNameChangedID,
		EventType: UserNameChanged,
		Data: eventstore.MustData(UserNameChangedEvent{
			UserNameChangedID: userNameChangedID,
			Name:              protected0,
			NameHash:          protector.BlindIndex(UserNameChangedNameField, name),
			ChangedAt:         formatEventTime(changedAt),
			Scopes:            UserRegisteredScope{UserRegisteredID: userRegisteredID},
		}),
		Metadata: metadata,
	}
	_, err = DecodeUserNameChangedEvent(event)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	return event, nil
}

func NewEmailVerificationOTPGeneratedEvent(emailVerificationOTPGeneratedID, otpCode string, expiresAt time.Time, userRegistrationStartedID string, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   emailVerificationOTPGeneratedID,
		EventType: EmailVerificationOTPGenerated,
		Data: eventstore.MustData(EmailVerificationOTPGeneratedEvent{
			EmailVerificationOTPGeneratedID: emailVerificationOTPGeneratedID,
			OTPCode:                         otpCode,
			ExpiresAt:                       formatEventTime(expiresAt),
			Scopes:                          UserRegistrationStartedScope{UserRegistrationStartedID: userRegistrationStartedID},
		}),
		Metadata: metadata,
	}
	_, err := DecodeEmailVerificationOTPGeneratedEvent(event)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	return event, nil
}

func NewEmailVerificationOTPValidatedEvent(emailVerificationOTPValidatedID string, validatedAt time.Time, emailVerificationOTPGeneratedID, userRegistrationStartedID string, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   emailVerificationOTPValidatedID,
		EventType: EmailVerificationOTPValidated,
		Data: eventstore.MustData(EmailVerificationOTPValidatedEvent{
			EmailVerificationOTPValidatedID: emailVerificationOTPValidatedID,
			ValidatedAt:                     formatEventTime(validatedAt),
			Scopes: EmailVerificationOTPValidatedScope{
				EmailVerificationOTPGeneratedID: emailVerificationOTPGeneratedID,
				UserRegistrationStartedID:       userRegistrationStartedID,
			},
		}),
		Metadata: metadata,
	}
	_, err := DecodeEmailVerificationOTPValidatedEvent(event)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	return event, nil
}

func NewEmailVerificationOTPSentEvent(emailVerificationOTPSentID string, sentAt time.Time, emailVerificationOTPGeneratedID string, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   emailVerificationOTPSentID,
		EventType: EmailVerificationOTPSent,
		Data: eventstore.MustData(EmailVerificationOTPSentEvent{
			EmailVerificationOTPSentID: emailVerificationOTPSentID,
			SentAt:                     formatEventTime(sentAt),
			Scopes:                     EmailVerificationOTPGeneratedScope{EmailVerificationOTPGeneratedID: emailVerificationOTPGeneratedID},
		}),
		Metadata: metadata,
	}
	_, err := DecodeEmailVerificationOTPSentEvent(event)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	return event, nil
}

func NewPasswordResetRequestedEvent(passwordResetRequestedID, emailAddress, resetToken string, expiresAt time.Time, userRegisteredID string, subjectKey protectedpii.SubjectDataKey, metadata map[string]any) (eventstore.DomainEvent, error) {
	if strings.TrimSpace(emailAddress) == "" || resetToken == "" {
		return eventstore.DomainEvent{}, eventstore.ErrInvalidEvent
	}
	protector := protectedpii.FromEnv()
	protected0, err := protector.ProtectWithDataKey(emailAddress, PasswordResetRequestedEmailField, subjectKey)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	protected1, err := protector.ProtectWithDataKey(resetToken, PasswordResetRequestedTokenField, subjectKey)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	event := eventstore.DomainEvent{
		EventID:   passwordResetRequestedID,
		EventType: PasswordResetRequested,
		Data: eventstore.MustData(PasswordResetRequestedEvent{
			PasswordResetRequestedID: passwordResetRequestedID,
			Email:                    protected0,
			EmailHash:                protector.BlindIndex(PasswordResetRequestedEmailField, emailAddress),
			ResetToken:               protected1,
			ResetTokenHash:           protector.SensitiveBlindIndex(PasswordResetRequestedTokenField, resetToken),
			ExpiresAt:                formatEventTime(expiresAt),
			Scopes:                   UserRegisteredScope{UserRegisteredID: userRegisteredID},
		}),
		Metadata: metadata,
	}
	_, err = DecodePasswordResetRequestedEvent(event)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	return event, nil
}

func NewPasswordResetEmailSentEvent(passwordResetEmailSentID string, sentAt time.Time, passwordResetRequestedID string, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   passwordResetEmailSentID,
		EventType: PasswordResetEmailSent,
		Data: eventstore.MustData(PasswordResetEmailSentEvent{
			PasswordResetEmailSentID: passwordResetEmailSentID,
			SentAt:                   formatEventTime(sentAt),
			Scopes:                   PasswordResetRequestedScope{PasswordResetRequestedID: passwordResetRequestedID},
		}),
		Metadata: metadata,
	}
	_, err := DecodePasswordResetEmailSentEvent(event)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	return event, nil
}

func NewPasswordResetCompletedEvent(passwordResetCompletedID string, resetAt time.Time, passwordResetRequestedID, userRegisteredID, passwordHash string, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   passwordResetCompletedID,
		EventType: PasswordResetCompleted,
		Data: eventstore.MustData(PasswordResetCompletedEvent{
			PasswordResetCompletedID: passwordResetCompletedID,
			ResetAt:                  formatEventTime(resetAt),
			PasswordHash:             passwordHash,
			Scopes: PasswordResetCompletedScope{
				PasswordResetRequestedID: passwordResetRequestedID,
				UserRegisteredID:         userRegisteredID,
			},
		}),
		Metadata: metadata,
	}
	_, err := DecodePasswordResetCompletedEvent(event)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	return event, nil
}

func NewPasswordChangedEvent(passwordChangedID string, changedAt time.Time, userRegisteredID, passwordHash string, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   passwordChangedID,
		EventType: PasswordChanged,
		Data: eventstore.MustData(PasswordChangedEvent{
			PasswordChangedID: passwordChangedID,
			ChangedAt:         formatEventTime(changedAt),
			PasswordHash:      passwordHash,
			Scopes:            UserRegisteredScope{UserRegisteredID: userRegisteredID},
		}),
		Metadata: metadata,
	}
	_, err := DecodePasswordChangedEvent(event)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	return event, nil
}

func NewAccountDeletionRequestedEvent(accountDeletionRequestedID string, requestedAt time.Time, authUserID, userRegisteredID string, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   accountDeletionRequestedID,
		EventType: AccountDeletionRequested,
		Data: eventstore.MustData(AccountDeletionRequestedEvent{
			AccountDeletionRequestedID: accountDeletionRequestedID,
			RequestedAt:                formatEventTime(requestedAt),
			AuthUserID:                 authUserID,
			Scopes:                     UserRegisteredScope{UserRegisteredID: userRegisteredID},
		}),
		Metadata: metadata,
	}
	_, err := DecodeAccountDeletionRequestedEvent(event)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	return event, nil
}

func NewAccountDeletedEvent(accountDeletedID string, deletedAt time.Time, accountDeletionRequestedID, authUserID, userRegisteredID string, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   accountDeletedID,
		EventType: AccountDeleted,
		Data: eventstore.MustData(AccountDeletedEvent{
			AccountDeletedID: accountDeletedID,
			DeletedAt:        formatEventTime(deletedAt),
			AuthUserID:       authUserID,
			Scopes: AccountDeletionScope{
				AccountDeletionRequestedID: accountDeletionRequestedID,
				UserRegisteredID:           userRegisteredID,
			},
		}),
		Metadata: metadata,
	}
	_, err := DecodeAccountDeletedEvent(event)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	return event, nil
}

func NewLoginAttemptRecordedEvent(loginAttemptRecordedID string, recordedAt time.Time, attemptedIdentifier, ipAddress, userRegisteredID string, succeeded bool, metadata map[string]any) (eventstore.DomainEvent, error) {
	protector := protectedpii.FromEnv()
	protected0, err := protector.Protect(attemptedIdentifier)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	protected1, err := protector.Protect(ipAddress)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	event := eventstore.DomainEvent{
		EventID:   loginAttemptRecordedID,
		EventType: LoginAttemptRecorded,
		Data: eventstore.MustData(LoginAttemptRecordedEvent{
			LoginAttemptRecordedID:  loginAttemptRecordedID,
			AttemptedIdentifier:     protected0,
			AttemptedIdentifierHash: protector.BlindIndex("attemptedIdentifier", attemptedIdentifier),
			IPAddress:               protected1,
			IPAddressHash:           protector.SensitiveBlindIndex("ipAddress", ipAddress),
			User:                    loginAttemptUser(userRegisteredID),
			Succeeded:               succeeded,
			RecordedAt:              formatEventTime(recordedAt),
		}),
		Metadata: metadata,
	}
	_, err = DecodeLoginAttemptRecordedEvent(event)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	return event, nil
}

func formatEventTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

// ValidateEvent checks the complete auth event contract without decrypting PII.
// It is safe to use after subject-key erasure and before any consumer effects.
func ValidateEvent(event eventstore.DomainEvent) error {
	switch event.EventType {
	case UserRegistrationStarted:
		_, err := DecodeUserRegistrationStartedEvent(event)
		return err
	case UserRegistrationCompleted:
		_, err := DecodeUserRegistrationCompletedEvent(event)
		return err
	case UserRegistrationFailed:
		_, err := DecodeUserRegistrationFailedEvent(event)
		return err
	case UserRegistered:
		_, err := DecodeUserRegisteredEvent(event)
		return err
	case UserNameChanged:
		_, err := DecodeUserNameChangedEvent(event)
		return err
	case EmailVerificationOTPGenerated:
		_, err := DecodeEmailVerificationOTPGeneratedEvent(event)
		return err
	case EmailVerificationOTPValidated:
		_, err := DecodeEmailVerificationOTPValidatedEvent(event)
		return err
	case EmailVerificationOTPSent:
		_, err := DecodeEmailVerificationOTPSentEvent(event)
		return err
	case PasswordResetRequested:
		_, err := DecodePasswordResetRequestedEvent(event)
		return err
	case PasswordResetEmailSent:
		_, err := DecodePasswordResetEmailSentEvent(event)
		return err
	case PasswordResetCompleted:
		_, err := DecodePasswordResetCompletedEvent(event)
		return err
	case PasswordChanged:
		_, err := DecodePasswordChangedEvent(event)
		return err
	case AccountDeletionRequested:
		_, err := DecodeAccountDeletionRequestedEvent(event)
		return err
	case AccountDeleted:
		_, err := DecodeAccountDeletedEvent(event)
		return err
	case LoginAttemptRecorded:
		_, err := DecodeLoginAttemptRecordedEvent(event)
		return err
	default:
		return fmt.Errorf("%w: unknown auth event type %q", eventstore.ErrInvalidEvent, event.EventType)
	}
}

func DecodeUserRegisteredEvent(event eventstore.DomainEvent) (UserRegisteredEvent, error) {
	var value UserRegisteredEvent
	if err := decodeAuthPayload(event, UserRegistered, &value); err != nil {
		return value, err
	}
	if value.UsernameHash != value.Username.BlindIndex || value.EmailHash != value.Email.BlindIndex {
		return value, eventstore.ErrInvalidEvent
	}
	if err := validateAuthValue(reflect.ValueOf(value), ""); err != nil {
		return value, err
	}
	registered, _ := time.Parse(time.RFC3339Nano, value.RegisteredAt)
	verified, _ := time.Parse(time.RFC3339Nano, value.EmailVerifiedAt)
	if registered.Before(verified) {
		return value, eventstore.ErrInvalidEvent
	}
	return value, nil
}

func DecodeUserNameChangedEvent(event eventstore.DomainEvent) (UserNameChangedEvent, error) {
	var value UserNameChangedEvent
	if err := decodeAuthPayload(event, UserNameChanged, &value); err != nil {
		return value, err
	}
	if value.NameHash != value.Name.BlindIndex {
		return value, eventstore.ErrInvalidEvent
	}
	return value, validateAuthValue(reflect.ValueOf(value), "")
}

func DecodeEmailVerificationOTPGeneratedEvent(event eventstore.DomainEvent) (EmailVerificationOTPGeneratedEvent, error) {
	var value EmailVerificationOTPGeneratedEvent
	if err := decodeAuthPayload(event, EmailVerificationOTPGenerated, &value); err != nil {
		return value, err
	}
	return value, validateAuthValue(reflect.ValueOf(value), "")
}

func DecodeEmailVerificationOTPValidatedEvent(event eventstore.DomainEvent) (EmailVerificationOTPValidatedEvent, error) {
	var value EmailVerificationOTPValidatedEvent
	if err := decodeAuthPayload(event, EmailVerificationOTPValidated, &value); err != nil {
		return value, err
	}
	return value, validateAuthValue(reflect.ValueOf(value), "")
}

func DecodeEmailVerificationOTPSentEvent(event eventstore.DomainEvent) (EmailVerificationOTPSentEvent, error) {
	var value EmailVerificationOTPSentEvent
	if err := decodeAuthPayload(event, EmailVerificationOTPSent, &value); err != nil {
		return value, err
	}
	return value, validateAuthValue(reflect.ValueOf(value), "")
}

func DecodePasswordResetRequestedEvent(event eventstore.DomainEvent) (PasswordResetRequestedEvent, error) {
	var value PasswordResetRequestedEvent
	if err := decodeAuthPayload(event, PasswordResetRequested, &value); err != nil {
		return value, err
	}
	if value.EmailHash != value.Email.BlindIndex {
		return value, eventstore.ErrInvalidEvent
	}
	return value, validateAuthValue(reflect.ValueOf(value), "")
}

func DecodePasswordResetEmailSentEvent(event eventstore.DomainEvent) (PasswordResetEmailSentEvent, error) {
	var value PasswordResetEmailSentEvent
	if err := decodeAuthPayload(event, PasswordResetEmailSent, &value); err != nil {
		return value, err
	}
	return value, validateAuthValue(reflect.ValueOf(value), "")
}

func DecodePasswordResetCompletedEvent(event eventstore.DomainEvent) (PasswordResetCompletedEvent, error) {
	var value PasswordResetCompletedEvent
	if err := decodeAuthPayload(event, PasswordResetCompleted, &value); err != nil {
		return value, err
	}
	return value, validateAuthValue(reflect.ValueOf(value), "")
}

func DecodePasswordChangedEvent(event eventstore.DomainEvent) (PasswordChangedEvent, error) {
	var value PasswordChangedEvent
	if err := decodeAuthPayload(event, PasswordChanged, &value); err != nil {
		return value, err
	}
	return value, validateAuthValue(reflect.ValueOf(value), "")
}

func DecodeAccountDeletionRequestedEvent(event eventstore.DomainEvent) (AccountDeletionRequestedEvent, error) {
	var value AccountDeletionRequestedEvent
	if err := decodeAuthPayload(event, AccountDeletionRequested, &value); err != nil {
		return value, err
	}
	return value, validateAuthValue(reflect.ValueOf(value), "")
}

func DecodeAccountDeletedEvent(event eventstore.DomainEvent) (AccountDeletedEvent, error) {
	var value AccountDeletedEvent
	if err := decodeAuthPayload(event, AccountDeleted, &value); err != nil {
		return value, err
	}
	return value, validateAuthValue(reflect.ValueOf(value), "")
}

func DecodeLoginAttemptRecordedEvent(event eventstore.DomainEvent) (LoginAttemptRecordedEvent, error) {
	var value LoginAttemptRecordedEvent
	if err := decodeAuthPayload(event, LoginAttemptRecorded, &value); err != nil {
		return value, err
	}
	if value.Succeeded && value.User.Kind != "registered" {
		return value, eventstore.ErrInvalidEvent
	}
	return value, validateAuthValue(reflect.ValueOf(value), "")
}

func decodeAuthPayload(event eventstore.DomainEvent, kind string, target any) error {
	invalid := func() error { return fmt.Errorf("%w: invalid %s contract", eventstore.ErrInvalidEvent, kind) }
	if event.EventType != kind || !validAuthID(event.EventID) {
		return invalid()
	}
	encoded, err := json.Marshal(event.Data)
	if err != nil {
		return invalid()
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return invalid()
	}
	canonical, err := json.Marshal(target)
	if err != nil || !bytes.Equal(encoded, canonicalJSON(canonical)) {
		return invalid()
	}
	fields := reflect.ValueOf(target).Elem()
	id := fields.FieldByName(kind + "ID")
	if !id.IsValid() || id.String() != event.EventID {
		return invalid()
	}
	return nil
}

// Maps are encoded with sorted keys; compare through JSON so stored numeric
// fields and typed integer fields have the same canonical representation.
func canonicalJSON(encoded []byte) []byte {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil
	}
	result, _ := json.Marshal(value)
	return result
}

func validAuthID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}
func validAuthHash(value string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil && len(decoded) == 32 && base64.RawURLEncoding.EncodeToString(decoded) == value
}
func validateAuthValue(value reflect.Value, field string) error {
	invalid := func() error { return fmt.Errorf("%w: invalid auth field %s", eventstore.ErrInvalidEvent, field) }
	if value.Type() == reflect.TypeOf(protectedpii.Value{}) {
		protected := value.Interface().(protectedpii.Value)
		nonce, nerr := base64.RawURLEncoding.DecodeString(protected.Nonce)
		ciphertext, cerr := base64.RawURLEncoding.DecodeString(protected.Ciphertext)
		if protected.Version != 1 || strings.TrimSpace(protected.KeyID) == "" || nerr != nil || len(nonce) != 12 || cerr != nil || len(ciphertext) < 16 || !validAuthHash(protected.BlindIndex) {
			return invalid()
		}
		return nil
	}
	if value.Type() == reflect.TypeOf(LoginAttemptUser{}) {
		user := value.Interface().(LoginAttemptUser)
		if user.Kind == "anonymous" && user.UserRegisteredID == "" {
			return nil
		}
		if user.Kind == "registered" && validAuthID(user.UserRegisteredID) {
			return nil
		}
		return invalid()
	}
	switch value.Kind() {
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if err := validateAuthValue(value.Field(i), value.Type().Field(i).Name); err != nil {
				return err
			}
		}
	case reflect.String:
		text := value.String()
		switch {
		case strings.HasSuffix(field, "ID"):
			if !validAuthID(text) {
				return invalid()
			}
		case strings.HasSuffix(field, "At"):
			instant, err := time.Parse(time.RFC3339Nano, text)
			if err != nil || instant.IsZero() || instant.UTC().Format(time.RFC3339Nano) != text {
				return invalid()
			}
		case field == "PasswordHash":
			if _, err := bcrypt.Cost([]byte(text)); err != nil {
				return invalid()
			}
		case strings.HasSuffix(field, "Hash"):
			if !validAuthHash(text) {
				return invalid()
			}
		case field == "OTPCode":
			if len(text) != 6 {
				return invalid()
			}
			for _, c := range text {
				if c < '0' || c > '9' {
					return invalid()
				}
			}
		}
	}
	return nil
}
