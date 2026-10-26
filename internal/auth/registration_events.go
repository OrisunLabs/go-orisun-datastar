package auth

import (
	"reflect"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

const (
	UserRegistrationStarted             = "UserRegistrationStarted"
	UserRegistrationCompleted           = "UserRegistrationCompleted"
	UserRegistrationFailed              = "UserRegistrationFailed"
	UserRegistrationStartedIDField      = "userRegistrationStartedId"
	ScopeUserRegistrationStartedIDField = "scopes.userRegistrationStartedId"
)

type UserRegistrationStartedScope struct {
	UserRegistrationStartedID string `json:"userRegistrationStartedId"`
}
type UserRegistrationStartedEvent struct {
	UserRegistrationStartedID string `json:"userRegistrationStartedId"`
	UserID                    string `json:"userId"`
	StartedAt                 string `json:"startedAt"`
	RegistrationDetails
}
type UserRegistrationCompletedScope struct {
	UserRegistrationStartedID       string `json:"userRegistrationStartedId"`
	EmailVerificationOTPValidatedID string `json:"emailVerificationOTPValidatedId"`
	UserRegisteredID                string `json:"userRegisteredId"`
}
type UserRegistrationCompletedEvent struct {
	UserRegistrationCompletedID string                         `json:"userRegistrationCompletedId"`
	CompletedAt                 string                         `json:"completedAt"`
	Scopes                      UserRegistrationCompletedScope `json:"scopes"`
}
type UserRegistrationFailedEvent struct {
	UserRegistrationFailedID string                       `json:"userRegistrationFailedId"`
	FailedAt                 string                       `json:"failedAt"`
	Reason                   string                       `json:"reason"`
	Scopes                   UserRegistrationStartedScope `json:"scopes"`
}

func NewUserRegistrationStartedEvent(id, userID, username, email, first, last string, birth int, hash string, at time.Time, key protectedpii.SubjectDataKey, metadata map[string]any) (eventstore.DomainEvent, error) {
	details, err := protectRegistrationDetails(username, email, first, last, birth, hash, key)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	e := eventstore.DomainEvent{EventID: id, EventType: UserRegistrationStarted, Data: eventstore.MustData(UserRegistrationStartedEvent{UserRegistrationStartedID: id, UserID: userID, StartedAt: formatEventTime(at), RegistrationDetails: details}), Metadata: metadata}
	_, err = DecodeUserRegistrationStartedEvent(e)
	return e, err
}
func DecodeUserRegistrationStartedEvent(e eventstore.DomainEvent) (UserRegistrationStartedEvent, error) {
	var v UserRegistrationStartedEvent
	if err := decodeAuthPayload(e, UserRegistrationStarted, &v); err != nil {
		return v, err
	}
	if v.UserRegistrationStartedID == v.UserID || v.UsernameHash != v.Username.BlindIndex || v.EmailHash != v.Email.BlindIndex {
		return v, eventstore.ErrInvalidEvent
	}
	return v, validateAuthValue(reflect.ValueOf(v), "")
}
func NewUserRegistrationCompletedEvent(id, registrationID, validationID, userID string, at time.Time, metadata map[string]any) (eventstore.DomainEvent, error) {
	e := eventstore.DomainEvent{EventID: id, EventType: UserRegistrationCompleted, Data: eventstore.MustData(UserRegistrationCompletedEvent{id, formatEventTime(at), UserRegistrationCompletedScope{registrationID, validationID, userID}}), Metadata: metadata}
	_, err := DecodeUserRegistrationCompletedEvent(e)
	return e, err
}
func DecodeUserRegistrationCompletedEvent(e eventstore.DomainEvent) (UserRegistrationCompletedEvent, error) {
	var v UserRegistrationCompletedEvent
	if err := decodeAuthPayload(e, UserRegistrationCompleted, &v); err != nil {
		return v, err
	}
	return v, validateAuthValue(reflect.ValueOf(v), "")
}
func NewUserRegistrationFailedEvent(id, registrationID string, at time.Time, metadata map[string]any) (eventstore.DomainEvent, error) {
	e := eventstore.DomainEvent{EventID: id, EventType: UserRegistrationFailed, Data: eventstore.MustData(UserRegistrationFailedEvent{id, formatEventTime(at), "identity_unavailable", UserRegistrationStartedScope{registrationID}}), Metadata: metadata}
	_, err := DecodeUserRegistrationFailedEvent(e)
	return e, err
}
func DecodeUserRegistrationFailedEvent(e eventstore.DomainEvent) (UserRegistrationFailedEvent, error) {
	var v UserRegistrationFailedEvent
	if err := decodeAuthPayload(e, UserRegistrationFailed, &v); err != nil {
		return v, err
	}
	if v.Reason != "identity_unavailable" {
		return v, eventstore.ErrInvalidEvent
	}
	return v, validateAuthValue(reflect.ValueOf(v), "")
}
