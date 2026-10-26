package auth

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"golang.org/x/crypto/bcrypt"
)

var ErrRegistrationIdentityUnavailable = errors.New("username or email is already registered")

type RegisterUserCommand struct {
	UserRegisteredID                     string
	Username, Email, FirstName, LastName string
	YearOfBirth                          int
	PasswordHash                         string
	EmailVerifiedAt                      time.Time
	Metadata                             CommandMetadata
}
type RegisterUserResult struct {
	UserRegisteredID, Username, Email, FirstName, LastName, PasswordHash string
}

// RegisterUserCommandHandler accepts complete ordinary domain input. It knows
// nothing about signup, OTPs, proposals or their history.
func RegisterUserCommandHandler(ctx context.Context, command RegisterUserCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, keys SubjectPiiKeyPort) (RegisterUserResult, error) {
	if err := commandlimits.Assert(command); err != nil {
		return RegisterUserResult{}, err
	}
	model, err := loadRegisterUserContext(ctx, command, retriever)
	if err != nil {
		return RegisterUserResult{}, err
	}
	result := RegisterUserResult{command.UserRegisteredID, command.Username, command.Email, command.FirstName, command.LastName, command.PasswordHash}
	if model.existing {
		return result, nil
	}
	key, err := keys.GetOrCreateSubjectDataKey(ctx, command.UserRegisteredID)
	if err != nil {
		return RegisterUserResult{}, err
	}
	event, err := NewUserRegisteredEvent(command.UserRegisteredID, command.Username, command.Email, command.FirstName, command.LastName, command.YearOfBirth, command.PasswordHash, time.Now(), command.EmailVerifiedAt, key, nil)
	if err != nil {
		return RegisterUserResult{}, err
	}
	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}}); err != nil {
		return RegisterUserResult{}, err
	}
	return result, nil
}

type registerUserContext struct {
	existing bool
	query    eventstore.Query
	position eventstore.Position
}

func loadRegisterUserContext(ctx context.Context, command RegisterUserCommand, retriever eventstore.LatestRetriever) (*registerUserContext, error) {
	if !validAuthID(command.UserRegisteredID) || len(command.Username) < 4 || command.Username != strings.TrimSpace(command.Username) || command.Email == "" || command.Email != strings.ToLower(strings.TrimSpace(command.Email)) || command.FirstName != strings.TrimSpace(command.FirstName) || command.LastName != strings.TrimSpace(command.LastName) || command.EmailVerifiedAt.IsZero() || command.EmailVerifiedAt.After(time.Now()) {
		return nil, eventstore.ErrInvalidEvent
	}
	if _, err := bcrypt.Cost([]byte(command.PasswordHash)); err != nil {
		return nil, eventstore.ErrInvalidEvent
	}
	protector := protectedpii.FromEnv()
	query := registerUserEffectiveIdentityQuery(command.UserRegisteredID, protector.BlindIndex(UserRegisteredUsernameField, command.Username), protector.BlindIndex(UserRegisteredEmailField, command.Email))
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model := &registerUserContext{query: query, position: latest.ContextPosition}
	for _, resolved := range eventstore.EventsFromLatest(latest.Results) {
		value, err := DecodeUserRegisteredEvent(resolved.Event)
		if err != nil {
			return nil, err
		}
		if value.UserRegisteredID == command.UserRegisteredID {
			if value.UsernameHash != protector.BlindIndex(UserRegisteredUsernameField, command.Username) || value.EmailHash != protector.BlindIndex(UserRegisteredEmailField, command.Email) || value.FirstName.BlindIndex != protector.BlindIndex(UserRegisteredFirstNameField, command.FirstName) || value.LastName.BlindIndex != protector.BlindIndex(UserRegisteredLastNameField, command.LastName) || value.YearOfBirth != command.YearOfBirth || value.PasswordHash != command.PasswordHash || value.EmailVerifiedAt != formatEventTime(command.EmailVerifiedAt) {
				return nil, eventstore.ErrConflict
			}
			model.existing = true
		} else {
			return nil, ErrRegistrationIdentityUnavailable
		}
	}
	return model, nil
}
func registerUserEffectiveIdentityQuery(id, usernameHash, emailHash string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}, {Key: UserRegisteredIDField, Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}, {Key: UserRegisteredUsernameHashField, Value: usernameHash}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}, {Key: UserRegisteredEmailHashField, Value: emailHash}}},
	}}
}
