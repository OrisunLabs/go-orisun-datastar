package auth

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

var ErrRegistrationIdentityUnavailable = errors.New("username or email is already registered")

type RegisterUserCommand struct {
	UserRegisteredID string
	RegistrationDetails
	EmailVerifiedAt time.Time
	Metadata        CommandMetadata
}
type RegisterUserResult struct{ UserRegisteredID string }

// RegisterUserCommandHandler accepts complete ordinary domain input. It knows
// nothing about signup, OTPs, proposals or their history.
func RegisterUserCommandHandler(ctx context.Context, command RegisterUserCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, keys SubjectPiiKeyReader) (RegisterUserResult, error) {
	if err := commandlimits.Assert(command); err != nil {
		return RegisterUserResult{}, err
	}
	model, err := loadRegisterUserContext(ctx, command, retriever)
	if err != nil {
		return RegisterUserResult{}, err
	}
	result := RegisterUserResult{command.UserRegisteredID}
	if model.existing {
		return result, nil
	}
	if err := validateRegistrationPlaintext(ctx, command, keys); err != nil {
		return RegisterUserResult{}, err
	}
	event, err := newAcceptedRegistrationEvent(command)
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
	if _, err := newAcceptedRegistrationEvent(command); err != nil {
		return nil, err
	}
	query := registerUserEffectiveIdentityQuery(command.UserRegisteredID, command.UsernameHash, command.EmailHash)
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
			if value.UsernameHash != command.UsernameHash || value.EmailHash != command.EmailHash || value.FirstName.BlindIndex != command.FirstName.BlindIndex || value.LastName.BlindIndex != command.LastName.BlindIndex || value.YearOfBirth != command.YearOfBirth || value.PasswordHash != command.PasswordHash || value.EmailVerifiedAt != formatEventTime(command.EmailVerifiedAt) {
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

func newAcceptedRegistrationEvent(command RegisterUserCommand) (eventstore.DomainEvent, error) {
	if command.EmailVerifiedAt.IsZero() || command.EmailVerifiedAt.After(time.Now()) {
		return eventstore.DomainEvent{}, eventstore.ErrInvalidEvent
	}
	e := eventstore.DomainEvent{EventID: command.UserRegisteredID, EventType: UserRegistered, Data: eventstore.MustData(UserRegisteredEvent{UserRegisteredID: command.UserRegisteredID, RegisteredAt: formatEventTime(time.Now()), EmailVerifiedAt: formatEventTime(command.EmailVerifiedAt), RegistrationDetails: command.RegistrationDetails})}
	_, err := DecodeUserRegisteredEvent(e)
	return e, err
}

func validateRegistrationPlaintext(ctx context.Context, command RegisterUserCommand, keys SubjectPiiKeyReader) error {
	key, ok, err := keys.GetSubjectDataKey(ctx, command.UserRegisteredID)
	if err != nil {
		return err
	}
	if !ok {
		return eventstore.ErrNotFound
	}
	protector := protectedpii.FromEnv()
	fields := []string{UserRegisteredUsernameField, UserRegisteredEmailField, UserRegisteredFirstNameField, UserRegisteredLastNameField}
	values := []protectedpii.Value{command.Username, command.Email, command.FirstName, command.LastName}
	plain := make([]string, len(values))
	for i, v := range values {
		plain[i], err = protector.DecryptWithDataKey(v, key)
		if err != nil {
			return err
		}
		if plain[i] != strings.TrimSpace(plain[i]) || protector.BlindIndex(fields[i], plain[i]) != v.BlindIndex {
			return eventstore.ErrInvalidEvent
		}
	}
	if len(plain[0]) < 4 || plain[1] == "" || plain[1] != strings.ToLower(plain[1]) {
		return eventstore.ErrInvalidEvent
	}
	return nil
}
