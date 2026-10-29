package developmentseed

import (
	"context"
	"errors"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"golang.org/x/crypto/bcrypt"
)

func seedUser(ctx context.Context, s Store, keys auth.SubjectPiiKeyPort, spec userSpec) (User, error) {
	p := protectedpii.FromEnv()
	criteria := []eventstore.Criterion{}
	for _, kind := range []string{auth.UserRegistrationStarted, auth.UserRegistered} {
		criteria = append(criteria, criterion(kind, auth.UserRegisteredUsernameHashField, p.BlindIndex(auth.UserRegisteredUsernameField, spec.username)), criterion(kind, auth.UserRegisteredEmailHashField, p.BlindIndex(auth.UserRegisteredEmailField, spec.email)))
	}
	facts, err := latest(ctx, s, criteria...)
	if err != nil {
		return User{}, err
	}
	var start *eventstore.ResolvedEvent
	for i := range facts {
		step := "registration-start"
		if facts[i].Event.EventType == auth.UserRegistered {
			step = "registration-accept"
		}
		if !owned(facts[i].Event, spec.key, step) {
			return User{}, errors.New("username or email has different ownership")
		}
		if facts[i].Event.EventType == auth.UserRegistrationStarted {
			if start != nil {
				return User{}, errors.New("conflicting seeded identities")
			}
			start = &facts[i]
		}
	}
	if start == nil {
		if len(facts) != 0 {
			return User{}, errors.New("seed registration source missing")
		}
		result, err := auth.StartUserRegistrationCommandHandler(ctx, auth.StartUserRegistrationCommand{Username: spec.username, Email: spec.email, Password: DemoPassword, FirstName: spec.first, LastName: spec.last, YearOfBirth: 1990, Metadata: metadata(spec.key, "registration-start")}, s, s, keys)
		if err != nil {
			return User{}, err
		}
		fact, err := exact(ctx, s, auth.UserRegistrationStarted, result.UserRegistrationStartedID)
		if err != nil {
			return User{}, err
		}
		start = &fact
	}
	started, err := auth.DecodeUserRegistrationStartedEvent(start.Event)
	if err != nil {
		return User{}, err
	}
	if !owned(start.Event, spec.key, "registration-start") {
		return User{}, errors.New("foreign registration source")
	}
	if err := validateIdentity(ctx, keys, started.UserID, started.RegistrationDetails, spec); err != nil {
		return User{}, err
	}
	for _, fact := range facts {
		if fact.Event.EventType == auth.UserRegistered {
			registered, err := auth.DecodeUserRegisteredEvent(fact.Event)
			if err != nil {
				return User{}, err
			}
			if registered.UserRegisteredID != started.UserID {
				return User{}, errors.New("seed identity sources disagree")
			}
		}
	}
	u := User{ID: started.UserID, Username: spec.username, Email: spec.email, Name: spec.first + " " + spec.last, Bio: spec.bio}
	// Ownership is reconstructed from immutable facts, never login projections.
	criteria = []eventstore.Criterion{criterion(auth.UserRegistered, auth.UserRegisteredIDField, u.ID)}
	for _, kind := range []string{auth.PasswordChanged, auth.UserNameChanged, auth.AccountDeleted} {
		criteria = append(criteria, criterion(kind, auth.ScopeUserRegisteredIDField, u.ID))
	}
	for _, kind := range []string{auth.EmailVerificationOTPGenerated, auth.EmailVerificationOTPValidated, auth.UserRegistrationCompleted} {
		criteria = append(criteria, criterion(kind, "scopes.userRegistrationStartedId", started.UserRegistrationStartedID))
	}
	facts, err = latest(ctx, s, criteria...)
	if err != nil {
		return User{}, err
	}
	var generated *auth.EmailVerificationOTPGeneratedEvent
	var validated *auth.EmailVerificationOTPValidatedEvent
	var accepted *eventstore.ResolvedEvent
	for i := range facts {
		e := facts[i].Event
		switch e.EventType {
		case auth.PasswordChanged, auth.UserNameChanged, auth.AccountDeleted:
			return User{}, errors.New("seeded user was changed or deleted; refusing to restore it")
		case auth.EmailVerificationOTPGenerated:
			v, err := auth.DecodeEmailVerificationOTPGeneratedEvent(e)
			if err != nil {
				return User{}, err
			}
			if !owned(e, spec.key, "registration-otp") {
				return User{}, errors.New("foreign OTP authority")
			}
			generated = &v
		case auth.EmailVerificationOTPValidated:
			v, err := auth.DecodeEmailVerificationOTPValidatedEvent(e)
			if err != nil {
				return User{}, err
			}
			if !owned(e, spec.key, "registration-validate") {
				return User{}, errors.New("foreign verification authority")
			}
			validated = &v
		case auth.UserRegistered:
			accepted = &facts[i]
		case auth.UserRegistrationCompleted:
			if _, err := auth.DecodeUserRegistrationCompletedEvent(e); err != nil {
				return User{}, err
			}
			if !owned(e, spec.key, "registration-complete") {
				return User{}, errors.New("foreign registration receipt")
			}
		default:
			return User{}, eventstore.ErrInvalidEvent
		}
	}
	if validated == nil {
		next, err := auth.GenerateEmailVerificationOTPCommandHandler(ctx, auth.GenerateEmailVerificationOTPCommand{UserRegistrationStartedID: started.UserRegistrationStartedID, Metadata: metadata(spec.key, "registration-otp")}, s, s)
		if err != nil {
			return User{}, err
		}
		code := next.Code
		if next.Skipped {
			if generated == nil {
				return User{}, eventstore.ErrNotFound
			}
			code = generated.OTPCode
		}
		if err := auth.ValidateEmailVerificationOTPCommandHandler(ctx, auth.ValidateEmailVerificationOTPCommand{UserRegistrationStartedID: started.UserRegistrationStartedID, Code: code, Metadata: metadata(spec.key, "registration-validate")}, s, s); err != nil {
			return User{}, err
		}
		fs, err := latest(ctx, s, criterion(auth.EmailVerificationOTPValidated, "scopes.userRegistrationStartedId", started.UserRegistrationStartedID))
		if err != nil {
			return User{}, err
		}
		if len(fs) != 1 {
			return User{}, eventstore.ErrNotFound
		}
		v, err := auth.DecodeEmailVerificationOTPValidatedEvent(fs[0].Event)
		if err != nil {
			return User{}, err
		}
		validated = &v
	}
	at, err := time.Parse(time.RFC3339Nano, validated.ValidatedAt)
	if err != nil {
		return User{}, err
	}
	if accepted != nil {
		if !owned(accepted.Event, spec.key, "registration-accept") {
			return User{}, errors.New("foreign accepted identity")
		}
		v, err := auth.DecodeUserRegisteredEvent(accepted.Event)
		if err != nil {
			return User{}, err
		}
		if v.UserRegisteredID != u.ID || v.PasswordHash != started.PasswordHash || v.EmailVerifiedAt != validated.ValidatedAt {
			return User{}, errors.New("accepted seed identity terms differ")
		}
		if err := validateIdentity(ctx, keys, u.ID, v.RegistrationDetails, spec); err != nil {
			return User{}, err
		}
	}
	if _, err := auth.RegisterUserCommandHandler(ctx, auth.RegisterUserCommand{UserRegisteredID: u.ID, RegistrationDetails: started.RegistrationDetails, EmailVerifiedAt: at, Metadata: metadata(spec.key, "registration-accept")}, s, s, keys); err != nil {
		return User{}, err
	}
	if err := auth.CompleteUserRegistrationCommandHandler(ctx, auth.CompleteUserRegistrationCommand{UserRegisteredID: u.ID, Metadata: metadata(spec.key, "registration-complete")}, s, s); err != nil {
		return User{}, err
	}
	return u, nil
}
func validateIdentity(ctx context.Context, keys auth.SubjectPiiKeyReader, id string, v auth.RegistrationDetails, spec userSpec) error {
	key, ok, err := keys.GetSubjectDataKey(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("seed user key is missing; refusing to recreate it")
	}
	p := protectedpii.FromEnv()
	for _, field := range []struct {
		name, want string
		value      protectedpii.Value
	}{{auth.UserRegisteredUsernameField, spec.username, v.Username}, {auth.UserRegisteredEmailField, spec.email, v.Email}, {auth.UserRegisteredFirstNameField, spec.first, v.FirstName}, {auth.UserRegisteredLastNameField, spec.last, v.LastName}} {
		text, err := p.DecryptWithDataKey(field.value, key)
		if err != nil {
			return err
		}
		if text != field.want || field.value.BlindIndex != p.BlindIndex(field.name, field.want) {
			return errors.New("seed identity terms differ")
		}
	}
	if v.YearOfBirth != 1990 || bcrypt.CompareHashAndPassword([]byte(v.PasswordHash), []byte(DemoPassword)) != nil {
		return errors.New("seed identity/password terms differ")
	}
	return nil
}
