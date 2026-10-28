package auth

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"golang.org/x/crypto/bcrypt"
)

type StartUserRegistrationCommand struct {
	Username    string
	Email       string
	Password    string
	FirstName   string
	LastName    string
	YearOfBirth int
	Metadata    CommandMetadata
}

type StartUserRegistrationResult struct{ UserRegistrationStartedID, UserID string }

func StartUserRegistrationCommandHandler(ctx context.Context, command StartUserRegistrationCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, keys SubjectPiiKeyPort) (StartUserRegistrationResult, error) {
	if err := commandlimits.Assert(command); err != nil {
		return StartUserRegistrationResult{}, err
	}
	if len(command.Password) < 6 {
		return StartUserRegistrationResult{}, errors.New("invalid registration input")
	}
	model, err := loadStartUserRegistrationContext(ctx, command, retriever)
	if err != nil {
		return StartUserRegistrationResult{}, err
	}
	if model.existingUsername || model.existingEmail {
		return StartUserRegistrationResult{}, errors.New("user already exists")
	}
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(command.Password), bcrypt.DefaultCost)
	if err != nil {
		return StartUserRegistrationResult{}, err
	}

	subjectKey, err := keys.GetOrCreateSubjectDataKey(ctx, model.userID)
	if err != nil {
		return StartUserRegistrationResult{}, err
	}
	event, err := NewUserRegistrationStartedEvent(model.registrationID, model.userID, model.username, model.email, model.firstName, model.lastName, command.YearOfBirth, string(passwordHash), time.Now(), subjectKey, nil)
	if err != nil {
		return StartUserRegistrationResult{}, err
	}
	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}}); err != nil {
		return StartUserRegistrationResult{}, err
	}
	return StartUserRegistrationResult{UserRegistrationStartedID: model.registrationID, UserID: model.userID}, nil
}

type startUserRegistrationContext struct {
	existingUsername bool
	existingEmail    bool
	userID           string
	registrationID   string
	username         string
	email            string
	firstName        string
	lastName         string
	position         eventstore.Position
	query            eventstore.Query
}

func loadStartUserRegistrationContext(ctx context.Context, command StartUserRegistrationCommand, retriever eventstore.LatestRetriever) (*startUserRegistrationContext, error) {
	username := strings.TrimSpace(command.Username)
	email := strings.ToLower(strings.TrimSpace(command.Email))
	if len(username) < 4 || email == "" {
		return nil, errors.New("invalid registration input")
	}

	protector := protectedpii.FromEnv()
	usernameHash := protector.BlindIndex(UserRegisteredUsernameField, username)
	emailHash := protector.BlindIndex(UserRegisteredEmailField, email)
	registrationID, userID := uuidv7.NewString(), uuidv7.NewString()
	query := startUserRegistrationQuery(usernameHash, emailHash, registrationID, userID)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model := &startUserRegistrationContext{
		userID:         userID,
		registrationID: registrationID,
		username:       username,
		email:          email,
		firstName:      strings.TrimSpace(command.FirstName),
		lastName:       strings.TrimSpace(command.LastName),
		position:       latest.ContextPosition,
		query:          query,
	}
	for _, e := range eventstore.EventsFromLatest(latest.Results) {
		switch e.Event.EventType {
		case UserRegistered:
			v, err := DecodeUserRegisteredEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.UserRegisteredID == userID {
				return nil, eventstore.ErrConflict
			}
			if v.UsernameHash == usernameHash {
				model.existingUsername = true
			}
			if v.EmailHash == emailHash {
				model.existingEmail = true
			}
			if v.UsernameHash != usernameHash && v.EmailHash != emailHash {
				return nil, eventstore.ErrInvalidEvent
			}
		case UserRegistrationStarted:
			v, err := DecodeUserRegistrationStartedEvent(e.Event)
			if err != nil {
				return nil, err
			}
			if v.UserRegistrationStartedID == registrationID || v.UserID == userID {
				return nil, eventstore.ErrConflict
			}
			return nil, eventstore.ErrInvalidEvent
		default:
			return nil, eventstore.ErrInvalidEvent
		}
	}
	return model, nil
}

func startUserRegistrationQuery(usernameHash, emailHash, registrationID, userID string) eventstore.Query {
	query := startUserRegistrationUserRegisteredByUsernameOrEmailQuery(usernameHash, emailHash)
	collision := startUserRegistrationCollisionQuery(registrationID, userID)
	query.Criteria = append(query.Criteria, collision.Criteria...)
	return query
}

func startUserRegistrationUserRegisteredByUsernameOrEmailQuery(usernameHash, emailHash string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{
			{Key: eventstore.EventTypeField, Value: UserRegistered},
			{Key: UserRegisteredUsernameHashField, Value: usernameHash},
		}},
		{Tags: []eventstore.Tag{
			{Key: eventstore.EventTypeField, Value: UserRegistered},
			{Key: UserRegisteredEmailHashField, Value: emailHash},
		}},
	}}
}

func startUserRegistrationCollisionQuery(id, userID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationStarted}, {Key: "eventId", Value: id}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}, {Key: "eventId", Value: userID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistrationStarted}, {Key: "userId", Value: userID}}},
	}}
}
