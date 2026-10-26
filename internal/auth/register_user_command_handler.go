package auth

import (
	"context"
	"errors"
	"strings"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"golang.org/x/crypto/bcrypt"
)

type RegisterUserCommand struct {
	Username    string
	Email       string
	Password    string
	FirstName   string
	LastName    string
	YearOfBirth int
	Metadata    CommandMetadata
}

type RegisterUserResult struct {
	UserRegisteredID string
	Username         string
	Email            string
	FirstName        string
	LastName         string
	PasswordHash     string
}

func RegisterUserCommandHandler(ctx context.Context, command RegisterUserCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, keys SubjectPiiKeyPort) (RegisterUserResult, error) {
	if err := commandlimits.Assert(command); err != nil {
		return RegisterUserResult{}, err
	}
	if len(command.Password) < 6 {
		return RegisterUserResult{}, errors.New("invalid registration input")
	}
	model, err := loadRegisterUserContext(ctx, command, retriever)
	if err != nil {
		return RegisterUserResult{}, err
	}
	if model.existingUsername || model.existingEmail {
		return RegisterUserResult{}, errors.New("user already exists")
	}
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(command.Password), bcrypt.DefaultCost)
	if err != nil {
		return RegisterUserResult{}, err
	}

	subjectKey, err := keys.GetOrCreateSubjectDataKey(ctx, model.userRegisteredID)
	if err != nil {
		return RegisterUserResult{}, err
	}
	event, err := NewUserRegisteredEvent(model.userRegisteredID, model.username, model.email, model.firstName, model.lastName, command.YearOfBirth, string(passwordHash), subjectKey, nil)
	if err != nil {
		return RegisterUserResult{}, err
	}
	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}}); err != nil {
		return RegisterUserResult{}, err
	}
	return RegisterUserResult{
		UserRegisteredID: model.userRegisteredID,
		Username:         model.username,
		Email:            model.email,
		FirstName:        model.firstName,
		LastName:         model.lastName,
		PasswordHash:     string(passwordHash),
	}, nil
}

type registerUserContext struct {
	existingUsername bool
	existingEmail    bool
	userRegisteredID string
	username         string
	usernameHash     string
	email            string
	emailHash        string
	firstName        string
	lastName         string
	position         eventstore.Position
	events           []eventstore.ResolvedEvent
	query            eventstore.Query
}

func loadRegisterUserContext(ctx context.Context, command RegisterUserCommand, retriever eventstore.LatestRetriever) (*registerUserContext, error) {
	username := strings.TrimSpace(command.Username)
	email := strings.ToLower(strings.TrimSpace(command.Email))
	if len(username) < 4 || email == "" {
		return nil, errors.New("invalid registration input")
	}

	protector := protectedpii.FromEnv()
	usernameHash := protector.BlindIndex(UserRegisteredUsernameField, username)
	emailHash := protector.BlindIndex(UserRegisteredEmailField, email)
	query := registerUserUserRegisteredByUsernameOrEmailQuery(usernameHash, emailHash)
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

	model := &registerUserContext{
		userRegisteredID: uuidv7.NewString(),
		username:         username,
		usernameHash:     usernameHash,
		email:            email,
		emailHash:        emailHash,
		firstName:        strings.TrimSpace(command.FirstName),
		lastName:         strings.TrimSpace(command.LastName),
		position:         latest.ContextPosition,
		events:           events,
		query:            query,
	}
	for _, event := range events {
		model.handle(event)
	}
	return model, nil
}

func (m *registerUserContext) handle(resolved eventstore.ResolvedEvent) {
	if resolved.Event.EventType == UserRegistered {
		usernameHash, _ := resolved.Event.Data[UserRegisteredUsernameHashField].(string)
		emailHash, _ := resolved.Event.Data[UserRegisteredEmailHashField].(string)
		if usernameHash == m.usernameHash {
			m.existingUsername = true
		}
		if emailHash == m.emailHash {
			m.existingEmail = true
		}
	}
}

func registerUserUserRegisteredByUsernameOrEmailQuery(usernameHash, emailHash string) eventstore.Query {
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
