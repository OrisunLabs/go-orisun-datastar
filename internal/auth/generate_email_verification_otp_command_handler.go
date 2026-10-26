package auth

import (
	"context"
	"errors"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

type GenerateEmailVerificationOTPCommand struct {
	UserRegistrationStartedID string
	Metadata                  CommandMetadata
}

type GenerateEmailVerificationOTPResult struct {
	EmailVerificationOTPGeneratedID string
	Code                            string
	ExpiresAt                       time.Time
	Skipped                         bool
}

func GenerateEmailVerificationOTPCommandHandler(ctx context.Context, command GenerateEmailVerificationOTPCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) (GenerateEmailVerificationOTPResult, error) {
	if err := commandlimits.Assert(command); err != nil {
		return GenerateEmailVerificationOTPResult{}, err
	}
	model, err := loadGenerateEmailVerificationOTPContext(ctx, command, retriever)
	if err != nil {
		return GenerateEmailVerificationOTPResult{}, err
	}
	if !model.registrationExists {
		return GenerateEmailVerificationOTPResult{}, errors.New("registration start event not found")
	}
	if model.emailValidated || model.latestOTPExpiresAt.After(time.Now()) {
		return GenerateEmailVerificationOTPResult{Skipped: true}, nil
	}

	code, err := numericCode(6)
	if err != nil {
		return GenerateEmailVerificationOTPResult{}, err
	}
	otpID := uuidv7.NewString()
	expiresAt := time.Now().Add(15 * time.Minute)
	event, err := NewEmailVerificationOTPGeneratedEvent(otpID, code, expiresAt, command.UserRegistrationStartedID, nil)
	if err != nil {
		return GenerateEmailVerificationOTPResult{}, err
	}
	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}}); err != nil {
		return GenerateEmailVerificationOTPResult{}, err
	}
	return GenerateEmailVerificationOTPResult{EmailVerificationOTPGeneratedID: otpID, Code: code, ExpiresAt: expiresAt}, nil
}

type generateEmailVerificationOTPContext struct {
	registrationExists bool
	emailValidated     bool
	latestOTPExpiresAt time.Time
	position           eventstore.Position
	events             []eventstore.ResolvedEvent
	query              eventstore.Query
}

func loadGenerateEmailVerificationOTPContext(ctx context.Context, command GenerateEmailVerificationOTPCommand, retriever eventstore.LatestRetriever) (*generateEmailVerificationOTPContext, error) {
	userQuery := generateEmailVerificationOTPUserRegistrationStartedQuery(command.UserRegistrationStartedID)
	stateQuery := generateEmailVerificationOTPEmailVerificationOTPStateQuery(command.UserRegistrationStartedID)
	query := generateEmailVerificationOTPCombineQueries(userQuery, stateQuery)

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
	model := &generateEmailVerificationOTPContext{position: latest.ContextPosition, events: events, query: query}
	for _, event := range events {
		model.handle(event)
	}
	return model, nil
}

func (m *generateEmailVerificationOTPContext) handle(resolved eventstore.ResolvedEvent) {
	switch resolved.Event.EventType {
	case UserRegistrationStarted:
		m.registrationExists = true
	case EmailVerificationOTPGenerated:
		expiresAt, _ := resolved.Event.Data[EmailVerificationOTPExpiresAtField].(string)
		parsed, err := time.Parse(time.RFC3339, expiresAt)
		if err == nil {
			m.latestOTPExpiresAt = parsed
		}
	case EmailVerificationOTPValidated:
		m.emailValidated = true
	}
}

func generateEmailVerificationOTPCombineQueries(queries ...eventstore.Query) eventstore.Query {
	combined := eventstore.Query{}
	for _, query := range queries {
		combined.Criteria = append(combined.Criteria, query.Criteria...)
	}
	return combined
}

func generateEmailVerificationOTPEmailVerificationOTPStateQuery(registrationID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPGenerated}, {Key: ScopeUserRegistrationStartedIDField, Value: registrationID}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPValidated}, {Key: ScopeUserRegistrationStartedIDField, Value: registrationID}}},
	}}
}

func generateEmailVerificationOTPUserRegistrationStartedQuery(registrationID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: UserRegistrationStarted},
		{Key: UserRegistrationStartedIDField, Value: registrationID},
	}}}}
}
