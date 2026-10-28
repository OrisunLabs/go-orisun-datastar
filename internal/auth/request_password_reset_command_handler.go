package auth

import (
	"context"
	"strings"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
)

type RequestPasswordResetCommand struct {
	EmailAddress string
	Metadata     CommandMetadata
}
type RequestPasswordResetResult struct {
	PasswordResetRequestedID, Token string
	ExpiresAt                       time.Time
}

func RequestPasswordResetCommandHandler(ctx context.Context, command RequestPasswordResetCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, keys SubjectPiiKeyReader) (RequestPasswordResetResult, error) {
	if err := commandlimits.Assert(command); err != nil {
		return RequestPasswordResetResult{}, err
	}
	model, err := loadRequestPasswordResetContext(ctx, command, retriever, keys)
	if err != nil {
		return RequestPasswordResetResult{}, err
	}
	if !model.eligible {
		return RequestPasswordResetResult{}, nil
	}
	token, err := randomToken(32)
	if err != nil {
		return RequestPasswordResetResult{}, err
	}
	requestID := uuidv7.NewString()
	expiresAt := time.Now().Add(30 * time.Minute)
	event, err := NewPasswordResetRequestedEvent(requestID, model.email, token, expiresAt, model.userID, model.subjectKey, nil)
	if err != nil {
		return RequestPasswordResetResult{}, err
	}
	if _, err := eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{event}, model.observations); err != nil {
		return RequestPasswordResetResult{}, err
	}
	return RequestPasswordResetResult{PasswordResetRequestedID: requestID, Token: token, ExpiresAt: expiresAt}, nil
}

type requestPasswordResetContext struct {
	eligible      bool
	userID, email string
	subjectKey    protectedpii.SubjectDataKey
	observations  []eventstore.ConsistencyObservation
}

func loadRequestPasswordResetContext(ctx context.Context, command RequestPasswordResetCommand, retriever eventstore.LatestRetriever, keys SubjectPiiKeyReader) (*requestPasswordResetContext, error) {
	email := strings.ToLower(strings.TrimSpace(command.EmailAddress))
	model := &requestPasswordResetContext{}
	if email == "" {
		return model, nil
	}
	protector := protectedpii.FromEnv()
	emailHash := protector.BlindIndex(UserRegisteredEmailField, email)
	query := requestPasswordResetRegistrationQuery(emailHash)
	latest, err := retriever.GetLatestByCriteria(ctx, query.Criteria)
	if err != nil {
		return nil, err
	}
	model.observations = []eventstore.ConsistencyObservation{{Query: query, Position: latest.ContextPosition}}
	facts := eventstore.EventsFromLatest(latest.Results)
	if len(facts) == 0 {
		return model, nil
	}
	if len(facts) != 1 {
		return nil, eventstore.ErrInvalidEvent
	}
	registered, err := DecodeUserRegisteredEvent(facts[0].Event)
	if err != nil {
		return nil, err
	}
	if registered.EmailHash != emailHash {
		return nil, eventstore.ErrInvalidEvent
	}
	model.userID = registered.UserRegisteredID
	// Only the accepted identity discovered above determines this second query.
	deletionQuery := requestPasswordResetDeletionQuery(model.userID)
	deletion, err := retriever.GetLatestByCriteria(ctx, deletionQuery.Criteria)
	if err != nil {
		return nil, err
	}
	model.observations = append(model.observations, eventstore.ConsistencyObservation{Query: deletionQuery, Position: deletion.ContextPosition})
	deleted := false
	for _, e := range eventstore.EventsFromLatest(deletion.Results) {
		v, err := DecodeAccountDeletedEvent(e.Event)
		if err != nil {
			return nil, err
		}
		if v.Scopes.UserRegisteredID != model.userID || !e.Position.After(facts[0].Position) {
			return nil, eventstore.ErrInvalidEvent
		}
		deleted = true
	}
	if deleted {
		return model, nil
	}
	key, ok, err := keys.GetSubjectDataKey(ctx, model.userID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, eventstore.ErrNotFound
	}
	recipient, err := protector.DecryptWithDataKey(registered.Email, key)
	if err != nil {
		return nil, err
	}
	if recipient != email || protector.BlindIndex(UserRegisteredEmailField, recipient) != registered.EmailHash {
		return nil, eventstore.ErrInvalidEvent
	}
	model.email = recipient
	model.subjectKey = key
	model.eligible = true
	return model, nil
}
func requestPasswordResetRegistrationQuery(emailHash string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}, {Key: UserRegisteredEmailHashField, Value: emailHash}}}}}
}
func requestPasswordResetDeletionQuery(userID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: AccountDeleted}, {Key: ScopeUserRegisteredIDField, Value: userID}}}}}
}
