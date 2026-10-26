package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"

	"github.com/OrisunLabs/go-orisun-datastar/internal/email"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

type SendPasswordResetEmailCommand struct {
	PasswordResetRequestedID string
	Metadata                 CommandMetadata
}

type sendPasswordResetEmailContext struct {
	requestID   string
	email       string
	token       string
	expiresAt   string
	subjectKey  protectedpii.SubjectDataKey
	alreadySent bool
	position    eventstore.Position
	events      []eventstore.ResolvedEvent
	query       eventstore.Query
}

func SendPasswordResetEmailCommandHandler(ctx context.Context, command SendPasswordResetEmailCommand, saver eventstore.SaverV2, retriever eventstore.LatestRetriever, sender EmailSender, appURL string, keys SubjectPiiKeyPort) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadSendPasswordResetEmailContext(ctx, command, retriever, keys)
	if err != nil {
		return err
	}
	if model.alreadySent {
		return nil
	}
	if model.requestID == "" {
		return errors.New("password reset request event not found")
	}

	resetURL := fmt.Sprintf("%s/reset-password/%s", appURL, model.token)
	if err := sender.Send(ctx, email.Message{
		To:    model.email,
		Title: "Reset your password",
		Body:  fmt.Sprintf(`Click the link below to reset your password. This link expires at %s.<br><br><a href="%s">%s</a>`, model.expiresAt, resetURL, resetURL),
	}); err != nil {
		return err
	}

	id := uuidv7.NewString()
	sent, err := NewPasswordResetEmailSentEvent(id, time.Now(), command.PasswordResetRequestedID, nil)
	if err != nil {
		return err
	}
	_, err = eventstore.SaveCommandEventsV2(ctx, saver, command.Metadata, []eventstore.DomainEvent{sent}, []eventstore.ConsistencyObservation{{Query: model.query, Position: model.position}})
	return err
}

func (m *sendPasswordResetEmailContext) handle(resolved eventstore.ResolvedEvent) error {
	switch resolved.Event.EventType {
	case PasswordResetRequested:
		m.requestID, _ = resolved.Event.Data[PasswordResetRequestedIDField].(string)
		protector := protectedpii.FromEnv()
		decryptedemail, err := protectedpii.DecryptEventStringWithDataKey(protector, m.subjectKey, resolved.Event.Data, PasswordResetRequestedEmailField)
		if err != nil {
			return err
		}
		m.email = decryptedemail
		decryptedtoken, err := protectedpii.DecryptEventStringWithDataKey(protector, m.subjectKey, resolved.Event.Data, PasswordResetRequestedTokenField)
		if err != nil {
			return err
		}
		if protector.SensitiveBlindIndex(PasswordResetRequestedTokenField, decryptedtoken) != resolved.Event.Data[PasswordResetRequestedTokenHashField] {
			return eventstore.ErrInvalidEvent
		}
		m.token = decryptedtoken
		m.expiresAt, _ = resolved.Event.Data[PasswordResetRequestedExpiresAtField].(string)
	case PasswordResetEmailSent:
		m.alreadySent = true
	}
	return nil
}

func sendPasswordResetEmailCombineQueries(queries ...eventstore.Query) eventstore.Query {
	combined := eventstore.Query{}
	for _, query := range queries {
		combined.Criteria = append(combined.Criteria, query.Criteria...)
	}
	return combined
}

func sendPasswordResetEmailPasswordResetEmailSentQuery(requestID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: PasswordResetEmailSent},
		{Key: ScopePasswordResetRequestedIDField, Value: requestID},
	}}}}
}

func sendPasswordResetEmailPasswordResetRequestedQuery(requestID string) eventstore.Query {
	return eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{
		{Key: eventstore.EventTypeField, Value: PasswordResetRequested},
		{Key: PasswordResetRequestedIDField, Value: requestID},
	}}}}
}

func loadSendPasswordResetEmailContext(ctx context.Context, command SendPasswordResetEmailCommand, retriever eventstore.LatestRetriever, keys SubjectPiiKeyPort) (*sendPasswordResetEmailContext, error) {
	requestedQuery := sendPasswordResetEmailPasswordResetRequestedQuery(command.PasswordResetRequestedID)
	sentQuery := sendPasswordResetEmailPasswordResetEmailSentQuery(command.PasswordResetRequestedID)
	query := sendPasswordResetEmailCombineQueries(requestedQuery, sentQuery)
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
	model := &sendPasswordResetEmailContext{
		position: latest.ContextPosition,
		events:   events,
		query:    query,
	}
	for _, resolved := range model.events {
		if resolved.Event.EventType == PasswordResetRequested {
			userRegisteredID, _ := eventstore.Scopes(resolved.Event.Data)[ScopeUserRegisteredIDKey].(string)
			subjectKey, ok, err := keys.GetSubjectDataKey(ctx, userRegisteredID)
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, eventstore.ErrNotFound
			}
			model.subjectKey = subjectKey
			break
		}
	}
	for _, resolved := range model.events {
		if err := model.handle(resolved); err != nil {
			return nil, err
		}
	}

	return model, nil
}
