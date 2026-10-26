package auth

import (
	"context"
	"log/slog"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

const AuthUserProjectionEventHandlerName = "auth_user_projection_event_handler"

type AuthUserProjectionEventHandler struct {
	global     *eventstore.GlobalEventHandler
	repository ProjectionRepository
	keys       SubjectPiiKeyReader
	source     ProfileSource
	publisher  UserChangePublisher
}

func NewAuthUserProjectionEventHandler(
	subscriber eventstore.Subscriber,
	checkpointer eventstore.Checkpointer,
	repository ProjectionRepository,
	keys SubjectPiiKeyReader,
	source ProfileSource,
	publisher UserChangePublisher,
	logger *slog.Logger) (*AuthUserProjectionEventHandler, error) {
	handler := &AuthUserProjectionEventHandler{repository: repository, keys: keys, source: source, publisher: publisher}
	global, err := eventstore.NewGlobalEventHandler(eventstore.GlobalEventHandlerConfig{
		Subscriber:      subscriber,
		Checkpointer:    checkpointer,
		Name:            AuthUserProjectionEventHandlerName,
		Query:           authUserProjectionEventHandlerQuery(source),
		Logger:          logger,
		MaxEventRetries: -1,
		HandleEvent:     handler.handle,
	})
	if err != nil {
		return nil, err
	}
	handler.global = global
	return handler, nil
}

func (h *AuthUserProjectionEventHandler) StartSubscribing(ctx context.Context) error {
	return h.global.StartSubscribing(ctx)
}

func (h *AuthUserProjectionEventHandler) StopSubscribing() {
	h.global.StopSubscribing()
}

func (h *AuthUserProjectionEventHandler) handle(ctx context.Context, resolved eventstore.ResolvedEvent) error {
	return ProjectBatch(ctx, []eventstore.ResolvedEvent{resolved}, h.repository, h.keys, h.source, h.publisher)
}

func authUserProjectionEventHandlerQuery(source ProfileSource) eventstore.Query {
	query := eventstore.Query{Criteria: []eventstore.Criterion{
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserRegistered}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: UserNameChanged}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPGenerated}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPValidated}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetRequested}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetCompleted}}},
		{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordChanged}}},
	}}
	for _, kind := range source.EventTypes {
		query.Criteria = append(query.Criteria, eventstore.Criterion{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: kind}}})
	}
	return query
}

func stringValue(value any) string {
	v, _ := value.(string)
	return v
}
