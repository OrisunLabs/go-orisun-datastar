package auth

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
	"golang.org/x/crypto/bcrypt"
)

func mustAuthEvent(event eventstore.DomainEvent, err error) eventstore.DomainEvent {
	if err != nil {
		panic(err)
	}
	return event
}
func testAuthHash(t *testing.T) string {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte("secret123"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	return string(hash)
}
func registeredAuthEvent(t *testing.T, id string) eventstore.DomainEvent {
	t.Helper()
	key, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	return mustAuthEvent(NewUserRegisteredEvent(id, "test-user", "test@example.com", "", "", 0, testAuthHash(t), key, nil))
}

func TestAuthContractsRejectMalformedFactsBeforeConsumerEffects(t *testing.T) {
	user, otp, request, deletion := uuidv7.NewString(), uuidv7.NewString(), uuidv7.NewString(), uuidv7.NewString()
	key, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	hash := testAuthHash(t)
	events := []eventstore.DomainEvent{
		registeredAuthEvent(t, user),
		mustAuthEvent(NewUserNameChangedEvent(uuidv7.NewString(), "Ada", now, user, key, nil)),
		mustAuthEvent(NewEmailVerificationOTPGeneratedEvent(otp, "123456", now, user, nil)),
		mustAuthEvent(NewEmailVerificationOTPValidatedEvent(uuidv7.NewString(), now, otp, user, nil)),
		mustAuthEvent(NewEmailVerificationOTPSentEvent(uuidv7.NewString(), now, otp, nil)),
		mustAuthEvent(NewPasswordResetRequestedEvent(request, "test@example.com", "token", now, user, key, nil)),
		mustAuthEvent(NewPasswordResetEmailSentEvent(uuidv7.NewString(), now, request, nil)),
		mustAuthEvent(NewPasswordResetCompletedEvent(uuidv7.NewString(), now, request, user, hash, nil)),
		mustAuthEvent(NewPasswordChangedEvent(uuidv7.NewString(), now, user, hash, nil)),
		mustAuthEvent(NewAccountDeletionRequestedEvent(deletion, now, user, user, nil)),
		mustAuthEvent(NewAccountDeletedEvent(uuidv7.NewString(), now, deletion, user, user, nil)),
		mustAuthEvent(NewLoginAttemptRecordedEvent(uuidv7.NewString(), now, "unknown@example.com", "127.0.0.1", "", false, nil)),
	}
	for _, event := range events {
		t.Run(event.EventType, func(t *testing.T) {
			if err := ValidateEvent(event); err != nil {
				t.Fatal(err)
			}
			if _, ok := event.Data["scope"]; ok {
				t.Fatal("singular scopes persisted")
			}
			if event.EventType == UserRegistered {
				if _, ok := event.Data["scopes"]; ok {
					t.Fatal("root has scopes")
				}
			}
			mutations := map[string]func(map[string]any){
				"unknown": func(data map[string]any) { data["extra"] = true },
				"identity mismatch": func(data map[string]any) {
					data[strings.ToLower(event.EventType[:1])+event.EventType[1:]+"Id"] = uuidv7.NewString()
				},
				"legacy scope": func(data map[string]any) { data["scope"] = map[string]any{} },
			}
			for field := range event.Data {
				mutations["missing "+field] = func(data map[string]any) { delete(data, field) }
				mutations["null "+field] = func(data map[string]any) { data[field] = nil }
			}
			for name, mutate := range mutations {
				t.Run(name, func(t *testing.T) {
					corrupt := event
					corrupt.Data = eventstore.MustData(event.Data)
					mutate(corrupt.Data)
					if err := ValidateEvent(corrupt); !errors.Is(err, eventstore.ErrInvalidEvent) {
						t.Fatalf("accepted corrupt contract: %v", err)
					}
					projector := &AuthUserProjectionEventHandler{}
					if err := projector.handle(t.Context(), eventstore.ResolvedEvent{Event: corrupt}); !errors.Is(err, eventstore.ErrInvalidEvent) {
						t.Fatalf("consumer accepted malformed fact: %v", err)
					}
				})
			}
		})
	}
}

func TestGenerateOTPRejectsCorruptRegisteredAuthority(t *testing.T) {
	user := uuidv7.NewString()
	root := registeredAuthEvent(t, user)
	root.Data["unexpected"] = true
	store := eventstoretest.New(t, root)
	_, err := GenerateEmailVerificationOTPCommandHandler(t.Context(), GenerateEmailVerificationOTPCommand{User: views.User{UserRegisteredID: user}}, store, store)
	if !errors.Is(err, eventstore.ErrInvalidEvent) {
		t.Fatalf("corrupt authority: %v", err)
	}
	events, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPGenerated}, {Key: ScopeUserRegisteredIDField, Value: user}}}}})
	if err != nil || len(events) != 0 {
		t.Fatalf("invalid OTP append: %v %v", events, err)
	}
}
