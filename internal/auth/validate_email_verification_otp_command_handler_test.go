package auth

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstoretest"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

func TestValidateOTPStoresBothIndependentObservations(t *testing.T) {
	store, user, otp := otpStore(t)
	reader := &otpObservationReader{EmbeddedOrisun: store}
	err := ValidateEmailVerificationOTPCommandHandler(t.Context(), ValidateEmailVerificationOTPCommand{User: views.User{UserRegisteredID: user}, Code: "123456"}, store, reader)
	if err != nil {
		t.Fatal(err)
	}
	if len(reader.observations) != 2 {
		t.Fatalf("reads: %d", len(reader.observations))
	}
	events, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: EmailVerificationOTPValidated}, {Key: ScopeEmailVerificationOTPGeneratedIDField, Value: otp}}}}})
	if err != nil || len(events) != 1 {
		t.Fatalf("validation events=%v error=%v", events, err)
	}
	stored, err := store.GetWriteContext(t.Context(), events[0].WriteID)
	if err != nil {
		t.Fatal(err)
	}
	if !equalObservations(stored.Consistency, reader.observations) {
		t.Fatalf("stored=%#v checked=%#v", stored.Consistency, reader.observations)
	}
	if _, exists := events[0].Event.Metadata["scope_event_ids"]; exists {
		t.Fatal("obsolete scope IDs persisted")
	}
}

func TestValidateOTPRejectsConcurrentReplacement(t *testing.T) {
	store, user, _ := otpStore(t)
	reader := &otpObservationReader{EmbeddedOrisun: store, afterFirst: func() {
		replacement := mustAuthEvent(NewEmailVerificationOTPGeneratedEvent(uuidv7.NewString(), "654321", time.Now().Add(time.Hour), user, nil))
		if _, err := store.SaveEventsV2(t.Context(), []eventstore.DomainEvent{replacement}, nil); err != nil {
			t.Fatal(err)
		}
	}}
	err := ValidateEmailVerificationOTPCommandHandler(t.Context(), ValidateEmailVerificationOTPCommand{User: views.User{UserRegisteredID: user}, Code: "123456"}, store, reader)
	if !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("got %v, want consistency conflict", err)
	}
}

func otpStore(t *testing.T) (*eventstore.EmbeddedOrisun, string, string) {
	t.Helper()
	user, otp := uuidv7.NewString(), uuidv7.NewString()
	store := eventstoretest.New(t,
		registeredAuthEvent(t, user),
		mustAuthEvent(NewEmailVerificationOTPGeneratedEvent(otp, "123456", time.Now().Add(time.Hour), user, nil)),
	)
	return store, user, otp
}

type otpObservationReader struct {
	*eventstore.EmbeddedOrisun
	observations []eventstore.ConsistencyObservation
	afterFirst   func()
}

func (r *otpObservationReader) GetLatestByCriteria(ctx context.Context, criteria []eventstore.Criterion) (eventstore.LatestByCriteriaResult, error) {
	result, err := r.EmbeddedOrisun.GetLatestByCriteria(ctx, criteria)
	r.observations = append(r.observations, eventstore.ConsistencyObservation{Query: eventstore.Query{Criteria: criteria}, Position: result.ContextPosition})
	if len(r.observations) == 1 && r.afterFirst != nil {
		r.afterFirst()
	}
	return result, err
}

func equalObservations(left, right []eventstore.ConsistencyObservation) bool {
	canonical := func(input []eventstore.ConsistencyObservation) string {
		type observation struct {
			Position eventstore.Position
			Criteria []string
		}
		result := make([]observation, len(input))
		for i, item := range input {
			result[i].Position = item.Position
			for _, criterion := range item.Query.Criteria {
				tags := map[string]string{}
				for _, tag := range criterion.Tags {
					tags[tag.Key] = tag.Value
				}
				encoded, _ := json.Marshal(tags)
				result[i].Criteria = append(result[i].Criteria, string(encoded))
			}
			sort.Strings(result[i].Criteria)
		}
		encoded, _ := json.Marshal(result)
		return string(encoded)
	}
	return canonical(left) == canonical(right)
}
