package auth

import (
	"strings"
	"testing"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

func TestSendPasswordResetEmailRetainsFailureRetryAndContext(t *testing.T) {
	store, user, keys := emailCommandFixture(t)
	requested, err := RequestPasswordResetCommandHandler(t.Context(), RequestPasswordResetCommand{EmailAddress: user.Email}, store, store, keys)
	if err != nil {
		t.Fatal(err)
	}
	command := SendPasswordResetEmailCommand{PasswordResetRequestedID: requested.PasswordResetRequestedID}
	sender := &commandTestEmailSender{fail: true}
	if err := SendPasswordResetEmailCommandHandler(t.Context(), command, store, store, sender, "https://app.example.com", keys); err == nil {
		t.Fatal("email failure ignored")
	}
	query := eventstore.Query{Criteria: []eventstore.Criterion{{Tags: []eventstore.Tag{{Key: eventstore.EventTypeField, Value: PasswordResetEmailSent}, {Key: ScopePasswordResetRequestedIDField, Value: requested.PasswordResetRequestedID}}}}}
	events, err := store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, query)
	if err != nil || len(events) != 0 {
		t.Fatal("failed email recorded as sent")
	}
	sender.fail = false
	for range 2 {
		if err := SendPasswordResetEmailCommandHandler(t.Context(), command, store, store, sender, "https://app.example.com", keys); err != nil {
			t.Fatal(err)
		}
	}
	if len(sender.messages) != 1 || sender.messages[0].To != user.Email || !strings.Contains(sender.messages[0].Body, requested.Token) {
		t.Fatal("reset email redelivery or decoded context changed")
	}
	events, err = store.GetEvents(t.Context(), eventstore.NoEventPosition, 100, eventstore.Forward, query)
	if err != nil || len(events) != 1 {
		t.Fatalf("sent outcomes=%d error=%v", len(events), err)
	}
	stored, err := store.GetWriteContext(t.Context(), events[0].WriteID)
	if err != nil || len(stored.Consistency) != 1 || len(stored.Consistency[0].Query.Criteria) != 2 {
		t.Fatalf("reset observations=%#v error=%v", stored, err)
	}
}
