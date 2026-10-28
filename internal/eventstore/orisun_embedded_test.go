package eventstore

import (
	"reflect"
	"testing"
)

func TestEnvelopeSelectorsMappedAcrossReadAndWriteQueries(t *testing.T) {
	query := Query{Criteria: []Criterion{{Tags: []Tag{
		{Key: EventTypeField, Value: "Created"},
		{Key: "eventId", Value: "id"},
		{Key: "scopes.todoCreatedId", Value: "user"},
	}}}}
	want := []string{"__eventType", "__eventId", "scopes.todoCreatedId"}
	for i, key := range want {
		if got := toOrisunQuery(query).Criteria[0].Tags[i].Key; got != key {
			t.Fatalf("write query selector: got %q, want %q", got, key)
		}
		if got := toOrisunReadCriteria(query.Criteria)[0].Tags[i].Key; got != key {
			t.Fatalf("read selector: got %q, want %q", got, key)
		}
		if got := toOrisunCoreQuery(query).Criteria[0].Tags[i].Key; got != key {
			t.Fatalf("subscription selector: got %q, want %q", got, key)
		}
	}
}

func TestLatestResultsDeduplicateOnlyFullStoredPosition(t *testing.T) {
	first := ResolvedEvent{Position: Position{Commit: 1, Prepare: 1}, Event: DomainEvent{EventID: "same-id"}}
	second := ResolvedEvent{Position: Position{Commit: 1, Prepare: 2}, Event: DomainEvent{EventID: "same-id"}}
	got := EventsFromLatest([]LatestCriterionResult{{Event: &second}, {Event: &first}, {Event: &first}})
	if !reflect.DeepEqual(got, []ResolvedEvent{first, second}) {
		t.Fatalf("occurrences: %#v", got)
	}
}
