package eventstore

import (
	"context"
	"strings"

	orisunapi "github.com/OrisunLabs/Orisun/orisun"
)

type BoundaryIndexDefinition struct {
	Name       string
	Fields     []string
	EventTypes []string
}

func (s *EmbeddedOrisun) EnsureBoundaryIndexes(ctx context.Context, definitions []BoundaryIndexDefinition) error {
	for _, definition := range definitions {
		fields := make([]orisunapi.BoundaryIndexField, 0, len(definition.Fields))
		for _, field := range definition.Fields {
			fields = append(fields, orisunapi.BoundaryIndexField{JsonKey: toOrisunQueryKey(field), ValueType: "text"})
		}

		conditions := make([]orisunapi.BoundaryIndexCondition, 0, len(definition.EventTypes))
		for _, eventType := range definition.EventTypes {
			conditions = append(conditions, orisunapi.BoundaryIndexCondition{Key: toOrisunQueryKey(EventTypeField), Operator: "=", Value: eventType})
		}

		combinator := orisunapi.IndexCombinatorAND
		if len(conditions) > 1 {
			combinator = orisunapi.IndexCombinatorOR
		}
		if err := s.indexManager.CreateBoundaryIndex(ctx, s.boundary, definition.Name, fields, conditions, combinator); err != nil && !isDuplicateIndexError(err) {
			return err
		}
	}
	return nil
}

func isDuplicateIndexError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "already exists") || strings.Contains(message, "duplicate") || strings.Contains(message, "exists")
}
