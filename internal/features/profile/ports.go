package profile

import (
	"context"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

type ObjectStore interface {
	PutObject(ctx context.Context, key string, data []byte, contentType string) error
	PublicURL(key string) string
}

// SubjectDataKeyReader exposes only the PII-key capability profile consumes.
type SubjectDataKeyReader interface {
	GetSubjectDataKey(context.Context, string) (protectedpii.SubjectDataKey, bool, error)
}
