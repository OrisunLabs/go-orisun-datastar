package auth

import (
	"context"

	"github.com/OrisunLabs/go-orisun-datastar/internal/email"
)

type EmailSender interface {
	Send(ctx context.Context, message email.Message) error
}

type AccountDataDeletionPort interface {
	DeleteAccountData(ctx context.Context, userRegisteredID string) error
}

type AccountPiiKeyPort interface {
	DestroySubjectKey(ctx context.Context, userRegisteredID string) error
}
