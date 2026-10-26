package auth

import (
	"context"

	"github.com/OrisunLabs/go-orisun-datastar/internal/email"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
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

type PasswordCredentialReader interface {
	UserByEmailWithPassword(ctx context.Context, emailAddress string) (views.User, string, error)
}

type AuthUserByIDReader interface {
	UserByIDOrRegisteredID(ctx context.Context, id string) (views.User, error)
}
