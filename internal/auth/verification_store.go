package auth

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

type VerificationStore struct {
	queries *dbsql.Queries
}

func NewVerificationStore(db *pgxpool.Pool) *VerificationStore {
	return &VerificationStore{queries: dbsql.New(db)}
}

func (s *VerificationStore) PasswordResetByToken(ctx context.Context, token string) (PasswordResetVerification, error) {
	verification, err := s.queries.PasswordResetVerificationByToken(ctx, verificationValueHash("password-reset-token", token))
	if err != nil {
		return PasswordResetVerification{}, errors.New("invalid or expired reset token")
	}
	return PasswordResetVerification{
		ID:     verification.ID,
		UserID: strings.TrimPrefix(verification.Identifier, "password-reset:"),
	}, nil
}

func verificationValueHash(field, value string) string {
	return protectedpii.FromEnv().SensitiveBlindIndex(field, value)
}

type PasswordResetVerification struct {
	ID     string
	UserID string
}

func pgTime(value time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: value, Valid: true}
}
