package auth

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

type VerificationStore struct {
	queries *dbsql.Queries
}

func NewVerificationStore(db *sql.DB) *VerificationStore {
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

func sqliteTime(value time.Time) sql.NullTime {
	return sql.NullTime{Time: value, Valid: true}
}
