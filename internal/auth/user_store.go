package auth

import (
	"context"
	"database/sql"

	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

type AuthUserStore struct {
	queries *dbsql.Queries
}

func NewAuthUserStore(db *sql.DB) *AuthUserStore {
	return &AuthUserStore{queries: dbsql.New(db)}
}

func (s *AuthUserStore) UserBySessionToken(ctx context.Context, token string) (views.User, error) {
	row, err := s.queries.UserBySessionToken(ctx, token)
	if err != nil {
		return views.User{}, err
	}
	return userFromSessionRow(row)
}

func (s *AuthUserStore) UserByRegisteredID(ctx context.Context, userRegisteredID string) (views.User, error) {
	row, err := s.queries.UserByRegisteredID(ctx, userRegisteredID)
	if err != nil {
		return views.User{}, err
	}
	return userFromRegisteredRow(row)
}

func (s *AuthUserStore) UserByIDOrRegisteredID(ctx context.Context, id string) (views.User, error) {
	row, err := s.queries.UserByIDOrRegisteredID(ctx, id)
	if err != nil {
		return views.User{}, err
	}
	return userFromIDOrRegisteredRow(row)
}

func (s *AuthUserStore) UserByEmailWithPassword(ctx context.Context, emailAddress string) (views.User, string, error) {
	row, err := s.queries.UserByEmailWithPassword(ctx, emailAddress)
	if err != nil {
		return views.User{}, "", err
	}
	return userFromEmailWithPasswordRow(row)
}

// RegistrationReady checks the login projection, including its credential row.
func (s *AuthUserStore) RegistrationReady(ctx context.Context, userRegisteredID string) (bool, error) {
	return s.queries.UserRegistrationReady(ctx, userRegisteredID)
}

func (s *AuthUserStore) PasswordResetReady(ctx context.Context, id string, position eventstore.Position) (bool, error) {
	return s.queries.PasswordResetReady(ctx, dbsql.PasswordResetReadyParams{UserRegisteredID: id, CommitPosition: &position.Commit, PreparePosition: &position.Prepare})
}
