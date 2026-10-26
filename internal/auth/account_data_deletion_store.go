package auth

import (
	"context"
	"errors"

	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AccountDeletionStorage interface {
	DeleteObject(ctx context.Context, key string) error
	ObjectKeyFromPublicURL(url string) (string, bool)
}

type AccountDataDeletionStore struct {
	db      *pgxpool.Pool
	storage AccountDeletionStorage
}

func NewAccountDataDeletionStore(db *pgxpool.Pool, storage AccountDeletionStorage) *AccountDataDeletionStore {
	return &AccountDataDeletionStore{db: db, storage: storage}
}

func (s *AccountDataDeletionStore) DeleteAccountData(ctx context.Context, userRegisteredID string) error {
	if err := s.deleteKnownMedia(ctx, userRegisteredID); err != nil {
		return err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	queries := dbsql.New(tx)
	if err := queries.DeleteAuthVerificationsByRegisteredID(ctx, stringPtr(userRegisteredID)); err != nil {
		return err
	}
	if err := queries.DeleteAuthSessionsByRegisteredID(ctx, userRegisteredID); err != nil {
		return err
	}
	if err := queries.DeleteAuthAccountsByRegisteredID(ctx, userRegisteredID); err != nil {
		return err
	}
	if err := queries.DeleteProfileByRegisteredID(ctx, userRegisteredID); err != nil {
		return err
	}
	if err := queries.DeleteTodosByUserID(ctx, userRegisteredID); err != nil {
		return err
	}
	if err := queries.DeleteAuthUserByRegisteredID(ctx, userRegisteredID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *AccountDataDeletionStore) deleteKnownMedia(ctx context.Context, userRegisteredID string) error {
	if s.storage == nil {
		return nil
	}
	row, err := dbsql.New(s.db).ProfileUser(ctx, userRegisteredID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, url := range []string{row.Image, row.HeaderImageUrl} {
		if url == "" {
			continue
		}
		key, ok := s.storage.ObjectKeyFromPublicURL(url)
		if !ok {
			continue
		}
		if err := s.storage.DeleteObject(ctx, key); err != nil {
			return err
		}
	}
	return nil
}
