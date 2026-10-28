package auth

import (
	"context"
	"database/sql"
	"errors"

	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

type SubjectPiiKeyPort interface {
	GetOrCreateSubjectDataKey(ctx context.Context, subjectID string) (protectedpii.SubjectDataKey, error)
	GetSubjectDataKey(ctx context.Context, subjectID string) (protectedpii.SubjectDataKey, bool, error)
	DestroySubjectKey(ctx context.Context, subjectID string) error
}

type SubjectPiiKeyStore struct {
	queries   *dbsql.Queries
	protector *protectedpii.Protector
}

func NewSubjectPiiKeyStore(db *sql.DB, protector *protectedpii.Protector) *SubjectPiiKeyStore {
	return &SubjectPiiKeyStore{queries: dbsql.New(db), protector: protector}
}

func (s *SubjectPiiKeyStore) GetOrCreateSubjectDataKey(ctx context.Context, subjectID string) (protectedpii.SubjectDataKey, error) {
	if key, ok, err := s.GetSubjectDataKey(ctx, subjectID); err != nil || ok {
		return key, err
	}
	generated, err := protectedpii.GenerateSubjectDataKey()
	if err != nil {
		return protectedpii.SubjectDataKey{}, err
	}
	protected, err := s.protector.ProtectSubjectDataKey(generated)
	if err != nil {
		return protectedpii.SubjectDataKey{}, err
	}
	if err := s.queries.CreateSubjectPiiKey(ctx, dbsql.CreateSubjectPiiKeyParams{
		SubjectID:        subjectID,
		EncryptedDataKey: protected.Ciphertext,
		EncryptionNonce:  protected.Nonce,
		KeyVersion:       protected.KeyID,
	}); err != nil {
		return protectedpii.SubjectDataKey{}, err
	}
	if key, ok, err := s.GetSubjectDataKey(ctx, subjectID); err != nil || ok {
		return key, err
	}
	return generated, nil
}

func (s *SubjectPiiKeyStore) GetSubjectDataKey(ctx context.Context, subjectID string) (protectedpii.SubjectDataKey, bool, error) {
	row, err := s.queries.SubjectPiiKey(ctx, subjectID)
	if errors.Is(err, sql.ErrNoRows) {
		return protectedpii.SubjectDataKey{}, false, nil
	}
	if err != nil {
		return protectedpii.SubjectDataKey{}, false, err
	}
	key, err := s.protector.UnprotectSubjectDataKey(protectedpii.Value{
		Version:    1,
		KeyID:      row.KeyVersion,
		Nonce:      row.EncryptionNonce,
		Ciphertext: row.EncryptedDataKey,
	})
	if err != nil {
		return protectedpii.SubjectDataKey{}, false, err
	}
	return key, true, nil
}

func (s *SubjectPiiKeyStore) DestroySubjectKey(ctx context.Context, subjectID string) error {
	return s.queries.DeleteSubjectPiiKey(ctx, subjectID)
}
