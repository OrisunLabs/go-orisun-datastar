package auth

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"math/big"
	"strings"

	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

func userFromSessionRow(row dbsql.UserBySessionTokenRow) (views.User, error) {
	return views.User{
		ID:               row.ID,
		UserRegisteredID: row.UserRegisteredID,
		Name:             row.Name,
		Username:         row.Username,
		Email:            row.Email,
		EmailVerified:    row.EmailVerified,
		Image:            row.Image,
		Bio:              row.Bio,
		HeaderImageURL:   row.HeaderImageUrl,
	}, nil
}

func userFromRegisteredRow(row dbsql.UserByRegisteredIDRow) (views.User, error) {
	return views.User{
		ID:               row.ID,
		UserRegisteredID: row.UserRegisteredID,
		Name:             row.Name,
		Username:         row.Username,
		Email:            row.Email,
		EmailVerified:    row.EmailVerified,
		Image:            row.Image,
		Bio:              row.Bio,
		HeaderImageURL:   row.HeaderImageUrl,
	}, nil
}

func userFromIDOrRegisteredRow(row dbsql.UserByIDOrRegisteredIDRow) (views.User, error) {
	return views.User{
		ID:               row.ID,
		UserRegisteredID: row.UserRegisteredID,
		Name:             row.Name,
		Username:         row.Username,
		Email:            row.Email,
		EmailVerified:    row.EmailVerified,
		Image:            row.Image,
		Bio:              row.Bio,
		HeaderImageURL:   row.HeaderImageUrl,
	}, nil
}

func userFromEmailWithPasswordRow(row dbsql.UserByEmailWithPasswordRow) (views.User, string, error) {
	if row.Password == nil {
		return views.User{}, "", sql.ErrNoRows
	}
	return views.User{
		ID:               row.ID,
		UserRegisteredID: row.UserRegisteredID,
		Name:             row.Name,
		Username:         row.Username,
		Email:            row.Email,
		EmailVerified:    row.EmailVerified,
		Image:            row.Image,
		Bio:              row.Bio,
		HeaderImageURL:   row.HeaderImageUrl,
	}, *row.Password, nil
}

func stringPtr(value string) *string {
	return &value
}

func randomToken(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func numericCode(size int) (string, error) {
	var b strings.Builder
	for i := 0; i < size; i++ {
		n, err := rand.Int(rand.Reader, big.NewInt(10))
		if err != nil {
			return "", err
		}
		b.WriteByte(byte('0' + n.Int64()))
	}
	return b.String(), nil
}
