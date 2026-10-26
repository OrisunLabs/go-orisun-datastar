package auth

import (
	"context"
	"time"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

type SubjectPiiKeyReader interface {
	GetSubjectDataKey(context.Context, string) (protectedpii.SubjectDataKey, bool, error)
}
type ProjectionRepository interface {
	WithinTransaction(context.Context, func(context.Context) error) error
	Processed(context.Context, eventstore.Position) (string, error)
	Load(context.Context, string) (*AuthState, error)
	LoadVerification(context.Context, string) (*VerificationFact, error)
	Save(context.Context, ProjectionUpdate) error
}
type UserChangePublisher interface {
	InvalidateAuthUser(context.Context, string) error
}

type Occurrence struct {
	UserID    string
	EventType string              `json:"eventType"`
	Position  eventstore.Position `json:"position"`
}
type ProjectionUpdate struct {
	State        AuthState
	Occurrence   Occurrence
	Verification *VerificationFact
}
type AuthState struct {
	EmailVerified                                                        bool
	UserID, Name, Username, Email, PasswordHash, Image, Bio, HeaderImage string
	Verification                                                         *EmailValidationFact
	Position                                                             *eventstore.Position
}
type EmailValidationFact struct {
	At time.Time
}
type VerificationFact struct {
	ID, UserID, Purpose, ValueHash string
	ExpiresAt                      time.Time
}
type AuthChange interface{ AuthUserID() string }
type RegistrationInput struct {
	UserID, Name, Username, Email, PasswordHash string
	EmailVerifiedAt                             time.Time
}

func (c RegistrationInput) AuthUserID() string { return c.UserID }

type NameInput struct{ UserID, Name string }

func (c NameInput) AuthUserID() string { return c.UserID }

type PasswordInput struct{ UserID, PasswordHash string }

func (c PasswordInput) AuthUserID() string { return c.UserID }

func (c VerificationFact) AuthUserID() string { return c.UserID }

type ImageInput struct{ UserID, URL string }

func (c ImageInput) AuthUserID() string { return c.UserID }

type BioInput struct{ UserID, Bio string }

func (c BioInput) AuthUserID() string { return c.UserID }

type HeaderImageInput struct{ UserID, URL string }

func (c HeaderImageInput) AuthUserID() string { return c.UserID }
