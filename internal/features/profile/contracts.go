package profile

import (
	"context"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

type ProjectionRepository interface {
	WithinTransaction(context.Context, func(context.Context) error) error
	FindProcessedProfile(context.Context, eventstore.Position) (string, error)
	LoadProfile(context.Context, string) (*ProfileState, error)
	SaveProfile(context.Context, ProfileState, ProfileUpdateIdentity) error
}
type ProfileState struct {
	UserID                                               string
	Name, Username, Email, Bio, ImageURL, HeaderImageURL *string
	Position                                             eventstore.Position
}
type ProfileChange interface{ identity() ProfileUpdateIdentity }
type ProfileUpdateIdentity struct {
	UserID    string
	Position  eventstore.Position
	EventType string
}

type ProcessedEventReceipt struct {
	EventType string              `json:"eventType"`
	Position  eventstore.Position `json:"position"`
}
type RegistrationUpdate struct {
	ProfileUpdateIdentity
	Name, Username, Email string
}
type NameUpdate struct {
	ProfileUpdateIdentity
	Name string
}
type BioUpdate struct {
	ProfileUpdateIdentity
	Bio string
}
type ImageUpdate struct {
	ProfileUpdateIdentity
	ImageURL string
}
type HeaderImageUpdate struct {
	ProfileUpdateIdentity
	ImageURL string
}

func (u RegistrationUpdate) identity() ProfileUpdateIdentity { return u.ProfileUpdateIdentity }

func (u NameUpdate) identity() ProfileUpdateIdentity { return u.ProfileUpdateIdentity }

func (u BioUpdate) identity() ProfileUpdateIdentity { return u.ProfileUpdateIdentity }

func (u ImageUpdate) identity() ProfileUpdateIdentity { return u.ProfileUpdateIdentity }

func (u HeaderImageUpdate) identity() ProfileUpdateIdentity { return u.ProfileUpdateIdentity }
