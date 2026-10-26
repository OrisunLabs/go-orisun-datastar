package profile

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

const (
	ProfileBioUpdated          = "ProfileBioUpdated"
	ProfileImageUploaded       = "ProfileImageUploaded"
	ProfileHeaderImageUploaded = "ProfileHeaderImageUploaded"
)

const (
	ProfileBioUpdatedIDField          = "profileBioUpdatedId"
	ProfileBioUpdatedBioField         = "bio"
	ProfileBioUpdatedBioHashField     = "bioHash"
	ProfileBioUpdatedAtField          = "updatedAt"
	ProfileImageUploadedIDField       = "profileImageUploadedId"
	ProfileHeaderImageUploadedIDField = "profileHeaderImageUploadedId"
	ProfileImageURLField              = "imageUrl"
	ProfileImageUploadedAtField       = "uploadedAt"
	ProfileUserIDField                = "userId"
)

type ProfileBioUpdatedEvent struct {
	ProfileBioUpdatedID string             `json:"profileBioUpdatedId"`
	Bio                 protectedpii.Value `json:"bio"`
	BioHash             string             `json:"bioHash"`
	UpdatedAt           string             `json:"updatedAt"`
	UserID              string             `json:"userId"`
}

type ProfileImageUploadedEvent struct {
	ProfileImageUploadedID string `json:"profileImageUploadedId"`
	ImageURL               string `json:"imageUrl"`
	UploadedAt             string `json:"uploadedAt"`
	UserID                 string `json:"userId"`
}

type ProfileHeaderImageUploadedEvent struct {
	ProfileHeaderImageUploadedID string `json:"profileHeaderImageUploadedId"`
	ImageURL                     string `json:"imageUrl"`
	UploadedAt                   string `json:"uploadedAt"`
	UserID                       string `json:"userId"`
}

func NewProfileBioUpdatedEvent(profileBioUpdatedID, bio string, updatedAt time.Time, userID string, subjectKey protectedpii.SubjectDataKey, metadata map[string]any) (eventstore.DomainEvent, error) {
	if strings.TrimSpace(bio) != bio || len(bio) > 280 {
		return eventstore.DomainEvent{}, eventstore.ErrInvalidEvent
	}
	protector := protectedpii.FromEnv()
	protected, err := protector.ProtectWithDataKey(bio, ProfileBioUpdatedBioField, subjectKey)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	event := eventstore.DomainEvent{
		EventID:   profileBioUpdatedID,
		EventType: ProfileBioUpdated,
		Data: eventstore.MustData(ProfileBioUpdatedEvent{
			ProfileBioUpdatedID: profileBioUpdatedID,
			Bio:                 protected,
			BioHash:             protector.BlindIndex(ProfileBioUpdatedBioField, bio),
			UpdatedAt:           updatedAt.UTC().Format(time.RFC3339Nano),
			UserID:              userID,
		}),
		Metadata: metadata,
	}
	_, err = DecodeProfileBioUpdatedEvent(event)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	return event, nil
}

func NewProfileImageUploadedEvent(profileImageUploadedID, imageURL string, uploadedAt time.Time, userID string, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   profileImageUploadedID,
		EventType: ProfileImageUploaded,
		Data: eventstore.MustData(ProfileImageUploadedEvent{
			ProfileImageUploadedID: profileImageUploadedID,
			ImageURL:               imageURL,
			UploadedAt:             uploadedAt.UTC().Format(time.RFC3339Nano),
			UserID:                 userID,
		}),
		Metadata: metadata,
	}
	_, err := DecodeProfileImageUploadedEvent(event)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	return event, nil
}

func NewProfileHeaderImageUploadedEvent(profileHeaderImageUploadedID, imageURL string, uploadedAt time.Time, userID string, metadata map[string]any) (eventstore.DomainEvent, error) {
	event := eventstore.DomainEvent{
		EventID:   profileHeaderImageUploadedID,
		EventType: ProfileHeaderImageUploaded,
		Data: eventstore.MustData(ProfileHeaderImageUploadedEvent{
			ProfileHeaderImageUploadedID: profileHeaderImageUploadedID,
			ImageURL:                     imageURL,
			UploadedAt:                   uploadedAt.UTC().Format(time.RFC3339Nano),
			UserID:                       userID,
		}),
		Metadata: metadata,
	}
	_, err := DecodeProfileHeaderImageUploadedEvent(event)
	if err != nil {
		return eventstore.DomainEvent{}, err
	}
	return event, nil
}

func Channel(userID string) string {
	return "profile." + userID
}

func DecodeProfileBioUpdatedEvent(event eventstore.DomainEvent) (ProfileBioUpdatedEvent, error) {
	var value ProfileBioUpdatedEvent
	if err := decodeProfilePayload(event, ProfileBioUpdated, &value); err != nil {
		return value, err
	}
	if err := validateProfileIdentity(event, value.ProfileBioUpdatedID, value.UserID, value.UpdatedAt); err != nil {
		return value, err
	}
	protected := value.Bio
	nonce, nerr := base64.RawURLEncoding.DecodeString(protected.Nonce)
	ciphertext, cerr := base64.RawURLEncoding.DecodeString(protected.Ciphertext)
	blind, berr := base64.RawURLEncoding.DecodeString(protected.BlindIndex)
	if protected.Version != 1 || strings.TrimSpace(protected.KeyID) == "" || nerr != nil || len(nonce) != 12 || cerr != nil || len(ciphertext) < 16 || berr != nil || len(blind) != 32 || value.BioHash != protected.BlindIndex {
		return value, fmt.Errorf("%w: invalid protected bio", eventstore.ErrInvalidEvent)
	}
	return value, nil
}
func DecodeProfileImageUploadedEvent(event eventstore.DomainEvent) (ProfileImageUploadedEvent, error) {
	var value ProfileImageUploadedEvent
	if err := decodeProfilePayload(event, ProfileImageUploaded, &value); err != nil {
		return value, err
	}
	if err := validateProfileIdentity(event, value.ProfileImageUploadedID, value.UserID, value.UploadedAt); err != nil {
		return value, err
	}
	return value, validateProfileImageURL(value.ImageURL)
}
func DecodeProfileHeaderImageUploadedEvent(event eventstore.DomainEvent) (ProfileHeaderImageUploadedEvent, error) {
	var value ProfileHeaderImageUploadedEvent
	if err := decodeProfilePayload(event, ProfileHeaderImageUploaded, &value); err != nil {
		return value, err
	}
	if err := validateProfileIdentity(event, value.ProfileHeaderImageUploadedID, value.UserID, value.UploadedAt); err != nil {
		return value, err
	}
	return value, validateProfileImageURL(value.ImageURL)
}

// profileEventUserID validates the owning contract before exposing its entity
// reference. Auth registration is never profile event ancestry.
func profileEventUserID(event eventstore.DomainEvent) (string, error) {
	switch event.EventType {
	case ProfileBioUpdated:
		value, err := DecodeProfileBioUpdatedEvent(event)
		return value.UserID, err
	case ProfileImageUploaded:
		value, err := DecodeProfileImageUploadedEvent(event)
		return value.UserID, err
	case ProfileHeaderImageUploaded:
		value, err := DecodeProfileHeaderImageUploadedEvent(event)
		return value.UserID, err
	default:
		return "", fmt.Errorf("%w: unknown profile event", eventstore.ErrInvalidEvent)
	}
}
func decodeProfilePayload(event eventstore.DomainEvent, kind string, target any) error {
	invalid := func() error { return fmt.Errorf("%w: invalid %s contract", eventstore.ErrInvalidEvent, kind) }
	if event.EventType != kind {
		return invalid()
	}
	encoded, err := json.Marshal(event.Data)
	if err != nil {
		return invalid()
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil {
		return invalid()
	}
	canonical, err := json.Marshal(eventstore.MustData(target))
	if err != nil || !bytes.Equal(encoded, canonical) {
		return invalid()
	}
	return nil
}
func validProfileID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}
func validateProfileIdentity(event eventstore.DomainEvent, id, userID, instant string) error {
	if event.EventID != id || !validProfileID(id) || !validProfileID(userID) {
		return fmt.Errorf("%w: invalid profile identity", eventstore.ErrInvalidEvent)
	}
	at, err := time.Parse(time.RFC3339Nano, instant)
	if err != nil || at.IsZero() || at.UTC().Format(time.RFC3339Nano) != instant {
		return fmt.Errorf("%w: invalid profile instant", eventstore.ErrInvalidEvent)
	}
	return nil
}
func validateProfileImageURL(value string) error {
	invalid := fmt.Errorf("%w: invalid profile image URL", eventstore.ErrInvalidEvent)
	if value == "" || strings.Contains(value, "\\") || strings.IndexFunc(value, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return invalid
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.User != nil || parsed.Fragment != "" {
		return invalid
	}
	if strings.HasPrefix(value, "/") && !strings.HasPrefix(value, "//") && parsed.Host == "" && parsed.Scheme == "" {
		return nil
	}
	if (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Hostname() != "" {
		return nil
	}
	return invalid
}
func decryptProfileBio(value ProfileBioUpdatedEvent, key protectedpii.SubjectDataKey) (string, error) {
	protector := protectedpii.FromEnv()
	bio, err := protector.DecryptWithDataKey(value.Bio, key)
	if err != nil {
		return "", err
	}
	if len(bio) > 280 || strings.TrimSpace(bio) != bio || protector.BlindIndex(ProfileBioUpdatedBioField, bio) != value.BioHash {
		return "", eventstore.ErrInvalidEvent
	}
	return bio, nil
}
