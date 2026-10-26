package main

import (
	"context"
	"strings"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/profile"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
)

type authUserInvalidator struct{ publisher eventstore.Publisher }

func (p authUserInvalidator) InvalidateAuthUser(ctx context.Context, id string) error {
	return p.publisher.Publish(ctx, profile.Channel(id), map[string]string{"userId": id})
}

func authProfileSource() auth.ProfileSource {
	return auth.ProfileSource{
		EventTypes: []string{profile.ProfileImageUploaded, profile.ProfileBioUpdated, profile.ProfileHeaderImageUploaded},
		Decode: func(ctx context.Context, event eventstore.DomainEvent, keys auth.SubjectPiiKeyReader) (auth.AuthChange, error) {
			switch event.EventType {
			case profile.ProfileImageUploaded:
				value, err := profile.DecodeProfileImageUploadedEvent(event)
				if err != nil {
					return nil, err
				}
				return auth.ImageInput{UserID: value.UserID, URL: value.ImageURL}, nil
			case profile.ProfileHeaderImageUploaded:
				value, err := profile.DecodeProfileHeaderImageUploadedEvent(event)
				if err != nil {
					return nil, err
				}
				return auth.HeaderImageInput{UserID: value.UserID, URL: value.ImageURL}, nil
			case profile.ProfileBioUpdated:
				value, err := profile.DecodeProfileBioUpdatedEvent(event)
				if err != nil {
					return nil, err
				}
				key, ok, err := keys.GetSubjectDataKey(ctx, value.UserID)
				if err != nil {
					return nil, err
				}
				if !ok {
					return nil, eventstore.ErrNotFound
				}
				bio, err := protectedpii.FromEnv().DecryptWithDataKey(value.Bio, key)
				if err != nil {
					return nil, err
				}
				if len(bio) > 280 || strings.TrimSpace(bio) != bio || protectedpii.FromEnv().BlindIndex(profile.ProfileBioUpdatedBioField, bio) != value.BioHash {
					return nil, eventstore.ErrInvalidEvent
				}
				return auth.BioInput{UserID: value.UserID, Bio: bio}, nil
			default:
				return nil, eventstore.ErrInvalidEvent
			}
		},
	}
}
