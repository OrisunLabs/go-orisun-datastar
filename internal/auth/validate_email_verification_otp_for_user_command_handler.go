package auth

import (
	"context"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
)

func ValidateEmailVerificationOTPForUserCommandHandler(ctx context.Context, userID, code string, metadata CommandMetadata, users AuthUserByIDReader, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(struct {
		UserID string
		Code   string
	}{UserID: userID, Code: code}); err != nil {
		return err
	}
	user, err := users.UserByIDOrRegisteredID(ctx, userID)
	if err != nil {
		return err
	}
	return ValidateEmailVerificationOTPCommandHandler(ctx, ValidateEmailVerificationOTPCommand{
		User:     user,
		Code:     code,
		Metadata: metadata,
	}, saver, retriever)
}
