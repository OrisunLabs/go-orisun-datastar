package eventcatalog

import (
	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/profile"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/todo"
)

func BoundaryIndexes() []eventstore.BoundaryIndexDefinition {
	return []eventstore.BoundaryIndexDefinition{
		{Name: "account_deletion_operation", Fields: []string{"operationId"}, EventTypes: []string{auth.AccountDeletionRequested}},
		{Name: "account_deletion_effect_completion", Fields: []string{"scopes.accountDeletedId"}, EventTypes: []string{auth.AccountDeletionCompleted}},
		{Name: "password_reset_authorized_id", Fields: []string{auth.PasswordResetAuthorizedIDField}, EventTypes: []string{auth.PasswordResetAuthorized}},
		{Name: "password_reset_operation", Fields: []string{"operationId"}, EventTypes: []string{auth.PasswordResetAuthorized}},
		{Name: "password_reset_completion", Fields: []string{auth.ScopePasswordResetAuthorizedIDField, "scopes.passwordChangedId"}, EventTypes: []string{auth.PasswordResetCompleted}},
		{Name: "registration_started_id", Fields: []string{auth.UserRegistrationStartedIDField}, EventTypes: []string{auth.UserRegistrationStarted}},
		{Name: "registration_user", Fields: []string{"userId"}, EventTypes: []string{auth.UserRegistrationStarted}},
		{Name: "registration_ancestry", Fields: []string{auth.ScopeUserRegistrationStartedIDField}, EventTypes: []string{auth.EmailVerificationOTPGenerated, auth.EmailVerificationOTPValidated, auth.UserRegistrationCompleted, auth.UserRegistrationFailed}},
		{
			Name:       "user_registered_email",
			Fields:     []string{auth.UserRegisteredEmailHashField},
			EventTypes: []string{auth.UserRegistered},
		},
		{
			Name:       "user_registered_username",
			Fields:     []string{auth.UserRegisteredUsernameHashField},
			EventTypes: []string{auth.UserRegistered},
		},
		{
			Name:       "user_registered_id",
			Fields:     []string{auth.UserRegisteredIDField},
			EventTypes: []string{auth.UserRegistered},
		},
		{
			Name:       "email_otp_generated_id",
			Fields:     []string{auth.EmailVerificationOTPGeneratedIDField},
			EventTypes: []string{auth.EmailVerificationOTPGenerated},
		},
		{
			Name:       "otp_ancestry",
			Fields:     []string{auth.ScopeEmailVerificationOTPGeneratedIDField},
			EventTypes: []string{auth.EmailVerificationOTPValidated, auth.EmailVerificationOTPSent},
		},
		{
			Name:       "password_reset_requested_id",
			Fields:     []string{auth.PasswordResetRequestedIDField},
			EventTypes: []string{auth.PasswordResetRequested},
		},
		{
			Name:       "reset_ancestry",
			Fields:     []string{auth.ScopePasswordResetRequestedIDField},
			EventTypes: []string{auth.PasswordResetEmailSent, auth.PasswordResetAuthorized, auth.PasswordResetCompleted},
		},
		{
			Name:   "auth_user_ancestry",
			Fields: []string{auth.ScopeUserRegisteredIDField},
			EventTypes: []string{
				auth.UserNameChanged,
				auth.PasswordResetRequested,
				auth.PasswordChanged,
				auth.AccountDeletionRequested,
				auth.AccountDeleted,
			},
		},
		{
			Name:       "profile_owner",
			Fields:     []string{profile.ProfileUserIDField},
			EventTypes: []string{profile.ProfileImageUploaded, profile.ProfileHeaderImageUploaded, profile.ProfileBioUpdated},
		},
		{
			Name:       "todo_created",
			Fields:     []string{todo.TodoCreatedIDField, todo.TodoUserIDField},
			EventTypes: []string{todo.TodoCreated},
		},
		{
			Name:       "todo_ancestry",
			Fields:     []string{todo.TodoScopeIDField, todo.TodoUserIDField},
			EventTypes: []string{todo.TodoRenamed, todo.TodoCompleted, todo.TodoReopened, todo.TodoDeleted},
		},
		{
			Name:       "account_deletion_requested_id",
			Fields:     []string{auth.AccountDeletionRequestedIDField},
			EventTypes: []string{auth.AccountDeletionRequested},
		},
		{
			Name:       "deletion_ancestry",
			Fields:     []string{"scopes.accountDeletionRequestedId"},
			EventTypes: []string{auth.AccountDeletionCompleted},
		},
		{
			Name:       "login_attempt_recorded_id",
			Fields:     []string{auth.LoginAttemptRecordedIDField},
			EventTypes: []string{auth.LoginAttemptRecorded},
		},
		{
			Name:       "login_attempt_identifier_hash",
			Fields:     []string{auth.LoginAttemptIdentifierHashField},
			EventTypes: []string{auth.LoginAttemptRecorded},
		},
		{
			Name:       "login_attempt_ip_hash",
			Fields:     []string{auth.LoginAttemptIPAddressHashField},
			EventTypes: []string{auth.LoginAttemptRecorded},
		},
		{
			Name:       "login_user",
			Fields:     []string{auth.LoginAttemptUserRegisteredIDField},
			EventTypes: []string{auth.LoginAttemptRecorded},
		},
	}
}
