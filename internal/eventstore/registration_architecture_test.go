package eventstore

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestAcceptedRegistrationHasNoWorkflowDependencies(t *testing.T) {
	forbidden := map[string]bool{
		"UserRegistrationStarted": true, "UserRegistrationCompleted": true, "UserRegistrationFailed": true, "UserRegistrationStartedIDField": true, "ScopeUserRegistrationStartedIDField": true,
		"EmailVerificationOTPGenerated": true, "EmailVerificationOTPValidated": true, "EmailVerificationOTPSent": true,
		"StartUserRegistrationCommand": true, "CompleteUserRegistrationCommand": true, "FinalizeUserRegistration": true,
	}
	for _, path := range []string{"../auth/register_user_command_handler.go", "../auth/project.go", "../auth/auth_user_projection_event_handler.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if name, ok := node.(*ast.Ident); ok && forbidden[name.Name] {
				t.Errorf("%s depends on workflow symbol %s", path, name.Name)
			}
			return true
		})
	}
}

func TestPasswordDomainHasNoResetWorkflowDependencies(t *testing.T) {
	forbidden := map[string]bool{"PasswordResetAuthorized": true, "PasswordResetCompleted": true, "PasswordResetRequested": true, "DecodePasswordResetAuthorizedEvent": true, "DecodePasswordResetCompletedEvent": true, "DecodePasswordResetRequestedEvent": true}
	for _, path := range []string{"../auth/set_password_command_handler.go", "../auth/change_password_command_handler.go", "../auth/request_account_deletion_command_handler.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if name, ok := node.(*ast.Ident); ok && forbidden[name.Name] {
				t.Errorf("%s depends on workflow symbol %s", path, name.Name)
			}
			return true
		})
	}
}

func TestWorkflowReactionsInvokeOnlyTheirOwnCommand(t *testing.T) {
	expected := map[string]string{
		"../auth/registration_execution_event_handler.go":      "RegisterUserCommandHandler",
		"../auth/registration_completion_event_handler.go":     "CompleteUserRegistrationCommandHandler",
		"../auth/password_reset_execution_event_handler.go":    "SetPasswordCommandHandler",
		"../auth/password_reset_completion_event_handler.go":   "CompletePasswordResetCommandHandler",
		"../auth/account_deletion_event_handler.go":            "DeleteAccountCommandHandler",
		"../auth/account_deletion_completion_event_handler.go": "CompleteAccountDeletionCommandHandler",
	}
	for path, command := range expected {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		ast.Inspect(file, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if name, ok := call.Fun.(*ast.Ident); ok && strings.HasSuffix(name.Name, "CommandHandler") {
					calls++
					if name.Name != command {
						t.Errorf("%s dispatches %s instead of %s", path, name.Name, command)
					}
				}
			}
			return true
		})
		if calls != 1 {
			t.Errorf("%s has %d command calls; expected exactly one", path, calls)
		}
	}
}

func TestDeleteAccountHasNoRequestWorkflowDependencies(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "../auth/delete_account_command_handler.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	forbidden := map[string]bool{"AccountDeletionRequested": true, "AccountDeletionCompleted": true, "DecodeAccountDeletionRequestedEvent": true, "DecodeAccountDeletionCompletedEvent": true}
	ast.Inspect(file, func(node ast.Node) bool {
		if name, ok := node.(*ast.Ident); ok && forbidden[name.Name] {
			t.Errorf("domain deletion depends on workflow symbol %s", name.Name)
		}
		return true
	})
}
