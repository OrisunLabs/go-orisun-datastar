package eventstore

import (
	"go/ast"
	"go/parser"
	"go/token"
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
