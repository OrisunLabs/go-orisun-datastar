package eventstore

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

func commandSaveName(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.SelectorExpr:
		return value.Sel.Name
	case *ast.Ident:
		return value.Name
	}
	return ""
}
func commandSavePrimitive(name string) bool {
	switch name {
	case "SaveCommandEventsV2", "SaveCommandEvents", "SaveEventsV2", "SaveEvents":
		return true
	}
	return false
}
func commandSaveAssignment(statement ast.Stmt) *ast.CallExpr {
	assignment, ok := statement.(*ast.AssignStmt)
	if !ok {
		return nil
	}
	for _, value := range assignment.Rhs {
		if call, ok := value.(*ast.CallExpr); ok && commandSavePrimitive(commandSaveName(call.Fun)) {
			return call
		}
	}
	return nil
}

// Discover declarations rather than relying on filenames or a fixed inventory.
// Feature helpers cannot conceal a save, including by capturing a save function
// or bound method as a value. Infrastructure adapters remain outside this rule.
func commandPersistenceViolations(file *ast.File, featureFile bool) (int, []string) {
	handlers := 0
	var violations []string
	if featureFile {
		// A package-level alias must not hide a second write behind a harmless
		// local name while the handler also contains its visible terminal save.
		for _, decl := range file.Decls {
			declaration, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range declaration.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, expression := range value.Values {
					ast.Inspect(expression, func(node ast.Node) bool {
						switch n := node.(type) {
						case *ast.SelectorExpr:
							if commandSavePrimitive(n.Sel.Name) {
								violations = append(violations, "package-level persistence capture or write is forbidden")
							}
						case *ast.Ident:
							if commandSavePrimitive(n.Name) {
								violations = append(violations, "package-level persistence capture or write is forbidden")
							}
						}
						return true
					})
				}
			}
		}
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		handler := strings.HasSuffix(fn.Name.Name, "CommandHandler")
		if handler {
			handlers++
		}
		if !handler && !featureFile {
			continue
		}
		var calls []*ast.CallExpr
		stack := []ast.Node{}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if node == nil {
				stack = stack[:len(stack)-1]
				return false
			}
			if call, ok := node.(*ast.CallExpr); ok {
				name := commandSaveName(call.Fun)
				if commandSavePrimitive(name) {
					calls = append(calls, call)
					if !handler || name != "SaveCommandEventsV2" {
						violations = append(violations, fmt.Sprintf("%s: persistence must be a handler-owned SaveCommandEventsV2 call", fn.Name.Name))
					}
					for _, parent := range stack {
						switch parent.(type) {
						case *ast.FuncLit, *ast.GoStmt, *ast.DeferStmt, *ast.ForStmt, *ast.RangeStmt:
							violations = append(violations, fn.Name.Name+": persistence cannot run in a closure, goroutine, defer or loop")
						}
					}
				}
				if handler && strings.HasSuffix(name, "CommandHandler") {
					violations = append(violations, fn.Name.Name+": cannot delegate to another command handler")
				}
			}
			if identifier, ok := node.(*ast.Ident); ok && commandSavePrimitive(identifier.Name) {
				parent := stack[len(stack)-1]
				_, selectorMember := parent.(*ast.SelectorExpr)
				call, called := parent.(*ast.CallExpr)
				if !selectorMember && (!called || call.Fun != identifier) {
					violations = append(violations, fn.Name.Name+": cannot capture a persistence function")
				}
			}
			if selector, ok := node.(*ast.SelectorExpr); ok && commandSavePrimitive(selector.Sel.Name) {
				parent := stack[len(stack)-1]
				call, ok := parent.(*ast.CallExpr)
				if !ok || call.Fun != selector {
					violations = append(violations, fn.Name.Name+": cannot capture or delegate a persistence function")
				}
			}
			stack = append(stack, node)
			return true
		})
		if handler {
			if len(calls) != 1 {
				violations = append(violations, fmt.Sprintf("%s: expected exactly one persistence call, got %d", fn.Name.Name, len(calls)))
			}
			terminalCalls := 0
			for _, statement := range fn.Body.List {
				call := commandSaveAssignment(statement)
				if conditional, ok := statement.(*ast.IfStmt); ok {
					call = commandSaveAssignment(conditional.Init)
				}
				if call != nil && commandSaveName(call.Fun) == "SaveCommandEventsV2" {
					terminalCalls++
				}
			}
			if terminalCalls != 1 {
				violations = append(violations, fn.Name.Name+": save must be in the common function body, outside branches")
			}
			if len(calls) == 1 {
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if ok && call.Pos() > calls[0].End() {
						name := commandSaveName(call.Fun)
						if strings.HasPrefix(name, "New") && strings.HasSuffix(name, "Event") {
							violations = append(violations, fn.Name.Name+": cannot construct another event after persistence")
						}
					}
					return true
				})
			}
		}
	}
	return handlers, violations
}

func TestEveryCommandHandlerOwnsOneTerminalV2Save(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	handlers := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != root && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "vendor" || entry.Name() == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative := filepath.ToSlash(rel)
		count, violations := commandPersistenceViolations(file, strings.HasPrefix(relative, "internal/features/") || strings.HasPrefix(relative, "internal/auth/"))
		handlers += count
		for _, violation := range violations {
			t.Errorf("%s: %s", rel, violation)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if handlers == 0 {
		t.Fatal("no command handlers discovered")
	}
	t.Logf("audited every %d command handler", handlers)
}

func TestCommandPersistenceGuardRejectsSplitOrDelegatedWrites(t *testing.T) {
	for name, body := range map[string]string{
		"split":             "func TestCommandHandler(){ eventstore.SaveCommandEventsV2(); eventstore.SaveCommandEventsV2() }",
		"branch":            "func TestCommandHandler(){ if direct { eventstore.SaveCommandEventsV2() } }",
		"loop":              "func TestCommandHandler(){ for {eventstore.SaveCommandEventsV2()} }",
		"closure":           "func TestCommandHandler(){ func(){eventstore.SaveCommandEventsV2()}() }",
		"defer":             "func TestCommandHandler(){ defer eventstore.SaveCommandEventsV2() }",
		"goroutine":         "func TestCommandHandler(){ go eventstore.SaveCommandEventsV2() }",
		"helper":            "func TestCommandHandler(){persist()};func persist(){eventstore.SaveCommandEventsV2()}",
		"alias":             "func TestCommandHandler(){persist:=eventstore.SaveCommandEventsV2;persist()}",
		"global alias":      "var persist=eventstore.SaveCommandEventsV2;func TestCommandHandler(){if _,err:=eventstore.SaveCommandEventsV2();err!=nil{return};persist()}",
		"dot alias":         "func TestCommandHandler(){persist:=SaveCommandEventsV2;persist()}",
		"raw":               "func TestCommandHandler(){saver.SaveEventsV2()}",
		"legacy":            "func TestCommandHandler(){eventstore.SaveCommandEvents()}",
		"delegated command": "func TestCommandHandler(){OtherCommandHandler();eventstore.SaveCommandEventsV2()}",
		"late event":        "func TestCommandHandler(){eventstore.SaveCommandEventsV2();NewLateEvent()}",
	} {
		t.Run(name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", "package fixture;"+body, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, violations := commandPersistenceViolations(file, true)
			if len(violations) == 0 {
				t.Fatal("invalid persistence ownership accepted")
			}
		})
	}
	file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", "package fixture; func TestCommandHandler(){if retry{return};events:=[]Event{NewOneEvent(),NewTwoEvent()};if _,err:=eventstore.SaveCommandEventsV2(events);err!=nil{return};return}", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, violations := commandPersistenceViolations(file, true); len(violations) != 0 {
		t.Fatal("valid atomic handler rejected", violations)
	}
}
