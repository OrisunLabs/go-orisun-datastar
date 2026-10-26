package eventstore

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// Authority selection and observations belong to one behavior. Event contracts,
// pure utilities and exported command composition remain independently shareable.
func TestCommandAuthorityHelpersStayWithinOwningSlice(t *testing.T) {
	for _, root := range []string{"../auth", "../features/todo", "../features/profile"} {
		fset := token.NewFileSet()
		files := map[string]*ast.File{}
		functions := map[string]*ast.FuncDecl{}
		owners := map[string]string{}
		contexts := map[string]string{}
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			files[path] = file
			for _, declaration := range file.Decls {
				switch declaration := declaration.(type) {
				case *ast.FuncDecl:
					if declaration.Recv == nil {
						functions[declaration.Name.Name] = declaration
						owners[declaration.Name.Name] = path
					}
				case *ast.GenDecl:
					for _, spec := range declaration.Specs {
						if named, ok := spec.(*ast.TypeSpec); ok && strings.HasSuffix(named.Name.Name, "Context") {
							contexts[named.Name.Name] = path
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		sensitive := map[string]bool{}
		calls := map[string][]string{}
		for name, function := range functions {
			ast.Inspect(function, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.SelectorExpr:
					if node.Sel.Name == "GetLatestByCriteria" || node.Sel.Name == "GetEvents" {
						sensitive[name] = true
					}
					if qualifier, ok := node.X.(*ast.Ident); ok && qualifier.Name == "eventstore" {
						switch node.Sel.Name {
						case "Query", "Tag", "Criterion", "ConsistencyObservation", "LatestByCriteriaResult":
							sensitive[name] = true
						}
					}
				case *ast.CallExpr:
					if callee, ok := node.Fun.(*ast.Ident); ok {
						calls[name] = append(calls[name], callee.Name)
					}
				}
				return true
			})
		}
		for changed := true; changed; {
			changed = false
			for caller, callees := range calls {
				for _, callee := range callees {
					if sensitive[callee] && !sensitive[caller] {
						sensitive[caller] = true
						changed = true
					}
				}
			}
		}
		for path, file := range files {
			if !strings.HasSuffix(path, "_command_handler.go") {
				continue
			}
			handlers, saves := 0, 0
			for _, declaration := range file.Decls {
				function, ok := declaration.(*ast.FuncDecl)
				if !ok {
					continue
				}
				publicHandler := function.Recv == nil && ast.IsExported(function.Name.Name) && strings.HasSuffix(function.Name.Name, "CommandHandler")
				if publicHandler {
					handlers++
				}
				ast.Inspect(function, func(node ast.Node) bool {
					switch node := node.(type) {
					case *ast.Ident:
						if owner, exists := contexts[node.Name]; exists && owner != path {
							t.Errorf("%s: context %s belongs to %s", fset.Position(node.Pos()), node.Name, owner)
						}
					case *ast.CallExpr:
						if callee, ok := node.Fun.(*ast.Ident); ok && sensitive[callee.Name] && owners[callee.Name] != path && !strings.HasSuffix(callee.Name, "CommandHandler") {
							t.Errorf("%s: authority helper %s belongs to %s", fset.Position(node.Pos()), callee.Name, owners[callee.Name])
						}
					case *ast.SelectorExpr:
						if node.Sel.Name == "SaveCommandEventsV2" {
							saves++
						}
						if node.Sel.Name == "GetEvents" {
							t.Errorf("%s: commands must use bounded latest-event context reads", fset.Position(node.Pos()))
						}
						if publicHandler && node.Sel.Name == "GetLatestByCriteria" {
							t.Errorf("%s: public handler must load authority through its private context loader", fset.Position(node.Pos()))
						}
					}
					return true
				})
			}
			if handlers != 1 {
				t.Errorf("%s: want one public command handler, got %d", path, handlers)
			}
			if saves > 1 {
				t.Errorf("%s: want one terminal save call site, got %d", path, saves)
			}
		}
	}
}
