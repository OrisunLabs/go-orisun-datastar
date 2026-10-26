package eventstore

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Envelope selectors are shared infrastructure; payload keys and JSON tags
// remain independently owned contracts.
func TestEventTypeSelectorsUseSharedConstant(t *testing.T) {
	for _, root := range []string{"../../internal", "../../cmd"} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			check := func(expr ast.Expr) {
				literal, ok := expr.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return
				}
				value, err := strconv.Unquote(literal.Value)
				if err == nil && value == EventTypeField {
					t.Errorf("%s: envelope selector must use eventstore.EventTypeField", fset.Position(expr.Pos()))
				}
			}
			ast.Inspect(file, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.KeyValueExpr:
					key, ok := node.Key.(*ast.Ident)
					if !ok {
						break
					}
					if key.Name == "Key" {
						check(node.Value)
					}
					if key.Name == "Fields" {
						if fields, ok := node.Value.(*ast.CompositeLit); ok {
							for _, field := range fields.Elts {
								check(field)
							}
						}
					}
				case *ast.BinaryExpr:
					for _, pair := range [][2]ast.Expr{{node.X, node.Y}, {node.Y, node.X}} {
						if field, ok := pair[0].(*ast.SelectorExpr); ok && field.Sel.Name == "Key" {
							check(pair[1])
						}
					}
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
