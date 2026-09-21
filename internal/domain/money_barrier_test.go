package domain_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// Floating point on the money path is an eliminating defect, and the project
// states that guarantee in prose. Prose does not fail a build.
//
// The investigation that prompted this test found a production banking codebase
// whose core was exact decimal end to end, with a float leak in one newer
// endpoint: a bonus amount typed `float64` on a JSON contract, parsed with
// ParseFloat, converted to decimal only afterwards -- by which point the
// precision was already gone. Nothing objected, because nothing could. The type
// is not what keeps a codebase honest; the barrier is.
//
// The rule targets TYPE POSITIONS -- fields, parameters, results, declarations --
// and float parsing. A conversion like float64(count) feeding a Prometheus gauge
// is not money and is deliberately allowed: metric instruments require float64,
// and no monetary value may reach them, which the label rules enforce separately.
func TestNoFloatingPointOnTheMoneyPath(t *testing.T) {
	guarded := []string{
		"../domain", "../application",
		"../adapters/postgres", "../adapters/httpapi", "../adapters/sqs",
		"../workers",
	}
	fset := token.NewFileSet()
	report := func(pos token.Pos, form, detail string) {
		p := fset.Position(pos)
		t.Errorf("%s:%d: %s (%s) on the money path", filepath.Base(p.Filename), p.Line, form, detail)
	}

	// Recursive rather than hand-unwrapped: map[string]float64, []*float64,
	// [][]float64 and func(float64) all hide one behind a different node, and a
	// barrier that only peels one layer is a barrier with a documented hole.
	isFloat := func(e ast.Expr) string {
		form := ""
		ast.Inspect(e, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && (id.Name == "float32" || id.Name == "float64") {
				form = id.Name
				return false
			}
			return form == ""
		})
		return form
	}

	found := 0
	// Waivers are per line and per file, so a blessing cannot drift onto the
	// next conversion somebody adds.
	waived := map[int]bool{}
	allowFile := ""
	for _, dir := range guarded {
		pkgs, err := parser.ParseDir(fset, dir, func(info fs.FileInfo) bool {
			return !strings.HasSuffix(info.Name(), "_test.go")
		}, parser.ParseComments)
		if err != nil {
			t.Fatalf("parse %s: %v", dir, err)
		}
		for _, pkg := range pkgs {
			for path, file := range pkg.Files {
				allowFile = filepath.Base(path)
				waived = map[int]bool{}
				for _, group := range file.Comments {
					for _, c := range group.List {
						if strings.Contains(c.Text, "money:allow-float") {
							waived[fset.Position(c.Pos()).Line] = true
						}
					}
				}
				ast.Inspect(file, func(n ast.Node) bool {
					switch v := n.(type) {
					case *ast.Field: // struct fields, parameters and results
						if form := isFloat(v.Type); form != "" {
							found++
							report(v.Pos(), form, "type position")
						}
					case *ast.ValueSpec: // var and const declarations
						if v.Type != nil {
							if form := isFloat(v.Type); form != "" {
								found++
								report(v.Pos(), form, "declaration")
							}
						}
					case *ast.TypeSpec:
						// `type Rate float64` launders a float behind a name, and
						// every field typed Rate afterwards looks clean.
						if form := isFloat(v.Type); form != "" {
							found++
							report(v.Pos(), form, "type definition")
						}
					case *ast.CallExpr:
						// An explicit conversion is the one form with a legitimate
						// use here -- float64(count) for a Prometheus gauge -- so it
						// is waived per line, by name, rather than by category.
						if id, ok := v.Fun.(*ast.Ident); ok && (id.Name == "float32" || id.Name == "float64") {
							if !waived[fset.Position(v.Pos()).Line] || !strings.HasSuffix(fset.Position(v.Pos()).Filename, allowFile) {
								found++
								report(v.Pos(), id.Name+"(...)", "conversion without a //money:allow-float waiver")
							}
						}
					case *ast.BasicLit:
						// An untyped float constant -- a rake of 0.05, a fee
						// multiplier -- carries no float64 in a type position and
						// would otherwise pass, then poison every expression it
						// touches through Go's untyped constant promotion.
						if v.Kind == token.FLOAT {
							found++
							report(v.Pos(), v.Value, "float literal")
						}
					case *ast.SelectorExpr: // ParseFloat, NewFromFloat, InexactFloat64
						name := v.Sel.Name
						if name == "ParseFloat" || strings.Contains(name, "FromFloat") || strings.Contains(name, "InexactFloat") {
							found++
							report(v.Pos(), name, "float conversion of a value")
						}
					}
					return true
				})
			}
		}
	}
	if found > 0 {
		t.Fatalf("%d floating point reference(s) reached the money path", found)
	}
}
