package core

// Offline checks of the contract-API adapter (../main.go). The adapter imports
// fabric-contract-api-go, which is not in the module cache, so it cannot be compiled here; these
// tests parse it with go/parser (standard library) and check its structure against the core
// registry. Compilation and real contractapi behaviour are verified in LA-2 (go build) and E0-2.

import (
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const contractAPI = "github.com/hyperledger/fabric-contract-api-go/v2/contractapi"

// plannedArity is the number of string arguments each transaction takes in the protocols:
// CreateAsset(id, value) (E1 v0.2 section 10), FailAlways() and ReadModifyWrite("hot") (E0-2).
var plannedArity = map[string]int{"CreateAsset": 2, "FailAlways": 0, "ReadModifyWrite": 1}

func parseAdapter(t *testing.T, name string) (*ast.File, []byte) {
	t.Helper()
	src, err := os.ReadFile(filepath.Join("..", name))
	if err != nil {
		t.Fatal(err)
	}
	f, err := parser.ParseFile(token.NewFileSet(), name, src, parser.ParseComments|parser.AllErrors)
	if err != nil {
		t.Fatalf("%s does not parse: %v", name, err)
	}
	return f, src
}

func exprText(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return exprText(x.X) + "." + x.Sel.Name
	case *ast.StarExpr:
		return "*" + exprText(x.X)
	case *ast.ArrayType:
		return "[]" + exprText(x.Elt)
	}
	return "?"
}

func contractMethods(f *ast.File) map[string]*ast.FuncDecl {
	out := map[string]*ast.FuncDecl{}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if ok && fn.Recv != nil && len(fn.Recv.List) == 1 && exprText(fn.Recv.List[0].Type) == "*E1AssetContract" {
			out[fn.Name.Name] = fn
		}
	}
	return out
}

func TestContractAdapterIsBuiltAndImportsOnlyContractAPIAndCore(t *testing.T) {
	f, src := parseAdapter(t, "main.go")
	if strings.Contains(string(src), "//go:build") {
		t.Fatal("main.go must not carry a build constraint: it is the adapter that gets built")
	}
	var got []string
	for _, imp := range f.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		got = append(got, p)
	}
	sort.Strings(got)
	want := []string{"e1asset/core", "fmt", contractAPI, "os"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("main.go imports %v, want %v", got, want)
	}
}

func TestContractEmbedsContractAndMainStartsIt(t *testing.T) {
	f, src := parseAdapter(t, "main.go")
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != "E1AssetContract" {
			return true
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok || len(st.Fields.List) != 1 || st.Fields.List[0].Names != nil ||
			exprText(st.Fields.List[0].Type) != "contractapi.Contract" {
			t.Fatalf("E1AssetContract must embed exactly contractapi.Contract")
		}
		found = true
		return false
	})
	if !found {
		t.Fatal("type E1AssetContract not found")
	}
	for _, s := range []string{"contractapi.NewChaincode(&E1AssetContract{})", "cc.Start()"} {
		if !strings.Contains(string(src), s) {
			t.Fatalf("main.go does not contain %q", s)
		}
	}
}

// Every registered core function, and nothing else, is a contract method with the planned arity,
// typed context first, string parameters, and the result shape that keeps the payload unchanged.
func TestContractMethodsMatchTheRegistry(t *testing.T) {
	f, _ := parseAdapter(t, "main.go")
	methods := contractMethods(f)
	var names []string
	for n := range methods {
		names = append(names, n)
	}
	sort.Strings(names)
	var registered []string
	for _, fi := range Functions() {
		registered = append(registered, fi.Name)
	}
	if !reflect.DeepEqual(names, registered) {
		t.Fatalf("contract methods %v, registry %v", names, registered)
	}
	for name, fn := range methods {
		params := fn.Type.Params.List
		if len(params) == 0 || exprText(params[0].Type) != "contractapi.TransactionContextInterface" ||
			len(params[0].Names) != 1 || params[0].Names[0].Name != "ctx" {
			t.Fatalf("%s: first parameter must be ctx contractapi.TransactionContextInterface", name)
		}
		n := 0
		for _, p := range params[1:] {
			if exprText(p.Type) != "string" {
				t.Fatalf("%s: non-string parameter %s", name, exprText(p.Type))
			}
			n += len(p.Names)
		}
		if n != plannedArity[name] {
			t.Fatalf("%s: %d string parameters, planned %d", name, n, plannedArity[name])
		}
		var results []string
		for _, r := range fn.Type.Results.List {
			results = append(results, exprText(r.Type))
		}
		want := []string{"error"}
		if name == "ReadModifyWrite" {
			want = []string{"string", "error"} // core returns the counter as text bytes; string keeps them unchanged
		}
		if !reflect.DeepEqual(results, want) {
			t.Fatalf("%s returns %v, want %v", name, results, want)
		}
	}
}

// Each method makes exactly one core.Dispatch call: (ctx.GetStub(), "<its own name>", its
// parameters in declaration order, or nil when it has none). No state call appears in main.go.
func TestContractMethodsDelegateUnchanged(t *testing.T) {
	f, src := parseAdapter(t, "main.go")
	for name, fn := range contractMethods(f) {
		var params []string
		for _, p := range fn.Type.Params.List[1:] {
			for _, id := range p.Names {
				params = append(params, id.Name)
			}
		}
		var calls []*ast.CallExpr
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if c, ok := n.(*ast.CallExpr); ok && exprText(c.Fun) == "core.Dispatch" {
				calls = append(calls, c)
			}
			return true
		})
		if len(calls) != 1 || len(calls[0].Args) != 3 {
			t.Fatalf("%s: want exactly one core.Dispatch call with 3 arguments, got %d", name, len(calls))
		}
		args := calls[0].Args
		if c, ok := args[0].(*ast.CallExpr); !ok || exprText(c.Fun) != "ctx.GetStub" || len(c.Args) != 0 {
			t.Fatalf("%s: first Dispatch argument must be ctx.GetStub()", name)
		}
		if lit, ok := args[1].(*ast.BasicLit); !ok || lit.Value != strconv.Quote(name) {
			t.Fatalf("%s: second Dispatch argument must be the literal %q", name, name)
		}
		if len(params) == 0 {
			if id, ok := args[2].(*ast.Ident); !ok || id.Name != "nil" {
				t.Fatalf("%s: no parameters, so the args must be nil", name)
			}
			continue
		}
		cl, ok := args[2].(*ast.CompositeLit)
		if !ok || exprText(cl.Type) != "[]string" || len(cl.Elts) != len(params) {
			t.Fatalf("%s: args must be []string{%s}", name, strings.Join(params, ", "))
		}
		for i, e := range cl.Elts {
			if id, ok := e.(*ast.Ident); !ok || id.Name != params[i] {
				t.Fatalf("%s: argument %d is %s, want %s (declaration order)", name, i, exprText(e), params[i])
			}
		}
	}
	for _, banned := range []string{"GetState", "PutState", "DelState", "GetFunctionAndParameters", "GetArgs",
		"GetStringArgs", "GetStateByRange", "GetPrivateData"} {
		if strings.Contains(string(src), banned+"(") {
			t.Fatalf("main.go calls %s: all state access belongs in core", banned)
		}
	}
}

// The superseded shim adapter is preserved byte for byte and excluded from builds and go mod tidy.
func TestShimAdapterPreservedAndExcluded(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "main_shim.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(src), "//go:build ignore\n\n") {
		t.Fatal("main_shim.go must start with the ignore build constraint")
	}
	parts := strings.SplitN(string(src), "// ----- original main.go follows -----\n", 2)
	if len(parts) != 2 {
		t.Fatal("preservation marker missing")
	}
	sum := sha256.Sum256([]byte(parts[1]))
	if got := hex.EncodeToString(sum[:]); got != "ec6029c3734bec3bfa102f1ea8c5a559dfcd4cc5ebe17d0db7c30bbce1aef295" {
		t.Fatalf("preserved shim adapter changed: sha256 %s", got)
	}
}
