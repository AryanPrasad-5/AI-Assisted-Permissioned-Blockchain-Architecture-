package core

// Synthetic tests only. The MVCC model below imitates Fabric's commit-time read-version check
// (later transactions in commit order are invalid if a key they read changed); it is a test
// model, not Fabric's validator.

import (
	"errors"
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

// ------------------------------------------------------------------ test doubles

type versioned struct {
	value   []byte
	version uint64
}

type world struct {
	kv      map[string]versioned
	version uint64
}

func newWorld() *world { return &world{kv: map[string]versioned{}} }

// fakeStub records every read (with the version seen) and every write, in order.
type fakeStub struct {
	w          *world
	txid       string
	reads      map[string]uint64
	readOrder  []string
	writes     map[string][]byte
	writeOrder []string
	getErr     error
	putErr     error
}

func (w *world) stub(txid string) *fakeStub {
	return &fakeStub{w: w, txid: txid, reads: map[string]uint64{}, writes: map[string][]byte{}}
}

func (s *fakeStub) GetState(key string) ([]byte, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	s.readOrder = append(s.readOrder, key)
	v, ok := s.w.kv[key]
	if !ok {
		s.reads[key] = 0
		return nil, nil
	}
	s.reads[key] = v.version
	return append([]byte(nil), v.value...), nil
}

func (s *fakeStub) PutState(key string, value []byte) error {
	if s.putErr != nil {
		return s.putErr
	}
	s.writeOrder = append(s.writeOrder, key)
	s.writes[key] = append([]byte(nil), value...)
	return nil
}

func (s *fakeStub) GetTxID() string { return s.txid }

// commit applies a simulated transaction if every key it read still has the version it saw.
func (w *world) commit(s *fakeStub) (valid bool) {
	for k, seen := range s.reads {
		if cur := w.kv[k]; cur.version != seen {
			return false // MVCC_READ_CONFLICT in Fabric terms
		}
	}
	w.version++
	for _, k := range s.writeOrder {
		w.kv[k] = versioned{value: s.writes[k], version: w.version}
	}
	return true
}

func code(t *testing.T, err error) string {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("want *core.Error, got %T %v", err, err)
	}
	if !strings.HasPrefix(err.Error(), e.Code+": ") {
		t.Fatalf("message %q does not start with its code %q", err.Error(), e.Code)
	}
	return e.Code
}

func value128() string { return strings.Repeat("v", 128) }

// ------------------------------------------------------------------ registry and separation

func TestRegistrySeparatesE1AndE0(t *testing.T) {
	if got := Names(KindE1); !reflect.DeepEqual(got, []string{"CreateAsset"}) {
		t.Fatalf("E1 functions = %v", got)
	}
	if got := Names(KindE0); !reflect.DeepEqual(got, []string{"FailAlways", "ReadModifyWrite"}) {
		t.Fatalf("E0-only functions = %v", got)
	}
	all := Functions()
	if len(all) != 3 || !sort.SliceIsSorted(all, func(i, j int) bool { return all[i].Name < all[j].Name }) {
		t.Fatalf("Functions() = %v", all)
	}
	for _, f := range all {
		if f.Kind != KindE1 && f.Kind != KindE0 {
			t.Fatalf("%s has kind %q", f.Name, f.Kind)
		}
	}
}

func TestUnknownFunctionFailsWithoutStateAccess(t *testing.T) {
	for _, fn := range []string{"", "createAsset", "Init", "DeleteAsset"} {
		s := newWorld().stub("tx1")
		_, err := Dispatch(s, fn, []string{"a", "b"})
		if code(t, err) != CodeUnknownFunction {
			t.Fatalf("%q: %v", fn, err)
		}
		if len(s.readOrder)+len(s.writeOrder) != 0 {
			t.Fatalf("%q touched state", fn)
		}
	}
}

// Static guard: the E1 file must stay write-only, and core must import the standard library only.
func TestE1FileNeverReadsState(t *testing.T) {
	src, err := os.ReadFile("e1_functions.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, banned := range []string{"GetState", "GetStateByRange", "GetHistoryForKey", "GetQueryResult",
		"GetPrivateData", "GetStateByPartialCompositeKey", "readModifyWrite", "failAlways"} {
		if strings.Contains(string(src), banned+"(") {
			t.Fatalf("e1_functions.go calls %s", banned)
		}
	}
}

func TestCoreImportsStandardLibraryOnly(t *testing.T) {
	files, _ := filepath.Glob("*.go")
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range parsed.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			if strings.Contains(strings.SplitN(path, "/", 2)[0], ".") {
				t.Fatalf("%s imports non-standard %s", f, path)
			}
		}
	}
}

// The preserved shim adapter (main_shim.go, excluded from the build since 2026-11-09) cannot be
// compiled offline. This checks that it parses and imports exactly core plus the two Fabric packages.
// The contract-API adapter in main.go is checked in adapter_test.go.
func TestAdapterParsesAndImportsOnlyShimProtosAndCore(t *testing.T) {
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, filepath.Join("..", "main_shim.go"), nil, parser.AllErrors)
	if err != nil {
		t.Fatalf("main_shim.go does not parse: %v", err)
	}
	var got []string
	for _, imp := range parsed.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		got = append(got, p)
	}
	sort.Strings(got)
	want := []string{"e1asset/core", "fmt", "github.com/hyperledger/fabric-chaincode-go/v2/shim",
		"github.com/hyperledger/fabric-protos-go-apiv2/peer", "os"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("main_shim.go imports %v, want %v", got, want)
	}
}

// ------------------------------------------------------------------ CreateAsset (E1)

func TestCreateAssetIsOneBlindWrite(t *testing.T) {
	w := newWorld()
	s := w.stub("tx1")
	payload, err := Dispatch(s, "CreateAsset", []string{"r001_1_0000001", value128()})
	if err != nil {
		t.Fatal(err)
	}
	if payload != nil {
		t.Fatalf("payload %q, want nil", payload)
	}
	if len(s.readOrder) != 0 {
		t.Fatalf("CreateAsset read %v (must be blind)", s.readOrder)
	}
	if !reflect.DeepEqual(s.writeOrder, []string{"r001_1_0000001"}) || string(s.writes["r001_1_0000001"]) != value128() {
		t.Fatalf("writes = %v", s.writes)
	}
}

func TestCreateAssetRejectsBadArgumentsWithoutWriting(t *testing.T) {
	long := strings.Repeat("k", MaxKeyBytes+1)
	cases := []struct {
		args []string
		code string
	}{
		{nil, CodeBadArgs},
		{[]string{"only-id"}, CodeBadArgs},
		{[]string{"a", "b", "c"}, CodeBadArgs},
		{[]string{"", "v"}, CodeBadArgs},
		{[]string{long, "v"}, CodeBadArgs},
		{[]string{"has space", "v"}, CodeBadArgs},
		{[]string{"\x00composite", "v"}, CodeBadArgs},
		{[]string{"ключ", "v"}, CodeBadArgs},
		{[]string{"k", ""}, CodeBadArgs},
		{[]string{"k", strings.Repeat("v", MaxValueBytes+1)}, CodeBadArgs},
		{[]string{"k", "tab\tvalue"}, CodeBadArgs},
		{[]string{"k", "café"}, CodeBadArgs},
		{[]string{"e0diag_hot", "v"}, CodeReservedKey},
		{[]string{"E0DIAG_hot", "v"}, CodeReservedKey},
	}
	for _, c := range cases {
		s := newWorld().stub("tx1")
		_, err := Dispatch(s, "CreateAsset", c.args)
		if got := code(t, err); got != c.code {
			t.Fatalf("%q: code %s, want %s (%v)", c.args, got, c.code, err)
		}
		if len(s.writeOrder) != 0 || len(s.readOrder) != 0 {
			t.Fatalf("%q touched state", c.args)
		}
	}
}

func TestCreateAssetBoundaryLengthsAccepted(t *testing.T) {
	s := newWorld().stub("tx1")
	if _, err := Dispatch(s, "CreateAsset", []string{strings.Repeat("k", MaxKeyBytes), strings.Repeat("v", MaxValueBytes)}); err != nil {
		t.Fatal(err)
	}
}

func TestCreateAssetPropagatesWriteError(t *testing.T) {
	s := newWorld().stub("tx1")
	s.putErr = errors.New("disk full")
	_, err := Dispatch(s, "CreateAsset", []string{"k", "v"})
	if code(t, err) != CodeStateWrite || !strings.Contains(err.Error(), "disk full") {
		t.Fatal(err)
	}
}

func TestConcurrentCreateAssetsNeverConflict(t *testing.T) {
	w := newWorld()
	var sims []*fakeStub
	for i := 0; i < 200; i++ { // all simulated against one snapshot, as within one block
		s := w.stub("tx" + strconv.Itoa(i))
		if _, err := Dispatch(s, "CreateAsset", []string{"r001_1_" + strconv.Itoa(1000000+i), value128()}); err != nil {
			t.Fatal(err)
		}
		sims = append(sims, s)
	}
	for i, s := range sims {
		if !w.commit(s) {
			t.Fatalf("tx %d invalid; blind writes must not conflict", i)
		}
	}
	if len(w.kv) != 200 {
		t.Fatalf("state holds %d keys", len(w.kv))
	}
}

// Two endorsers given the same proposal must produce identical read/write sets and payloads.
func TestDeterministicAcrossEndorsers(t *testing.T) {
	calls := []struct {
		fn   string
		args []string
	}{
		{"CreateAsset", []string{"r001_1_0000007", value128()}},
		{"ReadModifyWrite", []string{"hot"}},
		{"FailAlways", []string{"r001_1_0000025"}},
		{"Nope", nil},
	}
	for _, c := range calls {
		for rep := 0; rep < 50; rep++ {
			a, b := newWorld(), newWorld()
			for _, w := range []*world{a, b} {
				w.kv[E0KeyPrefix+"hot"] = versioned{value: []byte("v1|41|txprev"), version: 7}
			}
			sa, sb := a.stub("txSAME"), b.stub("txSAME")
			pa, ea := Dispatch(sa, c.fn, c.args)
			pb, eb := Dispatch(sb, c.fn, c.args)
			if !reflect.DeepEqual(pa, pb) || !reflect.DeepEqual(sa.reads, sb.reads) ||
				!reflect.DeepEqual(sa.writes, sb.writes) || !reflect.DeepEqual(sa.writeOrder, sb.writeOrder) ||
				(ea == nil) != (eb == nil) || (ea != nil && ea.Error() != eb.Error()) {
				t.Fatalf("%s: endorsers disagree", c.fn)
			}
		}
	}
}

// ------------------------------------------------------------------ FailAlways (E0 only)

func TestFailAlwaysAlwaysFailsAndTouchesNothing(t *testing.T) {
	for _, args := range [][]string{nil, {"r001_1_0000025"}, {"bad tag"}, {"a", "b"}} {
		s := newWorld().stub("tx1")
		payload, err := Dispatch(s, "FailAlways", args)
		if payload != nil || code(t, err) != CodeFailAlways {
			t.Fatalf("%q: payload %q err %v", args, payload, err)
		}
		if len(s.readOrder)+len(s.writeOrder) != 0 {
			t.Fatalf("%q touched state", args)
		}
	}
	_, err := Dispatch(newWorld().stub("tx"), "FailAlways", []string{"seq_25"})
	if err.Error() != "E0_FAIL_ALWAYS: injected endorsement failure tag=seq_25" {
		t.Fatalf("message %q", err)
	}
}

// ------------------------------------------------------------------ ReadModifyWrite (E0 only)

func TestReadModifyWriteSequentialCounts(t *testing.T) {
	w := newWorld()
	for i := 1; i <= 20; i++ {
		s := w.stub("tx" + strconv.Itoa(i))
		payload, err := Dispatch(s, "ReadModifyWrite", []string{"hot"})
		if err != nil {
			t.Fatal(err)
		}
		if string(payload) != strconv.Itoa(i) {
			t.Fatalf("call %d returned %q", i, payload)
		}
		if !reflect.DeepEqual(s.readOrder, []string{"e0diag_hot"}) || !reflect.DeepEqual(s.writeOrder, []string{"e0diag_hot"}) {
			t.Fatalf("rwset reads %v writes %v", s.readOrder, s.writeOrder)
		}
		if !w.commit(s) {
			t.Fatalf("sequential call %d conflicted", i)
		}
	}
	if got := string(w.kv["e0diag_hot"].value); got != "v1|20|tx20" {
		t.Fatalf("final state %q", got)
	}
}

// E0-2b expectation: back-to-back calls simulated against the same version conflict.
func TestReadModifyWriteConcurrentCallsConflict(t *testing.T) {
	w := newWorld()
	var sims []*fakeStub
	for i := 0; i < 20; i++ {
		s := w.stub("tx" + strconv.Itoa(i))
		if _, err := Dispatch(s, "ReadModifyWrite", []string{"hot"}); err != nil {
			t.Fatal(err)
		}
		sims = append(sims, s)
	}
	valid := 0
	for _, s := range sims {
		if w.commit(s) {
			valid++
		}
	}
	if valid != 1 {
		t.Fatalf("%d of 20 valid; want exactly 1 (19 MVCC conflicts)", valid)
	}
	if got := string(w.kv["e0diag_hot"].value); got != "v1|1|tx0" {
		t.Fatalf("state %q", got)
	}
}

func TestReadModifyWriteDoesNotTouchE1Keys(t *testing.T) {
	w := newWorld()
	w.kv["hot"] = versioned{value: []byte("e1 data"), version: 3}
	s := w.stub("tx1")
	if _, err := Dispatch(s, "ReadModifyWrite", []string{"hot"}); err != nil {
		t.Fatal(err)
	}
	if _, read := s.reads["hot"]; read || s.writes["hot"] != nil {
		t.Fatal("ReadModifyWrite touched an unprefixed key")
	}
}

func TestReadModifyWriteRejectsCorruptStateWithoutWriting(t *testing.T) {
	for _, raw := range []string{"garbage", "v1|x|tx", "v1|-1|tx", "v1|+5|tx", "v2|5|tx", "v1|5|", "v1|5", "v1||tx",
		"v1|99999999999999999999999|tx", "v1|18446744073709551615|tx"} {
		w := newWorld()
		w.kv["e0diag_hot"] = versioned{value: []byte(raw), version: 1}
		s := w.stub("tx1")
		_, err := Dispatch(s, "ReadModifyWrite", []string{"hot"})
		if c := code(t, err); c != CodeCorruptState {
			t.Fatalf("%q: code %s (%v)", raw, c, err)
		}
		if len(s.writeOrder) != 0 {
			t.Fatalf("%q: wrote despite corrupt state", raw)
		}
	}
}

func TestReadModifyWriteArgumentAndStubErrors(t *testing.T) {
	for _, args := range [][]string{nil, {}, {"a", "b"}, {""}, {"bad key"}} {
		s := newWorld().stub("tx1")
		_, err := Dispatch(s, "ReadModifyWrite", args)
		if code(t, err) != CodeBadArgs || len(s.readOrder)+len(s.writeOrder) != 0 {
			t.Fatalf("%q: %v", args, err)
		}
	}
	s := newWorld().stub("tx1")
	s.getErr = errors.New("ledger closed")
	if _, err := Dispatch(s, "ReadModifyWrite", []string{"hot"}); code(t, err) != CodeStateRead {
		t.Fatal(err)
	}
	s = newWorld().stub("tx1")
	s.putErr = errors.New("ledger closed")
	if _, err := Dispatch(s, "ReadModifyWrite", []string{"hot"}); code(t, err) != CodeStateWrite {
		t.Fatal(err)
	}
}
