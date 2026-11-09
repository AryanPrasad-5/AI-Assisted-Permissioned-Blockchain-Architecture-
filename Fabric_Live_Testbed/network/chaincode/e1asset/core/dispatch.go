// Package core holds all e1asset chaincode logic behind a three-method state interface, so it
// can be tested without the Fabric shim. The shim adapter is ../main.go.
//
// Function kinds (E1 v0.2 protocol section 10; E0 protocol section E0-2, both PROPOSED):
//
//	E1_EXPERIMENT       CreateAsset                     e1_functions.go
//	E0_DIAGNOSTIC_ONLY  FailAlways, ReadModifyWrite     e0_diagnostics.go
//
// An E1 workload must call only E1_EXPERIMENT functions. E0 functions never touch keys an E1
// function can write: ReadModifyWrite writes only under E0KeyPrefix, which CreateAsset rejects,
// so any E0 write is visible in a ledger read by its key prefix.
//
// Determinism: no clock, randomness, map-order dependence, environment or I/O. Output depends
// only on the function name, the arguments, the transaction ID and the state read.
package core

import (
	"fmt"
	"sort"
)

// Stub is the subset of fabric-chaincode-go's shim.ChaincodeStubInterface used by this package.
type Stub interface {
	GetState(key string) ([]byte, error)
	PutState(key string, value []byte) error
	GetTxID() string
}

// Kind separates experiment functions from E0-only diagnostics.
type Kind string

const (
	KindE1 Kind = "E1_EXPERIMENT"
	KindE0 Kind = "E0_DIAGNOSTIC_ONLY"
)

// Stable error codes. Every error message starts with one of them, so worker logs and peer
// logs can be classified by prefix.
const (
	CodeUnknownFunction = "E1ASSET_UNKNOWN_FUNCTION"
	CodeBadArgs         = "E1ASSET_BAD_ARGS"
	CodeReservedKey     = "E1ASSET_RESERVED_KEY"
	CodeStateRead       = "E1ASSET_STATE_READ"
	CodeStateWrite      = "E1ASSET_STATE_WRITE"
	CodeFailAlways      = "E0_FAIL_ALWAYS"
	CodeCorruptState    = "E0_RMW_CORRUPT_STATE"
)

// Error carries a stable code and a deterministic message.
type Error struct {
	Code string
	Msg  string
}

func (e *Error) Error() string { return e.Code + ": " + e.Msg }

func fail(code, format string, a ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, a...)}
}

type handler func(stub Stub, args []string) ([]byte, error)

type function struct {
	kind Kind
	run  handler
}

var registry = map[string]function{
	"CreateAsset":     {KindE1, createAsset},
	"FailAlways":      {KindE0, failAlways},
	"ReadModifyWrite": {KindE0, readModifyWrite},
}

// FunctionInfo describes one registered function.
type FunctionInfo struct {
	Name string
	Kind Kind
}

// Functions lists the registered functions sorted by name.
func Functions() []FunctionInfo {
	out := make([]FunctionInfo, 0, len(registry))
	for name, f := range registry {
		out = append(out, FunctionInfo{Name: name, Kind: f.kind})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Names lists the registered function names of one kind, sorted.
func Names(kind Kind) []string {
	var out []string
	for _, f := range Functions() {
		if f.Kind == kind {
			out = append(out, f.Name)
		}
	}
	return out
}

// Dispatch runs one transaction. A non-nil error fails the proposal at endorsement; the caller
// (main.go) turns it into shim.Error, so nothing reaches the orderer.
func Dispatch(stub Stub, fn string, args []string) ([]byte, error) {
	f, ok := registry[fn]
	if !ok {
		return nil, fail(CodeUnknownFunction, "unknown function %q; E1: %v; E0-only: %v",
			fn, Names(KindE1), Names(KindE0))
	}
	return f.run(stub, args)
}
