// Command e1asset is the Fabric chaincode entry point: a fabric-contract-api-go contract whose
// transactions delegate, unchanged, to core.Dispatch. All logic and validation live in ./core
// (master plan file 04 section 2: Go, fabric-contract-api-go, dependencies vendored).
//
// Status (2026-11-09): NOT COMPILED. The Fabric modules are not in this machine's module cache,
// and fetching them is an environment change (LA-2). Before deployment:
//
//	go mod tidy && go mod vendor   # pin fabric-contract-api-go/v2; record every module version
//	go build ./...
//
// The contractapi import path and API below are UNVERIFIED until that build succeeds.
// core/adapter_test.go checks offline (go/parser only) that every method maps one-to-one onto
// a registered core function, passes its arguments in order and touches no state itself.
//
// Transaction names are the method names. With one contract it is the default contract, so
// Caliper calls them unqualified ("CreateAsset"). Arities are fixed by the signatures:
//
//	CreateAsset(id, value)   E1 experiment    2 args, empty payload
//	FailAlways()             E0 diagnostic    0 args (as called in E0-2a); always fails
//	ReadModifyWrite(name)    E0 diagnostic    1 arg; returns the new counter as text
//
// A call with another arity is rejected by contractapi before core runs, with contractapi's own
// message instead of an E1ASSET_/E0_ code. It still fails at endorsement with no state access.
// The superseded plain-shim adapter is kept, excluded from the build, in main_shim.go.
package main

import (
	"fmt"
	"os"

	"github.com/hyperledger/fabric-contract-api-go/v2/contractapi"

	"e1asset/core"
)

// E1AssetContract exposes the core registry as contract transactions.
type E1AssetContract struct {
	contractapi.Contract
}

// CreateAsset is the only E1 experiment function: one blind PutState (core/e1_functions.go).
func (c *E1AssetContract) CreateAsset(ctx contractapi.TransactionContextInterface, id string, value string) error {
	_, err := core.Dispatch(ctx.GetStub(), "CreateAsset", []string{id, value})
	return err
}

// FailAlways is E0-only: it always returns an E0_FAIL_ALWAYS error (core/e0_diagnostics.go).
func (c *E1AssetContract) FailAlways(ctx contractapi.TransactionContextInterface) error {
	_, err := core.Dispatch(ctx.GetStub(), "FailAlways", nil)
	return err
}

// ReadModifyWrite is E0-only: read, increment and write e0diag_<name> (core/e0_diagnostics.go).
func (c *E1AssetContract) ReadModifyWrite(ctx contractapi.TransactionContextInterface, name string) (string, error) {
	payload, err := core.Dispatch(ctx.GetStub(), "ReadModifyWrite", []string{name})
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func main() {
	cc, err := contractapi.NewChaincode(&E1AssetContract{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "e1asset: NewChaincode failed: %v\n", err)
		os.Exit(1)
	}
	if err := cc.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "e1asset: Start failed: %v\n", err)
		os.Exit(1)
	}
}
