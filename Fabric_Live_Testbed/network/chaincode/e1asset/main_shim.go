//go:build ignore

// PRESERVED ADAPTER (2026-11-09). This is the plain-shim adapter that main.go used until the
// owner's ruling that the master plan's fabric-contract-api-go applies. The ignore constraint
// keeps it out of builds and out of `go mod tidy`, so it adds no dependency. Everything after
// the marker line below is byte-identical to that main.go (SHA-256
// ec6029c3734bec3bfa102f1ea8c5a559dfcd4cc5ebe17d0db7c30bbce1aef295); core/adapter_test.go checks it.
// Switching back is an owner decision: move the ignore constraint from this file to main.go.
// ----- original main.go follows -----
// Command e1asset is the Fabric chaincode entry point: a thin adapter from the Go shim to
// core.Dispatch. All logic and validation live in ./core.
//
// Status (2026-11-09): NOT COMPILED. The Fabric modules are not in this machine's module
// cache, and fetching them is an environment change (LA-2). Before deployment:
//
//	go mod tidy && go mod vendor   # with the pinned module versions recorded
//	go build ./...
//
// The two Fabric import paths below are UNVERIFIED until that build succeeds.
package main

import (
	"fmt"
	"os"

	"github.com/hyperledger/fabric-chaincode-go/v2/shim"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"

	"e1asset/core"
)

// E1Asset implements shim.Chaincode.
type E1Asset struct{}

// Init touches no state. The chaincode definition is committed without --init-required.
func (E1Asset) Init(_ shim.ChaincodeStubInterface) *peer.Response {
	return shim.Success(nil)
}

// Invoke routes every transaction through core.Dispatch. A dispatch error fails the proposal
// at endorsement, so the transaction never reaches the orderer.
func (E1Asset) Invoke(stub shim.ChaincodeStubInterface) *peer.Response {
	fn, args := stub.GetFunctionAndParameters()
	payload, err := core.Dispatch(stub, fn, args)
	if err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success(payload)
}

func main() {
	if err := shim.Start(E1Asset{}); err != nil {
		fmt.Fprintf(os.Stderr, "e1asset: shim.Start failed: %v\n", err)
		os.Exit(1)
	}
}
