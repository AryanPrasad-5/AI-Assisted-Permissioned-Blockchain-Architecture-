package core

// E0-ONLY DIAGNOSTIC FUNCTIONS (E0 protocol section E0-2, PROPOSED). An E1 workload must
// never call these. They exist to prove that failure accounting works:
//   FailAlways       endorsement failure: nothing reaches the orderer (E0-2a)
//   ReadModifyWrite  read-then-write of one shared key: concurrent calls produce
//                    MVCC_READ_CONFLICT at commit (E0-2b)

import (
	"strconv"
	"strings"
)

const rmwValueVersion = "v1"

// failAlways implements FailAlways([tag]): it always returns an error whose message starts with
// CodeFailAlways, and it touches no state, so the proposal fails at endorsement with an empty
// read/write set. The optional tag (for example the workload's sequence key) is echoed so the
// worker log line can be matched to the transaction. A bad tag or arity still fails with the same code.
func failAlways(_ Stub, args []string) ([]byte, error) {
	switch {
	case len(args) > 1:
		return nil, fail(CodeFailAlways, "injected endorsement failure (arity %d; want 0 or 1 tag)", len(args))
	case len(args) == 1:
		if validateKey("tag", args[0]) != nil {
			return nil, fail(CodeFailAlways, "injected endorsement failure (tag rejected)")
		}
		return nil, fail(CodeFailAlways, "injected endorsement failure tag=%s", args[0])
	default:
		return nil, fail(CodeFailAlways, "injected endorsement failure")
	}
}

// readModifyWrite implements ReadModifyWrite(name): it reads E0KeyPrefix+name, increments the
// counter stored there and writes it back as "v1|<counter>|<txid>". It returns the new counter.
// Two calls simulated against the same committed version conflict at validation.
// Unparseable existing state is an error, never silently overwritten.
func readModifyWrite(stub Stub, args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, fail(CodeBadArgs, "ReadModifyWrite wants 1 arg (name), got %d", len(args))
	}
	if err := validateKey("name", args[0]); err != nil {
		return nil, err
	}
	key := E0KeyPrefix + args[0]
	current, err := stub.GetState(key)
	if err != nil {
		return nil, fail(CodeStateRead, "GetState(%q): %v", key, err)
	}
	var counter uint64
	if len(current) > 0 {
		counter, err = parseCounter(current)
		if err != nil {
			return nil, fail(CodeCorruptState, "key %q holds %q: %v", key, string(current), err)
		}
	}
	if counter == ^uint64(0) {
		return nil, fail(CodeCorruptState, "key %q counter would overflow", key)
	}
	next := counter + 1
	value := rmwValueVersion + "|" + strconv.FormatUint(next, 10) + "|" + stub.GetTxID()
	if err := stub.PutState(key, []byte(value)); err != nil {
		return nil, fail(CodeStateWrite, "PutState(%q): %v", key, err)
	}
	return []byte(strconv.FormatUint(next, 10)), nil
}

// parseCounter reads "v1|<decimal counter>|<non-empty txid>".
func parseCounter(raw []byte) (uint64, error) {
	parts := strings.Split(string(raw), "|")
	if len(parts) != 3 || parts[0] != rmwValueVersion || parts[2] == "" {
		return 0, fail(CodeCorruptState, "want %s|<counter>|<txid>", rmwValueVersion)
	}
	if parts[1] == "" || strings.TrimLeft(parts[1], "0123456789") != "" {
		return 0, fail(CodeCorruptState, "counter %q is not a decimal number", parts[1])
	}
	return strconv.ParseUint(parts[1], 10, 64)
}
