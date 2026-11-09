package core

// E1 EXPERIMENT FUNCTIONS. This file must never read state (a guard test enforces it).

// createAsset implements CreateAsset(id, value): one blind PutState. The read set stays empty,
// so concurrent CreateAsset transactions never MVCC-conflict (E1 v0.2 protocol section 10).
// There is deliberately no existence check, because that would add a read. Key uniqueness is
// the workload's job and is audited by the reconciler (V1-duplicates).
// It returns an empty payload, so the response adds no variable bytes to the envelope.
func createAsset(stub Stub, args []string) ([]byte, error) {
	if len(args) != 2 {
		return nil, fail(CodeBadArgs, "CreateAsset wants 2 args (id, value), got %d", len(args))
	}
	id, value := args[0], args[1]
	if err := validateKey("id", id); err != nil {
		return nil, err
	}
	if isReserved(id) {
		return nil, fail(CodeReservedKey, "id %q uses the E0-only prefix %q", id, E0KeyPrefix)
	}
	if err := validateValue(value); err != nil {
		return nil, err
	}
	if err := stub.PutState(id, []byte(value)); err != nil {
		return nil, fail(CodeStateWrite, "PutState(%q): %v", id, err)
	}
	return nil, nil
}
