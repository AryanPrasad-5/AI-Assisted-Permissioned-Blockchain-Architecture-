package core

import "strings"

const (
	// MaxKeyBytes bounds a caller-supplied key or tag. The planned E1 key
	// "<runId>_<round>_<seq:07d>" is far shorter.
	MaxKeyBytes = 128
	// MaxValueBytes bounds a CreateAsset value. E1 uses a fixed 128-byte value (protocol section 10).
	MaxValueBytes = 4096
	// E0KeyPrefix is reserved for E0 diagnostics. CreateAsset rejects keys with this prefix, in any case.
	E0KeyPrefix = "e0diag_"
)

// validateKey accepts 1..MaxKeyBytes bytes from [A-Za-z0-9._-]. That excludes the 0x00 prefix
// Fabric reserves for composite keys, whitespace and non-ASCII bytes.
func validateKey(what, k string) error {
	if len(k) == 0 || len(k) > MaxKeyBytes {
		return fail(CodeBadArgs, "%s length %d outside 1..%d", what, len(k), MaxKeyBytes)
	}
	for i := 0; i < len(k); i++ {
		c := k[i]
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '_' || c == '-' || c == '.'
		if !ok {
			return fail(CodeBadArgs, "%s byte %d (0x%02x) not in [A-Za-z0-9._-]", what, i, c)
		}
	}
	return nil
}

// validateValue accepts 1..MaxValueBytes printable ASCII bytes (0x20..0x7e).
func validateValue(v string) error {
	if len(v) == 0 || len(v) > MaxValueBytes {
		return fail(CodeBadArgs, "value length %d outside 1..%d", len(v), MaxValueBytes)
	}
	for i := 0; i < len(v); i++ {
		if c := v[i]; c < 0x20 || c > 0x7e {
			return fail(CodeBadArgs, "value byte %d (0x%02x) is not printable ASCII", i, c)
		}
	}
	return nil
}

func isReserved(k string) bool {
	return strings.HasPrefix(strings.ToLower(k), E0KeyPrefix)
}
