#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fabric_source="${FABRIC_2_5_16_SOURCE:-}"
expected_commit="f871cf92a026aba7b12e6f06d71ded3e6e659d71"
if [[ -z "$fabric_source" || ! -f "$fabric_source/go.mod" ]]; then
  echo "FABRIC_2_5_16_SOURCE must name a source checkout of Fabric v2.5.16" >&2
  exit 2
fi
actual_commit="$(git -C "$fabric_source" rev-parse HEAD)"
if [[ "$actual_commit" != "$expected_commit" ]]; then
  echo "Fabric source mismatch: expected $expected_commit, found $actual_commit" >&2
  exit 2
fi
cd "$fabric_source"
GOTOOLCHAIN=local GOFLAGS=-mod=vendor go test "$repo/tools/fabric_config_verifier/main.go" \
  "$repo/tools/fabric_config_verifier/main_test.go" "$@"
