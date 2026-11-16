#!/bin/bash
# Build and unit-test grpc_keepalive_probe offline against the chaincode's exact vendored dependencies
# (E0-0 remediation F2, 2026-11-09). Run inside Ubuntu WSL.
#
# The probe has no go.mod of its own: it is compiled in a throw-away directory holding a copy of
# fabric-testbed/chaincode/e1asset/{go.mod,go.sum,vendor/} plus the probe sources, with -mod=vendor. So it
# uses byte-identical grpc v1.83.2 and fabric-protos-go-apiv2 v0.3.4, and the chaincode directory (whose
# bytes define the installed package ID) is only read, never written.
#
# Usage: build_probe.sh OUT_DIR      -> OUT_DIR/kaprobe_linux, OUT_DIR/kaprobe_windows.exe, OUT_DIR/build_record.txt
set -euo pipefail
OUT=${1:?usage: build_probe.sh OUT_DIR}
HERE=$(cd "$(dirname "$0")" && pwd)
CC=$(cd "$HERE/../../chaincode/e1asset" && pwd)
GO=${GO:-$HOME/sdk/go1.26.4/bin/go}
export GOTOOLCHAIN=local GOPROXY=off GOFLAGS="-mod=vendor -buildvcs=false" GOCACHE=${GOCACHE:-/tmp/kaprobe-gocache}
W=$(mktemp -d /tmp/kaprobe-build.XXXXXX)
cp "$CC/go.mod" "$CC/go.sum" "$W/"
cp -r "$CC/vendor" "$W/vendor"
cp "$HERE/main.go" "$HERE/main_test.go" "$W/"
mkdir -p "$OUT"
cd "$W"
{
  echo "built $(date -u +%FT%TZ) in $W"
  $GO version
  echo "chaincode go.mod/go.sum/modules.txt sha256:"; sha256sum "$CC/go.mod" "$CC/go.sum" "$CC/vendor/modules.txt"
  echo "probe sources sha256:"; sha256sum "$HERE/main.go" "$HERE/main_test.go"
  $GO vet .
  $GO test -count=1 -v . 2>&1
  $GO build -o "$OUT/kaprobe_linux" .
  GOOS=windows GOARCH=amd64 $GO build -o "$OUT/kaprobe_windows.exe" .
  echo "binaries sha256:"; sha256sum "$OUT/kaprobe_linux" "$OUT/kaprobe_windows.exe"
} 2>&1 | tee "$OUT/build_record.txt"
