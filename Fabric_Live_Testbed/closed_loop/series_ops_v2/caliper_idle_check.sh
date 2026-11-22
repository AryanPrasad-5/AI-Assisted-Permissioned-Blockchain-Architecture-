#!/usr/bin/env bash
# Caliper-idle gate (replaces the cl01 launcher test, which used `docker top -o args`; that form errors on this Docker
# version, so the old test could never fail). PASS only when docker top succeeds, lists at least one process, and every
# listed process is `sleep infinity` (the container's idle entrypoint). Any docker error, empty listing or other process
# (e.g. a running `caliper launch manager`) is a FAIL. Exit 0 = idle, 4 = busy/unknown.
C=${1:-caliper-benchmark-001}
OUT=$(docker top "$C" -o pid,args 2>&1); rc=$?
if [ $rc -ne 0 ]; then echo "CALIPER_IDLE FAIL: docker top exit $rc: $OUT"; exit 4; fi
BODY=$(printf '%s\n' "$OUT" | tail -n +2 | sed '/^[[:space:]]*$/d')
if [ -z "$BODY" ]; then echo "CALIPER_IDLE FAIL: no process listed: $OUT"; exit 4; fi
OTHER=$(printf '%s\n' "$BODY" | sed -E 's/^[[:space:]]*[0-9]+[[:space:]]+//' | grep -vx 'sleep infinity')
if [ -n "$OTHER" ]; then echo "CALIPER_IDLE FAIL: busy: $(printf '%s' "$OTHER" | tr '\n' ';')"; exit 4; fi
echo "CALIPER_IDLE PASS: $(printf '%s' "$BODY" | tr '\n' ';')"
exit 0
