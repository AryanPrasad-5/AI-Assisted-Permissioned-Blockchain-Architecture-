#!/usr/bin/env bash
# Offline self-test of caliper_idle_check.sh (fake `docker` on PATH) and check_s5_amendment.py (temporary approval files).
# Touches no container. Usage: bash selftest_gates.sh
OPS=$(cd "$(dirname "$0")" && pwd)
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
fails=0; n=0
expect() { n=$((n+1)); if [ "$2" = "$3" ]; then echo "ok   $1 (exit $3)"; else echo "FAIL $1: expected exit $2, got $3"; fails=$((fails+1)); fi; }
mkfake() { mkdir -p "$T/bin"; printf '#!/usr/bin/env bash\n%s\n' "$1" > "$T/bin/docker"; chmod +x "$T/bin/docker"; }
run_idle() { PATH="$T/bin:$PATH" bash "$OPS/caliper_idle_check.sh" caliper-benchmark-001 >"$T/o" 2>&1; echo $?; }

mkfake 'printf "PID                 COMMAND\n1308                sleep infinity\n"'
expect "idle: only sleep infinity" 0 "$(run_idle)"
mkfake 'printf "PID                 COMMAND\n1308                sleep infinity\n4411                node /usr/local/bin/caliper launch manager --caliper-workspace /x\n"'
expect "busy: caliper launch manager running" 4 "$(run_idle)"
mkfake 'printf "PID                 COMMAND\n1308                sleep infinity\n5000                sleep 25\n"'
expect "busy: any other process (sleep 25)" 4 "$(run_idle)"
mkfake 'echo "Error response from daemon: unknown option args" >&2; exit 1'
expect "docker top error (the cl01 failure mode)" 4 "$(run_idle)"
mkfake 'printf "PID                 COMMAND\n"'
expect "empty process list" 4 "$(run_idle)"
mkfake 'exit 0'
expect "no output at all" 4 "$(run_idle)"
mkfake 'printf "PID                 COMMAND\n1308                sleep infinityX\n"'
expect "near-miss name (sleep infinityX)" 4 "$(run_idle)"
# the cl01 as-run test, for contrast: with docker top erroring it reports "not busy"
mkfake 'echo "Error response from daemon: unknown option args" >&2; exit 1'
if PATH="$T/bin:$PATH" bash -c 'docker top caliper-benchmark-001 -o args | tail -n +2 | grep -qv "sleep infinity"'; then old=4; else old=0; fi
expect "cl01 as-run test on docker error (documents the defect: it passes)" 0 "$old"

# --- check_s5_amendment.py on temporary approval files (paths patched to a temp EXP) ---
python3 -B - "$OPS" "$T" <<'EOF'
import importlib.util, sys, datetime as dt, hashlib, shutil
from pathlib import Path
ops, t = Path(sys.argv[1]), Path(sys.argv[2])
spec = importlib.util.spec_from_file_location("ck", ops / "check_s5_amendment.py"); ck = importlib.util.module_from_spec(spec); spec.loader.exec_module(ck)
exp = t / "exp"; (exp / "stage_series").mkdir(parents=True)
(exp / "S5_AMENDMENT_A1_2026-11-22.md").write_text("record\n"); (exp / "AUTH_SERIES.txt").write_text("auth\n")
(exp / "stage_series" / "SHA256SUMS.txt").write_text("sums\n")
launcher = ops / "launch_run_v2.sh"
h = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
good = {"approved_by": "owner", "approved_utc": "2026-11-22T19:00:00Z", "amendment_id": "S5-A1",
        "amendment_record_sha256": h(exp / "S5_AMENDMENT_A1_2026-11-22.md"), "auth_series_sha256": h(exp / "AUTH_SERIES.txt"),
        "stage_sha256": h(exp / "stage_series" / "SHA256SUMS.txt"), "launcher_sha256": h(launcher),
        "continuation_check_sha256": h(ops / "s5a1_continuation_check.py"), "runs": ",".join(ck.SERIES_REST)}
now = dt.datetime(2026, 11, 22, 19, 30, tzinfo=dt.timezone.utc)
def run(name, kv, expect_ok, rid="cl02", raw=None):
    p = t / "a.txt"; p.write_bytes(raw if raw is not None else ("".join(f"{k}={v}\n" for k, v in kv.items())).encode())
    try: probs = ck.check(p, rid, launcher, now, exp=exp)
    except Exception as e: probs = [f"refused: {e}"]
    ok = (not probs) == expect_ok
    print(("ok   " if ok else "FAIL ") + f"amendment: {name} -> {'ACCEPTED' if not probs else 'REFUSED ' + probs[0][:70]}")
    return ok
res = [run("owner-filled file", good, True)]
for k in ("approved_by", "approved_utc"):
    b = dict(good); b[k] = ""; res.append(run(f"blank {k} (template as shipped)", b, False))
b = dict(good); b["approved_by"] = "assistant"; res.append(run("approved_by not owner", b, False))
b = dict(good); b["approved_utc"] = "2026-11-22T17:40:00Z"; res.append(run("approved before cl01 ended (backdated)", b, False))
b = dict(good); b["approved_utc"] = "2026-11-22T20:00:00Z"; res.append(run("approved in the future", b, False))
for k in ("amendment_record_sha256", "auth_series_sha256", "stage_sha256", "launcher_sha256", "continuation_check_sha256"):
    b = dict(good); b[k] = "0" * 64; res.append(run(f"{k} mismatch", b, False))
b = dict(good); b["runs"] = "cl01," + good["runs"]; res.append(run("runs include cl01", b, False))
res.append(run("run cl01 requested", good, False, rid="cl01"))
res.append(run("run cl13 requested", good, False, rid="cl13"))
b = dict(good); b["extra"] = "x"; res.append(run("unexpected key", b, False))
res.append(run("CRLF file", None, False, raw=("".join(f"{k}={v}\r\n" for k, v in good.items())).encode()))
res.append(run("duplicate key", None, False, raw=("".join(f"{k}={v}\n" for k, v in good.items()) + "approved_by=owner\n").encode()))
print("AMENDMENT_SELFTEST", "PASS" if all(res) else "FAIL", f"{sum(res)}/{len(res)}")
sys.exit(0 if all(res) else 1)
EOF
ar=$?
echo "IDLE_SELFTEST $([ $fails = 0 ] && echo PASS || echo FAIL) $((n-fails))/$n"
[ $fails = 0 ] && [ $ar = 0 ]
