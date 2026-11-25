"""Offline self-test of check_s5_a2.py on temporary files (touches no container, no real approval file).
Usage: python3 -B selftest_s5_a2.py   -> exit 0 only if every case behaves as expected."""
import datetime as dt
import hashlib
import importlib.util
import sys
import tempfile
from pathlib import Path

ops = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("ck", ops / "check_s5_a2.py")
ck = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ck)

t = Path(tempfile.mkdtemp())
exp, out = t / "exp", t / "out"
(exp / "stage_series").mkdir(parents=True)
(exp / "series_evidence_2026-11-22" / "cl04_ABORTED").mkdir(parents=True)
(out / "cl04").mkdir(parents=True)
(exp / ck.RECORD).write_text("a2 record\n")
(exp / "AUTH_S5_AMENDMENT.txt").write_text("a1\n")
(exp / "AUTH_SERIES.txt").write_text("auth\n")
(exp / "stage_series" / "SHA256SUMS.txt").write_text("sums\n")
launcher = ops / "launch_run_v3.sh"
h = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
now = dt.datetime(2026, 11, 25, 12, 0, 0, tzinfo=dt.timezone.utc)


def good():
    return {"approved_by": "owner", "approved_utc": "2026-11-25T11:00:00Z", "amendment_id": "S5-A2",
            "amendment_record_sha256": h(exp / ck.RECORD), "a1_approval_sha256": h(exp / "AUTH_S5_AMENDMENT.txt"),
            "auth_series_sha256": h(exp / "AUTH_SERIES.txt"), "stage_sha256": h(exp / "stage_series" / "SHA256SUMS.txt"),
            "launcher_sha256": h(launcher), "continuation_check_sha256": h(ops / "s5a1_continuation_check.py"),
            "runs": ",".join(ck.SERIES_REST)}


fails = 0


def case(name, kv, run="cl05", want_ok=False, mutate=None):
    global fails
    p = t / "approval.txt"
    p.write_bytes(("\n".join(f"{k}={v}" for k, v in kv.items()) + "\n").encode())
    if mutate:
        mutate()
    try:
        problems = ck.check(p, run, launcher, now, exp=exp, out=out)
    except Exception as exc:  # parser refusals count as REFUSED
        problems = [str(exc)]
    ok = not problems
    res = "ok  " if ok == want_ok else "FAIL"
    fails += ok != want_ok
    print(res, name, "->", "ACCEPTED" if ok else "REFUSED: " + "; ".join(problems)[:110])


case("valid approval", good(), want_ok=True)
case("valid approval for cl12", good(), run="cl12", want_ok=True)
case("run cl04 not governed", good(), run="cl04")
case("run cl03 not governed", good(), run="cl03")
for k, v in [("approved_by", "assistant"), ("amendment_id", "S5-A1"), ("runs", "cl04,cl05"),
             ("approved_utc", "2026-11-23T09:00:00Z"), ("approved_utc", "2026-11-26T00:00:00Z"),
             ("launcher_sha256", "0" * 64), ("auth_series_sha256", "0" * 64), ("stage_sha256", "0" * 64),
             ("amendment_record_sha256", "0" * 64), ("a1_approval_sha256", "0" * 64),
             ("continuation_check_sha256", "0" * 64)]:
    kv = good()
    kv[k] = v
    case(f"bad {k}={v[:16]}", kv)
kv = good()
del kv["runs"]
case("missing key", kv)
kv = good()
kv["extra"] = "1"
case("extra key", kv)
(out / "cl04" / "run_result.json").write_text("{}")
case("cl04 has run_result.json", good())
(out / "cl04" / "run_result.json").unlink()
(exp / "series_evidence_2026-11-22" / "cl04_ABORTED").rmdir()
case("cl04_ABORTED evidence absent", good())
print("FAILURES:", fails)
sys.exit(1 if fails else 0)
