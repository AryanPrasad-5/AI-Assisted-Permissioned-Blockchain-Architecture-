"""Focused offline self-test of s5a1_continuation_check.py and check_s5_amendment.py on COPIES of the cl01 evidence.
Reads the verified D: copy of cl01 and the frozen stage; writes only into a fresh temporary directory. No Fabric, no Docker.
Usage: python selftest_s5a1.py [evidence_cl01_dir] [stage_dir] [auth_file]"""
import copy
import csv
import importlib.util
import json
import shutil
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
EXP = Path(sys.argv[2]).parent if len(sys.argv) > 2 else Path(r"D:\PLAN_BLOCK18\closed_loop_experiment_2026-11-22")
EV = Path(sys.argv[1]) if len(sys.argv) > 1 else EXP / "series_evidence_2026-11-22" / "cl01"
STAGE = Path(sys.argv[2]) if len(sys.argv) > 2 else EXP / "stage_series"
AUTH = Path(sys.argv[3]) if len(sys.argv) > 3 else EXP / "AUTH_SERIES.txt"
spec = importlib.util.spec_from_file_location("s5a1", HERE / "s5a1_continuation_check.py")
s5 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(s5)


def build(tmp: Path):
    out, ws = tmp / "out", tmp / "ws"
    (out / "cl01").mkdir(parents=True)
    for n in ("run_result.json", "analysis.json"):
        shutil.copy(EV / "cl01" / n, out / "cl01" / n)
    shutil.copy(EV / "cl01_runner_console.log", out / "cl01_runner_console.log")
    (out / "cl01_verification").mkdir()
    shutil.copy(EV / "cl01_verification" / "independent_verification.json", out / "cl01_verification" / "independent_verification.json")
    src = EV / "caliper_run_workspace" / "cl01"
    (ws / "cl01").mkdir(parents=True)
    for p in src.iterdir():
        if p.is_file() and (p.name.startswith("dispatch_") or p.name.startswith("schedule_") or p.name == "tx_map.csv"):
            shutil.copy(p, ws / "cl01" / p.name)
    return out, ws


def jedit(path: Path, fn):
    d = json.loads(path.read_text(encoding="utf-8"))
    fn(d)
    path.write_text(json.dumps(d), encoding="utf-8")


def set_lag(ws, rnd, seq_pred, lag):
    p = ws / "cl01" / f"dispatch_r{rnd:02d}.csv"
    rows = list(csv.DictReader(p.open(encoding="utf-8")))
    for r in rows:
        if seq_pred(int(r["seq"]), r):
            r["lag_ms"] = str(lag)
            r["dispatch_ms"] = str(int(r["intended_ms"]) + lag)
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def flagged(an, flags):
    an["flags"] = flags


CASES = []


def case(name, expect, failing_ids=()):
    def deco(fn):
        CASES.append((name, expect, set(failing_ids), fn))
        return fn
    return deco


@case("cl01 as recorded", "CONTINUE")
def _(out, ws): pass


@case("extra flag FAILED_TRANSACTIONS", "STOP", {"C03"})
def _(out, ws):
    jedit(out / "cl01" / "analysis.json", lambda a: a["flags"].append("FAILED_TRANSACTIONS 1"))


@case("extra flag CLOCK_SUSPECT", "STOP", {"C03"})
def _(out, ws):
    jedit(out / "cl01" / "analysis.json", lambda a: a["flags"].append("CLOCK_SUSPECT round 2 (60.0 ms)"))


@case("DISPATCH_LAG in round 3 (no identified cause)", "STOP", {"C03", "C05"})
def _(out, ws):
    set_lag(ws, 3, lambda s, r: s % 10 == 0, 150)
    jedit(out / "cl01" / "analysis.json", lambda a: a["flags"].append("DISPATCH_LAG round 3 p99 150 ms"))


@case("recomputed lag disagrees with analyzer (unflagged round over 100)", "STOP", {"C04", "C05"})
def _(out, ws):
    set_lag(ws, 2, lambda s, r: s % 10 == 0, 150)


@case("round-6 p99 above ceiling 250", "STOP", {"C05"})
def _(out, ws):
    set_lag(ws, 6, lambda s, r: 374 <= s <= 493, 300)


@case("round-6 max above 500", "STOP", {"C05"})
def _(out, ws):
    set_lag(ws, 6, lambda s, r: s == 400, 600)


@case("round-6 lag>100 outside the burst window", "STOP", {"C05"})
def _(out, ws):
    set_lag(ws, 6, lambda s, r: s == 10, 120)


@case("a scheduled tx not dispatched", "STOP", {"C06"})
def _(out, ws):
    p = ws / "cl01" / "dispatch_r04.csv"
    lines = p.read_text(encoding="utf-8").splitlines()
    p.write_text("\n".join(lines[:-1]) + "\n", encoding="utf-8")


@case("overflow callback recorded", "STOP", {"C06"})
def _(out, ws):
    p = ws / "cl01" / "dispatch_r05.csv.overflow.csv"
    p.write_text(p.read_text(encoding="utf-8") + "999,1\n", encoding="utf-8")


@case("analyzer problem", "STOP", {"C02"})
def _(out, ws):
    jedit(out / "cl01" / "analysis.json", lambda a: a["problems"].append("reconciliation: 1 missing"))


@case("runner step failure", "STOP", {"C02"})
def _(out, ws):
    jedit(out / "cl01" / "run_result.json", lambda r: r.__setitem__("step_failure", "actuate_d05_B40"))


@case("runner verdict INVALID_INCOMPLETE", "STOP", {"C02"})
def _(out, ws):
    jedit(out / "cl01" / "run_result.json", lambda r: r.__setitem__("verdict", "INVALID_INCOMPLETE"))


@case("runner exit code 3", "STOP", {"C01"})
def _(out, ws):
    p = out / "cl01_runner_console.log"
    p.write_text(p.read_text(encoding="utf-8").replace("runner exit code 0 ", "runner exit code 3 "), encoding="utf-8")


@case("independent verifier: another check fails", "STOP", {"C07"})
def _(out, ws):
    def f(iv):
        for c in iv["checks"]:
            if c["check"].startswith("no container restarted"):
                c["pass"] = False
    jedit(out / "cl01_verification" / "independent_verification.json", f)


@case("independent verifier missing", "STOP", {"C07", "C10"})
def _(out, ws):
    (out / "cl01_verification" / "independent_verification.json").unlink()


@case("ERRO line in container logs", "STOP", {"C10"})
def _(out, ws):
    jedit(out / "cl01_verification" / "independent_verification.json",
          lambda iv: iv.__setitem__("fabric_log_erro_pani_lines", {"orderer.example.com": 1}))


@case("Caliper exit non-zero", "STOP", {"C08"})
def _(out, ws):
    jedit(out / "cl01" / "run_result.json", lambda r: r["caliper"].__setitem__("exit_code", 1))


@case("Caliper hit the hard timeout", "STOP", {"C08"})
def _(out, ws):
    jedit(out / "cl01" / "run_result.json", lambda r: r["caliper"].__setitem__("duration_s", 1200.0))


@case("as-run schedule differs from stage", "STOP", {"C09"})
def _(out, ws):
    p = ws / "cl01" / "schedule_r02.txt"
    p.write_text(p.read_text() + "59999\n")


@case("run bound to a different authorization", "STOP", {"C11"})
def _(out, ws):
    jedit(out / "cl01" / "run_result.json", lambda r: r.__setitem__("authorization_sha256", "0" * 64))


@case("clean VALID run (no flags, lag under 100)", "CONTINUE")
def _(out, ws):
    set_lag(ws, 6, lambda s, r: float(r["lag_ms"]) > 50, 40)
    jedit(out / "cl01" / "analysis.json", lambda a: (a.__setitem__("flags", []), a.__setitem__("verdict", "VALID")))
    jedit(out / "cl01" / "run_result.json", lambda r: r.__setitem__("verdict", "VALID"))

    def f(iv):
        for c in iv["checks"]:
            if c["check"] in s5.VERDICT_CHECKS:
                c["pass"] = True
    jedit(out / "cl01_verification" / "independent_verification.json", f)


@case("VALID verdict but verifier still fails verdict checks (inconsistent)", "STOP", {"C07"})
def _(out, ws):
    set_lag(ws, 6, lambda s, r: float(r["lag_ms"]) > 50, 40)
    jedit(out / "cl01" / "analysis.json", lambda a: (a.__setitem__("flags", []), a.__setitem__("verdict", "VALID")))
    jedit(out / "cl01" / "run_result.json", lambda r: r.__setitem__("verdict", "VALID"))


def main():
    fails = 0
    for name, expect, ids, fn in CASES:
        with tempfile.TemporaryDirectory() as t:
            out, ws = build(Path(t))
            fn(out, ws)
            try:
                res = s5.evaluate("cl01", out, ws, STAGE, AUTH)
                got = res["decision"]
                failing = {c["id"] for c in res["checks"] if not c["pass"]}
            except Exception as exc:  # a crash must never read as CONTINUE
                got, failing = f"CRASH {exc!r}", set()
            ok = got == expect and (failing == ids)
            fails += not ok
            print(("ok   " if ok else "FAIL ") + f"{name}: expected {expect} {sorted(ids)}, got {got} {sorted(failing)}")
    # the recorded verdict is copied verbatim, never rewritten
    with tempfile.TemporaryDirectory() as t:
        out, ws = build(Path(t))
        res = s5.evaluate("cl01", out, ws, STAGE, AUTH)
        an = json.loads((EV / "cl01" / "analysis.json").read_text(encoding="utf-8"))
        ok = res["recorded_analysis_verdict"] == an["verdict"] == "VALID_WITH_FLAGS" and res["recorded_flags"] == an["flags"]
        fails += not ok
        print(("ok   " if ok else "FAIL ") + "recorded verdict and flags preserved verbatim:", res["recorded_analysis_verdict"], res["recorded_flags"])
    print(f"SELFTEST {'PASS' if not fails else 'FAIL'}: {len(CASES) + 1 - fails}/{len(CASES) + 1}")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
