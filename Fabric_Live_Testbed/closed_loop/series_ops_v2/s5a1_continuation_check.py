"""S5-A1 continuation-policy check (post-cl01 protocol amendment, 2026-11-22). Read-only on the run's evidence.

This decides ONLY whether the series may continue to the next run. It never changes, re-labels or overrides the run's
recorded scientific verdict: the official runner/analyzer verdict and every flag are copied verbatim into the decision.

CONTINUE requires every check below. Anything else is STOP (no retry).
  C01 runner console records "runner exit code 0"
  C02 run_result.verdict == analysis.verdict, in {VALID, VALID_WITH_FLAGS}; no step failure; analysis.problems empty
  C03 every analysis flag is exactly "DISPATCH_LAG round 6 p99 <n> ms" (round 6 = the round whose frozen schedule holds the
      166-arrivals-in-1-s burst); any other flag (FAILED_TRANSACTIONS, CLOCK_SUSPECT, DISPATCH_LAG in another round, ...) stops
  C04 dispatch lag recomputed here from dispatch_rNN.csv: rounds with p99 > 100 ms == rounds the analyzer flagged
  C05 flagged round: p99 <= 250 ms, max <= 500 ms, and every lag > 100 ms has an intended offset inside the burst window
      [burst start, burst start + 3,000 ms]   (ceiling = "the flag does not imply a more serious delivery failure")
  C06 every round: dispatched rows == scheduled, overflow callbacks 0, lag == dispatch_ms - intended_ms on every row
  C07 independent verifier ran; its only failing checks are the two "verdict exactly VALID" checks, and those fail only
      when the verdict is VALID_WITH_FLAGS
  C08 Caliper exit code 0 and duration below the staged hard timeout
  C09 as-run schedules and tx_map are byte-identical to the frozen stage for this run; round-6 schedule hash == cl01's;
      round-6 burst recomputed from the stage = 166 arrivals from offset 31,722 ms
  C10 no ERRO/PANI line in the preserved container logs
  C11 run_result authorization / stage hashes == AUTH_SERIES.txt / stage SHA256SUMS in force
Usage: s5a1_continuation_check.py <run_id> [--out OUT] [--ws WS_ROOT] [--stage STAGE] [--auth AUTH] [--decisions DIR]
Exit 0 = CONTINUE, 5 = STOP, 2 = usage / decision file already exists.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
import re
import sys
from pathlib import Path

POLICY_ID = "S5-A1"
FLAG_LIMIT_MS = 100.0            # the analyzer's pre-registered DISPATCH_LAG_FLAG_P99_MS (unchanged)
PERMITTED_ROUND = 6
P99_CEILING_MS = 250.0
MAX_CEILING_MS = 500.0
BURST_WINDOW_MS = 3000
EXPECTED_BURST = (166, 31722)    # (arrivals in a sliding 1,000 ms window, start offset ms), recomputed from the stage
ROUND6_SCHEDULE_SHA256 = "c67b5edf3ae0"  # prefix of cl01's staged schedule_r06.txt sha256 (verified 2026-11-22)
FLAG_RE = re.compile(r"^DISPATCH_LAG round (\d+) p99 (\d+) ms$")
VERDICT_CHECKS = {"runner verdict exactly VALID", "analysis verdict exactly VALID"}


def sha(p: Path) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def nearest_rank(v, q):
    v = sorted(v)
    return v[max(0, math.ceil(q * len(v)) - 1)] if v else None


def peak_window(offsets, width=1000):
    best, j = (0, None), 0
    for k in range(len(offsets)):
        while offsets[k] - offsets[j] >= width:
            j += 1
        if k - j + 1 > best[0]:
            best = (k - j + 1, offsets[j])
    return best


def evaluate(run_id: str, out: Path, ws_root: Path, stage: Path, auth: Path) -> dict:
    checks = []

    def check(cid, name, ok, detail=""):
        checks.append({"id": cid, "check": name, "pass": bool(ok), "detail": detail})

    rd, ws = out / run_id, ws_root / run_id
    arm = json.loads((stage / run_id / "arm.json").read_text(encoding="utf-8"))
    rr = json.loads((rd / "run_result.json").read_text(encoding="utf-8"))
    an = json.loads((rd / "analysis.json").read_text(encoding="utf-8"))
    console = (out / f"{run_id}_runner_console.log").read_text(encoding="utf-8", errors="replace")
    check("C01", "runner exit code 0", re.search(r"^runner exit code 0 ", console, re.M) is not None)
    check("C02", "verdicts agree, comparable, no problems, no step failure",
          rr.get("verdict") == an.get("verdict") and an.get("verdict") in ("VALID", "VALID_WITH_FLAGS")
          and not an.get("problems") and rr.get("step_failure") is None,
          f"runner {rr.get('verdict')} analysis {an.get('verdict')} problems {an.get('problems')} step {rr.get('step_failure')}")
    flags = list(an.get("flags", []))
    flagged_rounds, bad_flags = set(), []
    for f in flags:
        m = FLAG_RE.match(f)
        if m and int(m.group(1)) == PERMITTED_ROUND:
            flagged_rounds.add(int(m.group(1)))
        else:
            bad_flags.append(f)
    check("C03", "only permitted flag (DISPATCH_LAG in round 6)", not bad_flags, f"not permitted: {bad_flags}")

    per_round, integrity_bad, over_rounds = {}, [], set()
    for r in arm["rounds"]:
        i = r["index"]
        p = ws / f"dispatch_r{i:02d}.csv"
        rows = list(csv.DictReader(p.open(encoding="utf-8"))) if p.exists() else []
        lags = [float(x["lag_ms"]) for x in rows]
        mism = sum(1 for x in rows if int(x["dispatch_ms"]) - int(x["intended_ms"]) != int(float(x["lag_ms"])))
        ovp = Path(f"{p}.overflow.csv")
        overflow = (len(ovp.read_text(encoding="utf-8").splitlines()) - 1) if ovp.exists() else None
        p99 = nearest_rank(lags, 0.99)
        per_round[i] = {"scheduled": r["n_tx"], "dispatched": len(rows), "p50": nearest_rank(lags, 0.5),
                        "p95": nearest_rank(lags, 0.95), "p99": p99, "max": max(lags) if lags else None,
                        "n_over_50": sum(x > 50 for x in lags), "n_over_100": sum(x > FLAG_LIMIT_MS for x in lags),
                        "overflow_rows": overflow, "lag_mismatch_rows": mism}
        if len(rows) != r["n_tx"] or overflow != 0 or mism:
            integrity_bad.append(i)
        if p99 is not None and p99 > FLAG_LIMIT_MS:
            over_rounds.add(i)
    check("C04", "recomputed p99>100 rounds == analyzer-flagged rounds",
          over_rounds == {int(FLAG_RE.match(f).group(1)) for f in flags if FLAG_RE.match(f)},
          f"recomputed {sorted(over_rounds)} flagged {flags}")
    burst_ok = True
    detail = ""
    if over_rounds:
        sched6 = [int(x) for x in (stage / run_id / "workspace" / f"schedule_r{PERMITTED_ROUND:02d}.txt").read_text().split()]
        n, start = peak_window(sched6)
        rows6 = list(csv.DictReader((ws / f"dispatch_r{PERMITTED_ROUND:02d}.csv").open(encoding="utf-8")))
        outside = [x["seq"] for x in rows6 if float(x["lag_ms"]) > FLAG_LIMIT_MS
                   and not (start <= int(x["intended_offset_ms"]) <= start + BURST_WINDOW_MS)]
        st = per_round.get(PERMITTED_ROUND, {})
        burst_ok = (over_rounds <= {PERMITTED_ROUND} and st.get("p99") is not None and st["p99"] <= P99_CEILING_MS
                    and st["max"] <= MAX_CEILING_MS and not outside)
        detail = f"p99 {st.get('p99')} max {st.get('max')} lag>100 outside burst window: {len(outside)}"
    check("C05", "flagged round within ceilings and confined to the burst window", burst_ok, detail)
    check("C06", "every scheduled tx dispatched once, no overflow, lag column consistent", not integrity_bad,
          f"rounds failing: {integrity_bad}")

    vpath = out / f"{run_id}_verification" / "independent_verification.json"
    if vpath.exists():
        iv = json.loads(vpath.read_text(encoding="utf-8"))
        failing = {c["check"] for c in iv["checks"] if not c["pass"]}
        is_flagged = an.get("verdict") == "VALID_WITH_FLAGS"
        ok = failing <= VERDICT_CHECKS and (failing == (VERDICT_CHECKS if is_flagged else set()))
        check("C07", "independent verifier: only the verdict-exactly-VALID checks fail, and only for VALID_WITH_FLAGS", ok,
              f"failing: {sorted(failing)}")
        erro = iv.get("fabric_log_erro_pani_lines", None)
        check("C10", "no ERRO/PANI lines in the preserved container logs", erro == {}, f"{erro}")
    else:
        check("C07", "independent verifier ran", False, "independent_verification.json missing")
        check("C10", "no ERRO/PANI lines in the preserved container logs", False, "verifier output missing")
    cal = rr.get("caliper") or {}
    check("C08", "Caliper exit 0 within the hard timeout",
          cal.get("exit_code") == 0 and cal.get("duration_s") is not None and cal["duration_s"] < arm["caliper_hard_timeout_s"],
          f"{cal}")
    names = ["tx_map.csv"] + [f"schedule_r{r['index']:02d}.txt" for r in arm["rounds"]]
    diff = [n for n in names if not (ws / n).exists() or sha(ws / n) != sha(stage / run_id / "workspace" / n)]
    s6 = stage / run_id / "workspace" / f"schedule_r{PERMITTED_ROUND:02d}.txt"
    burst = peak_window([int(x) for x in s6.read_text().split()])
    check("C09", "as-run workload == frozen stage; round-6 schedule == cl01's; burst 166 @ 31,722 ms",
          not diff and sha(s6).startswith(ROUND6_SCHEDULE_SHA256) and burst == EXPECTED_BURST,
          f"differs {diff}; round6 {sha(s6)[:12]}; burst {burst}")
    check("C11", "run bound to the AUTH_SERIES.txt and stage in force",
          rr.get("authorization_sha256") == sha(auth) and rr.get("stage_sha256") == sha(stage / "SHA256SUMS.txt"),
          f"auth {str(rr.get('authorization_sha256'))[:12]} vs {sha(auth)[:12]}")

    ok = all(c["pass"] for c in checks)
    return {"policy": POLICY_ID, "run_id": run_id, "arm": arm["arm_label"],
            "evaluated_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "decision": "CONTINUE" if ok else "STOP",
            "recorded_runner_verdict": rr.get("verdict"), "recorded_analysis_verdict": an.get("verdict"),
            "recorded_flags": flags, "recorded_problems": an.get("problems"),
            "note": "The recorded verdicts and flags are copied verbatim and are NOT changed by this policy decision.",
            "dispatch_lag_by_round": per_round, "checks": checks,
            "inputs_sha256": {"run_result.json": sha(rd / "run_result.json"), "analysis.json": sha(rd / "analysis.json"),
                              "independent_verification.json": sha(vpath) if vpath.exists() else None,
                              "AUTH_SERIES.txt": sha(auth), "stage_SHA256SUMS.txt": sha(stage / "SHA256SUMS.txt")},
            "checker_sha256": sha(Path(__file__))}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_id")
    ap.add_argument("--out", default="/home/mailf18/e1/closed-loop-20261123")
    ap.add_argument("--ws", default="/home/mailf18/e1/caliper-ws-benchmark/runs")
    ap.add_argument("--stage", default="/mnt/d/PLAN_BLOCK18/closed_loop_experiment_2026-11-22/stage_series")
    ap.add_argument("--auth", default="/mnt/d/PLAN_BLOCK18/closed_loop_experiment_2026-11-22/AUTH_SERIES.txt")
    ap.add_argument("--decisions", default=None, help="default <out>/s5a1_decisions")
    a = ap.parse_args(argv)
    if not re.fullmatch(r"cl(0[1-9]|1[0-2])", a.run_id):
        print("refusing: run id must be cl01..cl12")
        return 2
    out = Path(a.out)
    dec_dir = Path(a.decisions) if a.decisions else out / "s5a1_decisions"
    dst = dec_dir / f"{a.run_id}.json"
    if dst.exists():
        print(f"refusing: {dst} exists (decisions are written once)")
        return 2
    res = evaluate(a.run_id, out, Path(a.ws), Path(a.stage), Path(a.auth))
    dec_dir.mkdir(parents=True, exist_ok=True)
    dst.write_text(json.dumps(res, indent=2, sort_keys=True), encoding="utf-8")
    for c in res["checks"]:
        print(("PASS " if c["pass"] else "FAIL ") + c["id"] + " " + c["check"] + (f"  [{c['detail']}]" if not c["pass"] else ""))
    print(f"S5A1_DECISION {res['decision']} {a.run_id} | recorded verdict {res['recorded_analysis_verdict']} | flags {res['recorded_flags']}"
          f" | file {dst} sha256 {sha(dst)}")
    return 0 if res["decision"] == "CONTINUE" else 5


if __name__ == "__main__":
    sys.exit(main())
