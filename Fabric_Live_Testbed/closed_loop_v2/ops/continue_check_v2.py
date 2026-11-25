"""v2 continuation rule, PRE-REGISTERED with the protocol (not written after seeing any run). Stdlib, read-only on the run's evidence.

STOP (exit 5) only for INTEGRITY failures. Performance outcomes (latency, saturation, failed transactions, driver lag, a client-limited
round) are DATA, are classified in the analysis, and never stop the series: stopping on them would erase exactly the evidence X0 exists to find.

CONTINUE requires all of:
  C01 runner console records "runner exit code 0"
  C02 runner verdict in {VALID, VALID_WITH_FLAGS}, no step failure, analysis.problems empty
  C03 independent verification ran; its only failing checks are the two "verdict exactly VALID" checks, and those fail only when the
      verdict is VALID_WITH_FLAGS
  C04 every analysis flag is DISPATCH_LAG or FAILED_TRANSACTIONS (CLOCK_SUSPECT or anything else stops)
  C05 no ERRO/PANI line in the preserved container logs; 6 containers running with 0 restarts after the run
  C06 run_result's stage and authorization hashes equal the files in force
Usage: continue_check_v2.py <run_id> [--out OUT] [--stage STAGE] [--auth AUTH] [--decisions DIR]
Exit 0 = CONTINUE, 5 = STOP, 2 = usage / decision file exists.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path

POLICY_ID = "LPC-v1-continuation"
VERDICT_CHECKS = {"runner verdict exactly VALID", "analysis verdict exactly VALID"}
ALLOWED_FLAG = re.compile(r"^(DISPATCH_LAG round \d+ p99 \d+ ms|FAILED_TRANSACTIONS \d+)$")


def sha(p) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def evaluate(run_id: str, out: Path, stage: Path, auth: Path) -> dict:
    rd = out / run_id
    rr = json.loads((rd / "run_result.json").read_text())
    an = json.loads((rd / "analysis.json").read_text())
    iv = json.loads((out / f"{run_id}_verification" / "independent_verification.json").read_text())
    checks = []

    def add(cid, name, ok, detail=""):
        checks.append({"id": cid, "check": name, "pass": bool(ok), "detail": str(detail)})

    console = (out / f"{run_id}_runner_console.log").read_text(errors="replace")
    add("C01", "runner exit code 0", "runner exit code 0" in console)
    verdict = rr.get("verdict")
    add("C02", "verdict VALID or VALID_WITH_FLAGS, no step failure, no analysis problems",
        verdict in ("VALID", "VALID_WITH_FLAGS") and not rr.get("step_failure") and not an.get("problems"),
        f"{verdict} {rr.get('step_failure')} {an.get('problems')}")
    failing = [c["check"] for c in iv["checks"] if not c["pass"]]
    extra = [n for n in failing if n not in VERDICT_CHECKS]
    verdict_ok = (not failing) or (verdict == "VALID_WITH_FLAGS" and set(failing) <= VERDICT_CHECKS)
    add("C03", "independent verification: only the verdict checks may fail, and only for VALID_WITH_FLAGS", not extra and verdict_ok,
        f"failing {failing}")
    bad_flags = [f for f in an.get("flags", []) if not ALLOWED_FLAG.match(f)]
    add("C04", "every flag is DISPATCH_LAG or FAILED_TRANSACTIONS (data, not a stop)", not bad_flags, f"flags {an.get('flags')} bad {bad_flags}")
    log_dir = out / f"{run_id}_fabric_logs"
    n_bad = 0
    for f in log_dir.glob("*.log"):
        n_bad += sum(1 for line in f.read_text(errors="replace").splitlines() if re.search(r" (ERRO|PANI)", line))
    after = (log_dir / "containers_after.txt").read_text() if (log_dir / "containers_after.txt").exists() else ""
    add("C05", "no ERRO/PANI lines; 6 containers running with 0 restarts", n_bad == 0 and after.count("running=true restarts=0") == 6,
        f"erro_pani {n_bad}")
    add("C06", "run bound to the stage and authorization in force",
        rr.get("stage_sha256") == sha(stage / "SHA256SUMS.txt") and rr.get("authorization_sha256") == sha(auth),
        f"stage {str(rr.get('stage_sha256'))[:12]} auth {str(rr.get('authorization_sha256'))[:12]}")
    return {"policy": POLICY_ID, "run_id": run_id, "decision": "CONTINUE" if all(c["pass"] for c in checks) else "STOP",
            "recorded_runner_verdict": verdict, "recorded_flags": an.get("flags"), "independent_failing_checks": failing, "checks": checks}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("run_id")
    ap.add_argument("--out", type=Path, default=Path(os.environ.get("LPC_OUT", "/home/mailf18/e1/lpc-20261126")))
    ap.add_argument("--stage", type=Path, required=True)
    ap.add_argument("--auth", type=Path, required=True)
    ap.add_argument("--decisions", type=Path)
    a = ap.parse_args(argv)
    ddir = a.decisions or a.out / "decisions_v2"
    ddir.mkdir(parents=True, exist_ok=True)
    target = ddir / f"{a.run_id}.json"
    if target.exists():
        print("refusing: decision file exists", target)
        return 2
    try:
        res = evaluate(a.run_id, a.out, a.stage, a.auth)
    except (OSError, ValueError, KeyError) as exc:
        res = {"policy": POLICY_ID, "run_id": a.run_id, "decision": "STOP", "checks": [], "error": repr(exc)}
    target.write_text(json.dumps(res, indent=2, sort_keys=True) + "\n")
    for c in res["checks"]:
        print(("PASS " if c["pass"] else "FAIL ") + c["id"] + " " + c["check"] + ("" if c["pass"] else f"  [{c['detail']}]"))
    print("LPC_DECISION", res["decision"], a.run_id, "| verdict", res.get("recorded_runner_verdict"), "| flags", res.get("recorded_flags"),
          "|", target, hashlib.sha256(target.read_bytes()).hexdigest())
    return 0 if res["decision"] == "CONTINUE" else 5


if __name__ == "__main__":
    sys.exit(main())
