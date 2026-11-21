"""A0i coverage demonstration on a SYNTHETIC captured day (2026-11-21). Offline: no model, forecaster, Fabric, Docker, process or network.

    python scripts/e1/e1_a0i_coverage_demo.py --out-dir Integration/system_integration/a0i_2026-11-21/demo

Three Stage 3 fixture runs over the same synthetic UTC day (2026-09-21, every second a positive count except seconds 100 and 5000):

  attested      a hash-chained collector-health journal verifies continuous capture, so the two zeros are GENUINE ZEROS and the predictive
                controller proceeds (forecast and performance values are scripted fixtures);
  no_evidence   the same counts with no collector evidence: the zeros are UNKNOWN, the history is incomplete and the predictive controller abstains;
  interrupted   the collector reports an error at 12:00:02Z and never recovers: coverage from then on is INTERRUPTED and the history stops being valid.

Each run directory is verified against its manifest and accounted with ``abstention_accounting``. The out-dir must not exist. The inputs
(parameters, controller policy, fixture) are the stored, read-only A0h demonstration inputs; nothing stored is edited.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
for entry in (str(REPO / "src"),):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from fabric_perf.controllers import abstention_accounting as acc                  # noqa: E402
from fabric_perf.controllers import captured_history as ch                        # noqa: E402
from fabric_perf.controllers import collector_health as chj                       # noqa: E402
from fabric_perf.controllers import stage3_shadow_runner as runner               # noqa: E402
from fabric_perf.controllers import workload_coverage as wc                      # noqa: E402

A0H = REPO / "Integration" / "system_integration" / "a0h_2026-11-21"
PARAMS = A0H / "demo" / "run_20260921_strict" / "inputs" / "params.json"
POLICY = A0H / "fixtures" / "controller_policy_v2_h60.json"
FIXTURE = A0H / "fixtures" / "fixture_captured_2026-09-21_1200Z.json"
DAY = datetime(2026, 9, 21, tzinfo=timezone.utc)
SOURCE_ID = "synthetic-demo-day"
RUN_ID = "a0i-demo"
ZERO_SECONDS = (100, 5000)
LAST_SECOND = 43_205                                              # the fixture has five 1 s steps from 12:00:00Z
POLICY_GAP_S = 7200.0                                             # a synthetic placeholder so hourly heartbeats suffice


class Clock:
    def __init__(self, second: float = -0.5):
        self.second = second

    def __call__(self) -> datetime:
        return DAY + timedelta(seconds=self.second)


def at(second: float) -> datetime:
    return DAY + timedelta(seconds=second)


def binned() -> ch.BinnedDay:
    counts = [20 + (i % 7) for i in range(ch.SECONDS_PER_DAY)]
    for second in ZERO_SECONDS:
        counts[second] = 0
    return ch.BinnedDay(ch.CapturedSource(source_id=SOURCE_ID, utc_day="2026-09-21"), tuple(counts),
                        {"input": "synthetic", "duplicates_dropped": 0, "duplicate_handling": "none (synthetic)"})


def write_journal(path: Path, *, error_at: float | None = None) -> None:
    clock = Clock()
    journal = chj.CollectorHealthJournal(path, source_id=SOURCE_ID, collector_id="demo-collector", run_id=RUN_ID, clock=clock,
                                         collector_instance="demo-instance-1")
    journal.start(observation_start_utc=at(0), coverage_scope=wc.SCOPE_SOURCE_STREAM)      # A0k: a collector that itself receives the source stream
    for second in [3600 * k for k in range(1, 13)] + list(range(43_201, LAST_SECOND + 1)):
        if error_at is not None and second > error_at:
            break
        clock.second = second
        journal.heartbeat(at(second), events_received=0)
    if error_at is not None:
        clock.second = error_at + 0.5
        journal.error("DEMO_COLLECTOR_ERROR", "synthetic: the collector lost events from this second on")


def run_one(name: str, out_dir: Path, coverage: wc.VerifiedCoverage) -> dict:
    inputs = runner.load_inputs(PARAMS, POLICY, FIXTURE)
    run_dir = out_dir / name
    manifest = runner.run_fixture_shadow(inputs, run_dir, captured_history=ch.CapturedHistory(binned(), coverage=coverage))
    problems = runner.verify_run_directory(run_dir)
    accounting = acc.account_run_directory(run_dir, segment=f"synthetic-day/{name}")
    (out_dir / f"{name}_abstention_accounting.json").write_text(json.dumps(accounting, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    rows = [json.loads(line) for line in (run_dir / "shadow_decisions.jsonl").read_text(encoding="utf-8").splitlines()]
    predictive = [r for r in rows if r["controller"] == "predictive"]
    return {"run": name, "status": manifest["status"], "manifest_verifies": problems == [], "coverage_mode": manifest["captured_history"]["coverage_mode"],
            "history_outcomes": [f["validation_outcome"] for f in manifest["captured_history"]["frames"]],
            "predictive": [{"frame": r["frame"]["index"], "frame_status": r["frame"]["status"], "outcome": r["outcome"], "reason": r["reason"],
                            "contract_satisfied": r["predictive_evidence"]["workload_observation"]["coverage"]["contract_satisfied"],
                            "coverage_integrity": r["predictive_evidence"]["workload_observation"]["coverage"]["integrity"]["status"]} for r in predictive],
            "predictive_accounting": {k: accounting["controllers"]["predictive"][k] for k in (
                "total_scheduled_decision_opportunities", "initialization_frames", "controller_specific_abstentions", "valid_recommendations",
                "decision_coverage_rate")}}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.out_dir.exists():
        print(f"REFUSED: {args.out_dir} already exists; every run gets a new directory")
        return 2
    args.out_dir.mkdir(parents=True)
    journals = args.out_dir / "journals"
    journals.mkdir()
    write_journal(journals / "attested.jsonl")
    write_journal(journals / "interrupted.jsonl", error_at=43_202)
    policy = wc.CoveragePolicy(max_heartbeat_gap_s=POLICY_GAP_S)
    cases = {
        "attested": wc.VerifiedCoverage.from_journal_file(journals / "attested.jsonl", source_id=SOURCE_ID, policy=policy, run_id=RUN_ID),
        "no_evidence": wc.VerifiedCoverage.no_evidence(SOURCE_ID, "synthetic: no collector evidence was supplied"),
        "interrupted": wc.VerifiedCoverage.from_journal_file(journals / "interrupted.jsonl", source_id=SOURCE_ID, policy=policy, run_id=RUN_ID),
    }
    summary = {"schema": "a0i_coverage_demo/1", "synthetic": True, "is_fabric_observation": False,
               "note": "synthetic day; forecast and performance values are scripted fixtures; nothing was loaded or sent anywhere",
               "zero_seconds": list(ZERO_SECONDS),
               "runs": [run_one(name, args.out_dir, coverage) for name, coverage in cases.items()]}
    snapshot = cases["attested"].snapshot(at(LAST_SECOND))
    summary["classification_examples"] = {
        "attested_zero_at_100s": snapshot.classify(at(100), 0)[0].value,
        "attested_positive_at_50s": snapshot.classify(at(50), 25)[0].value,
        "no_evidence_zero_at_100s": cases["no_evidence"].snapshot(at(LAST_SECOND)).classify(at(100), 0)[0].value,
        "interrupted_zero_at_43203s": cases["interrupted"].snapshot(at(LAST_SECOND)).classify(at(43_203), 0)[0].value,
        "interrupted_positive_at_43203s": cases["interrupted"].snapshot(at(LAST_SECOND)).classify(at(43_203), 25)[0].value,
        "attested_zero_before_12_00_z_at_5000s": snapshot.classify(at(5000), 0)[0].value}
    (args.out_dir / "demo_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
