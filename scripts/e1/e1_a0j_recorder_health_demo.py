"""A0j recorder-health demonstration on a SYNTHETIC day (2026-11-21). Offline: no model, forecaster, Caliper, Fabric, Docker, process or network.

    python scripts/e1/e1_a0j_recorder_health_demo.py --out-dir Integration/system_integration/a0j_2026-11-21/demo

A scripted recorder (it writes what ``e1_per_tx_recorder_v3.js`` writes: a health spool and a live submits file) feeds the real bridge,
the recorder coverage layer and the Stage 3 fixture pipeline. Same synthetic UTC day as A0i (2026-09-21; zero-arrival seconds 100 and 5000):

  source_evidence    healthy recorder AND an independent source count that agrees: the two zeros are GENUINE ZEROS, the predictive controller decides;
  heartbeat_only     the same healthy recorder with NO source count: the zeros are UNKNOWN (recorder_coverage_only), the predictive controller abstains;
  interrupted        the recorder reports a health failure at 05:00 that is never recovered: everything after is uncovered;
  missed_callbacks   the source issued traffic at second 2000 that the callback never saw: a source-count mismatch uncovers that second;
  restart_gap        the first recorder instance dies at 06:00 (no stop); a second instance starts at 12:00:00: the gap is never certified;
  tampered           one journal byte is altered after writing: the hash chain fails and the whole attestation is invalid.

Everything is SYNTHETIC and labelled so; nothing is a Caliper or Fabric measurement and no live coverage is claimed. The out-dir must not exist.
The Stage 3 inputs (parameters, controller policy, fixture) are the stored, read-only A0h demonstration inputs.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))

from fabric_perf.controllers import abstention_accounting as acc                  # noqa: E402
from fabric_perf.controllers import recorder_coverage as rc                       # noqa: E402
from fabric_perf.controllers import recorder_health_bridge as bridge              # noqa: E402
from fabric_perf.controllers import stage3_shadow_runner as runner               # noqa: E402
from fabric_perf.controllers import workload_coverage as wc                      # noqa: E402

A0H = REPO / "Integration" / "system_integration" / "a0h_2026-11-21"
PARAMS = A0H / "demo" / "run_20260921_strict" / "inputs" / "params.json"
POLICY_DOC = A0H / "fixtures" / "controller_policy_v2_h60.json"
FIXTURE = A0H / "fixtures" / "fixture_captured_2026-09-21_1200Z.json"
DAY = datetime(2026, 9, 21, tzinfo=timezone.utc)
DAY_MS = int(DAY.timestamp() * 1000)
SOURCE_ID = "synthetic-demo-day"
RUN = "a0j-demo"
LAST = 43_205
ZEROS = (100, 5000)
# SYNTHETIC test policy (hourly declared heartbeats keep the fixture short); NOT the engineering proposal and NOT a recommendation.
POLICY = bridge.RecorderHealthPolicy(heartbeat_interval_s=3600, max_heartbeat_gap_s=7200.0, freshness_limit_s=7200.0, clock_tolerance_ms=250.0)
BASIS = "synthetic_independent_source_counter"
# A0k: a source count is believed only with a descriptor that classifies as SOURCE-INDEPENDENT AND INTERVAL-SUITABLE. SYNTHETIC.
DESCRIPTOR = {"counter_id": "synthetic-generator-issue-counter", "kind": "generator_issue_counter", "independent_of_recorder": True,
              "derived_from": "synthetic_workload_generator_counter", "timestamp_semantics": "generator_issue_time", "resolution": "interval"}


def at(second: float) -> datetime:
    return DAY + timedelta(seconds=second)


def units(second: int) -> int:
    return 20 + (second % 7)


class Script:
    """One scripted recorder instance: ``observed`` is what its callback saw, ``truth`` what the synthetic source issued."""

    def __init__(self, instance: str, *, source_count: bool, missed=(), zeros=ZEROS, first: int = 0, last: int = LAST):
        self.instance, self.source_count, self.first = instance, source_count, first
        self.n, self.rows = 0, []
        self.observed = {s: units(s) for s in range(first, last + 1) if s not in zeros and s not in missed}
        self.truth = {s: units(s) for s in range(first, last + 1) if s not in zeros}

    def row(self, kind, t, **fields):
        self.n += 1
        self.rows.append({"schema": bridge.SPOOL_SCHEMA, "n": self.n, "kind": kind, "t_utc": wc.iso(at(t)), "mono_ms": t * 1000.0, "source_id": SOURCE_ID,
                          "collector_id": "demo-recorder", "collector_instance": self.instance, "run_id": RUN, "worker": 0, "round": 0, "label": "synthetic", **fields})

    def start(self, observation: int):
        self.row("start", observation - 0.5, observation_start_utc=wc.iso(at(observation)), timestamp_provenance="synthetic_scripted_clock",
                 coverage_scope=bridge.COVERAGE_SCOPE_RECORDER, evidence_origin=bridge.ORIGIN_SYNTHETIC, recorder_version="synthetic", options_sha256="0" * 64,
                 lifecycle={"initialized_utc": wc.iso(at(observation - 0.6)), "subscription_active_utc": wc.iso(at(observation - 0.5)), "window_open_utc": wc.iso(at(observation))},
                 declared_policy={"heartbeat_interval_ms": 3_600_000, "checkpoint_every_heartbeats": 60}, source_count_declared=self.source_count,
                 **({"source_counter": dict(DESCRIPTOR)} if self.source_count else {}))

    def beat(self, second: int, *, health="healthy", errors=0):
        through = lambda table: sum(u for s, u in table.items() if self.first <= s < second)             # noqa: E731
        fields = {"observed_through_utc": wc.iso(at(second)), "events_received": through(self.observed), "last_event_seq": None, "health": health,
                  "errors_since_last": errors, "phase": "heartbeat"}
        if self.source_count and health == "healthy":
            fields.update(source_events_expected=through(self.truth), source_basis=BASIS, source_independence_claim=True)
        self.row("heartbeat", second, **fields)

    def beats(self, seconds, **kwargs):
        for s in seconds:
            self.beat(s, **kwargs)


def cadence(last: int = LAST, until: int | None = None):
    seconds = [3600 * k for k in range(1, 13)] + list(range(43_201, last + 1))
    return [s for s in seconds if until is None or s <= until]


def write(out: Path, name: str, scripts: list[Script], *, tamper: bool = False):
    spools, journal = [], out / "journals" / f"{name}.jsonl"
    events = []
    for index, script in enumerate(scripts):
        spool = out / "spools" / f"{name}_{index}.health.jsonl"
        spool.write_text("".join(json.dumps(r) + "\n" for r in script.rows), encoding="utf-8", newline="")
        submits = out / "spools" / f"{name}_{index}.live.submits.csv"
        submits.write_text("seq_submit,t_submit_evt_ms,count\n" + "".join(f"{i},{DAY_MS + s * 1000 + 250},{u}\n" for i, (s, u) in enumerate(sorted(script.observed.items()))),
                           encoding="utf-8", newline="")
        report = bridge.ingest_spool(spool, journal, source_id=SOURCE_ID, policy=POLICY, resume=index > 0)
        spools.append(report)
        events += rc.read_submit_events(submits, instance=script.instance)[0]
    if tamper:
        text = journal.read_text(encoding="utf-8")
        journal.write_text(text.replace('"health":"healthy"', '"health":"degraded"', 1), encoding="utf-8")
    coverage = rc.RecorderVerifiedCoverage.from_recorder_journal(journal, source_id=SOURCE_ID, submit_events=events, policy=POLICY, run_id=RUN)
    source = rc.RecorderArrivalSource(source_id=SOURCE_ID, utc_day="2026-09-21", dataset_path="synthetic", evidence_origin=bridge.ORIGIN_SYNTHETIC)
    history = rc.RecorderCapturedHistory(rc.bin_submit_events(events, source), coverage=coverage, policy=POLICY)
    return spools, coverage, history


def case_scripts(name: str) -> list[Script]:
    if name in ("source_evidence", "tampered"):
        s = Script("demo:r0", source_count=True)
        s.start(0)
        s.beats(cadence())
        return [s]
    if name == "heartbeat_only":
        s = Script("demo:r0", source_count=False)
        s.start(0)
        s.beats(cadence())
        return [s]
    if name == "interrupted":
        s = Script("demo:r0", source_count=True)
        s.start(0)
        s.beats([3600 * k for k in range(1, 5)])
        s.row("error", 18_001, error_code="live_submit_stream_failed", message="synthetic: the live submits file stopped growing", affects_from_utc=wc.iso(at(14_400)))
        s.beats([s_ for s_ in cadence() if s_ > 18_001], health="degraded", errors=1)
        return [s]
    if name == "missed_callbacks":
        s = Script("demo:r0", source_count=True, missed=(2000,))
        s.start(0)
        s.beats(cadence())
        return [s]
    if name == "restart_gap":
        first = Script("demo:r0", source_count=True, last=21_600)
        first.start(0)
        first.beats([3600 * k for k in range(1, 7)])                                         # dies at 06:00, no stop
        second = Script("demo:r1", source_count=True, first=43_200, zeros=ZEROS)
        second.start(43_200)
        second.beats(range(43_201, LAST + 1))
        return [first, second]
    raise ValueError(name)


def run_one(name: str, out: Path) -> dict:
    spools, coverage, history = write(out, name, case_scripts(name), tamper=name == "tampered")
    inputs = runner.load_inputs(PARAMS, POLICY_DOC, FIXTURE)
    run_dir = out / name
    manifest = runner.run_fixture_shadow(inputs, run_dir, captured_history=history)
    problems = runner.verify_run_directory(run_dir)
    accounting = acc.account_run_directory(run_dir, segment=f"synthetic-day/{name}")
    (out / f"{name}_abstention_accounting.json").write_text(json.dumps(accounting, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    rows = [json.loads(line) for line in (run_dir / "shadow_decisions.jsonl").read_text(encoding="utf-8").splitlines()]
    predictive = [r for r in rows if r["controller"] == "predictive"]
    snapshot = coverage.snapshot(at(LAST))
    return {"run": name, "status": manifest["status"], "manifest_verifies": problems == [],
            "bridge": [{"spool_rows_ingested": r["spool_rows_ingested"], "has_stop": r["has_stop"], "problems": [p["code"] for p in r["problems"]]} for r in spools],
            "zero_100s": snapshot.classify(at(100), 0)[0].value, "zero_5000s": snapshot.classify(at(5000), 0)[0].value,
            "history_outcomes": [f["validation_outcome"] for f in manifest["captured_history"]["frames"]],
            "predictive": [{"frame": r["frame"]["index"], "frame_status": r["frame"]["status"], "outcome": r["outcome"], "reason": r["reason"],
                            "reason_codes": r["predictive_evidence"]["workload_observation"]["coverage"].get("abstention_reason_codes"),
                            "integrity": r["predictive_evidence"]["workload_observation"]["coverage"]["integrity"]["status"]} for r in predictive],
            "static_reactive_outcomes": sorted({(r["controller"], r["outcome"]) for r in rows if r["controller"] != "predictive" and r["frame"]["index"] > 0}),
            "predictive_accounting": {k: accounting["controllers"]["predictive"][k] for k in (
                "total_scheduled_decision_opportunities", "initialization_frames", "controller_specific_abstentions", "valid_recommendations", "decision_coverage_rate")},
            "recorder_evidence": snapshot.summary()["recorder_evidence"]}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--out-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.out_dir.exists():
        print(f"REFUSED: {args.out_dir} already exists; every run gets a new directory")
        return 2
    for sub in ("journals", "spools"):
        (args.out_dir / sub).mkdir(parents=True)
    summary = {"schema": "a0j_recorder_health_demo/1", "synthetic": True, "is_fabric_observation": False, "live_source_coverage_verified": False,
               "note": "scripted recorder on a scripted clock; forecast and performance values are scripted fixtures; nothing was loaded, measured or sent anywhere",
               "zero_seconds": list(ZEROS), "policy": POLICY.as_dict(),
               "runs": [run_one(name, args.out_dir) for name in ("source_evidence", "heartbeat_only", "interrupted", "missed_callbacks", "restart_gap", "tampered")]}
    (args.out_dir / "demo_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
