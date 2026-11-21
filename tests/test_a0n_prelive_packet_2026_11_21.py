"""A0n (2026-11-21): offline checks of the pre-live timing register, the SourceCount contract mapping, the approval packet and the disabled live paths.

What this file does: reads repository files, hashes them, runs the unmodified offline libraries on STORED synthetic evidence (the A0m node-driver run, produced by the real
issue-log writer and recorder v3 under a SCRIPTED clock with fake Caliper objects) and on temporary copies of it. Nothing here contacts Caliper, Fabric, Docker, WSL,
Prometheus or a network; no model is loaded; nothing is measured. Values called "fixture" below are test parameters, not recommendations and not measurements.
"""

from __future__ import annotations

import ast
import dataclasses
import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
for _entry in (str(REPO / "src"), str(REPO / "scripts" / "e1")):
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

from fabric_perf.controllers import channel_selection as cs  # noqa: E402
from fabric_perf.controllers import clock_preflight as cp  # noqa: E402
from fabric_perf.controllers import issue_log_reader as ilr  # noqa: E402
from fabric_perf.controllers import prelive_contract as pc  # noqa: E402
from fabric_perf.controllers import recorder_health_bridge as bridge  # noqa: E402
from fabric_perf.controllers import source_completeness as sc  # noqa: E402
from fabric_perf.controllers import stage3_predictive as sp  # noqa: E402
from fabric_perf.controllers.stage3_frame_quality import Stage3FrameQualityContract  # noqa: E402
from fabric_perf.controllers.telemetry_freshness import FreshnessPolicy  # noqa: E402

SI = REPO / "Integration" / "system_integration"
A0N_DIR = SI / "a0n_2026-11-21"
E1 = REPO / "docs" / "research" / "e1"
A0M_MANIFEST = E1 / "prelive_candidate_manifest_2026-11-21_a0m.json"
A0M_MANIFEST_SHA = "64687487752fc097f4d18e5e86593936421b0738dc967d0c23f06b3b3ccdd0fd"
EVIDENCE = E1 / "a0m_dry_render_evidence_2026-11-21"
RENDER = EVIDENCE / "render" / "a0m-evidence-001"
REGISTER = A0N_DIR / "timing_parameter_register.json"
SPEC = SI / "A0N_TIMING_MEASUREMENT_SPECIFICATION_2026-11-21.md"
DESIGN = SI / "A0N_SOURCECOUNT_PROVIDER_DESIGN_2026-11-21.md"
PACKET = SI / "A0N_OWNER_APPROVAL_PACKET_2026-11-21.md"
RUNBOOK = SI / "A0N_BOUNDED_LIVE_VALIDATION_RUNBOOK_2026-11-21.md"
WORKSHEET = A0N_DIR / "approval_worksheet_a0m_candidate_manifest.tsv"

# SHA-256 of every earlier candidate manifest, captured at the start of A0n (before anything was written).
EARLIER = {
    "prelive_candidate_manifest_2026-11-21.json": "13640a7f683adcfec96f26f9060b05f29306bb2ebcfeb4c4b90c5fc43fa2b984",
    "prelive_candidate_manifest_2026-11-21_a0g.json": "491c7d0c296515bf24cd2a8d4462be3f34a81926f933e5d7542a45105eb5a5e7",
    "prelive_candidate_manifest_2026-11-21_a0h.json": "d6c451cfae089642d8c5c6be2c80d1fbdef7f718d1d2888c95d86275c1fda689",
    "prelive_candidate_manifest_2026-11-21_a0i.json": "7bc73bc7f648c92e3d515e4e9e98f41df1a552d2c9bd8bad204f78761d2a7c66",
    "prelive_candidate_manifest_2026-11-21_a0j.json": "831cb76ecdbd75318ebc7d4fe9be0d0c3eedfe7873a9a2190fa3446dd736c594",
    "prelive_candidate_manifest_2026-11-21_a0k.json": "d648a7151bf7d2688d2f658594a4d5adae4d08201b4428de5924372688ff756c",
    "prelive_candidate_manifest_2026-11-21_a0l.json": "b256ba9e04e699be412d362c3d167a7c5b58204c442cb3bb17afac5c2b0efcb4",
}
PINNED_AND_FROZEN = {
    "scripts/e1/e1_la2_render.py": "f77a83e7de106b3c1dac28f93333e8dbaf67ea869836944b906f9bc1aabe3d83",
    "scripts/e1/caliper/e1_per_tx_recorder.js": "b5496af3ca038728ff7490b45db726f27ebf0f7efe09b36f6d9330a8cbd1b0a8",
    "scripts/e1/run_contemporaneous_capture.py": "81dc1f4f77f85387bf9578e4293f9793051b93d5724496586878797ef904bd54",
    "fabric-testbed/caliper/workloads/e1_create_asset.js": "c5c234b71666e6b8e135f26f8c65d2e1639c3d54784213cfca0cc15a50f5667f",
    "fabric-testbed/caliper/workloads/e1_workload_core.js": "c2879d846f4630c9ed4e1386d1280aeb78b2a3ae5184823b504c170e142f5522",
}


def sha(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def register() -> dict:
    return json.loads(REGISTER.read_text(encoding="utf-8"))


def manifest() -> dict:
    return json.loads(A0M_MANIFEST.read_text(encoding="utf-8"))


def utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def iso_ms(text: str) -> int:
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)


# ======================================================================================================== register
class TestRegister:
    def test_01_register_is_proposed_and_approves_nothing(self):
        r = register()
        assert r["schema"] == "a0n_timing_parameter_register/1"
        assert r["approved"] is False and r["approval_record"] is None and r["status"] == "PROPOSED_NOT_APPROVED"
        ids = [p["id"] for p in r["parameters"]]
        assert ids == [f"P{i:02d}" for i in range(1, len(ids) + 1)]
        for p in r["parameters"]:
            assert "approved_value" not in p and p["status"] in r["status_vocabulary"]
            for key in ("name", "carrier", "units", "constraints", "evidence_required", "failure_behavior", "approval_decision", "measurement_ids"):
                assert p[key] not in (None, "", []) or key == "measurement_ids"
            for c in p["constraints"]:
                assert c["class"] in r["constraint_classes"]

    def test_02_candidates_are_exactly_the_four_a0j_values_and_labelled_unverified(self):
        r = register()
        candidates = {p["id"]: p["candidate"] for p in r["parameters"] if p["candidate"] is not None}
        proposal = bridge.CANDIDATE_PROPOSAL
        assert set(candidates) == {"P01", "P02", "P03", "P19"}
        assert candidates["P01"]["value"] == proposal["heartbeat_interval_s"]
        assert candidates["P02"]["value"] == proposal["max_heartbeat_gap_s"]
        assert candidates["P03"]["value"] == proposal["freshness_limit_s"]
        assert candidates["P19"]["value"] == proposal["clock_tolerance_ms"]
        assert all("unverified" in c["label"] for c in candidates.values())
        assert "not scientifically validated" in proposal["label"]
        for p in r["parameters"]:
            if p["status"] == "CANDIDATE_UNVERIFIED":
                assert p["id"] in candidates

    def test_03_code_defaults_in_the_register_match_the_code(self):
        by_id = {p["id"]: p for p in register()["parameters"]}
        fp = FreshnessPolicy()
        assert (fp.max_age_s, fp.max_future_skew_s, fp.max_source_skew_s, fp.max_clock_discrepancy_s) == (5.0, 0.0, 5.0, 1.0)
        assert by_id["P04"]["code_default"]["value"] == 5.0 and by_id["P13"]["code_default"]["value"] == 0.0
        assert by_id["P12"]["code_default"]["value"] == 5.0 and by_id["P14"]["code_default"]["value"] == 1.0
        pol = ilr.IssueLogPolicy(run_id="r", worker_id="w0", clock_domain="d")
        assert (pol.max_tick_gap_ms, pol.max_clock_skew_ms) == (5000, 1000) and pol.max_bytes_per_poll == 1 << 20
        assert (pol.max_records_per_poll, pol.max_line_bytes, pol.max_unresolved) == (10_000, 16_384, 10_000)
        assert by_id["P22"]["code_default"]["value"] == 5000 and by_id["P24"]["code_default"]["value"] == 1000
        workload = (REPO / "fabric-testbed/caliper/workloads/e1_create_asset_issue_logged.js").read_text(encoding="utf-8")
        writer = (REPO / "fabric-testbed/caliper/workloads/e1_issue_log.js").read_text(encoding="utf-8")
        v3 = (REPO / "scripts/e1/caliper/e1_per_tx_recorder_v3.js").read_text(encoding="utf-8")
        assert "const DEFAULT_TICK_MS = 1000;" in workload and by_id["P21"]["code_default"]["value"] == 1000
        assert "o.fsyncPolicy === undefined ? 'tick'" in writer and by_id["P23"]["code_default"]["value"] == "tick"
        assert "health.checkpointEveryHeartbeats === undefined ? 60" in v3 and by_id["P26"]["code_default"]["value"] == 60
        for pid in ("P01", "P02", "P03", "P05", "P09", "P10", "P11", "P15", "P16", "P17", "P18", "P19", "P20"):
            assert by_id[pid]["code_default"] is None and by_id[pid]["operator_required"] is True

    def test_04_measurement_ids_exist_in_the_specification_with_all_fourteen_items(self):
        text = SPEC.read_text(encoding="utf-8")
        sheets = re.split(r"\n### (T\d\d) — ", text)
        names = sheets[1::2]
        assert names == [f"T{i:02d}" for i in range(1, 13)]
        for name, body in zip(names, sheets[2::2]):
            numbers = [int(m.group(1)) for m in re.finditer(r"^\| (\d+) \| ", body, re.M)]
            assert numbers[:14] == list(range(1, 15)), name
        used = {m for p in register()["parameters"] for m in p["measurement_ids"]}
        assert used <= set(names)

    def test_05_every_cited_line_still_says_what_the_register_claims(self):
        """The register cites source lines; a drifting file must fail loudly rather than leave a stale citation."""
        def line(path: str, number: int) -> str:
            return (REPO / path).read_text(encoding="utf-8").splitlines()[number - 1]
        assert "heartbeat_interval_s: int" in line("src/fabric_perf/controllers/recorder_health_bridge.py", 92)
        assert "gap < 2 * interval" in line("src/fabric_perf/controllers/recorder_health_bridge.py", 103)
        assert "fresh < gap" in line("src/fabric_perf/controllers/recorder_health_bridge.py", 106)
        assert "1000 * interval" in line("src/fabric_perf/controllers/recorder_health_bridge.py", 109)
        assert "if not window_s > poll_s" in line("src/fabric_perf/controllers/prelive_contract.py", 292)
        assert "span < planned_round_s" in line("src/fabric_perf/controllers/prelive_contract.py", 305)
        assert "MAX_ALLOWED_OBSERVATION_AGE_S = 30.0" in line("src/fabric_perf/controllers/telemetry_freshness.py", 33)
        assert "max_tick_gap_ms: int = 5_000" in line("src/fabric_perf/controllers/issue_log_reader.py", 112)
        assert "skew > pol.max_clock_skew_ms" in line("src/fabric_perf/controllers/issue_log_reader.py", 343)
        assert "tickIntervalMs < 10" in line("fabric-testbed/caliper/workloads/e1_issue_log.js", 100)
        assert "defaultMono" in line("fabric-testbed/caliper/workloads/e1_issue_log.js", 81)
        assert "hb < 1000 || hb % 1000 !== 0" in line("scripts/e1/caliper/e1_per_tx_recorder_v3.js", 66)
        assert "if floor > read_time" in line("src/fabric_perf/controllers/issue_log_coverage.py", 204)
        assert ':r0"' in line("src/fabric_perf/controllers/issue_log_coverage.py", 160)
        assert "DEFAULT_CANDIDATE_MANIFEST" in line("scripts/e1/e1_stage3_shadow_runner.py", 42) and "2026-11-21.json" in line("scripts/e1/e1_stage3_shadow_runner.py", 42)


# ===================================================================================== hard constraints (executed)
def _bridge(interval, gap, fresh, tol):
    return bridge.RecorderHealthPolicy(heartbeat_interval_s=interval, max_heartbeat_gap_s=gap, freshness_limit_s=fresh, clock_tolerance_ms=tol)


def _params(**changes):
    raw = json.loads((EVIDENCE / "inputs" / "a0e_params.json").read_text(encoding="utf-8"))
    for dotted, value in changes.items():
        section, key = dotted.split("__")
        raw[section][key] = value
    return raw


def _issue_codes(raw) -> set:
    try:
        pc.validate_operator_parameters(raw)
    except pc.PreliveParameterError as exc:
        return {i.code for i in exc.issues}
    return set()


def _raises(exc_type, fn, *args, **kwargs) -> bool:
    try:
        fn(*args, **kwargs)
    except exc_type:
        return True
    return False


# register id -> a callable that executes the cited hard constraint and returns True when the code refuses a violation and accepts a legal value
COVERED = {
    "P01": lambda: _raises(bridge.BridgeError, _bridge, 1.5, 5.0, 10.0, 250.0) and _raises(bridge.BridgeError, _bridge, True, 5.0, 10.0, 250.0) and _bridge(1, 5.0, 10.0, 250.0),
    "P02": lambda: _raises(bridge.BridgeError, _bridge, 2, 3.0, 10.0, 250.0) and _bridge(2, 4.0, 10.0, 250.0),
    "P03": lambda: _raises(bridge.BridgeError, _bridge, 1, 5.0, 4.0, 250.0) and _bridge(1, 5.0, 5.0, 250.0),
    "P19": lambda: _raises(bridge.BridgeError, _bridge, 1, 5.0, 10.0, 1000.0) and _bridge(1, 5.0, 10.0, 999.0),
    "P04": lambda: _raises(ValueError, FreshnessPolicy, max_age_s=30.1) and _raises(ValueError, FreshnessPolicy, max_age_s=0) and FreshnessPolicy(max_age_s=30.0),
    "P12": lambda: _raises(ValueError, FreshnessPolicy, max_source_skew_s=30.1) and _raises(ValueError, FreshnessPolicy, max_source_skew_s=0) and FreshnessPolicy(max_source_skew_s=30.0),
    "P13": lambda: (_raises(ValueError, FreshnessPolicy, max_future_skew_s=1.01) and _raises(ValueError, FreshnessPolicy, max_future_skew_s=-0.1)
                    and FreshnessPolicy(max_future_skew_s=1.0) and FreshnessPolicy(max_future_skew_s=0.0)),
    "P14": lambda: _raises(ValueError, FreshnessPolicy, max_clock_discrepancy_s=5.01) and _raises(ValueError, FreshnessPolicy, max_clock_discrepancy_s=0) and FreshnessPolicy(max_clock_discrepancy_s=5.0),
    "P09": lambda: _issue_codes(_params()) == set() and "collector_stops_before_round_ends" in _issue_codes(_params(ledger_collector__max_polls=50)),
    "P10": lambda: "collector_stops_before_round_ends" in _issue_codes(_params(ledger_collector__max_polls=50)),
    "P11": lambda: ("window_not_longer_than_poll" in _issue_codes(_params(telemetry_adapter__window_s=1.0))
                    and "window_not_longer_than_poll" in _issue_codes(_params(telemetry_adapter__window_s=0.5))
                    and "window_not_longer_than_poll" not in _issue_codes(_params(telemetry_adapter__window_s=1.001))),
    "P15": lambda: all(_raises(ValueError, Stage3FrameQualityContract, max_consecutive_abstentions=v) for v in (0, True, None)) and Stage3FrameQualityContract(max_consecutive_abstentions=1),
    "P16": lambda: (all(_raises(ValueError, Stage3FrameQualityContract, max_consecutive_abstentions=1, max_initialization_frames=v) for v in (0, True))
                    and Stage3FrameQualityContract(max_consecutive_abstentions=1, max_initialization_frames=None)
                    and Stage3FrameQualityContract(max_consecutive_abstentions=1, max_initialization_frames=1)),
    "P17": lambda: all(_raises(ValueError, cp.assess_clock_agreement, [], tolerance_ms=1.0, min_valid_samples=v, max_wall_monotonic_disagreement_ms=1.0) for v in (0, True, None)),
    "P18": lambda: _raises(ValueError, cp.assess_clock_agreement, [], tolerance_ms=None, min_valid_samples=1, max_wall_monotonic_disagreement_ms=1.0),
    "P20": lambda: _raises(ValueError, cp.assess_clock_agreement, [], tolerance_ms=1.0, min_valid_samples=1, max_wall_monotonic_disagreement_ms=None),
    "P22": lambda: _raises(ilr.IssueLogError, ilr.IssueLogPolicy, run_id="r", worker_id="w0", clock_domain="d", max_tick_gap_ms=0),
    "P24": lambda: _raises(ilr.IssueLogError, ilr.IssueLogPolicy, run_id="r", worker_id="w0", clock_domain="d", max_clock_skew_ms=-1),
    "P25": lambda: _raises(ilr.IssueLogError, ilr.IssueLogPolicy, run_id="r", worker_id="w0", clock_domain="d", max_bytes_per_poll=100, max_line_bytes=200),
}


class TestHardConstraints:
    def test_06_the_covered_set_equals_the_set_marked_test_verified(self):
        marked = {p["id"] for p in register()["parameters"] if any(c.get("test_verified") for c in p["constraints"])}
        assert marked == set(COVERED)

    @pytest.mark.parametrize("pid", sorted(COVERED))
    def test_07_each_marked_constraint_is_enforced_by_the_code(self, pid):
        assert COVERED[pid]()

    def test_08_adapter_window_must_exceed_the_poll_interval_is_preserved(self):
        base = _params()
        assert base["telemetry_adapter"]["window_s"] > base["ledger_collector"]["poll_interval_s"]
        assert "window_not_longer_than_poll" in _issue_codes(_params(telemetry_adapter__window_s=base["ledger_collector"]["poll_interval_s"]))

    def test_09_no_default_for_missing_operator_values(self):
        raw = _params()
        del raw["telemetry_adapter"]["window_s"]
        assert "missing" in _issue_codes(raw)
        raw = _params()
        raw["stage3"]["max_consecutive_abstentions"] = None
        assert _issue_codes(raw)


# ================================================================================================== SourceCount contract
RUN, WORKER, DOMAIN = "a0m-dry-001", "w0", "caliper-worker-host:node_date_now"
ROUND1 = EVIDENCE / "node_driver_run" / "round1"
LOG1 = ROUND1 / "issuelog" / "a0m-dry-001_w0_r1_m-a0m-dry-001.issuelog.jsonl"
SPOOL1 = ROUND1 / "recorder" / "a0m-dry-001_w0_r1_m-a0m-dry-001.health.jsonl"
FIXTURE_LAG_MS = 100        # fixture value for max_callback_lag_ms: a test parameter, not a recommendation and not a measurement


def policy(**changes) -> ilr.IssueLogPolicy:
    values = dict(run_id=RUN, worker_id=WORKER, clock_domain=DOMAIN, max_tick_gap_ms=5000, max_clock_skew_ms=1000)   # fixture values
    values.update(changes)
    return ilr.IssueLogPolicy(**values)


def read_log(path: Path, pol: ilr.IssueLogPolicy, read_ms: int) -> ilr.IssueLogTail:
    tail = ilr.IssueLogTail(path, pol)
    tail.poll(utc(read_ms))
    return tail


def last_event_ms() -> int:
    return json.loads(LOG1.read_text(encoding="ascii").splitlines()[-1])["t_ms"]


def read_ms() -> int:
    return last_event_ms() + 1000      # scripted read time: one second after the last record (a test parameter)


class TestSourceCountContract:
    def test_10_the_stored_logs_carry_no_timing_information(self):
        """The evidence was produced under a scripted clock: every intent/attempt pair has a 0 ms monotonic delta. It validates the mapping, not any timing."""
        for round_dir in ("round0", "round1"):
            log = next((EVIDENCE / "node_driver_run" / round_dir / "issuelog").glob("*.jsonl"))
            rows = [json.loads(x) for x in log.read_text(encoding="ascii").splitlines()]
            pairs = [(a, b) for a, b in zip(rows, rows[1:]) if a["kind"] == "intent" and b["kind"] == "attempt"]
            assert len(pairs) == 5 and all(b["mono_ms"] - a["mono_ms"] == 0 for a, b in pairs)
        assert LOG1.stat().st_size == 8100 and len(LOG1.read_text(encoding="ascii").splitlines()) == 22     # the ~368 bytes per record quoted in the design

    def test_11_identity_counts_and_descriptor(self):
        before = sorted(p.name for p in LOG1.parent.iterdir())
        tail = read_log(LOG1, policy(), read_ms())
        assert tail.error is None and tail.start_record["worker_id"] == WORKER and tail.start_record["round_index"] == 1
        assert tuple(tail.start_record["worker_roster"]) == (WORKER,) and tail.start_record["total_workers"] == 1
        counts = tail.counts()
        assert (counts["intents"], counts["attempted"], counts["returned"], counts["rejected"], counts["unresolved"], counts["intent_without_attempt"]) == (5, 5, 5, 0, 0, 0)
        assert tail.writer_state()["state"] == ilr.W_CLOSED_CLEAN
        summary = tail.summary()
        assert sc.classify_descriptor(summary["descriptor"]).certifies_intervals is True
        assert "ledger commit" in summary["not_claimed"] and "external Ethereum mempool capture" in summary["not_claimed"]
        assert sorted(p.name for p in LOG1.parent.iterdir()) == before          # the reader wrote nothing

    def test_12_generator_count_equals_recorder_events_at_every_heartbeat(self):
        tail = read_log(LOG1, policy(), read_ms())
        spool = [json.loads(x) for x in SPOOL1.read_text(encoding="utf-8").splitlines()]
        start = spool[0]
        evidence = tail.evidence(instance=start["collector_instance"], max_callback_lag_ms=FIXTURE_LAG_MS)
        origin = iso_ms(start["observation_start_utc"])
        beats = [r for r in spool if r["kind"] in ("heartbeat", "stop")]
        assert len(beats) == 6
        for row in beats:
            assert evidence.issued_through(iso_ms(row["observed_through_utc"]), start_ms=origin) == row["events_received"]
        assert start["source_count_declared"] is False        # the recorder itself declares no source counter: certification comes from the log, not the recorder

    def test_13_a_record_not_yet_read_does_not_exist_for_an_earlier_decision(self):
        tail = read_log(LOG1, policy(), read_ms())
        assert tail.attempt_events("inst", as_of_ms=read_ms() - 1) == [] and tail.observed_segments(as_of_ms=read_ms() - 1) == []
        assert len(tail.attempt_events("inst", as_of_ms=read_ms())) == 5 and tail.observed_segments(as_of_ms=read_ms())

    def test_14_a_missing_log_yields_no_evidence_and_no_zero(self, tmp_path):
        tail = ilr.IssueLogTail(tmp_path / "absent.jsonl", policy())
        assert tail.poll(utc(read_ms())).status == ilr.S_MISSING
        assert tail.writer_state()["state"] == ilr.W_NOT_STARTED
        with pytest.raises(ilr.IssueLogError) as info:
            tail.evidence(instance="inst", max_callback_lag_ms=FIXTURE_LAG_MS)
        assert info.value.code == "issue_log_not_started"

    def test_15_a_partial_tail_is_reported_and_marks_the_evidence_torn(self, tmp_path):
        data = LOG1.read_bytes()
        cut = tmp_path / LOG1.name
        cut.write_bytes(data[:-30])
        tail = read_log(cut, policy(), read_ms())
        assert tail.error is None and tail.partial_tail_bytes > 0 and tail.stop_record is None
        assert tail.writer_state()["state"] != ilr.W_CLOSED_CLEAN
        assert tail.evidence(instance="inst", max_callback_lag_ms=FIXTURE_LAG_MS).torn_tail is True

    def test_16_a_removed_record_is_a_sticky_integrity_failure(self, tmp_path):
        lines = LOG1.read_bytes().split(b"\n")
        del lines[9]
        broken = tmp_path / LOG1.name
        broken.write_bytes(b"\n".join(lines))
        tail = read_log(broken, policy(), read_ms())
        assert tail.error is not None and tail.error.code == "record_sequence_gap"
        assert tail.poll(utc(read_ms() + 1)).status == ilr.S_ERROR           # sticky
        with pytest.raises(ilr.IssueLogError):
            tail.evidence(instance="inst", max_callback_lag_ms=FIXTURE_LAG_MS)

    def test_17_an_edited_record_fails_its_hash(self, tmp_path):
        lines = LOG1.read_bytes().split(b"\n")
        assert b"1795219201100" in lines[5]
        lines[5] = lines[5].replace(b"1795219201100", b"1795219201101", 1)
        edited = tmp_path / LOG1.name
        edited.write_bytes(b"\n".join(lines))
        assert read_log(edited, policy(), read_ms()).error.code == "hash_mismatch"

    def test_18_wrong_worker_and_wrong_run_are_refused(self):
        assert read_log(LOG1, policy(worker_id="w1", expected_roster=("w1",)), read_ms()).error.code == "worker_identity_mismatch"
        assert read_log(LOG1, policy(run_id="another-run"), read_ms()).error.code == "run_identity_mismatch"
        assert read_log(LOG1, policy(clock_domain="a-different-domain"), read_ms()).error.code == "clock_domain_mismatch"

    def test_19_evidence_objects_refuse_a_counter_descriptor(self):
        tail = read_log(LOG1, policy(), read_ms())
        evidence = tail.evidence(instance="inst", max_callback_lag_ms=FIXTURE_LAG_MS)
        counter = {"counter_id": "x", "kind": sc.KIND_ISSUE_COUNTER, "independent_of_recorder": True, "derived_from": ilr.ISSUE_DERIVED_FROM,
                   "timestamp_semantics": sc.SEMANTICS_ISSUE, "resolution": "interval"}
        with pytest.raises(sc.SourceEvidenceError) as info:
            dataclasses.replace(evidence, descriptor=counter)
        assert info.value.code == "source_descriptor_not_interval_suitable"

    def test_20_known_limitation_the_descriptor_classifier_trusts_a_declared_resolution(self):
        """Pins a LIMITATION, not an endorsement (design finding 4): a declared bare counter classifies as interval-suitable. If this ever fails, the code was
        tightened and the A0n design document must be updated."""
        counter = {"counter_id": "x", "kind": sc.KIND_ISSUE_COUNTER, "independent_of_recorder": True, "derived_from": "workload_module_issue_log",
                   "timestamp_semantics": sc.SEMANTICS_ISSUE, "resolution": "interval"}
        assert sc.classify_descriptor(counter).certifies_intervals is True
        recorder_derived = dict(counter, kind=sc.KIND_RECORDER_DERIVED)
        assert sc.classify_descriptor(recorder_derived).certifies_intervals is False
        assert sc.classify_descriptor(dict(counter, kind=sc.KIND_ROUND_OBSERVED)).certifies_intervals is False

    def test_21_generator_evidence_is_not_fabric_throughput(self):
        tail = read_log(LOG1, policy(), read_ms())
        evidence = tail.evidence(instance="inst", max_callback_lag_ms=FIXTURE_LAG_MS)
        provenance = evidence.timestamp_provenance()
        assert provenance["event_time"] == "generator_issue_time" and provenance["not_a_callback_time"] is True
        text = ilr.reconcile_counts(tail, recorder_submit_units=5)
        assert text["recorder_submit_callbacks"]["join"].startswith("counts only")
        assert "a difference is evidence of a failure mode" in text["note"]
        assert "ledger_committed" not in text                           # nothing is claimed about commitment unless ledger keys are supplied
        assert ilr.reconcile_counts(tail)["generator"]["send_returned"] == 5


# ===================================================================================================== safety gates
TOOLS = ["scripts/e1/e1_a0m_dry_render.py", "scripts/e1/e1_prelive_validation.py", "scripts/e1/e1_stage3_shadow_runner.py",
         "src/fabric_perf/controllers/channel_selection.py", "src/fabric_perf/controllers/issue_log_reader.py", "src/fabric_perf/controllers/issue_log_coverage.py",
         "src/fabric_perf/controllers/recorder_health_bridge.py", "src/fabric_perf/controllers/clock_preflight.py", "src/fabric_perf/controllers/prelive_contract.py",
         "src/fabric_perf/controllers/stage3_shadow_runner.py", "src/fabric_perf/controllers/stage3_predictive.py",
         "Integration/system_integration/a0n_2026-11-21/build_approval_worksheet.py"]
BAD_MODULES = {"subprocess", "socket", "urllib", "requests", "http", "ftplib", "smtplib", "multiprocessing"}
BAD_OS = {"environ", "getenv", "putenv", "system", "popen", "spawnl", "execv"}
LEGACY = {"run_contemporaneous_capture", "e1_existing_channel_pilot", "e1_final_comparison", "synchronized_shadow_replay"}


def used(path: str) -> tuple[set, set]:
    tree = ast.parse((REPO / path).read_text(encoding="utf-8"))
    bad, imported = set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""] + [f"{node.module}.{a.name}" for a in node.names]
        else:
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "os" and node.attr in BAD_OS:
                bad.add(f"os.{node.attr}")
            continue
        for name in names:
            imported.update(name.split("."))
            if name.split(".")[0] in BAD_MODULES:
                bad.add(name)
    return bad, imported


class TestSafetyGates:
    def test_22_channel_rules_have_no_default_and_refuse_protected_and_historical(self):
        for value, reason in ((None, "missing"), ("", "empty"), ("   ", "empty"), (5, "wrong_type"), ("e1-r002", cs.PROTECTED), ("E1-CFG001", cs.PROTECTED), (" e1-r002 ", cs.PROTECTED),
                              ("e1-b001", cs.HISTORICAL), ("e1-ql01-b10-l50", cs.HISTORICAL), ("e1-cp01-b10-l50", cs.HISTORICAL), ("e1-rp01-b10-r1", cs.HISTORICAL)):
            with pytest.raises(cs.ChannelSelectionError) as info:
                cs.select_experiment_channel(value)
            assert info.value.reason == reason, value
        with pytest.raises(cs.ChannelSelectionError):
            cs.select_experiment_channel("Not_A_Channel")

    def test_23_dry_render_has_no_default_channel_and_execute_is_refused(self, capsys):
        import e1_a0m_dry_render as dry
        with pytest.raises(SystemExit) as info:
            dry.main(["dry-render", "--params", "p", "--a0m-params", "q", "--out-root", "o", "--render-id", "r"])
        assert info.value.code == 2
        assert dry.main(["execute"]) == 5 and "REFUSED" in capsys.readouterr().out
        with pytest.raises(SystemExit) as info:
            dry.main(["dry-render", "--params", "p", "--a0m-params", "q", "--channel", "e1-r002", "--out-root", "o", "--render-id", "r"])
        assert info.value.code == 2

    def test_24_a0e_execute_and_stage3_live_shadow_are_refused(self, capsys):
        import e1_prelive_validation as pv
        import e1_stage3_shadow_runner as s3
        assert pv.main(["execute"]) == 5
        capsys.readouterr()
        code = s3.main(["live-shadow", "--params", str(EVIDENCE / "inputs" / "a0e_params.json"), "--candidate-manifest", str(A0M_MANIFEST)])
        report = json.loads(capsys.readouterr().out)
        assert code == 5 and report["live_permitted"] is False and report["connections_opened"] == 0
        assert report["gates"] and all(g["met"] is False for g in report["gates"])
        assert any(g["gate"] == "G4_operational_execution" and g["status"] == "NOT_AVAILABLE" for g in report["gates"])

    def test_25_real_inference_is_one_false_constant_and_nothing_else_assigns_it(self):
        assert sp.REAL_INFERENCE_AUTHORIZED is False
        assignments = []
        for path in list((REPO / "src").rglob("*.py")) + list((REPO / "scripts").rglob("*.py")) + list((REPO / "Integration").rglob("*.py")):
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except (SyntaxError, UnicodeDecodeError):
                continue
            for node in ast.walk(tree):
                if isinstance(node, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
                    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                    if any(isinstance(t, ast.Name) and t.id == "REAL_INFERENCE_AUTHORIZED" for t in targets):
                        assignments.append((path.relative_to(REPO).as_posix(), getattr(node.value, "value", None)))
        assert assignments == [("src/fabric_perf/controllers/stage3_predictive.py", False)]

    @pytest.mark.parametrize("path", TOOLS)
    def test_26_no_process_network_or_environment_authorization_path(self, path):
        bad, imported = used(path)
        assert not bad, bad
        assert not (imported & LEGACY), imported & LEGACY

    @pytest.mark.parametrize("path", ["fabric-testbed/caliper/workloads/e1_issue_log.js", "fabric-testbed/caliper/workloads/e1_create_asset_issue_logged.js",
                                      "scripts/e1/caliper/e1_per_tx_recorder_v3.js"])
    def test_27_node_modules_use_no_environment_process_or_network_api(self, path):
        text = (REPO / path).read_text(encoding="utf-8")
        for token in ("process.env", "child_process", "require('http')", "require('https')", "require('net')", "require('dgram')", "fetch("):
            assert token not in text, (path, token)

    def test_28_manifests_pins_and_frozen_files_are_unchanged(self):
        for name, digest in EARLIER.items():
            assert sha(E1 / name) == digest, name
        assert sha(A0M_MANIFEST) == A0M_MANIFEST_SHA
        for path, digest in PINNED_AND_FROZEN.items():
            assert sha(REPO / path) == digest, path
        m = manifest()
        assert m["status"] == "CANDIDATE_NOT_APPROVED" and m["approved"] is False and m["approval_record"] is None
        assert m["launch_configuration"]["live_run"] is False and m["launch_configuration"]["approved"] is False
        assert len(m["entries"]) == 79 and m["changed_since_a0l"] == [] and m["superseded_entries"] == []
        statuses = [e["approval_status"] for e in m["entries"]]
        assert (statuses.count("PENDING_OWNER_APPROVAL"), statuses.count("PINNED_ELSEWHERE_UNCHANGED"), statuses.count("EXCLUDED_NOT_APPROVED_ENTRY_POINT")) == (76, 2, 1)
        for e in m["entries"]:
            assert sha(REPO / e["path"]) == e["sha256"], e["path"]
            assert e["pinned_sha256"] in (None, e["sha256"])

    def test_29_the_stored_render_selects_v3_the_issue_logged_workload_and_one_worker(self):
        run = RENDER / "wired_v3" / "a0m-dry-001"
        bench = json.loads((run / "benchmark.yaml").read_text(encoding="utf-8"))
        assert bench["test"]["workers"]["number"] == 1
        modules = {p.name for p in (run / "modules").iterdir()}
        paths = []
        for rnd in bench["test"]["rounds"]:
            assert rnd["workload"]["module"].endswith("/modules/e1_create_asset_issue_logged.js")
            log = rnd["workload"]["arguments"]["issueLog"]
            assert log["workerId"] == "w0" and log["expectedWorkers"] == ["w0"] and log["path"].endswith(".issuelog.jsonl")
            paths.append(log["path"])
        assert len(set(paths)) == 2
        monitor = bench["monitors"]["transaction"][0]
        assert monitor["module"].endswith("/modules/e1_per_tx_recorder_v3.js") and "health" in monitor["options"]
        assert {"e1_create_asset_issue_logged.js", "e1_per_tx_recorder_v3.js", "e1_per_tx_recorder_v2.js", "e1_per_tx_recorder.js", "e1_issue_log.js",
                "e1_create_asset.js", "e1_workload_core.js"} <= modules
        everything = "\n".join(p.read_text(encoding="utf-8", errors="replace") for p in run.glob("*") if p.is_file())
        assert "e1-r002" not in everything and "e1-cfg001" not in everything.lower()

    def test_30_the_stored_render_still_passes_its_offline_preflight_and_is_never_live_ready(self):
        import e1_a0m_dry_render as dry
        result = dry.preflight(RENDER, candidate_manifest=A0M_MANIFEST, write=False)
        assert result["offline_preflight"] == dry.PASS and result["live_ready"] is False

    def test_31_nothing_is_staged(self):
        out = subprocess.run(["git", "diff", "--cached", "--name-only"], cwd=REPO, capture_output=True, text=True, check=True).stdout
        assert out.strip() == ""


# ===================================================================================================== packet and runbook
class TestPacketCompleteness:
    def test_32_the_five_deliverables_and_the_worksheet_exist(self):
        for path in (SPEC, DESIGN, PACKET, RUNBOOK, WORKSHEET, REGISTER, SI / "A0N_PRELIVE_TIMING_AND_APPROVAL_REPORT_2026-11-21.md"):
            assert path.is_file(), path

    def test_33_the_packet_table_matches_the_register_and_approves_nothing(self):
        text = PACKET.read_text(encoding="utf-8")
        rows = {m.group(1): m.group(0) for m in re.finditer(r"^\| (P\d\d) \|.*$", text, re.M)}
        reg = register()["parameters"]
        assert set(rows) == {p["id"] for p in reg}
        for p in reg:
            line = rows[p["id"]]
            assert f"| {p['status']} | UNSET |" in line and p["name"] in line
            if p["code_default"] is not None:
                assert "CODE DEFAULT" in line
        assert "**This packet is a form, not an authorization.**" in text
        assert not re.search(r"approved\W+true", text, re.I)

    def test_34_every_digest_in_the_packet_is_a_manifest_digest_for_the_named_file(self):
        text = PACKET.read_text(encoding="utf-8")
        entries = {e["path"]: e["sha256"] for e in manifest()["entries"]}
        pairs = re.findall(r"\| `([^`]+)`[^|]*\| `([0-9a-f]{64})` \|", text)
        assert len(pairs) >= 13
        for path, digest in pairs:
            assert entries[path] == digest, path
        allowed = set(entries.values()) | {A0M_MANIFEST_SHA}
        assert set(re.findall(r"\b[0-9a-f]{64}\b", text)) <= allowed

    def test_35_no_channel_or_cell_is_invented(self):
        text = PACKET.read_text(encoding="utf-8")
        assert "| Proposed channel identifier | **UNSET**" in text
        tokens = set(re.findall(r"`(e1-[^`\s]*)`", text))
        allowed = set(cs.PROTECTED_REFERENCE_CHANNELS) | set(cs.HISTORICAL_EVIDENCE_CHANNELS) | {"e1-offline.a0m", "e1-"}
        stray = {t for t in tokens if t not in allowed and not t.startswith(("e1-ql", "e1-cp", "e1-rp"))}
        assert not stray, stray
        cell_rows = [ln for ln in text.splitlines() if re.match(r"^\| `(max_message_count|batch_timeout|preferred_max_bytes|rate_tps|workload|warmup_s)`", ln)]
        assert len(cell_rows) == 6 and all(ln.rstrip().endswith("| UNSET |") for ln in cell_rows)

    def test_36_each_live_step_has_its_own_unset_authorization(self):
        text = PACKET.read_text(encoding="utf-8")
        section = text.split("## F. Live-step authorizations")[1].split("## G.")[0]
        rows = [ln for ln in section.splitlines() if re.match(r"^\| [1-9] \|", ln)]
        assert len(rows) == 9 and all(ln.rstrip().endswith("| UNSET |") for ln in rows)
        assert "Real-inference approval (separate from Fabric execution)" in text and "`False`" in text

    def test_37_the_worksheet_equals_the_manifest_and_has_empty_decisions(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("a0n_worksheet", A0N_DIR / "build_approval_worksheet.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        rows = mod.build_rows(A0M_MANIFEST)
        lines = WORKSHEET.read_text(encoding="utf-8").splitlines()
        assert lines[0].split("\t") == list(mod.COLUMNS) and len(lines) == 80 == len(rows) + 1
        for line, row in zip(lines[1:], rows):
            assert line.split("\t") == [row[c] for c in mod.COLUMNS]
            assert row["owner_decision"] == row["decision_date"] == row["evidence_reference"] == ""
        assert sum(1 for r in rows if r["new_in_manifest"] == "yes") == 3

    def test_38_the_worksheet_builder_refuses_a_tampered_manifest_and_an_existing_output(self, tmp_path):
        import importlib.util
        spec = importlib.util.spec_from_file_location("a0n_worksheet2", A0N_DIR / "build_approval_worksheet.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        bad = manifest()
        bad["entries"][0]["sha256"] = "0" * 64
        path = tmp_path / "m.json"
        path.write_text(json.dumps(bad), encoding="utf-8")
        assert mod.main(["--manifest", str(path), "--out", str(tmp_path / "o.tsv")]) == 2 and not (tmp_path / "o.tsv").exists()
        approved = manifest()
        approved["approved"] = True
        path.write_text(json.dumps(approved), encoding="utf-8")
        assert mod.main(["--manifest", str(path), "--out", str(tmp_path / "o2.tsv")]) == 2
        assert mod.main(["--manifest", str(A0M_MANIFEST), "--out", str(WORKSHEET)]) == 2          # exists: never overwritten

    def test_39_the_runbook_has_seven_gates_each_with_all_required_items(self):
        text = RUNBOOK.read_text(encoding="utf-8")
        gates = re.split(r"\n## (Gate \d) — ", text)
        assert gates[1::2] == [f"Gate {i}" for i in range(7)]
        for name, body in zip(gates[1::2], gates[2::2]):
            for item in ("Prerequisites", "Entry point", "Inputs", "Expected evidence", "Pass", "Fail", "Blocked", "Stop conditions", "Authorization", "Rollback / safe stop"):
                assert re.search(rf"^\| {item}s? \|", body, re.M) or re.search(rf"^\| {item}", body, re.M), (name, item)
        assert "NOT IMPLEMENTED" in text and "DESIGN ONLY" in text.split("\n")[0]


# ===================================================================================== documents cite real interfaces
SPEC_FIELDS = ("t_ms", "mono_ms", "t_submit_evt_ms", "t_utc", "observed_through_utc", "observed_utc", "poll_start_utc", "interval_s", "ref_wall_before",
               "remote_wall", "ref_mono_before_ns", "ref_mono_after_ns", "remote_resolution_ns", "observation_start_utc",
               "freshness_assessment", "window_covered", "observations_used", "source_timestamps", "read_ms", "latest_observed")
FIELD_SOURCES = ("fabric-testbed/caliper/workloads/e1_issue_log.js", "scripts/e1/caliper/e1_per_tx_recorder.js", "scripts/e1/caliper/e1_per_tx_recorder_v3.js",
                 "src/fabric_perf/controllers/ledger_evidence.py", "src/fabric_perf/controllers/clock_preflight.py", "src/fabric_perf/controllers/recorder_health_bridge.py",
                 "src/fabric_perf/controllers/issue_log_reader.py", "src/fabric_perf/controllers/live_integration.py")
DESIGN_SYMBOLS = {
    "fabric_perf.controllers.issue_log_reader": ("IssueLogTail", "IssueLogPolicy", "IssueLogEvidence", "reconcile_counts"),
    "fabric_perf.controllers.issue_log_coverage": ("IssueLogJournalBridge", "GeneratorIssueCoverage", "GeneratorIssueSource", "GeneratorIssueCapturedHistory"),
    "fabric_perf.controllers.source_completeness": ("classify_descriptor", "DispatchLogEvidence"),
    "fabric_perf.controllers.recorder_health_bridge": ("ingest_spool",),
}


class TestDocumentsReferenceRealInterfaces:
    def test_40_every_timestamp_field_named_in_the_specification_exists_in_the_sources(self):
        corpus = "\n".join((REPO / p).read_text(encoding="utf-8") for p in FIELD_SOURCES)
        spec = SPEC.read_text(encoding="utf-8")
        for token in SPEC_FIELDS:
            assert token in corpus, f"{token} is not in any source"
            assert f"`{token}`" in spec or f"`{token}" in spec, f"{token} is not named in the specification"

    def test_41_every_interface_named_in_the_sourcecount_design_exists(self):
        import importlib
        design = DESIGN.read_text(encoding="utf-8")
        for module, names in DESIGN_SYMBOLS.items():
            loaded = importlib.import_module(module)
            for name in names:
                assert hasattr(loaded, name), (module, name)
                assert name in design, name
        writer = (REPO / "fabric-testbed/caliper/workloads/e1_issue_log.js").read_text(encoding="utf-8")
        workload = (REPO / "fabric-testbed/caliper/workloads/e1_create_asset_issue_logged.js").read_text(encoding="utf-8")
        assert "class IssueLogWriter" in writer and "function wrapSutAdapter" in writer and "function validateIssueLog" in workload
        for name in ("IssueLogWriter", "wrapSutAdapter", "validateIssueLog"):
            assert name in design

    def test_42_no_document_presents_a_candidate_as_measured_or_approved(self):
        statements = {SPEC: "No quantity below has been measured", DESIGN: "NOT verified live", PACKET: "never measured",
                      RUNBOOK: "NOT READY TO EXECUTE"}
        for path, phrase in statements.items():
            assert phrase in path.read_text(encoding="utf-8"), (path.name, phrase)
        reg = register()
        assert all("measured" not in p and "approved_value" not in p for p in reg["parameters"])
        assert all(p["candidate"]["label"] == "unverified engineering candidate" for p in reg["parameters"] if p["candidate"])
