"""Task A0d (2026-11-21): ledger observations -> telemetry provider -> Stage 3 warm-up -> three shadow controllers, offline.

Fixtures only: the ledger is a ``ScriptedSource``, Prometheus is a fake endpoint function, the performance model and the
forecaster are fakes (no frozen model is loaded), and the client CSVs are written by the test (or, in one test, by the real v2
recorder under a fake Caliper dispatch). These tests show how the SOFTWARE behaves. They do not show how a real Fabric peer,
Caliper or Prometheus behave.
"""

import ast
import importlib.util
import json
import os
import shutil
import subprocess
from types import SimpleNamespace
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from fabric_perf.controllers.harness import ShadowActuator
from fabric_perf.controllers.ledger_evidence import STATE_INITIALIZING, STATE_MEASURING
from fabric_perf.controllers.stage3_frame_quality import (
    INITIALIZATION_LIMIT_REASON_CODE,
    INITIALIZATION_REASON_CODE,
    Stage3FrameQualityContract,
)
from tests.a0d_support import CHANNEL, CONFIG, LEDGER_TX_HEADER, THREE, Rig, build_runner, epoch_ms, iso

REPO = Path(__file__).resolve().parents[1]
NODE = shutil.which("node")
DRIVER = REPO / "tests" / "e1" / "js" / "a0c_live_round.js"


def reasons(outcome, index):
    return {r["decision"]["reason"] for r in outcome["records"] if r["integration"]["decision_index"] == index}


def codes(outcome, index):
    return {(r["integration"].get("observation_rejected") or {}).get("reason_code")
            for r in outcome["records"] if r["integration"]["decision_index"] == index}


def outcomes(outcome, index):
    return {r["decision"]["outcome"] for r in outcome["records"] if r["integration"]["decision_index"] == index}


def healthy(rig, count, *, tx_per_block=4):
    """Frame 0 on the baseline observation, then ``count - 1`` frames each after one more observed ledger interval."""
    frames = []
    rig.collector.poll()
    rig.client(5)
    frames.append(rig.frame(0))
    for index in range(1, count):
        rig.ledger(tx_per_block)
        rig.step(1.0)
        rig.client(5)
        frames.append(rig.frame(index))
    return frames


# ============================================================ 27/28. first-frame initialization, then measurement
def test_the_first_frame_is_an_initialization_frame_and_does_not_spend_the_abstention_budget(tmp_path):
    rig = Rig(tmp_path)
    frames = healthy(rig, 3)
    assert frames[0].provenance["continuous_caliper_health"]["ledger"]["state"] == STATE_INITIALIZING
    assert frames[0].state.observed_throughput_tps is None                                  # no interval yet: absent, not zero
    outcome = build_runner(limit=1, max_initialization_frames=1).run(frames)               # a bound of ONE abstention would end the run at once
    assert outcomes(outcome, 0) == {"abstain"} and reasons(outcome, 0) == {"telemetry_initializing_fail_safe"}
    assert codes(outcome, 0) == {INITIALIZATION_REASON_CODE}
    assert outcome["initialization_frame_count"] == 1 and outcome["longest_consecutive_abstention_run"] == 0
    assert outcome["stop_reasons"] == [] and outcome["consecutive_abstention_limit_reached"] is False
    assert outcome["frame_count"] == 3 and outcome["validation_records"][0]["handled"] == "initializing"


def test_the_transition_into_measurement_reaches_all_three_controllers_with_real_throughput(tmp_path):
    rig = Rig(tmp_path)
    frames = healthy(rig, 3)
    outcome = build_runner(limit=2, max_initialization_frames=1).run(frames)
    for index in (1, 2):
        assert frames[index].provenance["continuous_caliper_health"]["ledger"]["state"] == STATE_MEASURING
        records = [r for r in outcome["records"] if r["integration"]["decision_index"] == index]
        assert {r["integration"]["strategy"] for r in records} == THREE and len(records) == 3
        assert all(r["decision"]["outcome"] != "abstain" or r["integration"]["strategy"] != "static" for r in records)
        assert all(r["state"]["observed_throughput_tps"] == pytest.approx(4.0) for r in records)     # 4 valid tx in a 1 s observed interval
        assert all("observation_rejected" not in r["integration"] for r in records)
    assert outcome["stop_reasons"] == [] and outcome["initialization_frame_count"] == 1


def test_the_same_run_without_the_warm_up_option_abstains_on_frame_zero_exactly_as_before(tmp_path):
    rig = Rig(tmp_path)
    frames = healthy(rig, 2)
    outcome = build_runner(limit=1).run(frames)                                             # no max_initialization_frames
    assert codes(outcome, 0) == {"required_field_missing"} and outcome["initialization_frame_count"] == 0
    assert outcome["stop_reasons"] == ["consecutive_abstention_limit_reached"] and outcome["frame_count"] == 1


def test_a_provider_that_lists_throughput_as_required_is_handled_the_same_way(tmp_path):
    rig = Rig(tmp_path, provider_required_fields=("observed_arrival_rate_tps", "observed_throughput_tps",
                                                   "observed_latency_ms", "queue_depth"))
    frames = healthy(rig, 2)
    assert "observed_throughput_tps" in frames[0].critical_missing                           # the provider itself flags it
    outcome = build_runner(limit=1, max_initialization_frames=1).run(frames)
    assert codes(outcome, 0) == {INITIALIZATION_REASON_CODE} and outcomes(outcome, 1) != {"abstain"} and outcome["stop_reasons"] == []


# ============================================================ 29/30/31. a genuine invalid frame is never labelled initialization
def test_missing_client_metrics_on_a_baseline_frame_is_an_ordinary_abstention(tmp_path):
    rig = Rig(tmp_path)
    rig.collector.poll()                                                                    # baseline exists, but no client rows at all
    frame = rig.frame(0)
    assert frame.provenance["continuous_caliper_health"]["ledger"]["state"] == STATE_INITIALIZING
    outcome = build_runner(limit=1, max_initialization_frames=5).run([frame])
    assert codes(outcome, 0) == {"required_field_missing"} and outcome["initialization_frame_count"] == 0
    assert outcome["stop_reasons"] == ["consecutive_abstention_limit_reached"]


def test_missing_ledger_metrics_before_any_observation_is_an_ordinary_abstention(tmp_path):
    rig = Rig(tmp_path)
    rig.client(5)                                                                           # client is fine; the ledger journal has no rows
    frame = rig.frame(0)
    assert frame.provenance["continuous_caliper_health"]["ledger"]["state"] == "no_observations"
    outcome = build_runner(limit=1, max_initialization_frames=5).run([frame])
    assert codes(outcome, 0) == {"required_field_missing"} and outcome["initialization_frame_count"] == 0


def test_a_missing_ledger_journal_file_is_a_read_failure_not_initialization(tmp_path):
    rig = Rig(tmp_path)
    rig.collector.paths["observations"].unlink()
    rig.client(5)
    frame = rig.frame(0)
    outcome = build_runner(limit=1, max_initialization_frames=5).run([frame])
    assert outcome["initialization_frame_count"] == 0 and outcomes(outcome, 0) == {"abstain"}
    assert INITIALIZATION_REASON_CODE not in codes(outcome, 0)
    assert any("ledger_observations_missing" in code for code in frame.critical_missing) or frame.provenance["read_failures"]["items"]


def test_a_stale_client_stream_with_a_baseline_ledger_is_not_initialization(tmp_path):
    rig = Rig(tmp_path)
    rig.collector.poll()
    rig.client(5)
    rig.clock.advance(30.0)                                                                 # the client data is now 30 s old
    frame = rig.frame(0)
    outcome = build_runner(limit=1, max_initialization_frames=5).run([frame])
    assert outcome["initialization_frame_count"] == 0 and INITIALIZATION_REASON_CODE not in codes(outcome, 0)
    assert outcome["stop_reasons"] == ["consecutive_abstention_limit_reached"]


# ============================================================ 32/33. stale ledger observation and the consecutive threshold
def test_a_collector_that_stops_makes_the_ledger_metric_stale_and_the_run_stops_at_the_operator_bound(tmp_path):
    rig = Rig(tmp_path)
    frames = healthy(rig, 3)
    for index in (3, 4, 5):                                                                 # the collector is dead; the client keeps going
        rig.clock.advance(6.0)                                                              # past the 5 s limit
        rig.client(5)
        frames.append(rig.frame(index))
    assert frames[3].state.observed_throughput_tps is None                                    # no interval inside the window: missing, not the old value
    outcome = build_runner(limit=2, max_initialization_frames=1).run(frames)
    assert outcomes(outcome, 3) == {"abstain"} and outcomes(outcome, 4) == {"abstain"}
    assert outcome["stop_reasons"] == ["consecutive_abstention_limit_reached"] and outcome["longest_consecutive_abstention_run"] == 2
    assert INITIALIZATION_REASON_CODE not in codes(outcome, 3) | codes(outcome, 4)         # stale is stale, never initialization
    assert outcome["frame_count"] == 5                                                      # frame 5 was never reached


def test_a_valid_frame_resets_the_consecutive_count(tmp_path):
    rig = Rig(tmp_path)
    frames = healthy(rig, 2)
    rig.clock.advance(20.0)
    rig.client(5)
    frames.append(rig.frame(2))                                                             # stale ledger: abstain
    rig.ledger(4)
    rig.step(1.0)
    rig.client(5)
    frames.append(rig.frame(3))                                                             # collector recovered: a spanning interval, valid
    outcome = build_runner(limit=2, max_initialization_frames=1).run(frames)
    assert outcomes(outcome, 2) == {"abstain"} and outcomes(outcome, 3) != {"abstain"}
    assert outcome["longest_consecutive_abstention_run"] == 1 and outcome["stop_reasons"] == []


# ============================================================ 34. bounded warm-up
def test_warm_up_is_bounded_and_one_frame_beyond_the_bound_stops_the_run(tmp_path):
    rig = Rig(tmp_path)
    rig.collector.poll()                                                                    # the collector never takes a second observation
    frames = []
    for index in range(5):
        rig.clock.advance(1.0)
        rig.client(5)
        frames.append(rig.frame(index))
    outcome = build_runner(limit=10, max_initialization_frames=2).run(frames)
    assert outcome["initialization_frame_count"] == 2
    assert outcome["stop_reasons"] == ["initialization_limit_exceeded"] and outcome["initialization_limit_exceeded"] is True
    assert outcome["frame_count"] == 3                                                      # two warm-up frames, then the stopping frame
    assert INITIALIZATION_LIMIT_REASON_CODE in codes(outcome, 2)
    stop = outcome["validation_records"][-1]
    assert stop["status"] == "stopped" and stop["max_initialization_frames"] == 2


def test_warm_up_never_resets_the_abstention_count_so_alternation_cannot_dodge_the_safety_stop(tmp_path):
    rig = Rig(tmp_path)
    rig.collector.poll()
    frames = []
    rig.clock.advance(1.0)                                                                  # A: baseline ledger, NO client rows -> genuine invalid (1)
    frames.append(rig.frame(0))
    rig.clock.advance(1.0)
    rig.client(5)                                                                           # B: baseline ledger, client present -> warm-up
    frames.append(rig.frame(1))
    rig.clock.advance(6.0)                                                                  # C: client rows are now stale -> genuine invalid (2)
    frames.append(rig.frame(2))
    rig.clock.advance(1.0)
    rig.client(5)                                                                           # D: would be warm-up again, never reached
    frames.append(rig.frame(3))
    outcome = build_runner(limit=2, max_initialization_frames=5).run(frames)
    assert codes(outcome, 1) == {INITIALIZATION_REASON_CODE} and outcome["initialization_frame_count"] == 1
    assert outcome["stop_reasons"] == ["consecutive_abstention_limit_reached"]
    assert outcome["longest_consecutive_abstention_run"] == 2 and outcome["frame_count"] == 3


def test_warm_up_is_only_allowed_before_the_first_accepted_frame(tmp_path):
    early = Rig(tmp_path / "a")
    frames = healthy(early, 2)                                                              # frame 1 is accepted
    late = Rig(tmp_path / "b")
    late.clock.now = early.clock.now + timedelta(seconds=1)
    late.collector.poll()                                                                   # a NEW journal's baseline appears mid-run
    late.client(5)
    frames.append(replace(late.frame(2)))
    assert frames[2].provenance["continuous_caliper_health"]["ledger"]["state"] == STATE_INITIALIZING
    outcome = build_runner(limit=1, max_initialization_frames=5).run(frames)
    assert outcomes(outcome, 1) != {"abstain"}
    assert codes(outcome, 2) == {"required_field_missing"} and outcome["initialization_frame_count"] == 1
    assert outcome["stop_reasons"] == ["consecutive_abstention_limit_reached"]


# ============================================================ 35/36. consistency and actuator isolation
def test_every_controller_gets_the_same_abstention_for_warm_up_and_for_invalid_frames(tmp_path):
    rig = Rig(tmp_path)
    frames = healthy(rig, 2)
    rig.clock.advance(20.0)
    rig.client(5)
    frames.append(rig.frame(2))
    outcome = build_runner(limit=3, max_initialization_frames=1).run(frames)
    assert outcome["identical_input_digests"] is True
    for index in (0, 2):
        records = [r for r in outcome["records"] if r["integration"]["decision_index"] == index]
        assert {r["integration"]["strategy"] for r in records} == THREE
        assert len({r["integration"]["input_digest"] for r in records}) == 1
        assert len({r["decision"]["reason"] for r in records}) == 1 and outcomes(outcome, index) == {"abstain"}
        assert len({json.dumps(r["integration"]["observation_rejected"], sort_keys=True) for r in records}) == 1
    assert reasons(outcome, 0) != reasons(outcome, 2)                                       # warm-up and stale carry different reasons


def test_no_controller_applies_or_even_attempts_a_change_during_warm_up_or_afterwards(tmp_path, monkeypatch):
    calls = []
    original = ShadowActuator.apply
    monkeypatch.setattr(ShadowActuator, "apply", lambda self, *a, **k: (calls.append(1), original(self, *a, **k))[1])
    rig = Rig(tmp_path)
    frames = healthy(rig, 4)
    outcome = build_runner(limit=2, max_initialization_frames=1).run(frames)
    for record in outcome["records"]:
        assert record["integration"]["shadow_mode"] is True and record["integration"]["live_submission_enabled"] is False
        assert record["configuration_state"]["proposal_applied"] is False
        assert record["configuration_state"]["after"]["max_message_count"] == 45
    init_records = [r for r in outcome["records"] if r["integration"]["decision_index"] == 0]
    assert all(r["application"] is None and r["state"] is None for r in init_records)       # no state was even offered
    assert outcome["live_submission_enabled"] is False and outcome["mode"] == "shadow"


# ============================================================ contract validation: the bound is explicit or absent
@pytest.mark.parametrize("bad", [0, -1, True, 1.5, "2", [1]])
def test_the_warm_up_bound_must_be_an_operator_supplied_positive_integer(bad):
    with pytest.raises(ValueError, match="max_initialization_frames"):
        Stage3FrameQualityContract(max_consecutive_abstentions=3, max_initialization_frames=bad)


def test_the_warm_up_is_off_by_default_and_the_contract_says_so():
    contract = Stage3FrameQualityContract(max_consecutive_abstentions=3)
    described = contract.as_dict()
    assert contract.max_initialization_frames is None and described["max_initialization_frames"] is None
    assert "not enabled" in described["max_initialization_frames_basis"] and described["version"] == "stage3_frame_quality/1"
    on = Stage3FrameQualityContract.from_mapping({"max_consecutive_abstentions": 3, "max_initialization_frames": 2}).as_dict()
    assert on["max_initialization_frames"] == 2 and "no scientific value" in on["max_initialization_frames_basis"]


def test_the_warm_up_needs_throughput_to_be_a_required_field():
    with pytest.raises(ValueError, match="only applies"):
        Stage3FrameQualityContract(max_consecutive_abstentions=3, max_initialization_frames=1,
                                   required_client_fields=("observed_latency_ms",))


# ============================================================ 37/38. recorder v2 -> client adapter; collector -> provider
@pytest.mark.skipif(NODE is None, reason="Node.js not on PATH: the real v2 recorder step is NOT VERIFIED here")
def test_real_v2_recorder_rows_and_collector_observations_reach_the_controllers_with_separate_provenance(tmp_path):
    t0_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    out = tmp_path / "js"
    done = subprocess.run([NODE, str(DRIVER), str(out), "RUNA", "midround"], capture_output=True, text=True, timeout=60,
                          env={**os.environ, "A0C_T0": str(t0_ms), "A0C_SUBMIT_GAP_MS": "5"})
    assert done.returncode == 0, done.stderr
    result = json.loads(done.stdout.strip().splitlines()[-1])
    after = result["after_deactivate"]
    rig = Rig(tmp_path / "rig")
    rig.clock.now = datetime.fromtimestamp((t0_ms + 3000) / 1000, tz=timezone.utc)          # the round's completions are all in the past
    rig.collector.poll()                                                                    # ledger baseline
    rig.recorder.write_text("\n".join(after["live_csv_lines"]) + "\n", encoding="utf-8")
    rig.submits.write_text("\n".join(after["live_submits_lines"]) + "\n", encoding="utf-8")
    first = rig.frame(0)
    rig.clock.advance(1.0)
    rig.ledger(7)                                                                            # the ledger interval is the collector's own
    rig.collector.poll()
    second = rig.frame(1)
    assert first.state.observed_throughput_tps is None and first.state.observed_latency_ms is not None
    assert second.state.observed_throughput_tps == pytest.approx(7.0)                      # 7 VALID tx over the 1 s the collector observed
    metrics = second.provenance["metrics"]
    assert metrics["delivered_valid_tps"]["source"].startswith("Fabric ledger observation journal")
    assert metrics["latency_mean_ms"]["source"] == "Caliper recorder CSV"                   # independent provenance
    assert metrics["delivered_valid_tps"]["timestamp_utc"] == iso(rig.clock.now)             # ledger observation time ...
    assert metrics["latency_mean_ms"]["timestamp_utc"] != metrics["delivered_valid_tps"]["timestamp_utc"]   # ... not the client's event time
    outcome = build_runner(limit=2, max_initialization_frames=1).run([first, second])
    assert codes(outcome, 0) == {INITIALIZATION_REASON_CODE} and outcomes(outcome, 1) != {"abstain"}


def test_the_ledger_metric_is_stamped_with_observation_time_and_carries_its_interval(tmp_path):
    rig = Rig(tmp_path)
    frames = healthy(rig, 2)
    metric = frames[1].provenance["metrics"]["delivered_valid_tps"]
    assert metric["unit"] == "VALID transactions/s" and metric["value"] == pytest.approx(4.0)
    assert metric["timestamp_utc"] == iso(rig.clock.now) and metric["window_end_utc"] == metric["timestamp_utc"]
    assert metric["window_start_utc"] == iso(rig.clock.now - timedelta(seconds=1))          # the observed interval, not the adapter's window
    assert metric["sampling_interval_s"] == 1.0 and metric["run_id"] == "r"
    health = frames[1].provenance["continuous_caliper_health"]["ledger"]
    assert health["state"] == "measuring" and health["definition"].startswith("committed throughput = ENDORSER_TRANSACTION")
    assert health["latest_observation_seq"] == 2 and health["valid_in_interval"] == 4 and health["baseline_height"] == 0
    assert frames[1].provenance["continuous_caliper_source"]["ledger_observations"].endswith("ledger_observations.csv")


# ============================================================ 39. synchronization: the ledger rate does not depend on the reader's rhythm
def test_the_committed_rate_does_not_depend_on_how_often_the_adapter_is_read(tmp_path):
    def run(observations, cadence):
        rig = Rig(tmp_path / f"{observations}-{cadence}", observations=observations)
        rig.collector.poll()
        rig.client(5)
        rig.frame(0)
        rig.ledger(10)
        rig.step(2.0)                                                                       # 10 VALID tx observed over 2 s -> 5.0
        values = []
        for i in range(1, 4):
            rig.clock.advance(cadence)
            rig.client(5)
            values.append(rig.frame(i).state.observed_throughput_tps)
        return values
    stable = run(True, 0.2)
    assert stable == [pytest.approx(5.0)] * 3                                               # same interval, same rate, however often it is read
    legacy = run(False, 0.2)                                                                # discovery-time mode: the rate is whatever this reader's gaps make it
    assert legacy[0] != pytest.approx(5.0) or legacy[1] != pytest.approx(legacy[0])


def test_submitted_workload_and_committed_throughput_are_different_numbers(tmp_path):
    rig = Rig(tmp_path)
    frames = healthy(rig, 2, tx_per_block=2)
    state = frames[1].state
    assert state.observed_throughput_tps == pytest.approx(2.0) and state.observed_arrival_rate_tps != state.observed_throughput_tps
    assert state.observed_arrival_rate_tps > 2.0                                            # client submitted more than the ledger committed


def test_a_missing_observation_is_missing_never_zero_in_the_frame(tmp_path):
    rig = Rig(tmp_path)
    rig.client(5)
    frame = rig.frame(0)
    metric = frame.provenance["metrics"]["delivered_valid_tps"]
    assert frame.state.observed_throughput_tps is None and metric["value"] is None and metric["missing_reason"]
    assert frame.provenance["metrics"]["valid_transactions"]["value"] is None               # also not zero


def test_a_duplicated_or_rolled_back_observation_row_compromises_the_ledger_stream_and_withholds_the_value(tmp_path):
    rig = Rig(tmp_path)
    frames = healthy(rig, 2)
    lines = rig.collector.paths["observations"].read_text(encoding="utf-8").splitlines()
    with rig.collector.paths["observations"].open("a", encoding="utf-8", newline="\n") as fh:
        fh.write(lines[-1] + "\n")                                                           # the same observation row twice
    rig.clock.advance(1.0)
    rig.client(5)
    frame = rig.frame(2)
    assert frame.state.observed_throughput_tps is None
    assert "continuous_source_integrity_compromised:ledger" in frame.critical_missing
    assert frame.provenance["continuous_caliper_health"]["integrity"]["rows_rejected"]["ledger"] == 1


def test_an_observation_row_for_another_channel_is_rejected(tmp_path):
    rig = Rig(tmp_path)
    rig.collector.poll()
    path = rig.collector.paths["observations"]
    with path.open("a", encoding="utf-8", newline="\n") as fh:
        fh.write(path.read_text(encoding="utf-8").splitlines()[-1].replace(CHANNEL, "e1-another").replace("1,baseline", "2,baseline", 1) + "\n")
    rig.client(5)
    frame = rig.frame(0)
    assert frame.provenance["continuous_caliper_health"]["integrity"]["status"] == "COMPROMISED"


def test_a_halted_collector_yields_no_throughput_and_a_distinct_state(tmp_path):
    rig = Rig(tmp_path)
    healthy(rig, 2)
    rig.source.height_value_override = 0                                                    # the height goes backwards
    rig.clock.advance(1.0)
    rig.collector.poll()
    rig.client(5)
    frame = rig.frame(5)
    assert frame.provenance["continuous_caliper_health"]["ledger"]["state"] == "integrity_halted"
    assert frame.state.observed_throughput_tps is None
    outcome = build_runner(limit=1, max_initialization_frames=3).run([frame])
    assert outcome["initialization_frame_count"] == 0 and outcome["stop_reasons"] == ["consecutive_abstention_limit_reached"]


# ============================================================ 40/41. three controllers, output schema and provenance
def test_a_full_offline_run_writes_the_documented_outputs_with_ledger_provenance(tmp_path):
    rig = Rig(tmp_path / "rig")
    frames = healthy(rig, 4)
    out = tmp_path / "out"
    outcome = build_runner(limit=2, max_initialization_frames=1).run(frames, output_dir=out)
    manifest = json.loads((out / "integration_manifest.json").read_text(encoding="utf-8"))
    assert manifest["initialization_frame_count"] == 1 and manifest["frame_quality_contract"]["max_initialization_frames"] == 1
    assert manifest["stop_reasons"] == [] and manifest["identical_input_digests"] is True and outcome["frame_count"] == 4
    decisions = [json.loads(line) for line in (out / "controller_decisions.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(decisions) == 12 and {d["integration"]["strategy"] for d in decisions} == THREE
    measured = [d for d in decisions if d["integration"]["decision_index"] == 3]
    for record in measured:
        provenance = record["integration"]["observation_provenance"]
        assert provenance["continuous_caliper_health"]["ledger"]["state"] == "measuring"
        assert provenance["metrics"]["delivered_valid_tps"]["source"].startswith("Fabric ledger observation journal")
        assert record["state"]["observed_throughput_tps"] == pytest.approx(4.0)
        assert provenance["continuous_caliper_health"]["latency"]["source_mode"] == "derive_from_timestamps"
    predictive = next(r for r in measured if r["integration"]["strategy"] == "predictive")
    assert predictive["decision"]["predicted_arrival_rate_tps"] == 77.0                       # the forecast ...
    assert predictive["state"]["observed_arrival_rate_tps"] != 77.0                           # ... is not an observation
    validation = json.loads((out / "validation_evidence.json").read_text(encoding="utf-8"))
    assert validation["validation_records"][0]["handled"] == "initializing"


# ============================================================ 42. A0b / A0c compatibility: defaults are unchanged
def test_without_the_opt_in_the_adapter_behaves_exactly_as_in_a0c(tmp_path):
    rig = Rig(tmp_path, observations=False)
    rig.collector.poll()
    rig.client(5)
    snapshot = rig.adapter.collect(observation_timestamp_utc=iso(rig.clock.now))
    assert "ledger_observations" not in snapshot.health and "ledger" not in (rig.frame(0).provenance["continuous_caliper_health"])
    assert snapshot.metrics["delivered_valid_tps"].source == "Fabric ledger append observation"       # the discovery-time definition
    assert rig.adapter.ledger_observations is None


def test_the_default_contract_is_the_a0c_contract():
    contract = Stage3FrameQualityContract(max_consecutive_abstentions=4)
    assert contract.max_initialization_frames is None
    assert contract.check(type("F", (), {"provenance": {}, "state": None})()).reason_code == "client_source_not_continuous"


def test_the_ledger_tx_header_the_adapter_reads_is_the_collectors_header(tmp_path):
    rig = Rig(tmp_path)
    assert rig.collector.paths["tx"].read_text(encoding="utf-8") == LEDGER_TX_HEADER


# ============================================================ the warm-up rule, one condition at a time (no other failure may hide behind it)
def candidate_frame(**overrides):
    base = dict(provenance={"continuous_caliper_health": {"ledger": {"state": "initializing", "baseline_height": 7,
                                                                      "latest_observation_seq": 1, "reason": "initializing: ..."},
                                                          "integrity": {"status": "OK"}}},
                state=SimpleNamespace(observed_throughput_tps=None), critical_missing=(), inconsistencies=())
    base.update(overrides)
    return SimpleNamespace(**base)


def contract(frames=2):
    return Stage3FrameQualityContract(max_consecutive_abstentions=3, max_initialization_frames=frames)


def test_the_contract_accepts_exactly_the_documented_candidate_and_nothing_else():
    details = contract().initialization_candidate(candidate_frame())
    assert details["baseline_height"] == 7 and details["max_initialization_frames"] == 2
    assert contract().initialization_candidate(candidate_frame(critical_missing=("observed_throughput_tps",))) is not None
    health = candidate_frame().provenance["continuous_caliper_health"]
    cases = {
        "another_missing_code": candidate_frame(critical_missing=("observed_throughput_tps", "required_metric_missing:raft_leader_count")),
        "read_failure": candidate_frame(critical_missing=("continuous_source_read_error:ledger:file_missing",)),
        "inconsistent": candidate_frame(inconsistencies=("source_timestamp_skew_exceeds_limit",)),
        "throughput_present": candidate_frame(state=SimpleNamespace(observed_throughput_tps=3.0)),
        "integrity_compromised": candidate_frame(provenance={"continuous_caliper_health": {**health, "integrity": {"status": "COMPROMISED"}}}),
        "integrity_degraded": candidate_frame(provenance={"continuous_caliper_health": {**health, "integrity": {"status": "DEGRADED"}}}),
        "no_integrity_block": candidate_frame(provenance={"continuous_caliper_health": {"ledger": health["ledger"]}}),
        "ledger_measuring": candidate_frame(provenance={"continuous_caliper_health": {**health, "ledger": {"state": "measuring"}}}),
        "ledger_resumed": candidate_frame(provenance={"continuous_caliper_health": {**health, "ledger": {"state": "resumed_no_interval"}}}),
        "ledger_halted": candidate_frame(provenance={"continuous_caliper_health": {**health, "ledger": {"state": "integrity_halted"}}}),
        "no_ledger_health": candidate_frame(provenance={"continuous_caliper_health": {"integrity": {"status": "OK"}}}),
        "no_health": candidate_frame(provenance={}),
    }
    for name, frame in cases.items():
        assert contract().initialization_candidate(frame) is None, name
    assert Stage3FrameQualityContract(max_consecutive_abstentions=3).initialization_candidate(candidate_frame()) is None   # not enabled


def test_a_down_prometheus_endpoint_with_a_baseline_ledger_is_not_initialization(tmp_path):
    rig = Rig(tmp_path, prometheus_ok=False)
    rig.collector.poll()
    rig.client(5)
    frame = rig.frame(0)
    assert frame.provenance["continuous_caliper_health"]["ledger"]["state"] == STATE_INITIALIZING and frame.critical_missing
    outcome = build_runner(limit=1, max_initialization_frames=5).run([frame])
    assert outcome["initialization_frame_count"] == 0 and INITIALIZATION_REASON_CODE not in codes(outcome, 0)
    assert outcome["stop_reasons"] == ["consecutive_abstention_limit_reached"]


# ============================================================ command-line wiring of the new options
INTEGRATED = REPO / "Integration" / "system_integration" / "run_integrated_controller.py"
PRODUCTION = REPO / "Fabric_Live_Testbed" / "controllers" / "run_production_shadow_validation.py"


def load_script(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def integrated_args(tmp_path):
    return ["--run-id", "r", "--snapshots", str(tmp_path / "missing.jsonl"), "--out-dir", str(tmp_path / "out"),
            "--hfbtp-artifact-dir", str(tmp_path / "no_model"), "--flashbots-series", str(tmp_path / "no_series.csv"),
            "--candidate-batch-sizes", "40,45", "--hfbtp-block-map", "40:40,45:45", "--batch-size", "45"]


@pytest.mark.parametrize("extra, message", [
    (["--stage3-frame-quality", "--max-consecutive-abstentions", "3", "--max-initialization-frames", "0"], "positive integer"),
    (["--stage3-frame-quality", "--max-consecutive-abstentions", "3", "--max-initialization-frames", "-1"], "positive integer"),
    (["--stage3-frame-quality", "--max-initialization-frames", "2"], "needs --max-consecutive-abstentions"),
    (["--max-initialization-frames", "2"], "applies only with --stage3-frame-quality"),
])
def test_the_warm_up_option_fails_closed_at_argument_parsing(tmp_path, capsys, extra, message):
    module = load_script(INTEGRATED, "a0d_run_integrated_controller")
    with pytest.raises(SystemExit) as caught:
        module.main(integrated_args(tmp_path) + extra)
    assert caught.value.code == 2 and message in capsys.readouterr().err and not (tmp_path / "out").exists()


@pytest.mark.parametrize("extra, expected", [
    (["--stage3-frame-quality", "--max-consecutive-abstentions", "3", "--max-initialization-frames", "2"],
     {"max_consecutive_abstentions": 3, "max_initialization_frames": 2}),
    (["--stage3-frame-quality", "--max-consecutive-abstentions", "3"], {"max_consecutive_abstentions": 3}),
])
def test_the_command_passes_the_operators_numbers_to_the_contract_and_nothing_else(tmp_path, monkeypatch, extra, expected):
    module = load_script(INTEGRATED, "a0d_run_integrated_controller_2")
    seen = []
    real = Stage3FrameQualityContract.from_mapping.__func__
    monkeypatch.setattr(Stage3FrameQualityContract, "from_mapping",
                        classmethod(lambda cls, mapping: (seen.append(dict(mapping)), real(cls, mapping))[1]))
    try:
        module.main(integrated_args(tmp_path) + extra)
    except BaseException:                                                                    # it stops later, at the missing model directory
        pass
    assert seen == [expected]
    assert not (tmp_path / "out").exists()


def test_the_warm_up_option_has_no_default_value():
    tree = ast.parse(INTEGRATED.read_text(encoding="utf-8"))
    call = next(n for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "add_argument"
                and n.args and isinstance(n.args[0], ast.Constant) and n.args[0].value == "--max-initialization-frames")
    keywords = {k.arg: k.value for k in call.keywords}
    assert isinstance(keywords["default"], ast.Constant) and keywords["default"].value is None


def test_the_capture_command_takes_the_observation_journal_only_with_the_continuous_trio(tmp_path, capsys):
    module = load_script(PRODUCTION, "a0d_run_production_shadow_validation")
    with pytest.raises(SystemExit) as caught:
        module.main(["--run-id", "r", "--channel", CHANNEL, "--out", str(tmp_path / "out"),
                     "--ledger-observations-csv", str(tmp_path / "obs.csv")])
    assert caught.value.code == 2 and "--ledger-observations-csv needs" in capsys.readouterr().err and not (tmp_path / "out").exists()
    text = PRODUCTION.read_text(encoding="utf-8")
    assert "ledger_observations_csv=args.ledger_observations_csv" in text
    tree = ast.parse(text)
    call = next(n for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "add_argument"
                and n.args and isinstance(n.args[0], ast.Constant) and n.args[0].value == "--ledger-observations-csv")
    assert {k.arg: getattr(k.value, "value", "x") for k in call.keywords}["default"] is None


# ============================================================ the ledger value ages with the ledger observation, not with the reader
def test_a_ledger_value_read_later_keeps_its_observation_time_and_ages(tmp_path):
    rig = Rig(tmp_path, window_s=10.0)                                                      # a window longer than the freshness limit
    frames = healthy(rig, 2)
    polled_at = rig.clock.now
    rig.clock.advance(2.0)                                                                  # the reader looks 2 s after the collector did
    rig.client(5)
    later = rig.frame(2)
    metric = later.provenance["metrics"]["delivered_valid_tps"]
    assert metric["timestamp_utc"] == iso(polled_at) != iso(rig.clock.now)                  # the ledger's time, not the reader's
    assert metric["observation_timestamp_utc"] == iso(rig.clock.now) and metric["window_end_utc"] == iso(polled_at)
    assert later.freshness_seconds["delivered_valid_tps"] == pytest.approx(2.0)
    assert later.state.observed_throughput_tps == pytest.approx(4.0)                        # still inside the window: the value is real


def test_a_ledger_value_inside_the_window_but_older_than_the_freshness_limit_is_rejected_as_stale(tmp_path):
    rig = Rig(tmp_path, window_s=10.0)
    frames = healthy(rig, 2)
    rig.clock.advance(7.0)                                                                  # the collector is silent for 7 s; the client is not
    rig.client(5)
    frames.append(rig.frame(2))
    assert frames[2].state.observed_throughput_tps == pytest.approx(4.0)                    # the interval is still inside the 10 s window ...
    assert frames[2].freshness_seconds["delivered_valid_tps"] == pytest.approx(7.0)
    outcome = build_runner(limit=3, max_initialization_frames=1).run(frames)
    assert outcomes(outcome, 2) == {"abstain"} and codes(outcome, 2) == {"stale_observation"}   # ... but too old to decide on
    stale = next(v for v in outcome["validation_records"] if v["decision_index"] == 2)
    assert stale["details"] == {"age_s": pytest.approx(7.0), "max_age_s": 5.0} and "stale by 7.000s" in stale["error"]
