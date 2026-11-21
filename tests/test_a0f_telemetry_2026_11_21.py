"""A0f: the telemetry path from client rows and ledger evidence to validated frames, as the runner assembles it (2026-11-21).

Covers checklist items 10-19 and the rules that a missing value is never zero, a stale value is never healthy, an unknown
configuration is never reported, and the client workload rate is not the committed throughput. All sources are fixtures.
"""

from __future__ import annotations

import csv
import dataclasses
import socket
import subprocess
import urllib.request
from pathlib import Path

import pytest

from tests import a0f_support as s
from fabric_perf.controllers import client_latency as cl
from fabric_perf.controllers import ledger_evidence as le
from fabric_perf.controllers import prelive_contract as pc
from fabric_perf.controllers import stage3_shadow_runner as runner
from fabric_perf.controllers.telemetry_freshness import FreshnessPolicy


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("a process or network call was attempted")
    for obj, names in ((subprocess, ("run", "Popen", "call", "check_output")), (socket, ("create_connection", "getaddrinfo")),
                       (urllib.request, ("urlopen",))):
        for name in names:
            monkeypatch.setattr(obj, name, boom)


def c(n=5, latency=100, status="success"):
    return {"count": n, "latency_ms": latency, "status": status}


def frames_of(tmp_path, steps, **kw):
    manifest, rows, out = s.run(tmp_path, fixture_doc=s.fixture(steps=steps), **kw)
    return s.by_frame(rows), rows, out


def rig(tmp_path, *, window_s=4.0, stale_after_s=5.0):
    fixture = runner.validate_fixture(s.fixture())
    return runner.FixtureRig(tmp_path / "rig", channel="e1-offline.a0f", run_id="a0f-fixture-001", fixture=fixture, window_s=window_s,
                             stale_after_s=stale_after_s, recorder_latency="derive_from_timestamps",
                             endpoints={"orderer1": "http://localhost:9443/metrics"}, reported=fixture.channel_configuration,
                             freshness=FreshnessPolicy(max_age_s=5.0), horizon_seconds=10)


def primed(tmp_path, **kw):
    """A rig after baseline + one measured interval with 5 client completions and a 5-transaction block."""
    r = rig(tmp_path, **kw)
    r.advance(1); r.poll()
    r.advance(1); r.client(5); r.commit_ledger(5); r.poll()
    return r


# ---------------------------------------------------------------------------------------------- 10 valid client observation
def test_a_valid_client_observation_reaches_every_controller_with_its_measurement_definition(tmp_path):
    frames, _, _ = frames_of(tmp_path, s.steady(2))
    for name, row in frames[1].items():
        assert row["frame"]["status"] == "accepted"
        assert row["observed"]["client_latency_mean_ms"] == 100.0 and row["observed"]["client_latency_p95_ms"] == 100.0
        assert row["observed"]["client_offered_rate_tps"] > 0
        assert row["evidence"]["client_integrity"] == "OK"
        assert row["measurement"]["client_latency"]["definition"] == cl.LATENCY_DEFINITION      # the one verified definition
        assert row["measurement"]["client_latency"]["unit"] == "ms"
        assert row["measurement"]["client_latency"]["eligible_samples"] >= 5


def test_the_recorder_header_matches_the_a0d_synthetic_rig_and_the_v2_recorder_columns():
    from tests.a0d_support import RECORDER_HEADER, SUBMIT_HEADER
    assert list(runner.RECORDER_HEADER) == RECORDER_HEADER and list(runner.SUBMIT_HEADER) == SUBMIT_HEADER
    v2 = (s.E1 / "caliper" / "e1_per_tx_recorder_v2.js").read_text(encoding="utf-8")
    for column in ("tx_id", "latency_ms", "final_ms", "create_ms", "latency_state"):
        assert column in v2


# ---------------------------------------------------------------------------------------------- 11 valid ledger observation
def test_committed_throughput_comes_from_the_ledger_and_is_not_the_client_rate(tmp_path):
    steps = [s.step(client=c(5)), s.step(client=c(5), ledger_valid_tx=3)]
    frames, _, _ = frames_of(tmp_path, steps)
    row = frames[1]["static"]
    ledger = row["measurement"]["ledger_committed_throughput"]
    assert ledger["valid_in_interval"] == 3 and ledger["interval_s"] == 1.0
    assert row["observed"]["ledger_committed_throughput_tps"] == 3.0
    assert row["observed"]["client_offered_rate_tps"] != row["observed"]["ledger_committed_throughput_tps"]
    assert "VALID" in ledger["definition"] and "invalid-status envelopes are counted separately" in ledger["definition"]
    assert ledger["state"] == "measuring"
    assert row["evidence"]["ledger_latest_observation_seq"] == 2 and row["evidence"]["ledger_baseline_height"] == 0


def test_the_window_semantics_of_each_source_are_recorded_separately(tmp_path):
    frames, _, _ = frames_of(tmp_path, s.steady(2))
    m = frames[2]["static"]["measurement"]
    assert m["client_latency"]["window_start_utc"] and m["client_latency"]["window_end_utc"]
    assert m["ledger_committed_throughput"]["interval_start_utc"] and m["ledger_committed_throughput"]["interval_end_utc"]
    assert "event time" in m["client_latency"]["event_time_basis"]
    assert "observation time" in m["ledger_committed_throughput"]["time_basis"]


# ---------------------------------------------------------------------------------------------- 12 missing latency
def test_missing_latency_is_null_never_zero_and_every_controller_abstains(tmp_path):
    steps = [s.step(client=c()), s.step(client=c(), ledger_valid_tx=5), s.step(6.0, client=c(status="failed"), ledger_valid_tx=5)]
    frames, _, _ = frames_of(tmp_path, steps)
    for row in frames[2].values():
        assert row["observed"]["client_latency_mean_ms"] is None and row["observed"]["client_latency_p95_ms"] is None
        assert row["frame"]["status"] == "rejected" and row["outcome"] == "abstain" and row["recommended_config"] is None
        assert row["abstention_scope"] == "frame"


# ---------------------------------------------------------------------------------------------- 13 missing throughput
def test_missing_committed_throughput_is_null_never_zero(tmp_path):
    steps = [s.step(client=c(), poll=False), s.step(client=c(), poll=False)]
    frames, _, _ = frames_of(tmp_path, steps)
    for index in (0, 1):
        for row in frames[index].values():
            assert row["observed"]["ledger_committed_throughput_tps"] is None
            assert row["frame"]["status"] == "rejected" and row["outcome"] == "abstain"
            assert "observed_throughput_tps" in (row["error"] or "")


def test_a_baseline_only_journal_is_initialization_not_zero_throughput(tmp_path):
    frames, _, _ = frames_of(tmp_path, s.steady(1))
    for row in frames[0].values():
        assert row["observed"]["ledger_committed_throughput_tps"] is None
        assert row["frame"]["status"] == "initialization"


# ---------------------------------------------------------------------------------------------- 14 stale telemetry
def test_stale_client_telemetry_is_never_a_healthy_observation(tmp_path):
    steps = [s.step(client=c()), s.step(client=c(), ledger_valid_tx=5), s.step(20.0)]
    frames, _, _ = frames_of(tmp_path, steps)
    for row in frames[2].values():
        assert row["frame"]["status"] == "rejected" and row["outcome"] == "abstain"
        assert row["observed"]["client_latency_mean_ms"] is None and row["observed"]["client_offered_rate_tps"] is None
    assert frames[1]["static"]["frame"]["status"] == "accepted"          # the same run's earlier fresh frame was fine


def test_a_collector_that_stops_makes_ledger_evidence_unusable_not_healthy(tmp_path):
    steps = [s.step(client=c()), s.step(client=c(), ledger_valid_tx=5)] + [s.step(4.0, client=c(), poll=False)]
    frames, _, _ = frames_of(tmp_path, steps)
    for row in frames[2].values():
        assert row["frame"]["status"] == "rejected" and row["observed"]["ledger_committed_throughput_tps"] is None


def test_the_operator_freshness_limit_is_the_one_applied(tmp_path):
    # client completions 3 s old (the frame's telemetry age is about 3.15 s): fresh under a 5 s limit, stale under a 2 s limit
    steps = [s.step(client=c()), s.step(client=c(), ledger_valid_tx=5), s.step(3.0, ledger_valid_tx=5)]
    loose = frames_of(tmp_path, steps, name="loose")[0]
    assert loose[2]["static"]["frame"]["status"] == "accepted"
    policy = s.policy(telemetry={"freshness_max_age_s": 2.0, "client_stale_after_s": 5.0})
    strict = frames_of(tmp_path, steps, name="strict", policy_doc=policy)[0]
    assert strict[2]["static"]["frame"]["status"] == "rejected"
    assert all(r["outcome"] == "abstain" for r in strict[2].values())


def test_very_old_client_data_next_to_fresh_ledger_data_is_an_inconsistent_frame_not_a_healthy_one(tmp_path):
    steps = [s.step(client=c()), s.step(client=c(), ledger_valid_tx=5), s.step(6.0, ledger_valid_tx=5)]
    params = s.params()
    params["telemetry_adapter"]["window_s"] = 12.0
    policy = s.policy(telemetry={"freshness_max_age_s": 30.0, "client_stale_after_s": 30.0})
    frames = frames_of(tmp_path, steps, params_doc=params, policy_doc=policy)[0]
    # even with a generous operator limit the library refuses to pair 6 s old client evidence with a fresh ledger interval
    assert frames[2]["static"]["frame"]["status"] == "rejected"
    assert frames[2]["static"]["frame"]["reason_code"] == "telemetry_inconsistent"
    assert "source_timestamp_skew_exceeds_limit" in frames[2]["static"]["error"]


# ---------------------------------------------------------------------------------------------- 15 duplicate evidence
def test_duplicate_client_and_ledger_rows_are_not_double_counted(tmp_path):
    clean, dup = primed(tmp_path / "a"), primed(tmp_path / "b")
    lines = dup.recorder_csv.read_text().splitlines()
    dup.recorder_csv.write_text("\n".join(lines + lines[1:]) + "\n")
    tx = Path(dup.collector.paths["tx"])
    lines = tx.read_text().splitlines()
    tx.write_text("\n".join(lines + lines[1:]) + "\n")
    f_clean, f_dup = clean.frame(0), dup.frame(0)
    for field in ("observed_throughput_tps", "observed_latency_ms", "observed_arrival_rate_tps", "observed_latency_p95_ms"):
        assert getattr(f_dup.state, field) == getattr(f_clean.state, field), field
    assert f_dup.state.observed_throughput_tps == 5.0


# ---------------------------------------------------------------------------------------------- 16 timestamp mismatch
def future_source(frame, source):
    return dataclasses.replace(frame, source_timestamps_utc={**frame.source_timestamps_utc, source: "2026-11-21T10:05:00.000+00:00"})


def test_a_future_dated_source_timestamp_rejects_the_frame_for_every_controller_alike(tmp_path):
    steps = s.steady(2)
    frames, _, _ = frames_of(tmp_path, steps, transform=lambda i, f: future_source(f, "ledger") if i == 2 else f)
    reasons = {(r["frame"]["reason_code"], r["frame"]["abstention_reason"]) for r in frames[2].values()}
    assert len(reasons) == 1 and next(iter(reasons))[0] == "future_dated_observation"
    assert all(r["outcome"] == "abstain" for r in frames[2].values())
    assert {r["frame"]["id"] for r in frames[2].values()} and len({r["frame"]["id"] for r in frames[2].values()}) == 1


def test_a_state_timestamp_after_the_observation_rejects_the_frame(tmp_path):
    def later_state(index, frame):
        if index != 2:
            return frame
        state = dataclasses.replace(frame.state, timestamp_utc="2026-11-21T10:30:00.000+00:00")
        return dataclasses.replace(frame, state=state)
    frames, _, _ = frames_of(tmp_path, s.steady(2), transform=later_state)
    assert {r["frame"]["reason_code"] for r in frames[2].values()} == {"state_timestamp_after_observation"}


def test_a_client_row_stamped_after_the_frame_is_not_counted_in_that_frame(tmp_path):
    r = primed(tmp_path)
    base = r.frame(0)
    now = runner._epoch_ms(r.clock.now)
    with r.recorder_csv.open("a", newline="") as fh:
        csv.writer(fh, lineterminator="\n").writerow(["cfuture", 99, now + 59_900, now + 60_000, 100, "ok", "success", "", "", now, 0, 0, 1, "fixture"])
    after = r.frame(1)
    assert after.state.observed_latency_ms == base.state.observed_latency_ms == 100.0
    assert after.state.observed_arrival_rate_tps == base.state.observed_arrival_rate_tps


# ---------------------------------------------------------------------------------------------- 17 window mismatch
@pytest.mark.parametrize("window", [1.0, 0.5])
def test_an_adapter_window_not_above_the_poll_interval_is_refused_by_the_contract(window):
    doc = s.params()
    doc["telemetry_adapter"]["window_s"] = window                   # poll_interval_s is 1.0
    with pytest.raises(pc.PreliveParameterError):
        pc.validate_operator_parameters(doc)


@pytest.mark.parametrize("window", [4.0, 6.0])
def test_the_operator_window_is_the_window_the_adapter_measures_over(tmp_path, window):
    doc = s.params()
    doc["telemetry_adapter"]["window_s"] = window
    frames, _, _ = frames_of(tmp_path, s.steady(3), params_doc=doc)
    latency = frames[3]["static"]["measurement"]["client_latency"]
    from datetime import datetime
    start = datetime.fromisoformat(latency["window_start_utc"])
    end = datetime.fromisoformat(latency["window_end_utc"])
    assert (end - start).total_seconds() == window


def test_a_collector_span_shorter_than_the_planned_round_is_refused_by_the_contract():
    doc = s.params()
    doc["ledger_collector"]["max_polls"] = 2
    with pytest.raises(pc.PreliveParameterError):
        pc.validate_operator_parameters(doc)


# ---------------------------------------------------------------------------------------------- 18 failed collector observation
def test_a_failed_ledger_read_gives_no_throughput_value_and_abstention_not_zero(tmp_path):
    steps = [s.step(client=c()), s.step(client=c(), ledger_valid_tx=5), s.step(client=c(), ledger_read_ok=False)]
    frames, _, _ = frames_of(tmp_path, steps)
    for row in frames[2].values():
        assert row["observed"]["ledger_committed_throughput_tps"] is None
        assert row["frame"]["status"] == "rejected" and row["outcome"] == "abstain"


def test_the_run_recovers_after_a_failed_ledger_read_and_the_gap_is_not_filled(tmp_path):
    steps = [s.step(client=c()), s.step(client=c(), ledger_valid_tx=5), s.step(client=c(), ledger_read_ok=False),
             s.step(client=c(), ledger_valid_tx=5), s.step(client=c(), ledger_valid_tx=5)]
    frames, _, _ = frames_of(tmp_path, steps)
    assert [frames[i]["static"]["frame"]["status"] for i in range(5)] == ["initialization", "accepted", "rejected", "accepted", "accepted"]
    # The failed read is not an observation. The next rate is taken over the span between observations that really happened
    # (baseline at t=1 to the read at t=4: 10 valid transactions in 3 s), not a copy of the earlier value and not a fill-in.
    after = frames[3]["static"]
    assert after["observed"]["ledger_committed_throughput_tps"] == pytest.approx(10 / 3)
    assert after["measurement"]["ledger_committed_throughput"]["interval_s"] == 3.0
    assert after["measurement"]["ledger_committed_throughput"]["valid_in_interval"] == 10


# ---------------------------------------------------------------------------------------------- 19 genuine zero
def test_a_genuine_zero_committed_throughput_interval_is_zero_and_accepted(tmp_path):
    steps = [s.step(client=c()), s.step(client=c(), ledger_valid_tx=5), s.step(5.0, client=c())]
    frames, _, _ = frames_of(tmp_path, steps)
    for row in frames[2].values():
        assert row["observed"]["ledger_committed_throughput_tps"] == 0.0          # measured zero, not None
        assert row["observed"]["ledger_committed_throughput_tps"] is not None
        assert row["measurement"]["ledger_committed_throughput"]["valid_in_interval"] == 0
        assert row["measurement"]["ledger_committed_throughput"]["interval_s"] == 5.0
        assert row["frame"]["status"] == "accepted"
    assert frames[2]["static"]["outcome"] == "hold"


# ---------------------------------------------------------------------------------------------- configuration state
def test_an_unknown_configuration_is_recorded_as_unknown_not_as_the_library_default(tmp_path):
    steps = s.steady(2) + [s.step(client=c(), ledger_valid_tx=5, config_reader_ok=False)]
    frames, _, _ = frames_of(tmp_path, steps)
    for row in frames[3].values():
        assert row["known_actual_config"] is None
        assert "unknown" in row["known_actual_config_basis"]
        assert row["frame"]["status"] == "rejected" and row["outcome"] == "abstain"
        assert row["recommendation_matches_known_actual_config"] is None
    assert frames[1]["static"]["known_actual_config"]["max_message_count"] == 40


def test_the_known_actual_configuration_is_what_the_reader_reported_not_what_was_recommended(tmp_path):
    steps = s.steady(1) + [s.step(client=c(), ledger_valid_tx=5, reported_max_message_count=45)]
    frames, _, _ = frames_of(tmp_path, steps)
    static = frames[2]["static"]
    assert static["known_actual_config"]["max_message_count"] == 45 and static["recommended_config"]["max_message_count"] == 40
    assert static["recommendation_matches_known_actual_config"] is False
    assert "not independently verified" in static["known_actual_config_basis"]


def test_a_forecast_is_kept_apart_from_the_observed_workload(tmp_path):
    frames, _, _ = frames_of(tmp_path, s.steady(2))
    row = frames[2]["predictive"]
    assert row["predicted_arrival_rate_tps"] == 55.0 and row["forecast"]["value_tps"] == 55.0
    assert row["observed"]["client_offered_rate_tps"] != row["predicted_arrival_rate_tps"]
    assert row["forecast"]["model_identity"].startswith("FIXTURE_MOCK_FORECAST")
    assert frames[2]["static"]["forecast"] is None and frames[2]["reactive"]["forecast"] is None
