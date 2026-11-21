"""A0l workstream D (checklist items 32-39): generator issuance evidence carried through the A0f/A0h offline Stage 3 pipeline.

Generator issue log -> incremental reader -> source-count validation -> coverage attestation -> ArrivalHistoryStore -> Stage 3 history guard -> predictive
recommendation or abstention. Both routes are run: R1 (recorder arrivals, issue log as the independent source count) and R2 (generator attempts as arrivals).

Every run is the offline fixture pipeline fed by a SYNTHETIC scripted generator day. Forecast and performance values are scripted fixtures. Nothing is loaded,
forecast or sent anywhere; none of it is a Caliper or Fabric measurement, and no controller is judged.
"""

from __future__ import annotations

import json
import pickle
import socket
import subprocess

import pytest

from tests import a0j_support as j
from tests import a0l_support as a
from tests.a0j_support import by_controller
from tests.a0l_support import C, DAY_MS, Reader, SyntheticIssueLog, at, bridge, gc, ilr, rc, wc

from fabric_perf.controllers import abstention_accounting as abstention  # noqa: E402
from fabric_perf.controllers import live_foundation as lf  # noqa: E402

F = j.s.h.f
NL = bytes([10])


@pytest.fixture(autouse=True)
def nothing_loaded_nothing_sent(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a model load, process or network use is forbidden")
    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(pickle, "load", refuse)
    monkeypatch.setattr(pickle, "loads", refuse)
    monkeypatch.setattr(lf.StreamingEWMA, "__init__", refuse)


# ---------------------------------------------------------------------------------------------------------------------------- the two routes
def r2_run(tmp_path, *, name="r2", steps=None, tamper=False, **kw):
    """Generator route: the history is built from the issue log alone. ``tamper`` edits an already-read record just after the 5 h poll, so the next poll
    (at 6 h) notices it, long before the decision time."""
    def corrupt(tick_at, log):
        if tamper and tick_at == 18_000:
            data = log.path.read_bytes().split(NL)
            row = json.loads(data[-2])
            row["t_ms"] += 7
            data[-2] = wc.canonical(row).encode("ascii")
            log.path.write_bytes(NL.join(data))
    gen, reader = a.day_generator(tmp_path, name=name, after_poll=corrupt, **kw)
    history = a.day_history(reader)
    manifest, rows, out = j.s.h.run_captured(tmp_path, history, steps=steps, name=name)
    return reader, history, manifest, rows, out


def r1_run(tmp_path, *, name="r1", steps=None, missed=(), extra_zero=(), generator_every=10, **kw):
    """Recorder route: the recorder's submit callbacks are the arrivals; the issue log is the independent source count."""
    rec = a.recorder_for(tmp_path, missed=missed, extra_zero=extra_zero, name=name)
    gen, reader = a.day_generator(tmp_path, name=f"{name}_gen", every=generator_every, journal=False, **kw)
    evidence = reader.tail.evidence(instance=rec.instance, max_callback_lag_ms=a.LAG_MS)
    j.ingest(rec)
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    coverage = rc.RecorderVerifiedCoverage.from_recorder_journal(rec.dir / "journal.jsonl", source_id=rec.source_id, submit_events=events, policy=j.POLICY,
                                                                 run_id=j.RUN, source_evidence=evidence)
    history = rc.RecorderCapturedHistory(rc.bin_submit_events(events, j.recorder_source(rec.origin, source_id=rec.source_id)), coverage=coverage, policy=j.POLICY)
    manifest, rows, out = j.s.h.run_captured(tmp_path, history, steps=steps, name=name)
    return reader, history, manifest, rows, out


def coverage_block(rows):
    return by_controller(rows)["predictive"][1]["predictive_evidence"]["workload_observation"]["coverage"]


def codes(row) -> list:
    return row["predictive_evidence"]["workload_observation"]["coverage"].get("abstention_reason_codes")


# ---------------------------------------------------------------------------------------------------------------------- 32 valid construction
def test_32_a_complete_observed_generator_day_reaches_the_predictive_controller_with_issue_time_provenance(tmp_path):
    reader, history, manifest, rows, out = r2_run(tmp_path)
    assert manifest["status"] == "COMPLETED" and j.s.h.runner.verify_run_directory(out) == []
    groups = by_controller(rows)
    assert [r["outcome"] for r in groups["predictive"]][1:] == ["propose"] * 3
    block = coverage_block(rows)
    assert block["class_counts"]["verified_zero"] > 0 and block["class_counts"]["observed_positive"] > 0 and block["abstention_reason_codes"] == []
    assert block["coverage_scope"] == "source_stream_observer" and block["coverage_subject"] == "generator_issuance" and block["external_mempool_coverage"] is False
    assert block["issue_log"]["event_time"] == "generator_issue_time" and block["issue_log"]["descriptor"]["kind"] == "generator_issue_log"
    assert block["issue_log"]["run_id"] == a.RUN and block["issue_log"]["worker_id"] == "w0" and block["issue_log"]["worker_roster"] == ["w0"]
    assert {r["frame"]["status"] for r in rows if r["frame"]["index"] > 0} == {"accepted"}
    source = manifest["captured_history"]["source"]
    assert source["kind"] == gc.SOURCE_KIND_GENERATOR_ISSUE and source["event_time"] == "generator_issue_time" and source["synthetic"] is True
    assert "NOT Ethereum mempool arrivals" in source["represents"]


def test_32b_the_recorder_route_with_the_issue_log_as_source_evidence_also_reaches_the_predictive_controller(tmp_path):
    reader, history, manifest, rows, out = r1_run(tmp_path)
    assert manifest["status"] == "COMPLETED" and j.s.h.runner.verify_run_directory(out) == []
    assert [r["outcome"] for r in by_controller(rows)["predictive"]][1:] == ["propose"] * 3
    block = coverage_block(rows)
    assert block["class_counts"]["verified_zero"] > 0 and block["abstention_reason_codes"] == []
    assert block["coverage_scope"] == "recorder_callback" and block["source_evidence"]["kind"] == "issue_log"
    assert block["source_evidence"]["classifications"][history.coverage.events[0]["collector_instance"]]["class"] == a.sc.CLASS_INTERVAL
    assert block["live_quality"] is False and block["recorder_evidence"]["live_source_coverage_verified"] is False


def test_32c_the_arrivals_are_binned_by_generator_issue_time_not_by_recorder_callback_time(tmp_path):
    reader, history, *_ = r2_run(tmp_path)
    counts = history.binned.counts
    assert counts[10] == 1 and counts[11] == 0 and sum(counts) == len(reader.tail.attempt_events("x"))
    rec = a.recorder_for(tmp_path, name="cb")
    rec.write()
    events, _ = rc.read_submit_events(rec.submits_path, instance=rec.instance)
    assert events[1]["ts_ms"] - DAY_MS == 10_250                                          # the recorder's callback stamp is 150 ms later than the generator's issue time
    issued = reader.tail.attempt_events("x")[1]
    assert issued["ts_ms"] - DAY_MS == 10_100 and issued["read_ms"] >= issued["ts_ms"]


# ---------------------------------------------------------------------------------------------------------------------- 33 predictive-only abstention
HISTORY_REASONS = {"workload_prefix_incomplete_fail_safe", "prediction_failure_fail_safe", "arrival_history_missing"}   # incomplete, stale (A0i) or absent history


@pytest.mark.parametrize("variant", ["tick_gap", "generator_died_and_never_returned", "generator_never_ticked"])
def test_33_an_unobserved_generator_interval_abstains_only_the_predictive_controller(tmp_path, variant):
    hourly = {3600 * k for k in range(1, 13)}
    kwargs = {"tick_gap": {"ticks": hourly - {7200, 10_800}},
              "generator_died_and_never_returned": {"ticks": {t for t in hourly if t <= 18_000}, "late_ticks": False},
              "generator_never_ticked": {"ticks": set(), "late_ticks": False}}[variant]
    reader, history, manifest, rows, out = r2_run(tmp_path, **kwargs)
    groups = by_controller(rows)
    for row in groups["predictive"][1:]:
        assert row["outcome"] == "abstain" and row["abstention_scope"] == "controller" and row["frame"]["status"] == "accepted"
        assert row["reason"] in HISTORY_REASONS and row["forecast"] is None and row["predicted_performance"] is None, (variant, row["reason"])
        assert abstention.classify_row(row)[0] != abstention.VALID_RECOMMENDATION
    if variant == "tick_gap":
        assert {r["reason"] for r in groups["predictive"][1:]} == {"workload_prefix_incomplete_fail_safe"}
        assert gc.RC_INTERRUPTION in codes(groups["predictive"][1]) and abstention.classify_row(groups["predictive"][1])[0] == abstention.PREDICTIVE_HISTORY_ABSTENTION
    for name in ("static", "reactive"):
        assert [r["outcome"] for r in groups[name]][1:] == ["hold"] * 3                   # the shared frame is valid, so they still decide
    assert j.s.h.runner.verify_run_directory(out) == []


def test_33b_a_tampered_log_invalidates_the_history_and_abstains_only_the_predictive_controller(tmp_path):
    reader, history, manifest, rows, out = r2_run(tmp_path, tamper=True)
    assert reader.tail.error is not None and reader.tail.error.code == "log_replaced" and reader.tail.error_read_ms == DAY_MS + 21_600_000
    groups = by_controller(rows)
    assert {r["outcome"] for r in groups["predictive"][1:]} == {"abstain"} and all(r["forecast"] is None for r in groups["predictive"][1:])
    assert {r["outcome"] for name in ("static", "reactive") for r in groups[name][1:]} == {"hold"}
    assert gc.RC_INTEGRITY in codes(groups["predictive"][1])
    assert {f["validation_outcome"] for f in manifest["captured_history"]["frames"]} <= {a.ch.VALID_INCOMPLETE, a.ch.VALID_EMPTY, a.ch.VALID_INSUFFICIENT}


def test_33e_an_integrity_failure_found_after_a_decision_does_not_reach_back_into_it(tmp_path):
    gen, reader = a.day_generator(tmp_path, name="after")
    data = gen.log.path.read_bytes().split(NL)
    row = json.loads(data[-2])
    row["t_ms"] += 7
    data[-2] = wc.canonical(row).encode("ascii")
    gen.log.path.write_bytes(NL.join(data))
    reader.poll(43_210)                                                                  # noticed at 43,210, after the decisions at 43,201..43,204
    reader.bridge.sync(at(43_210))
    history = a.day_history(reader)
    manifest, rows, out = j.s.h.run_captured(tmp_path, history, name="after")
    assert reader.tail.error is not None and [r["outcome"] for r in by_controller(rows)["predictive"]][1:] == ["propose"] * 3
    assert not history.coverage.snapshot(at(43_211)).valid and history.coverage.snapshot(at(43_204)).valid


def test_33c_a_recorder_that_missed_callbacks_in_the_reconciled_span_abstains_the_predictive_controller_only(tmp_path):
    reader, history, manifest, rows, out = r1_run(tmp_path, missed={30_000}, name="miss")
    groups = by_controller(rows)
    for row in groups["predictive"][1:]:
        assert row["outcome"] == "abstain" and rc.RC_MISSING_CALLBACKS in codes(row), codes(row)
    assert {r["outcome"] for name in ("static", "reactive") for r in groups[name][1:]} == {"hold"}


def test_33d_a_dead_generator_log_leaves_the_recorder_route_unverified_and_the_predictive_controller_abstains(tmp_path):
    hourly = {3600 * k for k in range(1, 13)}
    reader, history, manifest, rows, out = r1_run(tmp_path, ticks={t for t in hourly if t <= 18_000}, until=43_204, name="dead", extra_zero=set(range(18_001, 43_205)))
    groups = by_controller(rows)
    assert {r["outcome"] for r in groups["predictive"][1:]} == {"abstain"} and {r["outcome"] for name in ("static", "reactive") for r in groups[name][1:]} == {"hold"}


# ---------------------------------------------------------------------------------------------------------------------- 34 shared-frame abstention
def test_34_an_invalid_shared_frame_gives_all_three_controllers_the_same_disposition_whatever_the_issue_evidence_says(tmp_path):
    steps = F.steady(3)
    steps[2] = F.step(client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5, config_reader_ok=False)
    steps[3] = F.step(client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5)
    reader, history, manifest, rows, out = r2_run(tmp_path, steps=steps, ticks={3600})  # the workload evidence is ALSO unobserved here
    frames: dict = {}
    for row in rows:
        frames.setdefault(row["frame"]["index"], []).append(row)
    bad = [i for i, group in frames.items() if {r["frame"]["status"] for r in group} == {"rejected"}]
    assert bad
    for index in bad:
        group = frames[index]
        assert {r["outcome"] for r in group} == {"abstain"} and {r["abstention_scope"] for r in group} == {"frame"}
        assert {abstention.classify_row(r)[0] for r in group} == {abstention.SHARED_INVALID_FRAME}
    assert manifest["status"] == "COMPLETED"


# ---------------------------------------------------------------------------------------------------------------------- 35 frame identity
@pytest.mark.parametrize("route", ["r2_ok", "r2_unobserved", "r1_ok", "r1_missed"])
def test_35_all_three_controllers_see_the_same_frame_on_every_issue_evidence_route(tmp_path, route):
    if route == "r2_ok":
        *_r, manifest, rows, _o = r2_run(tmp_path)
    elif route == "r2_unobserved":
        *_r, manifest, rows, _o = r2_run(tmp_path, ticks={3600})
    elif route == "r1_ok":
        *_r, manifest, rows, _o = r1_run(tmp_path)
    else:
        *_r, manifest, rows, _o = r1_run(tmp_path, missed={30_000})
    assert j.s.h.runner.check_comparability(rows) == {"frames": 4, "comparable": True, "problems": []}
    for index in range(1, 4):
        group = [r for r in rows if r["frame"]["index"] == index]
        assert len(group) == 3 and len({wc.canonical(r["frame"]) for r in group}) == 1


# ---------------------------------------------------------------------------------------------------------------------- 36 provenance
def test_36_the_decision_record_preserves_the_issue_evidence_provenance_on_both_routes(tmp_path):
    reader, history, manifest, rows, out = r2_run(tmp_path, name="p2")
    block = coverage_block(rows)
    assert block["evidence_origins"] == ["synthetic_fixture"] and block["synthetic_evidence"] is True and block["live_quality"] is False
    assert block["integrity"]["status"] == "VERIFIED" and block["chain_head_sha256"] and block["journal_sha256"]
    assert block["issue_log"]["attempts_read_by_cutoff"] > 0 and "DECLARED" in block["issue_log"]["independent_of_recorder"]
    assert "EXPERIMENTAL GENERATOR" in block["claim"] and "nothing about an external Ethereum mempool" in block["claim"]
    assert block["live_source_coverage_verified"] is False
    evidence = manifest["captured_history"]
    assert evidence["source"]["evidence_origin"] == "synthetic_fixture" and evidence["source"]["coverage_subject"] == "generator_issuance"
    reader1, history1, manifest1, rows1, out1 = r1_run(tmp_path, name="p1")
    block1 = coverage_block(rows1)
    assert block1["source_evidence"]["kind"] == "issue_log" and block1["source_evidence"]["independence"].startswith("DECLARED")
    assert block1["recorder_evidence"]["source_completeness"]["independence"].startswith("DECLARED") and block1["recorder_evidence"]["synthetic"] is True


def test_36b_a_decision_never_sees_issue_records_that_had_not_been_read_by_its_cutoff(tmp_path):
    reader, history, *_ = r2_run(tmp_path, name="late", latency=3.0)                      # every tick is read 3 s after it was written
    cutoff = at(43_201 + 1.0)
    early = history.coverage.snapshot(cutoff)
    assert early.derivation.frontier is None or early.derivation.frontier <= cutoff
    assert history.coverage.snapshot(at(43_205)).classify(at(43_203), 0)[0] == C.STALE_COLLECTOR_EVIDENCE      # the tick covering it is read at 43,207
    assert history.coverage.snapshot(at(43_210)).classify(at(43_203), 0)[0] in {C.VERIFIED_ZERO, C.OBSERVED_POSITIVE}
    reads = reader.tail.attempt_events("x", as_of_ms=DAY_MS + 43_201_000)
    assert all(e["read_ms"] <= DAY_MS + 43_201_000 for e in reads) and len(reads) < len(reader.tail.attempt_events("x"))


# ---------------------------------------------------------------------------------------------------------------------- 37 denominator
def test_37_every_scheduled_opportunity_is_counted_and_the_categories_sum_to_the_denominator(tmp_path):
    *_r, rows, _o = r2_run(tmp_path, ticks={3600 * k for k in range(1, 13)} - {7200, 10_800})
    result = abstention.account(rows, scheduled_frames=4, segment="a0l-synthetic")
    assert result["consistent"] is True
    predictive, static = result["controllers"]["predictive"], result["controllers"]["static"]
    counts = predictive["category_counts"]
    assert predictive["total_scheduled_decision_opportunities"] == 4 and sum(counts.values()) == 4
    assert counts[abstention.PREDICTIVE_HISTORY_ABSTENTION] == 3 and counts[abstention.INITIALIZATION] == 1 and counts[abstention.VALID_RECOMMENDATION] == 0
    assert static["category_counts"][abstention.VALID_RECOMMENDATION] == 3 and predictive["decision_coverage_rate"]["rate"] == 0.0
    *_r2, rows2, _o2 = r2_run(tmp_path, name="ok")
    ok = abstention.account(rows2, scheduled_frames=4, segment="a0l-synthetic")["controllers"]["predictive"]["category_counts"]
    assert ok[abstention.VALID_RECOMMENDATION] == 3 and sum(ok.values()) == 4


def test_37b_reason_codes_never_change_the_accounting_categories(tmp_path):
    *_r, rows, _o = r1_run(tmp_path, missed={30_000})
    counts = abstention.account(rows, scheduled_frames=4, segment="a0l-synthetic")["controllers"]["predictive"]["category_counts"]
    assert counts[abstention.PREDICTIVE_HISTORY_ABSTENTION] == 3 and sum(counts.values()) == 4


# ---------------------------------------------------------------------------------------------------------------------- 38 no actuation
@pytest.mark.parametrize("route", ["r2", "r1", "r2_unobserved"])
def test_38_no_actuator_is_invoked_on_any_issue_evidence_route(tmp_path, route):
    if route == "r2":
        _r, _h, manifest, rows, out = r2_run(tmp_path)
    elif route == "r1":
        _r, _h, manifest, rows, out = r1_run(tmp_path)
    else:
        _r, _h, manifest, rows, out = r2_run(tmp_path, ticks={3600})
    assert manifest["actuation"]["configuration_changes_applied"] == 0 and manifest["actuation"]["live_actuator_available"] is False
    assert all(r["actuator"]["submitted_to_fabric"] is False and r["actuator"]["live_actuator_available"] is False for r in rows)


# ---------------------------------------------------------------------------------------------------------------------- 39 real inference disabled
def test_39_real_inference_stays_disabled_and_no_model_or_ewma_is_loaded(tmp_path):
    for fn in (r2_run, r1_run):
        sub = tmp_path / fn.__name__
        sub.mkdir()
        _r, _h, manifest, rows, out = fn(sub)
        summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
        assert manifest["models"]["frozen_hfbtp_model_loaded"] is False and manifest["models"]["frozen_ewma_loaded"] is False
        assert summary["real_inference_performed"] is False and j.s.h.runner.sp.REAL_INFERENCE_AUTHORIZED is False
        assert "fixture" in json.dumps(manifest, sort_keys=True).lower()                  # the forecast stays labelled as a fixture, never as model output
