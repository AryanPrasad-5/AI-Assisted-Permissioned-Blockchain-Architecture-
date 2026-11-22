"""A0q workstream E (Stage 3 items 32-40): a RECOVERED generator-route journal carried through the existing offline Stage 3 pipeline.

RecoverableSourceCountReader (crashed and restarted) -> ``GeneratorIssueCoverage`` -> captured workload history -> Stage 3 history guard -> predictive recommendation or abstention.
Forecast and performance values are scripted fixtures; nothing is loaded, forecast or sent anywhere; none of it is a Caliper or Fabric measurement. No controller algorithm is changed."""

from __future__ import annotations

import json
import pickle
import socket
import subprocess

import pytest

from tests import a0j_support as j
from tests import a0q_support as q
from tests.a0j_support import by_controller
from tests.a0o_support import DAY_GAP_MS, NL, a, at, gc, history_of, ident, ilr, sr, wc

from fabric_perf.controllers import abstention_accounting as abstention  # noqa: E402
from fabric_perf.controllers import issue_log_bridge_recovery as rec     # noqa: E402
from fabric_perf.controllers import live_foundation as lf                # noqa: E402

F = j.s.h.f
HISTORY_REASONS = {"workload_prefix_incomplete_fail_safe", "prediction_failure_fail_safe", "arrival_history_missing"}
HOURLY = {3600 * k for k in range(1, 13)}


@pytest.fixture(autouse=True)
def nothing_loaded_nothing_sent(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a model load, process or network use is forbidden")
    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(pickle, "load", refuse)
    monkeypatch.setattr(pickle, "loads", refuse)
    monkeypatch.setattr(lf.StreamingEWMA, "__init__", refuse)


def day_reader(tmp, *, name, crash_poll=None, restart=True, ticks=None, late_ticks=True, every=10, until=43_204):
    """A day of generator traffic read INCREMENTALLY by a ``RecoverableSourceCountReader``. At poll number ``crash_poll`` the process dies after the checkpoint was replaced and before
    the bridge derived anything; the reader is then rebuilt from the files (``restart``) or, when ``restart`` is False, never advanced again (the generator keeps writing)."""
    log = a.SyntheticIssueLog(tmp / f"{name}.issue.jsonl", tick_interval_ms=3_600_000, round_index=q.ROUND)
    gen = a.Scripted(log).start(-0.5)
    identity = ident(log.path, round_index=q.ROUND, checkpoint=tmp / f"{name}.ckpt.json")
    make = lambda: rec.RecoverableSourceCountReader(identity, policy_limits={"max_tick_gap_ms": DAY_GAP_MS}, journal_path=tmp / f"{name}.journal.jsonl", source_id=a.SOURCE_ID,
                                                    origin=gc.ORIGIN_SYNTHETIC, recovery_log_path=tmp / f"{name}.recovery.jsonl")
    reader = make()
    alive = True
    polls = 0

    def poll(second):
        nonlocal reader, alive, polls
        if not alive:
            return
        polls += 1
        if crash_poll is not None and polls == crash_poll:
            mp = pytest.MonkeyPatch()
            q.install(mp, "death_after_checkpoint_before_sync", 1)
            try:
                reader.poll(at(second))
            except q.Death:
                pass
            finally:
                mp.undo()
            reader = make()
            reader.restart()
            alive = restart
            if restart:
                reader.poll(at(second))                          # the interrupted poll is repeated at its scheduled time
            return
        reader.poll(at(second))

    poll(-0.4)
    tick_seconds = sorted((ticks if ticks is not None else {t for t in HOURLY if t <= until}) | (set(range(43_201, until + 1)) if late_ticks else set()))
    done = -1
    for tick_at in tick_seconds:
        for sec in range(done + 1, tick_at):
            if sec % every == 0:
                gen.tx(sec + 0.1)
        done = tick_at - 1
        gen.tick(tick_at)
        poll(tick_at)
    return gen, reader


def run(tmp, *, name="a0q", steps=None, **kw):
    gen, reader = day_reader(tmp, name=name, **kw)
    history = history_of(reader)
    manifest, rows, out = j.s.h.run_captured(tmp, history, steps=steps, name=name)
    return reader, history, manifest, rows, out


def outcomes(rows):
    return {name: [r["outcome"] for r in group] for name, group in by_controller(rows).items()}


# ----------------------------------------------------------------------------------------------------------------------------- 32 recovered valid history
@pytest.mark.parametrize("crash_poll", [4, 9, 14])
def test_32_a_recovered_valid_source_history_gives_the_uninterrupted_decisions(tmp_path, crash_poll):
    plain_dir, crash_dir = tmp_path / "plain", tmp_path / "crash"
    plain_dir.mkdir(), crash_dir.mkdir()
    plain = run(plain_dir)
    crashed = run(crash_dir, crash_poll=crash_poll)
    assert q.sha(plain_dir / "a0q.journal.jsonl") == q.sha(crash_dir / "a0q.journal.jsonl")                      # byte-identical journal
    assert crashed[0].restarts == 1 and crashed[0].recovery_report["completed_after_restart"]                    # something WAS completed after the restart
    assert crashed[2]["status"] == "COMPLETED" and j.s.h.runner.verify_run_directory(crashed[4]) == []
    assert outcomes(crashed[3]) == outcomes(plain[3]) and [o for o in outcomes(crashed[3])["predictive"]][1:] == ["propose"] * 3
    for cutoff in (18_000.0, 43_201.0, 43_204.0):
        a_, b_ = plain[1].coverage_provenance(at(cutoff)), crashed[1].coverage_provenance(at(cutoff))
        assert a_["class_counts"] == b_["class_counts"] and a_["abstention_reason_codes"] == b_["abstention_reason_codes"] == []
    assert crashed[0].evidence()["status"] == sr.ST_VERIFIED


# ----------------------------------------------------------------------------------------------------------------------------- 33-34 incomplete recovery
@pytest.mark.parametrize("variant", ["reader_never_restarted_after_the_crash", "generator_never_ticked"])
def test_33_incomplete_recovered_history_abstains_only_the_predictive_controller(tmp_path, variant):
    if variant == "reader_never_restarted_after_the_crash":
        reader, history, manifest, rows, out = run(tmp_path, crash_poll=6, restart=False)               # the journal holds only the prefix the crash left (plus recovery)
        assert reader.recovery_report["status"] in {rec.ST_RECOVERED, rec.ST_NOTHING}
        total = len((tmp_path / "a0q.issue.jsonl").read_bytes().split(NL)) - 1
        assert reader.tail.counts()["records"] < total                                               # the reader is behind the generator: it never saw the rest
    else:
        reader, history, manifest, rows, out = run(tmp_path, ticks=set(), late_ticks=False)
    groups = by_controller(rows)
    for row in groups["predictive"][1:]:                                                                  # 34: predictive-only abstention
        assert row["outcome"] == "abstain" and row["abstention_scope"] == "controller" and row["frame"]["status"] == "accepted"
        assert row["reason"] in HISTORY_REASONS and row["forecast"] is None and row["predicted_performance"] is None
        assert abstention.classify_row(row)[0] != abstention.VALID_RECOMMENDATION
    for name in ("static", "reactive"):
        assert [r["outcome"] for r in groups[name]][1:] == ["hold"] * 3                                   # the shared frame is valid, so they still decide
    if variant == "generator_never_ticked":
        assert reader.evidence()["status"] != sr.ST_VERIFIED


def test_34_recorder_style_heartbeats_cannot_certify_a_recovered_generator_zero(tmp_path):
    """Generator zeros need generator-source liveness evidence; the recovered journal declares scope and subject and nothing else can stand in for them."""
    reader, history, manifest, rows, out = run(tmp_path, crash_poll=5)
    starts = [ev for ev in history.coverage.events if ev.get("kind") == "start"]
    assert len(starts) == 1 and starts[0]["coverage_scope"] == "source_stream_observer" and starts[0]["coverage_subject"] == gc.COVERAGE_SUBJECT
    block = by_controller(rows)["predictive"][1]["predictive_evidence"]["workload_observation"]["coverage"]
    assert block["coverage_scope"] == "source_stream_observer" and block["coverage_subject"] == "generator_issuance"
    assert block["external_mempool_coverage"] is False and block["live_quality"] is False and block["live_source_coverage_verified"] is False
    assert block["class_counts"]["verified_zero"] > 0 and block["synthetic_evidence"] is True
    assert reader.bridge.instance.endswith(":w0:r1")                                                      # the round is still in the journal's collector instance


# ----------------------------------------------------------------------------------------------------------------------------- 35-36 shared frame and identity
def test_35_an_invalid_shared_frame_abstains_all_three_controllers_with_one_disposition(tmp_path):
    steps = F.steady(3)
    steps[2] = F.step(client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5, config_reader_ok=False)
    steps[3] = F.step(client={"count": 5, "latency_ms": 100}, ledger_valid_tx=5)
    reader, history, manifest, rows, out = run(tmp_path, steps=steps, name="bad", crash_poll=7)
    frames: dict = {}
    for row in rows:
        frames.setdefault(row["frame"]["index"], []).append(row)
    bad = [i for i, group in frames.items() if {r["frame"]["status"] for r in group} == {"rejected"}]
    assert bad
    for index in bad:
        group = frames[index]
        assert {r["outcome"] for r in group} == {"abstain"} and {r["abstention_scope"] for r in group} == {"frame"}
        assert {abstention.classify_row(r)[0] for r in group} == {abstention.SHARED_INVALID_FRAME}
    assert manifest["status"] == "COMPLETED" and reader.restarts == 1


@pytest.mark.parametrize("route", ["recovered_valid", "recovered_tick_gap"])
def test_36_all_three_controllers_share_one_frame_identity_on_a_recovered_route(tmp_path, route):
    kwargs = {"crash_poll": 8} if route == "recovered_valid" else {"crash_poll": 8, "ticks": HOURLY - {7200, 10_800}}
    *_r, manifest, rows, _o = run(tmp_path, name=route, **kwargs)
    assert j.s.h.runner.check_comparability(rows) == {"frames": 4, "comparable": True, "problems": []}
    for index in range(1, 4):
        group = [r for r in rows if r["frame"]["index"] == index]
        assert len(group) == 3 and len({wc.canonical(r["frame"]) for r in group}) == 1


# ----------------------------------------------------------------------------------------------------------------------------- 37-38 no retroactive evidence, denominator
def test_37_evidence_read_after_a_decision_does_not_reach_back_into_it(tmp_path):
    reader, history, manifest, rows, out = run(tmp_path, crash_poll=6)
    before = [json.dumps(r, sort_keys=True) for r in rows]
    data = (tmp_path / "a0q.issue.jsonl").read_bytes().split(NL)
    row = json.loads(data[-2])
    row["t_ms"] += 7
    data[-2] = wc.canonical(row).encode("ascii")
    (tmp_path / "a0q.issue.jsonl").write_bytes(NL.join(data))
    reader.poll(at(43_210))                                                                               # noticed at 43,210, after the decisions at 43,201..43,204
    assert reader.tail.error is not None
    late = history_of(reader)
    assert not late.coverage.snapshot(at(43_211)).valid and late.coverage.snapshot(at(43_204)).valid
    assert [r["outcome"] for r in by_controller(rows)["predictive"]][1:] == ["propose"] * 3 and [json.dumps(r, sort_keys=True) for r in rows] == before
    # a restart cannot hide or replay it either: the failure is sticky and a restart after it is refused
    with pytest.raises(Exception) as info:
        rec.RecoverableSourceCountReader(reader.identity, policy_limits={"max_tick_gap_ms": DAY_GAP_MS}, journal_path=reader.journal_path, source_id=a.SOURCE_ID,
                                         origin=gc.ORIGIN_SYNTHETIC).restart()
    assert getattr(info.value, "code", None) in {"restart_after_integrity_failure", "hash_mismatch", "record_not_canonical", "log_replaced"}


def test_38_every_scheduled_opportunity_is_accounted_for_on_a_recovered_route(tmp_path):
    *_r, manifest, rows, out = run(tmp_path, crash_poll=6, ticks=HOURLY - {7200, 10_800})
    groups = by_controller(rows)
    assert {name: len(group) for name, group in groups.items()} == {"static": 4, "reactive": 4, "predictive": 4}
    classes = [abstention.classify_row(r)[0] for r in rows]
    assert len(classes) == 12 and sum(1 for c in classes if c == abstention.VALID_RECOMMENDATION) + sum(1 for c in classes if c != abstention.VALID_RECOMMENDATION) == 12
    assert manifest["status"] == "COMPLETED"


# ----------------------------------------------------------------------------------------------------------------------------- 39-40 no actuation, no inference
@pytest.mark.parametrize("route", ["valid", "tick_gap"])
def test_39_no_actuator_is_invoked_and_real_inference_stays_disabled_after_a_recovery(tmp_path, route):
    kwargs = {"crash_poll": 5} if route == "valid" else {"crash_poll": 5, "ticks": {3600}}
    reader, history, manifest, rows, out = run(tmp_path, name=route, **kwargs)
    assert manifest["actuation"]["configuration_changes_applied"] == 0 and manifest["actuation"]["live_actuator_available"] is False
    assert all(r["actuator"]["submitted_to_fabric"] is False and r["actuator"]["live_actuator_available"] is False for r in rows)
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert manifest["models"]["frozen_hfbtp_model_loaded"] is False and manifest["models"]["frozen_ewma_loaded"] is False
    assert summary["real_inference_performed"] is False and j.s.h.runner.sp.REAL_INFERENCE_AUTHORIZED is False
    assert "fixture" in json.dumps(manifest, sort_keys=True).lower()


def test_40_the_recovery_module_reaches_for_no_process_network_model_or_actuator():
    import ast
    from pathlib import Path
    path = Path(rec.__file__)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = {x.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for x in n.names}
    imported |= {(n.module or "").split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.level == 0}
    assert not imported & {"socket", "subprocess", "urllib", "http", "requests", "docker", "pickle", "joblib", "sklearn", "numpy", "torch", "multiprocessing"}, imported
    names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    assert not names & {"system", "Popen", "run", "connect", "load_model", "actuate", "apply_configuration"}, names & {"system", "Popen", "run", "connect"}
