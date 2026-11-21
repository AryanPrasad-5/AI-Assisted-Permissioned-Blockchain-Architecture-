"""A0f: decision-record schema, provenance, determinism, interruption, output integrity, A0e integration and frozen artifacts
(2026-11-21). Covers checklist items 36-44. Nothing here contacts a network or starts a process.
"""

from __future__ import annotations

import json
import pickle
import shutil
import socket
import subprocess
import urllib.request

import pytest

from tests import a0f_support as s
from tests import a0e_support as e
from fabric_perf.controllers import prelive_integrity as pi
from fabric_perf.controllers import stage3_shadow_runner as runner

import e1_prelive_validation as pv  # noqa: E402  (importable once tests.a0e_support has put scripts/e1 on sys.path)
import e1_stage3_shadow_runner as cli  # noqa: E402

SUMS = s.REPO / "Integration/system_integration/observability_recovery_2026-11-17/SHA256SUMS.txt"
PINNED = {
    "scripts/e1/e1_la2_render.py": "f77a83e7de106b3c1dac28f93333e8dbaf67ea869836944b906f9bc1aabe3d83",
    "scripts/e1/caliper/e1_per_tx_recorder.js": "b5496af3ca038728ff7490b45db726f27ebf0f7efe09b36f6d9330a8cbd1b0a8",
    "scripts/e1/caliper/e1_per_tx_recorder_v2.js": "7ed795b314931c63db1e1ed90e48569901173729ab174097f1a8b370e7ede8f0",
    "scripts/e1/run_contemporaneous_capture.py": "81dc1f4f77f85387bf9578e4293f9793051b93d5724496586878797ef904bd54",
    "src/fabric_perf/controllers/channel_selection.py": "7c59a9bd7ddcc2b536461c307ec1936ee78ad37444a0ad09fdba94417e2df6bc",
    "scripts/e1/e1_block_fetch.py": "08eedd2ea373dfa1aa9ce1567eca2de78fb3ab77462ac333208c02b6008670bc",
}
ROW_KEYS = {"schema", "run_id", "mode", "evidence_class", "channel", "cell", "cell_id", "frame", "measurement", "evidence", "observed",
            "controller", "outcome", "reason", "abstention_scope", "recommended_config", "config_change_proposed",
            "known_actual_config", "known_actual_config_source", "known_actual_config_basis",
            "recommendation_matches_known_actual_config", "proposal_applied", "remote_state_unknown", "actuator", "forecast",
            "predicted_arrival_rate_tps", "predicted_performance", "model", "error"}
FRAME_KEYS = {"index", "id", "observation_timestamp_utc", "status", "reason_code", "abstention_reason", "reason_details",
              "critical_missing", "inconsistencies"}
OBSERVED_KEYS = {"client_offered_rate_tps", "ledger_committed_throughput_tps", "client_latency_mean_ms", "client_latency_p95_ms",
                 "client_backlog", "failed_transactions", "telemetry_age_s"}


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("a process or network call was attempted")
    for obj, names in ((subprocess, ("run", "Popen", "call", "check_output")), (socket, ("create_connection", "getaddrinfo")),
                       (urllib.request, ("urlopen",))):
        for name in names:
            monkeypatch.setattr(obj, name, boom)
    monkeypatch.setattr(socket.socket, "connect", boom)
    monkeypatch.setattr(shutil, "which", lambda *a, **k: (_ for _ in ()).throw(AssertionError("which() called")))


def c(n=5, latency=100):
    return {"count": n, "latency_ms": latency}


def mixed_steps():
    """Initialization, accepted, a rejected frame, a controller-only abstention, a proposal and a reactive proposal."""
    return [s.step(client=c()), s.step(client=c(), ledger_valid_tx=5), s.step(client=c(), ledger_valid_tx=5, config_reader_ok=False),
            s.step(client=c(), ledger_valid_tx=5, forecast="missing")] + [s.step(client=c(latency=9000), ledger_valid_tx=4) for _ in range(3)]


def go(tmp_path, steps=None, **kw):
    return s.run(tmp_path, fixture_doc=s.fixture(steps=steps if steps is not None else mixed_steps()), **kw)


# ---------------------------------------------------------------------------------------------- 36 schema
def test_every_decision_record_has_exactly_the_documented_fields_whatever_the_outcome(tmp_path):
    _, rows, _ = go(tmp_path)
    assert {r["outcome"] for r in rows} == {"abstain", "hold", "propose"} and len(rows) == 7 * 3
    assert {r["frame"]["status"] for r in rows} == {"initialization", "accepted", "rejected"}
    for row in rows:
        assert set(row) == ROW_KEYS, set(row) ^ ROW_KEYS
        assert set(row["frame"]) == FRAME_KEYS and set(row["observed"]) == OBSERVED_KEYS
        assert row["schema"] == runner.DECISION_SCHEMA and row["mode"] == "offline_fixture" and row["evidence_class"] == "SYNTHETIC_FIXTURE"
        assert set(row["actuator"]) == {"kind", "live_actuator_available", "configuration_changes_applied",
                                        "proposal_recorded_by_shadow_actuator", "submitted_to_fabric"}


def test_unavailable_values_are_explicit_nulls_and_a_zero_is_only_ever_a_measurement(tmp_path):
    _, rows, _ = go(tmp_path)
    for row in rows:
        if row["outcome"] == "abstain":
            assert row["recommended_config"] is None and row["config_change_proposed"] is False
        if row["controller"] != "predictive":
            assert row["predicted_performance"] is None and row["predicted_arrival_rate_tps"] is None
        if row["frame"]["status"] == "initialization":
            assert row["observed"]["ledger_committed_throughput_tps"] is None
        if row["controller"] != "predictive":
            assert row["forecast"] is None and row["model"] is None
    assert all(r["proposal_applied"] is False and r["remote_state_unknown"] is False for r in rows)


def test_the_cell_and_channel_identity_are_recorded_on_every_row(tmp_path):
    manifest, rows, _ = go(tmp_path)
    cell = s.load(s.PARAMS_FILE)["cell"]
    assert {r["channel"] for r in rows} == {"e1-offline.a0f"} and all(r["cell"] == cell for r in rows)
    assert len({r["cell_id"] for r in rows}) == 1 and rows[0]["cell_id"] == manifest["cell_id"] == runner.digest(cell)
    assert {r["run_id"] for r in rows} == {s.load(s.PARAMS_FILE)["run_id"]}


def test_a_changed_cell_changes_the_cell_identity(tmp_path):
    doc = s.params()
    doc["cell"]["rate_tps"] = 5.0
    a = go(tmp_path, name="a")[0]
    b = go(tmp_path, name="b", params_doc=doc)[0]
    assert a["cell_id"] != b["cell_id"]


# ---------------------------------------------------------------------------------------------- 37 provenance
def test_every_evidence_reference_is_a_relative_path_to_a_file_the_manifest_hashes(tmp_path):
    manifest, rows, out = go(tmp_path)
    refs = {r["evidence"][k] for r in rows for k in ("client_recorder", "client_submits", "ledger_tx", "ledger_observations")}
    assert refs == {"recorder.csv", "submits.csv", "ledger/ledger_tx.csv", "ledger/ledger_observations.csv"}
    for ref in refs:
        assert (out / "fixture_evidence" / ref).is_file() and "fixture_evidence/" + ref in manifest["files"]
    text = (out / "shadow_decisions.jsonl").read_text(encoding="utf-8")
    assert str(tmp_path) not in text and "\\\\" not in text            # no absolute machine path leaks into the records


def test_the_manifest_binds_the_inputs_the_fixture_evidence_and_the_tool(tmp_path):
    manifest, _, out = go(tmp_path)
    for name, label in (("params", "params.json"), ("controller_policy", "controller_policy.json"), ("fixture", "fixture.json")):
        assert manifest["input_sha256"][name] == s.sha(out / "inputs" / label)
    assert manifest["evidence_class"] == "SYNTHETIC_FIXTURE" and manifest["live_ready"] is False
    assert manifest["models"]["frozen_hfbtp_model_loaded"] is False
    assert manifest["limitations"] and any("clock" in item for item in manifest["limitations"])
    assert manifest["actuation"]["configuration_changes_applied"] == 0


def test_the_cli_run_manifest_records_the_hashes_of_the_tool_files(tmp_path, capsys):
    p, f = s.write(tmp_path / "p.json", s.params()), s.write(tmp_path / "f.json", s.fixture())
    assert cli.main(["fixture", "--params", str(p), "--controller-policy", str(s.POLICY_FILE), "--fixture", str(f), "--out-dir", str(tmp_path / "o")]) == 0
    manifest = json.loads((tmp_path / "o" / "run_manifest.json").read_text(encoding="utf-8"))
    assert set(manifest["tool_sha256"]) == set(cli.TOOL_FILES)
    for name, rel in cli.TOOL_FILES.items():
        assert manifest["tool_sha256"][name] == s.sha(s.REPO / rel)


# ---------------------------------------------------------------------------------------------- 38 deterministic replay
def test_replaying_the_same_inputs_gives_byte_identical_decision_records_summary_and_evidence(tmp_path):
    m1, r1, o1 = go(tmp_path, name="one")
    m2, r2, o2 = go(tmp_path, name="two")
    assert r1 == r2
    for name in ("shadow_decisions.jsonl", "run_summary.json"):
        assert s.sha(o1 / name) == s.sha(o2 / name), name
    assert s.tree(o1 / "fixture_evidence") == s.tree(o2 / "fixture_evidence")
    assert s.tree(o1 / "inputs") == s.tree(o2 / "inputs")
    # the native runner files carry real-time decision overheads and run-directory paths, so they are excluded from the claim
    strip = lambda m: {k: v for k, v in m["files"].items() if not k.startswith("native/")}
    assert strip(m1) == strip(m2)


def test_the_run_reads_no_wall_clock_for_any_recorded_timestamp(tmp_path, monkeypatch):
    import time
    real = time.time
    monkeypatch.setattr(time, "time", lambda: 0.0)                       # a recorded timestamp derived from time.time() would become 1970
    _, rows, _ = go(tmp_path)
    assert all(r["frame"]["observation_timestamp_utc"].startswith("2026-11-21T10:") for r in rows)
    monkeypatch.setattr(time, "time", real)


# ---------------------------------------------------------------------------------------------- 39 interruption
def test_an_interruption_keeps_what_was_streamed_marks_the_run_interrupted_and_stays_verifiable(tmp_path):
    def interrupt(index):
        if index == 2:
            raise KeyboardInterrupt
    manifest, rows, out = go(tmp_path, s.steady(5), hook=interrupt)
    assert manifest["status"] == "INTERRUPTED" and manifest["live_ready"] is False
    frames = s.by_frame(rows)
    assert set(frames) <= {0, 1, 2} and 0 in frames
    assert all(len(group) == 3 for group in frames.values())               # a frame is recorded for all three controllers or not at all
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert summary["status"] == "INTERRUPTED" and summary["frames_processed"] < summary["frames_in_fixture"]
    assert runner.verify_run_directory(out) == []


def test_an_interrupted_run_is_never_reported_as_completed_by_the_cli(tmp_path, monkeypatch):
    real = runner.run_fixture_shadow
    def with_interrupt(inputs, out, **kw):
        def hook(index):
            if index == 1:
                raise KeyboardInterrupt
        return real(inputs, out, _frame_hook=hook, **kw)
    monkeypatch.setattr(runner, "run_fixture_shadow", with_interrupt)
    p, f = s.write(tmp_path / "p.json", s.params()), s.write(tmp_path / "f.json", s.fixture(steps=s.steady(4)))
    assert cli.main(["fixture", "--params", str(p), "--controller-policy", str(s.POLICY_FILE), "--fixture", str(f), "--out-dir", str(tmp_path / "o")]) == 130


def test_a_torn_last_line_is_dropped_and_reported_but_a_corrupt_middle_line_is_an_error(tmp_path):
    good = json.dumps({"a": 1})
    path = tmp_path / "x.jsonl"
    path.write_text(good + "\n" + good + '\n{"a": ', encoding="utf-8")
    assert runner.read_jsonl_tolerant(path) == ([{"a": 1}, {"a": 1}], True)
    path.write_text(good + "\n{bad\n" + good + "\n", encoding="utf-8")
    with pytest.raises(runner.Stage3RunnerError):
        runner.read_jsonl_tolerant(path)
    assert runner.read_jsonl_tolerant(tmp_path / "missing.jsonl") == ([], False)


def test_an_unexpected_failure_writes_a_failed_manifest_then_raises(tmp_path):
    def explode(index):
        if index == 1:
            raise RuntimeError("boom")
    with pytest.raises(runner.Stage3RunnerError, match="boom"):
        go(tmp_path, s.steady(3), hook=explode)
    out = tmp_path / "out_run"
    manifest = json.loads((out / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "FAILED" and "RuntimeError" in manifest["error"] and runner.verify_run_directory(out) == []


# ---------------------------------------------------------------------------------------------- 40 output integrity
def test_a_finished_run_directory_verifies_against_its_manifest(tmp_path):
    manifest, _, out = go(tmp_path)
    assert runner.verify_run_directory(out) == []
    assert "shadow_decisions.jsonl" in manifest["files"] and "run_summary.json" in manifest["files"]
    assert all((out / name).is_file() for name in manifest["files"])


@pytest.mark.parametrize("mutation", ["modify", "delete", "add"])
def test_any_change_to_a_finished_run_directory_is_detected(tmp_path, mutation):
    _, _, out = go(tmp_path)
    if mutation == "modify":
        with (out / "shadow_decisions.jsonl").open("a", encoding="utf-8") as stream:
            stream.write("\n")
    elif mutation == "delete":
        (out / "run_summary.json").unlink()
    else:
        (out / "extra.txt").write_text("x", encoding="utf-8")
    problems = runner.verify_run_directory(out)
    assert len(problems) == 1 and ("modified" in problems[0] or "missing" in problems[0] or "not in the manifest" in problems[0])


def test_a_missing_or_unreadable_manifest_is_a_problem(tmp_path):
    assert runner.verify_run_directory(tmp_path / "nothing")
    (tmp_path / "d").mkdir()
    (tmp_path / "d" / "run_manifest.json").write_text("{", encoding="utf-8")
    assert runner.verify_run_directory(tmp_path / "d")


def test_a_run_never_overwrites_an_existing_directory_or_file(tmp_path):
    _, _, out = go(tmp_path)
    before = s.tree(out)
    inputs = runner.load_inputs(out / "inputs" / "params.json", out / "inputs" / "controller_policy.json", out / "inputs" / "fixture.json")
    with pytest.raises(FileExistsError):
        runner.run_fixture_shadow(inputs, out)
    assert s.tree(out) == before


def test_the_manifest_hashes_match_the_summary_and_the_rows_on_disk(tmp_path):
    manifest, rows, out = go(tmp_path)
    assert manifest["files"]["shadow_decisions.jsonl"] == s.sha(out / "shadow_decisions.jsonl")
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    counts = {"propose": 0, "hold": 0, "abstain": 0}
    for row in rows:
        counts[row["outcome"]] += 1
    assert sum(sum(v.values()) for v in summary["controller_outcome_counts"].values()) == len(rows) == sum(counts.values())


# ---------------------------------------------------------------------------------------------- 41 A0e integration
def a0e_prepared(tmp_path):
    doc = e.params()
    p = e.write_params(tmp_path / "a0e_params.json", doc)
    pv.prepare(p, tmp_path / "prep", "p1")
    return tmp_path / "prep" / "p1", doc


def test_a_fixture_run_can_be_tied_to_an_a0e_prepared_directory_without_modifying_it(tmp_path):
    run_dir, doc = a0e_prepared(tmp_path)
    before = e.tree_hashes(run_dir)
    p, f = s.write(tmp_path / "p.json", doc), s.write(tmp_path / "f.json", s.fixture())
    code = cli.main(["fixture", "--params", str(p), "--controller-policy", str(s.POLICY_FILE), "--fixture", str(f),
                     "--out-dir", str(tmp_path / "o"), "--prelive-run-dir", str(run_dir)])
    assert code == 0
    link = json.loads((tmp_path / "o" / "run_manifest.json").read_text(encoding="utf-8"))["prelive_link"]
    assert link["launch_manifest_sha256"] == s.sha(run_dir / "launch_manifest.json") and link["live_ready"] is False
    assert link["offline_preflight"] in {pv.PASS, pv.INCONCLUSIVE} and "not a live validation" in link["note"]
    assert e.tree_hashes(run_dir) == before                                # no preflight_results/ or other file was written there


def test_parameters_that_differ_from_the_prepared_directory_are_refused(tmp_path):
    run_dir, doc = a0e_prepared(tmp_path)
    doc["cell"]["rate_tps"] = 9.0
    p, f = s.write(tmp_path / "p.json", doc), s.write(tmp_path / "f.json", s.fixture())
    assert cli.main(["fixture", "--params", str(p), "--controller-policy", str(s.POLICY_FILE), "--fixture", str(f),
                     "--out-dir", str(tmp_path / "o"), "--prelive-run-dir", str(run_dir)]) == cli.EXIT_INVALID
    assert not (tmp_path / "o").exists()


def test_a_prepared_directory_that_fails_its_offline_preflight_is_refused(tmp_path):
    run_dir, doc = a0e_prepared(tmp_path)
    (run_dir / "params.validated.json").write_text((run_dir / "params.validated.json").read_text(encoding="utf-8") + " ", encoding="utf-8")
    p, f = s.write(tmp_path / "p.json", doc), s.write(tmp_path / "f.json", s.fixture())
    assert cli.main(["fixture", "--params", str(p), "--controller-policy", str(s.POLICY_FILE), "--fixture", str(f),
                     "--out-dir", str(tmp_path / "o"), "--prelive-run-dir", str(run_dir)]) == cli.EXIT_INVALID


def test_the_a0e_execute_phase_is_still_refused_and_the_runner_adds_no_execute_path(tmp_path, capsys):
    assert pv.main(["execute"]) == pv.EXIT_EXECUTE_REFUSED
    assert cli.main(["live-shadow", "--params", str(s.PARAMS_FILE)]) == cli.EXIT_LIVE_REFUSED == pv.EXIT_EXECUTE_REFUSED


def test_the_runner_uses_the_a0e_contract_and_a0b_validator_objects_not_copies():
    assert runner.pc is __import__("fabric_perf.controllers.prelive_contract", fromlist=["x"])
    assert runner.cs is __import__("fabric_perf.controllers.channel_selection", fromlist=["x"])
    from fabric_perf.controllers.stage3_frame_quality import Stage3FrameQualityContract
    assert runner.Stage3FrameQualityContract is Stage3FrameQualityContract


# ---------------------------------------------------------------------------------------------- 42 protected-channel regression
@pytest.mark.parametrize("channel", ["e1-r002", "e1-cfg001", "E1-CFG001", " e1-r002 ", "e1-b001", "e1-r001"])
def test_protected_channels_stay_rejected_in_every_runner_path(tmp_path, channel, capsys):
    p = s.write(tmp_path / "p.json", s.params(channel=channel))
    assert cli.main(["fixture", "--params", str(p), "--controller-policy", str(s.POLICY_FILE), "--fixture", str(s.NOMINAL_FILE),
                     "--out-dir", str(tmp_path / "o")]) == cli.EXIT_INVALID
    capsys.readouterr()
    assert cli.main(["live-shadow", "--params", str(p)]) == cli.EXIT_LIVE_REFUSED
    gate = next(g for g in json.loads(capsys.readouterr().out)["gates"] if g["gate"] == "G1_channel")
    assert gate["met"] is False and ("protected" in gate["detail"] or "malformed" in gate["detail"] or "channel" in gate["detail"])
    assert not (tmp_path / "o").exists()


# ---------------------------------------------------------------------------------------------- 43 frozen artifacts
def test_a_run_leaves_pinned_files_the_frozen_manifest_and_the_candidate_manifest_unchanged(tmp_path):
    protected = {rel: s.sha(s.REPO / rel) for rel in PINNED}
    assert protected == PINNED
    frozen_before = s.tree(s.REPO / "Integration/system_integration/observability_recovery_2026-11-17")
    candidate_before = s.sha(s.CANDIDATE_MANIFEST)
    go(tmp_path)
    assert {rel: s.sha(s.REPO / rel) for rel in PINNED} == PINNED
    assert s.tree(s.REPO / "Integration/system_integration/observability_recovery_2026-11-17") == frozen_before
    assert s.sha(s.CANDIDATE_MANIFEST) == candidate_before
    frozen = pi.verify_sha256sums(SUMS)
    assert frozen["failed"] == [] and frozen["missing"] == [] and frozen["ok"] == frozen["listed"] == 199


def test_the_candidate_manifest_still_verifies_against_the_current_files_and_is_not_approved():
    manifest = json.loads(s.CANDIDATE_MANIFEST.read_text(encoding="utf-8"))
    assert manifest["approved"] is False and manifest["status"] == "CANDIDATE_NOT_APPROVED"
    assert list(pi.verify_candidate_manifest(manifest, s.REPO)) == []


def test_no_frozen_model_file_is_opened_or_unpickled_during_a_run(tmp_path, monkeypatch):
    import joblib
    def forbidden(*a, **k):
        raise AssertionError("a model artifact was loaded")
    monkeypatch.setattr(joblib, "load", forbidden)
    monkeypatch.setattr(pickle, "load", forbidden)
    monkeypatch.setattr(pickle, "loads", forbidden)
    manifest, _, _ = go(tmp_path)
    assert manifest["status"] == "COMPLETED" and manifest["models"]["frozen_hfbtp_model_loaded"] is False


def test_the_runner_writes_only_inside_its_new_output_directory(tmp_path):
    before = s.tree(s.REPO / "docs/research/e1")
    snapshot = sorted(p.name for p in s.FIXTURE_DIR.iterdir())
    go(tmp_path)
    assert s.tree(s.REPO / "docs/research/e1") == before and sorted(p.name for p in s.FIXTURE_DIR.iterdir()) == snapshot
    assert {p.name for p in (tmp_path / "out_run").iterdir()} == {"inputs", "fixture_evidence", "native", "shadow_decisions.jsonl",
                                                                  "run_summary.json", "run_manifest.json"}
