"""Task A0e (2026-11-21), Workstreams A and E: the prepare/preflight launch path and its offline safety.

Everything runs in tmp directories. Process, network and container APIs are patched to fail wherever the code is exercised, so a
dangerous call would fail the test instead of reaching anything. The synthetic channel ``e1-offline.a0e`` is not approved or created.
"""

import ast
import copy
import hashlib
import json
import os
import shutil
import socket
import subprocess
import urllib.request
from pathlib import Path

import pytest

from fabric_perf.controllers import channel_selection as cs
from fabric_perf.controllers import prelive_contract as pc
from tests.a0e_support import CHANNEL, CLOCK_PARAMS, DROP, GOOD_PARAMS, MS, REPO, RUN_ID, clock_sample, params, sha, tree_hashes, write_params

import e1_prelive_validation as pv  # noqa: E402  (importable once tests.a0e_support has put scripts/e1 on sys.path)
import e1_recorder_v2_wire as wire  # noqa: E402

SUMS = REPO / "Integration/system_integration/observability_recovery_2026-11-17/SHA256SUMS.txt"
PINNED = {  # files that must stay byte-identical (hashes from the A0d report / A0d pin test)
    "scripts/e1/e1_la2_render.py": "f77a83e7de106b3c1dac28f93333e8dbaf67ea869836944b906f9bc1aabe3d83",
    "scripts/e1/caliper/e1_per_tx_recorder.js": "b5496af3ca038728ff7490b45db726f27ebf0f7efe09b36f6d9330a8cbd1b0a8",
    "scripts/e1/caliper/e1_per_tx_recorder_v2.js": "7ed795b314931c63db1e1ed90e48569901173729ab174097f1a8b370e7ede8f0",
    "scripts/e1/run_contemporaneous_capture.py": "81dc1f4f77f85387bf9578e4293f9793051b93d5724496586878797ef904bd54",
    "src/fabric_perf/controllers/channel_selection.py": "7c59a9bd7ddcc2b536461c307ec1936ee78ad37444a0ad09fdba94417e2df6bc",
    "scripts/e1/e1_block_fetch.py": "08eedd2ea373dfa1aa9ce1567eca2de78fb3ab77462ac333208c02b6008670bc",
}


@pytest.fixture(autouse=True)
def no_live_apis(monkeypatch):
    """Any process, socket, HTTP or container call made while a test runs is a test failure."""
    def boom(*args, **kwargs):
        raise AssertionError("a process or network API was called during an offline phase")
    for owner, names in ((subprocess, ("run", "Popen", "call", "check_call", "check_output", "getoutput", "getstatusoutput")),
                         (os, ("system", "popen", "spawnl", "spawnv", "startfile", "execv", "execvp")),
                         (socket, ("create_connection", "getaddrinfo", "gethostbyname")), (urllib.request, ("urlopen", "build_opener"))):
        for name in names:
            if hasattr(owner, name):
                monkeypatch.setattr(owner, name, boom)
    monkeypatch.setattr(socket.socket, "connect", boom)
    monkeypatch.setattr(shutil, "which", lambda *a, **k: (_ for _ in ()).throw(AssertionError("which() called")))


def prepared(tmp_path, doc=None, prepare_id="p1", **kwargs):
    p = write_params(tmp_path / "params.json", doc if doc is not None else params())
    manifest = pv.prepare(p, tmp_path / "out", prepare_id, **kwargs)
    return tmp_path / "out" / prepare_id, manifest


def check(result, check_id):
    return next(c for c in result["checks"] if c["id"] == check_id)


def failing(result):
    return sorted(c["id"] for c in result["checks"] if c["status"] == pv.FAIL)


# ================================================================ Workstream A: explicit phases and prepare-only behaviour
def test_the_phase_must_be_chosen_explicitly_and_only_four_exist(capsys):
    with pytest.raises(SystemExit) as caught:
        pv.main([])
    assert caught.value.code == 2
    with pytest.raises(SystemExit):
        pv.main(["run-everything"])
    parser_phases = {"prepare", "preflight", "execute", "candidate-manifest"}
    tree = ast.parse((REPO / "scripts/e1/e1_prelive_validation.py").read_text(encoding="utf-8"))
    added = {c.args[0].value for c in ast.walk(tree) if isinstance(c, ast.Call) and getattr(c.func, "attr", "") == "add_parser"}
    assert added == parser_phases


def test_prepare_writes_one_new_isolated_directory_and_nothing_else(tmp_path):
    before = tree_hashes(REPO / "scripts/e1")
    run_dir, manifest = prepared(tmp_path)
    assert sorted(p.name for p in (tmp_path / "out").iterdir()) == ["p1"]
    assert {p.name for p in run_dir.iterdir()} == {"launch_manifest.json", "params.validated.json", "rendered", "wired"}
    assert tree_hashes(REPO / "scripts/e1") == before                            # no source file touched
    assert manifest["status"] == pv.STATUS_PREPARED and manifest["caliper_invoked"] is False and manifest["fabric_contacted"] is False
    stored = json.loads((run_dir / "launch_manifest.json").read_text(encoding="utf-8"))
    assert stored["files"] == manifest["files"] and "launch_manifest.json" not in stored["files"]


def test_the_generated_caliper_configuration_follows_the_parameters(tmp_path):
    run_dir, manifest = prepared(tmp_path, params(cell={**GOOD_PARAMS["cell"], "warmup_s": 3, "duration_s": 7}, ledger_collector={"poll_interval_s": 1.0, "max_polls": 40}))
    bench = json.loads((run_dir / "wired" / RUN_ID / "benchmark.yaml").read_text(encoding="utf-8"))
    rounds = bench["test"]["rounds"]
    assert [r["label"] for r in rounds] == [f"wu-{RUN_ID}", f"m-{RUN_ID}"] and rounds[0]["txDuration"] == 3 and rounds[1]["txDuration"] == 7
    assert all(r["rateControl"] == {"type": "fixed-rate", "opts": {"tps": 4.0}} for r in rounds)
    assert rounds[1]["workload"]["arguments"]["settleSeconds"] == pc.RENDERER_SETTLE_SECONDS_AFTER_WARMUP
    net = json.loads((run_dir / "wired" / RUN_ID / "networkconfig.yaml").read_text(encoding="utf-8"))
    assert [c["channelName"] for c in net["channels"]] == [CHANNEL]
    plan = json.loads((run_dir / "rendered" / "plan.json").read_text(encoding="utf-8"))
    assert plan["channel"] == CHANNEL and plan["cell"]["B"] == 40 and plan["cell"]["tau"] == "2s" and plan["caliper"]["measured_round"] == 1
    live = manifest["expected_live_files"]
    assert live["live_csv"].endswith(f"/{RUN_ID}_w0_r1_m-{RUN_ID}.live.csv") and live["live_final_json"].endswith(".live.final.json")


def test_a_transaction_count_run_is_rendered_as_a_count(tmp_path):
    cell = {k: v for k, v in GOOD_PARAMS["cell"].items() if k != "duration_s"} | {"tx_count": 20}
    run_dir, _ = prepared(tmp_path, params(cell=cell))
    bench = json.loads((run_dir / "wired" / RUN_ID / "benchmark.yaml").read_text(encoding="utf-8"))
    assert bench["test"]["rounds"][-1]["txNumber"] == 20 and "txDuration" not in bench["test"]["rounds"][-1]
    assert pv.preflight(run_dir, write=False)["offline_preflight"] == pv.PASS


def test_recorder_v2_is_selected_in_the_wired_copy_and_v1_remains_in_the_source_copy(tmp_path):
    run_dir, manifest = prepared(tmp_path)
    wired = json.loads((run_dir / "wired" / RUN_ID / "benchmark.yaml").read_text(encoding="utf-8"))
    source = json.loads((run_dir / "rendered/runs" / RUN_ID / "benchmark.yaml").read_text(encoding="utf-8"))
    new, old = wired["monitors"]["transaction"][0], source["monitors"]["transaction"][0]
    assert new["module"].endswith("/e1_per_tx_recorder_v2.js") and old["module"].endswith("/e1_per_tx_recorder.js")
    assert new["options"]["liveFlushRows"] == 20 and new["options"]["liveFlushIntervalMs"] == 250 and "liveFlushRows" not in old["options"]
    modules = run_dir / "wired" / RUN_ID / "modules"
    assert sha(modules / "e1_per_tx_recorder.js") == wire.PINNED_V1_SHA256 == PINNED["scripts/e1/caliper/e1_per_tx_recorder.js"]
    assert sha(modules / "e1_per_tx_recorder_v2.js") == PINNED["scripts/e1/caliper/e1_per_tx_recorder_v2.js"]
    assert manifest["recorder_wire_record"] == "RENDERED_OFFLINE_NOT_EXECUTED"


def test_the_live_options_are_omitted_when_the_operator_gives_none(tmp_path):
    run_dir, _ = prepared(tmp_path, params(recorder=None))
    options = json.loads((run_dir / "wired" / RUN_ID / "benchmark.yaml").read_text(encoding="utf-8"))["monitors"]["transaction"][0]["options"]
    assert "liveFlushRows" not in options and "liveFlushIntervalMs" not in options          # v2's own defaults apply; none was invented
    assert pv.preflight(run_dir, write=False)["offline_preflight"] == pv.PASS


def test_every_preparation_needs_a_new_directory_and_an_id_that_is_a_name(tmp_path):
    prepared(tmp_path)
    with pytest.raises(pv.PrepareError, match="refusing to reuse"):
        pv.prepare(tmp_path / "params.json", tmp_path / "out", "p1")
    for bad in ("", "a b", "../x", "x" * 61, "a/b"):
        with pytest.raises(pv.PrepareError):
            pv.prepare(tmp_path / "params.json", tmp_path / "out", bad)


def test_invalid_parameters_create_nothing_at_all(tmp_path):
    doc = params()
    doc["telemetry_adapter"]["window_s"] = 1.0                                         # equals the poll interval
    p = write_params(tmp_path / "bad.json", doc)
    with pytest.raises(pc.PreliveParameterError):
        pv.prepare(p, tmp_path / "out", "x1")
    assert not (tmp_path / "out").exists()
    assert pv.main(["prepare", "--params", str(p), "--out-root", str(tmp_path / "out"), "--prepare-id", "x1"]) == 2
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("channel", ["e1-r002", "e1-cfg001", "E1-R002", "e1-b001", "e1-ql01-b40-l1", "", None, "mychannel"])
def test_a_protected_reserved_or_malformed_channel_stops_preparation_before_anything_is_written(tmp_path, channel, capsys):
    p = write_params(tmp_path / "p.json", params(channel=channel))
    assert pv.main(["prepare", "--params", str(p), "--out-root", str(tmp_path / "out"), "--prepare-id", "x"]) == 2
    out = json.loads(capsys.readouterr().out)
    assert out["valid"] is False and any(i["field"] == "channel" for i in out["issues"])
    assert not (tmp_path / "out").exists()


def test_a_missing_or_unreadable_input_file_is_reported_without_creating_a_directory(tmp_path):
    doc = params(environment={"renderer_env_path": str(tmp_path / "absent.json"), "network_json_path": GOOD_PARAMS["environment"]["network_json_path"]})
    with pytest.raises(pv.PrepareError, match="renderer_env_path"):
        pv.prepare(write_params(tmp_path / "p.json", doc), tmp_path / "out", "m1")
    assert not (tmp_path / "out" / "m1").exists()
    with pytest.raises(pc.PreliveParameterError):
        pv.prepare(tmp_path / "nothing.json", tmp_path / "out", "m2")
    (tmp_path / "notjson.json").write_text("{", encoding="utf-8")
    with pytest.raises(pc.PreliveParameterError):
        pv.prepare(tmp_path / "notjson.json", tmp_path / "out", "m3")


def test_an_incompatible_renderer_environment_is_refused(tmp_path):
    env = json.loads(Path(GOOD_PARAMS["environment"]["renderer_env_path"]).read_text(encoding="utf-8"))
    env["surprise"] = 1
    (tmp_path / "env.json").write_text(json.dumps(env), encoding="utf-8")
    doc = params(environment={**GOOD_PARAMS["environment"], "renderer_env_path": str(tmp_path / "env.json")})
    with pytest.raises(pv.PrepareError, match="environment file rejected"):
        pv.prepare(write_params(tmp_path / "p.json", doc), tmp_path / "out", "e1x")
    env.pop("surprise")
    env["test_network"] = "C:\\wsl\\path"
    (tmp_path / "env.json").write_text(json.dumps(env), encoding="utf-8")
    with pytest.raises(pv.PrepareError):
        pv.prepare(write_params(tmp_path / "p.json", doc), tmp_path / "out", "e2x")


def test_a_missing_dependency_leaves_a_marker_and_preflight_refuses_the_directory(tmp_path):
    fake_repo = tmp_path / "repo"
    for rel in ("scripts/e1/caliper/e1_per_tx_recorder.js", "scripts/e1/caliper/e1_per_tx_recorder_v2.js",
                "fabric-testbed/caliper/workloads/e1_workload_core.js"):                  # e1_create_asset.js is deliberately absent
        (fake_repo / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO / rel, fake_repo / rel)
    p = write_params(tmp_path / "p.json", params())
    with pytest.raises(OSError):
        pv.prepare(p, tmp_path / "out", "inc", repo_root=fake_repo)
    run_dir = tmp_path / "out" / "inc"
    marker = json.loads((run_dir / pv.INCOMPLETE_MARKER).read_text(encoding="utf-8"))
    assert marker["status"] == "PREPARE_INCOMPLETE" and not (run_dir / "launch_manifest.json").exists()
    result = pv.preflight(run_dir, write=False)
    assert result["offline_preflight"] == pv.FAIL and check(result, "P01_launch_manifest")["status"] == pv.FAIL


# ================================================================ Workstream A: offline preflight
def test_a_clean_preparation_passes_the_offline_preflight_and_is_never_live_ready(tmp_path):
    run_dir, _ = prepared(tmp_path)
    result = pv.preflight(run_dir, frozen_sums=SUMS)
    assert result["offline_preflight"] == pv.PASS and result["live_ready"] is False
    assert [c["status"] for c in result["checks"] if c["id"] in ("P01_launch_manifest", "P02_parameters_and_channel", "P03_file_inventory",
            "P04_recorder_v2_selected", "P05_no_conflicting_values", "P06_declared_outputs", "P07_dependencies", "P08_nothing_executed")] == [pv.PASS] * 8
    assert check(result, "P10_frozen_manifest")["detail"]["ok"] == 199
    gates = {g["gate"]: g["state"] for g in result["live_gates"]}
    assert gates["owner_approval_of_validation_channel"] == "NOT_GRANTED" and gates["authorization_to_execute_any_live_step"] == "NOT_GRANTED"
    assert gates["stage3_operational_live_entry_point"].startswith("BLOCKED") and gates["windows_wsl_clock_agreement"].startswith("UNVERIFIED")
    assert gates["validation_channel_exists_with_expected_configuration"] == "UNVERIFIED"
    assert "not an authorization" in result["statement"]


def test_the_preflight_writes_a_machine_readable_and_a_human_readable_result(tmp_path):
    run_dir, _ = prepared(tmp_path)
    result = pv.preflight(run_dir)
    names = result["written_to"]
    assert len(names) == 2 and names[0].endswith(".json") and names[1].endswith(".md")
    stored = json.loads((run_dir / names[0]).read_text(encoding="utf-8"))
    assert stored["live_ready"] is False and stored["offline_preflight"] == pv.PASS
    text = (run_dir / names[1]).read_text(encoding="utf-8")
    assert "Live ready: false" in text and "Gates that remain open" in text and "P04_recorder_v2_selected" in text
    again = pv.preflight(run_dir)                                                      # a second run does not disturb the first or the inventory
    assert again["offline_preflight"] == pv.PASS and again["written_to"] != names


def test_preflight_cli_exit_codes_are_pass_zero_fail_one(tmp_path, capsys):
    run_dir, _ = prepared(tmp_path)
    assert pv.main(["preflight", "--run-dir", str(run_dir), "--no-write"]) == 0
    (run_dir / "evidence").mkdir()                                                    # an output that already exists
    assert pv.main(["preflight", "--run-dir", str(run_dir), "--no-write"]) == 1
    assert "P06_declared_outputs" in capsys.readouterr().out


def test_selecting_v1_again_in_the_generated_benchmark_is_detected(tmp_path):
    run_dir, _ = prepared(tmp_path)
    path = run_dir / "wired" / RUN_ID / "benchmark.yaml"
    doc = json.loads(path.read_text(encoding="utf-8"))
    monitor = doc["monitors"]["transaction"][0]
    monitor["module"] = monitor["module"].replace("_v2.js", ".js")
    path.write_text(json.dumps(doc), encoding="utf-8")
    result = pv.preflight(run_dir, write=False)
    assert {"P03_file_inventory", "P04_recorder_v2_selected"} <= set(failing(result)) and result["offline_preflight"] == pv.FAIL
    assert "does not select recorder v2" in check(result, "P04_recorder_v2_selected")["detail"]


def test_a_missing_v2_or_a_modified_v1_module_is_detected(tmp_path):
    run_dir, _ = prepared(tmp_path)
    modules = run_dir / "wired" / RUN_ID / "modules"
    (modules / "e1_per_tx_recorder_v2.js").unlink()
    assert "P04_recorder_v2_selected" in failing(pv.preflight(run_dir, write=False))
    shutil.copyfile(REPO / "scripts/e1/caliper/e1_per_tx_recorder_v2.js", modules / "e1_per_tx_recorder_v2.js")
    (modules / "e1_per_tx_recorder.js").write_text("// tampered\n", encoding="utf-8")
    result = pv.preflight(run_dir, write=False)
    assert "not the pinned file" in check(result, "P04_recorder_v2_selected")["detail"] and "P03_file_inventory" in failing(result)


def test_a_protected_channel_appearing_in_a_generated_file_is_detected(tmp_path):
    run_dir, _ = prepared(tmp_path)
    path = run_dir / "wired" / RUN_ID / "ccp-org1.json"
    path.write_text(path.read_text(encoding="utf-8").replace("e1-org1", "e1-org1-e1-r002"), encoding="utf-8")
    result = pv.preflight(run_dir, write=False)
    assert "P04_recorder_v2_selected" in failing(result) and "protected reference channel" in check(result, "P04_recorder_v2_selected")["detail"]


def test_a_channel_swap_in_the_network_configuration_is_detected_as_a_mismatch_not_a_fallback(tmp_path):
    run_dir, _ = prepared(tmp_path)
    path = run_dir / "wired" / RUN_ID / "networkconfig.yaml"
    doc = json.loads(path.read_text(encoding="utf-8"))
    doc["channels"][0]["channelName"] = "e1-other.channel"
    path.write_text(json.dumps(doc), encoding="utf-8")
    assert "P04_recorder_v2_selected" in failing(pv.preflight(run_dir, write=False))


def test_conflicting_values_between_parameters_and_generated_files_are_detected(tmp_path):
    run_dir, _ = prepared(tmp_path)
    path = run_dir / "wired" / RUN_ID / "benchmark.yaml"
    original = path.read_text(encoding="utf-8")
    for mutate, needle in (
            (lambda d: d["test"]["rounds"][-1]["rateControl"]["opts"].update(tps=9.0), "rate"),
            (lambda d: d["test"]["rounds"][-1].update(txDuration=99), "txDuration"),
            (lambda d: d["monitors"]["transaction"][0]["options"].update(liveFlushRows=21), "liveFlushRows"),
            (lambda d: d["monitors"]["transaction"][0]["options"].update(runId="other"), "runId"),
            (lambda d: d["test"]["rounds"][-1]["workload"]["arguments"].update(mode="rmw"), "workload mode")):
        doc = json.loads(original)
        mutate(doc)
        path.write_text(json.dumps(doc), encoding="utf-8")
        result = pv.preflight(run_dir, write=False)
        assert any(needle in issue for issue in check(result, "P05_no_conflicting_values")["detail"]), needle
    path.write_text(original, encoding="utf-8")
    assert pv.preflight(run_dir, write=False)["offline_preflight"] == pv.PASS


def test_the_parameter_copy_is_revalidated_so_an_edit_that_breaks_the_window_rule_is_caught(tmp_path):
    run_dir, _ = prepared(tmp_path)
    path = run_dir / "params.validated.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    doc["values"]["telemetry_adapter"]["window_s"] = 1.0
    path.write_text(json.dumps(doc), encoding="utf-8")
    result = pv.preflight(run_dir, write=False)
    assert "window_not_longer_than_poll" in check(result, "P02_parameters_and_channel")["detail"]
    assert {"P02_parameters_and_channel", "P03_file_inventory"} <= set(failing(result))


def test_a_protected_channel_in_the_stored_parameters_is_rejected_not_defaulted(tmp_path):
    run_dir, _ = prepared(tmp_path)
    path = run_dir / "params.validated.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    doc["values"]["channel"] = "e1-cfg001"
    path.write_text(json.dumps(doc), encoding="utf-8")
    assert "protected reference channel" in check(pv.preflight(run_dir, write=False), "P02_parameters_and_channel")["detail"]


def test_declared_outputs_must_be_new_inside_the_run_and_away_from_protected_evidence(tmp_path):
    run_dir, _ = prepared(tmp_path)
    path = run_dir / "launch_manifest.json"
    original = json.loads(path.read_text(encoding="utf-8"))
    cases = {
        "ledger_journal_dir": "../elsewhere", "shadow_capture": "evidence/ledger_journal",                  # escape; duplicate path
        "evidence_dir": "C:/somewhere/else" if os.name == "nt" else "/somewhere/else"}
    for key, value in cases.items():
        doc = copy.deepcopy(original)
        doc["declared_outputs"][key] = value
        path.write_text(json.dumps(doc), encoding="utf-8")
        assert "P06_declared_outputs" in failing(pv.preflight(run_dir, write=False)), key
    doc = copy.deepcopy(original)
    doc["declared_outputs"]["recorder_dir_in_workspace"] = "/ws/runs/e1-cfg001/recorder"
    path.write_text(json.dumps(doc), encoding="utf-8")
    assert "P06_declared_outputs" in failing(pv.preflight(run_dir, write=False))


def test_an_output_that_would_land_in_protected_evidence_is_refused(tmp_path):
    """A run directory under a protected prefix of the repository (here a stand-in repository) is refused."""
    fake_repo = tmp_path / "repo"
    protected_root = fake_repo / pv.PROTECTED_EVIDENCE_PREFIXES[1]                      # experiments/results
    protected_root.mkdir(parents=True)
    write_params(tmp_path / "params.json", params())
    pv.prepare(tmp_path / "params.json", protected_root, "inside")
    result = pv.preflight(protected_root / "inside", repo_root=fake_repo, write=False)
    detail = check(result, "P06_declared_outputs")
    assert detail["status"] == pv.FAIL and any("points into protected evidence" in line for line in detail["detail"])
    outside = pv.preflight(tmp_path / "out" / "none", repo_root=fake_repo, write=False)         # a directory that is not a preparation
    assert outside["offline_preflight"] == pv.FAIL


def test_a_dependency_that_changed_after_preparation_is_detected(tmp_path):
    run_dir, _ = prepared(tmp_path)
    path = run_dir / "launch_manifest.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    doc["tool_hashes"]["ledger_follower"]["sha256"] = "0" * 64
    del doc["tool_hashes"]["telemetry_adapter"]
    path.write_text(json.dumps(doc), encoding="utf-8")
    detail = check(pv.preflight(run_dir, write=False), "P07_dependencies")["detail"]
    assert any("ledger_follower" in line and "changed" in line for line in detail) and any("telemetry_adapter" in line for line in detail)


def test_the_launch_manifest_lists_every_telemetry_and_ledger_dependency(tmp_path):
    _, manifest = prepared(tmp_path)
    assert set(manifest["tool_hashes"]) == set(pv.TOOL_FILES)
    for role in ("ledger_follower", "ledger_collector", "telemetry_adapter", "shadow_capture", "recorder_v1", "recorder_v2", "stage3_contract"):
        entry = manifest["tool_hashes"][role]
        assert sha(REPO / entry["path"]) == entry["sha256"]
    steps = {s["step"]: s for s in manifest["future_execution"]["steps"]}
    capture = steps["R10_shadow_capture"]["argv"]
    assert capture[capture.index("--adapter-window-s") + 1] == "4.0" and capture[capture.index("--recorder-latency") + 1] == "derive_from_timestamps"
    assert capture[capture.index("--channel") + 1] == CHANNEL and "--ledger-observations-csv" in capture
    follow = steps["R07_ledger_follow"]["argv"]
    assert follow[follow.index("--poll-interval-s") + 1] == "1.0" and follow[follow.index("--max-polls") + 1] == "20" and follow[-1] == "--execute"


def test_the_documented_execution_steps_are_text_only_and_nothing_is_marked_authorized(tmp_path):
    run_dir, manifest = prepared(tmp_path)
    steps = manifest["future_execution"]["steps"]
    assert manifest["future_execution"]["status"] == "NOT_AUTHORIZED" and manifest["future_execution"]["invoked"] is False
    assert all(s["invoked"] is False and s["status"] in ("NOT_AUTHORIZED_NOT_RUN", "BLOCKED_NO_OPERATIONAL_LIVE_ENTRY_POINT") for s in steps)
    assert {s["kind"] for s in steps} == {"read_only_observation", "transaction_producing", "preparation"}
    assert [s["step"] for s in steps if s["kind"] == "transaction_producing"] == ["R09_caliper_smoke"]
    assert len({s["authorization"] for s in steps}) == len(steps)                      # one authorization per step; not chained
    stage3 = next(s for s in steps if s["step"] == "R11_stage3_shadow")
    assert stage3["status"].startswith("BLOCKED") and "make_shadow_foundation" in stage3["note"]
    assert "--max-consecutive-abstentions" in stage3["argv"] and "--max-initialization-frames" in stage3["argv"]
    disabled = params(stage3={"max_consecutive_abstentions": 3, "max_initialization_frames": None})
    (tmp_path / "second").mkdir()
    _, manifest2 = prepared(tmp_path / "second", disabled)
    assert "--max-initialization-frames" not in next(s for s in manifest2["future_execution"]["steps"] if s["step"] == "R11_stage3_shadow")["argv"]


def test_preflight_tampering_with_the_manifest_status_is_refused(tmp_path):
    run_dir, _ = prepared(tmp_path)
    path = run_dir / "launch_manifest.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    for mutate in (lambda d: d.update(status="RUNNING"), lambda d: d.update(schema="other"), lambda d: d.update(invoked=True)):
        changed = copy.deepcopy(doc)
        mutate(changed)
        path.write_text(json.dumps(changed), encoding="utf-8")
        assert check(pv.preflight(run_dir, write=False), "P01_launch_manifest")["status"] == pv.FAIL
    changed = copy.deepcopy(doc)
    changed["future_execution"]["steps"][0]["status"] = "AUTHORIZED"
    path.write_text(json.dumps(changed), encoding="utf-8")
    assert "P08_nothing_executed" in failing(pv.preflight(run_dir, write=False))
    changed = copy.deepcopy(doc)
    changed["caliper_invoked"] = True
    path.write_text(json.dumps(changed), encoding="utf-8")
    assert "P08_nothing_executed" in failing(pv.preflight(run_dir, write=False))


def test_an_extra_file_in_the_prepared_run_is_detected(tmp_path):
    run_dir, _ = prepared(tmp_path)
    (run_dir / "wired" / RUN_ID / "surprise.js").write_text("x", encoding="utf-8")
    assert "not in the manifest" in " ".join(check(pv.preflight(run_dir, write=False), "P03_file_inventory")["detail"])


def test_a_directory_that_is_not_a_preparation_fails_closed(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    result = pv.preflight(empty, write=True)
    assert result["offline_preflight"] == pv.FAIL and result["live_ready"] is False and "written_to" in result
    absent = pv.preflight(tmp_path / "absent", write=True)
    assert absent["offline_preflight"] == pv.FAIL and "written_to" not in absent


# ---------------------------------------------------------------- clock evidence through preflight
def clock_file(tmp_path, samples):
    path = tmp_path / "clock.json"
    path.write_text(json.dumps(samples), encoding="utf-8")
    return path


def good_samples(n=3, **kw):
    return [clock_sample(base_ns=1_800_000_000_000_000_000 + i * 10**10, mono_base_ns=i * 10**10, **kw) for i in range(n)]


def test_clock_evidence_is_optional_and_never_assumed(tmp_path):
    run_dir, _ = prepared(tmp_path)
    result = pv.preflight(run_dir, write=False)
    assert check(result, "P11_clock_agreement")["status"] == pv.NOT_CHECKED
    assert {g["gate"]: g["state"] for g in result["live_gates"]}["windows_wsl_clock_agreement"].startswith("UNVERIFIED")


@pytest.mark.parametrize("offset_ns,rtt_ns,status,exit_code,gate", [(0, 2 * MS, pv.PASS, 0, "AGREE"), (5_000 * MS, 2 * MS, pv.FAIL, 1, "DISAGREE"),
                                                                    (0, 100 * MS, pv.INCONCLUSIVE, 3, "INCONCLUSIVE")])
def test_clock_samples_decide_the_clock_check_and_the_exit_code(tmp_path, offset_ns, rtt_ns, status, exit_code, gate):
    run_dir, _ = prepared(tmp_path)
    samples = clock_file(tmp_path, {"pre_run": good_samples(offset_ns=offset_ns, rtt_ns=rtt_ns)})
    result = pv.preflight(run_dir, clock_samples=samples, clock_parameters=CLOCK_PARAMS, write=False)
    assert check(result, "P11_clock_agreement")["status"] == status
    assert {g["gate"]: g["state"] for g in result["live_gates"]}["windows_wsl_clock_agreement"] == gate and result["live_ready"] is False
    argv = ["preflight", "--run-dir", str(run_dir), "--no-write", "--clock-samples", str(samples), "--clock-tolerance-ms", "10",
            "--clock-min-valid-samples", "3", "--clock-max-wall-monotonic-disagreement-ms", "1"]
    assert pv.main(argv) == exit_code


def test_the_cli_refuses_clock_samples_without_the_three_operator_parameters(tmp_path, capsys):
    run_dir, _ = prepared(tmp_path)
    samples = clock_file(tmp_path, good_samples())
    assert pv.main(["preflight", "--run-dir", str(run_dir), "--clock-samples", str(samples), "--clock-tolerance-ms", "10"]) == 2
    assert "no defaults" in capsys.readouterr().out


# ================================================================ candidate manifest and frozen evidence are only read
def test_preflight_verifies_the_frozen_manifest_without_writing_it(tmp_path):
    before = sha(SUMS)
    run_dir, _ = prepared(tmp_path)
    result = pv.preflight(run_dir, frozen_sums=SUMS)
    assert check(result, "P10_frozen_manifest")["detail"] == {"failed": [], "listed": 199, "missing": [], "ok": 199}
    assert sha(SUMS) == before


def test_a_failing_frozen_manifest_fails_the_preflight(tmp_path):
    run_dir, _ = prepared(tmp_path)
    base = tmp_path / "frozen"
    base.mkdir()
    (base / "a.txt").write_text("a", encoding="utf-8")
    sums = base / "SHA256SUMS.txt"
    sums.write_text(f"{hashlib.sha256(b'different').hexdigest()} *./a.txt\n{'0' * 64} *./gone.txt\n", encoding="utf-8")
    detail = check(pv.preflight(run_dir, frozen_sums=sums, write=False), "P10_frozen_manifest")
    assert detail["status"] == pv.FAIL and detail["detail"]["failed"] == ["./a.txt"] and detail["detail"]["missing"] == ["./gone.txt"]


# ================================================================ execution is not available, and cannot be switched on
@pytest.mark.parametrize("env", [{}, {"E1_AUTHORIZE": "1", "FABRIC_LIVE": "1", "ALLOW_LIVE": "true", "CI": "1", "I_AM_THE_OWNER": "yes"}])
def test_the_execute_phase_is_refused_with_and_without_authorization_looking_environment_variables(monkeypatch, env, capsys):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    assert pv.main(["execute"]) == pv.EXIT_EXECUTE_REFUSED == 5
    assert "REFUSED" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        pv.main(["execute", "--authorized"])                                          # no such flag
    with pytest.raises(SystemExit):
        pv.main(["execute", "--run-dir", "x", "--yes"])


def test_the_new_modules_read_no_environment_and_import_no_process_or_network_api():
    forbidden_imports = {"subprocess", "socket", "urllib", "http", "requests", "docker", "asyncio", "multiprocessing", "ssl", "ftplib",
                         "smtplib", "ctypes", "os"}
    forbidden_attributes = {"environ", "getenv", "system", "Popen", "popen", "urlopen", "create_connection", "startfile", "check_output"}
    for rel in ("scripts/e1/e1_prelive_validation.py", "src/fabric_perf/controllers/prelive_contract.py",
                "src/fabric_perf/controllers/clock_preflight.py", "src/fabric_perf/controllers/prelive_integrity.py"):
        tree = ast.parse((REPO / rel).read_text(encoding="utf-8"))
        names = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} |                 {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
        assert not names & forbidden_imports, (rel, names & forbidden_imports)
        used = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        assert not used & forbidden_attributes, (rel, used & forbidden_attributes)


def test_the_orchestrator_has_no_function_that_could_run_a_documented_step():
    tree = ast.parse((REPO / "scripts/e1/e1_prelive_validation.py").read_text(encoding="utf-8"))
    names = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert not {n for n in names if any(w in n for w in ("run_step", "launch", "invoke", "start_", "exec"))} - {"refuse_execute", "future_execution_steps"}
    calls = {c.func.attr if isinstance(c.func, ast.Attribute) else getattr(c.func, "id", "") for c in ast.walk(tree) if isinstance(c, ast.Call)}
    assert not calls & {"run", "Popen", "call", "check_output", "system", "popen", "urlopen", "connect", "create_connection"}


def test_the_whole_workflow_runs_with_every_process_and_network_api_patched_to_fail(tmp_path):
    """prepare, preflight (with clock and frozen checks), candidate manifest and execute, all under the autouse patches."""
    run_dir, _ = prepared(tmp_path)
    samples = clock_file(tmp_path, {"pre_run": good_samples()})
    assert pv.preflight(run_dir, frozen_sums=SUMS, clock_samples=samples, clock_parameters=CLOCK_PARAMS)["offline_preflight"] == pv.PASS
    assert pv.main(["candidate-manifest", "--out", str(tmp_path / "cand.json")]) == 0
    assert pv.main(["execute"]) == 5
    assert pv.main(["preflight", "--run-dir", str(run_dir), "--no-write", "--candidate-manifest", str(tmp_path / "cand.json")]) == 0


# ================================================================ legacy runner isolation and no mutation of protected files
def test_the_legacy_capture_runner_is_not_used_wired_or_approved_by_the_new_workflow():
    orchestrator = (REPO / "scripts/e1/e1_prelive_validation.py").read_text(encoding="utf-8")
    tree = ast.parse(orchestrator)
    assert "run_contemporaneous_capture" not in {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    assert not [n for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and "run_contemporaneous_capture" in (n.module or "")]
    mentions = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)
                and "run_contemporaneous_capture" in n.value and n.value != ast.get_docstring(tree, clean=False)]
    assert mentions == ["scripts/e1/run_contemporaneous_capture.py"]                     # named once, as an excluded candidate file
    legacy = (REPO / "scripts/e1/run_contemporaneous_capture.py").read_text(encoding="utf-8")
    assert "e1_per_tx_recorder_v2" not in legacy and "e1_recorder_v2_wire" not in legacy          # it can never select recorder v2
    assert "subprocess.run(command" in legacy and "copy_run_template" in legacy                       # it launches Caliper in the same step
    spec = next(s for s in pv.CANDIDATE_SPECS if s.path.endswith("run_contemporaneous_capture.py"))
    assert spec.role == "legacy_not_approved" and spec.approval_status == "EXCLUDED_NOT_APPROVED_ENTRY_POINT"
    assert sha(REPO / spec.path) == spec.pinned_sha256                                                # and it is unchanged


def test_prepare_and_preflight_leave_every_pinned_and_frozen_file_byte_identical(tmp_path):
    before = {rel: sha(REPO / rel) for rel in PINNED}
    assert before == PINNED
    frozen_before = tree_hashes(SUMS.parent)
    run_dir, _ = prepared(tmp_path)
    pv.preflight(run_dir, frozen_sums=SUMS)
    pv.main(["candidate-manifest", "--out", str(tmp_path / "cand.json")])
    assert {rel: sha(REPO / rel) for rel in PINNED} == PINNED
    assert tree_hashes(SUMS.parent) == frozen_before and len(frozen_before) >= 199


def test_no_silent_channel_fallback_exists_anywhere_in_the_launch_path():
    for rel in ("scripts/e1/e1_prelive_validation.py", "src/fabric_perf/controllers/prelive_contract.py"):
        source = (REPO / rel).read_text(encoding="utf-8")
        for name in cs.PROTECTED_REFERENCE_CHANNELS | cs.HISTORICAL_EVIDENCE_CHANNELS:
            assert f'"{name}"' not in source and f"'{name}'" not in source, (rel, name)
    tree = ast.parse((REPO / "scripts/e1/e1_prelive_validation.py").read_text(encoding="utf-8"))
    checked = set()
    for call in ast.walk(tree):
        if isinstance(call, ast.Call) and getattr(call.func, "attr", "") == "add_argument" and call.args:
            flag = getattr(call.args[0], "value", "")
            keywords = {k.arg: k.value for k in call.keywords}
            assert "default" not in keywords, flag                                                  # no option of the orchestrator has a default
            if flag in ("--params", "--out-root", "--prepare-id", "--run-dir", "--out"):
                assert getattr(keywords.get("required"), "value", None) is True, flag
                checked.add(flag)
    assert checked == {"--params", "--out-root", "--prepare-id", "--run-dir", "--out"}


def test_a_run_id_that_would_name_a_protected_channel_in_the_generated_files_is_refused_before_anything_is_written(tmp_path):
    """The pinned renderer writes 'e1-<run id>' into the Caliper test name; the wrapper would refuse it late, the contract refuses it first."""
    source = (REPO / "scripts/e1/e1_prelive_validation.py").read_text(encoding="utf-8")
    assert 'channel=values["channel"]' in source                                          # the renderer is never left to derive a channel
    for run_id in ("cfg001", "r002", "CFG001"):
        with pytest.raises(pc.PreliveParameterError) as caught:
            pv.prepare(write_params(tmp_path / "p.json", params(run_id=run_id)), tmp_path / "out", "x")
        assert any(i.code == "protected_name_in_run_id" for i in caught.value.issues)
    assert not (tmp_path / "out").exists()


# ================================================================ the --adapter-window-s option on the shadow capture CLI
def load_shadow_cli():
    import importlib.util
    path = REPO / "Fabric_Live_Testbed/controllers/run_production_shadow_validation.py"
    spec = importlib.util.spec_from_file_location("shadow_cli_a0e", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Stop(Exception):
    pass


def run_shadow(module, monkeypatch, tmp_path, extra, with_trio=True):
    seen = {}

    class Spy:
        RECORDER_LATENCY_MODES = ("column", "derive_from_timestamps")

        def __init__(self, **kwargs):
            seen.update(kwargs)
            raise Stop

    monkeypatch.setattr(module, "ContinuousCaliperLedgerAdapter", Spy)
    argv = ["--run-id", "r", "--channel", CHANNEL, "--out", str(tmp_path / "o.json")]
    if with_trio:
        argv += ["--recorder-csv", "a.csv", "--submits-csv", "b.csv", "--ledger-tx-csv", "c.csv"]
    with pytest.raises(Stop):
        module.main(argv + extra)
    return seen


def test_the_shadow_cli_passes_the_window_only_when_the_operator_gives_it(monkeypatch, tmp_path):
    module = load_shadow_cli()
    assert "window_s" not in run_shadow(module, monkeypatch, tmp_path, [])                             # default behaviour unchanged
    assert run_shadow(module, monkeypatch, tmp_path, ["--adapter-window-s", "4.5"])["window_s"] == 4.5


@pytest.mark.parametrize("bad", ["0", "-1", "nan", "inf"])
def test_the_shadow_cli_refuses_an_unusable_window(tmp_path, bad):
    module = load_shadow_cli()
    with pytest.raises(SystemExit) as caught:                                          # refused at argument checking, before any adapter exists
        module.main(["--run-id", "r", "--channel", CHANNEL, "--out", str(tmp_path / "o.json"), "--recorder-csv", "a", "--submits-csv", "b",
                     "--ledger-tx-csv", "c", "--adapter-window-s", bad])
    assert caught.value.code == 2


def test_the_shadow_cli_refuses_a_window_without_the_continuous_trio(tmp_path):
    module = load_shadow_cli()
    with pytest.raises(SystemExit) as caught:
        module.main(["--run-id", "r", "--channel", CHANNEL, "--out", str(tmp_path / "o.json"), "--adapter-window-s", "4"])
    assert caught.value.code == 2