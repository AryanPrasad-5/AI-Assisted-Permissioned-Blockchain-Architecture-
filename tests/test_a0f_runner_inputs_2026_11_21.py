"""A0f: runner initialization, mode selection, validation of every input, and the live-mode refusal (2026-11-21).

Covers task checklist items 1-9 and 44. Nothing here contacts a network or starts a process.
"""

from __future__ import annotations

import ast
import json
import socket
import subprocess
import urllib.request

import pytest

from tests import a0f_support as s
from fabric_perf.controllers import channel_selection as cs
from fabric_perf.controllers import stage3_shadow_runner as runner

import e1_stage3_shadow_runner as cli  # noqa: E402  (importable once tests.a0f_support has put scripts/e1 on sys.path)


@pytest.fixture(autouse=True)
def no_process_or_network(monkeypatch):
    """Every test in this file runs with process and network entry points patched to raise."""
    def boom(*a, **k):
        raise AssertionError("a process or network call was attempted")
    for obj, names in ((subprocess, ("run", "Popen", "call", "check_call", "check_output")),
                       (socket, ("create_connection", "getaddrinfo", "gethostbyname")),
                       (urllib.request, ("urlopen",))):
        for name in names:
            monkeypatch.setattr(obj, name, boom)
    monkeypatch.setattr(socket.socket, "connect", boom)


def cli_fixture(tmp_path, *, fixture_doc=None, params_doc=None, policy_doc=None, out="out"):
    p = s.write(tmp_path / "p.json", params_doc if params_doc is not None else s.params())
    c = s.write(tmp_path / "c.json", policy_doc if policy_doc is not None else s.policy())
    f = s.write(tmp_path / "f.json", fixture_doc if fixture_doc is not None else s.fixture())
    return cli.main(["fixture", "--params", str(p), "--controller-policy", str(c), "--fixture", str(f), "--out-dir", str(tmp_path / out)])


# ---------------------------------------------------------------------------------------------- 1-3 mode selection
def test_explicit_fixture_mode_runs_and_reports_no_live_readiness(tmp_path, capsys):
    assert cli_fixture(tmp_path) == cli.EXIT_OK
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "COMPLETED" and report["live_ready"] is False and report["configuration_changes_applied"] == 0


def test_a_missing_mode_is_refused_before_anything_runs(tmp_path, capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main([])
    assert exc.value.code == 2


@pytest.mark.parametrize("mode", ["live", "run", "replay", "--live", "execute", ""])
def test_an_unsupported_mode_is_refused(mode):
    with pytest.raises(SystemExit) as exc:
        cli.main([mode])
    assert exc.value.code == 2


@pytest.mark.parametrize("flag", [["--live"], ["--authorized"], ["--yes"], ["--force"], ["--execute"], ["--enable-live"]])
def test_no_flag_turns_a_live_run_on(flag, tmp_path):
    with pytest.raises(SystemExit) as exc:
        cli.main(["fixture", "--params", "p", "--controller-policy", "c", "--fixture", "f", "--out-dir", str(tmp_path / "o"), *flag])
    assert exc.value.code == 2
    with pytest.raises(SystemExit) as exc:
        cli.main(["live-shadow", "--params", "p", *flag])
    assert exc.value.code == 2


# ---------------------------------------------------------------------------------------------- 4-5 channel
def test_a_missing_channel_is_refused_and_nothing_is_created(tmp_path):
    doc = s.params()
    del doc["channel"]
    assert cli_fixture(tmp_path, params_doc=doc) == cli.EXIT_INVALID
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("channel", ["", "   ", "e1-r002", "E1-R002", " e1-cfg001 ", "e1-cfg001", "e1-b001", "e1-r001", "not a channel", None, 7])
def test_missing_blank_malformed_and_protected_channels_are_refused(tmp_path, channel):
    assert cli_fixture(tmp_path, params_doc=s.params(channel=channel)) == cli.EXIT_INVALID
    assert not (tmp_path / "out").exists()


def test_the_runner_rejects_a_protected_channel_through_the_a0b_validator_not_a_private_list(tmp_path):
    p = s.write(tmp_path / "p.json", s.params(channel="e1-r002"))
    with pytest.raises(runner.Stage3RunnerError, match="channel"):
        runner.load_inputs(p, s.POLICY_FILE, s.NOMINAL_FILE)
    with pytest.raises(cs.ChannelSelectionError):
        cs.select_experiment_channel("e1-r002")


# ---------------------------------------------------------------------------------------------- 6 cell
@pytest.mark.parametrize("key", ["max_message_count", "batch_timeout", "preferred_max_bytes", "rate_tps", "workload", "warmup_s"])
def test_a_missing_experimental_cell_value_is_refused(tmp_path, key):
    doc = s.params()
    del doc["cell"][key]
    assert cli_fixture(tmp_path, params_doc=doc) == cli.EXIT_INVALID


def test_a_missing_cell_section_is_refused(tmp_path):
    doc = s.params()
    del doc["cell"]
    assert cli_fixture(tmp_path, params_doc=doc) == cli.EXIT_INVALID


def test_the_static_configuration_comes_from_the_cell_and_nothing_else(tmp_path):
    doc = s.params()
    doc["cell"]["max_message_count"] = 45
    pol = s.policy(reactive={**s.policy()["reactive"], "congested_max_message_count": 40})
    manifest, rows, _ = s.run(tmp_path, fixture_doc=s.fixture(steps=s.steady(2)), params_doc=doc, policy_doc=pol)
    assert manifest["static_configuration"]["max_message_count"] == 45
    static = [r for r in rows if r["controller"] == "static" and r["outcome"] == "hold"]
    assert static and all(r["recommended_config"]["max_message_count"] == 45 for r in static)


def test_the_cell_must_be_one_of_the_policy_candidates(tmp_path):
    doc = s.params()
    doc["cell"]["max_message_count"] = 41
    assert cli_fixture(tmp_path, params_doc=doc) == cli.EXIT_INVALID


def test_the_policy_must_list_the_cells_configuration_even_when_the_fixture_has_a_row_for_it(tmp_path):
    doc = s.params()
    doc["cell"]["max_message_count"] = 41
    fx = s.fixture()
    fx["fixture_performance_model"]["41"] = {"throughput_tps": 1.0, "latency_ms": 1.0}
    assert cli_fixture(tmp_path, params_doc=doc, fixture_doc=fx) == cli.EXIT_INVALID          # policy candidates are {40, 45}
    assert not (tmp_path / "out").exists()


def test_the_fixture_must_have_a_performance_row_for_the_cell_even_when_the_policy_lists_it(tmp_path):
    doc = s.params()
    doc["cell"]["max_message_count"] = 41
    pol = s.policy(candidate_hfbtp_block_size_by_max_message_count={"40": 40, "41": 41, "45": 45})
    assert cli_fixture(tmp_path, params_doc=doc, policy_doc=pol) == cli.EXIT_INVALID          # fixture table has rows for 40 and 45 only
    assert not (tmp_path / "out").exists()


# ---------------------------------------------------------------------------------------------- 7 operator parameters
@pytest.mark.parametrize("mutate", [
    lambda d: d["stage3"].__setitem__("max_consecutive_abstentions", 0),
    lambda d: d["stage3"].__setitem__("max_consecutive_abstentions", None),
    lambda d: d["stage3"].__setitem__("max_consecutive_abstentions", True),
    lambda d: d["stage3"].__setitem__("max_initialization_frames", 0),
    lambda d: d["stage3"].pop("max_initialization_frames"),
    lambda d: d["telemetry_adapter"].__setitem__("window_s", 1.0),          # not above poll_interval_s
    lambda d: d["telemetry_adapter"].__setitem__("window_s", float("nan")),
    lambda d: d["telemetry_adapter"].__setitem__("recorder_latency", "guess"),
    lambda d: d["telemetry_adapter"].pop("window_s"),
    lambda d: d["telemetry_sources"].__setitem__("prometheus_endpoints", []),
    lambda d: d["cell"].__setitem__("channel", "e1-offline.a0f"),
    lambda d: d.__setitem__("schema", "prelive_params/2"),
])
def test_invalid_operator_parameters_are_refused_and_nothing_is_created(tmp_path, mutate):
    doc = s.params()
    mutate(doc)
    assert cli_fixture(tmp_path, params_doc=doc) == cli.EXIT_INVALID
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("path,value", [
    (("reactive", "latency_high_ms"), None), (("reactive", "latency_high_ms"), -1), (("reactive", "latency_low_ms"), 500.0),
    (("reactive", "consecutive_samples"), 0), (("reactive", "consecutive_samples"), 1.5),
    (("reactive", "congested_max_message_count"), 40),         # equals the cell: a reactive controller that can never act
    (("reactive", "congested_max_message_count"), 99),         # not a candidate
    (("predictive", "horizon_seconds"), 0), (("predictive", "cooldown_samples"), -1),
    (("predictive", "latency_penalty_per_ms"), float("inf")), (("telemetry", "freshness_max_age_s"), 0),
    (("telemetry", "client_stale_after_s"), None),
])
def test_an_invalid_or_missing_controller_policy_value_is_refused(tmp_path, path, value):
    doc = s.policy()
    doc[path[0]][path[1]] = value
    assert cli_fixture(tmp_path, policy_doc=doc) == cli.EXIT_INVALID
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("key", ["telemetry", "reactive", "predictive", "candidate_hfbtp_block_size_by_max_message_count", "schema"])
def test_a_policy_section_has_no_default(key, tmp_path):
    doc = s.policy()
    del doc[key]
    assert cli_fixture(tmp_path, policy_doc=doc) == cli.EXIT_INVALID


def test_every_policy_problem_is_reported_at_once():
    doc = s.policy()
    doc["reactive"]["latency_high_ms"] = -1
    doc["predictive"]["horizon_seconds"] = 0
    with pytest.raises(runner.Stage3RunnerError) as exc:
        runner.validate_controller_policy(doc, cell_max_message_count=40)
    assert "reactive.latency_high_ms" in str(exc.value) and "predictive.horizon_seconds" in str(exc.value)


@pytest.mark.parametrize("mutate", [
    lambda d: d.__setitem__("provenance", "REAL"),
    lambda d: d.__setitem__("steps", []),
    lambda d: d["steps"][0].__setitem__("advance_s", 0),
    lambda d: d["steps"][0].__setitem__("bogus", 1),
    lambda d: d["steps"][0].__setitem__("ledger_valid_tx", 0),
    lambda d: d["steps"][0].__setitem__("model", "oracle"),
    lambda d: d["steps"][0].__setitem__("forecast", "high"),
    lambda d: d.__setitem__("start_utc", "2026-11-21T10:00:00"),            # naive timestamp
    lambda d: d["channel_configuration"].pop("orderers"),
    lambda d: d.__setitem__("fixture_performance_model", {}),
    lambda d: d.__setitem__("description", " "),
])
def test_a_malformed_fixture_is_refused(tmp_path, mutate):
    doc = s.fixture()
    mutate(doc)
    assert cli_fixture(tmp_path, fixture_doc=doc) == cli.EXIT_INVALID
    assert not (tmp_path / "out").exists()


def test_an_existing_output_directory_is_never_reused(tmp_path):
    (tmp_path / "out").mkdir()
    assert cli_fixture(tmp_path) == cli.EXIT_INVALID
    assert list((tmp_path / "out").iterdir()) == []


def test_an_unreadable_or_non_json_input_is_refused(tmp_path):
    p = tmp_path / "p.json"
    p.write_text("not json", encoding="utf-8")
    assert cli.main(["fixture", "--params", str(p), "--controller-policy", str(s.POLICY_FILE), "--fixture", str(s.NOMINAL_FILE),
                     "--out-dir", str(tmp_path / "o")]) == cli.EXIT_INVALID
    assert cli.main(["fixture", "--params", str(tmp_path / "missing.json"), "--controller-policy", str(s.POLICY_FILE),
                     "--fixture", str(s.NOMINAL_FILE), "--out-dir", str(tmp_path / "o2")]) == cli.EXIT_INVALID


# ---------------------------------------------------------------------------------------------- 8 artifact approval status
def test_the_candidate_manifest_is_reported_not_approved_and_never_upgraded(tmp_path, capsys):
    before = s.sha(s.CANDIDATE_MANIFEST)
    code = cli.main(["live-shadow", "--params", str(s.PARAMS_FILE), "--candidate-manifest", str(s.CANDIDATE_MANIFEST)])
    report = json.loads(capsys.readouterr().out)
    assert code == cli.EXIT_LIVE_REFUSED and report["live_permitted"] is False
    gate = next(g for g in report["gates"] if g["gate"] == "G2_artifact_integrity")
    assert gate["met"] is False and "approved=False" in gate["detail"] and "not owner approval" in gate["detail"]
    assert s.sha(s.CANDIDATE_MANIFEST) == before


def test_a_run_manifest_never_claims_approval_or_live_readiness(tmp_path):
    manifest, _, _ = s.run(tmp_path)
    text = json.dumps(manifest)
    assert manifest["live_ready"] is False and "approved\": true" not in text.lower()


# ---------------------------------------------------------------------------------------------- 9 + 44 no live connection
@pytest.mark.parametrize("env", [{}, {"E1_AUTHORIZE": "1", "FABRIC_LIVE": "1", "ALLOW_LIVE": "1", "CI": "1", "I_AM_THE_OWNER": "yes"}])
def test_live_mode_is_refused_with_or_without_authorization_looking_environment(tmp_path, capsys, monkeypatch, env):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    assert cli.main(["live-shadow", "--params", str(s.PARAMS_FILE)]) == cli.EXIT_LIVE_REFUSED
    report = json.loads(capsys.readouterr().out)
    assert report["live_permitted"] is False and report["connections_opened"] == 0
    assert all(g["met"] is False for g in report["gates"])
    assert {g["gate"] for g in report["gates"]} == {"G1_channel", "G2_artifact_integrity", "G3_clock_preflight", "G4_operational_execution"}


def test_live_mode_reports_a_protected_channel_as_a_failed_gate_and_still_refuses(tmp_path, capsys):
    p = s.write(tmp_path / "p.json", s.params(channel="e1-cfg001"))
    assert cli.main(["live-shadow", "--params", str(p)]) == cli.EXIT_LIVE_REFUSED
    gate = next(g for g in json.loads(capsys.readouterr().out)["gates"] if g["gate"] == "G1_channel")
    assert gate["met"] is False and "protected_reference_channel" in gate["detail"]


def test_live_mode_with_unreadable_parameters_still_refuses(tmp_path, capsys):
    assert cli.main(["live-shadow", "--params", str(tmp_path / "nope.json")]) == cli.EXIT_LIVE_REFUSED


def test_live_gate_report_is_constant_false_for_every_input(tmp_path):
    for params_doc in (s.params(), s.params(channel="e1-offline.other")):
        p = s.write(tmp_path / "p.json", params_doc)
        assert runner.evaluate_live_gates(p, candidate_manifest_path=s.CANDIDATE_MANIFEST, repo_root=s.REPO)["live_permitted"] is False


def test_fixture_mode_works_with_process_and_network_calls_patched_to_raise(tmp_path):
    # the autouse fixture patches subprocess, socket and urllib to raise; a full run completing proves none was reached
    manifest, rows, _ = s.run(tmp_path)
    assert manifest["status"] == "COMPLETED" and rows


def test_the_fixture_prometheus_url_is_never_contacted(tmp_path):
    # the parameters name a localhost URL; the fixture fetch ignores it (a real fetch would raise under the patches above)
    manifest, rows, _ = s.run(tmp_path)
    assert any(r["frame"]["status"] == "accepted" for r in rows)


FORBIDDEN_IMPORTS = {"subprocess", "socket", "urllib", "http", "requests", "docker", "asyncio", "multiprocessing", "ssl", "ctypes",
                     "os", "joblib", "pickle", "shutil", "sklearn"}
FORBIDDEN_ATTRIBUTES = {"environ", "getenv", "system", "Popen", "popen", "urlopen", "create_connection", "startfile", "check_output",
                        "load_hfbtp_artifact", "HFBTPArtifactPerformancePredictor", "FlashbotsSeriesForecaster"}


@pytest.mark.parametrize("path", [s.REPO / "src/fabric_perf/controllers/stage3_shadow_runner.py", s.E1 / "e1_stage3_shadow_runner.py"])
def test_the_runner_files_import_no_process_network_environment_or_model_loading_api(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
            imported |= {a.name for a in node.names}
    assert not (imported & FORBIDDEN_IMPORTS), imported & FORBIDDEN_IMPORTS
    attributes = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
    assert not (attributes & FORBIDDEN_ATTRIBUTES), attributes & FORBIDDEN_ATTRIBUTES


def test_no_runner_file_names_a_protected_or_earlier_channel_as_a_literal():
    for path in (s.REPO / "src/fabric_perf/controllers/stage3_shadow_runner.py", s.E1 / "e1_stage3_shadow_runner.py"):
        source = path.read_text(encoding="utf-8")
        for name in ("e1-r002", "e1-cfg001", "e1-b001", "e1-r001"):
            assert name not in source, (path.name, name)


def test_the_example_inputs_validate_and_name_themselves_synthetic():
    inputs = runner.load_inputs(s.PARAMS_FILE, s.POLICY_FILE, s.NOMINAL_FILE)
    assert inputs.fixture.provenance == "SYNTHETIC" and inputs.values["channel"] == "e1-offline.a0f"
    assert runner.load_inputs(s.PARAMS_FILE, s.POLICY_FILE, s.DEGRADED_FILE).fixture.provenance == "SYNTHETIC"
