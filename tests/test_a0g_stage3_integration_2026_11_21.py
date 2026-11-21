"""A0g: the Stage 3 runner with the real predictive interface prepared but DISABLED (2026-11-21).

Checklist items 20-28. Fixture predictive mode is the only authorized mode; real mode is refused before anything is read.
Everything here is offline: process/network APIs, model loading and model training are patched to raise.
"""

from __future__ import annotations

import ast
import builtins
import json
import pickle
import socket
import subprocess
import urllib.request
from pathlib import Path

import pytest

from tests import a0f_support as f
from tests import a0g_support as g
from fabric_perf.controllers import adapters
from fabric_perf.controllers import harness
from fabric_perf.controllers import stage3_predictive as sp
from fabric_perf.controllers import stage3_shadow_runner as runner

import e1_stage3_shadow_runner as cli

SRC = g.REPO / "src" / "fabric_perf" / "controllers"
MODEL_PATH_MARKERS = ("artifacts", ".joblib", "hfbtp", "throughput.joblib", "avg_latency.joblib")


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    import joblib

    def boom(*a, **k):
        raise AssertionError("a process, network or model-loading call was attempted")
    for obj, names in ((subprocess, ("run", "Popen", "call", "check_output")), (socket, ("create_connection", "getaddrinfo")),
                       (urllib.request, ("urlopen",)), (joblib, ("load", "dump")), (pickle, ("load", "loads"))):
        for name in names:
            monkeypatch.setattr(obj, name, boom)
    monkeypatch.setattr(adapters, "load_hfbtp_artifact", boom)


def c(n=5, latency=100):
    return {"count": n, "latency_ms": latency, "status": "success"}


# ---------------------------------------------------------------------------------------------- 20 fixture predictive mode
def test_20_fixture_predictive_mode_labels_every_value_as_fixture_data(tmp_path):
    manifest, rows, out = g.run(tmp_path, steps=f.steady(3))
    assert manifest["status"] == "COMPLETED" and manifest["predictive"]["mode"] == "fixture"
    assert manifest["predictive"]["fixture_data"] is True and manifest["predictive"]["real_inference_performed"] is False
    assert manifest["models"]["frozen_hfbtp_model_loaded"] is False and manifest["models"]["frozen_ewma_loaded"] is False
    assert {r["schema"] for r in rows} == {"stage3_shadow_decision/2"}
    predictive = [r for r in rows if r["controller"] == "predictive"]
    assert all(r["predictive_evidence"] is not None for r in predictive)
    assert all(r["predictive_evidence"] is None for r in rows if r["controller"] != "predictive")
    accepted = next(r for r in predictive if r["outcome"] == "propose")
    ev = accepted["predictive_evidence"]
    assert ev["mode"] == "fixture" and ev["fixture_data"] is True and ev["real_inference_performed"] is False
    assert ev["forecast"]["identity"] == runner.FIXTURE_FORECAST_IDENTITY and ev["forecast"]["is_fixture_value"] is True
    assert ev["forecast"]["unit"] == "transactions/s" and ev["forecast"]["horizon_seconds"] == 10 and ev["forecast"]["value_tps"] == 55.0
    assert ev["performance_model"]["identity"] == runner.FIXTURE_PERFORMANCE_IDENTITY
    assert ev["performance_model"]["predictions_are_fixture_values"] is True
    assert ev["performance_model"]["predicted_throughput_tps"] == 60.0 and ev["performance_model"]["predicted_latency_ms"] == 300.0
    assert ev["ood_status"] == sp.OOD_NOT_APPLICABLE_FIXTURE and ev["recommendation_is_not_applied"] is True
    assert ev["forecast"]["target"].endswith("a forecast, not an observation")


def test_a_fixture_prediction_is_never_labelled_as_a_real_or_in_grid_prediction(tmp_path):
    _, rows, _ = g.run(tmp_path, steps=f.steady(3))
    for row in rows:
        ev = row["predictive_evidence"]
        if ev is not None:
            assert ev["real_inference_performed"] is False and ev["fixture_data"] is True
            assert ev["ood_status"] != sp.OOD_WITHIN_GRID
            assert ev["performance_model"]["identity"] == runner.FIXTURE_PERFORMANCE_IDENTITY != sp.REAL_PERFORMANCE_IDENTITY
            assert ev["forecast"]["identity"] == runner.FIXTURE_FORECAST_IDENTITY != sp.REAL_FORECAST_IDENTITY
            assert ev["performance_model"]["feature_schema_is_the_contract_a_real_model_must_meet_not_what_a_fixture_uses"] is True


# ---------------------------------------------------------------------------------------------- 21 disabled real mode
def test_21_real_predictive_mode_is_disabled_and_refuses_before_anything_is_created(tmp_path):
    assert sp.REAL_INFERENCE_AUTHORIZED is False
    inputs = runner.load_inputs(f.PARAMS_FILE, g.POLICY_V2_FILE, f.NOMINAL_FILE)
    out = tmp_path / "out"
    with pytest.raises(sp.RealInferenceNotAuthorized) as e:
        runner.run_fixture_shadow(inputs, out, predictive_mode="real")
    assert e.value.code == "real_inference_not_authorized" and not out.exists()
    with pytest.raises(runner.Stage3RunnerError):
        runner.run_fixture_shadow(inputs, out, predictive_mode="REAL")
    with pytest.raises(sp.RealInferenceNotAuthorized):
        sp.build_real_predictive_components(root=tmp_path, manifest_path=tmp_path / "m.json", space=None, workload_source="x", operator_freshness=True)


def test_21b_the_cli_refuses_real_mode_before_opening_any_input(tmp_path, capsys):
    code = cli.main(["fixture", "--params", str(tmp_path / "missing.json"), "--controller-policy", str(tmp_path / "missing2.json"),
                     "--fixture", str(tmp_path / "missing3.json"), "--out-dir", str(tmp_path / "out"), "--predictive-mode", "real"])
    assert code == cli.EXIT_REAL_INFERENCE_REFUSED == 6 and not (tmp_path / "out").exists()
    assert "not authorized" in capsys.readouterr().out


@pytest.mark.parametrize("flag", ["--live", "--authorized", "--yes", "--force", "--execute", "--enable-live", "--real-inference",
                                  "--authorize-real-inference", "--allow-real-predictive"])
def test_21c_no_flag_enables_real_inference(tmp_path, flag):
    with pytest.raises(SystemExit) as e:
        cli.main(["fixture", "--params", str(f.PARAMS_FILE), "--controller-policy", str(g.POLICY_V2_FILE), "--fixture", str(f.NOMINAL_FILE),
                  "--out-dir", str(tmp_path / "o"), flag])
    assert e.value.code == 2 and not (tmp_path / "o").exists()


def test_21d_environment_variables_do_not_change_the_boundary(tmp_path, monkeypatch):
    for name in ("REAL_INFERENCE_AUTHORIZED", "A0G_REAL_INFERENCE", "STAGE3_REAL_PREDICTIVE", "ALLOW_REAL_INFERENCE", "FABRIC_PERF_REAL_MODEL"):
        monkeypatch.setenv(name, "1")
    assert sp.REAL_INFERENCE_AUTHORIZED is False
    inputs = runner.load_inputs(f.PARAMS_FILE, g.POLICY_V2_FILE, f.NOMINAL_FILE)
    with pytest.raises(sp.RealInferenceNotAuthorized):
        runner.run_fixture_shadow(inputs, tmp_path / "out", predictive_mode="real")
    assert cli.main(["fixture", "--params", "x", "--controller-policy", "y", "--fixture", "z", "--out-dir", str(tmp_path / "o"),
                     "--predictive-mode", "real"]) == 6


def test_21e_nothing_in_the_runner_modules_reads_an_environment_variable_argument_or_enabling_input():
    forbidden_names = {"environ", "getenv", "argv", "putenv"}
    for module in ("stage3_predictive.py", "stage3_shadow_runner.py"):
        tree = ast.parse((SRC / module).read_text(encoding="utf-8"))
        names = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        assert not (names & forbidden_names), (module, names & forbidden_names)
        imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
                   {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module and not n.level}
        assert not (imported & {"os", "sys", "subprocess", "socket", "urllib", "http", "requests", "joblib", "pickle", "shutil", "sklearn", "asyncio"}), imported
    source = (SRC / "stage3_predictive.py").read_text(encoding="utf-8")
    assigned = [n for n in ast.walk(ast.parse(source)) if isinstance(n, ast.Assign) and any(getattr(t, "id", None) == "REAL_INFERENCE_AUTHORIZED" for t in n.targets)]
    assert len(assigned) == 1 and isinstance(assigned[0].value, ast.Constant) and assigned[0].value.value is False


def test_21f_the_prepared_real_route_wires_guards_and_pins_without_loading_a_model(tmp_path, monkeypatch):
    """With the constant forced on in THIS TEST ONLY and every dependency faked, the route is wired as intended and loads nothing."""
    monkeypatch.setattr(sp, "REAL_INFERENCE_AUTHORIZED", True)
    selection = tmp_path / "selection.json"
    selection.write_text(json.dumps({"horizons": {"60": {"selected_model": "ewma", "alpha": 0.005}}, "min_history_s": 600}), encoding="utf-8")
    meta_dir = tmp_path / "artifact"
    meta_dir.mkdir()
    manifest_path = tmp_path / "pins.json"
    manifest_path.write_text(json.dumps({"files": {k: {"path": k, "sha256": "AA"} for k in ("selection", "model_metadata", "throughput_model", "latency_model")}}),
                             encoding="utf-8")
    from fabric_perf.controllers import offline_replay, live_foundation
    pins = {"selection": selection, "model_metadata": meta_dir / "metadata.json", "throughput_model": meta_dir / "t.joblib",
            "latency_model": meta_dir / "l.joblib"}
    monkeypatch.setattr(offline_replay, "verified_inputs", lambda root, manifest, required_keys=None: pins)
    built = {}

    class FakeHFBTP:
        def __init__(self, directory, *, candidate_space):
            built["dir"], built["space"] = Path(directory), candidate_space
            self.last_prediction_metadata = {}

        def predict(self, arrival, config):
            raise AssertionError("no prediction may run while preparing")

    class FakeEWMA:
        requires_timestamped_history = True
        accepts_history_reference = True

        def __init__(self, source):
            built["source"] = source

        def predict_timestamped(self, times, values, horizon, *, now_utc, max_history_age_s=5.0, history_ref=None):
            raise AssertionError("no forecast may run while preparing")
    monkeypatch.setattr(adapters, "HFBTPArtifactPerformancePredictor", FakeHFBTP)
    monkeypatch.setattr(live_foundation, "StreamingEWMA", FakeEWMA)
    space = adapters.ApprovedMaxMessageCountSpace(frozenset({40, 45}), {40: 40, 45: 45}, runner.coerce_config({"max_message_count": 40}))
    with pytest.raises(sp.PredictiveIntegrationError) as e:
        sp.build_real_predictive_components(root=tmp_path, manifest_path=manifest_path, space=space, workload_source="s", operator_freshness=False)
    assert e.value.code == "freshness_limits_not_operator_supplied" and built == {}
    parts = sp.build_real_predictive_components(root=tmp_path, manifest_path=manifest_path, space=space, workload_source="stream-1", operator_freshness=True)
    assert parts.mode == "real" and parts.fixture_data is False and parts.real_inference_performed is False
    assert isinstance(parts.performance_predictor, sp.GuardedPerformancePredictor) and isinstance(parts.forecaster, sp.GuardedForecaster)
    assert built["dir"] == meta_dir and built["source"] == "stream-1" and built["space"] is space
    assert parts.label()["feature_schema_id"] == sp.FEATURE_SCHEMA_ID
    inputs = runner.load_inputs(f.PARAMS_FILE, g.POLICY_V2_FILE, f.NOMINAL_FILE)
    with pytest.raises(runner.Stage3RunnerError, match="no captured-frame route"):       # even then the runner has no real route
        runner.run_fixture_shadow(inputs, tmp_path / "out", predictive_mode="real")
    assert not (tmp_path / "out").exists()


# ---------------------------------------------------------------------------------------------- 22 no frozen-model loading
def test_22_a_run_opens_no_model_artifact_and_never_calls_the_loader(tmp_path, monkeypatch):
    opened: list[str] = []
    real_open, real_read_bytes, real_read_text = builtins.open, Path.read_bytes, Path.read_text

    def spy_open(file, *a, **k):
        opened.append(str(file))
        return real_open(file, *a, **k)

    def spy_bytes(self, *a, **k):
        opened.append(str(self))
        return real_read_bytes(self, *a, **k)

    def spy_text(self, *a, **k):
        opened.append(str(self))
        return real_read_text(self, *a, **k)
    monkeypatch.setattr(builtins, "open", spy_open)
    monkeypatch.setattr(Path, "read_bytes", spy_bytes)
    monkeypatch.setattr(Path, "read_text", spy_text)
    manifest, _, _ = g.run(tmp_path, steps=f.steady(3))
    assert manifest["status"] == "COMPLETED"
    lowered = [p.lower().replace("\\", "/") for p in opened]
    assert not [p for p in lowered if p.endswith(".joblib") or "experiments/artifacts" in p or p.endswith("metadata.json")]
    assert not [p for p in lowered if "flashbots" in p and p.endswith((".csv", ".gz"))]


# ---------------------------------------------------------------------------------------------- 23 no retraining
def test_23_no_training_or_fitting_is_reachable_from_the_runner_modules(tmp_path, monkeypatch):
    from fabric_perf.models import artifacts
    from sklearn.ensemble import RandomForestRegressor
    called = []
    monkeypatch.setattr(artifacts, "train_and_save_hfbtp", lambda *a, **k: called.append("train"))
    monkeypatch.setattr(RandomForestRegressor, "fit", lambda *a, **k: called.append("fit"))
    g.run(tmp_path, steps=f.steady(3))
    assert called == []
    for module in ("stage3_predictive.py", "stage3_shadow_runner.py"):
        tree = ast.parse((SRC / module).read_text(encoding="utf-8"))
        names = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)} | {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        assert not (names & {"fit", "fit_transform", "partial_fit", "train_and_save_hfbtp", "build_pipeline", "dump", "RandomForestRegressor"}), module


# ---------------------------------------------------------------------------------------------- 24-27 comparability and provenance
def test_24_every_frame_reaches_all_three_controllers_with_one_identity(tmp_path):
    manifest, rows, out = g.run(tmp_path, steps=f.steady(3))
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert summary["comparability"]["comparable"] is True and summary["freshness_limits"]["max_future_skew_s"] == 0.0
    for index, group in f.by_frame(rows).items():
        assert sorted(group) == ["predictive", "reactive", "static"]
        assert len({r["frame"]["id"] for r in group.values()}) == 1 and len({json.dumps(r["observed"], sort_keys=True) for r in group.values()}) == 1


def test_25_a_missing_forecast_is_a_recorded_controller_abstention_with_the_reason(tmp_path):
    steps = f.steady(3)
    steps[2] = {**steps[2], "forecast": "missing"}
    _, rows, _ = g.run(tmp_path, steps=steps)
    row = f.by_frame(rows)[2]["predictive"]
    assert row["outcome"] == "abstain" and row["abstention_scope"] == "controller" and row["frame"]["status"] == "accepted"
    ev = row["predictive_evidence"]
    assert ev["failure"] is not None and ev["failure"]["stage"] == "forecast" and ev["failure"]["error_type"] == "ValueError" and ev["forecast"]["value_tps"] is None
    assert ev["performance_model"]["predicted_throughput_tps"] is None and ev["performance_model"]["predictions_are_fixture_values"] is False
    assert row["recommended_config"] is None and f.by_frame(rows)[2]["static"]["outcome"] != "abstain"


def test_26_an_invalid_frame_makes_every_controller_abstain_alike_and_the_evidence_says_why(tmp_path):
    steps = [f.step(client=c()), f.step(client=c(), ledger_valid_tx=5), f.step(client=c(), ledger_valid_tx=5, ledger_read_ok=False, poll=True)]
    _, rows, _ = g.run(tmp_path, steps=steps)
    group = f.by_frame(rows)[2]
    assert {r["outcome"] for r in group.values()} == {"abstain"} and {r["abstention_scope"] for r in group.values()} == {"frame"}
    assert len({r["frame"]["reason_code"] for r in group.values()}) == 1 and group["static"]["frame"]["status"] == "rejected"
    ev = group["predictive"]["predictive_evidence"]
    assert ev["frame_status"] == "rejected" and ev["performance_model"]["predicted_throughput_tps"] is None


def test_27_decision_provenance_is_stable_across_runs_and_carries_the_schema_identity(tmp_path):
    _, rows_a, out_a = g.run(tmp_path, steps=f.steady(3), name="a")
    _, rows_b, out_b = g.run(tmp_path, steps=f.steady(3), name="b")
    assert (out_a / "shadow_decisions.jsonl").read_bytes() == (out_b / "shadow_decisions.jsonl").read_bytes()
    schema_ids = {r["predictive_evidence"]["performance_model"]["feature_schema_id"] for r in rows_a if r["predictive_evidence"]}
    assert schema_ids == {sp.FEATURE_SCHEMA_ID}
    for r in rows_a:
        if r["predictive_evidence"]:
            assert r["predictive_evidence"]["performance_model"]["feature_order"] == sp.PERFORMANCE_CONTRACT["feature_order"]
            assert r["predictive_evidence"]["workload_observation"]["basis"] == "fixture placeholder history"
    manifest = json.loads((out_a / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["predictive"]["feature_schema"]["id"] == sp.FEATURE_SCHEMA_ID and runner.verify_run_directory(out_a) == []


# ---------------------------------------------------------------------------------------------- 28 actuator isolation
def test_28_no_live_actuator_is_reachable_and_every_proposal_is_a_record(tmp_path, monkeypatch):
    def boom(*a, **k):
        raise AssertionError("a live actuator was reached")
    for cls in ("CallableActuator", "VerifiedCallableActuator"):
        if hasattr(harness, cls):
            monkeypatch.setattr(getattr(harness, cls), "apply", boom)
    manifest, rows, out = g.run(tmp_path, steps=f.steady(3))
    assert manifest["actuation"]["configuration_changes_applied"] == 0 and manifest["actuation"]["live_actuator_available"] is False
    for row in rows:
        assert row["proposal_applied"] is False and row["actuator"]["configuration_changes_applied"] == 0 and row["actuator"]["submitted_to_fabric"] is False
        if row["predictive_evidence"]:
            assert row["predictive_evidence"]["recommendation_is_not_applied"] is True
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert summary["shadow_actuator_proposals_accepted"] == 0 and summary["real_inference_performed"] is False
    for module in ("stage3_predictive.py", "stage3_shadow_runner.py"):
        text = (SRC / module).read_text(encoding="utf-8")
        assert "enable_live" not in text and "shadow=False" not in text and "CallableActuator" not in text


def test_the_protected_channels_are_still_refused_with_a_v2_policy(tmp_path):
    for channel in ("e1-r002", "e1-cfg001", "E1-B001", " e1-r001 "):
        params = f.params(channel=channel)
        inputs_dir = tmp_path / channel.strip().lower()
        inputs_dir.mkdir()
        with pytest.raises(Exception) as e:
            runner.load_inputs(f.write(inputs_dir / "p.json", params), g.POLICY_V2_FILE, f.NOMINAL_FILE)
        assert "protected" in str(e.value).lower() or "channel" in str(e.value).lower()
