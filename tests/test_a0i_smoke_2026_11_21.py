"""A0i real-inference smoke-test PREPARATION (checklist items 26-33). Nothing here loads a model, runs the frozen EWMA or produces a genuine forecast.

The artifacts are hashed as bytes; the spec, the command, the output schema and the validator are exercised; ``execute_smoke`` is driven only
with labelled fixture interfaces, and its output never carries a genuine value.
"""

from __future__ import annotations

import ast
import json
import os
import pickle
import socket
import subprocess
from pathlib import Path

import pytest

from tests import a0i_support as s
from tests.a0i_support import at, ch, smoke, wc

from fabric_perf.controllers import adapters
from fabric_perf.controllers import live_foundation as lf
from fabric_perf.controllers import stage3_predictive as sp
from fabric_perf.controllers.base import FabricConfig, PerformancePrediction
from fabric_perf.models import artifacts

import e1_a0i_real_inference_smoke as cli

REPO = s.REPO
SMOKE_NAMES = ("real_inference_smoke.py",)


@pytest.fixture(autouse=True)
def nothing_loaded_nothing_sent(monkeypatch):
    fired: list[str] = []

    def trap(name):
        def refuse(*args, **kwargs):
            fired.append(name)
            raise AssertionError(f"{name} is forbidden in the smoke-test preparation")
        return refuse
    import joblib
    monkeypatch.setattr(joblib, "load", trap("joblib.load"))
    monkeypatch.setattr(pickle, "load", trap("pickle.load"))
    monkeypatch.setattr(pickle, "loads", trap("pickle.loads"))
    monkeypatch.setattr(subprocess, "Popen", trap("subprocess.Popen"))
    monkeypatch.setattr(socket, "socket", trap("socket.socket"))
    monkeypatch.setattr(lf.StreamingEWMA, "__init__", trap("StreamingEWMA"))
    monkeypatch.setattr(adapters.HFBTPArtifactPerformancePredictor, "__init__", trap("HFBTPArtifactPerformancePredictor"))
    monkeypatch.setattr(artifacts, "load_hfbtp_artifact", trap("load_hfbtp_artifact"))
    yield fired
    assert fired == []


# ------------------------------------------------------------------------------------------------ 26 exact artifact selection
def test_26_the_artifacts_are_the_repository_files_the_task_names():
    spec = smoke.build_spec()
    assert set(spec["inputs"]) == {"series", "selection", "model_metadata", "throughput_model", "latency_model", "stored_forecast_reference", "controller_policy"}
    for name, entry in spec["inputs"].items():
        assert (REPO / entry["path"]).is_file(), name
    assert spec["inputs"]["series"]["path"] == "data/processed/flashbots/series_2026-09-21_1s.csv.gz"
    assert spec["observation_cutoff_utc"] == "2026-09-21T12:00:00+00:00" and spec["forecast_horizon_seconds"] == 60
    assert spec["ewma"]["alpha"] == 0.005 and spec["ewma"]["min_history_bins"] == 600 and spec["ewma"]["identity"] == sp.REAL_FORECAST_IDENTITY
    selection = json.loads((REPO / spec["inputs"]["selection"]["path"]).read_text(encoding="utf-8"))
    assert selection["horizons"]["60"]["selected_model"] == "ewma" and selection["horizons"]["60"]["alpha"] == 0.005 and selection["min_history_s"] == 600
    policy = json.loads((REPO / spec["inputs"]["controller_policy"]["path"]).read_text(encoding="utf-8"))
    assert sorted(policy["candidate_hfbtp_block_size_by_max_message_count"]) == ["40", "45"]
    assert spec["candidate_configuration_source"]["orderers"] == smoke.ORDERERS_PLACEHOLDER                       # never defaulted
    assert spec["input_coverage_mode"]["mode"] == "benchmark_zero_fill" and spec["input_coverage_mode"]["live_quality"] is False
    assert spec["input_coverage_mode"]["satisfies_a0i_coverage_contract"] is False and spec["separate_authorization_required"] is True


def test_26b_the_stored_reference_forecast_is_read_from_the_pinned_table():
    value = smoke.read_stored_reference(REPO)
    assert value == pytest.approx(15.754735501056683) and sp.PERFORMANCE_CONTRACT["domain"]["arrival_rate_tps"][0] <= value <= 200
    header = (REPO / smoke.ARTIFACTS["stored_forecast_reference"][0]).read_text(encoding="utf-8").splitlines()[1].split(",")
    assert int(header[0]) == smoke.STORED_ORIGIN_S == 12 * 3600


# --------------------------------------------------------------------------------------------------------- 27 hash verification
def test_27_every_input_hash_matches_the_spec_and_the_pin_file():
    report = smoke.verify_inputs(REPO)
    assert report["all_ok"] is True and report["models_loaded"] is False
    pins = json.loads((REPO / smoke.PIN_FILE).read_text(encoding="utf-8"))["files"]
    for name in smoke.PIN_KEYS:
        assert report["inputs"][name]["matches_pin_file"] is True and report["inputs"][name]["actual_sha256"] == pins[name]["sha256"].lower()


def test_27b_a_wrong_hash_or_a_missing_file_fails_verification(monkeypatch):
    broken = dict(smoke.ARTIFACTS)
    broken["selection"] = (broken["selection"][0], "0" * 64)
    monkeypatch.setattr(smoke, "ARTIFACTS", broken)
    report = smoke.verify_inputs(REPO)
    assert report["all_ok"] is False and report["inputs"]["selection"]["matches_spec"] is False and report["inputs"]["series"]["matches_spec"] is True
    missing = dict(smoke.ARTIFACTS)
    missing["series"] = ("data/processed/flashbots/no_such_file.csv.gz", missing["series"][1])
    monkeypatch.setattr(smoke, "ARTIFACTS", missing)
    gone = smoke.verify_inputs(REPO)
    assert gone["all_ok"] is False and gone["inputs"]["series"]["actual_sha256"] is None
    monkeypatch.setattr(smoke, "ARTIFACTS", dict(smoke.ARTIFACTS, stored_forecast_reference=("experiments/results/flashbots_horizon_benchmark_v2/final/forecasts_F_H60.csv",
                                                                                             smoke.ARTIFACTS["stored_forecast_reference"][1])))
    with pytest.raises(ValueError, match="pinned hash"):
        smoke.read_stored_reference(REPO)


def test_27c_the_cli_reports_a_hash_failure_with_a_nonzero_exit(monkeypatch, capsys):
    broken = dict(smoke.ARTIFACTS)
    broken["latency_model"] = (broken["latency_model"][0], "f" * 64)
    monkeypatch.setattr(smoke, "ARTIFACTS", broken)
    assert cli.main(["verify-inputs"]) == 1 and '"all_ok": false' in capsys.readouterr().out


# ------------------------------------------------------------------------------------------------------- 28 schema compatibility
def test_28_the_hfbtp_feature_schema_matches_the_stored_model_metadata():
    spec = smoke.build_spec()
    metadata = json.loads((REPO / spec["inputs"]["model_metadata"]["path"]).read_text(encoding="utf-8"))
    assert spec["hfbtp_feature_schema"]["feature_order"] == metadata["features"] == list(sp.FEATURE_COLUMNS)
    assert spec["hfbtp_feature_schema"]["targets"] == metadata["targets"] and spec["hfbtp_feature_schema"]["id"] == sp.FEATURE_SCHEMA_ID
    assert metadata["dataset_sha256"] == sp.HFBTP_DATASET_SHA256 and metadata["model_name"] == "random_forest"


def test_28b_the_real_input_prefix_fits_the_forecast_contract_without_any_forecast(tmp_path):
    source = ch.CapturedSource(source_id="flashbots-mempool-2026-09-21", utc_day="2026-09-21")
    binned = ch.load_binned_series(REPO / smoke.ARTIFACTS["series"][0], source, expected_sha256=smoke.ARTIFACTS["series"][1])
    history = ch.CapturedHistory(binned, zero_policy=ch.ZERO_POLICY_BENCHMARK_ZERO_FILL)
    history.advance_to(smoke.CUTOFF_UTC)
    evidence = history.evidence(smoke.CUTOFF_UTC)
    assert evidence["validation_outcome"] == ch.VALID_COMPLETE and evidence["valid_bin_count"] == smoke.HISTORY_BINS == 43_200
    assert evidence["history_start_utc"].startswith("2026-09-21T00:00:00") and evidence["history_end_utc"].startswith("2026-09-21T11:59:59")
    assert evidence["no_future_data"] is True and evidence["live_semantics"] is False and evidence["coverage_provenance"]["contract_satisfied"] is False
    strict = ch.CapturedHistory(binned)                                       # strict mode would not even start: the reason the smoke test cannot use it
    strict.advance_to(smoke.CUTOFF_UTC)
    assert strict.evidence(smoke.CUTOFF_UTC)["validation_outcome"] == ch.VALID_INCOMPLETE


# ------------------------------------------------------------------------------------------------------------ 29 command generation
def test_29_the_command_is_exact_has_no_enabling_flag_and_names_an_existing_script():
    command = smoke.build_command()
    assert command[:3] == [".venv/Scripts/python.exe", "scripts/e1/e1_a0i_real_inference_smoke.py", "run"] and (REPO / command[1]).is_file()
    assert "--out-dir" in command and "--orderers" in command and smoke.build_spec()["command"] == command
    text = " ".join(command).lower()
    assert not any(word in text for word in ("authorize", "enable", "force", "--yes", "--real", "environ"))


# -------------------------------------------------------------------------------------------------- 30 disabled execution, 32 gate
def test_30_the_run_entry_is_refused_before_anything_is_read_or_created(tmp_path, capsys):
    target = tmp_path / "never"
    with pytest.raises(sp.RealInferenceNotAuthorized, match="real_inference_not_authorized"):
        smoke.run_real_smoke(out_dir=target, orderers=3)
    assert cli.main(["run", "--out-dir", str(target), "--orderers", "3"]) == 6 and "REFUSED" in capsys.readouterr().out
    assert not target.exists() and sp.REAL_INFERENCE_AUTHORIZED is False


def test_30b_the_post_authorization_wiring_is_deliberately_absent(monkeypatch):
    monkeypatch.setattr(sp, "REAL_INFERENCE_AUTHORIZED", True)                  # even with the constant flipped, nothing here loads a model
    with pytest.raises(smoke.SmokeProcedureNotWired):
        smoke.run_real_smoke()


# ------------------------------------------------------------------------------------------------------- 31 no real model loading
def test_31_the_preparation_loads_no_model_and_runs_no_forecaster(nothing_loaded_nothing_sent):
    smoke.build_spec()
    smoke.verify_inputs(REPO)
    smoke.read_stored_reference(REPO)
    assert smoke.validate_output(smoke.empty_output("r")) == []
    cli.main(["spec"])
    assert nothing_loaded_nothing_sent == []


def test_31b_the_smoke_module_imports_no_model_process_network_or_file_system_api():
    tree = ast.parse((REPO / "src" / "fabric_perf" / "controllers" / "real_inference_smoke.py").read_text(encoding="utf-8"))
    imported = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    imported |= {(n.module or "").split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.level == 0}
    assert not imported & {"joblib", "pickle", "sklearn", "numpy", "pandas", "subprocess", "socket", "urllib", "os", "shutil", "requests"}, imported


# ---------------------------------------------------------------------------------------------------- 32 missing authorization
def test_32_no_flag_or_environment_variable_can_enable_real_inference(monkeypatch, tmp_path):
    for name in ("REAL_INFERENCE_AUTHORIZED", "FABRIC_REAL_INFERENCE", "A0I_REAL_INFERENCE", "AUTHORIZE_REAL_INFERENCE"):
        monkeypatch.setenv(name, "1")
    assert cli.main(["run", "--out-dir", str(tmp_path / "x"), "--orderers", "3"]) == 6
    for flag in ("--authorize", "--enable", "--force", "--real"):
        with pytest.raises(SystemExit):
            cli.main(["run", "--out-dir", str(tmp_path / "y"), "--orderers", "3", flag])
    sources = [(REPO / "scripts/e1/e1_a0i_real_inference_smoke.py").read_text(encoding="utf-8"),
               (REPO / "src/fabric_perf/controllers/real_inference_smoke.py").read_text(encoding="utf-8")]
    assert all("os.environ" not in text and "getenv" not in text for text in sources)
    assert sum(1 for line in (REPO / "src/fabric_perf/controllers/stage3_predictive.py").read_text(encoding="utf-8").splitlines()
               if line.startswith("REAL_INFERENCE_AUTHORIZED")) == 1


def test_32b_real_mode_of_execute_smoke_is_refused_and_a_fake_cannot_pose_as_real():
    forecaster = sp.GuardedForecaster(sp.TimestampedFixtureForecaster("FIXTURE_X", 15.0), expected_identity="FIXTURE_X")
    with pytest.raises(sp.RealInferenceNotAuthorized):
        smoke.execute_smoke(run_id="r", mode="real", forecaster=forecaster, performance=object(), history_times=(), history_values=(),
                            history_ref=None, candidates=(), stored_value_tps=None)


def test_32c_even_with_the_gate_open_a_fixture_interface_cannot_run_as_real(monkeypatch):
    monkeypatch.setattr(sp, "REAL_INFERENCE_AUTHORIZED", True)
    forecaster = sp.GuardedForecaster(sp.TimestampedFixtureForecaster("FIXTURE_X", 15.0), expected_identity="FIXTURE_X")
    with pytest.raises(sp.PredictiveIntegrationError, match="real mode needs a GuardedForecaster"):
        smoke.execute_smoke(run_id="r", mode="real", forecaster=forecaster, performance=object(), history_times=(), history_values=(),
                            history_ref=None, candidates=(), stored_value_tps=None)
    posing = sp.GuardedForecaster(sp.TimestampedFixtureForecaster(sp.REAL_FORECAST_IDENTITY, 15.0), expected_identity=sp.REAL_FORECAST_IDENTITY)
    with pytest.raises(sp.PredictiveIntegrationError, match="real mode needs a GuardedPerformancePredictor"):          # a plain fake is not the guarded model
        smoke.execute_smoke(run_id="r", mode="real", forecaster=posing, performance=FakePerformance(), history_times=(), history_values=(),
                            history_ref=None, candidates=(), stored_value_tps=None)


# ---------------------------------------------------------------------------------------------------------- 33 output schema
class FakePerformance:
    """A labelled fixture: scripted numbers behind the real range and candidate checks are NOT applied here."""

    def __init__(self):
        self.calls = 0

    def predict(self, rate, config):
        self.calls += 1
        if config.max_message_count == 45:
            raise sp.UnsupportedExtrapolationError("candidate_block_size_outside_training_grid", "scripted")
        return PerformancePrediction(throughput_tps=50.0, latency_ms=400.0)


def _history_for_fixture(bins=43_200):
    history = ch.CapturedHistory(s.h.binned(), zero_policy=ch.ZERO_POLICY_BENCHMARK_ZERO_FILL)
    history.advance_to(at(bins))
    times, counts, reference = history.store.snapshot(now_utc=at(bins).isoformat())
    return times, counts, reference


def _run_fixture(value=15.75, bins=43_200):
    times, counts, reference = _history_for_fixture(bins)
    forecaster = sp.GuardedForecaster(sp.TimestampedFixtureForecaster("FIXTURE_X", value), expected_identity="FIXTURE_X")
    return smoke.execute_smoke(run_id="fixture-run", mode="fixture", forecaster=forecaster, performance=FakePerformance(), history_times=times,
                               history_values=counts, history_ref=reference,
                               candidates=[FabricConfig(max_message_count=40, orderers=3), FabricConfig(max_message_count=45, orderers=3)], stored_value_tps=15.754735501056683)


def test_33_a_not_run_record_has_every_required_field_and_no_value():
    record = smoke.empty_output("rid")
    assert smoke.validate_output(record) == [] and set(smoke.REQUIRED_OUTPUT_KEYS) <= set(record)
    assert record["inference"] == {"status": "NOT_RUN", "real_inference_performed": False, "error": None}
    assert record["forecast"]["value_tps"] is None and record["candidates"] == [] and record["fabric_execution_occurred"] is False
    assert record["input_history"]["coverage_mode"] == "benchmark_zero_fill" and record["input_history"]["live_quality"] is False
    assert record["model_identity"]["performance_identity"] == sp.REAL_PERFORMANCE_IDENTITY and record["forecast_horizon_seconds"] == 60
    assert any("No Fabric" in line for line in record["statements"]) and any("not independent validation" in line for line in record["statements"])


def test_33b_a_fixture_run_fills_no_genuine_field_and_keeps_its_values_labelled():
    record = _run_fixture()
    assert smoke.validate_output(record) == [] and record["run_identity"]["mode"] == "fixture"
    assert record["forecast"]["value_tps"] is None and record["forecast"]["is_fixture_value"] is True and record["forecast"]["origin_utc"]
    assert all(c["predicted_throughput_tps"] is None and c["predicted_latency_ms"] is None for c in record["candidates"])
    assert record["fixture_values_not_genuine"]["forecast_tps"] == 15.75 and record["fixture_values_not_genuine"]["candidates"] == [
        {"throughput_tps": 50.0, "latency_ms": 400.0}]
    assert record["out_of_grid_status"] == "within_training_grid" and record["input_history"]["bins"] == 43_200 and len(record["input_history"]["digest_sha256"]) == 64
    assert [c["status"] for c in record["candidates"]] == ["OK", "OUT_OF_GRID"] and record["inference"]["status"] == "FAILED"
    assert record["abstention_reason"].startswith("candidate_failures:") and record["stored_reference_comparison"]["within_tolerance"] is False


def test_33c_a_forecast_outside_the_grid_abstains_and_predicts_nothing():
    record = _run_fixture(value=7.5)
    assert record["out_of_grid_status"] == "outside_training_grid" and record["abstention_reason"] == "forecast_outside_training_grid"
    assert record["inference"]["status"] == "ABSTAINED" and record["candidates"] == [] and smoke.validate_output(record) == []


def test_33d_a_short_history_fails_with_the_guard_code_and_no_value():
    record = _run_fixture(bins=300)
    assert record["inference"]["status"] == "FAILED" and record["abstention_reason"] == "forecast_history_insufficient"
    assert record["forecast"]["value_tps"] is None and record["candidates"] == [] and smoke.validate_output(record) == []


def test_33e_the_validator_refuses_fixture_values_presented_as_genuine():
    record = smoke.empty_output("r")
    record["forecast"]["value_tps"] = 15.75
    assert any("without a successful real run" in p for p in smoke.validate_output(record))
    forged = _run_fixture()
    forged["forecast"]["value_tps"] = 15.75                                   # promote the fixture value to a genuine field
    forged["run_identity"]["mode"] = "real"
    forged["inference"] = {"status": "SUCCESS", "real_inference_performed": True, "error": None}
    problems = smoke.validate_output(forged)
    assert any("labelled as, or not declared not to be, a fixture" in p for p in problems)
    relabelled = json.loads(json.dumps(forged))
    relabelled["forecast"]["is_fixture_value"] = False
    relabelled["model_identity"]["performance_identity"] = "FIXTURE_MOCK_PERFORMANCE_MODEL"
    assert any("real model and forecaster identities" in p or "fixture identity" in p for p in smoke.validate_output(relabelled))
    unhashed = json.loads(json.dumps(relabelled))
    unhashed["model_identity"]["performance_identity"] = sp.REAL_PERFORMANCE_IDENTITY
    unhashed["model_identity"]["performance_artifact_sha256"]["throughput_model"] = "0" * 64
    assert any("model hash throughput_model is not the pinned hash" in p for p in smoke.validate_output(unhashed))
    assert any("no input-history digest" in p for p in smoke.validate_output({**unhashed, "input_history": {**unhashed["input_history"], "digest_sha256": None}}))


def test_33f_other_schema_violations_are_reported():
    record = smoke.empty_output("r")
    record["fabric_execution_occurred"] = True
    assert "fabric_execution_occurred must be false" in smoke.validate_output(record)
    record = smoke.empty_output("r")
    del record["candidates"]
    assert smoke.validate_output(record) == ["missing field 'candidates'"]
    record = smoke.empty_output("r")
    record["out_of_grid_status"] = "maybe"
    assert any("out_of_grid_status" in p for p in smoke.validate_output(record))
    record = smoke.empty_output("r")
    record["inference"]["status"] = "SUCCESS"
    assert "status SUCCESS but no value was produced" in smoke.validate_output(record)
    record = smoke.empty_output("r")
    record["input_history"]["live_quality"] = True
    assert any("not live-quality" in p for p in smoke.validate_output(record))
