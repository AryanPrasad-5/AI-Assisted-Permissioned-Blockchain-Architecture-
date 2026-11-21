"""Specification of ONE bounded real-inference smoke test (task A0i, 2026-11-21). DISABLED. Nothing here loads a model or runs a forecast.

What this is
------------
A written, hash-pinned contract for a single future check that the REAL interfaces (the frozen EWMA forecaster and the frozen HFBTP
random forests) accept the real, hash-verified 2026-09-21 history prefix and return well-formed outputs. This module holds the
contract, the input verification (it hashes files, it never opens a model), the exact command, the output schema with a validator
that refuses fixture values presented as genuine, and ``execute_smoke`` (pure orchestration over INJECTED, already-guarded interfaces).

What this is not
----------------
* It is not authorization. Real inference stays behind ``stage3_predictive.REAL_INFERENCE_AUTHORIZED`` (one ``False`` constant). There is
  no flag, argument or environment variable here that turns it on, and ``run_real_smoke`` refuses before it reads a pin, a model or a file.
* It is not validation. 2026-09-21 12:00Z lies in the half-day (12:00-24:00Z) that was USED FOR MODEL SELECTION, so a match with the
  stored value shows reproducibility of the stored pipeline, never independent accuracy. The history is Ethereum mempool arrivals, not Fabric.
* It never trains, refits, exports or regenerates anything and never runs Fabric, Docker, WSL or Caliper.

The post-authorization wiring (build the real components, feed the history, write the output file) is DELIBERATELY NOT WRITTEN here: code
that loads models should be written, reviewed and hashed together with the authorization change, not sit unexercised behind a constant.
``execute_smoke`` is the part that can be tested offline, and it is tested with labelled fakes whose values are never reported as genuine.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import stage3_predictive as sp
from .base import FabricConfig
from .stage3_predictive import PredictiveIntegrationError, RealInferenceNotAuthorized, UnsupportedExtrapolationError
from .workload_history import prefix_sha256

SPEC_SCHEMA = "real_inference_smoke_spec/1"
OUTPUT_SCHEMA = "real_inference_smoke_output/1"
SMOKE_ID = "a0i-real-inference-smoke-2026-09-21T1200Z"
UTC_DAY = "2026-09-21"
CUTOFF_UTC = "2026-09-21T12:00:00+00:00"
HISTORY_BINS = 43_200                                   # bins [00:00:00, 12:00:00)
HORIZON_SECONDS = 60
EWMA_ALPHA = 0.005
MIN_HISTORY_BINS = 600
MAX_HISTORY_AGE_S = 5.0
COVERAGE_MODE = "benchmark_zero_fill"
STORED_ORIGIN_S = 43_200
STORED_TOLERANCE_ABS = 1e-9
ORDERERS_PLACEHOLDER = "OWNER_MUST_SUPPLY"

# repository-relative path -> SHA-256 (lowercase). The model, series and selection hashes must equal Integration/system_integration/offline_replay_inputs.json.
ARTIFACTS: Mapping[str, tuple[str, str]] = {
    "series": ("data/processed/flashbots/series_2026-09-21_1s.csv.gz", "2d54edd894dd7ccda67d9df81de1a753d7b4cef4b6da0271f5fd418d2a98ffb6"),
    "selection": ("experiments/results/flashbots_horizon_benchmark_v2/select/selection_frozen.json",
                  "888e4a48b59a5c543d9016878a5194266de47eeaabdf0f33f51b10f146f05a72"),
    "model_metadata": ("experiments/artifacts/hfbtp/metadata.json", "e2928cb80f3d5d7d143fcd622fa0500a2c95305ed5a6d90970ee25a0165ae94d"),
    "throughput_model": ("experiments/artifacts/hfbtp/throughput.joblib", "b0ed880ffdc1ac641e0a9f9bafe8d403d92b9c81a2193af5ea3f87204df1b660"),
    "latency_model": ("experiments/artifacts/hfbtp/avg_latency.joblib", "1e7746a293ff95f1d95203d39a602da8be5395a19aba26f9ad655c8ae165e610"),
    "stored_forecast_reference": ("experiments/results/flashbots_horizon_benchmark_v2/select/forecasts_V_H60.csv",
                                  "99d73e43cf2e7d779b56eaa8027ba6b5d30e6f5b71ffde411331802a8b41ab41"),
    "controller_policy": ("Integration/system_integration/a0h_2026-11-21/fixtures/controller_policy_v2_h60.json",
                          "a957058683177530b6b5e5003a91ac218d0bb3bdfde32c152067294bca862d9d"),
}
PIN_FILE = "Integration/system_integration/offline_replay_inputs.json"
PIN_KEYS = ("series", "selection", "model_metadata", "throughput_model", "latency_model")
CLI = "scripts/e1/e1_a0i_real_inference_smoke.py"

GENUINE_FIELDS = ("forecast.value_tps", "candidates[].predicted_throughput_tps", "candidates[].predicted_latency_ms")
REQUIRED_OUTPUT_KEYS = ("schema", "run_identity", "dataset_identity", "model_identity", "forecast_identity", "forecast_horizon_seconds",
                        "input_history", "forecast", "candidates", "out_of_grid_status", "abstention_reason", "inference",
                        "stored_reference_comparison", "fabric_execution_occurred", "statements")
OUT_OF_GRID_STATUSES = ("within_training_grid", "outside_training_grid", "not_evaluated")
STATEMENTS = ("No Fabric, Docker, WSL, Caliper or network operation occurred.",
              "The history is Ethereum mempool arrivals (first-seen times), not Fabric observations.",
              "2026-09-21 12:00Z lies in the half-day used for model selection: this is a reproducibility check, not independent validation.",
              "The input coverage mode is benchmark_zero_fill: it reproduces the stored benchmark preprocessing and is NOT live-quality.",
              "A predicted value is a prediction, not a measured committed throughput or latency.")


class SmokeProcedureNotWired(RuntimeError):
    """The post-authorization wiring is not part of A0i by design."""


# ------------------------------------------------------------------------------------------------------------------ spec
def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:                       # bytes only; nothing is unpickled or loaded
        for chunk in iter(lambda: stream.read(1 << 22), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_command() -> list[str]:
    """The exact command of the future, separately authorized run. Generating it executes nothing, and today it is refused (exit 6)."""
    return [".venv/Scripts/python.exe", CLI, "run", "--out-dir", "<NEW directory that does not exist>", "--orderers", "<owner-verified integer>"]


def build_spec() -> dict[str, Any]:
    """The smoke-test contract as data. Pure; reads no file."""
    return {
        "schema": SPEC_SCHEMA, "smoke_id": SMOKE_ID, "status": "DESIGNED BUT NOT EXECUTED",
        "execution_gate": {"constant": "fabric_perf.controllers.stage3_predictive.REAL_INFERENCE_AUTHORIZED", "value_in_source": False,
                           "how_to_enable": "a reviewed source change by the owner, with a separate computation authorization (PROJECT_RULES.md, Computation boundary); "
                                            "no flag, argument or environment variable exists",
                           "refusal": "the CLI 'run' subcommand exits 6 before reading any file"},
        "computation_class": "Generating model predictions from a frozen model and running the frozen EWMA: both need separate explicit authorization",
        "inputs": {name: {"path": path, "sha256": digest} for name, (path, digest) in ARTIFACTS.items()},
        "pin_file": PIN_FILE, "pin_keys_that_must_equal_the_pin_file": list(PIN_KEYS),
        "input_schema": {"series": "flashbots_series_1s/1: columns timestamp_utc, arrival_count, date_utc, frequency_seconds; 86,400 whole-second UTC bins, "
                                   "one UTC day; arrival_count is a non-negative integer = transactions per 1 s bin",
                         "history_prefix": f"bins [00:00:00, 12:00:00) of {UTC_DAY} = {HISTORY_BINS} bins, contiguous, from midnight",
                         "source_kind": "ethereum_mempool_first_seen (not Fabric)"},
        "observation_cutoff_utc": CUTOFF_UTC, "forecast_origin": "end of the last bin of the prefix = the cutoff",
        "forecast_horizon_seconds": HORIZON_SECONDS,
        "ewma": {"identity": sp.REAL_FORECAST_IDENTITY, "alpha": EWMA_ALPHA, "frequency_seconds": 1, "min_history_bins": MIN_HISTORY_BINS,
                 "max_history_age_s": MAX_HISTORY_AGE_S, "history": "complete UTC-day prefix from 00:00:00; a missing second is never zero-filled by the forecaster"},
        "input_coverage_mode": {"mode": COVERAGE_MODE, "live_quality": False,
                                "why": "the frozen forecast was built from a zero-filled series, so a faithful reproduction needs the same input; strict mode would "
                                       "abstain on the first unverified zero (second 207) and the check would not run (A0h, demonstrated)",
                                "satisfies_a0i_coverage_contract": False},
        "hfbtp_feature_schema": {"id": sp.FEATURE_SCHEMA_ID, "feature_order": list(sp.PERFORMANCE_CONTRACT["feature_order"]),
                                 "raw_inputs": dict(sp.PERFORMANCE_CONTRACT["raw_inputs"]), "targets": list(sp.PERFORMANCE_CONTRACT["targets"]),
                                 "domain": dict(sp.PERFORMANCE_CONTRACT["domain"]), "model_identity": sp.REAL_PERFORMANCE_IDENTITY},
        "candidate_configuration_source": {
            "candidate_max_message_counts": "keys of candidate_hfbtp_block_size_by_max_message_count in the pinned controller_policy file",
            "other_fields": "batch_timeout_ms, preferred_max_bytes, absolute_max_bytes from the same fixture-declared channel configuration (NOT independently verified)",
            "orderers": ORDERERS_PLACEHOLDER,
            "orderers_note": "never defaulted: the Orderers feature must come from a verified channel configuration block (A0g finding F7). The run refuses without it."},
        "stored_reference": {"file": ARTIFACTS["stored_forecast_reference"][0], "column": "yhat_ewma", "origin_s": STORED_ORIGIN_S,
                             "tolerance_abs": STORED_TOLERANCE_ABS,
                             "meaning": "a real EWMA result equal to the stored value shows the stored pipeline is reproducible; a mismatch is a FAILURE to report, not to tune away"},
        "expected_output_fields": list(REQUIRED_OUTPUT_KEYS), "output_schema": OUTPUT_SCHEMA, "genuine_value_fields": list(GENUINE_FIELDS),
        "validity_and_abstention_rules": [
            "history shorter than 600 bins, not a UTC-day prefix, not contiguous, stale or from the future -> inference status FAILED with the guard's code; no value",
            "forecast outside [10, 200] tx/s -> out_of_grid_status outside_training_grid, abstention_reason set, NO prediction is made and nothing is clipped",
            "a candidate that is not approved, off the HFBTP grid or on an absent grid cell -> that candidate fails; the others are reported",
            "a non-finite or negative output -> FAILED; no value is substituted",
            "Orderers missing -> refused before any model is touched"],
        "expected_failure_conditions": ["input hash mismatch", "pin file disagrees with the spec constants", "real inference not authorized (exit 6)",
                                        "Orderers not supplied", "output directory already exists", "stored-reference mismatch beyond tolerance"],
        "evidence_to_preserve": ["the output JSON (new file, never overwritten)", "the hash report of every input", "the exact command and its exit code",
                                 "the run's stdout and stderr", "the SHA-256 of the output file", "git HEAD and status at the time of the run"],
        "prohibitions": ["no training, refitting, export or regeneration of any artifact", "no change to a pinned or frozen file",
                         "no Fabric, Docker, WSL, Caliper, Prometheus or network use", "no use of the output as a controller comparison or a performance claim",
                         "no second history, cutoff or configuration in the same run (single, bounded)"],
        "command": build_command(), "separate_authorization_required": True,
        "non_claims": list(STATEMENTS),
    }


def verify_inputs(repo: Path) -> dict[str, Any]:
    """Hash every input and compare it with the spec and with the pin file. Reads bytes only; opens no model."""
    repo = Path(repo)
    pins = json.loads((repo / PIN_FILE).read_text(encoding="utf-8"))["files"]
    report: dict[str, Any] = {}
    ok = True
    for name, (path, expected) in ARTIFACTS.items():
        target = repo / path
        actual = sha256_file(target) if target.is_file() else None
        pin = pins.get(name) if name in PIN_KEYS else None
        pin_ok = True if pin is None else (pin["path"] == path and pin["sha256"].lower() == expected)
        entry_ok = actual == expected and pin_ok
        report[name] = {"path": path, "expected_sha256": expected, "actual_sha256": actual, "matches_spec": actual == expected,
                        "matches_pin_file": pin_ok if name in PIN_KEYS else "not pinned there"}
        ok = ok and entry_ok
    return {"schema": "real_inference_smoke_input_report/1", "all_ok": ok, "inputs": report,
            "models_loaded": False, "note": "files were hashed as bytes; no joblib, pickle, sklearn or numpy call was made"}


def read_stored_reference(repo: Path) -> float:
    """The stored ``yhat_ewma`` at origin 43,200 s, read as text from the hash-pinned table. Verifies the file first."""
    path, expected = ARTIFACTS["stored_forecast_reference"]
    target = Path(repo) / path
    if sha256_file(target) != expected:
        raise ValueError("the stored forecast table does not match its pinned hash")
    lines = target.read_text(encoding="utf-8").splitlines()
    header = lines[0].split(",")
    origin_col, value_col = header.index("origin_s"), header.index("yhat_ewma")
    for line in lines[1:]:
        cells = line.split(",")
        if int(float(cells[origin_col])) == STORED_ORIGIN_S:
            return float(cells[value_col])
    raise ValueError(f"no stored forecast at origin {STORED_ORIGIN_S}")


# ------------------------------------------------------------------------------------------------------------- the gate
def run_real_smoke(*args: Any, **kwargs: Any) -> None:
    """The only entry to the real run. Refuses while real inference is not authorized, before reading any file, pin or model."""
    if sp.REAL_INFERENCE_AUTHORIZED is not True:
        raise RealInferenceNotAuthorized("real_inference_not_authorized",
                                         "the real-inference smoke test is prepared but disabled; it needs the owner's separate authorization")
    raise SmokeProcedureNotWired("the post-authorization wiring is not part of A0i; it must be written, reviewed and hashed with the authorization change")


# --------------------------------------------------------------------------------------------------------------- output
def _finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def empty_output(run_id: str) -> dict[str, Any]:
    """A NOT_RUN record: every genuine value is null. This is what exists today."""
    return {
        "schema": OUTPUT_SCHEMA, "run_identity": {"run_id": run_id, "smoke_id": SMOKE_ID, "mode": "not_run"},
        "dataset_identity": {"series_path": ARTIFACTS["series"][0], "series_sha256": ARTIFACTS["series"][1], "utc_day": UTC_DAY,
                             "source_kind": "ethereum_mempool_first_seen", "is_fabric_observation": False},
        "model_identity": {"performance_identity": sp.REAL_PERFORMANCE_IDENTITY,
                           "performance_artifact_sha256": {"throughput_model": ARTIFACTS["throughput_model"][1], "latency_model": ARTIFACTS["latency_model"][1],
                                                           "model_metadata": ARTIFACTS["model_metadata"][1]},
                           "feature_schema_id": sp.FEATURE_SCHEMA_ID},
        "forecast_identity": {"identity": sp.REAL_FORECAST_IDENTITY, "selection_sha256": ARTIFACTS["selection"][1], "alpha": EWMA_ALPHA},
        "forecast_horizon_seconds": HORIZON_SECONDS,
        "input_history": {"digest_sha256": None, "bins": None, "start_utc": None, "end_utc": None, "cutoff_utc": CUTOFF_UTC,
                          "coverage_mode": COVERAGE_MODE, "live_quality": False},
        "forecast": {"value_tps": None, "origin_utc": None, "is_fixture_value": False},
        "candidates": [], "out_of_grid_status": "not_evaluated", "abstention_reason": "not_run",
        "inference": {"status": "NOT_RUN", "real_inference_performed": False, "error": None},
        "stored_reference_comparison": {"stored_value_tps": None, "difference": None, "within_tolerance": None, "tolerance_abs": STORED_TOLERANCE_ABS},
        "fabric_execution_occurred": False, "statements": list(STATEMENTS),
    }


def validate_output(record: Mapping[str, Any]) -> list[str]:
    """Problems with an output record (empty = well-formed). A genuine value is accepted only from a successful, real, non-fixture run."""
    problems: list[str] = []
    for key in REQUIRED_OUTPUT_KEYS:
        if key not in record:
            problems.append(f"missing field {key!r}")
    if problems:
        return problems
    if record["schema"] != OUTPUT_SCHEMA:
        problems.append(f"schema is {record['schema']!r}")
    if record["fabric_execution_occurred"] is not False:
        problems.append("fabric_execution_occurred must be false")
    if record["out_of_grid_status"] not in OUT_OF_GRID_STATUSES:
        problems.append(f"out_of_grid_status {record['out_of_grid_status']!r} is not one of {OUT_OF_GRID_STATUSES}")
    inference = record["inference"]
    genuine = []
    if record["forecast"].get("value_tps") is not None:
        genuine.append("forecast.value_tps")
    for index, candidate in enumerate(record["candidates"]):
        for key in ("predicted_throughput_tps", "predicted_latency_ms"):
            if candidate.get(key) is not None:
                genuine.append(f"candidates[{index}].{key}")
    if genuine:
        mode = record["run_identity"].get("mode")
        if mode != "real" or inference.get("status") != "SUCCESS" or inference.get("real_inference_performed") is not True:
            problems.append(f"genuine value(s) {genuine} present without a successful real run (mode {mode!r}, status {inference.get('status')!r})")
        if record["forecast"].get("is_fixture_value") is not False:
            problems.append("a genuine forecast value is labelled as, or not declared not to be, a fixture value")
        if "FIXTURE" in json.dumps([record["model_identity"], record["forecast_identity"]]).upper():
            problems.append("a genuine value carries a fixture identity")
        if record["model_identity"].get("performance_identity") != sp.REAL_PERFORMANCE_IDENTITY or \
                record["forecast_identity"].get("identity") != sp.REAL_FORECAST_IDENTITY:
            problems.append("a genuine value does not carry the real model and forecaster identities")
        for name in ("throughput_model", "latency_model", "model_metadata"):
            if record["model_identity"]["performance_artifact_sha256"].get(name) != ARTIFACTS[name][1]:
                problems.append(f"model hash {name} is not the pinned hash")
        if record["input_history"].get("digest_sha256") is None:
            problems.append("a genuine value has no input-history digest")
        for value in [record["forecast"].get("value_tps")] + [c.get(k) for c in record["candidates"] for k in ("predicted_throughput_tps", "predicted_latency_ms")]:
            if value is not None and (not _finite(value) or value < 0):
                problems.append("a genuine value is not finite and non-negative")
    elif inference.get("status") == "SUCCESS":
        problems.append("status SUCCESS but no value was produced")
    if inference.get("status") in ("FAILED", "ABSTAINED") and not record["abstention_reason"]:
        problems.append("a failed or abstained run needs an abstention_reason")
    if inference.get("status") == "NOT_RUN" and (genuine or inference.get("real_inference_performed")):
        problems.append("NOT_RUN cannot carry values or claim inference")
    if record["input_history"].get("coverage_mode") == COVERAGE_MODE and record["input_history"].get("live_quality") is not False:
        problems.append("benchmark_zero_fill history must be labelled not live-quality")
    return problems


def execute_smoke(*, run_id: str, mode: str, forecaster: Any, performance: Any, history_times: Sequence[str], history_values: Sequence[float],
                  history_ref: Any, candidates: Sequence[FabricConfig], stored_value_tps: float | None) -> dict[str, Any]:
    """Orchestrate ONE smoke run over INJECTED interfaces (a ``GuardedForecaster`` and a ``GuardedPerformancePredictor``, or labelled fakes).

    ``mode="real"`` is for guarded real interfaces after authorization; ``mode="fixture"`` is for tests with fakes, and then no genuine
    field is populated: the values go to ``fixture_values_not_genuine`` instead. Nothing is loaded, trained or written here.
    """
    if mode not in ("real", "fixture"):
        raise ValueError("mode must be 'real' or 'fixture'")
    if mode == "real":
        # A fake can never pose as the real interfaces: real mode needs the authorization constant AND guarded interfaces carrying the real identities.
        if sp.REAL_INFERENCE_AUTHORIZED is not True:
            raise RealInferenceNotAuthorized("real_inference_not_authorized", "mode 'real' is refused while real inference is not authorized")
        if not isinstance(forecaster, sp.GuardedForecaster) or forecaster.expected_identity != sp.REAL_FORECAST_IDENTITY:
            raise PredictiveIntegrationError("real_mode_interface_invalid", "real mode needs a GuardedForecaster with the real forecast identity")
        if not isinstance(performance, sp.GuardedPerformancePredictor) or performance.expected_identity != sp.REAL_PERFORMANCE_IDENTITY:
            raise PredictiveIntegrationError("real_mode_interface_invalid", "real mode needs a GuardedPerformancePredictor with the real model identity")
    record = empty_output(run_id)
    record["run_identity"]["mode"] = mode
    record["inference"] = {"status": "FAILED", "real_inference_performed": mode == "real", "error": None}
    times, values = tuple(history_times), tuple(history_values)
    if times:
        record["input_history"].update({"digest_sha256": prefix_sha256(times, values), "bins": len(times), "start_utc": times[0], "end_utc": times[-1]})
    fixture_values: dict[str, Any] = {}
    try:
        result = forecaster.predict_timestamped(times, values, HORIZON_SECONDS, now_utc=CUTOFF_UTC, max_history_age_s=MAX_HISTORY_AGE_S,
                                                history_ref=history_ref)
    except PredictiveIntegrationError as exc:
        record["inference"]["error"] = str(exc)
        record["abstention_reason"] = getattr(exc, "code", "forecast_failure")
        return record
    except ValueError as exc:                                   # the frozen forecaster raises ValueError for malformed history
        record["inference"]["error"] = str(exc)
        record["abstention_reason"] = "forecast_failure"
        return record
    value = result.value_tps
    record["forecast"].update({"origin_utc": result.origin_timestamp_utc, "is_fixture_value": mode == "fixture"})
    if mode == "real":
        record["forecast"]["value_tps"] = value
    else:
        fixture_values["forecast_tps"] = value
    if stored_value_tps is not None:
        difference = value - stored_value_tps
        record["stored_reference_comparison"].update({"stored_value_tps": stored_value_tps, "difference": difference,
                                                      "within_tolerance": abs(difference) <= STORED_TOLERANCE_ABS})
    lo, hi = sp.PERFORMANCE_CONTRACT["domain"]["arrival_rate_tps"]
    if not lo <= value <= hi:
        record["out_of_grid_status"] = "outside_training_grid"
        record["abstention_reason"] = "forecast_outside_training_grid"
        record["inference"]["status"] = "ABSTAINED"
        if fixture_values:
            record["fixture_values_not_genuine"] = fixture_values
        return record
    record["out_of_grid_status"] = "within_training_grid"
    failures = 0
    for config in candidates:
        entry: dict[str, Any] = {"config": config.as_dict() if hasattr(config, "as_dict") else dict(vars(config)), "status": "OK",
                                 "predicted_throughput_tps": None, "predicted_latency_ms": None, "abstention_reason": None}
        try:
            prediction = performance.predict(value, config)
        except UnsupportedExtrapolationError as exc:
            entry.update(status="OUT_OF_GRID", abstention_reason=exc.code)
            failures += 1
        except PredictiveIntegrationError as exc:
            entry.update(status="FAILED", abstention_reason=exc.code)
            failures += 1
        else:
            if mode == "real":
                entry.update(predicted_throughput_tps=prediction.throughput_tps, predicted_latency_ms=prediction.latency_ms)
            else:
                fixture_values.setdefault("candidates", []).append({"throughput_tps": prediction.throughput_tps, "latency_ms": prediction.latency_ms})
        record["candidates"].append(entry)
    record["inference"]["status"] = "SUCCESS" if failures == 0 and candidates else "FAILED"
    if failures:
        record["abstention_reason"] = "candidate_failures:" + ",".join(c["abstention_reason"] for c in record["candidates"] if c["abstention_reason"])
    if fixture_values:
        record["fixture_values_not_genuine"] = fixture_values
    return record
