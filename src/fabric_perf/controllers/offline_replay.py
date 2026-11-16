"""Recorded-data observational replay. No network, broadcast or live actuator.

Fabric observations remain attached to their recorded configuration. Proposals
cannot produce counterfactual performance measurements from a recorded stream.
The workload-only path has no Fabric latency/throughput measurements at all.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import replace
from pathlib import Path

from .adapters import ApprovedMaxMessageCountSpace, FlashbotsSeriesForecaster, HFBTPArtifactPerformancePredictor
from .base import ControllerState, FabricConfig, StaticController
from .harness import RecordingActuator
from .live_integration import IntegratedControllerRunner, TelemetryFrame, load_jsonl_frames
from .strategies import PredictiveController, ReactiveController


def verified_inputs(root: Path, manifest_path: Path, *, required_keys: tuple[str, ...] | None = None) -> dict[str, Path]:
    """Check all pins before deserializing trusted local model artifacts."""
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    paths = {}
    entries = manifest["files"]
    for key in (entries if required_keys is None else required_keys):
        entry = entries[key]
        path = root / entry["path"]
        digest = None
        if path.is_file():
            with path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest().upper()
        if digest != entry["sha256"]:
            raise ValueError(f"replay input missing or SHA-256 mismatch: {path}")
        paths[key] = path
    return paths


def workload_frames(forecaster: FlashbotsSeriesForecaster, config: FabricConfig, *,
                    run_id: str, start: int = 600, samples: int = 10):
    """Use complete day prefixes, at frozen 60-second origins; never future bins."""
    if isinstance(start, bool) or not isinstance(start, int) or start < 600 or start % 60:
        raise ValueError("start must be a 60-second origin at or after 600 seconds")
    if isinstance(samples, bool) or not isinstance(samples, int) or samples < 1:
        raise ValueError("samples must be a positive integer")
    if start + samples * 60 > len(forecaster.values):
        raise ValueError("replay origins must leave a complete horizon within the observation day")
    for index in range(samples):
        origin = start + index * 60
        timestamp = forecaster.frame.iloc[origin]["timestamp_utc"].isoformat()
        history_times = tuple(t.isoformat() for t in forecaster.frame["timestamp_utc"].iloc[:origin])
        history = tuple(forecaster.values[:origin])
        state = ControllerState(timestamp_utc=timestamp, current_config=config,
                                observed_arrival_rate_tps=history[-1], arrival_history_tps=history,
                                arrival_history_timestamps_utc=history_times, telemetry_age_s=1)
        yield TelemetryFrame(run_id, index, timestamp, origin * 1_000_000_000, state,
                             source_timestamps_utc={"flashbots_last_complete_bin": history_times[-1]},
                             freshness_seconds={"flashbots_last_complete_bin": 1},
                             provenance={"kind": "recorded_flashbots_workload_only",
                                         "source": str(forecaster.series_path), "origin_bin": origin,
                                         "configuration_is_offline_reference": True,
                                         "fabric_performance_observed": False})


def run_offline_replay(root: Path, manifest_path: Path, *, source: str = "fabric", start: int = 600,
                       samples: int = 10, run_id: str = "offline-controller-replay",
                       output_dir: Path | None = None, modes: tuple[str, ...] = ("static", "reactive", "predictive"),
                       actuator_mode: str = "shadow", initial_batch_size: int | None = None) -> dict:
    if output_dir is not None and output_dir.exists():
        raise FileExistsError(f"refusing to overwrite {output_dir}")
    if not modes or len(set(modes)) != len(modes) or set(modes) - {"static", "reactive", "predictive"}:
        raise ValueError("modes must select distinct static, reactive or predictive controllers")
    if actuator_mode not in {"shadow", "recording"}:
        raise ValueError("actuator_mode must be shadow or recording")
    keys = ["snapshots"] if source == "fabric" else ["series"]
    if "predictive" in modes:
        keys += ["series", "selection", "model_metadata", "throughput_model", "latency_model"]
    paths = verified_inputs(root, manifest_path, required_keys=tuple(dict.fromkeys(keys)))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))["files"]
    forecaster = None
    if "predictive" in modes:
        forecaster = FlashbotsSeriesForecaster.from_frozen_selection(
            paths["series"], paths["selection"], selection_sha256=manifest["selection"]["sha256"],
            series_sha256=manifest["series"]["sha256"])
    if source == "fabric":
        frames = load_jsonl_frames(paths["snapshots"], run_id=run_id)
        initial = frames[0].state.current_config
        if initial_batch_size is not None and initial_batch_size != initial.max_message_count:
            raise ValueError("Fabric replay must start at its recorded configuration")
        # Preserve genuine recorded byte caps and explicit historical B40/B45 map.
        sizes = (40, 45)
    elif source == "flashbots":
        initial = FabricConfig(max_message_count=10 if initial_batch_size is None else initial_batch_size)
        sizes = (10, 40)  # offline convention only; no live approval implied
        if forecaster is None:
            forecaster = FlashbotsSeriesForecaster(paths["series"], baseline="persistence")
        frames = workload_frames(forecaster, initial, run_id=run_id, start=start, samples=samples)
    else:
        raise ValueError("source must be fabric or flashbots")
    space = ApprovedMaxMessageCountSpace(frozenset(sizes), {size: size for size in sizes}, initial)
    controllers = {}
    if "static" in modes:
        controllers["static"] = StaticController(initial)
    if "reactive" in modes:
        controllers["reactive"] = ReactiveController(initial, congested_config=replace(initial, max_message_count=min(sizes)),
                                       latency_high_ms=1500, latency_low_ms=1200,
                                       consecutive_samples=2, cooldown_samples=3)
    if "predictive" in modes:
        predictor = HFBTPArtifactPerformancePredictor(paths["model_metadata"].parent, candidate_space=space)
        controllers["predictive"] = PredictiveController(predictor, initial, arrival_forecaster=forecaster,
                                           candidate_configs=[replace(initial, max_message_count=size) for size in sizes],
                                           horizon_seconds=60)
    runner = IntegratedControllerRunner(controllers, initial, run_id=run_id, configuration_policy=space)  # shadow only
    dry_run = {name: RecordingActuator(initial) for name in controllers} if actuator_mode == "recording" else None
    result = runner.run_offline(frames, source_kind=source, output_dir=output_dir, recording_actuators=dry_run,
                                input_pins={key: manifest[key] for key in paths})
    result["replay"] = {
        "source": source, "counterfactual_performance_evaluated": False,
        "reactive_profile": "historical no-action B40 profile" if source == "fabric" else "missing Fabric telemetry: abstain",
        "live_fabric_validated": False, "frozen_alpha": 0.005 if "predictive" in modes else None,
        "horizon_seconds": 60 if "predictive" in modes else None,
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest().upper(),
        "code_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest().upper() for path in (
            *[Path(__file__).with_name(name + ".py") for name in (
                "offline_replay", "adapters", "base", "strategies", "harness", "telemetry", "live_integration")],
            Path(__file__).parents[1] / "models" / "artifacts.py")},
        "outcomes": {name: dict(Counter(row["decision"]["kind"] for row in result["records"]
                                        if row["integration"]["strategy"] == name)) for name in controllers},
    }
    # Existing writer refuses overwrite. Write only after replay metadata exists.
    if output_dir is not None:
        # runner.run already wrote the records; metadata gets an exclusive sidecar.
        with (output_dir / "replay_summary.json").open("x", encoding="utf-8") as stream:
            json.dump(result["replay"], stream, indent=2)
            stream.write("\n")
    return result
