"""Task A0c (2026-11-21): the client-telemetry chain, end to end and offline.

    v2 recorder (real JS, fake Caliper dispatch)  ->  live CSV  ->  ContinuousCaliperLedgerAdapter
        ->  ProductionTelemetryProvider (with a fake Prometheus endpoint)  ->  IntegratedControllerRunner + Stage 3 contract
        ->  static / reactive / predictive shadow records

What this does and does not show. It shows that the pieces connect and that their provenance, time bases and fail-safe
behaviour hold together. It does NOT show behaviour against Fabric, Caliper, Docker or WSL: the recorder is driven by a test
double, the Prometheus endpoint is a fixture, the ledger stream is a fixture, and the performance model and forecaster are
fakes (no frozen model is loaded). The ledger CSV in particular is written by the test, because no incremental ledger
producer exists in the repository (see the A0c report, blocker B-ledger).
"""

import json
import os
import shutil
import subprocess
import time
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from fabric_perf.controllers import (
    ContinuousCaliperLedgerAdapter,
    FabricConfig,
    IntegratedControllerRunner,
    PredictiveController,
    ProductionTelemetryProvider,
    PrometheusTelemetryAdapter,
    ReactiveController,
    StaticController,
)
from fabric_perf.controllers import live_foundation
from fabric_perf.controllers.base import PerformancePrediction
from fabric_perf.controllers.harness import ShadowActuator
from fabric_perf.controllers.stage3_frame_quality import Stage3FrameQualityContract

REPO = Path(__file__).resolve().parents[1]
NODE = shutil.which("node")
DRIVER = REPO / "tests" / "e1" / "js" / "a0c_live_round.js"
CHANNEL = "ch"
CONFIG = FabricConfig(max_message_count=45)
THREE = {"static", "reactive", "predictive"}
LEDGER_HEADER = "tx_id,block,index,header_type,validation_code,env_bytes,channel_id,ch_ts_ms\n"


class FakeModel:
    def predict(self, arrival, config):
        return PerformancePrediction(10.0, 100.0)


class FakeForecaster:
    def predict(self, history, horizon):
        return 77.0


def iso(epoch_ms: float) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds")


def ledger_rows(tx_ids, at_ms):
    return "".join(f"{tx},{i // 10},{i % 10},3,0,100,{CHANNEL},{int(at_ms)}\n" for i, tx in enumerate(tx_ids))


def build_runner(limit):
    controllers = {
        "static": StaticController(CONFIG),
        "reactive": ReactiveController(CONFIG, congested_config=replace(CONFIG, max_message_count=40), latency_high_ms=1e9,
                                       latency_low_ms=1.0, consecutive_samples=2, cooldown_samples=0),
        "predictive": PredictiveController(FakeModel(), CONFIG, arrival_forecaster=FakeForecaster(),
                                           candidate_configs=[CONFIG, replace(CONFIG, max_message_count=40)]),
    }
    return IntegratedControllerRunner(controllers, CONFIG, run_id="r", invalid_frame_policy="abstain",
                                      frame_quality=Stage3FrameQualityContract(max_consecutive_abstentions=limit))


@pytest.mark.skipif(NODE is None, reason="Node.js not on PATH: end-to-end recorder chain NOT VERIFIED")
def test_recorder_to_adapter_to_provider_to_three_controllers(tmp_path):
    t0 = int(time.time() * 1000)
    out = tmp_path / "out"
    done = subprocess.run([NODE, str(DRIVER), str(out), "RUNE", "midround"], capture_output=True, text=True, timeout=60,
                          env={**os.environ, "A0C_T0": str(t0), "A0C_SUBMIT_GAP_MS": "5"})
    assert done.returncode == 0, done.stderr
    result = json.loads(done.stdout.strip().splitlines()[-1])
    before, after = result["before_deactivate"], result["after_deactivate"]
    base = out / "RUNE_w0_r1_m-RUNE"
    rows_visible_mid_round = len(before["live_csv_lines"]) - 1
    assert rows_visible_mid_round == 7 and before["final_csv"] is False                    # genuinely continuous, not round-end

    recorder, submits, ledger = tmp_path / "recorder.csv", tmp_path / "submits.csv", tmp_path / "ledger_tx.csv"
    recorder.write_text("\n".join(before["live_csv_lines"]) + "\n", encoding="utf-8")        # the stream as it was mid-round
    submits.write_text("\n".join(after["live_submits_lines"]) + "\n", encoding="utf-8")
    ledger.write_text(LEDGER_HEADER + ledger_rows([f"tx{i}" for i in range(7)], t0 + 2000), encoding="utf-8")

    adapter = ContinuousCaliperLedgerAdapter(recorder_csv=recorder, submits_csv=submits, ledger_csv=ledger, channel_id=CHANNEL,
                                             run_id="r", window_s=5, recorder_latency="derive_from_timestamps")
    prometheus = PrometheusTelemetryAdapter({"orderer1": "http://orderer1/metrics"},
                                            fetch=lambda url, timeout: f'consensus_etcdraft_is_leader{{channel="{CHANNEL}"}} 1\n')

    def history():
        obs = obs_ms[0]
        return (iso(obs - 2000), iso(obs - 1000)), (10.0, 12.0)

    obs_ms = [0.0]
    provider = ProductionTelemetryProvider(prometheus, CHANNEL, config_reader=lambda: CONFIG, continuous_source=adapter,
                                           arrival_history_reader=history, required_fields=(),
                                           required_prometheus_metrics=("raft_leader_count",))
    runner = build_runner(limit=2)

    def collect(index, at_ms):
        obs_ms[0] = at_ms
        return provider.collect(run_id="r", decision_index=index, observation_timestamp_utc=iso(at_ms))

    frames = []
    frames.append(collect(0, t0 + 3000))                                                    # first look: no ledger interval yet
    recorder.write_text("\n".join(after["live_csv_lines"]) + "\n", encoding="utf-8")          # the rest of the round
    with ledger.open("a", encoding="utf-8") as fh:
        fh.write(ledger_rows([f"tx{i}" for i in range(7, 10)], t0 + 3500))
    frames.append(collect(1, t0 + 4000))
    frames.append(collect(2, t0 + 30_000))                                                  # nothing new for 26 s
    frames.append(collect(3, t0 + 31_000))
    outcome = runner.run(frames)

    # frame 0: a delta needs two observations, so committed throughput is absent: every controller abstains.
    assert "observed_throughput_tps" in frames[0].critical_missing or frames[0].state.observed_throughput_tps is None
    startup = [r for r in outcome["records"] if r["integration"]["decision_index"] == 0]
    assert {r["decision"]["outcome"] for r in startup} == {"abstain"} and len(startup) == 3

    # frame 1: complete, contemporaneous client telemetry reaches all three controllers identically.
    good = [r for r in outcome["records"] if r["integration"]["decision_index"] == 1]
    assert {r["integration"]["strategy"] for r in good} == THREE
    assert len({r["integration"]["input_digest"] for r in good}) == 1
    live = [row.split(",") for row in after["live_csv_lines"][1:]]
    successes = [float(r[4]) for r in live if r[6] == "success"]
    state = frames[1].state
    assert state.observed_latency_ms == pytest.approx(sum(successes) / len(successes))       # measured, from the recorder rows
    assert state.observed_latency_p95_ms == sorted(successes)[-1]                            # nearest-rank p95 of 9 samples
    assert state.observed_throughput_tps == pytest.approx(3.0)                               # 3 newly seen valid rows in 1 s
    assert state.queue_depth == 0.0 and state.failed_transactions == 1
    provenance = frames[1].provenance
    health = provenance["continuous_caliper_health"]
    assert health["latency"]["source_mode"] == "derive_from_timestamps" and health["latency"]["eligible_samples_in_window"] == 9
    assert health["integrity"]["status"] == "OK" and health["totals"]["completed"] == 10
    assert provenance["continuous_caliper_source"]["recorder"] == str(recorder)
    latency_metric = provenance["metrics"]["latency_mean_ms"]
    assert latency_metric["source"] == "Caliper recorder CSV" and latency_metric["run_id"] == "r"
    assert latency_metric["observation_timestamp_utc"] == iso(t0 + 4000)                      # collection time
    assert latency_metric["timestamp_utc"] == iso(max(int(r[3]) for r in live))               # event time of the newest completion
    assert latency_metric["window_end_utc"] == latency_metric["observation_timestamp_utc"]
    for record in good:                                                                       # shadow only, nothing applied
        assert record["integration"]["shadow_mode"] is True and record["integration"]["live_submission_enabled"] is False
        assert record["configuration_state"]["proposal_applied"] is False
        assert record["configuration_state"]["after"]["max_message_count"] == 45
    predictive = next(r for r in good if r["integration"]["strategy"] == "predictive")
    assert predictive["decision"]["predicted_arrival_rate_tps"] == 77.0                       # forecast
    assert predictive["state"]["observed_arrival_rate_tps"] == pytest.approx(state.observed_arrival_rate_tps)   # observed, different

    # frame 2 and 3: the data is 26 s old. The window is empty, so latency is missing, not stale-but-healthy and not zero.
    assert frames[2].state.observed_latency_ms is None
    assert provenance_missing(frames[2], "latency_mean_ms") == "no completed Caliper records in measurement window"
    stale = [r for r in outcome["records"] if r["integration"]["decision_index"] in (2, 3)]
    assert {r["decision"]["outcome"] for r in stale} == {"abstain"} and len(stale) == 6
    # two consecutive abstentions (frames 2 and 3) reach the configured limit of 2 and end the run.
    assert outcome["stop_reasons"] == ["consecutive_abstention_limit_reached"]
    assert outcome["longest_consecutive_abstention_run"] == 2 and outcome["frame_count"] == 4


def provenance_missing(frame, metric):
    return frame.provenance["metrics"][metric]["missing_reason"]


# ============================================================ the factory passes the contract through
class _Workload:
    source = "fixture-workload"

    def history(self):
        return (), ()

    def provenance(self, *_):
        return {"source": self.source}


class _FakePredictor:
    def __init__(self, directory, candidate_space=None):
        self.directory = directory

    def validate_config(self, config):
        return None

    def predict(self, arrival, config):
        return PerformancePrediction(10.0, 100.0)


def foundation(tmp_path, monkeypatch, **kwargs):
    for key in ("selection", "model_metadata", "throughput_model", "latency_model"):
        (tmp_path / f"{key}.json").write_text("{}", encoding="utf-8")
    (tmp_path / "selection.json").write_text(json.dumps({"horizons": {"60": {"selected_model": "ewma", "alpha": 0.005}},
                                                         "min_history_s": 600}), encoding="utf-8")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"files": {k: {"sha256": "0" * 64} for k in
                                              ("selection", "model_metadata", "throughput_model", "latency_model")}}), encoding="utf-8")
    monkeypatch.setattr(live_foundation, "verified_inputs",
                        lambda root, manifest_path, required_keys: {k: tmp_path / f"{k}.json" for k in required_keys})
    monkeypatch.setattr(live_foundation, "HFBTPArtifactPerformancePredictor", _FakePredictor)
    return live_foundation.make_shadow_foundation(
        root=tmp_path, manifest_path=manifest, endpoints={"o": "http://o/metrics"}, channel_id=CHANNEL, current_config=CONFIG,
        config_reader=lambda: CONFIG, workload=_Workload(), candidate_map={40: 40, 45: 45}, latency_high_ms=1500.0,
        latency_low_ms=1200.0, congested_max_message_count=40, consecutive_samples=2, **kwargs)


def test_the_foundation_passes_the_contract_to_the_runner_and_stays_shadow_only(tmp_path, monkeypatch):
    contract = Stage3FrameQualityContract(max_consecutive_abstentions=5)
    provider, runner = foundation(tmp_path, monkeypatch, invalid_frame_policy="abstain", frame_quality=contract)
    assert runner.frame_quality is contract and runner.invalid_frame_policy == "abstain" and runner.shadow is True
    assert all(isinstance(a, ShadowActuator) for a in runner.actuators.values())
    assert provider.required_fields == ()                                   # the provider is unchanged; the runner enforces


def test_without_a_contract_the_foundation_is_exactly_what_it_was(tmp_path, monkeypatch):
    provider, runner = foundation(tmp_path, monkeypatch)
    assert runner.frame_quality is None and runner.invalid_frame_policy == "stop" and runner.required_fields == ()


def test_the_foundation_refuses_a_contract_with_the_default_stop_policy(tmp_path, monkeypatch):
    with pytest.raises(ValueError, match="requires invalid_frame_policy='abstain'"):
        foundation(tmp_path, monkeypatch, frame_quality=Stage3FrameQualityContract(max_consecutive_abstentions=5))
