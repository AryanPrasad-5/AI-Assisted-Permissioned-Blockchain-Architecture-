import json
from pathlib import Path

import pytest

from fabric_perf.controllers.adapters import FlashbotsSeriesForecaster
from fabric_perf.controllers.telemetry import (
    PrometheusTelemetryAdapter,
    TelemetryDataError,
    caliper_snapshot_from_summary,
)


PROM = """# TYPE consensus_etcdraft_is_leader gauge
consensus_etcdraft_is_leader{channel=\"e1-cfg001\"} 1
# TYPE blockcutter_block_fill_duration_sum counter
blockcutter_block_fill_duration_sum{channel=\"e1-cfg001\"} 2.0
# TYPE blockcutter_block_fill_duration_count counter
blockcutter_block_fill_duration_count{channel=\"e1-cfg001\"} 1
"""


def test_prometheus_adapter_records_provenance_and_missing_client_metrics():
    counter = [PROM, PROM.replace("2.0", "4.0").replace("duration_count{channel=\"e1-cfg001\"} 1", "duration_count{channel=\"e1-cfg001\"} 2")]

    def fetch(_url, _timeout):
        return counter.pop(0)

    adapter = PrometheusTelemetryAdapter({"orderer1": "http://example/metrics"}, fetch=fetch)
    first = adapter.collect("e1-cfg001", timestamp_utc="2026-11-11T00:00:00Z")
    second = adapter.collect("e1-cfg001", timestamp_utc="2026-11-11T00:00:01Z")
    assert first.metric("raft_leader_count").value == 1
    assert second.metric("block_fill_duration_s").value == 2.0
    assert second.metric("block_fill_duration_s").unit == "s"
    assert second.metric("block_fill_duration_s").source.startswith("Fabric Prometheus")
    assert first.metric("latency_mean_ms").value is None
    assert "client transaction telemetry" in first.metric("latency_mean_ms").missing_reason


def test_snapshot_rejects_stale_observations_at_controller_boundary():
    adapter = PrometheusTelemetryAdapter({"orderer1": "http://example/metrics"}, fetch=lambda *_: PROM)
    snapshot = adapter.collect("e1-cfg001", timestamp_utc="2026-11-11T00:00:00Z")
    with pytest.raises(TelemetryDataError, match="stale telemetry"):
        snapshot.to_controller_state(now_utc="2026-11-11T00:00:06Z")


def test_prometheus_adapter_rejects_invalid_sample():
    adapter = PrometheusTelemetryAdapter({"orderer1": "http://example/metrics"}, fetch=lambda *_: "metric nope\n")
    snapshot = adapter.collect("e1-cfg001")
    assert snapshot.health["endpoint_errors"]["orderer1"].startswith("TelemetryDataError")
    assert snapshot.metric("latency_mean_ms").value is None


def test_caliper_summary_requires_provenance_timestamp(tmp_path):
    path = tmp_path / "summary.json"
    path.write_text(json.dumps({"metrics": {}, "counts": {}, "inputs": {}}), encoding="utf-8")
    with pytest.raises(TelemetryDataError):
        caliper_snapshot_from_summary(path)


def test_flashbots_series_forecaster_is_chronological_and_reports_identity(tmp_path):
    path = tmp_path / "series.csv"
    path.write_text(
        "timestamp_utc,arrival_count,date_utc,frequency_seconds\n"
        "2026-09-21 00:00:00+00:00,10,2026-09-21,1\n"
        "2026-09-21 00:00:01+00:00,20,2026-09-21,1\n"
        "2026-09-21 00:00:02+00:00,30,2026-09-21,1\n"
        "2026-09-21 00:00:03+00:00,40,2026-09-21,1\n",
        encoding="utf-8",
    )
    forecaster = FlashbotsSeriesForecaster(path, baseline="persistence")
    result = forecaster.forecast_at(3, 1)
    assert result.value_tps == 30
    assert result.model_identity == "flashbots-baseline:persistence:v1:1s"
    assert result.inference_duration_ms >= 0
    fresh = forecaster.predict_timestamped(
        ("2026-11-11T00:00:00Z",), (30,), 1,
        now_utc="2026-11-11T00:00:01Z", max_history_age_s=2,
    )
    assert fresh.origin_timestamp_utc == "2026-11-11T00:00:00Z"
    with pytest.raises(ValueError, match="stale"):
        forecaster.predict_timestamped(
            ("2026-11-11T00:00:00Z",), (30,), 1,
            now_utc="2026-11-11T00:00:06Z", max_history_age_s=2,
        )
    with pytest.raises(ValueError):
        forecaster.predict((10, 20), 0)
