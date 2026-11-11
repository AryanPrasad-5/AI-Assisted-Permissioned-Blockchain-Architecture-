from datetime import datetime, timedelta, timezone
import json

from fabric_perf.controllers import (
    ContinuousCaliperLedgerAdapter,
    FabricConfig,
    IntegratedControllerRunner,
    ProductionTelemetryProvider,
    PrometheusTelemetryAdapter,
    StaticController,
)


PROM = """# TYPE consensus_etcdraft_is_leader gauge
consensus_etcdraft_is_leader{channel=\"e1-cfg001\"} 1
# TYPE blockcutter_block_fill_duration_sum counter
blockcutter_block_fill_duration_sum{channel=\"e1-cfg001\"} 1
# TYPE blockcutter_block_fill_duration_count counter
blockcutter_block_fill_duration_count{channel=\"e1-cfg001\"} 1
"""


def iso(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def write_sources(tmp_path, now):
    recorder = tmp_path / "recorder.csv"
    submits = tmp_path / "recorder.submits.csv"
    ledger = tmp_path / "ledger_tx.csv"
    recorder.write_text(
        "tx_id,seq_finish,create_ms,final_ms,latency_ms,status\n"
        f"a,0,{ms(now - timedelta(seconds=4))},{ms(now - timedelta(seconds=3))},100,success\n"
        f"b,1,{ms(now - timedelta(seconds=3))},{ms(now - timedelta(seconds=2))},300,success\n",
        encoding="utf-8",
    )
    submits.write_text(
        "seq_submit,t_submit_evt_ms,count\n"
        f"0,{ms(now - timedelta(seconds=4))},1\n"
        f"1,{ms(now - timedelta(seconds=3))},1\n",
        encoding="utf-8",
    )
    ledger.write_text(
        "tx_id,block,index,header_type,validation_code,env_bytes,channel_id,ch_ts_ms\n"
        f"a,10,0,3,0,100,e1-cfg001,{ms(now - timedelta(seconds=3))}\n"
        f"b,10,1,3,0,100,e1-cfg001,{ms(now - timedelta(seconds=2))}\n",
        encoding="utf-8",
    )
    return recorder, submits, ledger


def adapter(tmp_path, now):
    paths = write_sources(tmp_path, now)
    return ContinuousCaliperLedgerAdapter(
        recorder_csv=paths[0], submits_csv=paths[1], ledger_csv=paths[2],
        channel_id="e1-cfg001", run_id="continuous-test", window_s=5,
    ), paths


def test_continuous_adapter_preserves_windows_and_does_not_invent_commit_time(tmp_path):
    now = datetime.now(timezone.utc)
    source, _ = adapter(tmp_path, now)
    first = source.collect(observation_timestamp_utc=iso(now))
    assert first.metric("offered_tps").value == 2.0
    assert first.metric("latency_mean_ms").value == 200.0
    assert first.metric("latency_p95_ms").value == 300.0
    assert first.metric("inflight_backlog").value == 0.0
    assert first.metric("delivered_valid_tps").value is None
    assert first.metric("delivered_valid_tps").missing_reason == "no newly observed ledger interval"
    assert first.metric("delivered_valid_tps").timestamp_utc == iso(now).replace("Z", "+00:00")
    assert first.health["cross_source"]["ledger_header_timestamp_is_not_commit_timestamp"] is True
    assert first.metric("latency_p95_ms").run_id == "continuous-test"
    assert first.metric("latency_p95_ms").window_end_utc == iso(now).replace("Z", "+00:00")


def test_continuous_adapter_cross_validates_statuses_and_duplicates(tmp_path):
    now = datetime.now(timezone.utc)
    source, paths = adapter(tmp_path, now)
    first = source.collect(observation_timestamp_utc=iso(now))
    assert first.health["cross_source"]["status_validation_contradictions"] == []
    with paths[2].open("a", encoding="utf-8") as fh:
        fh.write(f"a,10,2,3,1,100,e1-cfg001,{ms(now)}\n")
    second = source.collect(observation_timestamp_utc=iso(now + timedelta(seconds=1)))
    assert second.health["cross_source"]["duplicate_ids"] == 1


def test_provider_uses_continuous_source_and_rejects_first_incomplete_delivery_interval(tmp_path):
    now = datetime.now(timezone.utc)
    source, _ = adapter(tmp_path, now)

    def fetch(_url, _timeout):
        return PROM

    provider = ProductionTelemetryProvider(
        PrometheusTelemetryAdapter({"orderer1": "http://orderer1/metrics"}, fetch=fetch),
        "e1-cfg001", config_reader=lambda: FabricConfig(max_message_count=40),
        continuous_source=source,
    )
    first = provider.collect(run_id="continuous-test", decision_index=0, observation_timestamp_utc=iso(now))
    assert "observed_throughput_tps" in first.critical_missing
    with (tmp_path / "recorder.submits.csv").open("a", encoding="utf-8") as fh:
        fh.write(f"2,{ms(now + timedelta(seconds=1))},1\n")
    with (tmp_path / "recorder.csv").open("a", encoding="utf-8") as fh:
        fh.write(f"c,2,{ms(now)},{ms(now + timedelta(seconds=1))},200,success\n")
    with (tmp_path / "ledger_tx.csv").open("a", encoding="utf-8") as fh:
        fh.write(f"c,11,0,3,0,100,e1-cfg001,{ms(now + timedelta(seconds=1))}\n")
    second = provider.collect(run_id="continuous-test", decision_index=1, observation_timestamp_utc=iso(now + timedelta(seconds=1)))
    assert second.critical_missing == ()
    assert second.state.observed_latency_p95_ms == 300.0
    assert second.state.observed_throughput_tps == 1.0
    assert second.provenance["continuous_caliper_source"]["ledger"].endswith("ledger_tx.csv")


def test_missing_continuous_sources_are_explicit(tmp_path):
    now = datetime.now(timezone.utc)
    source = ContinuousCaliperLedgerAdapter(
        recorder_csv=tmp_path / "missing-recorder.csv", submits_csv=tmp_path / "missing-submits.csv",
        ledger_csv=tmp_path / "missing-ledger.csv", channel_id="e1-cfg001", run_id="missing-test",
    )
    snapshot = source.collect(observation_timestamp_utc=iso(now))
    assert snapshot.metric("offered_tps").value is None
    assert snapshot.metric("latency_p95_ms").value is None
    assert len(snapshot.health["read_errors"]) == 3


def test_bounded_provider_stream_reaches_common_shadow_runner(tmp_path):
    now = datetime.now(timezone.utc)
    source, _ = adapter(tmp_path, now)
    provider = ProductionTelemetryProvider(
        PrometheusTelemetryAdapter({"orderer1": "http://orderer1/metrics"}, fetch=lambda *_: PROM),
        "e1-cfg001", config_reader=lambda: FabricConfig(max_message_count=40),
        continuous_source=source, required_fields=(),
    )
    config = FabricConfig(max_message_count=40)
    runner = IntegratedControllerRunner(
        {"static": StaticController(config), "reactive": StaticController(config), "predictive": StaticController(config)},
        config, run_id="continuous-test", required_fields=(), shadow=True,
    )
    result = runner.run_provider(provider, samples=2)
    assert result["frame_count"] == 2
    assert result["record_count"] == 6
    assert result["identical_input_digests"] is True
    assert result["live_submission_enabled"] is False
