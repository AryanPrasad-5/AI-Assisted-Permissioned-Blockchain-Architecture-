"""Live-shadow fix 3 (2026-11-17): adapter read errors are preserved, classified and surfaced.

A failed read must never reach a controller as a healthy frame. Each failure keeps its category, the
original exception type and sanitized context; each frame has one status (SUCCESSFUL, PARTIAL, FAILED,
INVALID); the context survives into the controller abstention record. No value is ever substituted.
"""

import http.client
import json
import socket
import urllib.error
from datetime import datetime, timedelta, timezone

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
    TelemetryQualityError,
)
from fabric_perf.controllers.base import PerformancePrediction
from fabric_perf.controllers.telemetry_errors import (
    CATEGORIES,
    TelemetryReadFailure,
    aggregate_read_status,
    bounded_details,
    classify_exception,
    failure_from_category,
    failure_from_exception,
    sanitize_text,
    sanitize_url,
)

CHANNEL = "ch"
CONFIG = FabricConfig(max_message_count=45)
OBS = "2026-11-17T12:00:00.000+00:00"
GOOD = f'consensus_etcdraft_is_leader{{channel="{CHANNEL}"}} 1\n'


def leader(value):
    return f'consensus_etcdraft_is_leader{{channel="{CHANNEL}"}} {value}\n'


def prom(fetch, endpoints=None):
    return PrometheusTelemetryAdapter(endpoints or {"orderer1": "http://orderer1/metrics"}, fetch=fetch)


def provider(adapter, **kwargs):
    kwargs.setdefault("required_fields", ())
    return ProductionTelemetryProvider(adapter, CHANNEL, config_reader=lambda: CONFIG, **kwargs)


def raising(exc):
    def fetch(*_):
        raise exc
    return fetch


def collect(adapter_or_provider, index=0, observed=OBS):
    return adapter_or_provider.collect(run_id="r", decision_index=index, observation_timestamp_utc=observed)


# ------------------------------------------------------------- Prometheus: categories
@pytest.mark.parametrize("exc, category", [
    (TimeoutError("timed out after 2.0s"), "timeout"),
    (socket.timeout("read timed out"), "timeout"),
    (urllib.error.URLError(TimeoutError("timed out")), "timeout"),
    (ConnectionRefusedError(111, "Connection refused"), "connection_refused"),
    (urllib.error.URLError(ConnectionRefusedError(111, "Connection refused")), "connection_refused"),
    (urllib.error.URLError("[Errno 111] Connection refused"), "connection_refused"),
    (ConnectionResetError(104, "reset"), "connection_error"),
    (http.client.RemoteDisconnected("closed"), "connection_error"),
    (OSError(101, "Network is unreachable"), "connection_error"),
    (urllib.error.HTTPError("http://orderer1/metrics", 503, "Service Unavailable", {}, None), "http_error"),
])
def test_endpoint_failures_keep_their_category_type_and_context(exc, category):
    snapshot = prom(raising(exc)).collect(CHANNEL, timestamp_utc=OBS)
    detail = snapshot.health["endpoint_error_details"]["orderer1"]
    assert detail["category"] == category and detail["error_type"] == type(exc).__name__
    assert detail["source"] == "prometheus:orderer1" and detail["endpoint"] == "http://orderer1/metrics"
    assert detail["outcome"] == "failed"
    assert snapshot.health["read_status"]["status"] == "FAILED"
    assert snapshot.health["endpoint_errors"]["orderer1"].startswith(type(exc).__name__)    # legacy string form kept
    assert snapshot.metric("orderer_peer_health").value == 0.0
    assert snapshot.metric("raft_leader_count").value is None                              # not a fabricated zero
    if category == "http_error":
        assert detail["http_status"] == 503


def test_timeout_reaches_the_frame_with_a_read_failed_abstention_reason():
    frame = collect(provider(prom(raising(TimeoutError("timed out")))))
    assert "endpoint_read_error:orderer1:timeout" in frame.critical_missing and "orderer_peer_health" in frame.critical_missing
    assert frame.provenance["read_status"]["status"] == "FAILED"
    item = frame.provenance["read_failures"]["items"][0]
    assert (item["source"], item["category"], item["error_type"]) == ("prometheus:orderer1", "timeout", "TimeoutError")
    with pytest.raises(TelemetryQualityError) as caught:
        frame.validate()
    assert caught.value.abstention_reason == "telemetry_read_failed_fail_safe"
    assert caught.value.reason_code == "critical_telemetry_missing"


@pytest.mark.parametrize("body, category, status", [
    ("metric nope\n", "malformed_response", "INVALID"),
    ("metric NaN\n", "malformed_response", "INVALID"),
    ("metric 1 1700000000000\n", "malformed_response", "INVALID"),
    ("", "empty_response", "INVALID"),
])
def test_malformed_or_empty_responses_are_invalid_not_failed_and_not_healthy(body, category, status):
    snapshot = prom(lambda *_: body).collect(CHANNEL, timestamp_utc=OBS)
    detail = snapshot.health["endpoint_error_details"]["orderer1"]
    assert detail["category"] == category and detail["outcome"] == "invalid"
    assert snapshot.health["read_status"]["status"] == status
    assert snapshot.metric("orderer_peer_health").value == 0.0
    frame = collect(provider(prom(lambda *_: body)))
    assert f"endpoint_read_error:orderer1:{category}" in frame.critical_missing


def test_missing_required_metric_is_partial_and_names_the_metric():
    adapter = prom(lambda *_: "some_other_metric 1\n")
    frame = collect(provider(adapter, required_prometheus_metrics=("raft_leader_count",)))
    assert "required_metric_missing:raft_leader_count" in frame.critical_missing
    assert frame.provenance["read_status"]["status"] == "PARTIAL"
    assert frame.provenance["read_status"]["missing_required"] == ["required_metric_missing:raft_leader_count"]
    item = frame.provenance["read_failures"]["items"][0]
    assert item["category"] == "missing_required_metric" and item["outcome"] == "partial"
    with pytest.raises(TelemetryQualityError) as caught:
        frame.validate()
    assert caught.value.abstention_reason == "telemetry_missing_fail_safe"
    # Without a declared requirement the same frame is unchanged from the established behaviour.
    collect(provider(prom(lambda *_: "some_other_metric 1\n"))).validate()


ENDPOINTS3 = {"orderer1": "http://orderer1/metrics", "orderer2": "http://orderer2/metrics",
              "orderer3": "http://orderer3/metrics"}


def per_endpoint(bodies):
    def fetch(url, _timeout):
        item = bodies[url.split("//")[1].split("/")[0]]
        if isinstance(item, BaseException):
            raise item
        return item
    return fetch


def test_partial_metrics_from_one_endpoint_are_partial_and_withhold_cross_endpoint_values():
    fetch = per_endpoint({"orderer1": leader(0), "orderer2": TimeoutError("timed out"), "orderer3": leader(1)})
    frame = collect(provider(prom(fetch, ENDPOINTS3)))
    status = frame.provenance["read_status"]
    assert status["status"] == "PARTIAL" and status["sources_failed"] == ["prometheus:orderer2"]
    assert status["sources_ok"] == ["prometheus:orderer1", "prometheus:orderer3"]
    leader_metric = frame.provenance["metrics"]["raft_leader_count"]
    assert leader_metric["value"] is None and "orderer2" in leader_metric["missing_reason"]
    assert "endpoint_read_error:orderer2:timeout" in frame.critical_missing
    with pytest.raises(TelemetryQualityError) as caught:
        frame.validate()
    assert caught.value.abstention_reason == "telemetry_read_failed_fail_safe"


def test_a_failed_leader_endpoint_is_not_misreported_as_zero_leaders():
    fetch = per_endpoint({"orderer1": leader(0), "orderer2": ConnectionRefusedError(111, "refused"), "orderer3": leader(0)})
    frame = collect(provider(prom(fetch, ENDPOINTS3)))
    assert "expected_exactly_one_raft_leader" not in frame.inconsistencies      # the leader's own endpoint failed
    assert frame.provenance["metrics"]["raft_leader_count"]["value"] is None


def test_failure_from_all_endpoints_is_failed_and_lists_every_cause():
    fetch = per_endpoint({"orderer1": TimeoutError("t"), "orderer2": ConnectionRefusedError(111, "r"),
                          "orderer3": urllib.error.HTTPError("u", 500, "x", {}, None)})
    frame = collect(provider(prom(fetch, ENDPOINTS3)))
    assert frame.provenance["read_status"]["status"] == "FAILED"
    categories = {i["source"]: i["category"] for i in frame.provenance["read_failures"]["items"]}
    assert categories == {"prometheus:orderer1": "timeout", "prometheus:orderer2": "connection_refused",
                          "prometheus:orderer3": "http_error"}
    assert {c for c in frame.critical_missing if c.startswith("endpoint_read_error:")} == {
        "endpoint_read_error:orderer1:timeout", "endpoint_read_error:orderer2:connection_refused",
        "endpoint_read_error:orderer3:http_error"}
    mixed = per_endpoint({"orderer1": TimeoutError("t"), "orderer2": "bad{\n", "orderer3": TimeoutError("t")})
    assert collect(provider(prom(mixed, ENDPOINTS3))).provenance["read_status"]["status"] == "INVALID"


def test_recovery_after_an_error_returns_a_healthy_frame():
    calls = {"n": 0}

    def flaky(*_):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError("timed out")
        return GOOD

    probe = provider(prom(flaky))
    frames = [collect(probe, i, f"2026-11-17T12:00:0{i}.000+00:00") for i in range(3)]
    assert frames[0].critical_missing and frames[0].provenance["read_status"]["status"] == "FAILED"
    for healthy in frames[1:]:
        assert healthy.critical_missing == () and healthy.provenance["read_status"]["status"] == "SUCCESSFUL"
        healthy.validate()
    assert frames[1].provenance["metrics"]["raft_leader_count"]["value"] == 1


# -------------------------------------------------------------- nothing substituted
def test_a_failed_observation_never_becomes_a_zero():
    frame = collect(provider(prom(raising(TimeoutError("t")))))
    for name in ("raft_leader_count", "block_fill_duration_s", "inflight_backlog", "latency_mean_ms", "offered_tps"):
        assert frame.provenance["metrics"][name]["value"] is None, name
    assert frame.state.queue_depth is None and frame.state.observed_latency_ms is None


# ----------------------------------------------- error context survives into records
class FakeModel:
    def predict(self, arrival, config):
        return PerformancePrediction(10.0, 100.0)


class FakeForecaster:
    def predict(self, history, horizon):
        return 10.0


def controllers():
    from dataclasses import replace
    return {"static": StaticController(CONFIG),
            "reactive": ReactiveController(CONFIG, congested_config=replace(CONFIG, max_message_count=40),
                                           latency_high_ms=1500, latency_low_ms=1200),
            "predictive": PredictiveController(FakeModel(), CONFIG, arrival_forecaster=FakeForecaster(),
                                               candidate_configs=[CONFIG])}


def test_error_context_survives_into_every_abstention_record():
    frame = collect(provider(prom(raising(TimeoutError("timed out after 2.0s")))))
    runner = IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="abstain")
    result = runner.run([frame])
    assert result["record_count"] == 3 and result["abstained_frame_count"] == 1 and result["stop_reasons"] == []
    for record in result["records"]:
        assert record["decision"]["reason"] == "telemetry_read_failed_fail_safe"
        assert record["decision"]["outcome"] == "abstain" and record["application"] is None
        rejection = record["decision"]["metadata"]["telemetry_rejection"]
        item = rejection["read_failures"]["items"][0]
        assert (item["source"], item["category"], item["error_type"]) == ("prometheus:orderer1", "timeout", "TimeoutError")
        assert "timed out after 2.0s" in item["message"]
        assert rejection["read_status"]["status"] == "FAILED"
        assert "endpoint_read_error:orderer1:timeout" in rejection["critical_missing"]
        provenance = record["integration"]["observation_provenance"]
        assert provenance["read_failures"]["items"][0]["category"] == "timeout"
    validation = result["validation_records"][0]
    assert validation["reason_code"] == "critical_telemetry_missing" and validation["handled"] == "abstained"
    json.dumps(result["records"])                                   # the whole record is serializable


def test_default_stop_policy_keeps_the_same_evidence_in_the_validation_record():
    frame = collect(provider(prom(raising(TimeoutError("t")))))
    result = IntegratedControllerRunner(controllers(), CONFIG, run_id="r").run([frame])
    assert result["record_count"] == 0 and result["stop_reasons"] == ["telemetry_validation_failed"]
    observation = result["validation_records"][0]["observation"]
    assert observation["provenance"]["read_failures"]["items"][0]["category"] == "timeout"


# ------------------------------------------------------------------ no secrets leak
SECRET_ERROR = ("GET http://admin:hunter2@orderer1:9443/metrics?token=abc123secret failed: Authorization: Bearer "
                "abcdefghijklmnopqrstuv PRIVATE_KEY=0badc0de api_key=XYZ789 "
                "-----BEGIN EC PRIVATE KEY-----\nMIIBsecretbody\n-----END EC PRIVATE KEY-----")
SECRETS = ["hunter2", "abc123secret", "abcdefghijklmnopqrstuv", "0badc0de", "XYZ789", "MIIBsecretbody"]


def test_credentials_keys_and_tokens_never_reach_frames_or_records():
    url = "http://admin:hunter2@orderer1:9443/metrics?token=abc123secret"
    frame = collect(provider(prom(raising(RuntimeError(SECRET_ERROR)), {"orderer1": url})))
    runner = IntegratedControllerRunner(controllers(), CONFIG, run_id="r", invalid_frame_policy="abstain")
    result = runner.run([frame])
    dump = json.dumps({"frame": frame.as_dict(), "records": result["records"], "validation": result["validation_records"]})
    for secret in SECRETS:
        assert secret not in dump, secret
    assert "[REDACTED" in dump
    assert frame.provenance["prometheus_endpoints"]["orderer1"] == "http://orderer1:9443/metrics?[query omitted]"
    assert frame.provenance["read_failures"]["items"][0]["endpoint"] == "http://orderer1:9443/metrics?[query omitted]"


def test_provider_level_error_strings_are_sanitized_too():
    def broken_config():
        raise RuntimeError("cannot read http://u:hunter2@cfg/block?password=abc123secret")
    frame = collect(ProductionTelemetryProvider(prom(lambda *_: GOOD), CHANNEL, config_reader=broken_config,
                                                required_fields=()))
    text = " ".join(frame.critical_missing)
    assert "configuration_read_error:RuntimeError" in text and "hunter2" not in text and "abc123secret" not in text


# ------------------------------------------------------- continuous adapter (G1, G5)
def ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def write_streams(tmp_path, now, *, with_latency=True, validation_code="0"):
    recorder, submits, ledger = tmp_path / "recorder.csv", tmp_path / "submits.csv", tmp_path / "ledger.csv"
    header = "tx_id,seq_finish,create_ms,final_ms,latency_ms,status\n" if with_latency else "tx_id,seq_finish,create_ms,final_ms,status\n"
    lines = []
    for i, tx in enumerate("abc"):
        created, finished = now - timedelta(seconds=4 - i), now - timedelta(seconds=3 - i)
        lines.append(f"{tx},{i},{ms(created)},{ms(finished)},{'100,' if with_latency else ''}success\n")
    recorder.write_text(header + "".join(lines), encoding="utf-8")
    submits.write_text("seq_submit,t_submit_evt_ms,count\n" + "".join(
        f"{i},{ms(now - timedelta(seconds=4 - i))},1\n" for i in range(3)), encoding="utf-8")
    ledger.write_text("tx_id,block,index,header_type,validation_code,env_bytes,channel_id,ch_ts_ms\n" + "".join(
        f"{tx},10,{i},3,{validation_code},100,{CHANNEL},{ms(now - timedelta(seconds=3 - i))}\n" for i, tx in enumerate("abc")),
        encoding="utf-8")
    return recorder, submits, ledger


def continuous(paths):
    return ContinuousCaliperLedgerAdapter(recorder_csv=paths[0], submits_csv=paths[1], ledger_csv=paths[2],
                                          channel_id=CHANNEL, run_id="r", window_s=5)


def test_a_recorder_without_latency_is_flagged_not_turned_into_a_bogus_backlog(tmp_path):
    now = datetime.now(timezone.utc)
    adapter = continuous(write_streams(tmp_path, now, with_latency=False))
    probe = provider(prom(lambda *_: GOOD), continuous_source=adapter)
    first = collect(probe, 0, now.isoformat(timespec="milliseconds"))
    assert "continuous_source_read_error:recorder:schema_error" in first.critical_missing
    assert "continuous_source_integrity_compromised:recorder" in first.critical_missing
    backlog = first.provenance["metrics"]["inflight_backlog"]
    assert backlog["value"] is None and "withheld" in backlog["missing_reason"]       # was a bogus 3 (101 on the real file)
    assert first.state.queue_depth is None
    assert first.provenance["read_status"]["sources_invalid"] == ["continuous_caliper"]
    with pytest.raises(TelemetryQualityError) as caught:
        first.validate()
    assert caught.value.abstention_reason == "telemetry_read_failed_fail_safe"
    # The error scrolls out of the next call, but the rows are gone for good: still not healthy.
    second = collect(probe, 1, (now + timedelta(seconds=1)).isoformat(timespec="milliseconds"))
    assert "continuous_source_integrity_compromised:recorder" in second.critical_missing
    assert second.state.queue_depth is None
    health = adapter.collect(observation_timestamp_utc=(now + timedelta(seconds=2)).isoformat()).health
    assert health["integrity"]["status"] == "COMPROMISED" and health["integrity"]["rows_rejected"]["recorder"] == 3
    # Two streams are intact and one is invalid: a partial observation, never a healthy one.
    assert health["read_status"]["status"] == "PARTIAL" and health["read_status"]["sources_invalid"] == ["recorder"]


def test_missing_streams_are_degraded_not_a_zero_backlog_and_they_heal(tmp_path):
    now = datetime.now(timezone.utc)
    paths = (tmp_path / "recorder.csv", tmp_path / "submits.csv", tmp_path / "ledger.csv")
    adapter = continuous(paths)
    snapshot = adapter.collect(observation_timestamp_utc=now.isoformat())
    assert snapshot.metric("inflight_backlog").value is None                      # was 0.0 with all files missing
    assert snapshot.health["integrity"]["status"] == "DEGRADED"
    assert {i["category"] for i in snapshot.health["read_error_details"]["items"]} == {"file_missing"}
    frame = collect(provider(prom(lambda *_: GOOD), continuous_source=adapter), 0, now.isoformat(timespec="milliseconds"))
    assert {"continuous_source_read_error:recorder:file_missing", "continuous_source_read_error:submits:file_missing",
            "continuous_source_read_error:ledger:file_missing"} <= set(frame.critical_missing)
    assert not any("integrity_compromised" in code for code in frame.critical_missing)
    write_streams(tmp_path, now)                                                  # the files appear: nothing was lost
    later = (now + timedelta(seconds=1)).isoformat(timespec="milliseconds")
    healed = collect(provider(prom(lambda *_: GOOD), continuous_source=adapter), 1, later)
    assert not any(code.startswith("continuous_source") for code in healed.critical_missing)
    assert adapter.collect(observation_timestamp_utc=later).health["integrity"]["status"] == "OK"


def test_rejected_ledger_and_recorder_rows_withhold_what_they_feed(tmp_path):
    now = datetime.now(timezone.utc)
    paths = write_streams(tmp_path, now, validation_code="x")
    snapshot = continuous(paths).collect(observation_timestamp_utc=now.isoformat())
    assert snapshot.health["integrity"]["rows_rejected"]["ledger"] == 3
    for name in ("valid_transactions", "invalid_transactions", "delivered_valid_tps"):
        assert snapshot.metric(name).value is None and "ledger" in snapshot.metric(name).missing_reason
    assert snapshot.metric("latency_mean_ms").value == 100.0                     # the recorder stream is intact
    recorder = paths[0]
    recorder.write_text(recorder.read_text(encoding="utf-8").replace("100,success", "-5,success", 1), encoding="utf-8")
    again = continuous(paths).collect(observation_timestamp_utc=now.isoformat())
    assert again.health["integrity"]["rows_rejected"]["recorder"] == 1
    assert again.metric("latency_mean_ms").value is None and again.metric("inflight_backlog").value is None


# ------------------------------------------------------------------- pure helpers
def test_status_taxonomy_matrix():
    ok, failed, invalid = "ok", "failed", "invalid"
    assert aggregate_read_status({"a": ok, "b": ok})["status"] == "SUCCESSFUL"
    assert aggregate_read_status({"a": ok, "b": failed})["status"] == "PARTIAL"
    assert aggregate_read_status({"a": ok, "b": invalid})["status"] == "PARTIAL"
    assert aggregate_read_status({"a": ok}, ["metric_x"])["status"] == "PARTIAL"
    assert aggregate_read_status({"a": failed, "b": failed})["status"] == "FAILED"
    assert aggregate_read_status({"a": failed, "b": invalid})["status"] == "INVALID"
    assert aggregate_read_status({})["status"] == "FAILED"
    assert aggregate_read_status({"a": ok, "b": ok}, ["m"])["missing_required"] == ["m"]
    with pytest.raises(ValueError):
        aggregate_read_status({"a": "mostly_ok"})


def test_classification_depends_on_whether_the_source_is_a_network_or_a_file():
    assert classify_exception(OSError(5, "I/O error"), kind="file") == "file_read_error"
    assert classify_exception(OSError(5, "I/O error"), kind="network") == "connection_error"
    assert classify_exception(FileNotFoundError("x"), kind="file") == "file_missing"
    assert classify_exception(KeyError("latency_ms"), kind="file") == "schema_error"
    assert classify_exception(ValueError("bad"), kind="file") == "invalid_value"
    assert classify_exception(ValueError("bad"), kind="network") == "malformed_response"
    assert classify_exception(UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad"), kind="network") == "malformed_response"
    assert classify_exception(RuntimeError("?")) == "internal_error"
    with pytest.raises(ValueError):
        classify_exception(ValueError("x"), kind="socket")
    assert {classify_exception(e) for e in (TimeoutError(), ConnectionRefusedError(), KeyError("x"))} <= CATEGORIES


def test_sanitizers_redact_and_bound():
    assert "hunter2" not in sanitize_text("see https://u:hunter2@host/x") and "[REDACTED]@host" in sanitize_text("https://u:hunter2@host/x")
    redacted = sanitize_text("http://h/p?access_token=tok123&x=1")
    assert "tok123" not in redacted and "access_token=[REDACTED]" in redacted and "x=1" in redacted
    assert "eyJhbGciOiJIUzI1NiJ9" not in sanitize_text("jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcdefghijklmnop")
    assert '"password": "[REDACTED]"' in sanitize_text('{"password": "p4ss", "user": "u"}')
    long = sanitize_text("x" * 1000)
    assert long.startswith("x" * 300) and "truncated 700 chars" in long
    assert sanitize_text("a\n  b\t c") == "a b c"
    assert sanitize_url("https://u:pw@host:8443/a/b?token=1") == "https://host:8443/a/b?[query omitted]"
    assert sanitize_url("not a url") == "not a url"


def test_failure_records_validate_and_are_bounded_when_serialized():
    with pytest.raises(ValueError):
        TelemetryReadFailure("s", "nonsense", "T", "m")
    with pytest.raises(ValueError):
        TelemetryReadFailure("", "timeout", "T", "m")
    failures = [failure_from_category("s", "missing_required_metric", f"m{i}") for i in range(25)]
    packed = bounded_details(failures)
    assert packed["count"] == 25 and packed["truncated"] == 15 and len(packed["items"]) == 10
    assert failure_from_exception("s", TimeoutError("x")).code == "s:timeout"
