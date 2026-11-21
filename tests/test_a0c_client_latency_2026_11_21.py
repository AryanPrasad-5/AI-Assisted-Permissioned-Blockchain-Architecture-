"""Task A0c (2026-11-21), G1: client latency is measured, validated and never zero-filled.

The v1 recorder writes ``create_ms`` and ``final_ms`` but no ``latency_ms``. ``ContinuousCaliperLedgerAdapter`` reads the
column by default (unchanged, so that file is still reported as a schema error) and can derive the interval from the
two timestamps when asked to (``recorder_latency="derive_from_timestamps"``). These tests pin the definition, the
boundary cases and the aggregation rules. Offline, in-process, files only under ``tmp_path``.
"""

import math
from datetime import datetime, timedelta, timezone

import pytest

from fabric_perf.controllers import ContinuousCaliperLedgerAdapter
from fabric_perf.controllers.client_latency import (
    ELIGIBLE_STATUSES,
    LATENCY_DEFINITION,
    LatencyState,
    derive_latency_ms,
    is_eligible_status,
    nearest_rank,
)

CHANNEL = "ch"
V1_HEADER = "tx_id,seq_finish,create_ms,final_ms,status,verified,err,obs_hr_ns,worker,round,label\n"
SUBMITS_HEADER = "seq_submit,t_submit_evt_ms,count\n"
LEDGER_HEADER = "tx_id,block,index,header_type,validation_code,env_bytes,channel_id,ch_ts_ms\n"


def ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def iso(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds")


def v1_line(tx, seq, create, final, status="success"):
    return f"{tx},{seq},{create},{final},{status},false,,0,0,1,m\n"


def make(tmp_path, rows, *, header=V1_HEADER, mode="derive_from_timestamps", window_s=5.0):
    recorder, submits, ledger = tmp_path / "recorder.csv", tmp_path / "submits.csv", tmp_path / "ledger.csv"
    recorder.write_text(header + "".join(rows), encoding="utf-8")
    submits.write_text(SUBMITS_HEADER, encoding="utf-8")
    ledger.write_text(LEDGER_HEADER, encoding="utf-8")
    adapter = ContinuousCaliperLedgerAdapter(recorder_csv=recorder, submits_csv=submits, ledger_csv=ledger,
                                             channel_id=CHANNEL, run_id="r", window_s=window_s, recorder_latency=mode)
    return adapter, recorder


def rows_with_latencies(now, latencies, *, status="success", start=0, spacing_s=0.2):
    """One v1 row per latency; each finishes ``spacing_s`` apart, the last just before ``now``."""
    out = []
    n = len(latencies)
    for i, latency in enumerate(latencies):
        final = now - timedelta(seconds=spacing_s * (n - 1 - i) + 0.1)
        out.append(v1_line(f"t{start + i}", start + i, ms(final) - latency, ms(final), status))
    return out


# ------------------------------------------------------------------------- the definition
@pytest.mark.parametrize("create, final, state, latency", [
    (1000, 1450, LatencyState.OK, 450.0),
    ("1000", "1450", LatencyState.OK, 450.0),                      # CSV fields are strings
    (1000.5, 1450.25, LatencyState.OK, 449.75),
    (1000, 1000, LatencyState.OK, 0.0),                            # boundary: equal timestamps are a valid 0 ms
    (1700000000000, 1700000001500, LatencyState.OK, 1500.0),       # 1.5 s expressed in milliseconds
    (None, 1450, LatencyState.MISSING_CREATE, None),
    ("", 1450, LatencyState.MISSING_CREATE, None),
    ("   ", 1450, LatencyState.MISSING_CREATE, None),
    (1000, None, LatencyState.MISSING_FINAL, None),
    (1000, "", LatencyState.MISSING_FINAL, None),
    (1000, "nan", LatencyState.NONFINITE, None),
    ("inf", 1450, LatencyState.NONFINITE, None),
    (float("nan"), 1450, LatencyState.NONFINITE, None),
    (1000, float("-inf"), LatencyState.NONFINITE, None),
    ("abc", 1450, LatencyState.NOT_NUMERIC, None),
    (1000, "12ms", LatencyState.NOT_NUMERIC, None),
    (True, 1450, LatencyState.BOOLEAN, None),
    (0, 1450, LatencyState.NONPOSITIVE_TIMESTAMP, None),           # a zero epoch against a real one would be absurd
    (-5, 1450, LatencyState.NONPOSITIVE_TIMESTAMP, None),
    (1450, 1000, LatencyState.NEGATIVE, None),                     # final before create
])
def test_derivation_gives_the_interval_or_the_reason_and_never_a_zero_for_missing(create, final, state, latency):
    sample = derive_latency_ms(create, final)
    assert sample.state is state and sample.latency_ms == latency
    assert sample.ok is (state is LatencyState.OK)
    if not sample.ok:
        assert sample.latency_ms is None                           # not 0, not NaN


def test_the_definition_is_the_project_verified_caliper_definition():
    assert "final_ms - create_ms" in LATENCY_DEFINITION and "epoch milliseconds" in LATENCY_DEFINITION
    assert ELIGIBLE_STATUSES == {"success", "succeeded", "valid"}
    assert is_eligible_status("success") and is_eligible_status(" Success ") and not is_eligible_status("failed")
    assert not is_eligible_status("") and not is_eligible_status("created")


def test_state_values_are_stable_strings():
    """The JavaScript recorder emits these exact strings (see test_a0c_recorder_v2: parity test)."""
    assert [s.value for s in LatencyState] == ["ok", "missing_create_timestamp", "missing_final_timestamp", "not_numeric",
                                               "nonfinite", "nonpositive_timestamp", "negative_latency", "boolean_value"]


# ------------------------------------------------------------------------- nearest-rank percentile
@pytest.mark.parametrize("n, expected_rank", [(1, 1), (2, 2), (19, 19), (20, 19), (21, 20), (100, 95), (101, 96)])
def test_nearest_rank_p95_is_the_ceil_0_95_n_th_smallest(n, expected_rank):
    values = list(range(1, n + 1))
    assert nearest_rank(reversed(values), 0.95) == values[expected_rank - 1]


def test_nearest_rank_of_an_empty_window_is_none_not_zero():
    assert nearest_rank([], 0.95) is None
    assert nearest_rank(iter(()), 0.5) is None


@pytest.mark.parametrize("q", [0, -0.1, 1.01, True, float("nan")])
def test_nearest_rank_refuses_a_nonsense_quantile(q):
    with pytest.raises(ValueError):
        nearest_rank([1.0, 2.0], q)


# ------------------------------------------------------------------------- adapter: default is unchanged
def test_default_mode_still_reports_a_v1_recorder_file_as_a_schema_error_and_withholds_the_metrics(tmp_path):
    now = datetime.now(timezone.utc)
    adapter, _ = make(tmp_path, rows_with_latencies(now, [100, 200, 300]), mode="column")
    snapshot = adapter.collect(observation_timestamp_utc=iso(now))
    assert adapter.recorder_latency == "column"
    assert snapshot.health["integrity"]["status"] == "COMPROMISED" and snapshot.health["integrity"]["rows_rejected"]["recorder"] == 3
    assert snapshot.metrics["latency_mean_ms"].value is None and "withheld" in snapshot.metrics["latency_mean_ms"].missing_reason


def test_the_constructor_default_is_column_and_an_unknown_mode_is_refused(tmp_path):
    import inspect
    assert inspect.signature(ContinuousCaliperLedgerAdapter.__init__).parameters["recorder_latency"].default == "column"
    with pytest.raises(ValueError, match="recorder_latency"):
        make(tmp_path, [], mode="guess")


# ------------------------------------------------------------------------- adapter: derivation
def test_derivation_reads_a_v1_recorder_file_and_computes_mean_and_p95_in_milliseconds(tmp_path):
    now = datetime.now(timezone.utc)
    adapter, _ = make(tmp_path, rows_with_latencies(now, [100, 200, 300, 400]))
    snapshot = adapter.collect(observation_timestamp_utc=iso(now))
    mean, p95 = snapshot.metrics["latency_mean_ms"], snapshot.metrics["latency_p95_ms"]
    assert mean.value == 250.0 and mean.unit == "ms"
    assert p95.value == 400.0 and p95.unit == "ms"                  # ceil(0.95 * 4) = 4th smallest
    assert snapshot.health["integrity"]["status"] == "OK" and snapshot.health["integrity"]["rows_rejected"]["recorder"] == 0
    assert snapshot.health["totals"]["completed"] == 4 and snapshot.health["totals"]["failed"] == 0


def test_a_latency_in_seconds_is_not_mistaken_for_milliseconds(tmp_path):
    """The interval is final_ms - create_ms: 1.5 s apart is 1500, never 1.5."""
    now = datetime.now(timezone.utc)
    final = now - timedelta(milliseconds=200)
    adapter, _ = make(tmp_path, [v1_line("a", 0, ms(final) - 1500, ms(final))])
    assert adapter.collect(observation_timestamp_utc=iso(now)).metrics["latency_mean_ms"].value == 1500.0


def test_a_successful_row_without_a_create_timestamp_is_rejected_and_nothing_is_zero_filled(tmp_path):
    now = datetime.now(timezone.utc)
    good = rows_with_latencies(now, [100, 200])
    bad = f"bad,9,,{ms(now - timedelta(milliseconds=300))},success,false,,0,0,1,m\n"
    adapter, _ = make(tmp_path, good + [bad])
    snapshot = adapter.collect(observation_timestamp_utc=iso(now))
    assert snapshot.health["integrity"]["status"] == "COMPROMISED"
    assert snapshot.health["integrity"]["rows_rejected"]["recorder"] == 1
    for name in ("latency_mean_ms", "latency_p95_ms", "failed_transactions"):
        assert snapshot.metrics[name].value is None and "withheld" in snapshot.metrics[name].missing_reason
    assert any("recorder_latency_error:schema_error:bad" in item for item in snapshot.health["read_errors"])


@pytest.mark.parametrize("create, final, category", [
    ("nan", "{final}", "invalid_value"),
    ("0", "{final}", "invalid_value"),
    ("{create_after}", "{final}", "invalid_value"),                  # final before create
])
def test_a_successful_row_with_an_unusable_timestamp_is_rejected(tmp_path, create, final, category):
    now = datetime.now(timezone.utc)
    f = ms(now - timedelta(milliseconds=300))
    row = f"x,0,{create.format(create_after=f + 500)},{final.format(final=f)},success,false,,0,0,1,m\n"
    adapter, _ = make(tmp_path, [row])
    health = adapter.collect(observation_timestamp_utc=iso(now)).health
    assert health["integrity"]["rows_rejected"]["recorder"] == 1
    assert any(f"recorder_latency_error:{category}:x" in item for item in health["read_errors"])


def test_a_failed_row_without_a_valid_interval_counts_as_failed_and_contributes_no_latency(tmp_path):
    now = datetime.now(timezone.utc)
    f = ms(now - timedelta(milliseconds=200))
    rows = rows_with_latencies(now, [100, 300]) + [f"bad,9,,{f},failed,false,boom,0,0,1,m\n"]
    adapter, _ = make(tmp_path, rows)
    snapshot = adapter.collect(observation_timestamp_utc=iso(now))
    assert snapshot.health["integrity"]["rows_rejected"]["recorder"] == 0              # not a corrupt row: a failed transaction
    assert snapshot.metrics["failed_transactions"].value == 1.0
    assert snapshot.metrics["latency_mean_ms"].value == 200.0                           # the failed row is not in the mean
    assert snapshot.health["latency"]["eligible_samples_in_window"] == 2 and snapshot.health["latency"]["failed_rows_in_window"] == 1


@pytest.mark.parametrize("mode", ["derive_from_timestamps"])
def test_a_window_with_only_failed_transactions_has_no_latency_and_says_why(tmp_path, mode):
    """Previously the metric had value None and no reason, which made the metric itself invalid."""
    now = datetime.now(timezone.utc)
    adapter, _ = make(tmp_path, rows_with_latencies(now, [100, 200], status="failed"), mode=mode)
    snapshot = adapter.collect(observation_timestamp_utc=iso(now))
    for name in ("latency_mean_ms", "latency_p95_ms"):
        metric = snapshot.metrics[name]
        assert metric.value is None and metric.missing_reason == "no successful transaction in measurement window"
    assert snapshot.metrics["failed_transactions"].value == 2.0


def test_the_failed_only_window_is_also_handled_in_the_default_column_mode(tmp_path):
    now = datetime.now(timezone.utc)
    header = "tx_id,seq_finish,create_ms,final_ms,latency_ms,status\n"
    rows = [f"f{i},{i},1,{ms(now - timedelta(milliseconds=100 * (i + 1)))},50,failed\n" for i in range(2)]
    adapter, _ = make(tmp_path, rows, header=header, mode="column")
    snapshot = adapter.collect(observation_timestamp_utc=iso(now))
    assert snapshot.metrics["latency_mean_ms"].value is None
    assert snapshot.metrics["latency_mean_ms"].missing_reason == "no successful transaction in measurement window"


def test_an_empty_window_has_no_latency_and_says_so(tmp_path):
    now = datetime.now(timezone.utc)
    adapter, _ = make(tmp_path, [])
    snapshot = adapter.collect(observation_timestamp_utc=iso(now))
    for name in ("latency_mean_ms", "latency_p95_ms"):
        assert snapshot.metrics[name].value is None
        assert snapshot.metrics[name].missing_reason == "no completed Caliper records in measurement window"


# ------------------------------------------------------------------------- explicit column cross-check
def test_a_file_that_carries_the_column_must_agree_with_its_timestamps(tmp_path):
    now = datetime.now(timezone.utc)
    f = ms(now - timedelta(milliseconds=200))
    header = "tx_id,seq_finish,create_ms,final_ms,latency_ms,status\n"
    agree = f"a,0,{f - 450},{f},450,success\n"
    within_tolerance = f"b,1,{f - 450},{f},450.9,success\n"
    adapter, _ = make(tmp_path, [agree, within_tolerance], header=header)
    snapshot = adapter.collect(observation_timestamp_utc=iso(now))
    assert snapshot.health["integrity"]["rows_rejected"]["recorder"] == 0
    assert snapshot.metrics["latency_mean_ms"].value == 450.0           # the derived interval, not the written column


@pytest.mark.parametrize("written", ["900", "-5", "abc", "nan", "inf"])
def test_a_column_that_disagrees_or_is_malformed_rejects_the_row(tmp_path, written):
    now = datetime.now(timezone.utc)
    f = ms(now - timedelta(milliseconds=200))
    header = "tx_id,seq_finish,create_ms,final_ms,latency_ms,status\n"
    adapter, _ = make(tmp_path, [f"a,0,{f - 450},{f},{written},success\n"], header=header)
    health = adapter.collect(observation_timestamp_utc=iso(now)).health
    assert health["integrity"]["status"] == "COMPROMISED" and health["integrity"]["rows_rejected"]["recorder"] == 1


def test_a_blank_column_is_treated_as_absent_and_the_timestamps_decide(tmp_path):
    now = datetime.now(timezone.utc)
    f = ms(now - timedelta(milliseconds=200))
    header = "tx_id,seq_finish,create_ms,final_ms,latency_ms,status\n"
    adapter, _ = make(tmp_path, [f"a,0,{f - 450},{f},,success\n"], header=header)
    assert adapter.collect(observation_timestamp_utc=iso(now)).metrics["latency_mean_ms"].value == 450.0


# ------------------------------------------------------------------------- incomplete rows, eligibility
def test_rows_without_a_final_timestamp_are_counted_as_incomplete_and_are_not_completions(tmp_path):
    now = datetime.now(timezone.utc)
    rows = rows_with_latencies(now, [100]) + [f"p,5,{ms(now)},,,false,,0,0,1,m\n"]
    adapter, _ = make(tmp_path, rows)
    snapshot = adapter.collect(observation_timestamp_utc=iso(now))
    assert snapshot.health["totals"]["completed"] == 1
    assert snapshot.health["latency"]["rows_without_final_timestamp_cumulative"] == 1
    assert snapshot.metrics["latency_mean_ms"].value == 100.0


def test_only_successful_transactions_are_latency_eligible(tmp_path):
    now = datetime.now(timezone.utc)
    rows = rows_with_latencies(now, [100, 100], status="success") + rows_with_latencies(now, [5000], status="failed", start=10)
    adapter, _ = make(tmp_path, rows)
    snapshot = adapter.collect(observation_timestamp_utc=iso(now))
    assert snapshot.metrics["latency_mean_ms"].value == 100.0 and snapshot.metrics["latency_p95_ms"].value == 100.0


def test_a_duplicate_transaction_id_is_counted_once(tmp_path):
    now = datetime.now(timezone.utc)
    rows = rows_with_latencies(now, [100, 200])
    adapter, recorder = make(tmp_path, rows + [rows[0]])
    snapshot = adapter.collect(observation_timestamp_utc=iso(now))
    assert snapshot.health["totals"]["completed"] == 2 and snapshot.health["cross_source"]["duplicate_ids"] == 1


# ------------------------------------------------------------------------- windows and time bases
def test_p95_is_computed_from_one_windows_raw_samples_and_never_averaged_across_windows(tmp_path):
    now = datetime.now(timezone.utc)
    adapter, recorder = make(tmp_path, rows_with_latencies(now, [100] * 20), window_s=5.0)
    first = adapter.collect(observation_timestamp_utc=iso(now))
    assert first.metrics["latency_p95_ms"].value == 100.0
    later = now + timedelta(seconds=10)                                   # the first window has fully expired
    with recorder.open("a", encoding="utf-8") as fh:
        fh.write("".join(rows_with_latencies(later, [1000] * 20, start=100)))
    second = adapter.collect(observation_timestamp_utc=iso(later))
    assert second.metrics["latency_p95_ms"].value == 1000.0              # not 550.0 (mean of the two p95 values)
    assert second.health["latency"]["eligible_samples_in_window"] == 20  # the expired window's samples do not leak in
    assert "never averaged" in second.health["latency"]["percentile_method"]


def test_the_metric_timestamp_is_event_time_and_the_observation_is_collection_time(tmp_path):
    now = datetime.now(timezone.utc)
    rows = rows_with_latencies(now, [100, 200, 300], spacing_s=1.0)
    adapter, _ = make(tmp_path, rows)
    observation = now + timedelta(seconds=2)                              # read two seconds after the last completion
    metric = adapter.collect(observation_timestamp_utc=iso(observation)).metrics["latency_mean_ms"]
    last_final = now - timedelta(seconds=0.1)
    assert abs((datetime.fromisoformat(metric.timestamp_utc) - last_final).total_seconds()) < 0.002
    assert datetime.fromisoformat(metric.observation_timestamp_utc) == datetime.fromisoformat(iso(observation))
    assert metric.window_end_utc == metric.observation_timestamp_utc
    assert datetime.fromisoformat(metric.window_end_utc) - datetime.fromisoformat(metric.window_start_utc) == timedelta(seconds=5)


def test_rows_that_finished_before_the_window_are_not_in_the_statistic(tmp_path):
    now = datetime.now(timezone.utc)
    old_final = now - timedelta(seconds=30)
    rows = [v1_line("old", 0, ms(old_final) - 9000, ms(old_final))] + rows_with_latencies(now, [100])
    adapter, _ = make(tmp_path, rows)
    snapshot = adapter.collect(observation_timestamp_utc=iso(now))
    assert snapshot.metrics["latency_mean_ms"].value == 100.0              # the 9000 ms transaction finished too long ago
    assert snapshot.health["totals"]["completed"] == 2                     # but it is still counted as a completion


def test_a_late_delivered_old_completion_is_not_made_fresh_by_the_time_it_was_read(tmp_path):
    """Round-end dumps (the v1 behaviour) carry old event times: the window is empty, the metric is missing, not stale-fresh."""
    now = datetime.now(timezone.utc)
    old = now - timedelta(seconds=60)
    adapter, _ = make(tmp_path, [v1_line("a", 0, ms(old) - 100, ms(old))])
    metric = adapter.collect(observation_timestamp_utc=iso(now)).metrics["latency_mean_ms"]
    assert metric.value is None and metric.missing_reason == "no completed Caliper records in measurement window"


# ------------------------------------------------------------------------- provenance
def test_the_health_block_says_what_the_latency_is_and_where_it_came_from(tmp_path):
    now = datetime.now(timezone.utc)
    adapter, _ = make(tmp_path, rows_with_latencies(now, [100, 200]))
    block = adapter.collect(observation_timestamp_utc=iso(now)).health["latency"]
    assert block["definition"] == LATENCY_DEFINITION and block["unit"] == "ms"
    assert block["source_mode"] == "derive_from_timestamps" and "final_ms - create_ms" in block["source"]
    assert block["eligible_statuses"] == "success" and "nearest-rank" in block["percentile_method"]
    assert block["event_time_basis"].startswith("final_ms")
    assert block["eligible_samples_in_window"] == 2 and block["rows_rejected_cumulative"] == 0


def test_every_reported_latency_is_finite_and_non_negative(tmp_path):
    now = datetime.now(timezone.utc)
    adapter, _ = make(tmp_path, rows_with_latencies(now, [0, 1, 2, 999]))
    for name in ("latency_mean_ms", "latency_p95_ms"):
        value = adapter.collect(observation_timestamp_utc=iso(now)).metrics[name].value
        assert value is not None and math.isfinite(value) and value >= 0
