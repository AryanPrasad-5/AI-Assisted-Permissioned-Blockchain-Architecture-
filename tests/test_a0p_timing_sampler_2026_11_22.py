"""A0p workstream D (checklist items 31-39): T01 (direct write and sync timing) and T06 (bridge ingestion delay) from the new evidence, with T02-T05 and historical inputs unchanged.

EVERY value here is fixture arithmetic on scripted clocks: it is NOT Caliper latency, NOT a live measurement and NOT a clock-agreement measurement."""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from tests.a0p_support import (BridgeClock, FAST, NL, bridge_config, chain, create_bridge, forge_sidecar, ilt, j, lines_of, rhi, rows_of, scripted_log, spool_recorder, td, ts, wc,
                               write_lines)

ORIGIN = "synthetic_fixture"


def ceil_rank(values, q):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q * len(ordered)) - 1)]


def t01_for(tmp: Path, seconds: int = 20, **forge):
    gen = scripted_log(tmp, seconds=seconds)
    side = forge_sidecar(gen.log.path, tmp / "side.jsonl", **forge)
    log = ts.read_issue_log(gen.log.path)
    ev = ilt.verify_timing(gen.log.path, side)
    return gen, side, log, ev, td.t01_direct(log, ev, ORIGIN)


def timed_ingest(rec, tmp: Path, *, latency_ms: float = 250.0, name: str = "t", spool_domain: str = "recorder-host", bridge_domain: str = "bridge-host", offset_ms: float = 0.0):
    """Ingest a spool one row per poll, the bridge host's wall clock set to the row's spool time + ``latency_ms`` + ``offset_ms`` just before each poll."""
    cfg = bridge_config(rec, tmp, name=name, spool_domain=spool_domain, bridge_domain=bridge_domain)
    clock = BridgeClock(step_ms=1.0)
    b = rhi.IncrementalRecorderBridge.create(cfg, wall_clock=clock.wall, mono_us=clock.mono)
    from datetime import timedelta
    for row in rows_of(rec.spool_path):
        clock.set(wc.parse_utc(row["t_utc"], "t") + timedelta(milliseconds=latency_ms + offset_ms))
        assert b.poll(max_rows=1)["status"] == "progress"
    b.close(source_ended=True)
    return cfg


# ----------------------------------------------------------------------------------------------------------------------------- 31 T01 write duration
def test_31_t01_reports_the_direct_write_duration_per_attested_record_from_the_sidecar(tmp_path):
    gen, side, log, ev, result = t01_for(tmp_path, dur_us=lambda rseq: 100 + rseq * 10)
    assert ev.status == ilt.VERIFIED and result["status"] == ts.S_MEASURED and result["measurement"] == "T01"
    assert result["eligible_samples"] == ev.log_records and result["quantity_measured"].startswith("direct write-call duration")
    first = result["samples"][0]
    assert first["basis"] == "write_call_mono_us_delta" and first["clock_pair"] == ["worker_mono_us"] and first["schema"] == "a0n_timing_sample/1"
    assert first["index"] == 1 and first["value_lo_ms"] == pytest.approx(0.110 - 0.001) and first["value_hi_ms"] == pytest.approx(0.110 + 0.001)
    assert first["record_sha256"] == log["records"][0]["sha256"] and first["record_kind"] == "start"
    agg = result["write_call_ms"]
    assert agg["n"] == ev.log_records and agg["percentile_method"] == ts.PERCENTILE_METHOD
    assert result["intent_attempt_proxy"]["status"] in (ts.S_MEASURED, ts.S_NONE) and "different quantity" in result["note"]
    assert "NOT Caliper latency" in result["provenance"] and result["evidence_origin"] == ORIGIN and "fixture_arithmetic" in result["observation_class"]
    assert {i["role"] for i in result["source_artifacts"]} == {"issue_log", "timing_sidecar"} and all(len(i["sha256"]) == 64 for i in result["source_artifacts"])


# ----------------------------------------------------------------------------------------------------------------------------- 32 sync duration
def test_32_the_sync_duration_is_reported_separately_from_the_write_duration(tmp_path):
    gen, side, log, ev, result = t01_for(tmp_path, dur_us=lambda rseq: 80, sync_dur_us=1_500)
    assert result["sync_count"] == len(ev.syncs) > 0 and result["sync_call_ms"]["n"] == len(ev.syncs)
    assert {s["basis"] for s in result["sync_samples"]} == {"sync_call_mono_us_delta"} and result["sync_samples"][0]["value_lo_ms"] == pytest.approx(1.5 - 0.001)
    assert result["write_call_ms"]["lower_bound"]["max"] == pytest.approx(0.079)           # the sync cost is NOT folded into any write sample
    assert result["persistence"]["records_with_no_covering_sync"] == 0 and "not a guarantee against every storage failure" in result["persistence"]["sync_meaning"]
    assert "not a persistence claim" in result["persistence"]["write_end_meaning"]
    partial = t01_for(tmp_path / "p", sync_after=lambda rseq, kind: kind == "tick")[4]      # no sync after the stop record
    assert partial["persistence"]["records_with_no_covering_sync"] > 0


# ----------------------------------------------------------------------------------------------------------------------------- 33 T06 valid
def test_33_t06_is_reported_for_a_declared_same_clock_domain_with_the_bridge_append_return_as_the_ingest_time(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = timed_ingest(rec, tmp_path, latency_ms=250.0, spool_domain="shared-host", bridge_domain="shared-host")
    result = td.run_sampler_a0p(origin=ORIGIN, spool=rec.spool_path, ingest_ledger=cfg.ledger_path)["measurements"]["T06"]
    assert result["status"] == ts.S_MEASURED and result["eligible_samples"] == 22 and "operator-declared SAME clock domain" in result["clock_basis"]
    assert "not a measurement of clock agreement" in result["clock_basis"]
    lo, hi = result["aggregate"]["lower_bound"]["median"], result["aggregate"]["upper_bound"]["median"]
    assert 250.0 <= hi <= 262.0 and lo == pytest.approx(hi - 1.0 - 0.002, abs=0.01)         # the 250 ms scripted lag, plus the few scripted 1 ms clock calls inside a poll
    assert result["samples"][0]["basis"] == "journal_append_return_wall_minus_spool_t_utc" and result["samples"][0]["clock_pair"] == ["shared-host", "shared-host"]
    assert result["bridge_internal"]["eligible_samples"] == 22 and "elapsed time inside the bridge process" in result["bridge_internal"]["meaning"]
    assert "NOT Caliper latency" in result["provenance"]


def test_33b_t06_with_a_supplied_t12_offset_interval_shifts_and_widens_the_band(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = timed_ingest(rec, tmp_path, latency_ms=250.0, offset_ms=100.0, spool_domain="recorder-host", bridge_domain="bridge-host")     # the bridge clock runs 100 ms ahead
    result = td.run_sampler_a0p(origin=ORIGIN, spool=rec.spool_path, ingest_ledger=cfg.ledger_path, bridge_offset_interval_ms=[95.0, 105.0])["measurements"]["T06"]
    assert result["status"] == ts.S_MEASURED and "T12 offset interval [95.0, 105.0]" in result["clock_basis"]
    s = result["samples"][5]
    assert s["offset_interval_ms"] == [95.0, 105.0]
    assert s["value_hi_ms"] - s["value_lo_ms"] == pytest.approx((105.0 - 95.0) + 1.0 + 0.002, abs=1e-6)       # offset width + spool ms precision + quantisation
    assert s["value_lo_ms"] <= 250.0 <= s["value_hi_ms"]                                      # the true 250 ms lag (a few scripted 1 ms calls aside) is inside the band
    with pytest.raises(ts.TimingSamplerError) as exc:
        td.run_sampler_a0p(origin=ORIGIN, ingest_ledger=cfg.ledger_path, bridge_offset_interval_ms=[105.0, 95.0])
    assert exc.value.code == "offset_interval_invalid"


# ----------------------------------------------------------------------------------------------------------------------------- 34 T06 cross-host
def test_34_t06_across_different_clock_domains_without_an_offset_is_not_measurable_but_the_in_process_quantities_are_reported(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = timed_ingest(rec, tmp_path, spool_domain="recorder-host", bridge_domain="bridge-host")
    result = td.run_sampler_a0p(origin=ORIGIN, spool=rec.spool_path, ingest_ledger=cfg.ledger_path)["measurements"]["T06"]
    assert result["status"] == ts.S_NOT_MEASURABLE and result["aggregate"] is None and result["eligible_samples"] == 0
    assert "'recorder-host'" in result["missing_dependency"] and "'bridge-host'" in result["missing_dependency"] and "T12" in result["missing_dependency"]
    assert result["internal_status"] == ts.S_MEASURED and result["bridge_internal"]["bridge_read_to_append_return_ms"]["n"] == 22
    assert result["bridge_internal"]["journal_append_ms"]["lower_bound"]["max"] >= 0.0 and result["clock_basis"].startswith("none")
    assert "within-process" in result["reason"]


# ----------------------------------------------------------------------------------------------------------------------------- 35 missing timestamps
def test_35_a_missing_completion_or_ingest_timestamp_is_missing_never_estimated(tmp_path):
    gen, side, log, ev, result = t01_for(tmp_path, drop_last_write=True)
    assert ev.status == ilt.UNRESOLVED_TAIL and result["status"] == ts.S_MEASURED
    missing = [e for e in result["exclusions"] if e["reason"].startswith("missing")]
    assert result["missing_count"] == len(missing) == ev.log_records - len(ev.writes) and result["eligible_samples"] == len(ev.writes)
    no_field = [dict(r) for r in rows_of(side)]
    del no_field[3]["write_end_mono_us"]
    write_lines(tmp_path / "nofield.jsonl", chain(no_field))
    bad = ilt.verify_timing(gen.log.path, tmp_path / "nofield.jsonl")
    assert bad.status == ilt.INVALID and bad.error["code"] == "timing_field_invalid"
    refused = td.t01_direct(log, bad, ORIGIN)
    assert refused["status"] == ts.S_INVALID and refused["aggregate"] is None and "no timing is taken from it" in refused["reason"]
    rec = spool_recorder(tmp_path / "r")
    cfg = timed_ingest(rec, tmp_path / "r", spool_domain="x", bridge_domain="x")
    rows = rows_of(cfg.ledger_path)
    victim = next(i for i, r in enumerate(rows) if r["kind"] == "ingest" and r["spool_n"] == 5)
    rows[victim]["journal_append_return_mono_us"] = None
    rows[victim + 1]["journal_append_confirmation"] = "not_confirmed"
    write_lines(tmp_path / "r" / "gap.ledger.jsonl", chain(rows))
    t06 = td.run_sampler_a0p(origin=ORIGIN, ingest_ledger=tmp_path / "r" / "gap.ledger.jsonl")["measurements"]["T06"]
    assert t06["eligible_samples"] == 20 and t06["invalid_count"] == 2 and {e["reason"] for e in t06["exclusions"]} == {"invalid: stamp_unusable", "invalid: append_not_confirmed"}


# ----------------------------------------------------------------------------------------------------------------------------- 36 ordering
def test_36_impossible_timestamp_orderings_are_refused_not_clamped(tmp_path):
    gen = scripted_log(tmp_path)
    cases = {"end_before_start": lambda rows: rows[4].update(write_end_mono_us=rows[4]["write_start_mono_us"] - 1),
             "write_before_creation": lambda rows: rows[4].update(write_start_wall_ms=rows[4]["target_t_ms"] - 5000),
             "overlapping_writes": lambda rows: rows[5].update(write_start_mono_us=rows[4]["write_end_mono_us"] - 5),
             "wall_clock_step": lambda rows: rows[6].update(write_start_wall_ms=rows[6]["write_start_wall_ms"] + 60_000, write_end_wall_ms=rows[6]["write_end_wall_ms"] + 60_000)}
    expected = {"end_before_start": "timestamp_order_invalid", "write_before_creation": "timestamp_order_invalid", "overlapping_writes": "monotonic_regression",
                "wall_clock_step": "wall_clock_step"}
    for name, change in cases.items():
        out = forge_sidecar(gen.log.path, tmp_path / f"{name}.jsonl", mutate=change)
        ev = ilt.verify_timing(gen.log.path, out)
        assert ev.status == ilt.INVALID and ev.error["code"] == expected[name], name
    early = forge_sidecar(gen.log.path, tmp_path / "early_sync.jsonl", mutate=lambda rows: next(r for r in rows if r["kind"] == "sync").update(
        sync_start_mono_us=next(r for r in rows if r["kind"] == "write")["write_end_mono_us"]))
    assert ilt.verify_timing(gen.log.path, early).error["code"] == "timestamp_order_invalid"
    rec = spool_recorder(tmp_path / "s")
    cfg = timed_ingest(rec, tmp_path / "s", latency_ms=-5000.0, spool_domain="h", bridge_domain="h")          # the bridge "ingested" 5 s BEFORE the recorder wrote the row
    t06 = td.run_sampler_a0p(origin=ORIGIN, ingest_ledger=cfg.ledger_path)["measurements"]["T06"]
    assert t06["eligible_samples"] == 0 and t06["invalid_count"] == 22 and {e["reason"] for e in t06["exclusions"]} == {"invalid: assumption_violated"}
    assert t06["status"] == ts.S_NONE and "never clamped" in t06["exclusions"][0]["detail"]


# ----------------------------------------------------------------------------------------------------------------------------- 37 uncertainty
def test_37_uncertainty_is_carried_into_every_sample_and_the_result(tmp_path):
    gen, side, log, ev, result = t01_for(tmp_path, dur_us=lambda rseq: 500)
    for s in result["samples"]:
        assert s["value_hi_ms"] - s["value_lo_ms"] == pytest.approx(0.002) and s["value_lo_ms"] < 0.5 < s["value_hi_ms"]
    assert result["uncertainty_ms"]["quantisation"] == 0.001 and "scheduler preemption" in result["uncertainty_ms"]["excludes"]
    tiny = t01_for(tmp_path / "t", dur_us=lambda rseq: 0)[4]
    assert tiny["samples"][0]["value_lo_ms"] == 0.0 and tiny["samples"][0]["value_hi_ms"] == pytest.approx(0.001)              # floored at 0, never negative
    rec = spool_recorder(tmp_path / "u")
    cfg = timed_ingest(rec, tmp_path / "u", spool_domain="h", bridge_domain="h")
    t06 = td.run_sampler_a0p(origin=ORIGIN, ingest_ledger=cfg.ledger_path)["measurements"]["T06"]
    assert t06["uncertainty_ms"] == {"spool_time_precision": 1.0, "quantisation": 0.001, "clock_offset": None,
                                     "bridge_cadence": "the poll interval bounds how late a row can be read"}
    assert all(s["value_hi_ms"] - s["value_lo_ms"] == pytest.approx(1.002) for s in t06["samples"])


# ----------------------------------------------------------------------------------------------------------------------------- 38 percentiles
def test_38_percentiles_are_nearest_rank_and_p99_is_reported_only_from_100_samples(tmp_path):
    gen, side, log, ev, big = t01_for(tmp_path, seconds=60, dur_us=lambda rseq: 1_000 + 37 * ((rseq * 7919) % 211))
    n = big["write_call_ms"]["n"]
    assert n >= 100 and ev.log_records == n
    durations = [(w["write_end_mono_us"] - w["write_start_mono_us"]) / 1000.0 for w in ev.writes.values()]
    lower, upper = [max(0.0, d - 0.001) for d in durations], [d + 0.001 for d in durations]
    for side_name, values in (("lower_bound", lower), ("upper_bound", upper)):
        got = big["write_call_ms"][side_name]
        assert got["median"] == ceil_rank(values, 0.5) and got["p95"] == ceil_rank(values, 0.95) and got["p99"] == ceil_rank(values, 0.99) and got["max"] == max(values)
    small = t01_for(tmp_path / "small", seconds=10, dur_us=lambda rseq: 100 * rseq)[4]
    assert small["write_call_ms"]["n"] < 100 and small["write_call_ms"]["lower_bound"]["p99"] is None and "below that the p99 is the maximum" in small["write_call_ms"]["p99_note"]
    values = [0.1, 0.2, 0.3, 0.4]
    assert ts.aggregate([ts.sample("T01", "r", 0, i, v, v, "x", ["c"]) for i, v in enumerate(values)])["lower_bound"]["median"] == 0.2          # ceil(0.5*4)=2nd, not an interpolation
    assert big["write_call_ms"]["percentile_method"] == ts.PERCENTILE_METHOD


# ----------------------------------------------------------------------------------------------------------------------------- 39 historical behaviour
def test_39_t02_to_t05_are_the_a0o_results_and_historical_or_foreign_schemas_are_refused(tmp_path):
    rec = spool_recorder(tmp_path)
    gen = scripted_log(tmp_path / "g")
    historical = ts.run_sampler(origin=ORIGIN, issue_log=gen.log.path, spool=rec.spool_path)
    cfg = timed_ingest(rec, tmp_path, spool_domain="x", bridge_domain="y")
    side = forge_sidecar(gen.log.path, tmp_path / "g" / "side.jsonl")
    wrapped = td.run_sampler_a0p(origin=ORIGIN, issue_log=gen.log.path, issue_log_timing=side, spool=rec.spool_path, ingest_ledger=cfg.ledger_path)
    for name in ("T02", "T03", "T04", "T05"):
        assert wrapped["measurements"][name] == historical["measurements"][name], name
    assert wrapped["measurements"]["T01"]["measurement"] == "T01" and wrapped["measurements"]["T01"]["status"] == ts.S_MEASURED
    assert historical["measurements"]["T06"]["status"] == ts.S_NOT_MEASURABLE and wrapped["measurements"]["T06"]["status"] == ts.S_NOT_MEASURABLE
    assert "T06" in wrapped["measurements"] and wrapped["measurements"]["T06"]["bridge_internal"]["eligible_samples"] == 22
    assert "T07 to T12" in wrapped["not_covered"][0]
    with pytest.raises(ts.TimingSamplerError) as no_log:
        td.run_sampler_a0p(origin=ORIGIN, issue_log_timing=side)
    assert no_log.value.code == "issue_log_required"
    with pytest.raises(ts.TimingSamplerError) as live:
        td.run_sampler_a0p(origin="live_recorder", ingest_ledger=cfg.ledger_path)
    assert live.value.code == "origin_not_supported"
    foreign = [json.loads(line) for line in lines_of(cfg.ledger_path)]
    foreign[0]["schema"] = "recorder_health_ingest/2"
    write_lines(tmp_path / "v2.ledger.jsonl", [wc.canonical(r).encode("ascii") for r in foreign])
    with pytest.raises(ts.TimingSamplerError) as schema:
        td.read_ingest_ledger(tmp_path / "v2.ledger.jsonl")
    assert schema.value.code == "ledger_schema_mismatch"
    old = [dict(r) for r in rows_of(side)]
    for r in old:
        r["schema"] = "e1_issue_log_timing/2"
    write_lines(tmp_path / "v2.side.jsonl", chain(old))
    assert ilt.verify_timing(gen.log.path, tmp_path / "v2.side.jsonl").error["code"] == "sidecar_schema_mismatch"
