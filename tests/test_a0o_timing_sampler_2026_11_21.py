"""A0o workstream C (checklist items 16-28): the post-hoc timing sampler for T01-T06. Synthetic fixtures and the STORED scripted-clock A0m evidence; offline only.

Every value computed here is fixture arithmetic. None is a Caliper or Fabric latency, and the scripted clocks of the stored evidence carry no timing information of their own."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import socket
import subprocess
from pathlib import Path

import pytest

from tests.a0l_support import ms
from tests.a0o_support import DAY_MS, DOMAIN, LIMITS, REPO, STORED, SyntheticIssueLog, Scripted, a, at, ident, sr, ts

ORIGIN = "synthetic_fixture"
STORED_ORIGIN = "stored_scripted_clock_evidence"
SPOOL = STORED / "round1/recorder/a0m-dry-001_w0_r1_m-a0m-dry-001.health.jsonl"
SUBMITS = STORED / "round1/recorder/a0m-dry-001_w0_r1_m-a0m-dry-001.live.submits.csv"
STORED_LOG = STORED / "round1/issuelog/a0m-dry-001_w0_r1_m-a0m-dry-001.issuelog.jsonl"
SCRIPT = REPO / "scripts" / "e1" / "e1_a0o_timing_sampler.py"
NOT_MEASURABLE = "NOT_MEASURABLE_FROM_EXISTING_EVIDENCE"
NL_TEXT = chr(10)


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a process or network use is forbidden")
    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(socket, "socket", refuse)


def timed_log(tmp: Path, *, name="t", widths=(0, 3, 7, 12, 1), round_index=1, dangling_intent=False) -> tuple[Path, list[int]]:
    """A scripted log whose k-th attempt record carries a monotonic stamp ``widths[k]`` ms after its intent. Returns the path and the attempt wall stamps."""
    log = SyntheticIssueLog(tmp / f"{name}.jsonl", tick_interval_ms=1000, round_index=round_index)
    gen = Scripted(log).start(-0.5)
    stamps = []
    for k, w in enumerate(widths):
        t = 0.1 + 0.2 * k
        n = log.intent(t)
        log.attempt(t, n, mono_ms=ms(t) - log.t0_ms + w)
        log.outcome(t + 0.1, n)
        gen.intents += 1
        gen.attempted += 1
        gen.outcomes += 1
        stamps.append(ms(t))
    if dangling_intent:
        log.intent(1.2)                                                                     # an intent that never got its attempt record
    else:
        gen.tick(1.2)
        gen.stop(1.3)
    return log.path, stamps


def submits_file(path: Path, stamps: list[int], lags: list[int], *, counts=None, divisor=1) -> Path:
    rows = ["seq_submit,t_submit_evt_ms,count"] + [f"{i},{(s + lag) // divisor},{1 if counts is None else counts[i]}" for i, (s, lag) in enumerate(zip(stamps, lags))]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def replayed_checkpoint(tmp: Path, log: Path, schedule) -> Path:
    work = tmp / "rp"
    sr.replay_log(ident(work / "w.jsonl", checkpoint=work / "ck.json"), source_log=log, schedule=schedule, policy_limits=LIMITS)
    return work / "ck.json"


def respool(tmp: Path, mutate, name="spool.jsonl") -> Path:
    rows = [json.loads(line) for line in SPOOL.read_text().splitlines()]
    rows = mutate(rows)
    for number, row in enumerate(rows, 1):
        row["n"] = number
    out = tmp / name
    out.write_text("\n".join(json.dumps(r, separators=(",", ":")) for r in rows) + "\n", encoding="utf-8")
    return out


def run(**kw):
    kw.setdefault("origin", ORIGIN)
    return ts.run_sampler(**kw)


# ----------------------------------------------------------------------------------------------------------------------------- 16 valid elapsed samples
def test_16_valid_elapsed_samples_follow_the_specified_formulas(tmp_path):
    log, stamps = timed_log(tmp_path)
    lags = [7, 9, 3, 150, 20]
    doc = run(issue_log=log, submits=submits_file(tmp_path / "s.csv", stamps, lags), submits_clock_domain=DOMAIN)
    t01, t02 = doc["measurements"]["T01"], doc["measurements"]["T02"]
    assert t01["status"] == ts.S_MEASURED and t01["eligible_samples"] == 5
    assert [(s["value_lo_ms"], s["value_hi_ms"]) for s in t01["samples"]] == [(max(0, w - 1), w + 1) for w in (0, 3, 7, 12, 1)]      # [W-1, W+1] floored at 0
    assert t02["status"] == ts.S_MEASURED and [(s["value_lo_ms"], s["value_hi_ms"]) for s in t02["samples"]] == [(max(0, d - 1), d + 1) for d in lags]
    assert all(s["schema"] == "a0n_timing_sample/1" and s["valid"] and s["basis"] == "order_matched_unit" for s in t02["samples"])
    assert t02["matched_fraction"] == 1.0 and t01["samples"][0]["clock_pair"] == ["worker_mono"]


def test_16b_the_stored_a0m_evidence_gives_t04_t05_with_a_scripted_clock_label(tmp_path):
    doc = run(origin=STORED_ORIGIN, spool=SPOOL, candidate_gap_limit_s=5.0)
    t04, t05 = doc["measurements"]["T04"], doc["measurements"]["T05"]
    assert t04["status"] == ts.S_MEASURED and t04["eligible_samples"] == 3 and t04["exclusion_counts"] == {"missing": 1}
    assert {s["value_lo_ms"] for s in t04["samples"]} == {1000.0} and {s["lateness_ms"] for s in t04["samples"]} == {0.0} and t04["window_close_rows_excluded"] == 1
    assert t05["max_gap_s"] == 1.0 and t05["count_above_candidate_limit"] == 0 and t05["candidate_limit_s"] == 5.0
    assert [s["kind"] for s in t05["samples"]] == ["leading", "interior", "interior", "interior", "trailing"] and t05["unbounded_gap_count"] == 0
    assert "unverified engineering candidate" in t05["candidate_limit_basis"]
    assert doc["measurements"]["T05"]["provenance"].startswith("offline fixture arithmetic") and doc["evidence_origin"] == STORED_ORIGIN


def test_16c_an_error_row_is_counted_as_an_unbounded_gap_and_a_missing_stop_is_not_a_sample(tmp_path):
    def mutate(rows):
        err = {**rows[3], "kind": "error", "code": "live_submit_stream_failed"}
        for key in ("observed_through_utc", "events_received", "health", "phase", "errors_since_last", "last_event_seq"):
            err.pop(key, None)
        return rows[:3] + [err] + rows[3:]
    t05 = run(origin=STORED_ORIGIN, spool=respool(tmp_path, mutate))["measurements"]["T05"]
    assert t05["unbounded_gap_count"] == 1 and "frozen" in t05["unbounded_gaps"][0]["reason"]
    no_stop = run(origin=STORED_ORIGIN, spool=respool(tmp_path, lambda rows: [r for r in rows if r["kind"] != "stop"], "nostop.jsonl"))["measurements"]["T05"]
    assert no_stop["exclusion_counts"] == {"missing": 1} and all(s["kind"] != "trailing" for s in no_stop["samples"])


# ----------------------------------------------------------------------------------------------------------------------------- 17 missing timestamps
def test_17_a_missing_field_or_input_is_counted_as_missing_or_not_measurable_never_filled(tmp_path):
    log, stamps = timed_log(tmp_path, name="dang", dangling_intent=True)
    t01 = run(issue_log=log)["measurements"]["T01"]
    assert t01["eligible_samples"] == 5 and t01["exclusion_counts"] == {"missing": 1} and t01["missing_count"] == 1 and t01["invalid_count"] == 0
    t02 = run(issue_log=log)["measurements"]["T02"]
    assert t02["status"] == NOT_MEASURABLE and "live.submits.csv" in t02["missing_dependency"] and t02["aggregate"] is None
    no_policy = respool(tmp_path, lambda rows: [{k: v for k, v in rows[0].items() if k != "declared_policy"}] + rows[1:], "nopol.jsonl")
    assert run(origin=STORED_ORIGIN, spool=no_policy)["measurements"]["T04"]["status"] == NOT_MEASURABLE
    no_start = respool(tmp_path, lambda rows: [{k: v for k, v in rows[0].items() if k != "observation_start_utc"}] + rows[1:], "nostart.jsonl")
    t05 = run(origin=STORED_ORIGIN, spool=no_start)["measurements"]["T05"]
    assert t05["status"] == NOT_MEASURABLE and "observation_start_utc" in t05["missing_dependency"]
    nothing = run()["measurements"]
    assert {k: v["status"] for k, v in nothing.items()} == {k: NOT_MEASURABLE for k in ("T01", "T02", "T03", "T04", "T05", "T06")}
    assert all(v["missing_dependency"] for v in nothing.values())


# ----------------------------------------------------------------------------------------------------------------------------- 18 incompatible clocks
def test_18_two_clock_domains_are_never_differenced_without_an_offset_interval(tmp_path):
    log, stamps = timed_log(tmp_path)
    sub = submits_file(tmp_path / "s.csv", stamps, [5] * 5)
    t02 = run(issue_log=log, submits=sub, submits_clock_domain="recorder-host:node_date_now")["measurements"]["T02"]
    assert t02["status"] == NOT_MEASURABLE and "T12" in t02["missing_dependency"] and t02["samples"] == [] and t02["aggregate"] is None
    sched = [{"read_time": at(1.0), "reveal_records": 10}, {"read_time": at(2.0), "reveal_records": None}]
    ck = replayed_checkpoint(tmp_path, log, sched)
    other = run(issue_log=log, checkpoint=ck, reader_clock_domain="windows-reader:python_wall")["measurements"]["T03"]
    assert other["status"] == NOT_MEASURABLE and "offset interval" in other["missing_dependency"]
    shifted = run(issue_log=log, checkpoint=ck, reader_clock_domain="windows-reader:python_wall", offset_interval_ms=[-10, 10])["measurements"]["T03"]
    same = run(issue_log=log, checkpoint=ck, reader_clock_domain=DOMAIN)["measurements"]["T03"]
    assert shifted["status"] == same["status"] == ts.S_MEASURED and shifted["clock_pair_same_domain"] is False and same["clock_pair_same_domain"] is True
    for s_same, s_shift in zip(same["samples"], shifted["samples"]):
        assert s_shift["offset_interval_ms"] == [-10.0, 10.0] and s_shift["value_hi_ms"] - s_shift["value_lo_ms"] >= s_same["value_hi_ms"] - s_same["value_lo_ms"]
    with pytest.raises(ts.TimingSamplerError) as exc:
        run(issue_log=log, checkpoint=ck, reader_clock_domain=DOMAIN, offset_interval_ms=[5, -5])
    assert exc.value.code == "offset_interval_invalid"


def test_18b_t03_values_are_read_time_minus_generator_issue_time_per_poll(tmp_path):
    log, stamps = timed_log(tmp_path)
    sched = [{"read_time": at(1.0), "reveal_records": 10}, {"read_time": at(2.0), "reveal_records": None}]
    ck = replayed_checkpoint(tmp_path, log, sched)
    t03 = run(issue_log=log, checkpoint=ck, reader_clock_domain=DOMAIN)["measurements"]["T03"]
    expected = []
    for k, stamp in enumerate(stamps):
        rseq = 1 + 3 * k + 2                                                                   # records: 1 start, then intent, attempt, outcome per transaction
        read = ms(1.0) if rseq <= 10 else ms(2.0)                                              # the first poll revealed 10 records, the second the rest
        expected.append(read - stamp)
    got = [s["value_lo_ms"] + 1 for s in t03["samples"]]
    assert got == [float(e) for e in expected] and all(s["basis"] == "read_ms_minus_t_ms" for s in t03["samples"])
    assert "poll interval quantisation is not added" in t03["note"]


# ----------------------------------------------------------------------------------------------------------------------------- 19-20 negative elapsed, units
def test_19_negative_elapsed_values_are_excluded_with_a_reason_and_never_clamped(tmp_path):
    log, stamps = timed_log(tmp_path)
    t02 = run(issue_log=log, submits=submits_file(tmp_path / "s.csv", stamps, [7, -4, 3, 9, 2]), submits_clock_domain=DOMAIN)["measurements"]["T02"]
    assert t02["eligible_samples"] == 4 and t02["exclusion_counts"] == {"invalid": 1} and t02["exclusions"][0]["reason"] == "invalid: assumption_violated"
    assert "never clamped" in t02["exclusions"][0]["detail"] and all(s["value_lo_ms"] >= 0 for s in t02["samples"])
    sched = [{"read_time": at(0.2), "reveal_records": 10}, {"read_time": at(2.0), "reveal_records": None}]
    ck = replayed_checkpoint(tmp_path, log, sched)
    t03 = run(issue_log=log, checkpoint=ck, reader_clock_domain=DOMAIN)["measurements"]["T03"]
    assert t03["exclusion_counts"] == {"invalid": 2} and t03["eligible_samples"] == 3 and all(e["reason"] == "invalid: generator_clock_ahead_of_reader" for e in t03["exclusions"])


def test_19b_a_spool_heartbeat_with_a_regressing_monotonic_clock_is_excluded(tmp_path):
    def mutate(rows):
        rows[3] = {**rows[3], "mono_ms": 100}                                                 # the third heartbeat claims a monotonic time before the second's
        return rows
    t04 = run(origin=STORED_ORIGIN, spool=respool(tmp_path, mutate))["measurements"]["T04"]
    assert t04["exclusion_counts"].get("invalid") == 1 and any(e["reason"] == "invalid: negative_elapsed" for e in t04["exclusions"])
    assert all(s["value_lo_ms"] >= 0 for s in t04["samples"])


def test_20_a_unit_mismatch_is_refused_and_never_rescaled(tmp_path):
    log, stamps = timed_log(tmp_path)
    for tag, divisor in (("seconds", 1000), ("nanoseconds_scaled_up", 0)):
        if divisor:
            sub = submits_file(tmp_path / f"{tag}.csv", stamps, [5] * 5, divisor=divisor)
        else:
            sub = tmp_path / f"{tag}.csv"
            sub.write_text("seq_submit,t_submit_evt_ms,count\n" + "\n".join(f"{i},{s * 1_000_000},1" for i, s in enumerate(stamps)) + "\n")
        t02 = run(issue_log=log, submits=sub, submits_clock_domain=DOMAIN)["measurements"]["T02"]
        assert t02["status"] == ts.S_INVALID and "unit_mismatch" in json.dumps(t02["exclusions"]) and t02["eligible_samples"] == 0 and t02["aggregate"] is None
    assert ts._unit_problem(1_795_219_200_107) is None and ts._unit_problem(1_795_219_200) .startswith("unit_mismatch") and ts._unit_problem(True) == "not_a_number"


# ----------------------------------------------------------------------------------------------------------------------------- 21-23 statistics
def fake(values):
    return [{"valid": True, "value_lo_ms": float(v), "value_hi_ms": float(v) + 1} for v in values]


def test_21_the_median_is_the_nearest_rank_value_not_an_interpolation():
    odd, even = ts.aggregate(fake([5, 1, 3, 2, 4])), ts.aggregate(fake([4, 1, 3, 2]))
    assert odd["lower_bound"]["median"] == 3.0 and odd["upper_bound"]["median"] == 4.0
    assert even["lower_bound"]["median"] == 2.0                                                # ceil(0.5 * 4) = 2nd smallest; no averaging of 2 and 3
    assert odd["percentile_method"].startswith("nearest-rank") and odd["median_rule"] == "nearest-rank at q=0.5"


def test_22_p95_is_the_ceil_q_n_th_smallest_sample():
    agg = ts.aggregate(fake(range(1, 21)))                                                    # n=20: ceil(0.95*20) = 19th smallest
    assert agg["lower_bound"]["p95"] == 19.0 and agg["lower_bound"]["max"] == 20.0 and agg["n"] == 20
    assert ts.aggregate(fake([7]))["lower_bound"]["p95"] == 7.0
    assert ts.aggregate(fake(range(1, 101)))["lower_bound"]["p95"] == 95.0


def test_23_p99_is_reported_only_from_one_hundred_samples():
    small = ts.aggregate(fake(range(1, 100)))
    assert small["lower_bound"]["p99"] is None and "below that the p99 is the maximum" in small["p99_note"] and small["lower_bound"]["max"] == 99.0
    big = ts.aggregate(fake(range(1, 101)))
    assert big["lower_bound"]["p99"] == 99.0 and "p99_note" not in big and math.isclose(big["upper_bound"]["p99"], 100.0)
    assert ts.aggregate(fake(range(1, 201)))["lower_bound"]["p99"] == 198.0


# ----------------------------------------------------------------------------------------------------------------------------- 24-25 empty sets and exclusions
def test_24_an_empty_eligible_set_has_no_statistics_and_is_never_summarised_as_zero(tmp_path):
    assert ts.aggregate([]) is None and ts.aggregate([{"valid": False, "value_lo_ms": 1, "value_hi_ms": 1}]) is None
    log, stamps = timed_log(tmp_path)
    t02 = run(issue_log=log, submits=submits_file(tmp_path / "s.csv", stamps[:3], [5, 5, 5]), submits_clock_domain=DOMAIN)["measurements"]["T02"]      # 3 callbacks for 5 attempts
    assert t02["status"] == ts.S_NONE and t02["eligible_samples"] == 0 and t02["aggregate"] is None and t02["unmatched"] == {"generator_without_recorder": 2, "recorder_without_generator": 0}
    assert {e["reason"] for e in t02["exclusions"]} == {"invalid: total_mismatch"} and "deficit" in t02["exclusions"][0]["detail"]
    surplus = run(issue_log=log, submits=submits_file(tmp_path / "s2.csv", stamps + stamps[:2], [5] * 7), submits_clock_domain=DOMAIN)["measurements"]["T02"]
    assert surplus["unmatched"]["recorder_without_generator"] == 2 and "recorder_exceeds_source" in surplus["exclusions"][0]["detail"]


def test_25_invalid_samples_are_excluded_counted_and_kept_apart_from_the_statistics(tmp_path):
    log, stamps = timed_log(tmp_path)
    t02 = run(issue_log=log, submits=submits_file(tmp_path / "s.csv", stamps, [10, -1, 30, -2, 50]), submits_clock_domain=DOMAIN)["measurements"]["T02"]
    assert t02["eligible_samples"] == 3 and t02["invalid_count"] == 2 and t02["missing_count"] == 0 and t02["exclusion_counts"] == {"invalid": 2}
    assert t02["aggregate"]["n"] == 3 and t02["aggregate"]["upper_bound"]["max"] == 51.0 and sorted(e["index"] for e in t02["exclusions"]) == [1, 3]
    batch_csv = tmp_path / "b.csv"
    rows = [(stamps[1] + 10, 2), (stamps[2] + 10, 1), (stamps[3] + 10, 1), (stamps[4] + 10, 1)]
    batch_csv.write_text(NL_TEXT.join(["seq_submit,t_submit_evt_ms,count"] + [f"{i},{t},{c}" for i, (t, c) in enumerate(rows)]) + NL_TEXT)
    batched = run(issue_log=log, submits=batch_csv, submits_clock_domain=DOMAIN)["measurements"]["T02"]
    assert batched["batch_level_rows"] is True and batched["samples"][0]["basis"] == "order_matched_batch" and batched["samples"][0]["batch_count"] == 2
    assert (batched["samples"][0]["value_lo_ms"], batched["samples"][0]["value_hi_ms"]) == (9, stamps[1] + 10 - stamps[0] + 1)          # [t - last attempt - 1, t - first attempt + 1]


# ----------------------------------------------------------------------------------------------------------------------------- 26 unmeasurable
def test_26_the_end_of_write_and_the_ingest_delay_are_not_measurable_and_name_their_dependency(tmp_path):
    log, stamps = timed_log(tmp_path)
    doc = run(issue_log=log, spool=SPOOL) if False else run(origin=STORED_ORIGIN, issue_log=STORED_LOG, spool=SPOOL)
    t01 = doc["measurements"]["T01"]
    direct = t01["not_measured"]["end_of_write_latency"]
    assert direct["status"] == NOT_MEASURABLE and "end-of-write" in direct["missing_dependency"] and "e1_issue_log/1 stores only t_ms" in direct["missing_dependency"]
    assert t01["status"] == ts.S_MEASURED and "proxy" in t01["quantity_measured"]               # the specification's available proxy, labelled as such
    assert {s["value_lo_ms"] for s in t01["samples"]} == {0} and "below the 1 ms resolution" in t01["quantity_measured"]
    t06 = doc["measurements"]["T06"]
    assert t06["status"] == NOT_MEASURABLE and t06["eligible_samples"] == 0 and t06["samples"] == [] and t06["aggregate"] is None
    assert "ingest timestamp" in t06["missing_dependency"] and "incremental recorder bridge" in t06["missing_dependency"] and t06["ingest_fields_found_in_spool"] == []
    assert "T03" in t06["generator_route"] and "not substituted" in t06["generator_route"]
    unrelated = run(origin=STORED_ORIGIN, spool=SPOOL)["measurements"]["T06"]
    assert unrelated["status"] == NOT_MEASURABLE                                                # no nearby timestamp is used to estimate the duration


# ----------------------------------------------------------------------------------------------------------------------------- 27 provenance
def test_27_fixture_provenance_is_on_every_result_and_a_live_claim_is_refused(tmp_path):
    doc = run(origin=STORED_ORIGIN, issue_log=STORED_LOG, submits=SUBMITS, submits_clock_domain="caliper-worker-host:node_date_now", spool=SPOOL)
    assert doc["evidence_origin"] == STORED_ORIGIN and "No value here is a Caliper, Fabric or live-clock measurement" in doc["statement"]
    for name, m in doc["measurements"].items():
        assert m["evidence_origin"] == STORED_ORIGIN and "NOT Caliper latency" in m["provenance"] and m["specification"].endswith("A0N_TIMING_MEASUREMENT_SPECIFICATION_2026-11-21.md")
    for origin in ("live_caliper", "live_run", "caliper", "", None):
        with pytest.raises(ts.TimingSamplerError) as exc:
            ts.run_sampler(origin=origin, spool=SPOOL)
        assert exc.value.code == "origin_not_supported"
    assert doc["measurements"]["T02"]["time_range"]["first_event_ms"] == 1_795_219_200_107                        # event times, not an observation time


# ----------------------------------------------------------------------------------------------------------------------------- 28 reproducibility and inputs
def test_28_the_output_is_reproducible_and_carries_the_input_digests(tmp_path):
    kwargs = dict(origin=STORED_ORIGIN, issue_log=STORED_LOG, submits=SUBMITS, submits_clock_domain="caliper-worker-host:node_date_now", spool=SPOOL, candidate_gap_limit_s=5.0)
    one, two = ts.run_sampler(**kwargs), ts.run_sampler(**kwargs)
    assert json.dumps(one, sort_keys=True) == json.dumps(two, sort_keys=True)
    digests = {i["role"]: i["sha256"] for i in one["inputs"]}
    assert digests == {"issue_log": hashlib.sha256(STORED_LOG.read_bytes()).hexdigest(), "live_submits": hashlib.sha256(SUBMITS.read_bytes()).hexdigest(),
                       "health_spool": hashlib.sha256(SPOOL.read_bytes()).hexdigest()}
    for m in one["measurements"].values():
        assert all(a_["sha256"] for a_ in m["source_artifacts"])
    spec = importlib.util.spec_from_file_location("e1_a0o_timing_sampler", SCRIPT)
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    before = [STORED_LOG.read_bytes(), SPOOL.read_bytes()]
    argv = ["--origin", STORED_ORIGIN, "--issue-log", str(STORED_LOG), "--spool", str(SPOOL), "--out", str(tmp_path / "t.json")]
    assert cli.main(argv) == 0 and cli.main(argv) == 2                                              # the output path must not exist
    on_disk = json.loads((tmp_path / "t.json").read_text())
    assert on_disk["measurements"]["T06"]["status"] == NOT_MEASURABLE and [STORED_LOG.read_bytes(), SPOOL.read_bytes()] == before
    assert cli.main(["--origin", "live_run", "--spool", str(SPOOL), "--out", str(tmp_path / "x.json")]) == 2


def test_28b_a_damaged_log_or_spool_yields_no_timing(tmp_path):
    log, _ = timed_log(tmp_path)
    data = log.read_bytes().split(b"\n")
    data[4] = data[4].replace(b'"t_ms":', b'"t_ms": ', 1)
    log.write_bytes(b"\n".join(data))
    with pytest.raises(ts.TimingSamplerError) as exc:
        run(issue_log=log)
    assert exc.value.code == "issue_log_invalid"
    torn = tmp_path / "torn.jsonl"
    torn.write_bytes(SPOOL.read_bytes()[:-25])
    doc = run(origin=STORED_ORIGIN, spool=torn)
    assert {doc["measurements"][k]["status"] for k in ("T04", "T05", "T06")} == {ts.S_INVALID} and "torn tail" in doc["measurements"]["T04"]["reason"]
