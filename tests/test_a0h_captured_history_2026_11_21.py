"""A0h: the captured workload-history adapter (2026-11-21). Checklist items 1-16.

Offline. Synthetic days and temporary files only, except the two tests that READ the stored Flashbots series (hash-verified, never
written). No forecaster or model is executed: the estimator is the labelled fixture.
"""

from __future__ import annotations

import csv
import gzip
import socket
import subprocess
import urllib.request
from datetime import datetime, timedelta, timezone

import pytest

from tests import a0h_support as h
from fabric_perf.controllers import captured_history as ch
from fabric_perf.controllers import stage3_predictive as sp
from fabric_perf.controllers.workload_history import ArrivalHistoryStore


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("a process or network call was attempted")
    for obj, names in ((subprocess, ("run", "Popen", "call", "check_output")), (socket, ("create_connection", "getaddrinfo")),
                       (urllib.request, ("urlopen",))):
        for name in names:
            monkeypatch.setattr(obj, name, boom)


DAY_MS = int(h.DAY_START.timestamp() * 1000)


def write_series(path, counts, *, day=h.DAY, frequency="1", gz=False, stamp=None, header=("timestamp_utc", "arrival_count", "date_utc", "frequency_seconds")):
    """A flashbots_series_1s/1 file. ``counts`` maps second -> count (a second absent from the dict has no row)."""
    opener = gzip.open if gz else open
    start = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    with opener(path, "wt", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(header)
        for second in sorted(counts):
            moment = (start + timedelta(seconds=second))
            writer.writerow([stamp(moment) if stamp else str(moment), counts[second], day, frequency])
    return path


def load(path, **kwargs):
    return ch.load_binned_series(path, h.source(), expected_sha256=ch.sha256_stream(path), **kwargs)


# ------------------------------------------------------------------------------------------------ 1 parsing, 2 UTC, 3 order
def test_01_a_stored_series_is_parsed_into_one_second_counts_and_its_hash_is_checked(tmp_path):
    path = write_series(tmp_path / "s.csv", {0: 5, 1: 0, 3: 7})
    day = load(path)
    assert day.counts[:5] == (5, 0, None, 7, None)                      # second 2 has no row: a hole, not a zero
    assert day.report["rows_read"] == 3 and day.report["series_rows_absent"] == ch.SECONDS_PER_DAY - 3
    assert day.source.dataset_sha256 == ch.sha256_stream(path)
    with pytest.raises(ch.CapturedHistoryError) as caught:
        ch.load_binned_series(path, h.source(), expected_sha256="0" * 64)
    assert caught.value.code == "dataset_hash_mismatch"


def test_01b_gzip_and_plain_files_give_the_same_day(tmp_path):
    counts = {i: 10 + i % 3 for i in range(50)}
    assert load(write_series(tmp_path / "a.csv", counts)).counts == load(write_series(tmp_path / "b.csv.gz", counts, gz=True)).counts


def test_02_timestamps_are_normalised_to_utc_and_naive_ones_are_refused(tmp_path):
    shifted = write_series(tmp_path / "p.csv", {0: 5, 1: 6},
                           stamp=lambda m: m.astimezone(timezone(timedelta(hours=5, minutes=30))).isoformat())
    assert load(shifted).counts[:2] == (5, 6)                             # same instants written with a +05:30 offset
    naive = write_series(tmp_path / "n.csv", {0: 5}, stamp=lambda m: m.replace(tzinfo=None).isoformat())
    with pytest.raises(ch.CapturedHistoryError) as caught:
        load(naive)
    assert caught.value.code == "timestamp_naive"


def test_03_a_series_must_be_chronological_without_duplicates(tmp_path):
    path = tmp_path / "o.csv"
    with path.open("w", newline="") as stream:
        stream.write("timestamp_utc,arrival_count,date_utc,frequency_seconds\n"
                     "2026-09-21 00:00:05+00:00,1,2026-09-21,1\n2026-09-21 00:00:04+00:00,1,2026-09-21,1\n")
    with pytest.raises(ch.CapturedHistoryError) as caught:
        load(path)
    assert caught.value.code == "series_not_chronological"
    path.write_text("timestamp_utc,arrival_count,date_utc,frequency_seconds\n"
                    "2026-09-21 00:00:05+00:00,1,2026-09-21,1\n2026-09-21 00:00:05+00:00,1,2026-09-21,1\n")
    with pytest.raises(ch.CapturedHistoryError) as caught:
        load(path)
    assert caught.value.code == "series_not_chronological"


def test_03b_the_store_hands_out_chronologically_ordered_contiguous_bins():
    captured = h.history()
    captured.advance_to(h.iso(h.at(900)))
    times, counts, _ = captured.store.snapshot(now_utc=h.iso(h.at(900)))
    parsed = [datetime.fromisoformat(t) for t in times]
    assert all(b - a == timedelta(seconds=1) for a, b in zip(parsed, parsed[1:])) and parsed[0] == h.DAY_START and len(times) == 900


# ------------------------------------------------------------------------------------------------ 4 binning, 5 dedup
RECORDS = [("0xa", DAY_MS + 0), ("0xb", DAY_MS + 999), ("0xc", DAY_MS + 1000), ("0xd", DAY_MS + 1000), ("0xe", DAY_MS + 5_000)]


def test_04_transactions_are_binned_into_deterministic_one_second_bins():
    day = ch.bin_transactions(RECORDS, h.source())
    assert day.counts[:6] == (2, 2, 0, 0, 0, 1)                           # 999 ms is still second 0; 1000 ms opens second 1
    assert ch.bin_transactions(list(reversed(RECORDS)), h.source()).counts == day.counts     # order of arrival does not matter
    assert ch.bin_transactions(RECORDS, h.source()).counts == day.counts                     # and the result is reproducible
    assert sum(day.counts) == len(RECORDS) and len(day.counts) == ch.SECONDS_PER_DAY


def test_05_a_duplicate_transaction_id_is_counted_once_and_reported():
    day = ch.bin_transactions(RECORDS + [("0xa", DAY_MS + 0), ("0xe", DAY_MS + 9_000)], h.source())
    assert day.counts[0] == 2 and day.counts[9] == 0 and sum(day.counts) == len(RECORDS)    # the second 0xe is dropped wherever it is stamped
    assert day.report["duplicates_dropped"] == 2 and day.report["unique_transactions"] == len(RECORDS)
    assert "first occurrence" in day.report["duplicate_handling"]
    twin = ch.bin_transactions(RECORDS + [("0xf", DAY_MS + 5_000)], h.source())               # a different id in the same millisecond is a new arrival
    assert twin.counts[5] == 2


# ------------------------------------------------------------------------------------------------ 6 zero bins, 7 unknown coverage
def test_06_a_zero_inside_attested_coverage_is_a_genuine_zero():
    attestation = ch.CoverageAttestation(((h.iso(h.at(0)), h.iso(h.at(2000))),), attested_by="owner (synthetic)", basis="synthetic collector log")
    captured = h.history({100: 0, 101: 0}, attestation=attestation)
    captured.advance_to(h.iso(h.at(700)))
    times, counts, ref = captured.store.snapshot(now_utc=h.iso(h.at(700)))
    assert ref.status == "complete" and len(times) == 700 and counts[100] == 0 and counts[101] == 0 and ref.unavailable_seconds == 0
    assert captured.counts_fed["zero_attested"] == 2
    evidence = captured.evidence(h.iso(h.at(700)))
    assert evidence["validation_outcome"] == ch.VALID_COMPLETE and evidence["live_semantics"] is True


def test_07_a_zero_without_verified_coverage_is_unknown_not_zero():
    captured = h.history({100: 0})
    captured.advance_to(h.iso(h.at(700)))
    times, counts, ref = captured.store.snapshot(now_utc=h.iso(h.at(700)))
    assert (times, counts) == ((), ()) and ref.status == "incomplete"      # the prefix cannot be formed past an unknown second
    assert ref.complete_prefix_bins == 100 and ref.unavailable_seconds == 1 and ref.missing_seconds == 0
    evidence = captured.evidence(h.iso(h.at(700)))
    assert evidence["validation_outcome"] == ch.VALID_INCOMPLETE and evidence["valid_bin_count"] == 0
    assert any("unverified coverage" in reason for reason in evidence["reasons"]) and captured.counts_fed["zero_unverified_unavailable"] == 1


def test_07b_a_second_absent_from_the_series_is_a_hole_never_a_zero(tmp_path):
    counts = {i: 12 for i in range(700) if i != 300}
    captured = ch.CapturedHistory(load(write_series(tmp_path / "h.csv", counts)))
    captured.advance_to(h.iso(h.at(700)))
    _, _, ref = captured.store.snapshot(now_utc=h.iso(h.at(700)))
    assert ref.status == "incomplete" and ref.missing_seconds == 1 and ref.unavailable_seconds == 0 and ref.complete_prefix_bins == 300
    assert captured.counts_fed["absent_from_series"] == 1
    assert captured.evidence(h.iso(h.at(700)))["missing_interval_count"] == 1


def test_07c_the_benchmark_zero_fill_policy_is_explicit_labelled_and_not_live():
    captured = h.history({100: 0}, zero_policy=ch.ZERO_POLICY_BENCHMARK_ZERO_FILL)
    captured.advance_to(h.iso(h.at(700)))
    evidence = captured.evidence(h.iso(h.at(700)))
    assert evidence["validation_outcome"] == ch.VALID_COMPLETE and evidence["live_semantics"] is False
    assert "UNVERIFIED" in evidence["zero_semantics"] and captured.counts_fed["zero_assumed_benchmark"] == 1
    attestation = ch.CoverageAttestation(((h.iso(h.at(0)), h.iso(h.at(10))),), attested_by="x", basis="y")
    with pytest.raises(ch.CapturedHistoryError) as caught:                 # two answers to one question are refused
        h.history(zero_policy=ch.ZERO_POLICY_BENCHMARK_ZERO_FILL, attestation=attestation)
    assert caught.value.code == "zero_policy_conflict"
    with pytest.raises(ch.CapturedHistoryError):
        h.history(zero_policy="assume_zero")


def test_07d_an_attestation_must_name_who_and_on_what_basis_and_be_ordered():
    with pytest.raises(ch.CapturedHistoryError) as caught:
        ch.CoverageAttestation(((h.iso(h.at(0)), h.iso(h.at(10))),))
    assert caught.value.code == "attestation_unsourced"
    with pytest.raises(ch.CapturedHistoryError):
        ch.CoverageAttestation(((h.iso(h.at(10)), h.iso(h.at(0))),), attested_by="x", basis="y")
    with pytest.raises(ch.CapturedHistoryError):
        ch.CoverageAttestation(((h.iso(h.at(0)), h.iso(h.at(10))), (h.iso(h.at(5)), h.iso(h.at(20)))), attested_by="x", basis="y")


# ------------------------------------------------------------------------------------------------ 8 minimum, 9 prefix
def forecaster_and_guard():
    inner = sp.TimestampedFixtureForecaster("fixture-ts", 55.0)
    return inner, sp.GuardedForecaster(inner, expected_identity="fixture-ts")


def history_at(seconds, **kwargs):
    captured = h.history(**kwargs)
    captured.advance_to(h.iso(h.at(seconds)))
    return captured, *captured.store.snapshot(now_utc=h.iso(h.at(seconds)))


def test_08_the_minimum_history_is_600_bins_at_the_adapter_and_at_the_guard():
    inner, guard = forecaster_and_guard()
    captured, times, counts, ref = history_at(599)
    assert captured.evidence(h.iso(h.at(599)))["validation_outcome"] == ch.VALID_INSUFFICIENT and len(times) == 599
    with pytest.raises(sp.PredictiveIntegrationError) as caught:
        guard.predict_timestamped(times, counts, 60, now_utc=h.iso(h.at(599)), max_history_age_s=5.0, history_ref=ref)
    assert caught.value.code == "forecast_history_insufficient" and inner.calls == 0
    captured, times, counts, ref = history_at(600)
    assert captured.evidence(h.iso(h.at(600)))["validation_outcome"] == ch.VALID_COMPLETE
    assert guard.predict_timestamped(times, counts, 60, now_utc=h.iso(h.at(600)), max_history_age_s=5.0, history_ref=ref).value_tps == 55.0
    assert inner.calls == 1


def test_09_only_the_complete_utc_day_prefix_is_offered():
    captured, times, counts, ref = history_at(900)
    assert times[0] == "2026-09-21T00:00:00+00:00" and ref.first_bin_timestamp_utc == times[0]
    _, guard = forecaster_and_guard()
    tail = (times[300:], counts[300:])                                    # a window that does not start at midnight
    with pytest.raises(sp.PredictiveIntegrationError) as caught:
        guard.predict_timestamped(*tail, 60, now_utc=h.iso(h.at(900)), max_history_age_s=5.0)
    assert caught.value.code == "forecast_history_not_utc_day_prefix"
    empty = h.history()
    empty.advance_to(h.iso(h.at(0)))                                     # at 00:00:00 the current day has no bin yet
    assert empty.evidence(h.iso(h.at(0)))["validation_outcome"] == ch.VALID_EMPTY


# ------------------------------------------------------------------------------------------------ 10 missing, 11 malformed
@pytest.mark.parametrize("record,code", [
    (("0xa", None), "timestamp_missing"), (("0xa", ""), "timestamp_missing"), ((None, DAY_MS), "transaction_id_missing"),
    (("", DAY_MS), "transaction_id_missing")])
def test_10_a_record_without_a_timestamp_or_id_is_refused_not_dropped(record, code):
    with pytest.raises(ch.CapturedHistoryError) as caught:
        ch.bin_transactions([("0x1", DAY_MS), record], h.source())
    assert caught.value.code == code


@pytest.mark.parametrize("stamp", ["abc", 12.5, True, float("nan"), "12.5"])
def test_11_a_malformed_timestamp_is_refused(stamp):
    with pytest.raises(ch.CapturedHistoryError) as caught:
        ch.bin_transactions([("0xa", stamp)], h.source())
    assert caught.value.code == "timestamp_malformed"


@pytest.mark.parametrize("stamp", [DAY_MS - 1, DAY_MS + 86_400_000])
def test_11b_a_timestamp_outside_the_utc_day_is_refused(stamp):
    with pytest.raises(ch.CapturedHistoryError) as caught:
        ch.bin_transactions([("0xa", stamp)], h.source())
    assert caught.value.code == "timestamp_outside_day"


def test_11c_series_timestamps_must_be_parseable_whole_seconds(tmp_path):
    for body, code in (("not-a-time,5,2026-09-21,1\n", "timestamp_malformed"), (",5,2026-09-21,1\n", "timestamp_malformed"),
                       ("2026-09-21 00:00:00.500+00:00,5,2026-09-21,1\n", "timestamp_not_whole_second"),
                       ("2026-09-20 23:59:59+00:00,5,2026-09-21,1\n", "timestamp_outside_day"),
                       ("2026-09-21 00:00:00+00:00,-3,2026-09-21,1\n", "count_invalid"),
                       ("2026-09-21 00:00:00+00:00,2.5,2026-09-21,1\n", "count_invalid")):
        path = tmp_path / "bad.csv"
        path.write_text("timestamp_utc,arrival_count,date_utc,frequency_seconds\n" + body, encoding="utf-8")
        with pytest.raises(ch.CapturedHistoryError) as caught:
            load(path)
        assert caught.value.code == code, body
    path.write_text("when,how_many\n2026-09-21 00:00:00+00:00,5\n", encoding="utf-8")
    with pytest.raises(ch.CapturedHistoryError) as caught:
        load(path)
    assert caught.value.code == "series_schema_mismatch"


# ------------------------------------------------------------------------------------------------ 12 leakage
def test_12_no_bin_that_was_incomplete_at_the_cutoff_is_ever_visible():
    captured = h.history()
    cutoff = h.at(1000) + timedelta(milliseconds=900)                    # 0.9 s into second 1000: that bin is not complete
    captured.advance_to(h.iso(cutoff))
    times, _, _ = captured.store.snapshot(now_utc=h.iso(cutoff))
    assert datetime.fromisoformat(times[-1]) == h.at(999) and len(times) == 1000
    evidence = captured.evidence(h.iso(cutoff))
    assert evidence["no_future_data"] is True and evidence["history_end_utc"] == times[-1]
    captured.advance_to(h.iso(h.at(1001)))                               # a later cutoff reveals the next bin, and only then
    assert len(captured.store.snapshot(now_utc=h.iso(h.at(1001)))[0]) == 1001


def test_12b_decision_cutoffs_cannot_move_backwards_or_leave_the_day():
    captured = h.history()
    captured.advance_to(h.iso(h.at(1000)))
    with pytest.raises(ch.CapturedHistoryError) as caught:
        captured.advance_to(h.iso(h.at(999)))
    assert caught.value.code == "cutoff_regressed"
    for outside in (h.DAY_START - timedelta(seconds=1), h.DAY_START + timedelta(days=1, seconds=1)):
        with pytest.raises(ch.CapturedHistoryError) as caught:
            h.history().advance_to(h.iso(outside))
        assert caught.value.code == "cutoff_outside_captured_day"


def test_12c_the_guard_refuses_a_history_whose_last_bin_had_not_completed():
    _, times, counts, ref = history_at(900)
    _, guard = forecaster_and_guard()
    early = h.iso(h.at(899) + timedelta(milliseconds=500))              # the decision predates the last bin's end
    with pytest.raises(sp.PredictiveIntegrationError) as caught:
        guard.predict_timestamped(times, counts, 60, now_utc=early, max_history_age_s=5.0, history_ref=ref)
    assert caught.value.code == "forecast_input_from_future"


# ------------------------------------------------------------------------------------------------ 13 horizon, 14 duration, 15 stale
def test_13_a_forecast_horizon_other_than_60_is_refused_before_the_estimator_runs():
    inner, guard = forecaster_and_guard()
    _, times, counts, ref = history_at(900)
    for wrong in (10, 59, 61, 300, True):
        with pytest.raises(sp.PredictiveIntegrationError) as caught:
            guard.predict_timestamped(times, counts, wrong, now_utc=h.iso(h.at(900)), max_history_age_s=5.0, history_ref=ref)
        assert caught.value.code == "forecast_horizon_unavailable"
    assert inner.calls == 0


def test_14_a_bin_duration_other_than_one_second_is_refused(tmp_path):
    with pytest.raises(ch.CapturedHistoryError) as caught:
        load(write_series(tmp_path / "f5.csv", {0: 5, 5: 6}, frequency="5"))
    assert caught.value.code == "bin_duration_mismatch"
    _, times, counts, _ = history_at(900)
    _, guard = forecaster_and_guard()
    spaced = tuple((h.DAY_START + timedelta(seconds=2 * i)).isoformat() for i in range(len(times)))
    with pytest.raises(sp.PredictiveIntegrationError) as caught:
        guard.predict_timestamped(spaced, counts, 60, now_utc=h.iso(h.at(2 * 900)), max_history_age_s=5.0)
    assert caught.value.code == "forecast_history_timestamps_inconsistent"
    with pytest.raises(ch.CapturedHistoryError):                          # a day that is not 86,400 one-second bins is not a captured day
        ch.CapturedHistory(ch.BinnedDay(h.source(), (5,) * 100, {}))


def test_15_stale_history_is_refused_and_the_age_limit_is_inclusive():
    inner, guard = forecaster_and_guard()
    _, times, counts, ref = history_at(900)
    last_start = h.at(899)
    ok = guard.predict_timestamped(times, counts, 60, now_utc=h.iso(last_start + timedelta(seconds=5)), max_history_age_s=5.0, history_ref=ref)
    assert ok.value_tps == 55.0                                          # age exactly equal to the limit is fresh
    with pytest.raises(sp.PredictiveIntegrationError) as caught:
        guard.predict_timestamped(times, counts, 60, now_utc=h.iso(last_start + timedelta(seconds=5, milliseconds=1)),
                                  max_history_age_s=5.0, history_ref=ref)
    assert caught.value.code == "forecast_history_stale" and inner.calls == 1


# ------------------------------------------------------------------------------------------------ 16 provenance
def test_16_every_evidence_record_names_its_source_and_says_it_is_not_fabric():
    captured = h.history()
    captured.advance_to(h.iso(h.at(900)))
    evidence = captured.evidence(h.iso(h.at(900)))
    source = evidence["source"]
    assert source["source_id"] == "synthetic-test-day" and source["is_fabric_observation"] is False
    assert "NOT Fabric" in source["represents"] and source["kind"] == "ethereum_mempool_first_seen" and source["utc_day"] == h.DAY
    assert evidence["schema"] == "captured_workload_history/1" and evidence["bin_seconds"] == 1 and evidence["forecast_horizon_seconds"] == 60
    assert evidence["history_start_utc"] == "2026-09-21T00:00:00+00:00" and datetime.fromisoformat(evidence["decision_cutoff_utc"]) == h.at(900)
    assert evidence["valid_bin_count"] == 900 and evidence["missing_interval_count"] == 0 and evidence["min_history_bins"] == 600
    assert evidence["history_reference"]["source"] == "synthetic-test-day" and evidence["duplicate_handling"] == "none (synthetic)"
    assert ch.CapturedSource("s", h.DAY).as_dict()["is_fabric_observation"] is False


def test_16b_the_history_store_is_the_existing_arrival_history_store():
    assert isinstance(h.history().store, ArrivalHistoryStore)


# ------------------------------------------------------------------------------------------------ authentic stored data
def real_zero_seconds(path):
    with gzip.open(path, "rt", encoding="utf-8", newline="") as stream:
        return [i for i, row in enumerate(csv.DictReader(stream)) if int(row["arrival_count"]) == 0]


def test_real_day_with_strict_coverage_rules_is_incomplete_from_its_first_unverified_zero():
    zeros = real_zero_seconds(h.SERIES_09_21)
    first = zeros[0]
    assert first < 600                                                   # the stored day's first zero is inside the first 600 s
    day = ch.load_binned_series(h.SERIES_09_21, ch.CapturedSource("flashbots-mempool-2026-09-21", "2026-09-21"), expected_sha256=h.SERIES_09_21_SHA)
    assert day.report["rows_read"] == 86_400 and day.report["zero_count_seconds"] == len(zeros) == 41
    captured = ch.CapturedHistory(day)
    cutoff = h.iso(h.at(12 * 3600))
    captured.advance_to(cutoff)
    evidence = captured.evidence(cutoff)
    assert evidence["validation_outcome"] == ch.VALID_INCOMPLETE and evidence["valid_bin_count"] == 0
    assert evidence["history_reference"]["complete_prefix_bins"] == first
    assert evidence["unavailable_seconds"] == sum(1 for z in zeros if z < 12 * 3600)
    assert evidence["source"]["dataset_sha256"] == h.SERIES_09_21_SHA and evidence["source"]["is_fabric_observation"] is False


def test_real_day_with_the_benchmark_zero_fill_policy_passes_the_real_history_guard():
    day = ch.load_binned_series(h.SERIES_09_21, ch.CapturedSource("flashbots-mempool-2026-09-21", "2026-09-21"), expected_sha256=h.SERIES_09_21_SHA)
    captured = ch.CapturedHistory(day, zero_policy=ch.ZERO_POLICY_BENCHMARK_ZERO_FILL)
    cutoff = h.iso(h.at(12 * 3600))
    captured.advance_to(cutoff)
    times, counts, ref = captured.store.snapshot(now_utc=cutoff)
    assert len(times) == 12 * 3600 and ref.status == "complete" and times[-1] == "2026-09-21T11:59:59+00:00"
    inner, guard = forecaster_and_guard()
    result = guard.predict_timestamped(times, counts, 60, now_utc=cutoff, max_history_age_s=5.0, history_ref=ref)
    assert result.value_tps == 55.0 and inner.calls == 1                 # a SCRIPTED value: the history is real, the estimate is not
    assert result.origin_timestamp_utc == "2026-09-21T12:00:00+00:00"
    assert captured.counts_fed["zero_assumed_benchmark"] == sum(1 for z in real_zero_seconds(h.SERIES_09_21) if z < 12 * 3600)
    assert captured.evidence(cutoff)["live_semantics"] is False
