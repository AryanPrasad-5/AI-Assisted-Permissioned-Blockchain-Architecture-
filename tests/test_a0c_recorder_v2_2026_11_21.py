"""Task A0c (2026-11-21), G2: the v2 recorder emits client telemetry while the round runs.

Runs the REAL ``scripts/e1/caliper/e1_per_tx_recorder_v2.js`` under the local Node.js through the synthetic driver
``tests/e1/js/a0c_live_round.js`` (a fake Caliper dispatch: no Caliper, Docker, WSL or Fabric). Offline; files only under
``tmp_path``. Skipped, and so NOT VERIFIED, where Node.js is not on PATH.
"""

import csv
import hashlib
import json
import os
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from fabric_perf.controllers import ContinuousCaliperLedgerAdapter
from fabric_perf.controllers.client_latency import derive_latency_ms

REPO = Path(__file__).resolve().parents[1]
NODE = shutil.which("node")
DRIVER = REPO / "tests" / "e1" / "js" / "a0c_live_round.js"
V1 = REPO / "scripts" / "e1" / "caliper" / "e1_per_tx_recorder.js"
V2 = REPO / "scripts" / "e1" / "caliper" / "e1_per_tx_recorder_v2.js"
V1_SHA256 = "b5496af3ca038728ff7490b45db726f27ebf0f7efe09b36f6d9330a8cbd1b0a8"   # the pinned v1 recorder
LIVE_COLUMNS = ["tx_id", "seq_finish", "create_ms", "final_ms", "latency_ms", "latency_state", "status", "verified", "err",
                "obs_wall_ms", "obs_hr_ns", "worker", "round", "label"]
V1_COLUMNS = ["tx_id", "seq_finish", "create_ms", "final_ms", "status", "verified", "err", "obs_hr_ns", "worker", "round",
              "label"]

pytestmark = pytest.mark.skipif(NODE is None, reason="Node.js not on PATH: recorder v2 NOT VERIFIED")


def run_driver(tmp_path, scenario, run_id="RUNA", env=None):
    out = tmp_path / "out"
    completed = subprocess.run([NODE, str(DRIVER), str(out), run_id, scenario], capture_output=True, text=True, timeout=60,
                               env={**os.environ, **(env or {})})
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.strip().splitlines()[-1]), out / f"{run_id}_w0_r1_m-{run_id}"


def read_csv(path):
    with Path(path).open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


# ------------------------------------------------------------------ G2: rows exist before the round ends
def test_rows_are_on_disk_before_deactivate_and_the_final_files_appear_only_after(tmp_path):
    result, base = run_driver(tmp_path, "midround")
    before, after = result["before_deactivate"], result["after_deactivate"]
    assert len(before["live_csv_lines"]) == 1 + 7                       # header + the 7 results finished so far
    assert len(before["live_submits_lines"]) == 1 + 10                  # header + every submit event so far
    assert before["open_marker"] is True
    assert before["final_csv"] is False and before["final_manifest"] is False and before["final_marker"] is False
    assert len(after["live_csv_lines"]) == 1 + 10                       # the last three arrived and were flushed at deactivate
    assert after["final_csv"] and after["final_manifest"] and after["final_marker"]


def test_the_live_stream_is_bounded_buffered_and_the_final_flush_loses_nothing(tmp_path):
    """liveFlushRows 1000 and a 60 s timer: nothing is visible mid-round (the documented granularity), nothing is lost."""
    result, base = run_driver(tmp_path, "nolive")
    assert len(result["before_deactivate"]["live_csv_lines"]) == 1      # only the header: the buffer has not been flushed
    assert len(result["after_deactivate"]["live_csv_lines"]) == 1 + 10
    assert result["final_marker"]["counts"]["tx_rows_written"] == 10 and result["final_marker"]["live_stream_failed"] is False


def test_the_live_stream_reconciles_with_the_final_files_without_double_counting(tmp_path):
    result, base = run_driver(tmp_path, "midround")
    live, final = read_csv(f"{base}.live.csv"), read_csv(f"{base}.csv")
    assert [r["tx_id"] for r in live] == [r["tx_id"] for r in final] == [f"tx{i}" for i in range(10)]   # once each, same order
    for a, b in zip(live, final):
        assert (a["create_ms"], a["final_ms"], a["status"], a["seq_finish"]) == (b["create_ms"], b["final_ms"], b["status"], b["seq_finish"])
    marker = result["final_marker"]
    assert marker["state"] == "finalized" and marker["provisional"] is False
    assert marker["reconciles_with_final_files"] is True and marker["final_files_present"] is True
    assert marker["counts"] == {"results_seen": 10, "tx_rows_pushed": 10, "tx_rows_written": 10, "submit_events_seen": 10,
                                "submit_rows_pushed": 10, "submit_rows_written": 10}
    assert marker["files"]["live_csv_sha256"] == sha(f"{base}.live.csv")           # the marker commits to the content
    assert marker["files"]["live_submits_sha256"] == sha(f"{base}.live.submits.csv")


def test_the_final_files_keep_the_v1_schema_and_the_pinned_v1_recorder_is_untouched(tmp_path):
    result, base = run_driver(tmp_path, "midround")
    header = Path(f"{base}.csv").read_text(encoding="utf-8").splitlines()[0]
    assert header.split(",") == V1_COLUMNS                              # unchanged: pinned consumers still read it
    assert sha(V1) == V1_SHA256
    manifest = json.loads(Path(f"{base}.manifest.json").read_text(encoding="utf-8"))
    assert manifest["recorder"].startswith("e1_per_tx_recorder 0.1.0")  # the final manifest is v1's own


# ------------------------------------------------------------------ G1 in the recorder: the live schema
def test_the_live_schema_carries_a_measured_latency_and_separate_event_and_collection_times(tmp_path):
    result, base = run_driver(tmp_path, "midround")
    live = read_csv(f"{base}.live.csv")
    assert list(live[0]) == LIVE_COLUMNS
    for row in live:
        assert row["latency_state"] == "ok"
        assert float(row["latency_ms"]) == float(row["final_ms"]) - float(row["create_ms"])
        assert int(row["obs_wall_ms"]) > 1_700_000_000_000 and int(row["obs_hr_ns"]) >= 0     # collection time, not event time
    open_marker = json.loads(Path(f"{base}.live.open.json").read_text(encoding="utf-8"))
    assert open_marker["schema"] == "e1_per_tx_live/1" and open_marker["provisional"] is True
    assert "final_ms - create_ms" in open_marker["latency"]["definition"]
    assert "event time" in open_marker["time_semantics"]["create_ms_final_ms"]
    assert "not an event time" in open_marker["time_semantics"]["obs_wall_ms_obs_hr_ns"]


def test_an_error_text_is_one_line_in_the_live_stream_and_raw_in_the_final_csv(tmp_path):
    result, base = run_driver(tmp_path, "midround")
    live = {r["tx_id"]: r for r in read_csv(f"{base}.live.csv")}
    final = {r["tx_id"]: r for r in read_csv(f"{base}.csv")}
    assert "\n" not in live["tx3"]["err"] and "second line" in live["tx3"]["err"]
    assert "\n" in final["tx3"]["err"]                                   # v1's own output is unchanged
    text = Path(f"{base}.live.csv").read_text(encoding="utf-8")
    assert text.endswith("\n") and text.count("\n") == 1 + 10            # one physical line per row: no torn or wrapped row


def test_malformed_timestamps_give_an_empty_latency_with_a_reason_never_zero(tmp_path):
    result, base = run_driver(tmp_path, "edge")
    live = {r["tx_id"]: r for r in read_csv(f"{base}.live.csv")}
    expected = {"ok": ("250", "ok"), "zero_latency": ("0", "ok"), "missing_create": ("", "missing_create_timestamp"),
                "final_before_create": ("", "negative_latency"), "nan_final": ("", "nonfinite"),
                "zero_create": ("", "nonpositive_timestamp"), "failed_missing_create": ("", "missing_create_timestamp")}
    assert {k: (v["latency_ms"], v["latency_state"]) for k, v in live.items()} == expected
    assert live["zero_latency"]["latency_ms"] == "0" and live["missing_create"]["latency_ms"] != "0"


def test_the_javascript_and_python_latency_definitions_agree_on_every_edge_case(tmp_path):
    """Parity: the recorder's latency_state equals client_latency.LatencyState for the same inputs."""
    result, base = run_driver(tmp_path, "edge")
    for row in read_csv(f"{base}.live.csv"):
        sample = derive_latency_ms(row["create_ms"], row["final_ms"])
        assert sample.state.value == row["latency_state"], row["tx_id"]
        assert ("" if sample.latency_ms is None else f"{sample.latency_ms:g}") == row["latency_ms"], row["tx_id"]


def test_a_failed_live_stream_is_reported_and_never_damages_the_authoritative_final_files(tmp_path):
    result, base = run_driver(tmp_path, "livefail")
    marker = result["final_marker"]
    assert marker["live_stream_failed"] is True and marker["reconciles_with_final_files"] is False
    assert marker["first_tx_stream_error"]                                   # the cause is kept
    assert marker["counts"]["results_seen"] == 6 and marker["counts"]["tx_rows_written"] == 3
    assert marker["final_files_present"] is True and result["final_csv_rows"] == 6      # the final CSV is complete regardless
    assert [r["tx_id"] for r in read_csv(f"{base}.csv")] == [f"tx{i}" for i in range(6)]
    assert len(read_csv(f"{base}.live.csv")) == 3                            # only what was written before the failure, no torn row


# ------------------------------------------------------------------ interruption
def test_a_killed_worker_leaves_a_provisional_stream_with_no_final_marker_and_no_torn_row(tmp_path):
    result, base = run_driver(tmp_path, "interrupt")
    assert len(result["before_exit"]["live_csv_lines"]) == 1 + 5
    assert result["before_exit"]["open_marker"] is True
    assert not Path(f"{base}.live.final.json").exists()                  # absence = provisional / interrupted
    assert not Path(f"{base}.csv").exists() and not Path(f"{base}.manifest.json").exists()
    assert Path(f"{base}.live.csv").read_text(encoding="utf-8").endswith("\n")
    assert len(read_csv(f"{base}.live.csv")) == 5


def test_a_second_run_with_the_same_ids_refuses_to_overwrite(tmp_path):
    run_driver(tmp_path, "interrupt")
    again = subprocess.run([NODE, str(DRIVER), str(tmp_path / "out"), "RUNA", "interrupt"], capture_output=True, text=True, timeout=60)
    assert again.returncode == 0                                         # the observer never throws into Caliper...
    assert len(read_csv(tmp_path / "out" / "RUNA_w0_r1_m-RUNA.live.csv")) == 5   # ...and the first run's file was not overwritten


# ------------------------------------------------------------------ option validation
@pytest.mark.parametrize("option, value", [("liveFlushRows", 0), ("liveFlushRows", 1.5), ("liveFlushRows", "10"),
                                           ("liveFlushIntervalMs", 5), ("liveFlushIntervalMs", 0)])
def test_unusable_flush_options_are_refused_at_construction(tmp_path, option, value):
    script = (f"const r=require({json.dumps(str(V2))});"
              f"try{{r.createTxObserver({{runId:'x',outputDir:{json.dumps(str(tmp_path / 'o'))},{option}:{json.dumps(value)}}},null,0);"
              "console.log('constructed')}catch(e){console.log('refused:'+e.message)}")
    completed = subprocess.run([NODE, "-e", script], capture_output=True, text=True, timeout=30)
    assert completed.stdout.startswith("refused:") and option in completed.stdout


def test_the_observer_options_are_validated_like_v1(tmp_path):
    for options in ("{}", "{runId:'x',outputDir:'relative'}"):
        script = (f"const r=require({json.dumps(str(V2))});"
                  f"try{{r.createTxObserver({options},null,0);console.log('constructed')}}catch(e){{console.log('refused')}}")
        assert subprocess.run([NODE, "-e", script], capture_output=True, text=True, timeout=30).stdout.strip() == "refused"


# ------------------------------------------------------------------ the Python adapter reads the live stream
def adapter_for(tmp_path, recorder_lines, *, mode="derive_from_timestamps"):
    recorder, submits, ledger = tmp_path / "recorder.csv", tmp_path / "submits.csv", tmp_path / "ledger.csv"
    recorder.write_text("\n".join(recorder_lines) + "\n", encoding="utf-8")
    submits.write_text("seq_submit,t_submit_evt_ms,count\n", encoding="utf-8")
    ledger.write_text("tx_id,block,index,header_type,validation_code,env_bytes,channel_id,ch_ts_ms\n", encoding="utf-8")
    return ContinuousCaliperLedgerAdapter(recorder_csv=recorder, submits_csv=submits, ledger_csv=ledger, channel_id="ch",
                                          run_id="r", window_s=5, recorder_latency=mode), recorder


def observation_after(base):
    rows = read_csv(f"{base}.live.csv")
    last = max(int(r["final_ms"]) for r in rows)
    return datetime.fromtimestamp(last / 1000, tz=timezone.utc) + timedelta(milliseconds=500)


def test_the_adapter_reads_a_mid_round_live_file_and_counts_each_row_once_as_it_grows(tmp_path):
    result, base = run_driver(tmp_path, "midround")
    before, after = result["before_deactivate"]["live_csv_lines"], result["after_deactivate"]["live_csv_lines"]
    adapter, recorder = adapter_for(tmp_path, before)
    obs = observation_after(base)
    first = adapter.collect(observation_timestamp_utc=obs.isoformat())
    assert first.health["totals"]["completed"] == 7 and first.health["totals"]["failed"] == 1
    assert first.health["integrity"]["rows_rejected"]["recorder"] == 0
    successes = [float(r["latency_ms"]) for r in read_csv(f"{base}.live.csv") if r["status"] == "success" and r["tx_id"] in {f"tx{i}" for i in range(7)}]
    assert first.metrics["latency_mean_ms"].value == pytest.approx(sum(successes) / len(successes))
    with recorder.open("a", encoding="utf-8") as fh:                     # the rest of the round arrives
        fh.write("\n".join(after[len(before):]) + "\n")
    second = adapter.collect(observation_timestamp_utc=(obs + timedelta(seconds=1)).isoformat())
    assert second.health["totals"]["completed"] == 10 and second.health["cross_source"]["duplicate_ids"] == 0


def test_a_torn_last_line_is_not_read_until_it_is_complete(tmp_path):
    result, base = run_driver(tmp_path, "midround")
    lines = result["after_deactivate"]["live_csv_lines"]
    full = "\n".join(lines) + "\n"
    cut = len(full) - 25                                                 # the last row is cut in the middle
    adapter, recorder = adapter_for(tmp_path, [])
    recorder.write_text(full[:cut], encoding="utf-8")
    obs = observation_after(base)
    assert adapter.collect(observation_timestamp_utc=obs.isoformat()).health["totals"]["completed"] == 9
    with recorder.open("a", encoding="utf-8") as fh:
        fh.write(full[cut:])
    done = adapter.collect(observation_timestamp_utc=(obs + timedelta(seconds=1)).isoformat())
    assert done.health["totals"]["completed"] == 10 and done.health["integrity"]["rows_rejected"]["recorder"] == 0


def test_the_javascript_and_python_latencies_cross_check_inside_the_adapter(tmp_path):
    """Derive mode recomputes final-create and compares it with the recorder's own latency_ms column: no row is rejected."""
    result, base = run_driver(tmp_path, "midround")
    adapter, _ = adapter_for(tmp_path, result["after_deactivate"]["live_csv_lines"])
    snapshot = adapter.collect(observation_timestamp_utc=observation_after(base).isoformat())
    assert snapshot.health["integrity"]["rows_rejected"]["recorder"] == 0 and snapshot.health["integrity"]["status"] == "OK"
    assert snapshot.health["totals"]["completed"] == 10


def test_default_column_mode_also_reads_the_v2_live_file_because_it_carries_latency_ms(tmp_path):
    result, base = run_driver(tmp_path, "midround")
    adapter, _ = adapter_for(tmp_path, result["after_deactivate"]["live_csv_lines"], mode="column")
    snapshot = adapter.collect(observation_timestamp_utc=observation_after(base).isoformat())
    assert snapshot.health["integrity"]["rows_rejected"]["recorder"] == 0
    assert snapshot.metrics["latency_mean_ms"].value is not None


def test_a_round_end_only_v1_file_gives_no_fresh_latency_until_the_round_has_ended(tmp_path):
    """The v1 limitation, shown: nothing exists mid-round, and after the round the old event times are outside the window."""
    adapter, recorder = adapter_for(tmp_path, [])
    recorder.unlink()                                                    # v1 has written nothing yet
    snapshot = adapter.collect(observation_timestamp_utc=datetime.now(timezone.utc).isoformat())
    assert snapshot.metrics["latency_mean_ms"].value is None
    assert snapshot.health["read_status"]["status"] != "SUCCESSFUL"
