"""Offline validation of a rendered replay run against Caliper's replay-rate semantics (Phase 2). No Caliper, Docker or WSL is started.

What is established here, and how (labels: SOURCE_READ, OFFLINE_TESTED, CALIPER_LIVE_UNVALIDATED):

  SOURCE_READ   The v0.6.0 source of caliper-core (replayRate.js, rateControl.js, caliper-worker.js, worker-orchestrator.js) was read on
                2026-11-14 from raw.githubusercontent.com (tag v0.6.0). Line numbers in FACTS were counted by hand from unnumbered text and are
                accurate to about 1-2 lines. The container image is hyperledger/caliper:0.6.0 (driver gate S5); the code INSIDE the image was NOT
                inspected (that needs Docker), so "same as the tag" is an assumption. Not read: util.sleep, TransactionStatisticsCollector
                (round start time, counters), the Fabric connector (when TxsSubmitted is emitted), TestMessage.
  OFFLINE_TESTED  The one-line TEXT parser of replayRate.js is transcribed verbatim (PARSE_JS) and run under node on the rendered schedule files;
                the controller's delay rule is transcribed (model_dispatch_times) and tested on synthetic schedules.
  CALIPER_LIVE_UNVALIDATED  Everything that needs a running Caliper: real replay-rate timing, round turnover time, the connector's submit
                accounting, the Fabric behaviour under bursts.

Semantics that matter for the replay (all SOURCE_READ unless stated):
  * replay-rate delays are ABSOLUTE offsets from the round start, `sleep = records[i] - (now - roundStart)`, and a sleep of 5 ms or less is skipped.
    So under ideal conditions a dispatch is never late by the controller's own doing and is early by at most 5 ms (derived, not measured).
  * `i` is Caliper's own submitted-transaction counter, not the workload's `seq`; the two stay in step only if every submitTransaction call
    produces exactly one submitted event (one request per call: true for this workload; the connector side was NOT read).
  * In txNumber mode the round ends when `submitted == txNumber` AND every submitted transaction has finished; the wait polls every 100 ms and has
    NO timeout. There is no 600 s timer. The configured 600 s is a property of the schedule, not of Caliper.
"""

from __future__ import annotations

import csv
import json
import math
import re
import shutil
import subprocess
from pathlib import Path

import numpy as np

from . import PACKAGE_VERSION, ensure_paths
from . import driver as dr
from . import offsets as of

ensure_paths()

CALIPER_TAG = "v0.6.0"
CALIPER_STATUS = "CALIPER_LIVE_UNVALIDATED"
# The expression on replayRate.js line ~96, verbatim apart from the path argument.
PARSE_JS = "const fs=require('fs');const r=fs.readFileSync(process.argv[1],'utf-8').split('\\n').map(line => parseInt(line));process.stdout.write(JSON.stringify(r.map(x=>Number.isNaN(x)?null:x)));"
CALIPER_OPTION_KEYS_READ = frozenset({"pathTemplate", "inputFormat", "defaultSleepTime"})      # replayRate.js:50-67; logChunkSize is NOT read
SKIP_SLEEP_AT_OR_BELOW_MS = 5                                                                     # replayRate.js:135 (`if (sleepTime > 5)`)
DEFAULT_SLEEP_MS = 100                                                                            # replayRate.js:55 (value 0 is also replaced by 100)

FACTS = [
    {"id": "F1", "claim": "TEXT parse is fs.readFileSync(path,'utf-8').split('\\n').map(line => parseInt(line)): split on LF only, parseInt, no trim, no filter",
     "source": "replayRate.js:96", "status": "SOURCE_READ", "consequence": "an empty or non-numeric line inside the first txNumber records is NaN and is dispatched with NO sleep (immediately); "
                                                                          "a trailing newline adds a NaN record that is never reached while txNumber equals the count; CRLF is harmless (parseInt stops at the CR)"},
    {"id": "F2", "claim": "delay = records[idx] - (Date.now() - stats.getRoundStartTime()), idx = stats.getTotalSubmittedTx(); sleep only if > 5 ms; no catch-up",
     "source": "replayRate.js:131-137", "status": "SOURCE_READ", "consequence": "absolute offsets; dispatch early by <= 5 ms at most under ideal conditions; a late dispatch is never compensated"},
    {"id": "F3", "claim": "past-the-end records: one warning then a fixed util.sleep(defaultSleepTime) per call; the round is not stopped",
     "source": "replayRate.js:133-143", "status": "SOURCE_READ", "consequence": "txNumber must equal the record count or the loop would idle at 100 ms per call"},
    {"id": "F4", "claim": "unknown opts keys are not rejected; logChunkSize is not read; pathTemplate <R>/<C> are substituted case-insensitively",
     "source": "replayRate.js:50-77", "status": "SOURCE_READ", "consequence": "the renderer must use only pathTemplate, inputFormat, defaultSleepTime and no placeholder in the path"},
    {"id": "F5", "claim": "txNumber per worker = Math.floor(numb / workers), minimum 1",
     "source": "worker-orchestrator.js:441-451", "status": "SOURCE_READ", "consequence": "one worker gives exactly txNumber"},
    {"id": "F6", "claim": "txNumber loop: while (getTotalSubmittedTx() < number) { applyRateControl(); submitTransaction() not awaited }, then waitForTxsToFinish polls every 100 ms, no timeout",
     "source": "caliper-worker.js:58-64, 90-106", "status": "SOURCE_READ", "consequence": "round end = last submission + commit of every in-flight transaction; unbounded wait, so the driver's hard timeout is the only bound"},
    {"id": "F7", "claim": "no wall-clock stop exists in the txNumber path", "source": "caliper-worker.js (absence)", "status": "SOURCE_READ",
     "consequence": "a 600 s configured round does not exist; the observation period is last offset + commit tail"},
    {"id": "F8", "claim": "the manager waits for every worker with Promise.all, no timeout", "source": "worker-orchestrator.js:482", "status": "SOURCE_READ",
     "consequence": "a transaction that never finishes hangs the round until the driver aborts it"},
    {"id": "U1", "claim": "when the Fabric connector emits TxsSubmitted (before or after the first await of sendRequests)", "source": "NOT READ", "status": "UNREAD",
     "consequence": "decides whether replay-rate's index and the workload's seq stay in step in a burst; a divergence shows as early dispatch (negative lag) and is caught by the lag checks, not prevented"},
    {"id": "U2", "claim": "the gap between the pre-roll round and the measured round", "source": "NOT READ", "status": "UNREAD", "consequence": "not recorded; unknown until a live run"},
    {"id": "U3", "claim": "the code inside the hyperledger/caliper:0.6.0 image equals tag v0.6.0", "source": "NOT CHECKED (needs Docker)", "status": "UNREAD", "consequence": "assumed"},
]


# ----------------------------------------------------------------------------------------------- the parser, transcribed
def js_parse_int(text: str) -> int | None:
    """JavaScript parseInt(text) with no radix: skip leading whitespace, optional sign, '0x' prefix means hex, else leading decimal digits, NaN (None) if none."""
    s = text.lstrip(" \t\n\r\v\f ﻿")
    sign = 1
    if s[:1] in "+-" and s[:1]:
        sign = -1 if s[0] == "-" else 1
        s = s[1:]
    if s[:2].lower() == "0x":
        m = re.match(r"[0-9a-fA-F]+", s[2:])
        return sign * int(m.group(0), 16) if m else None
    m = re.match(r"[0-9]+", s)
    return sign * int(m.group(0)) if m else None


def parse_text_records(body: bytes) -> list[int | None]:
    """replayRate.js:96 in Python: decode, split on LF, parseInt each line (None = NaN)."""
    return [js_parse_int(line) for line in body.decode("utf-8").split("\n")]


def node_parse_records(path: Path, node: str | None = None) -> list[int | None] | None:
    """Run the verbatim JavaScript expression under node on a file. None when node is not installed."""
    exe = node or shutil.which("node")
    if exe is None:
        return None
    proc = subprocess.run([exe, "-e", PARSE_JS, str(path)], capture_output=True, text=True, timeout=60, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"node parse failed: {proc.stderr}")
    return json.loads(proc.stdout)


# ----------------------------------------------------------------------------------------------- the controller delay rule, transcribed
def model_dispatch_times(offsets, *, call_cost_ms: float = 0.0) -> np.ndarray:
    """Virtual-time replay-rate controller for ONE worker (replayRate.js:131-145 + caliper-worker.js:90-98): returns the dispatch time of every record in
    ms from the round start. `call_cost_ms` is a fixed client cost added after each submission (0 = an ideal client). Never late by the controller itself;
    with call_cost_ms = 0 each dispatch is within [offset - 5, offset]. This is a model of the source text, NOT a measurement of a running Caliper."""
    now = 0.0
    out = np.empty(len(offsets), dtype=float)
    for i, off in enumerate(offsets):
        sleep = off - now
        if sleep > SKIP_SLEEP_AT_OR_BELOW_MS:
            now += sleep
        out[i] = now
        now += call_cost_ms
    return out


# ----------------------------------------------------------------------------------------------- explicit round semantics and the run-length budget
def round_semantics(measured_last_offset_ms: int, pre_last_offset_ms: int, *, window_ms: int = of.MEASURED_S * 1000, pre_ms: int = of.PRE_ROLL_S * 1000) -> dict:
    """The facts about 'a 600 s round' that must not be assumed. All derived from the schedule and the SOURCE_READ facts above."""
    return {
        "measured_schedule_window_ms": window_ms, "last_scheduled_offset_ms": int(measured_last_offset_ms),
        "offered_load_span_s": measured_last_offset_ms / 1000.0, "ms_between_last_offset_and_window_end": int(window_ms - measured_last_offset_ms),
        "pre_roll_window_ms": pre_ms, "pre_roll_last_offset_ms": int(pre_last_offset_ms),
        "the_600_s_is": "a property of the schedule (all offsets inside [0, 600000) ms); Caliper has no 600 s timer in txNumber mode (F6, F7)",
        "caliper_round_ends_when": "submitted == txNumber AND every submitted transaction has finished; the wait polls every 100 ms and has no timeout (F6)",
        "observation_period": f"from the round start to the last commit: at least {measured_last_offset_ms / 1000.0:.3f} s plus the commit latency of the last "
                              "transactions, so it is longer than the offered-load span by the drain tail and is NOT exactly 600 s",
        "analysis_cohort_rule": "cohort by INTENDED submission time (P4 draft section 5): a transaction belongs to the measured window by its intended offset, "
                                "not by when it committed, so the drain tail does not change the cohort",
        "round_start_reference": "Caliper's round start is set after the workload module is initialised, so dispatch_ms - intended_ms carries a constant offset "
                                 "(5.5-8.5 ms in run 005); the relative-to-first-dispatch lag removes it (P4 SF-2)",
        "pre_roll_state_carryover": "none for queue state: round 0 waits for its in-flight transactions to finish before round 1 starts (F6), so the pre-roll warms "
                                    "connections, chaincode container and caches but does not hand a loaded ordering queue to the measured round; the turnover gap is not recorded (U2)",
        "ideal_dispatch_error_ms": f"[-{SKIP_SLEEP_AT_OR_BELOW_MS}, 0] from the controller's 5 ms skip rule alone (F2, derived)",
    }


def expected_caliper_seconds(pre_last_ms: int, measured_last_ms: int, *, drain_allowance_s: float = 10.0, turnover_allowance_s: float = 60.0,
                             startup_allowance_s: float = 60.0) -> float:
    """Expected Caliper wall time for one replay: both load spans plus a drain after each round, the round turnover and the launch. The three allowances
    are PROPOSED and have no measurement behind them (the drain one equals the proposed drain deadline)."""
    return pre_last_ms / 1000.0 + measured_last_ms / 1000.0 + 2 * drain_allowance_s + turnover_allowance_s + startup_allowance_s


def run_length_budget(pre_last_ms: int, measured_last_ms: int, caliper_timeout_s: float, host_max_seconds: float, *, n_runs: int = 7,
                      sampler_margin_s: float = 120.0) -> dict:
    """Consistency of the three run-length limits. All must hold or a replay is aborted by its own guard."""
    need = expected_caliper_seconds(pre_last_ms, measured_last_ms)
    checks = {"caliper_timeout_covers_expected_wall_time": caliper_timeout_s >= need,
              "caliper_timeout_covers_phase1_floor": caliper_timeout_s >= dr.MIN_CALIPER_TIMEOUT_S,
              "host_sampler_guard_outlives_caliper_timeout": host_max_seconds >= caliper_timeout_s + sampler_margin_s,
              "p1b_defaults_would_abort": dr.p1b_module().THR.caliper_timeout_s < need and dr.p1b_module().EVID.host_max_seconds < need}
    return {"ok": all(v for k, v in checks.items() if k != "p1b_defaults_would_abort"), "checks": checks, "expected_caliper_s": need,
            "caliper_timeout_s": caliper_timeout_s, "host_max_seconds": host_max_seconds, "timeout_margin_s": caliper_timeout_s - need,
            "keep_awake_seconds": keep_awake_seconds(n_runs, caliper_timeout_s)}


PER_RUN_OVERHEAD_S = 900.0     # PROPOSED: channel creation (limit 300 s), config fetches, block fetch (limit 300 s + 1 s per block), analysis; worst case, unmeasured
SERIES_MARGIN_S = 1200.0       # P1b's margin


def keep_awake_seconds(n_runs: int, caliper_timeout_s: float) -> int:
    """Upper bound on how long the series helper and clock probe must live: every run at its worst case plus the margin. They are stopped at the end
    of the series, so this is a ceiling and not a prediction (P1b's `540 x runs + 1200` was sized for 150 s Caliper runs and would end mid-replay)."""
    return int(math.ceil(n_runs * (caliper_timeout_s + PER_RUN_OVERHEAD_S) + SERIES_MARGIN_S))


# ----------------------------------------------------------------------------------------------- validation of a rendered run folder
def _read_rows(path: Path) -> list[dict]:
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def per_second_counts(offsets_ms, length_s: int) -> np.ndarray:
    return np.bincount(np.asarray(offsets_ms, dtype=np.int64) // 1000, minlength=length_s)[:length_s]


def validate_rendered_run(run_dir: Path, *, caliper_timeout_s: float = dr.CALIPER_TIMEOUT_S_PROPOSED, host_max_seconds: float = dr.HOST_MAX_SECONDS_PROPOSED,
                          use_node: bool = True, burst_rate_tps: int = 40) -> dict:
    """Inspect a rendered replay run folder (benchmark.yaml, schedule_*.txt, tx_map.csv, render_record.json). Reads only. Every check names what it proves.
    The result's `caliper_status` is always CALIPER_LIVE_UNVALIDATED: passing here does not mean Caliper accepted or timed these files."""
    out = Path(run_dir)
    checks: list[dict] = []

    def add(cid: str, name: str, ok: bool, observed=None, expected=None) -> None:
        checks.append({"id": cid, "name": name, "ok": bool(ok), "observed": observed, "expected": expected})

    bench = json.loads((out / "benchmark.yaml").read_text(encoding="utf-8"))
    record = json.loads((out / "render_record.json").read_text(encoding="utf-8"))
    tx_rows = _read_rows(out / "tx_map.csv")
    run_id = record["run_id"]
    rounds = bench["test"]["rounds"]
    names = ("pre_roll", "measured")
    add("V1", "two rounds in execution order with the registered labels", [r["label"] for r in rounds] == [f"wu-{run_id}", f"m-{run_id}"],
        [r["label"] for r in rounds], [f"wu-{run_id}", f"m-{run_id}"])
    add("V3", "one worker (txNumber is floor-divided per worker, F5)", bench["test"]["workers"]["number"] == 1, bench["test"]["workers"]["number"], 1)
    parsed_by_round: dict[str, list] = {}
    files_seen = []
    for name, doc in zip(names, rounds):
        rc = doc["rateControl"]
        opts = rc.get("opts", {})
        path_t = str(opts.get("pathTemplate", ""))
        add(f"V2@{name}", "replay-rate with only the keys Caliper reads, TEXT, no <R>/<C> placeholder, absolute path to this round's file",
            rc.get("type") == "replay-rate" and set(opts) <= CALIPER_OPTION_KEYS_READ and str(opts.get("inputFormat", "")).upper() == "TEXT"
            and not re.search(r"<[RC]>", path_t, re.I) and path_t.startswith("/") and path_t.endswith(f"schedule_{name}.txt"),
            {"type": rc.get("type"), "opts": sorted(opts), "pathTemplate": path_t}, "replay-rate; opts within pathTemplate/inputFormat/defaultSleepTime")
        body = (out / f"schedule_{name}.txt").read_bytes()
        files_seen.append(body)
        py = parse_text_records(body)
        node = node_parse_records(out / f"schedule_{name}.txt") if use_node else None
        parsed_by_round[name] = py
        n = doc["txNumber"]
        args = doc["workload"]["arguments"]
        nan_in_used = sum(1 for x in py[:n] if x is None)
        add(f"V4@{name}", "Caliper's TEXT parse (parseInt of each LF-separated line) yields only numbers in the first txNumber records",
            nan_in_used == 0 and len(py) >= n, {"records": len(py), "nan_in_first_txNumber": nan_in_used}, "no NaN")
        if node is not None:
            add(f"V4n@{name}", "the verbatim JavaScript expression under node agrees with the Python transcription", node == py, "agree" if node == py else "differ", "agree")
        add(f"V5@{name}", "txNumber == parsed record count == scheduleMs length (no idle past-the-end calls, F3)",
            n == len([x for x in py if x is not None]) == len(args["scheduleMs"]) and b"\r" not in body and not body.endswith(b"\n"),
            {"txNumber": n, "records": len(py), "scheduleMs": len(args["scheduleMs"]), "crlf": b"\r" in body, "trailing_newline": body.endswith(b"\n")},
            "all equal, LF only, no trailing newline")
        add(f"V8a@{name}", "schedule file == round arguments (millisecond offsets preserved)", [x for x in py if x is not None] == [int(v) for v in args["scheduleMs"]])
        mine = [r for r in tx_rows if r["round"] == name]
        seqs = [int(r["seq"]) for r in mine]
        keys = [r["create_asset_key"] for r in mine]
        idx = 0 if name == "pre_roll" else 1
        add(f"V7@{name}", "tx map: one CreateAsset key per scheduled transaction, seq contiguous, keys follow the chaincode key rule",
            len(mine) == n and seqs == list(range(n)) and keys == [of.tx_key(run_id, idx, s) for s in range(n)], {"rows": len(mine), "txNumber": n}, "equal")
        intended = [int(r["intended_offset_ms"]) for r in mine]
        add(f"V8b@{name}", "tx map intended offsets == schedule file", intended == [x for x in py if x is not None])
        window_off = [int(r["window_offset_ms"]) for r in mine]
        shift = 0 if name == "measured" else of.PRE_ROLL_S * 1000
        add(f"V8c@{name}", "tx map window offsets map to the intended offsets (measured: identical; pre-roll: shifted by the 60 s)", [w + shift for w in window_off] == intended)
        day = [int(r["day_offset_ms"]) for r in mine]
        zero_file = int((np.diff(np.asarray(intended)) == 0).sum()) if n > 1 else 0
        zero_src = int((np.diff(np.asarray(day)) == 0).sum()) if n > 1 else 0
        add(f"V9@{name}", "exact ties are kept as repeated offsets (no deduplication)", zero_file == zero_src, {"zero_gaps_in_file": zero_file, "zero_gaps_in_source": zero_src}, "equal")
        add(f"V10@{name}", "arrival ordering: offsets non-decreasing, source row indices strictly increasing (file order)",
            all(b >= a for a, b in zip(intended, intended[1:])) and all(int(b["source_row_index"]) > int(a["source_row_index"]) for a, b in zip(mine, mine[1:])))
        length_s = of.PRE_ROLL_S if name == "pre_roll" else of.MEASURED_S
        c_file, c_src = per_second_counts(intended, length_s), per_second_counts([w + shift for w in window_off], length_s)
        add(f"V11@{name}", "zero-arrival seconds preserved", np.array_equal(c_file == 0, c_src == 0), {"empty_seconds": int((c_file == 0).sum())}, "same empty seconds")
        add(f"V12@{name}", "burst structure preserved (per-second counts of the file equal those of the source)", np.array_equal(c_file, c_src),
            {"peak_per_second": int(c_file.max()), "seconds_at_or_above_burst_rate": int((c_file >= burst_rate_tps).sum())}, "equal")
        add(f"V13@{name}", "CreateAsset mapping arguments: mode create, rate controlled, this round's own dispatch log, the replay workload module",
            args.get("mode") == "create" and args.get("rateControlled") is True and str(args.get("dispatchLog", "")).endswith(f"dispatch_{name}.csv")
            and doc["workload"]["module"].endswith("/modules/e1_trace_replay.js"), {"mode": args.get("mode"), "rateControlled": args.get("rateControlled")}, "create / True")
    pre, meas = parsed_by_round["pre_roll"], parsed_by_round["measured"]
    pre_ok, meas_ok = [x for x in pre if x is not None], [x for x in meas if x is not None]
    add("V6", "pre-roll and measured rounds are separate: different files, offsets inside their own windows, disjoint key spaces, round indices 0 and 1",
        files_seen[0] != files_seen[1] and pre_ok[-1] < of.PRE_ROLL_S * 1000 and meas_ok[-1] < of.MEASURED_S * 1000
        and not ({r["create_asset_key"] for r in tx_rows if r["round"] == "pre_roll"} & {r["create_asset_key"] for r in tx_rows if r["round"] == "measured"})
        and record["rounds"]["pre_roll"]["round_index"] == 0 and record["rounds"]["measured"]["round_index"] == 1 and record["rounds"]["pre_roll"]["measured"] is False,
        {"pre_last": pre_ok[-1], "measured_last": meas_ok[-1]}, "inside windows")
    budget = run_length_budget(pre_ok[-1], meas_ok[-1], caliper_timeout_s, host_max_seconds)
    add("V15", "Caliper hard timeout and host-sampler guard fit the replay's own length (round-completion semantics F6-F8)", budget["ok"],
        {k: budget[k] for k in ("expected_caliper_s", "caliper_timeout_s", "host_max_seconds", "timeout_margin_s")}, "timeout >= expected; guard >= timeout + 120 s")
    semantics = round_semantics(meas_ok[-1], pre_ok[-1])
    add("V14", "last scheduled offset lies strictly inside the 600 s window; the round-completion semantics are stated, not assumed", meas_ok[-1] < of.MEASURED_S * 1000,
        {"last_scheduled_offset_ms": meas_ok[-1], "ms_to_window_end": of.MEASURED_S * 1000 - meas_ok[-1]}, "< 600000")
    failed = [c["id"] for c in checks if not c["ok"]]
    return {"tool": f"p4_replay.caliper_check {PACKAGE_VERSION}", "run_id": run_id, "ok": not failed, "failed": failed, "checks": checks,
            "caliper_status": CALIPER_STATUS, "semantics": semantics, "budget": budget,
            "facts": [f["id"] for f in FACTS], "not_validated": [f["id"] for f in FACTS if f["status"] == "UNREAD"] + ["real replay-rate timing", "round turnover duration", "Fabric under bursts"]}
