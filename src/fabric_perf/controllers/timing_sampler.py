"""Post-hoc timing sampler for T01 to T06 (task A0o, 2026-11-21). OFFLINE ONLY: it reads finished files and returns a document; it measures nothing by itself.

Authority. ``Integration/system_integration/A0N_TIMING_MEASUREMENT_SPECIFICATION_2026-11-21.md`` defines T01 to T06 (source event, observation event, fields, units, clock, formula,
uncertainty). The definitions are implemented as written; none is redefined to make it computable. Where the specification says a quantity cannot be formed from the stored
fields, this module says ``NOT_MEASURABLE_FROM_EXISTING_EVIDENCE`` and names the exact missing dependency. It never estimates a missing duration from nearby timestamps and never
differences two clock domains without clock evidence.

What the numbers are. Every result carries its evidence origin. The only origins accepted here are ``synthetic_fixture`` and ``stored_scripted_clock_evidence``: values computed from
them are fixture arithmetic, NOT Caliper latency and NOT a measurement of any live system. An origin that claims a live run is refused (this task has none).

Statistics. Percentiles use the project's nearest-rank rule (``client_latency.nearest_rank``: the ceil(q*n)-th smallest sample of ONE window). A p99 is reported only for n >= 100 (below
that it equals the maximum, which the specification says to state, not to disguise). Statistics are given on the lower and the upper bound of each sample interval.

Samples use the proposed ``a0n_timing_sample/1`` shape: measurement, run_id, round, index, value_lo_ms, value_hi_ms, basis, clock_pair, offset_interval_ms, valid, reason_code.
"""

from __future__ import annotations

import bisect
import csv
import hashlib
import io
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import issue_log_reader as ilr
from . import recorder_health_bridge as rhb
from .client_latency import PERCENTILE_METHOD, nearest_rank
from .workload_coverage import canonical, parse_utc

SAMPLER_VERSION = "timing_sampler 0.1 (A0o, 2026-11-21)"
RESULT_SCHEMA = "a0o_timing_results/1"
SAMPLE_SCHEMA = "a0n_timing_sample/1"
SPEC = "Integration/system_integration/A0N_TIMING_MEASUREMENT_SPECIFICATION_2026-11-21.md"
ORIGINS = ("synthetic_fixture", "stored_scripted_clock_evidence")
S_MEASURED = "MEASURED_FROM_OFFLINE_EVIDENCE"
S_NONE = "NO_ELIGIBLE_SAMPLES"
S_NOT_MEASURABLE = "NOT_MEASURABLE_FROM_EXISTING_EVIDENCE"
S_INVALID = "INPUT_INVALID"
P99_MIN_N = 100
MAX_LISTED = 5000
# epoch-millisecond plausibility window (2001-09 .. 2286-11): a value outside it is a unit mismatch (seconds, microseconds, nanoseconds), never silently rescaled
EPOCH_MS_LO, EPOCH_MS_HI = 10 ** 12, 10 ** 13

DEFINITIONS = {
    "T01": "Generator issue-log write latency. Spec formula (available proxy only): W_n = mono_ms(attempt n) - mono_ms(intent n), the time to build, hash and write the intent record; "
           "interval [W-1, W+1] ms floored at 0. The direct end-of-write and tick-fsync cost is not stored in the log.",
    "T02": "Transaction issuance-to-recorder lag: t_submit_evt_ms(unit k) - attempt.t_ms(k), order-matched (a submit callback carries no shared identifier); valid only if totals agree and "
           "every count is 1, otherwise a batch-level interval; a negative lag is assumption_violated and is never clamped.",
    "T03": "Issue-log tail-reader lag: L_i = read_ms_i - t_ms_i = true visibility delay + write latency + (reader clock - generator clock). A negative apparent lag is "
           "generator_clock_ahead_of_reader. Cross-domain values need the T12 offset interval.",
    "T04": "Collector heartbeat interval (observed): delta_mono_i = mono_ms_i - mono_ms_(i-1) between consecutive heartbeat rows, lateness_i = delta_mono_i - heartbeatIntervalMs, "
           "delta_wm_i = observed_through_i - observed_through_(i-1) (s). The window_close row is excluded.",
    "T05": "Maximum heartbeat gap: gap_i = delta of the whole-second watermark over [observation_start, heartbeat watermarks..., stop], including the leading and the trailing gap; "
           "an error row with a frozen watermark is an unbounded gap, counted separately.",
    "T06": "Health-spool bridge delay: delay = ingest_wall - spool.t_utc (+ T12 band). Undefined for the recorder route today: the bridge replays a finished spool and stamps no ingest time.",
}
TIMESTAMP_FIELDS = {
    "T01": ["issue log intent.mono_ms", "issue log attempt.mono_ms", "(not stored) end-of-write / fsync completion"],
    "T02": ["issue log attempt.t_ms", "live.submits.csv t_submit_evt_ms"],
    "T03": ["issue log attempt.t_ms", "reader checkpoint marks [rseq_end, read_ms]"],
    "T04": ["spool heartbeat mono_ms", "spool heartbeat observed_through_utc", "spool start declared_policy.heartbeat_interval_ms"],
    "T05": ["spool start observation_start_utc", "spool heartbeat/stop observed_through_utc"],
    "T06": ["spool t_utc", "(not stored) bridge ingest wall time"],
}


class TimingSamplerError(ValueError):
    """The request is invalid (an unsupported origin, an unreadable or inconsistent input). ``code`` is machine-readable."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


def _fail(code: str, message: str) -> TimingSamplerError:
    return TimingSamplerError(code, message)


# ------------------------------------------------------------------------------------------------------------------ small pieces
def _sha(path: Path) -> dict[str, Any]:
    data = Path(path).read_bytes()
    return {"name": Path(path).name, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}


def sample(measurement: str, run_id: str, round_index: int | None, index: int, lo: float | None, hi: float | None, basis: str, clock_pair: Sequence[str],
           *, valid: bool = True, reason: str | None = None, offset: Sequence[float] | None = None, **extra: Any) -> dict[str, Any]:
    return {"schema": SAMPLE_SCHEMA, "measurement": measurement, "run_id": run_id, "round": round_index, "index": index, "value_lo_ms": lo, "value_hi_ms": hi, "basis": basis,
            "clock_pair": list(clock_pair), "offset_interval_ms": None if offset is None else list(offset), "valid": valid, "reason_code": reason, **extra}


def aggregate(samples: Sequence[Mapping[str, Any]], *, key_lo: str = "value_lo_ms", key_hi: str = "value_hi_ms") -> dict[str, Any] | None:
    """Median, p95, p99, maximum on the lower and the upper bound (nearest-rank). ``None`` for an empty set: an empty set is never summarised as zero."""
    eligible = [s for s in samples if s.get("valid")]
    if not eligible:
        return None
    out: dict[str, Any] = {"n": len(eligible), "percentile_method": PERCENTILE_METHOD, "median_rule": "nearest-rank at q=0.5"}
    for side, key in (("lower_bound", key_lo), ("upper_bound", key_hi)):
        values = [float(s[key]) for s in eligible]
        out[side] = {"median": nearest_rank(values, 0.5), "p95": nearest_rank(values, 0.95),
                     "p99": nearest_rank(values, 0.99) if len(values) >= P99_MIN_N else None, "max": max(values)}
    if len(eligible) < P99_MIN_N:
        out["p99_note"] = f"not reported: n={len(eligible)} < {P99_MIN_N}; below that the p99 is the maximum"
    return out


def _unit_problem(value: Any) -> str | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return "not_a_number"
    if value < EPOCH_MS_LO:
        return f"unit_mismatch: {value} is below the epoch-millisecond range (seconds?)"
    if value >= EPOCH_MS_HI:
        return f"unit_mismatch: {value} is above the epoch-millisecond range (microseconds or nanoseconds?)"
    return None


def _result(measurement: str, run_id: str | None, *, status: str, origin: str, inputs: Sequence[Mapping[str, Any]], clock_domain: Any, samples: Sequence[Mapping[str, Any]] = (),
            exclusions: Sequence[Mapping[str, Any]] = (), missing_dependency: str | None = None, reason: str | None = None, time_range: Mapping[str, Any] | None = None,
            extra: Mapping[str, Any] | None = None, stats_keys: Sequence[tuple[str, str, str]] | None = None) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for item in exclusions:
        counts[item["reason"].split(":")[0]] = counts.get(item["reason"].split(":")[0], 0) + 1
    out = {"measurement": measurement, "definition": DEFINITIONS[measurement], "specification": SPEC, "status": status, "evidence_origin": origin,
           "provenance": "offline fixture arithmetic on stored files; NOT Caliper latency and NOT a live measurement", "run_id": run_id, "source_artifacts": list(inputs),
           "timestamp_fields": TIMESTAMP_FIELDS[measurement], "clock_domain": clock_domain,
           "eligible_samples": len([s for s in samples if s.get("valid")]), "samples": list(samples)[:MAX_LISTED], "samples_truncated": len(samples) > MAX_LISTED,
           "exclusions": list(exclusions)[:MAX_LISTED], "exclusion_counts": counts, "missing_count": counts.get("missing", 0), "invalid_count": len(exclusions) - counts.get("missing", 0),
           "aggregate": aggregate(samples), "time_range": time_range, "reason": reason, "missing_dependency": missing_dependency}
    if stats_keys:
        out["aggregates"] = {name: aggregate(samples, key_lo=lo, key_hi=hi) for name, lo, hi in stats_keys}
    if extra:
        out.update(extra)
    return out


def _not_measurable(measurement: str, origin: str, inputs: Sequence[Mapping[str, Any]], dependency: str, clock_domain: Any = None, run_id: str | None = None,
                    extra: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return _result(measurement, run_id, status=S_NOT_MEASURABLE, origin=origin, inputs=inputs, clock_domain=clock_domain, missing_dependency=dependency,
                   reason="the quantity cannot be formed from the stored fields; no estimate is made from nearby timestamps or unrelated clocks", extra=extra)


# ------------------------------------------------------------------------------------------------------------------ inputs
def read_issue_log(path: Path) -> dict[str, Any]:
    """Validate the log with the unmodified strict reader (hash chain, sequence, state machine, clocks), then parse its records for the stamps the reader does not expose.
    Raises ``TimingSamplerError`` if the reader refuses the log: no timing is taken from a log that failed integrity."""
    path = Path(path)
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise _fail("issue_log_unreadable", f"{path}: {exc}") from exc
    first = raw.split(b"\n", 1)[0]
    try:
        start = json.loads(first.decode("ascii"))
        policy = ilr.IssueLogPolicy(run_id=start["run_id"], worker_id=start["worker_id"], clock_domain=start["clock_domain"], max_tick_gap_ms=600_000)
    except (ValueError, KeyError, UnicodeDecodeError, ilr.IssueLogError) as exc:
        raise _fail("issue_log_start_unreadable", f"the first record of {path.name} cannot be read as a start record: {exc}") from exc
    tail = ilr.IssueLogTail(path, policy)
    tail.poll(ilr.ms_to_utc(int(start["t_ms"])))                 # validity only: the read time is irrelevant to T01/T02 and T03 takes its own marks
    for _ in range(100_000):
        if tail.error is not None or tail.partial_tail_bytes or tail.offset >= len(raw):
            break
        result = tail.poll(ilr.ms_to_utc(int(start["t_ms"])))
        if result.records == 0:
            break
    if tail.error is not None:
        raise _fail("issue_log_invalid", f"{tail.error.code}: {tail.error}")
    if tail.partial_tail_bytes or tail.offset != len(raw):
        raise _fail("issue_log_incomplete", "the log has an unterminated tail or was not read to its end")
    records = [json.loads(line) for line in raw.decode("ascii").split("\n")[:-1]]
    return {"records": records, "run_id": policy.run_id, "round_index": start.get("round_index"), "clock_domain": policy.clock_domain, "inputs": _sha(path), "tail": tail}


def _pairs(records: Sequence[Mapping[str, Any]]) -> tuple[dict[int, Mapping[str, Any]], dict[int, Mapping[str, Any]]]:
    intents = {r["n"]: r for r in records if r["kind"] == "intent"}
    attempts = {r["n"]: r for r in records if r["kind"] == "attempt"}
    return intents, attempts


# ------------------------------------------------------------------------------------------------------------------ T01
def t01(log: Mapping[str, Any], origin: str) -> dict[str, Any]:
    run, rnd = log["run_id"], log["round_index"]
    intents, attempts = _pairs(log["records"])
    samples, exclusions = [], []
    for n in sorted(intents):
        a = attempts.get(n)
        if a is None:
            exclusions.append({"index": n, "reason": "missing: attempt record for the intent", "detail": "intent without attempt"})
            continue
        w = a["mono_ms"] - intents[n]["mono_ms"]
        if w < 0:
            exclusions.append({"index": n, "reason": "invalid: negative_elapsed", "detail": f"attempt mono_ms precedes the intent's by {-w} ms"})
            continue
        samples.append(sample("T01", run, rnd, n, max(0, w - 1), w + 1, "mono_delta_intent_attempt", ["worker_mono"]))
    not_measured = {"end_of_write_latency": {"status": S_NOT_MEASURABLE,
                                             "missing_dependency": "an end-of-write (and tick fsync completion) timestamp in the issue-log records: e1_issue_log/1 stores only t_ms (read at the START of the write) "
                                                                   "and an integer-millisecond mono_ms; a direct measure needs a versioned writer that records write_ms and fsync_ms, "
                                                                   "or a micro-benchmark on the target filesystem"}}
    times = [r["t_ms"] for r in log["records"] if r["kind"] == "intent"]
    return _result("T01", run, status=S_MEASURED if samples else S_NONE, origin=origin, inputs=[log["inputs"]], clock_domain=f"{log['clock_domain']} (worker monotonic)",
                   samples=samples, exclusions=exclusions, time_range={"first_event_ms": min(times), "last_event_ms": max(times)} if times else None,
                   extra={"quantity_measured": "intent-record write time proxy (the specification's available formula); a delta of 0 ms means below the 1 ms resolution, not zero",
                          "not_measured": not_measured,
                          "note": "the proxy excludes the tick fsync cost and the attempt-record write; it is not the end-of-write latency"})


# ------------------------------------------------------------------------------------------------------------------ T02
def read_submits(path: Path) -> dict[str, Any]:
    path = Path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise _fail("submits_unreadable", f"{path}: {exc}") from exc
    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows and "seq_submit" not in text.split("\n", 1)[0]:
        raise _fail("submits_header_missing", "the submits file has no header seq_submit,t_submit_evt_ms,count")
    out = []
    for number, row in enumerate(rows):
        try:
            out.append({"seq": int(row["seq_submit"]), "t": int(row["t_submit_evt_ms"]), "count": int(row["count"])})
        except (KeyError, ValueError, TypeError) as exc:
            raise _fail("submits_row_invalid", f"row {number} of {path.name} is not (seq_submit,t_submit_evt_ms,count) integers") from exc
    return {"rows": out, "inputs": _sha(path)}


def t02(log: Mapping[str, Any], submits: Mapping[str, Any] | None, submits_clock_domain: str | None, origin: str) -> dict[str, Any]:
    run, rnd = log["run_id"], log["round_index"]
    inputs = [log["inputs"]] + ([submits["inputs"]] if submits else [])
    if submits is None:
        return _not_measurable("T02", origin, inputs, "the recorder's live.submits.csv (seq_submit,t_submit_evt_ms,count) for the same run, round and worker", log["clock_domain"], run)
    if submits_clock_domain != log["clock_domain"]:
        return _not_measurable("T02", origin, inputs, f"clock evidence: the issue log's clock domain is {log['clock_domain']!r} but the submits stream was declared as {submits_clock_domain!r}; "
                               "a difference between two clock domains needs the offset interval (T12) and none was supplied", f"{log['clock_domain']} vs {submits_clock_domain}", run)
    attempts = [r for r in log["records"] if r["kind"] == "attempt"]
    rows = submits["rows"]
    units = sum(r["count"] for r in rows)
    samples, exclusions = [], []
    for a in attempts:
        problem = _unit_problem(a["t_ms"])
        if problem:
            return _result("T02", run, status=S_INVALID, origin=origin, inputs=inputs, clock_domain=log["clock_domain"], reason=f"attempt n={a['n']}: {problem}")
    for r in rows:
        problem = _unit_problem(r["t"])
        if problem:
            exclusions.append({"index": r["seq"], "reason": f"invalid: {problem.split(':')[0]}", "detail": problem})
    if exclusions:
        return _result("T02", run, status=S_INVALID, origin=origin, inputs=inputs, clock_domain=log["clock_domain"], exclusions=exclusions,
                       reason="a submit timestamp is not in epoch milliseconds; nothing is rescaled")
    if units != len(attempts):
        for r in rows:
            exclusions.append({"index": r["seq"], "reason": "invalid: total_mismatch", "detail": f"recorder units {units} != generator attempts {len(attempts)} "
                               f"({'deficit: failed or missed callbacks' if units < len(attempts) else 'recorder_exceeds_source'})"})
        return _result("T02", run, status=S_NONE, origin=origin, inputs=inputs, clock_domain=log["clock_domain"], exclusions=exclusions,
                       extra={"unmatched": {"generator_without_recorder": max(0, len(attempts) - units), "recorder_without_generator": max(0, units - len(attempts))}},
                       reason="totals disagree: no order-matched lag is formed")
    cursor, batch = 0, any(r["count"] != 1 for r in rows)
    for r in rows:
        if r["count"] < 1:
            exclusions.append({"index": r["seq"], "reason": "invalid: count_not_positive", "detail": f"count {r['count']}"})
            continue
        block = attempts[cursor:cursor + r["count"]]
        cursor += r["count"]
        if r["count"] == 1:
            d = r["t"] - block[0]["t_ms"]
            if d < 0:
                exclusions.append({"index": r["seq"], "reason": "invalid: assumption_violated", "detail": f"submit callback is {-d} ms BEFORE its order-matched attempt; never clamped"})
                continue
            samples.append(sample("T02", run, rnd, r["seq"], max(0, d - 1), d + 1, "order_matched_unit", ["worker_wall", "worker_wall"]))
        else:
            earliest, latest = r["t"] - block[-1]["t_ms"], r["t"] - block[0]["t_ms"]
            if earliest < 0:
                exclusions.append({"index": r["seq"], "reason": "invalid: assumption_violated", "detail": "a batched callback precedes the last attempt it is matched to"})
                continue
            samples.append(sample("T02", run, rnd, r["seq"], max(0, earliest - 1), latest + 1, "order_matched_batch", ["worker_wall", "worker_wall"], batch_count=r["count"]))
    times = [r["t"] for r in rows]
    return _result("T02", run, status=S_MEASURED if samples else S_NONE, origin=origin, inputs=inputs, clock_domain=log["clock_domain"], samples=samples, exclusions=exclusions,
                   time_range={"first_event_ms": min(times), "last_event_ms": max(times)} if times else None,
                   extra={"matched_fraction": (len(samples) / len(rows)) if rows else None, "batch_level_rows": batch,
                          "note": "an order-matched estimate: a submit callback carries no shared identifier, so unit order is assumed to be call order (unverified against Caliper)"})


# ------------------------------------------------------------------------------------------------------------------ T03
def read_checkpoint_marks(path: Path, log: Mapping[str, Any]) -> dict[str, Any]:
    path = Path(path)
    try:
        doc = json.loads(path.read_text(encoding="ascii"))
    except (OSError, ValueError) as exc:
        raise _fail("checkpoint_unreadable", f"{path}: {exc}") from exc
    body = {k: v for k, v in doc.items() if k != "self_sha256"}
    if doc.get("self_sha256") != hashlib.sha256(canonical(body).encode("utf-8")).hexdigest():
        raise _fail("checkpoint_corrupt", f"{path.name} fails its own digest")
    if body.get("run_id") != log["run_id"] or body.get("worker_id") != log["tail"].policy.worker_id:
        raise _fail("checkpoint_identity_mismatch", "the checkpoint is for another run or worker than the log")
    if body.get("round_index") is not None and body.get("round_index") != log["round_index"]:
        raise _fail("checkpoint_identity_mismatch", f"the checkpoint is for round {body.get('round_index')!r}, the log for round {log['round_index']!r}")
    return {"marks": [list(m) for m in body.get("marks", [])], "rseq": body.get("rseq"), "inputs": _sha(path)}


def t03(log: Mapping[str, Any], checkpoint: Mapping[str, Any] | None, reader_clock_domain: str | None, offset_interval_ms: Sequence[float] | None, origin: str) -> dict[str, Any]:
    run, rnd = log["run_id"], log["round_index"]
    inputs = [log["inputs"]] + ([checkpoint["inputs"]] if checkpoint else [])
    if checkpoint is None or not checkpoint["marks"]:
        return _not_measurable("T03", origin, inputs, "per-poll read times: the reader checkpoint's marks [rseq_end, read_ms] (a reader driver run that stored them)", log["clock_domain"], run)
    same_domain = reader_clock_domain == log["clock_domain"]
    if reader_clock_domain is None or (not same_domain and offset_interval_ms is None):
        return _not_measurable("T03", origin, inputs, f"clock evidence: the generator clock domain is {log['clock_domain']!r} and the reader's is {reader_clock_domain!r}; "
                               "a cross-domain difference needs the T12 offset interval and none was supplied", f"{log['clock_domain']} vs {reader_clock_domain}", run)
    if offset_interval_ms is not None:
        low, high = float(offset_interval_ms[0]), float(offset_interval_ms[1])
        if low > high:
            raise _fail("offset_interval_invalid", "the offset interval must be [low, high] with low <= high")
    else:
        low = high = 0.0
    marks = checkpoint["marks"]
    ends = [m[0] for m in marks]
    samples, exclusions = [], []
    for r in log["records"]:
        if r["kind"] != "attempt":
            continue
        problem = _unit_problem(r["t_ms"])
        if problem:
            exclusions.append({"index": r["n"], "reason": f"invalid: {problem.split(':')[0]}", "detail": problem})
            continue
        slot = bisect.bisect_left(ends, r["rseq"])
        if slot >= len(marks):
            exclusions.append({"index": r["n"], "reason": "missing: read_mark", "detail": f"no poll mark covers record {r['rseq']} (the checkpoint ends at {ends[-1]})"})
            continue
        read_ms = marks[slot][1]
        lag = read_ms - r["t_ms"]
        if lag < 0:
            exclusions.append({"index": r["n"], "reason": "invalid: generator_clock_ahead_of_reader", "detail": f"apparent lag {lag} ms is negative (zero tolerance); never clamped"})
            continue
        samples.append(sample("T03", run, rnd, r["n"], max(0.0, lag - 1 - high), lag + 1 - low, "read_ms_minus_t_ms", ["worker_wall", "reader_wall"], offset=[low, high],
                              poll_index=slot))
    times = [r["t_ms"] for r in log["records"] if r["kind"] == "attempt"]
    return _result("T03", run, status=S_MEASURED if samples else S_NONE, origin=origin, inputs=inputs, clock_domain=f"{log['clock_domain']} -> {reader_clock_domain}",
                   samples=samples, exclusions=exclusions, time_range={"first_event_ms": min(times), "last_event_ms": max(times)} if times else None,
                   extra={"clock_pair_same_domain": same_domain, "offset_interval_ms": [low, high],
                          "note": "an upper bound on the visibility delay within one poll interval: it also contains the write latency (T01) and the reader-minus-generator offset; "
                                  "the poll interval quantisation is not added to the bounds"})


# ------------------------------------------------------------------------------------------------------------------ T04, T05, T06
def _spool(path: Path) -> tuple[rhb.SpoolRead, dict[str, Any]]:
    try:
        read = rhb.read_spool(path)
    except (rhb.BridgeError, OSError) as exc:
        raise _fail("spool_invalid", f"{path}: {exc}") from exc
    return read, _sha(path)


def _wm_s(row: Mapping[str, Any]) -> float:
    return parse_utc(row["observed_through_utc"], "observed_through_utc").timestamp()


def t04(spool: rhb.SpoolRead, spool_inputs: Mapping[str, Any], origin: str) -> dict[str, Any]:
    rows = spool.rows
    start = next((r for r in rows if r["kind"] == "start"), None)
    run = None if start is None else start.get("run_id")
    if start is None or not isinstance(start.get("declared_policy"), Mapping) or "heartbeat_interval_ms" not in start["declared_policy"]:
        return _not_measurable("T04", origin, [spool_inputs], "a spool start row declaring declared_policy.heartbeat_interval_ms", None, run)
    interval = start["declared_policy"]["heartbeat_interval_ms"]
    beats = [r for r in rows if r["kind"] == "heartbeat" and r.get("phase") == "heartbeat"]
    samples, exclusions = [], []
    previous = None
    for r in beats:
        if previous is None:
            exclusions.append({"index": r["n"], "reason": "missing: no_predecessor", "detail": "the first heartbeat has no earlier heartbeat (the start row is not a cadence point)"})
        else:
            dm = float(r["mono_ms"]) - float(previous["mono_ms"])
            if dm < 0:
                exclusions.append({"index": r["n"], "reason": "invalid: negative_elapsed", "detail": f"mono_ms moved back by {-dm} ms"})
            else:
                dwm = _wm_s(r) - _wm_s(previous)
                samples.append(sample("T04", run, start.get("round"), r["n"], dm, dm, "delta_mono_ms", ["worker_mono"], lateness_ms=dm - interval, delta_wm_s=dwm,
                                      heartbeat_interval_ms=interval))
        previous = r
    stamps = [r["t_utc"] for r in beats]
    out = _result("T04", run, status=S_MEASURED if samples else S_NONE, origin=origin, inputs=[spool_inputs], clock_domain="worker wall and monotonic (one process)", samples=samples,
                  exclusions=exclusions, time_range={"first_t_utc": stamps[0], "last_t_utc": stamps[-1]} if stamps else None,
                  stats_keys=[("delta_mono_ms", "value_lo_ms", "value_hi_ms")],
                  extra={"declared_heartbeat_interval_ms": interval, "window_close_rows_excluded": sum(1 for r in rows if r["kind"] == "heartbeat" and r.get("phase") == "window_close"),
                         "note": "a heartbeat row is not written when the watermark did not advance, so row spacing can exceed the interval while the timer fired on time; use delta_wm for "
                                 "coverage and delta_mono for timer behaviour"})
    if out["aggregates"]["delta_mono_ms"] is not None:
        late = [{**s, "value_lo_ms": s["lateness_ms"], "value_hi_ms": s["lateness_ms"]} for s in samples]
        out["aggregates"]["lateness_ms"] = aggregate(late)
        wm = [{**s, "value_lo_ms": s["delta_wm_s"], "value_hi_ms": s["delta_wm_s"]} for s in samples]
        out["aggregates"]["delta_wm_s"] = aggregate(wm)
    return out


def t05(spool: rhb.SpoolRead, spool_inputs: Mapping[str, Any], origin: str, candidate_limit_s: float | None) -> dict[str, Any]:
    rows = spool.rows
    start = next((r for r in rows if r["kind"] == "start"), None)
    run = None if start is None else start.get("run_id")
    if start is None or "observation_start_utc" not in start:
        return _not_measurable("T05", origin, [spool_inputs], "a spool start row with observation_start_utc", None, run)
    marks = [("start", parse_utc(start["observation_start_utc"], "observation_start_utc").timestamp(), start["n"])]
    samples, exclusions = [], []
    unbounded = []
    stop = None
    for r in rows:
        if r["kind"] == "heartbeat" and r.get("phase") == "heartbeat":
            marks.append(("heartbeat", _wm_s(r), r["n"]))
        elif r["kind"] == "error":
            unbounded.append({"n": r["n"], "t_utc": r.get("t_utc"), "reason": "error row: the watermark is frozen until a recovered row"})
        elif r["kind"] == "stop":
            stop = r
    if stop is None:
        exclusions.append({"index": None, "reason": "missing: no_stop_row", "detail": "no stop row: the time after the last watermark is uncovered and is not a gap sample"})
    else:
        marks.append(("stop", _wm_s(stop), stop["n"]))
    for (k0, w0, n0), (k1, w1, n1) in zip(marks, marks[1:]):
        gap = w1 - w0
        if gap < 0:
            exclusions.append({"index": n1, "reason": "invalid: negative_elapsed", "detail": f"the watermark moved back by {-gap} s"})
            continue
        samples.append(sample("T05", run, start.get("round"), n1, gap * 1000.0, gap * 1000.0, "delta_watermark_ms", ["worker_wall_whole_second"], gap_s=gap,
                              kind=("leading" if k0 == "start" else "trailing" if k1 == "stop" else "interior")))
    gaps = [s["gap_s"] for s in samples]
    above = None if candidate_limit_s is None else sum(1 for g in gaps if g > candidate_limit_s)
    return _result("T05", run, status=S_MEASURED if samples else S_NONE, origin=origin, inputs=[spool_inputs], clock_domain="worker wall (whole-second watermark)", samples=samples,
                   exclusions=exclusions, time_range={"first_t_utc": rows[0]["t_utc"], "last_t_utc": rows[-1]["t_utc"]},
                   extra={"max_gap_s": max(gaps) if gaps else None, "candidate_limit_s": candidate_limit_s,
                          "candidate_limit_basis": "caller-supplied; the A0j value is an unverified engineering candidate, not approved" if candidate_limit_s is not None else "none supplied",
                          "count_above_candidate_limit": above, "unbounded_gaps": unbounded, "unbounded_gap_count": len(unbounded),
                          "stall_cause": "not observable from the spool alone"})


def t06(spool: rhb.SpoolRead, spool_inputs: Mapping[str, Any], origin: str) -> dict[str, Any]:
    start = next((r for r in spool.rows if r["kind"] == "start"), None)
    run = None if start is None else start.get("run_id")
    ingest_fields = sorted({key for r in spool.rows for key in r if "ingest" in key.lower()})
    dependency = ("a bridge ingest timestamp: recorder_health_bridge.ingest_spool replays a finished spool and uses each row's own t_utc as the journal clock, so no ingest wall time "
                  "is stored anywhere; an incremental recorder bridge that stamps ingest wall and monotonic time into its report does not exist")
    return _not_measurable("T06", origin, [spool_inputs], dependency, "spool: worker; ingest: bridge host (undefined)", run,
                           extra={"ingest_fields_found_in_spool": ingest_fields,
                                  "generator_route": "for the generator route the delay of a liveness point is read_time - t_ms, which is T03; it is not substituted here"})


# ------------------------------------------------------------------------------------------------------------------ entry point
def run_sampler(*, origin: str, issue_log: Path | None = None, submits: Path | None = None, submits_clock_domain: str | None = None, checkpoint: Path | None = None,
                reader_clock_domain: str | None = None, offset_interval_ms: Sequence[float] | None = None, spool: Path | None = None,
                candidate_gap_limit_s: float | None = None) -> dict[str, Any]:
    """Compute T01 to T06 from whatever inputs are supplied. A measurement whose inputs are absent is NOT_MEASURABLE_FROM_EXISTING_EVIDENCE (never skipped, never zero)."""
    if origin not in ORIGINS:
        raise _fail("origin_not_supported", f"evidence origin {origin!r} is not one of {ORIGINS}: this sampler labels fixture arithmetic only and never accepts a live-run claim")
    results: dict[str, Any] = {}
    inputs: list[dict[str, Any]] = []
    log = None
    if issue_log is not None:
        log = read_issue_log(issue_log)
        inputs.append({"role": "issue_log", **log["inputs"]})
        results["T01"] = t01(log, origin)
        sub = read_submits(submits) if submits is not None else None
        if sub:
            inputs.append({"role": "live_submits", **sub["inputs"]})
        results["T02"] = t02(log, sub, submits_clock_domain, origin)
        chk = read_checkpoint_marks(checkpoint, log) if checkpoint is not None else None
        if chk:
            inputs.append({"role": "reader_checkpoint", **chk["inputs"]})
        results["T03"] = t03(log, chk, reader_clock_domain, offset_interval_ms, origin)
    else:
        for name, dep in (("T01", "the generator issue log"), ("T02", "the generator issue log and the recorder submits file"), ("T03", "the generator issue log and the reader checkpoint")):
            results[name] = _not_measurable(name, origin, [], dep)
    if spool is not None:
        read, spool_inputs = _spool(spool)
        inputs.append({"role": "health_spool", **spool_inputs})
        if read.break_at is not None or read.torn_tail:
            for name in ("T04", "T05", "T06"):
                results[name] = _result(name, None, status=S_INVALID, origin=origin, inputs=[spool_inputs], clock_domain=None,
                                        reason=f"the spool is not clean ({'torn tail' if read.torn_tail else ''}{'; ' if read.torn_tail and read.break_at else ''}"
                                               f"{'sequence break: ' + str(read.break_reason) if read.break_at else ''}); no cadence is taken from a damaged spool")
        else:
            results["T04"] = t04(read, spool_inputs, origin)
            results["T05"] = t05(read, spool_inputs, origin, candidate_gap_limit_s)
            results["T06"] = t06(read, spool_inputs, origin)
    else:
        for name in ("T04", "T05", "T06"):
            results[name] = _not_measurable(name, origin, [], "the recorder health spool (<base>.health.jsonl)")
    return {"schema": RESULT_SCHEMA, "sampler": SAMPLER_VERSION, "specification": SPEC, "evidence_origin": origin, "percentile_method": PERCENTILE_METHOD,
            "inputs": inputs, "measurements": {k: results[k] for k in sorted(results)},
            "not_covered": ["T07 to T12 (ledger interval, adapter window, source skew, future skew, same-host clock discrepancy, Windows-WSL offset) are outside A0o's T01-T06 scope"],
            "statement": "Offline fixture arithmetic. No value here is a Caliper, Fabric or live-clock measurement, no operator threshold is approved, and Windows-WSL clock agreement was not measured."}
