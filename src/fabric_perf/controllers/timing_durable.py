"""Post-hoc timing sampler extension for T01 (direct write and sync timing) and T06 (bridge ingestion delay) (task A0p, 2026-11-22). OFFLINE ONLY.

``timing_sampler`` (A0o) is unchanged. This module wraps it: T02 to T05 are computed by the A0o code exactly as before; T01 and T06 are REPLACED only when the new evidence exists
(the issue-log timing sidecar for T01, the incremental bridge's ingest ledger for T06). Without that evidence the A0o behaviour is kept: T01 stays the intent-to-attempt proxy with the
direct measure not measurable, and T06 stays ``NOT_MEASURABLE_FROM_EXISTING_EVIDENCE``. A historical file is never given a timing it does not contain.

Authority: ``Integration/system_integration/A0N_TIMING_MEASUREMENT_SPECIFICATION_2026-11-21.md`` (T01 and T06 sheets). The definitions below implement the sheets' "direct measure" and
"ingest" rows; T02 to T05 are not redefined.

T01 (direct). Per attested log record: ``write_call = (write_end_mono_us - write_start_mono_us) / 1000`` ms, the time ``fs.writeSync`` took for that record (all bytes accepted by the
operating system). Per sync row: ``sync_call = (sync_end_mono_us - sync_start_mono_us) / 1000`` ms, the time ``fs.fsyncSync`` took. Both differences are between two stamps of the SAME
process monotonic clock. A write duration is NOT a persistence delay: only a covering sync row says a flush request returned, and a record with no covering sync row is reported as having none.
Uncertainty: each stamp is a floor to a microsecond, so a duration is known to within +/- 0.001 ms of quantisation; scheduler preemption, filesystem and bind-mount effects are part of the value.

T06 (ingest). ``delay = journal_append_return_wall - spool.t_utc`` where the append return is the moment the journal row was written, flushed and ``os.fsync`` returned (the row can then
be read by Stage 3). The two stamps come from different hosts' clocks, so the difference is formed only (a) when the operator declared the SAME clock domain for the spool and the bridge
(a declaration, not a measurement: it is reported as such) or (b) with a supplied T12 clock-offset interval ``[lo, hi]`` = bridge clock minus spool clock. Otherwise T06 is
``NOT_MEASURABLE_FROM_EXISTING_EVIDENCE``. In every case the quantities that are defensible within ONE process are reported separately and exactly as what they are:
``bridge_read_to_append_return_ms`` (poll read to journal append return, bridge monotonic clock; includes the wait behind earlier rows of the same poll) and ``journal_append_ms``
(journal append start to return, bridge monotonic clock). The spool's ``t_utc`` has millisecond precision (the true time lies in [t, t+1) ms), which widens the interval by 1 ms.
A negative delay is an ``assumption_violated`` and is never clamped. Rows whose ingest stamp is unknown (a recovery row, a journal row found without a ledger row) are missing, never estimated.

Every result states its evidence origin; only ``synthetic_fixture`` and ``stored_scripted_clock_evidence`` are accepted, so every value is fixture arithmetic, NOT Caliper latency.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import issue_log_timing as ilt
from . import recorder_health_bridge as rhb
from . import recorder_health_incremental as rhi
from . import timing_sampler as ts
from .workload_coverage import GENESIS_SHA256, canonical, event_digest, parse_utc

SAMPLER_VERSION = "timing_durable 0.1 (A0p, 2026-11-22)"
RESULT_SCHEMA = "a0p_timing_results/1"
QUANT_MS = 0.001                       # +/- quantisation of a microsecond-floored monotonic difference
SPOOL_MS_WIDTH = 1.0                   # the spool's t_utc is millisecond precision: the true time lies in [t, t+1) ms
T01_DEFINITION = ("Generator issue-log write latency, DIRECT: per attested log record write_call = (write_end_mono_us - write_start_mono_us)/1000 ms (fs.writeSync of the record); per sync row "
                  "sync_call = (sync_end_mono_us - sync_start_mono_us)/1000 ms (fs.fsyncSync of the log). A write duration is not a persistence delay.")
T06_DEFINITION = ("Health-spool bridge delay: delay = journal_append_return_wall - spool.t_utc (+ T12 band when the clock domains differ); formed only for a declared same clock domain or with "
                  "a T12 offset interval. Within-process quantities are reported separately.")
T01_FIELDS = ["sidecar write row: write_start_mono_us, write_end_mono_us (worker process monotonic, microseconds)", "sidecar sync row: sync_start_mono_us, sync_end_mono_us",
              "sidecar write row: target_rseq, target_sha256 (link to the log record)"]
T06_FIELDS = ["ingest ledger: spool_t_utc (source event time, spool host clock)", "ingest ledger: read_wall_utc / read_mono_us (bridge read)",
              "ingest ledger: journal_append_start_* / journal_append_return_* (bridge journal append, fsync returned)", "ledger_start: spool_clock_domain, bridge_clock_domain"]


def _fail(code: str, message: str) -> ts.TimingSamplerError:
    return ts.TimingSamplerError(code, message)


def _sha(path: Path, role: str) -> dict[str, Any]:
    data = Path(path).read_bytes()
    return {"role": role, "name": Path(path).name, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}


def _retarget(result: dict[str, Any], definition: str, fields: Sequence[str]) -> dict[str, Any]:
    result["definition"], result["timestamp_fields"] = definition, list(fields)
    result["observation_class"] = "fixture_arithmetic: not Caliper latency, not a live measurement, not a clock-agreement measurement"
    return result


# ------------------------------------------------------------------------------------------------------------------ T01
def t01_direct(log: Mapping[str, Any], evidence: ilt.TimingEvidence, origin: str) -> dict[str, Any]:
    """T01 from the issue-log timing sidecar. ``log`` is the dict ``timing_sampler.read_issue_log`` returned (the validated log)."""
    inputs = [dict(i) for i in evidence.inputs]
    if evidence.status == ilt.INVALID:
        return _retarget(ts._result("T01", evidence.run_id, status=ts.S_INVALID, origin=origin, inputs=inputs, clock_domain=evidence.clock_domain,
                                    reason=f"the timing sidecar failed verification ({(evidence.error or {}).get('code')}): {(evidence.error or {}).get('message')}; "
                                           "no timing is taken from it"), T01_DEFINITION, T01_FIELDS)
    samples, exclusions = [], []
    kinds: dict[str, int] = {}
    for rseq in range(1, evidence.log_records + 1):
        w = evidence.writes.get(rseq)
        if w is None:
            exclusions.append({"index": rseq, "reason": "missing: write row for the log record (unattested)", "detail": "a crash between the log write and the sidecar write, or a failed sidecar"})
            continue
        d = (w["write_end_mono_us"] - w["write_start_mono_us"]) / 1000.0
        samples.append(ts.sample("T01", evidence.run_id, evidence.round_index, rseq, max(0.0, d - QUANT_MS), d + QUANT_MS, "write_call_mono_us_delta", ["worker_mono_us"],
                                 record_kind=w["kind"], record_sha256=w["sha256"]))
        kinds[w["kind"]] = kinds.get(w["kind"], 0) + 1
    syncs = []
    for index, s in enumerate(evidence.syncs, 1):
        d = (s["sync_end_mono_us"] - s["sync_start_mono_us"]) / 1000.0
        syncs.append(ts.sample("T01", evidence.run_id, evidence.round_index, index, max(0.0, d - QUANT_MS), d + QUANT_MS, "sync_call_mono_us_delta", ["worker_mono_us"],
                               covered_from_rseq=s["covered_from_rseq"], covered_through_rseq=s["covered_through_rseq"]))
    proxy = ts.t01(log, origin) if log is not None else None
    times = [w["t_ms"] for w in evidence.writes.values()]
    result = ts._result("T01", evidence.run_id, status=ts.S_MEASURED if samples else ts.S_NONE, origin=origin, inputs=inputs,
                        clock_domain=f"{evidence.clock_domain} (worker monotonic, microseconds)", samples=samples, exclusions=exclusions,
                        time_range={"first_event_ms": min(times), "last_event_ms": max(times)} if times else None,
                        extra={"quantity_measured": "direct write-call duration per attested log record (fs.writeSync), from the timing sidecar",
                               "write_call_ms": ts.aggregate(samples), "write_call_by_kind_counts": kinds,
                               "sync_call_ms": ts.aggregate(syncs), "sync_samples": syncs, "sync_count": len(syncs),
                               "uncertainty_ms": {"quantisation": QUANT_MS, "excludes": "scheduler preemption, filesystem, bind-mount and storage effects are part of the value, not of the uncertainty"},
                               "persistence": {"records_with_no_covering_sync": len(evidence.unsynced_rseqs), "sync_meaning": "fsyncSync returned without an exception: the operating "
                                               "system accepted the flush request; not a guarantee against every storage failure",
                                               "write_end_meaning": "fs.writeSync returned having accepted all bytes; not a persistence claim"},
                               "sidecar_status": evidence.status, "unattested_rseqs": list(evidence.unattested_rseqs), "recorded_failures": list(evidence.failures),
                               "intent_attempt_proxy": None if proxy is None else {k: proxy[k] for k in ("status", "eligible_samples", "aggregate")},
                               "note": "the intent-to-attempt proxy of A0o is kept for comparison; it is a different quantity"})
    return _retarget(result, T01_DEFINITION, T01_FIELDS)


# ------------------------------------------------------------------------------------------------------------------ T06
def read_ingest_ledger(path: Path, *, run_id: str | None = None, source_id: str | None = None) -> dict[str, Any]:
    """Strictly parse an ingest ledger (canonical ASCII JSON, contiguous lseq, hash chain). Raises ``TimingSamplerError`` for a ledger that fails verification."""
    raw = Path(path).read_bytes()
    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError as exc:
        raise _fail("ledger_not_ascii", "the ingest ledger is not ASCII") from exc
    lines = text.split("\n")
    torn = len(lines[-1])
    rows: list[dict[str, Any]] = []
    prev = GENESIS_SHA256
    for number, line in enumerate(lines[:-1], 1):
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise _fail("ledger_row_malformed", f"ledger line {number} is not JSON") from exc
        if not isinstance(row, dict) or canonical(row) != line:
            raise _fail("ledger_row_not_canonical", f"ledger line {number} is not canonical (edited or re-serialised)")
        if row.get("schema") != rhi.INGEST_SCHEMA:
            raise _fail("ledger_schema_mismatch", f"ledger line {number} schema is {row.get('schema')!r}; a mixed bridge version is refused")
        if row.get("kind") not in rhi.LEDGER_KINDS:
            raise _fail("ledger_kind_unknown", f"ledger line {number} has kind {row.get('kind')!r}")
        if row.get("lseq") != number or row.get("prev") != prev or row.get("sha256") != event_digest(row, prev):
            raise _fail("ledger_chain_broken", f"ledger row {number} fails verification")
        prev = row["sha256"]
        rows.append(row)
    if not rows or rows[0]["kind"] != "ledger_start":
        raise _fail("ledger_not_started", "the ledger must begin with ledger_start")
    head = rows[0]
    if run_id is not None and head.get("run_id") != run_id or source_id is not None and head.get("source_id") != source_id:
        raise _fail("ledger_identity_mismatch", f"the ledger is for run {head.get('run_id')!r} / source {head.get('source_id')!r}")
    if any(r.get("run_id") != head["run_id"] or r.get("source_id") != head["source_id"] for r in rows):
        raise _fail("ledger_identity_mismatch", "the ledger mixes runs or sources")
    if any(r["kind"] == "ledger_start" for r in rows[1:]):
        raise _fail("ledger_start_repeated", "a second ledger_start")
    return {"rows": rows, "head": head, "torn_tail_bytes": torn, "input": _sha(path, "ingest_ledger"), "has_stop": rows[-1]["kind"] == "ledger_stop"}


def _utc_ms(text: Any) -> float:
    moment = parse_utc(text, "ledger time")
    return (moment - datetime(1970, 1, 1, tzinfo=timezone.utc)).total_seconds() * 1000.0


def t06_ingest(ledger: Mapping[str, Any], spool: rhb.SpoolRead | None, origin: str, offset_interval_ms: Sequence[float] | None) -> dict[str, Any]:
    """T06 from the incremental bridge's ingest ledger. ``offset_interval_ms`` is a T12 interval for (bridge clock - spool clock)."""
    head, rows = ledger["head"], ledger["rows"]
    spool_domain, bridge_domain = head.get("spool_clock_domain"), head.get("bridge_clock_domain")
    inputs = [ledger["input"]]
    spool_rows = {r["n"]: r for r in spool.rows} if spool is not None else None
    ingests = [r for r in rows if r["kind"] == "ingest"]
    exclusions, internal_read, internal_append, delays = [], [], [], []
    same = spool_domain == bridge_domain
    offset = None if offset_interval_ms is None else [float(offset_interval_ms[0]), float(offset_interval_ms[1])]
    if offset is not None and offset[0] > offset[1]:
        raise _fail("offset_interval_invalid", "the clock-offset interval must be [lo, hi] with lo <= hi")
    run = head.get("run_id")
    for r in rows:
        if r["kind"] == "recovery":
            exclusions.append({"index": r["lseq"], "reason": "missing: ingest_time_unknown",
                               "detail": f"journal rows {r.get('journal_seq_first')}..{r.get('journal_seq_last')} have no ingest stamp (restart recovery); the recovery time is not used"})
    for r in ingests:
        index = r["spool_n"] if r.get("spool_n") is not None else r["lseq"]
        if r.get("journal_rows_appended", 0) == 0:
            exclusions.append({"index": index, "reason": "missing: no_journal_row", "detail": "the spool row produced no journal row (nothing was ingested)"})
            continue
        if r.get("journal_append_confirmation") != "os_fsync_returned":
            exclusions.append({"index": index, "reason": "invalid: append_not_confirmed", "detail": "the journal append did not return from os.fsync"})
            continue
        if r.get("completed_after_restart"):
            exclusions.append({"index": index, "reason": "invalid: completed_after_restart", "detail": "this row was appended during restart recovery; its read stamp is the recovery time, not a poll read"})
            continue
        if spool_rows is not None and r.get("spool_n") in spool_rows and spool_rows[r["spool_n"]].get("t_utc") != r.get("spool_t_utc"):
            exclusions.append({"index": index, "reason": "invalid: spool_link_mismatch", "detail": "the ledger's spool_t_utc differs from the spool row's"})
            continue
        try:
            read_mono, ss, se = r["read_mono_us"], r["journal_append_start_mono_us"], r["journal_append_return_mono_us"]
            if not all(isinstance(v, int) and not isinstance(v, bool) for v in (read_mono, ss, se)) or not (read_mono <= ss <= se):
                raise ValueError("monotonic stamps are not ordered")
            ret_wall, spool_t = _utc_ms(r["journal_append_return_wall_utc"]), r.get("spool_t_utc")
        except Exception as exc:                  # noqa: BLE001  (any malformed stamp is an invalid sample, never a crash)
            exclusions.append({"index": index, "reason": "invalid: stamp_unusable", "detail": str(exc)[:120]})
            continue
        d_read = (se - read_mono) / 1000.0
        d_app = (se - ss) / 1000.0
        internal_read.append(ts.sample("T06", run, None, index, max(0.0, d_read - QUANT_MS), d_read + QUANT_MS, "bridge_read_to_append_return_mono_us", ["bridge_mono_us"]))
        internal_append.append(ts.sample("T06", run, None, index, max(0.0, d_app - QUANT_MS), d_app + QUANT_MS, "journal_append_mono_us", ["bridge_mono_us"]))
        if not (same or offset is not None) or spool_t is None:
            continue
        try:
            d = ret_wall - _utc_ms(spool_t)
        except Exception as exc:                                                                 # noqa: BLE001
            exclusions.append({"index": index, "reason": "invalid: spool_time_unusable", "detail": str(exc)[:120]})
            continue
        lo_off, hi_off = (0.0, 0.0) if same and offset is None else offset
        lo, hi = d - hi_off - SPOOL_MS_WIDTH - QUANT_MS, d - lo_off + QUANT_MS
        if hi < 0:
            exclusions.append({"index": index, "reason": "invalid: assumption_violated", "detail": f"the journal append returned {-d:.1f} ms BEFORE the spool row's time: the clocks disagree or the domains are not the same; never clamped"})
            continue
        delays.append(ts.sample("T06", run, None, index, max(0.0, lo), hi, "journal_append_return_wall_minus_spool_t_utc", [str(spool_domain), str(bridge_domain)],
                                offset=offset if offset is not None else [0.0, 0.0]))
    internal = {"bridge_read_to_append_return_ms": ts.aggregate(internal_read), "journal_append_ms": ts.aggregate(internal_append),
                "meaning": "elapsed time inside the bridge process (monotonic clock); not a cross-host delay; the first includes the wait behind earlier rows of the same poll",
                "eligible_samples": len(internal_read)}
    cross = same or offset is not None
    if cross:
        status = ts.S_MEASURED if delays else ts.S_NONE
        dependency = None
        clock_basis = ("operator-declared SAME clock domain for the spool and the bridge (a declaration, not a measurement of clock agreement)" if same and offset is None
                       else f"T12 offset interval [{offset[0]}, {offset[1]}] ms (bridge clock minus spool clock), supplied by the caller")
    else:
        status = ts.S_NOT_MEASURABLE
        dependency = (f"a T12 clock-offset interval between the spool clock domain {spool_domain!r} and the bridge clock domain {bridge_domain!r} (or an operator declaration that they "
                      "are the same domain): without it ingest_wall - spool.t_utc differences two unrelated clocks")
        clock_basis = "none: the domains differ and no offset interval was supplied"
    result = ts._result("T06", run, status=status, origin=origin, inputs=inputs, clock_domain=f"spool: {spool_domain}; ingest: {bridge_domain}", samples=delays, exclusions=exclusions,
                        missing_dependency=dependency,
                        reason=None if cross else "the delay cannot be formed across clock domains without clock evidence; the within-process quantities are reported separately",
                        extra={"quantity_measured": "journal_append_return_wall - spool t_utc (+ offset band)", "clock_basis": clock_basis, "bridge_internal": internal,
                               "internal_status": ts.S_MEASURED if internal_read else ts.S_NONE,
                               "uncertainty_ms": {"spool_time_precision": SPOOL_MS_WIDTH, "quantisation": QUANT_MS, "clock_offset": offset, "bridge_cadence": "the poll interval bounds how late a row can be read"},
                               "ingest_rows": len(ingests), "unknown_ingest_rows": sum(1 for r in rows if r["kind"] == "recovery"), "ledger_has_stop": ledger["has_stop"],
                               "torn_tail_bytes": ledger["torn_tail_bytes"]})
    return _retarget(result, T06_DEFINITION, T06_FIELDS)


# ------------------------------------------------------------------------------------------------------------------ entry point
def run_sampler_a0p(*, origin: str, issue_log: Path | None = None, issue_log_timing: Path | None = None, submits: Path | None = None, submits_clock_domain: str | None = None,
                    checkpoint: Path | None = None, reader_clock_domain: str | None = None, offset_interval_ms: Sequence[float] | None = None, spool: Path | None = None,
                    ingest_ledger: Path | None = None, bridge_offset_interval_ms: Sequence[float] | None = None, candidate_gap_limit_s: float | None = None) -> dict[str, Any]:
    """T01 to T06. T02 to T05 are the A0o computations; T01 and T06 use the new evidence when it is supplied and otherwise keep the A0o answer (proxy / not measurable)."""
    if issue_log_timing is not None and issue_log is None:
        raise _fail("issue_log_required", "the timing sidecar is verified against its issue log: supply the log too")
    out = ts.run_sampler(origin=origin, issue_log=issue_log, submits=submits, submits_clock_domain=submits_clock_domain, checkpoint=checkpoint,
                         reader_clock_domain=reader_clock_domain, offset_interval_ms=offset_interval_ms, spool=spool, candidate_gap_limit_s=candidate_gap_limit_s)
    out["schema"], out["sampler"], out["wrapped_sampler"] = RESULT_SCHEMA, SAMPLER_VERSION, ts.SAMPLER_VERSION
    out["supersedes_results_of"] = {"T01": "only when issue_log_timing is supplied", "T06": "only when ingest_ledger is supplied"}
    if issue_log_timing is not None:
        log = ts.read_issue_log(issue_log)
        evidence = ilt.verify_timing(issue_log, issue_log_timing)
        out["inputs"].extend(i for i in evidence.inputs if i["role"] == "timing_sidecar")
        out["measurements"]["T01"] = t01_direct(log, evidence, origin)
    if ingest_ledger is not None:
        ledger = read_ingest_ledger(ingest_ledger)
        out["inputs"].append(ledger["input"])
        read = None
        if spool is not None:
            read, _ = ts._spool(spool)
            if read.break_at is not None or read.torn_tail:
                read = None
        out["measurements"]["T06"] = t06_ingest(ledger, read, origin, bridge_offset_interval_ms)
    out["measurements"] = {k: out["measurements"][k] for k in sorted(out["measurements"])}
    out["statement"] = ("Offline fixture arithmetic. No value here is a Caliper, Fabric or live-clock measurement, no operator threshold is approved, and Windows-WSL clock agreement was not "
                        "measured. A write or sync duration is not a persistence guarantee; a same-domain T06 rests on an operator declaration.")
    return out
