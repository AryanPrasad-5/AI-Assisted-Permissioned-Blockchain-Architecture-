'use strict';

// Write-timing successor of the generator-side issue-log writer (task A0p, 2026-11-22).
//
// e1_issue_log.js (A0l) is left byte-identical. This module extends its writer and changes NOTHING in the issue log it writes: the log is still
// schema e1_issue_log/1, record for record, so the A0l/A0o reader and every historical tool read it unchanged. What it adds is a SECOND file, the
// timing sidecar (schema e1_issue_log_timing/1): an append-only, hash-chained companion that says WHEN each log record's write finished and WHEN a
// sync of the log file returned.
//
// Why a sidecar and not a field in the record: a record cannot contain the time its own write finished (that time does not exist until the write
// has returned, and a predicted value would be a fabrication). The completion evidence is therefore a LATER record, linked to the earlier one by
// rseq and by the earlier record's sha256:
//
//   tstart   the sidecar opened (identity, clock sources, sync semantics)
//   write    log record <target_rseq> was written: write_start (before the first writeSync of the record) and write_end (after the last writeSync
//            returned), each as wall-clock ms (Date.now) and monotonic microseconds (process.hrtime). The record's own t_ms is its CREATION time.
//   sync     a sync of the log file RETURNED without error: it covers log records covered_from_rseq..covered_through_rseq. sync_start / sync_end.
//   failure  a write or a sync of the log file failed (the writer is then failed and refuses every later issuance)
//   tstop    the sidecar closed cleanly (final counts)
//
// What the evidence does and does not say (it never says more than the API returned):
//   * "write_end" means fs.writeSync returned having accepted every byte of the record. It does NOT mean the bytes reached storage.
//   * "sync_end" means fs.fsyncSync(fd) returned without an exception. That is the operating system's own flush request (fsync, or
//     FlushFileBuffers on Windows) having reported success; it is not an absolute guarantee against every storage, controller or filesystem failure
//     (bind mounts and network filesystems can weaken it). The word "durable" is never written into the sidecar.
//   * A log record with no `write` row in the sidecar is UNATTESTED (a crash between the log write and the sidecar write, or a failed sidecar).
//   * A log record not covered by any `sync` row is NOT claimed to have reached storage.
//
// Fail-closed: the sidecar is written inside the same _write call that writes the log record. If the sidecar write fails, the writer is marked
// failed (the log record already written stays; nothing further is issued) and the caller sees write_failed, exactly as for a failed log write. An
// intent whose attempt could not be written is therefore never followed by a send. After a send, a failed outcome write marks the writer failed and
// never changes the send's result (inherited behaviour).
//
// The sidecar has its own clock and monotonic hooks (timingClock, monoUs) so a test can script them without perturbing the log's t_ms / mono_ms.
// The sidecar is fsynced once, at close. A sidecar lost in a crash costs timing evidence only; it never changes what the log proves.

const fs = require('fs');
const path = require('path');
const base = require('./e1_issue_log.js');

const TIMING_SCHEMA = 'e1_issue_log_timing/1';
const TIMED_WRITER_VERSION = 'e1_issue_log_timed_writer 0.1 (A0p, 2026-11-22)';
const LOG_SCHEMA = base.ISSUE_LOG_SCHEMA;
const { IssueLogError, GENESIS_SHA256, canonical, digestOf } = base;
const SYNC_SEMANTICS = 'fs.fsyncSync(fd) returned without an exception: the operating system accepted the flush request for the file; not a guarantee against every storage failure';
const WRITE_SEMANTICS = 'fs.writeSync returned having accepted all bytes of the record; the bytes may still be in an operating-system cache';

function defaultMonoUs() { return Number(process.hrtime.bigint() / 1000n); }

function errText(err) {
    const t = err && err.message ? err.message : String(err);
    return String(t).replace(/[^\x20-\x7e]/g, '?').slice(0, 200);
}

// Wraps a file-system implementation so the calls the log writer makes are timed. It records; it never changes what the call returns or throws.
function timedFs(inner, cap, clocks) {
    return {
        mkdirSync: (...a) => inner.mkdirSync(...a),
        existsSync: (...a) => inner.existsSync(...a),
        openSync: (...a) => inner.openSync(...a),
        closeSync: (...a) => inner.closeSync(...a),
        writeSync: (...a) => {
            if (cap.write.start === null) { cap.write.start = { wall: clocks.wall(), mono: clocks.mono() }; }
            const n = inner.writeSync(...a);
            cap.write.end = { wall: clocks.wall(), mono: clocks.mono() };
            return n;
        },
        fsyncSync: (...a) => {
            const start = { wall: clocks.wall(), mono: clocks.mono() };
            try {
                const out = inner.fsyncSync(...a);
                cap.syncs.push({ start, end: { wall: clocks.wall(), mono: clocks.mono() }, inWrite: cap.inWrite });
                return out;
            } catch (err) {
                cap.syncFailures.push({ start, end: { wall: clocks.wall(), mono: clocks.mono() }, error: errText(err), inWrite: cap.inWrite });
                throw err;
            }
        },
    };
}

class TimedIssueLogWriter extends base.IssueLogWriter {
    constructor(options) {
        const o = options || {};
        if (typeof o.timingPath !== 'string' || !path.isAbsolute(o.timingPath)) { throw new IssueLogError('timing_path_invalid', 'timingPath must be an absolute path'); }
        if (o.timingPath === o.path) { throw new IssueLogError('timing_path_invalid', 'the timing sidecar must be a different file from the log'); }
        const inner = o.fsImpl || fs;
        const clocks = { wall: o.timingClock || (() => Date.now()), mono: o.monoUs || defaultMonoUs };
        const cap = { write: { start: null, end: null }, syncs: [], syncFailures: [], inWrite: false };
        super({ ...o, fsImpl: timedFs(inner, cap, clocks) });
        this.timingPath = o.timingPath;
        this._inner = inner;
        this._clocks = clocks;
        this._cap = cap;
        this.tfd = null;
        this.tseq = 0;
        this.tprev = GENESIS_SHA256;
        this.syncedThrough = 0;
        this.timingFailed = false;
        this.timingFailure = null;
        this.timingCounts = { writes: 0, syncs: 0, failures: 0 };
        this.timingClosed = false;
    }

    open() {
        if (this.fd !== null || this.tfd !== null) { throw new IssueLogError('already_open', this.path); }
        this._inner.mkdirSync(path.dirname(this.path), { recursive: true });
        if (this._inner.existsSync(this.path)) { throw new IssueLogError('log_exists', `refusing to overwrite ${this.path}`); }   // before the sidecar is created
        this._inner.mkdirSync(path.dirname(this.timingPath), { recursive: true });
        if (this._inner.existsSync(this.timingPath)) { throw new IssueLogError('timing_exists', `refusing to overwrite ${this.timingPath}`); }
        try {
            this.tfd = this._inner.openSync(this.timingPath, 'wx');
        } catch (err) {
            this._fail(`timing open: ${err.message}`);
            throw new IssueLogError('write_failed', `cannot create the timing sidecar: ${err.message}`);
        }
        try {
            this._twrite('tstart', {
                timing_writer_version: TIMED_WRITER_VERSION, log_name: path.basename(this.path), log_schema: LOG_SCHEMA, round_index: this.roundIndex,
                clock_domain: this.clockDomain, wall_clock_source: 'Date.now()', wall_precision: 'ms', mono_clock_source: 'process.hrtime.bigint()',
                mono_unit: 'us', mono_origin: 'process_monotonic_arbitrary', fsync_policy: this.fsyncPolicy, write_semantics: WRITE_SEMANTICS,
                sync_semantics: SYNC_SEMANTICS,
            });
        } catch (err) {
            this._fail(`timing start: ${err.message}`);
            throw new IssueLogError('write_failed', `timing start: ${err.message}`);
        }
        return super.open();
    }

    _twrite(kind, fields) {
        const body = {
            schema: TIMING_SCHEMA, tseq: this.tseq + 1, kind, run_id: this.runId, worker_id: this.workerId, ...fields,
        };
        body.prev = this.tprev;
        body.sha256 = digestOf(body, this.tprev);
        const buffer = Buffer.from(`${canonical(body)}\n`, 'utf8');
        let written = 0;
        while (written < buffer.length) {
            const n = this._inner.writeSync(this.tfd, buffer, written, buffer.length - written);
            if (!(n > 0)) { throw new IssueLogError('short_write', 'timing writeSync wrote nothing'); }
            written += n;
        }
        this.tseq += 1;
        this.tprev = body.sha256;
        return body;
    }

    // The log record is written by the inherited _write; the sidecar rows that describe it are written right after, inside the same call.
    _write(kind, fields) {
        this._cap.write = { start: null, end: null };
        this._cap.inWrite = true;
        let body;
        try {
            body = super._write(kind, fields);
        } catch (err) {
            this._cap.inWrite = false;
            if (this._cap.syncFailures.length) {                                   // the log write itself was fine; its fsync failed (policy 'every')
                try { this._flushSyncs(this.rseq); } catch (e) { this._timingFail(`failure record: ${e.message}`); }
            } else { this._recordFailure('write', kind, err); }
            throw err;
        }
        this._cap.inWrite = false;
        try {
            const w = this._cap.write;
            if (w.start === null || w.end === null) { throw new IssueLogError('timing_unavailable', 'the log write was not observed'); }
            this._twrite('write', {
                target_rseq: body.rseq, target_kind: body.kind, target_sha256: body.sha256, target_t_ms: body.t_ms, target_mono_ms: body.mono_ms,
                write_start_wall_ms: w.start.wall, write_end_wall_ms: w.end.wall, write_start_mono_us: w.start.mono, write_end_mono_us: w.end.mono,
                write_status: 'returned',
            });
            this.timingCounts.writes += 1;
            this._flushSyncs(body.rseq);
        } catch (err) {
            this._timingFail(`write attestation for record ${body.rseq}: ${err.message}`);
            throw new IssueLogError('write_failed', `${kind}: timing sidecar: ${err.message}`);
        }
        return body;
    }

    // Writes the sync rows for every fsync that returned (or failed) since the last flush. A sync covers every record written before it was called.
    _flushSyncs(throughRseq) {
        const cap = this._cap;
        const done = cap.syncs.splice(0, cap.syncs.length);
        for (const s of done) {
            const through = s.inWrite ? throughRseq : this.rseq;
            this._twrite('sync', {
                call: 'fsyncSync', covered_from_rseq: this.syncedThrough + 1, covered_through_rseq: through, sync_start_wall_ms: s.start.wall,
                sync_end_wall_ms: s.end.wall, sync_start_mono_us: s.start.mono, sync_end_mono_us: s.end.mono, sync_status: 'returned',
            });
            this.syncedThrough = Math.max(this.syncedThrough, through);
            this.timingCounts.syncs += 1;
        }
        const failed = cap.syncFailures.splice(0, cap.syncFailures.length);
        for (const s of failed) {
            this._twrite('failure', {
                op: 'sync', target_kind: null, target_rseq: this.rseq, error: s.error, failure_wall_ms: s.end.wall, failure_mono_us: s.end.mono,
                sync_start_wall_ms: s.start.wall, sync_start_mono_us: s.start.mono,
            });
            this.timingCounts.failures += 1;
        }
    }

    _recordFailure(op, kind, err) {
        try {
            if (this.tfd === null || this.timingFailed || this.timingClosed) { return; }
            this._twrite('failure', {
                op, target_kind: kind, target_rseq: this.rseq + 1, error: errText(err), failure_wall_ms: this._clocks.wall(), failure_mono_us: this._clocks.mono(),
            });
            this.timingCounts.failures += 1;
        } catch (e) { this._timingFail(`failure record: ${e.message}`); }
    }

    _timingFail(reason) {
        this.timingFailed = true;
        this.timingFailure = this.timingFailure || String(reason).slice(0, 200);
        this._fail(`timing: ${reason}`);
    }

    // tick: the inherited method writes the tick record and then fsyncs the log; the fsync is attested here.
    tick() {
        const ok = super.tick();
        this._afterSyncCall('tick');
        return ok && !this.failed;
    }

    _afterSyncCall(what) {
        try {
            if (this.tfd !== null && !this.timingClosed && !this.timingFailed && (this._cap.syncs.length || this._cap.syncFailures.length)) { this._flushSyncs(this.rseq); }
        } catch (err) { this._timingFail(`${what} sync attestation: ${err.message}`); }
    }

    close(reason) {
        const ok = super.close(reason);
        this._afterSyncCall('close');
        this._closeTiming();
        return ok && !this.timingFailed;
    }

    _closeTiming() {
        if (this.tfd === null || this.timingClosed) { return; }
        try {
            if (!this.timingFailed) {
                this._twrite('tstop', { log_records: this.rseq, writes: this.timingCounts.writes, syncs: this.timingCounts.syncs, failures: this.timingCounts.failures,
                    synced_through_rseq: this.syncedThrough });
                this._inner.fsyncSync(this.tfd);
            }
        } catch (err) { this.timingFailed = true; this.timingFailure = this.timingFailure || `tstop: ${errText(err)}`; }
        try { this._inner.closeSync(this.tfd); } catch (err) { this.timingFailed = true; }
        this.timingClosed = true;
    }

    status() {
        return { ...super.status(), timing_path: this.timingPath, timing_failed: this.timingFailed, timing_failure: this.timingFailure,
            timing_rows: this.tseq, timing_writes: this.timingCounts.writes, timing_syncs: this.timingCounts.syncs, synced_through_rseq: this.syncedThrough };
    }
}

module.exports = { TIMING_SCHEMA, TIMED_WRITER_VERSION, SYNC_SEMANTICS, WRITE_SEMANTICS, TimedIssueLogWriter, wrapSutAdapter: base.wrapSutAdapter, IssueLogError };
