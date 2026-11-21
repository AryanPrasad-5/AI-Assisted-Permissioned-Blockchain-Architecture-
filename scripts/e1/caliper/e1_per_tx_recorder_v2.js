'use strict';
/*
 * e1_per_tx_recorder_v2: the v1 per-transaction recorder plus an INCREMENTAL live stream (task A0c, G1 and G2).
 *
 * v1 (e1_per_tx_recorder.js, pinned, unchanged) keeps every row in memory and writes its three files once, in
 * deactivate(). Caliper delivers TxsFinished/TxsSubmitted to observers as transactions complete (caliper-worker.js:
 * connector.on(Events.TxsFinished, ...) -> txObserverDispatch.txFinished), so the rows can be appended while the
 * round runs. This class extends v1 and adds only that. It does NOT replace v1's outputs: the final
 * <base>.csv, <base>.submits.csv and <base>.manifest.json are produced by v1's own deactivate(), byte for byte.
 *
 * Extra files, in the same output directory, with <base> = <runId>_w<worker>_r<round>_<label>:
 *   <base>.live.csv          one row per finished transaction, appended as it finishes (PROVISIONAL until finalized)
 *   <base>.live.submits.csv  one row per submit event, appended as it happens (PROVISIONAL until finalized)
 *   <base>.live.open.json    written at activate: schema, columns and definitions of the live stream
 *   <base>.live.final.json   written at deactivate: row counts, SHA-256 of both live files, reconciliation with the
 *                            final files. Its ABSENCE means the live stream is provisional, in progress or interrupted.
 *
 * Live row schema "e1_per_tx_live/1" (columns, in order):
 *   tx_id, seq_finish, create_ms, final_ms, latency_ms, latency_state, status, verified, err,
 *   obs_wall_ms, obs_hr_ns, worker, round, label
 *   create_ms / final_ms  EVENT time: Caliper client wall clock, epoch milliseconds (TxStatus.GetTimeCreate/GetTimeFinal).
 *   obs_wall_ms / obs_hr_ns  COLLECTION time: when this observer saw the result (Date.now, and process.hrtime relative
 *                            to the round start). They are not transaction event times.
 *   latency_ms   final_ms - create_ms in milliseconds, measured, never predicted. EMPTY (never 0) when it cannot be
 *                computed; latency_state then says why. latency_state values equal client_latency.LatencyState.
 *                Eligibility for a latency statistic is decided by the reader: status == success AND latency_state == ok.
 *   err          first error message, single line, at most 200 characters (the final CSV keeps the raw text).
 *
 * Guarantees and limits (what is and is not claimed):
 *   - Each result appears once in the live stream. Read EITHER the live stream OR the final CSV for a round, not both.
 *   - Writes contain whole lines only, so a reader that ignores a last line without "\n" never sees a torn row.
 *   - Bounded buffering: at most liveFlushRows rows wait in memory for the live stream (default 50), and a timer
 *     flushes every liveFlushIntervalMs (default 500) even when fewer have arrived. The rows kept in memory for the
 *     final files grow with the round, exactly as in v1.
 *   - Freshness granularity is therefore about liveFlushIntervalMs. A reader's staleness limit must exceed it.
 *   - The live stream is best effort. If writing it fails, the failure is counted and reported in live.final.json and
 *     the round's final files (the authoritative record) are unaffected.
 *   - This is continuous only for the client side (recorder rows and submit events). It says nothing about the ledger.
 *   - A killed worker leaves a live stream with no live.final.json and no final files: provisional, incomplete.
 *   - The observer never throws into Caliper (R3 of v1): every failure is caught and counted.
 *
 * Nothing here talks to Fabric, Docker or a network. It only writes files under options.outputDir.
 */
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const v1 = require('./e1_per_tx_recorder.js');

const RECORDER_V2_VERSION = 'e1_per_tx_recorder_v2 0.1.0 (2026-11-21)';
const LIVE_SCHEMA = 'e1_per_tx_live/1';
const LIVE_COLUMNS = ['tx_id', 'seq_finish', 'create_ms', 'final_ms', 'latency_ms', 'latency_state', 'status',
    'verified', 'err', 'obs_wall_ms', 'obs_hr_ns', 'worker', 'round', 'label'];
const LIVE_SUBMIT_COLUMNS = ['seq_submit', 't_submit_evt_ms', 'count'];
const ERR_MAX = 200;
const DEFAULT_FLUSH_ROWS = 50;
const DEFAULT_FLUSH_INTERVAL_MS = 500;
const LATENCY_DEFINITION = 'latency_ms = final_ms - create_ms (client wall clock, Caliper Date.now(), epoch milliseconds)';

function csvField(value) {
    if (value === undefined || value === null) { return ''; }
    const s = String(value);
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function singleLine(text) {
    return String(text === undefined || text === null ? '' : text).replace(/[\r\n]+/g, ' ').slice(0, ERR_MAX);
}

function writeNew(target, content) {
    if (fs.existsSync(target)) { throw new Error(`refusing to overwrite existing ${target}`); }
    const tmp = `${target}.tmp`;
    fs.writeFileSync(tmp, content, { flag: 'wx' });
    fs.renameSync(tmp, target);
}

/** One epoch-millisecond field: a finite positive number, or the reason it is not. Blank is missing, never zero. */
function epochField(value, missingState) {
    if (value === undefined || value === null || (typeof value === 'string' && value.trim() === '')) {
        return { state: missingState };
    }
    if (typeof value === 'boolean') { return { state: 'boolean_value' }; }
    let n;
    if (typeof value === 'number') {
        n = value;
    } else {
        n = Number(String(value).trim());
        if (Number.isNaN(n)) { return { state: 'not_numeric' }; }
    }
    if (!Number.isFinite(n)) { return { state: 'nonfinite' }; }
    if (n <= 0) { return { state: 'nonpositive_timestamp' }; }
    return { value: n };
}

/** Mirrors fabric_perf.controllers.client_latency.derive_latency_ms (state strings are identical). */
function latencyOf(create, final) {
    const c = epochField(create, 'missing_create_timestamp');
    if (c.state) { return { latency_ms: '', state: c.state }; }
    const f = epochField(final, 'missing_final_timestamp');
    if (f.state) { return { latency_ms: '', state: f.state }; }
    const latency = f.value - c.value;
    if (latency < 0) { return { latency_ms: '', state: 'negative_latency' }; }
    return { latency_ms: latency, state: 'ok' };
}

/** Append-only line file with a bounded buffer. Every write holds whole lines only. */
class LineStream {
    constructor(filePath, header, flushRows) {
        this.path = filePath;
        this.flushRows = flushRows;
        this.fd = fs.openSync(filePath, 'wx');
        this.buf = [];
        this.rowsPushed = 0;
        this.rowsWritten = 0;
        this.hash = crypto.createHash('sha256');
        this.failed = false;
        this.firstError = null;
        this.closed = false;
        this._write(`${header}\n`);
    }

    _write(text) {
        fs.writeSync(this.fd, text);
        this.hash.update(text);
    }

    push(line) {
        if (this.closed || this.failed) { return; }
        this.rowsPushed += 1;
        this.buf.push(line);
        if (this.buf.length >= this.flushRows) { this.flush(); }
    }

    flush() {
        if (this.closed || this.failed || this.buf.length === 0) { return; }
        const n = this.buf.length;
        try {
            this._write(this.buf.join(''));
            this.rowsWritten += n;
        } catch (err) {
            // Disk problems must not grow the buffer without bound or reach Caliper: give the stream up, say so.
            this.failed = true;
            this.firstError = String(err && err.stack || err);
        }
        this.buf = [];
    }

    close() {
        if (this.closed) { return; }
        this.flush();
        this.closed = true;
        try { fs.closeSync(this.fd); } catch (err) { this.failed = true; this.firstError = this.firstError || String(err); }
    }
}

class LiveStreamRecorder extends v1.PerTxRecorder {
    constructor(options, workerIndex) {
        super(options, workerIndex);
        const rows = options.liveFlushRows === undefined ? DEFAULT_FLUSH_ROWS : options.liveFlushRows;
        const ms = options.liveFlushIntervalMs === undefined ? DEFAULT_FLUSH_INTERVAL_MS : options.liveFlushIntervalMs;
        if (!Number.isInteger(rows) || rows < 1) { throw new Error('e1_per_tx_recorder_v2: liveFlushRows must be a positive integer'); }
        if (!Number.isInteger(ms) || ms < 10) { throw new Error('e1_per_tx_recorder_v2: liveFlushIntervalMs must be an integer >= 10'); }
        this.flushRows = rows;
        this.flushIntervalMs = ms;
        this.live = null;
        this.timer = null;
        this.liveErrors = 0;
        this.firstLiveError = null;
    }

    _liveNote(err) {
        this.liveErrors += 1;
        if (!this.firstLiveError) { this.firstLiveError = String(err && err.stack || err); }
    }

    _liveBase() { return this._base(); }

    async activate(roundIndex, roundLabel) {
        await super.activate(roundIndex, roundLabel);
        this.liveErrors = 0;
        this.firstLiveError = null;
        try {
            const base = this._liveBase();
            this.live = {
                tx: new LineStream(`${base}.live.csv`, LIVE_COLUMNS.join(','), this.flushRows),
                sub: new LineStream(`${base}.live.submits.csv`, LIVE_SUBMIT_COLUMNS.join(','), this.flushRows),
                openedWallMs: Date.now(),
            };
            writeNew(`${base}.live.open.json`, `${JSON.stringify({
                recorder: RECORDER_V2_VERSION, schema: LIVE_SCHEMA, state: 'open', provisional: true,
                run_id: this.runId, worker: this.worker, round: this.round, label: this.label,
                columns: LIVE_COLUMNS, submit_columns: LIVE_SUBMIT_COLUMNS,
                latency: { definition: LATENCY_DEFINITION, unit: 'ms', empty_means: 'not computable; see latency_state' },
                time_semantics: { create_ms_final_ms: 'event time, client wall clock, epoch ms',
                    obs_wall_ms_obs_hr_ns: 'collection time at this observer; not an event time' },
                flush: { rows: this.flushRows, interval_ms: this.flushIntervalMs },
                opened_wall_ms: this.live.openedWallMs,
                finalized_by: `${path.basename(base)}.live.final.json (absent while provisional or if interrupted)`,
            }, null, 2)}\n`);
            this.timer = setInterval(() => { this.flushLive(); }, this.flushIntervalMs);
            if (this.timer && typeof this.timer.unref === 'function') { this.timer.unref(); }
        } catch (err) {
            this._liveNote(err);
        }
    }

    txSubmitted(count) {
        super.txSubmitted(count);
        try {
            if (!this.live || !this.active) { return; }
            const seq = this.subs.count.length - 1;
            this.live.sub.push(`${seq},${this.subs.t_submit_evt_ms[seq]},${this.subs.count[seq]}\n`);
        } catch (err) {
            this._liveNote(err);
        }
    }

    txFinished(results) {
        const before = this.cols.tx_id.length;
        super.txFinished(results);
        try {
            if (!this.live || !this.active) { return; }
            const c = this.cols;
            const seenAt = Date.now();
            const hr0 = this.anchors.hr0;
            for (let i = before; i < c.tx_id.length; i++) {
                const lat = latencyOf(c.create_ms[i], c.final_ms[i]);
                this.live.tx.push([c.tx_id[i], i, c.create_ms[i], c.final_ms[i], lat.latency_ms, lat.state, c.status[i],
                    c.verified[i], singleLine(c.err[i]), seenAt, (c.obs_hr_ns[i] - hr0).toString(), this.worker,
                    this.round, this.label].map(csvField).join(',') + '\n');
            }
        } catch (err) {
            this._liveNote(err);
        }
    }

    /** Write any buffered live rows now. Safe to call at any time; never throws. */
    flushLive() {
        try {
            if (this.live) { this.live.tx.flush(); this.live.sub.flush(); }
        } catch (err) {
            this._liveNote(err);
        }
    }

    async deactivate() {
        if (this.timer) { clearInterval(this.timer); this.timer = null; }
        const live = this.live;
        const base = this._liveBase();
        try {
            if (live) { live.tx.close(); live.sub.close(); }
        } catch (err) {
            this._liveNote(err);
        }
        // The final files are v1's own, unchanged.
        await super.deactivate();
        try {
            if (!live) { return; }
            const finalManifest = `${base}.manifest.json`;
            const finalOk = fs.existsSync(finalManifest) && fs.existsSync(`${base}.csv`);
            const finalRows = this.cols.tx_id.length;
            const failed = live.tx.failed || live.sub.failed || this.liveErrors > 0;
            writeNew(`${base}.live.final.json`, `${JSON.stringify({
                recorder: RECORDER_V2_VERSION, schema: LIVE_SCHEMA, state: 'finalized', provisional: false,
                run_id: this.runId, worker: this.worker, round: this.round, label: this.label,
                live_stream_failed: failed, live_errors: this.liveErrors, first_live_error: this.firstLiveError,
                first_tx_stream_error: live.tx.firstError, first_submit_stream_error: live.sub.firstError,
                counts: {
                    results_seen: finalRows, tx_rows_pushed: live.tx.rowsPushed, tx_rows_written: live.tx.rowsWritten,
                    submit_events_seen: this.subs.count.length, submit_rows_pushed: live.sub.rowsPushed,
                    submit_rows_written: live.sub.rowsWritten,
                },
                reconciles_with_final_files: finalOk && !failed && live.tx.rowsWritten === finalRows
                    && live.sub.rowsWritten === this.subs.count.length,
                final_files_present: finalOk,
                files: {
                    live_csv: path.basename(`${base}.live.csv`), live_csv_sha256: live.tx.hash.digest('hex'),
                    live_submits_csv: path.basename(`${base}.live.submits.csv`),
                    live_submits_sha256: live.sub.hash.digest('hex'),
                    final_csv: path.basename(`${base}.csv`), final_manifest: path.basename(finalManifest),
                },
                note: 'the final files are authoritative; read either the live stream or the final CSV for a round, not both',
            }, null, 2)}\n`);
        } catch (err) {
            this._liveNote(err);
            try { fs.writeFileSync(`${base}.live.FAILED`, `${new Date().toISOString()} ${err && err.stack || err}\n`, { flag: 'a' }); } catch (inner) { /* nothing more can be done without throwing */ }
        }
    }
}

function createTxObserver(options, messenger, workerIndex) {
    if (!options || typeof options.runId !== 'string' || options.runId.length === 0) {
        throw new Error('e1_per_tx_recorder_v2: options.runId (non-empty string) is required');
    }
    if (typeof options.outputDir !== 'string' || !path.isAbsolute(options.outputDir)) {
        throw new Error('e1_per_tx_recorder_v2: options.outputDir must be an absolute path');
    }
    fs.mkdirSync(options.outputDir, { recursive: true });
    const probe = path.join(options.outputDir, `.probe_${process.pid}_${workerIndex}_${Date.now()}`);
    fs.writeFileSync(probe, 'probe', { flag: 'wx' });
    fs.unlinkSync(probe);
    return new LiveStreamRecorder(options, workerIndex);
}

module.exports.createTxObserver = createTxObserver;
module.exports.LiveStreamRecorder = LiveStreamRecorder;
module.exports.LIVE_COLUMNS = LIVE_COLUMNS;
module.exports.LIVE_SUBMIT_COLUMNS = LIVE_SUBMIT_COLUMNS;
module.exports.LIVE_SCHEMA = LIVE_SCHEMA;
module.exports.latencyOf = latencyOf;
