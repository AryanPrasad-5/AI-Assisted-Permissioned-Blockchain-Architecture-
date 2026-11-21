'use strict';
/*
 * e1_per_tx_recorder_v3: the v2 live recorder plus a RECORDER-HEALTH SPOOL (task A0j, 2026-11-21).
 *
 * v1 (e1_per_tx_recorder.js) and v2 (e1_per_tx_recorder_v2.js) are pinned and unchanged. v3 extends v2's LiveStreamRecorder and adds one
 * file per round, <base>.health.jsonl, that describes WHAT THE RECORDER ITSELF OBSERVED about its own operation. It never changes the
 * v1/v2 outputs.
 *
 * WHAT THIS SPOOL CAN AND CANNOT ESTABLISH (read before relying on it)
 *   - It can establish that the recorder process was running, that its TxSubmitted callback fed it submit events, that those events
 *     were durably appended to <base>.live.submits.csv before each heartbeat, and that no recorder-internal error occurred.
 *   - It CANNOT establish that Caliper delivered every submit event to the callback, or that the workload generator issued no
 *     transaction the callback never saw. The recorder is a passive observer: it has no view of the source.
 *   - Therefore the start row declares coverage_scope = "recorder_callback". A zero-arrival second is NOT certified by heartbeats alone.
 *     A zero can be certified only when the operator also supplies an independent source count (options.health.sourceCount) and the
 *     recorder's count agrees with it. No such provider exists in this repository today; see the A0j report.
 *   - Time covered is only time between a start row and the last durable heartbeat watermark. A killed process leaves no stop row.
 *
 * The spool is NOT hash-chained here: a second hash-chain implementation would be a second, incompatible integrity format. The Python
 * bridge (fabric_perf.controllers.recorder_health_bridge) replays it into the one authoritative CollectorHealthJournal, which chains it.
 *
 * Spool row schema "recorder_health_spool/1": one JSON object per line, whole lines only, fsynced per row. Common fields:
 *   schema, n (1,2,3..., contiguous), kind (start | heartbeat | error | recovered | stop), t_utc (ISO-8601 UTC, milliseconds),
 *   mono_ms (monotonic milliseconds since activate), source_id, collector_id, collector_instance, run_id, worker, round, label.
 *   start     observation_start_utc (the NEXT whole second after the row is written: never retroactive), coverage_scope,
 *             evidence_origin, recorder_version, options_sha256, initialized_utc, declared_policy, timestamp_provenance.
 *   heartbeat observed_through_utc (whole second W: every submit event with ts < W is already in the live submits file),
 *             events_received (cumulative submitted TRANSACTIONS with obsStart <= ts < W), health, errors_since_last, phase, and
 *             optionally source_events_expected / source_basis / source_independence_claim.
 *   error     error_code, message, affects_from_utc.
 *   stop      observed_through_utc, events_received, reason (clean | abort), phase = 'window_close'.
 *   phase on a heartbeat: heartbeat | checkpoint | window_close (informational). initialization, callback activation and window open are
 *   recorded in the start row's "lifecycle" object (a heartbeat cannot precede the observation start). A round that ends inside its first
 *   second never opened its window and writes no stop row.
 *
 * Time: t_utc and event times are wall-clock Date.now() (event time of a submit event = the moment this observer saw TxSubmitted, taken
 * by v1). mono_ms is process.hrtime based so a wall-clock step can be detected by the reader. They are never the same quantity.
 *
 * The observer never throws into Caliper: every failure is caught and counted. If the spool cannot be written, health evidence simply
 * stops; the absence of a stop row and of later heartbeats leaves the remaining time UNCOVERED (fail closed).
 *
 * Nothing here talks to Fabric, Docker or a network. It writes files under options.outputDir.
 */
const fs = require('fs');
const path = require('path');
const v2 = require('./e1_per_tx_recorder_v2.js');

const RECORDER_V3_VERSION = 'e1_per_tx_recorder_v3 0.1.0 (2026-11-21)';   // A0k added the optional sourceCount.descriptor pass-through; no other behaviour changed
const SPOOL_SCHEMA = 'recorder_health_spool/1';
const COVERAGE_SCOPE = 'recorder_callback';
const EVIDENCE_ORIGINS = ['live_recorder', 'synthetic_fixture'];

function isoMs(ms) { return new Date(ms).toISOString(); }
function floorSecond(ms) { return Math.floor(ms / 1000) * 1000; }

/** Validated, explicit health options. There are NO defaults for the timing or origin: the operator must state them. */
function parseHealthOptions(health) {
    if (health === undefined || health === null) { return null; }
    const bad = (m) => { throw new Error(`e1_per_tx_recorder_v3: options.health ${m}`); };
    if (typeof health !== 'object') { bad('must be an object'); }
    for (const key of ['sourceId', 'collectorId']) {
        if (typeof health[key] !== 'string' || health[key].trim() === '') { bad(`.${key} must be a non-empty string`); }
    }
    if (!EVIDENCE_ORIGINS.includes(health.evidenceOrigin)) { bad(`.evidenceOrigin must be one of ${EVIDENCE_ORIGINS.join(', ')}`); }
    const hb = health.heartbeatIntervalMs;
    if (!Number.isInteger(hb) || hb < 1000 || hb % 1000 !== 0) { bad('.heartbeatIntervalMs must be a positive whole number of seconds (ms, >= 1000, multiple of 1000)'); }
    const cp = health.checkpointEveryHeartbeats === undefined ? 60 : health.checkpointEveryHeartbeats;
    if (!Number.isInteger(cp) || cp < 1) { bad('.checkpointEveryHeartbeats must be a positive integer'); }
    let sourceCount = null;
    if (health.sourceCount !== undefined && health.sourceCount !== null) {
        const sc = health.sourceCount;
        if (typeof sc.provider !== 'function') { bad('.sourceCount.provider must be a function (throughMs, startMs) -> cumulative arrivals in [startMs, throughMs)'); }
        if (typeof sc.basis !== 'string' || sc.basis.trim() === '') { bad('.sourceCount.basis must describe where the count comes from'); }
        if (sc.descriptor !== undefined && sc.descriptor !== null && (typeof sc.descriptor !== 'object' || Array.isArray(sc.descriptor))) {
            bad('.sourceCount.descriptor must be an object (the source_counter descriptor the Python contract classifies)');
        }
        // A0k: the descriptor says WHAT the counter is (kind, independence declaration, derived_from, time semantics, resolution). It is
        // copied unchanged into the start row; the recorder never judges it. Without one the count is recorded but certifies nothing.
        sourceCount = { provider: sc.provider, basis: sc.basis, independenceClaim: sc.independenceClaim === true,
            descriptor: sc.descriptor === undefined ? null : sc.descriptor };
    }
    if (health.autoTimer !== undefined && typeof health.autoTimer !== 'boolean') { bad('.autoTimer must be a boolean'); }
    return {
        sourceId: health.sourceId, collectorId: health.collectorId, evidenceOrigin: health.evidenceOrigin,
        heartbeatIntervalMs: hb, checkpointEveryHeartbeats: cp, sourceCount,
        autoTimer: health.autoTimer !== false,
        nowMs: typeof health.nowMs === 'function' ? health.nowMs : Date.now,
        monoMs: typeof health.monoMs === 'function' ? health.monoMs : null,
    };
}

class HealthRecorder extends v2.LiveStreamRecorder {
    constructor(options, workerIndex) {
        super(options, workerIndex);
        this.health = parseHealthOptions(options.health);
        this.initializedMs = this.health ? this.health.nowMs() : null;
        this._hReset();
    }

    _hReset() {
        this.hfd = null;
        this.hn = 0;
        this.hTimer = null;
        this.hFailed = false;
        this.hFirstError = null;
        this.hObsStartMs = null;
        this.hLastWmMs = null;
        this.hCommitted = 0;
        this.hPending = [];          // [ts, count] of submit events with ts >= the last watermark
        this.hAnomalies = 0;
        this.hLastErrCount = 0;
        this.hBeats = 0;
        this.hStopped = false;
        this.hOpenError = false;
        this.hAnomalyNotes = [];
        this.hInstance = null;
        this.hMonoBase = null;
    }

    _hNote(err) {
        if (!this.hFirstError) { this.hFirstError = String(err && err.stack || err); }
    }

    _hAnomaly(code) {
        this.hAnomalies += 1;
        if (this.hAnomalyNotes.length < 20) { this.hAnomalyNotes.push(code); }
    }

    _healthBase() { return `${this._base()}.health`; }

    _mono() {
        if (this.health.monoMs) { return this.health.monoMs(); }
        return Number(process.hrtime.bigint() - this.hMonoBase) / 1e6;
    }

    /** Append one spool row durably. Returns false (and stops all further health output) on any failure. */
    _hWrite(kind, fields, tMs) {
        if (this.hFailed || this.hfd === null) { return false; }
        try {
            const row = Object.assign({
                schema: SPOOL_SCHEMA, n: this.hn + 1, kind, t_utc: isoMs(tMs === undefined ? this.health.nowMs() : tMs), mono_ms: this._mono(),
                source_id: this.health.sourceId, collector_id: this.health.collectorId, collector_instance: this.hInstance,
                run_id: this.runId, worker: this.worker, round: this.round, label: this.label,
            }, fields);
            fs.writeSync(this.hfd, `${JSON.stringify(row)}\n`);
            fs.fsyncSync(this.hfd);
            this.hn += 1;
            return true;
        } catch (err) {
            this.hFailed = true;
            this._hNote(err);
            try {
                fs.writeFileSync(`${this._healthBase()}.FAILED`, `${new Date().toISOString()} ${err && err.stack || err}\n`, { flag: 'a' });
            } catch (inner) { /* nothing more can be done without throwing */ }
            return false;
        }
    }

    async activate(roundIndex, roundLabel) {
        await super.activate(roundIndex, roundLabel);
        this._hReset();
        if (!this.health) { return; }                     // health not configured: behaves as v2, emits no evidence
        try {
            this.hMonoBase = process.hrtime.bigint();
            this.hInstance = `${this.health.collectorId}:${this.runId}:w${this.worker}:r${this.round}`;
            const target = `${this._healthBase()}.jsonl`;
            this.hfd = fs.openSync(target, 'wx');
            const now = this.health.nowMs();
            this.hObsStartMs = floorSecond(now) + 1000;     // the next whole second: never earlier than the write
            this.hLastWmMs = this.hObsStartMs;
            const wrote = this._hWrite('start', {
                observation_start_utc: isoMs(this.hObsStartMs), timestamp_provenance: 'node_date_now_utc',
                coverage_scope: COVERAGE_SCOPE, evidence_origin: this.health.evidenceOrigin,
                recorder_version: RECORDER_V3_VERSION, options_sha256: this.optionsHash,
                // Lifecycle markers that precede the first whole observed second ride on the start row (a heartbeat cannot precede
                // the observation start). Callbacks are live from the moment v1.activate set active = true.
                lifecycle: { initialized_utc: isoMs(this.initializedMs), subscription_active_utc: isoMs(now),
                    window_open_utc: isoMs(this.hObsStartMs) },
                declared_policy: { heartbeat_interval_ms: this.health.heartbeatIntervalMs,
                    checkpoint_every_heartbeats: this.health.checkpointEveryHeartbeats },
                source_count_declared: this.health.sourceCount !== null,
                source_counter: this.health.sourceCount !== null ? this.health.sourceCount.descriptor : null,
            }, now);
            if (!wrote) { return; }
            if (this.health.autoTimer) {
                this.hTimer = setInterval(() => { this.tick(); }, this.health.heartbeatIntervalMs);
                if (this.hTimer && typeof this.hTimer.unref === 'function') { this.hTimer.unref(); }
            }
        } catch (err) {
            this.hFailed = true;
            this._hNote(err);
        }
    }

    txSubmitted(count) {
        const before = this.subs.count.length;
        super.txSubmitted(count);
        if (!this.health || this.hfd === null || !this.active) { return; }
        try {
            const recorded = this.subs.count.length === before + 1;
            if (!recorded) { this._hAnomaly('submit_event_not_recorded'); return; }
            if (!Number.isInteger(count) || count < 1) { this._hAnomaly('submit_count_not_positive_integer'); return; }
            const ts = this.subs.t_submit_evt_ms[before];
            if (ts < this.hObsStartMs) { return; }                      // before the observation window: not covered, not counted
            if (ts < this.hLastWmMs) { this._hAnomaly('wall_clock_regression'); return; }
            this.hPending.push([ts, count]);
        } catch (err) {
            this._hAnomaly('submit_accounting_failed');
            this._hNote(err);
        }
    }

    /** Emit one heartbeat for the current whole second. ``force`` writes even when the watermark did not advance. Never throws. */
    tick() {
        try {
            if (!this.health || this.hfd === null || this.hFailed || this.hStopped) { return false; }
            return this._hHeartbeat(null, false);
        } catch (err) {
            this._hNote(err);
            return false;
        }
    }

    _hHeartbeat(phase, force) {
        const now = this.health.nowMs();
        const wm = floorSecond(now);
        if (wm < this.hLastWmMs || (!force && wm === this.hLastWmMs)) { return false; }
        if (wm < this.hObsStartMs) { return false; }
        this.flushLive();                                                // every submit row with ts < wm must be in the file before it is certified
        let flushOk = true;
        if (this.live) {
            flushOk = !this.live.sub.failed && this.live.sub.rowsWritten === this.live.sub.rowsPushed;
            if (!flushOk) { this._hAnomaly('live_submit_stream_incomplete'); }
        }
        let effectiveWm = wm;
        if (flushOk) {
            const keep = [];
            let folded = 0;
            for (const [ts, count] of this.hPending) { if (ts < wm) { folded += count; } else { keep.push([ts, count]); } }
            this.hPending = keep;
            this.hCommitted += folded;
        } else {
            // The live submits file no longer holds every event, so nothing after the last good watermark can be counted or certified:
            // the watermark is frozen and the pending events are dropped (they are in memory only, never in the file).
            effectiveWm = this.hLastWmMs;
            this.hPending = [];
        }
        const errCount = this.counters.observer_errors + this.liveErrors + this.hAnomalies;
        const newErrors = errCount - this.hLastErrCount;
        this.hLastErrCount = errCount;
        if (!flushOk && !this.hOpenError) {
            this._hWrite('error', { error_code: 'live_submit_stream_failed', message: 'the live submits file is incomplete; arrivals can no longer be certified',
                affects_from_utc: isoMs(this.hLastWmMs) });
            this.hOpenError = true;                                       // never recovered in this round: the stream gave up
        }
        this.hBeats += 1;
        const cp = phase === null && this.hBeats % this.health.checkpointEveryHeartbeats === 0;
        const fields = {
            observed_through_utc: isoMs(effectiveWm), events_received: this.hCommitted, last_event_seq: null,
            health: newErrors > 0 || this.hOpenError ? 'degraded' : 'healthy', errors_since_last: newErrors > 0 ? newErrors : 0,
            phase: phase || (cp ? 'checkpoint' : 'heartbeat'),
        };
        if (fields.health === 'degraded' && newErrors <= 0) { fields.errors_since_last = 1; }
        if (cp || phase === 'window_close') {
            fields.progress = { submitted_total: this.counters.submitted, finished_total: this.counters.finished,
                pending_total: this.counters.submitted - this.counters.finished, observer_errors: this.counters.observer_errors,
                live_errors: this.liveErrors, health_anomalies: this.hAnomalies, anomaly_codes: this.hAnomalyNotes.slice() };
        }
        if (this.health.sourceCount && flushOk) {                   // an unverifiable heartbeat reports no source count
            try {
                const expected = this.health.sourceCount.provider(wm, this.hObsStartMs);
                if (Number.isInteger(expected) && expected >= 0) {
                    fields.source_events_expected = expected;
                    fields.source_basis = this.health.sourceCount.basis;
                    fields.source_independence_claim = this.health.sourceCount.independenceClaim;
                } else {
                    this._hAnomaly('source_count_invalid');
                }
            } catch (err) {
                this._hAnomaly('source_count_provider_failed');
            }
        }
        const ok = this._hWrite('heartbeat', fields);
        if (ok && flushOk) { this.hLastWmMs = wm; }
        return ok;
    }

    async deactivate() {
        if (this.hTimer) { clearInterval(this.hTimer); this.hTimer = null; }
        let finalWatermark = null;
        let windowOpened = false;
        const active = this.health && this.hfd !== null && !this.hFailed;
        try {
            if (active) {
                const wm = floorSecond(this.health.nowMs());
                windowOpened = wm >= this.hObsStartMs;            // a round that ends inside the first second never opened its window
                if (windowOpened) { this._hHeartbeat('window_close', true); }
                finalWatermark = this.hLastWmMs;
            }
        } catch (err) { this._hNote(err); }
        await super.deactivate();                                        // v2 closes the live streams and writes the final files (unchanged)
        try {
            if (!active || this.hFailed || !windowOpened) { return; }
            const base = this._base();
            const finalOk = fs.existsSync(`${base}.manifest.json`) && fs.existsSync(`${base}.csv`) && !fs.existsSync(`${base}.FAILED`);
            const liveOk = !!this.live && !this.live.sub.failed && !this.live.tx.failed && this.liveErrors === 0;
            const clean = finalOk && liveOk && !this.hOpenError && this.hAnomalies === 0 && this.counters.observer_errors === 0;
            this._hWrite('stop', { observed_through_utc: isoMs(finalWatermark), events_received: this.hCommitted, last_event_seq: null,
                reason: clean ? 'clean' : 'abort', phase: 'window_close',
                final_files_present: finalOk, live_stream_ok: liveOk, health_anomaly_codes: this.hAnomalyNotes.slice() });
            this.hStopped = true;
        } catch (err) {
            this._hNote(err);
        } finally {
            try { if (this.hfd !== null) { fs.closeSync(this.hfd); this.hfd = null; } } catch (err) { this._hNote(err); }
        }
    }
}

function createTxObserver(options, messenger, workerIndex) {
    if (!options || typeof options.runId !== 'string' || options.runId.length === 0) {
        throw new Error('e1_per_tx_recorder_v3: options.runId (non-empty string) is required');
    }
    if (typeof options.outputDir !== 'string' || !path.isAbsolute(options.outputDir)) {
        throw new Error('e1_per_tx_recorder_v3: options.outputDir must be an absolute path');
    }
    parseHealthOptions(options.health);                                   // refuse an invalid health block before anything is created
    fs.mkdirSync(options.outputDir, { recursive: true });
    const probe = path.join(options.outputDir, `.probe_${process.pid}_${workerIndex}_${Date.now()}`);
    fs.writeFileSync(probe, 'probe', { flag: 'wx' });
    fs.unlinkSync(probe);
    return new HealthRecorder(options, workerIndex);
}

module.exports.createTxObserver = createTxObserver;
module.exports.HealthRecorder = HealthRecorder;
module.exports.SPOOL_SCHEMA = SPOOL_SCHEMA;
module.exports.COVERAGE_SCOPE = COVERAGE_SCOPE;
module.exports.EVIDENCE_ORIGINS = EVIDENCE_ORIGINS;
module.exports.parseHealthOptions = parseHealthOptions;
