'use strict';
/*
 * E1/E0 per-transaction recorder: a Caliper transaction observer (monitors.transaction module).
 *
 * Status: written 2026-11-09; tested only with a fake dispatch and fake TxStatus objects
 * (tests/e1/js/e1_per_tx_recorder.test.js). Not yet loaded by a real Caliper worker: E0-0
 * must confirm the interface against the pinned Caliper (v0.6.0) before any E0/E1 run.
 * Spec: D:/PLAN_BLOCK18/e1_v02_proposal_2026-11-09/03_E1_instrumentation_correction_spec_PROPOSED_2026-11-09.md
 * (sections 1, 4, 6). Replaces the v0.1 draft docs/research/e1/proposed_configs/e1_per_tx_observer.js,
 * which is kept unchanged.
 *
 * Requirements implemented (spec section 4):
 *   R1  duck typing: activate / deactivate / txSubmitted / txFinished, no caliper-core import (fixes O-1, O-2, O-3, O-11)
 *   R2  construction fails loudly if outputDir is not absolute or not writable (O-8, O-10)
 *   R3  never throws after construction (O-8, O-9: a throw would skip the internal observer's cleanup)
 *   R4  one CSV + one submits CSV + one manifest per (worker, round); temp file then rename;
 *       never overwrites an existing file (O-6)
 *   R5  per tx: tx_id, seq_finish, create_ms, final_ms, status, verified, err, obs_hr_ns, worker, round, label (O-4, O-5, O-14)
 *   R6  per submit event: seq_submit, t_submit_evt_ms, count (O-7)
 *   R7  hot path = counter increments and array pushes only (O-12)
 *   R8  manifest: counts incl. pending_at_deactivate, clock anchors, event-loop delay, versions,
 *       config in force, options hash, host, file SHA-256 (O-13, O-15, O-16)
 *   R9  config in force read from the Caliper config singleton global.caliper.config.get(key)
 *       (method name to be confirmed in E0-0; recorded as UNAVAILABLE if absent)
 *   R10 txFinished accepts a single TxStatus or an array
 *
 * Benchmark config:
 *   monitors:
 *     transaction:
 *       - module: ./e1_per_tx_recorder.js        # resolved against the Caliper workspace
 *         options: { runId: <RUN_ID>, outputDir: <absolute path>, binding: 'fabric:2.5',
 *                    eventSemantics: 'gateway_peer_commit_status', caliperVersion: '0.6.0' }
 */
const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { monitorEventLoopDelay } = require('perf_hooks');

const RECORDER_VERSION = 'e1_per_tx_recorder 0.1.0 (2026-11-09)';
const TX_COLUMNS = ['tx_id', 'seq_finish', 'create_ms', 'final_ms', 'status', 'verified', 'err',
    'obs_hr_ns', 'worker', 'round', 'label'];
const CONFIG_KEYS = {
    event_strategy: 'caliper-fabric-gateway-eventstrategy',
    invoke_timeout_s: 'caliper-fabric-timeout-invokeorquery',
    workspace: 'caliper-workspace',
};

function csvField(value) {
    if (value === undefined || value === null) { return ''; }
    const s = String(value);
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

function call(obj, method) {
    try {
        return obj && typeof obj[method] === 'function' ? obj[method]() : undefined;
    } catch (err) {
        return undefined;
    }
}

function firstError(result) {
    const msgs = call(result, 'GetErrMsg');
    if (Array.isArray(msgs)) { return msgs.length ? String(msgs[0]) : ''; }
    return msgs ? String(msgs) : '';
}

function sanitise(text) {
    return String(text).replace(/[^A-Za-z0-9._-]/g, '_');
}

function writeAtomic(target, content) {
    if (fs.existsSync(target)) {
        throw new Error(`refusing to overwrite existing ${target}`);
    }
    const tmp = `${target}.tmp`;
    fs.writeFileSync(tmp, content, { flag: 'wx' });
    fs.renameSync(tmp, target);
}

function readConfigInForce() {
    const out = {};
    const cfg = global.caliper && global.caliper.config;
    for (const [field, key] of Object.entries(CONFIG_KEYS)) {
        try {
            out[field] = cfg && typeof cfg.get === 'function' ? cfg.get(key) : 'UNAVAILABLE';
        } catch (err) {
            out[field] = 'UNAVAILABLE';
        }
        if (out[field] === undefined) { out[field] = null; }
    }
    if (typeof out.invoke_timeout_s === 'string' && out.invoke_timeout_s !== 'UNAVAILABLE') {
        const n = Number(out.invoke_timeout_s);
        out.invoke_timeout_s = Number.isFinite(n) ? n : out.invoke_timeout_s;
    }
    return out;
}

function caliperVersion(options) {
    if (options.caliperVersion) { return { value: String(options.caliperVersion), source: 'options' }; }
    try {
        // Best effort only (R1: the recorder must not depend on this resolving).
        return { value: require('@hyperledger/caliper-core/package.json').version, source: 'caliper-core package.json' };
    } catch (err) {
        return { value: null, source: 'UNAVAILABLE' };
    }
}

class PerTxRecorder {
    constructor(options, workerIndex) {
        this.options = options;
        this.worker = workerIndex;
        this.runId = sanitise(options.runId);
        this.outputDir = options.outputDir;
        this.optionsHash = crypto.createHash('sha256')
            .update(JSON.stringify(options, Object.keys(options).sort())).digest('hex');
        this.active = false;
        this._reset(null, null);
    }

    _reset(roundIndex, roundLabel) {
        this.round = roundIndex;
        this.label = roundLabel;
        this.cols = { tx_id: [], create_ms: [], final_ms: [], status: [], verified: [], err: [], obs_hr_ns: [] };
        this.subs = { t_submit_evt_ms: [], count: [] };
        this.counters = { submitted: 0, finished: 0, no_id_records: 0, array_events: 0, observer_errors: 0 };
        this.firstObserverError = null;
        this.anchors = {};
        this.eld = null;
    }

    _note(err) {
        this.counters.observer_errors += 1;
        if (!this.firstObserverError) { this.firstObserverError = String(err && err.stack || err); }
    }

    async activate(roundIndex, roundLabel) {
        try {
            this._reset(roundIndex, roundLabel);
            this.anchors.wall0_ms = Date.now();
            this.anchors.hr0 = process.hrtime.bigint();
            try {
                this.eld = monitorEventLoopDelay({ resolution: 10 });
                this.eld.enable();
            } catch (err) {
                this.eld = null;
                this._note(err);
            }
            this.active = true;
        } catch (err) {
            this._note(err);
        }
    }

    txSubmitted(count) {
        try {
            this.counters.submitted += count;
            this.subs.t_submit_evt_ms.push(Date.now());
            this.subs.count.push(count);
        } catch (err) {
            this._note(err);
        }
    }

    txFinished(results) {
        try {
            const hr = process.hrtime.bigint();
            let list = results;
            if (Array.isArray(results)) {
                this.counters.array_events += 1;
            } else {
                list = [results];
            }
            const c = this.cols;
            for (const r of list) {
                const id = call(r, 'GetID');
                if (id === undefined || id === null || id === '') { this.counters.no_id_records += 1; }
                c.tx_id.push(id);
                c.create_ms.push(call(r, 'GetTimeCreate'));
                c.final_ms.push(call(r, 'GetTimeFinal'));
                c.status.push(call(r, 'GetStatus'));
                c.verified.push(call(r, 'IsVerified'));
                c.err.push(firstError(r));
                c.obs_hr_ns.push(hr);
            }
            this.counters.finished += list.length;
        } catch (err) {
            this._note(err);
        }
    }

    _base() {
        const label = sanitise(this.label === null || this.label === undefined ? 'nolabel' : this.label);
        return path.join(this.outputDir, `${this.runId}_w${this.worker}_r${this.round}_${label}`);
    }

    _eventLoop() {
        if (!this.eld) { return null; }
        const ms = (ns) => (Number.isFinite(ns) ? ns / 1e6 : null);
        try {
            return { p50_ms: ms(this.eld.percentile(50)), p99_ms: ms(this.eld.percentile(99)),
                max_ms: ms(this.eld.max), samples: this.eld.count !== undefined ? this.eld.count : null };
        } catch (err) {
            this._note(err);
            return null;
        }
    }

    async deactivate() {
        let base = null;
        try {
            this.anchors.wall1_ms = Date.now();
            this.anchors.hr1 = process.hrtime.bigint();
            if (this.eld) { this.eld.disable(); }
            this.active = false;
            base = this._base();
            const c = this.cols;
            const n = c.tx_id.length;
            const hr0 = this.anchors.hr0;

            const lines = [TX_COLUMNS.join(',')];
            const seen = new Map();
            let success = 0;
            let failed = 0;
            for (let i = 0; i < n; i++) {
                const id = c.tx_id[i];
                if (id) { seen.set(id, (seen.get(id) || 0) + 1); }
                if (c.status[i] === 'success') { success += 1; } else if (c.status[i] === 'failed') { failed += 1; }
                lines.push([id, i, c.create_ms[i], c.final_ms[i], c.status[i], c.verified[i], c.err[i],
                    (c.obs_hr_ns[i] - hr0).toString(), this.worker, this.round, this.label].map(csvField).join(','));
            }
            const duplicates = [...seen.values()].filter((v) => v > 1).length;
            const txCsv = `${lines.join('\n')}\n`;
            const subLines = ['seq_submit,t_submit_evt_ms,count'];
            for (let i = 0; i < this.subs.count.length; i++) {
                subLines.push(`${i},${this.subs.t_submit_evt_ms[i]},${this.subs.count[i]}`);
            }
            const subCsv = `${subLines.join('\n')}\n`;
            const monoMs = Number(this.anchors.hr1 - hr0) / 1e6;
            const wallMs = this.anchors.wall1_ms - this.anchors.wall0_ms;
            const cfg = readConfigInForce();
            const manifest = {
                recorder: RECORDER_VERSION,
                run_id: this.runId, worker: this.worker, round: this.round, label: this.label,
                counts: {
                    submitted: this.counters.submitted, finished: this.counters.finished,
                    pending_at_deactivate: this.counters.submitted - this.counters.finished,
                    rows: n, success, failed, other_status: n - success - failed,
                    no_id_records: this.counters.no_id_records, array_events: this.counters.array_events,
                    duplicate_ids: duplicates, submit_events: this.subs.count.length,
                    observer_errors: this.counters.observer_errors,
                },
                first_observer_error: this.firstObserverError,
                clock: { wall0_ms: this.anchors.wall0_ms, wall1_ms: this.anchors.wall1_ms,
                    monotonic_elapsed_ms: monoMs, wall_elapsed_ms: wallMs, wall_minus_monotonic_ms: wallMs - monoMs },
                event_loop_delay: this._eventLoop(),
                config_in_force: {
                    event_strategy: cfg.event_strategy, invoke_timeout_s: cfg.invoke_timeout_s,
                    workspace: cfg.workspace, binding: this.options.binding || null,
                    event_semantics: this.options.eventSemantics || null,
                    source: 'global.caliper.config.get (E0-0 to confirm); binding and event_semantics as declared in options',
                },
                versions: { node: process.version, caliper: caliperVersion(this.options) },
                host: os.hostname(),
                options_sha256: this.optionsHash,
                files: {
                    csv: path.basename(`${base}.csv`),
                    csv_sha256: crypto.createHash('sha256').update(txCsv).digest('hex'),
                    submits_csv: path.basename(`${base}.submits.csv`),
                    submits_sha256: crypto.createHash('sha256').update(subCsv).digest('hex'),
                },
            };
            writeAtomic(`${base}.csv`, txCsv);
            writeAtomic(`${base}.submits.csv`, subCsv);
            writeAtomic(`${base}.manifest.json`, `${JSON.stringify(manifest, null, 2)}\n`);
        } catch (err) {
            const marker = `${base || path.join(this.outputDir || '.', 'unknown_round')}.FAILED`;
            try {
                fs.writeFileSync(marker, `${new Date().toISOString()} ${err && err.stack || err}\n`, { flag: 'a' });
            } catch (inner) {
                // Nothing more can be done without throwing (R3).
            }
            try {
                console.error(`E1_OBSERVER_WRITE_FAILED ${base} ${err && err.message || err}`);
            } catch (inner) {
                // ignore
            }
        }
    }
}

function createTxObserver(options, messenger, workerIndex) {
    if (!options || typeof options.runId !== 'string' || options.runId.length === 0) {
        throw new Error('e1_per_tx_recorder: options.runId (non-empty string) is required');
    }
    if (typeof options.outputDir !== 'string' || !path.isAbsolute(options.outputDir)) {
        throw new Error('e1_per_tx_recorder: options.outputDir must be an absolute path');
    }
    fs.mkdirSync(options.outputDir, { recursive: true });
    const probe = path.join(options.outputDir, `.probe_${process.pid}_${workerIndex}_${Date.now()}`);
    fs.writeFileSync(probe, 'probe', { flag: 'wx' });
    fs.unlinkSync(probe);
    return new PerTxRecorder(options, workerIndex);
}

module.exports.createTxObserver = createTxObserver;
module.exports.PerTxRecorder = PerTxRecorder;
module.exports.TX_COLUMNS = TX_COLUMNS;
