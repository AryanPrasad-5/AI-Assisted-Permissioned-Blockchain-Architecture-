'use strict';

// Generator-side issuance log for the E1 pilot workload (task A0l, 2026-11-21).
//
// What it records: one hash-chained JSON line per event, written by the WORKLOAD GENERATOR path, never by the
// recorder's callbacks. The log is opt-in and is not read by Caliper, the recorder, Fabric or anything else here.
//
//   start    the writer opened the log (identity, roster, clock domain, declared tick interval)
//   intent   the generator decided to issue transaction n (written BEFORE sendRequests; durable once the write returned)
//   attempt  written immediately before sendRequests is called; "intent without attempt" means the call was NOT made
//   outcome  sendRequests returned or rejected, as seen by the generator (not a ledger commit, not a recorder event)
//   tick     liveness watermark with the cumulative counts; a quiet second is only believable if ticks show the writer was alive
//   stop     clean close with the final counts; its absence means an interrupted generator
//
// What it does not prove: that Caliper emitted txSubmitted, that Fabric accepted anything, that anything committed, or that an
// external (Ethereum mempool) source was fully captured. The record "attempt" is written before the call and is therefore not proof
// that the call ran; only a later "outcome" shows the call completed from the generator's side. The clock is the worker's own
// Date.now(); no other clock domain is assumed.
//
// Failure policy (fail closed): when a required record cannot be written, the transaction is NOT sent, the writer is marked failed
// and every later issuance is refused. After a send, a failed outcome write never changes the send's result or error; it marks the
// writer failed so no further transaction is issued unrecorded.

const fs = require('fs');
const crypto = require('crypto');
const path = require('path');

const ISSUE_LOG_SCHEMA = 'e1_issue_log/1';
const WRITER_VERSION = 'e1_issue_log_writer 0.1 (2026-11-21)';
const GENESIS_SHA256 = '0'.repeat(64);
const KINDS = ['start', 'intent', 'attempt', 'outcome', 'tick', 'stop'];
const FSYNC_POLICIES = ['tick', 'every', 'none'];
const ID_RE = /^[A-Za-z0-9_.-]{1,40}$/;
const WORKER_RE = /^w[0-9]{1,4}$/;
const TEXT_RE = /^[\x20-\x7e]*$/;

class IssueLogError extends Error {
    constructor(code, message) {
        super(`e1 issue log: ${code}: ${message}`);
        this.code = code;
    }
}

// Same canonical form as the Python side (json.dumps sort_keys, compact separators, ensure_ascii). Only safe integers, printable
// ASCII strings, booleans, null, arrays and plain objects are allowed, so the two encoders cannot disagree.
function canonical(value) {
    if (value === null) { return 'null'; }
    if (typeof value === 'boolean') { return value ? 'true' : 'false'; }
    if (typeof value === 'number') {
        if (!Number.isSafeInteger(value) || Object.is(value, -0)) { throw new IssueLogError('value_not_integer', `${value}`); }
        return String(value);
    }
    if (typeof value === 'string') {
        if (!TEXT_RE.test(value)) { throw new IssueLogError('text_not_ascii', 'issue-log text must be printable ASCII'); }
        return JSON.stringify(value);
    }
    if (Array.isArray(value)) { return `[${value.map(canonical).join(',')}]`; }
    if (typeof value === 'object') {
        const keys = Object.keys(value).sort();
        return `{${keys.map((k) => {
            if (value[k] === undefined) { throw new IssueLogError('value_undefined', k); }
            return `${JSON.stringify(k)}:${canonical(value[k])}`;
        }).join(',')}}`;
    }
    throw new IssueLogError('value_unsupported', typeof value);
}

function digestOf(body, previous) {
    const plain = {};
    for (const key of Object.keys(body)) { if (key !== 'prev' && key !== 'sha256') { plain[key] = body[key]; } }
    plain.prev = previous;
    return crypto.createHash('sha256').update(canonical(plain), 'utf8').digest('hex');
}

function text(value, limit) {
    if (value === undefined || value === null) { return null; }
    const clean = String(value).replace(/[^\x20-\x7e]/g, '?');
    return clean.length > limit ? clean.slice(0, limit) : clean;
}

function defaultMono() { return Number(process.hrtime.bigint() / 1000000n); }

class IssueLogWriter {
    constructor(options) {
        const o = options || {};
        if (typeof o.path !== 'string' || !path.isAbsolute(o.path)) { throw new IssueLogError('path_invalid', 'path must be absolute'); }
        if (typeof o.runId !== 'string' || !ID_RE.test(o.runId)) { throw new IssueLogError('run_id_invalid', `${o.runId}`); }
        if (!Number.isInteger(o.workerIndex) || o.workerIndex < 0) { throw new IssueLogError('worker_identity_invalid', 'workerIndex'); }
        if (typeof o.workerId !== 'string' || !WORKER_RE.test(o.workerId) || o.workerId !== `w${o.workerIndex}`) {
            throw new IssueLogError('worker_identity_mismatch', `workerId ${o.workerId} is not w${o.workerIndex}`);
        }
        if (o.totalWorkers !== 1) { throw new IssueLogError('worker_count_unsupported', `exactly 1 worker is supported, got ${o.totalWorkers}`); }
        if (!Array.isArray(o.expectedWorkers) || o.expectedWorkers.length !== 1 || o.expectedWorkers[0] !== o.workerId) {
            throw new IssueLogError('worker_roster_mismatch', 'expectedWorkers must be exactly [workerId]');
        }
        if (typeof o.clockDomain !== 'string' || !o.clockDomain || o.clockDomain.length > 80 || !TEXT_RE.test(o.clockDomain)) {
            throw new IssueLogError('clock_domain_invalid', 'a declared clock domain label is required');
        }
        if (!Number.isInteger(o.roundIndex) || o.roundIndex < 0) { throw new IssueLogError('round_invalid', `${o.roundIndex}`); }
        if (!Number.isInteger(o.tickIntervalMs) || o.tickIntervalMs < 10 || o.tickIntervalMs > 600000) {
            throw new IssueLogError('tick_interval_invalid', 'tickIntervalMs must be an integer in 10..600000');
        }
        const policy = o.fsyncPolicy === undefined ? 'tick' : o.fsyncPolicy;
        if (!FSYNC_POLICIES.includes(policy)) { throw new IssueLogError('fsync_policy_invalid', `${policy}`); }
        this.path = o.path;
        this.runId = o.runId;
        this.workerIndex = o.workerIndex;
        this.workerId = o.workerId;
        this.totalWorkers = o.totalWorkers;
        this.expectedWorkers = o.expectedWorkers.slice();
        this.clockDomain = o.clockDomain;
        this.roundIndex = o.roundIndex;
        this.tickIntervalMs = o.tickIntervalMs;
        this.fsyncPolicy = policy;
        this.workloadId = text(o.workloadId || 'unspecified', 80);
        this.config = o.config || {};
        this.fs = o.fsImpl || fs;
        this.clock = o.clock || (() => Date.now());
        this.mono = o.mono || defaultMono;
        this.fd = null;
        this.rseq = 0;
        this.prev = GENESIS_SHA256;
        this.nextN = 0;
        this.lastTxSeq = -1;
        this.pendingIntent = null;
        this.inFlight = new Set();
        this.counts = { intents: 0, attempts: 0, outcomes: 0, ticks: 0 };
        this.failed = false;
        this.failure = null;
        this.closed = false;
    }

    open() {
        if (this.fd !== null) { throw new IssueLogError('already_open', this.path); }
        this.fs.mkdirSync(path.dirname(this.path), { recursive: true });
        if (this.fs.existsSync(this.path)) { throw new IssueLogError('log_exists', `refusing to overwrite ${this.path}`); }
        try {
            this.fd = this.fs.openSync(this.path, 'wx');
        } catch (err) {
            this._fail(`open: ${err.message}`);
            throw new IssueLogError('write_failed', `cannot create the log: ${err.message}`);
        }
        this._write('start', {
            writer_version: WRITER_VERSION, workload_id: this.workloadId, worker_index: this.workerIndex,
            total_workers: this.totalWorkers, worker_roster: this.expectedWorkers, round_index: this.roundIndex,
            clock_domain: this.clockDomain, wall_clock_source: 'Date.now()', timestamp_precision: 'ms',
            tick_interval_ms: this.tickIntervalMs, fsync_policy: this.fsyncPolicy, config: this.config,
            event_time_semantics: 'generator_issue_time',
        });
        return this;
    }

    _fail(reason) {
        this.failed = true;
        this.failure = this.failure || String(reason).slice(0, 200);
    }

    _usable(what) {
        if (this.failed) { throw new IssueLogError('writer_failed', `${what} refused: ${this.failure}`); }
        if (this.closed) { throw new IssueLogError('writer_closed', `${what} refused: the log is closed`); }
        if (this.fd === null) { throw new IssueLogError('writer_not_open', `${what} refused: the log is not open`); }
    }

    _write(kind, fields) {
        try {
            const t = this.clock();
            if (!Number.isSafeInteger(t) || t <= 0) { throw new IssueLogError('clock_invalid', `${t}`); }
            const body = {
                schema: ISSUE_LOG_SCHEMA, rseq: this.rseq + 1, kind, run_id: this.runId, worker_id: this.workerId, t_ms: t,
                mono_ms: this.mono(), ...fields,
            };
            body.prev = this.prev;
            body.sha256 = digestOf(body, this.prev);
            const buffer = Buffer.from(`${canonical(body)}\n`, 'utf8');
            let written = 0;
            while (written < buffer.length) {
                const n = this.fs.writeSync(this.fd, buffer, written, buffer.length - written);
                if (!(n > 0)) { throw new IssueLogError('short_write', 'writeSync wrote nothing'); }
                written += n;
            }
            if (this.fsyncPolicy === 'every') { this.fs.fsyncSync(this.fd); }
            this.rseq += 1;
            this.prev = body.sha256;
            return body;
        } catch (err) {
            this._fail(`${kind}: ${err.message}`);
            throw err instanceof IssueLogError && err.code === 'write_failed' ? err
                : new IssueLogError('write_failed', `${kind}: ${err.message}`);
        }
    }

    // Written BEFORE sendRequests. Refuses a repeated txSeq (a retry or double send): the call must not be made.
    intent(info) {
        this._usable('intent');
        const txSeq = info.txSeq;
        if (!Number.isInteger(txSeq) || txSeq < 0) { this._fail('intent: txSeq invalid'); throw new IssueLogError('tx_seq_invalid', `${txSeq}`); }
        if (txSeq <= this.lastTxSeq) {
            this._fail(`duplicate issuance of txSeq ${txSeq}`);
            throw new IssueLogError('duplicate_issuance', `txSeq ${txSeq} was already issued (last ${this.lastTxSeq}); retries are not logged as new issuances`);
        }
        if (this.pendingIntent !== null) {
            this._fail('intent while an earlier intent has no attempt');
            throw new IssueLogError('intent_ordering', `intent ${this.pendingIntent} has no attempt record`);
        }
        const n = this.nextN;
        this._write('intent', {
            n, tx_seq: txSeq, issue_id: `${this.runId}:${this.roundIndex}:${n}`, op: text(info.op, 40), asset_key: text(info.assetKey, 128),
        });
        this.nextN += 1;
        this.lastTxSeq = txSeq;
        this.pendingIntent = n;
        this.counts.intents += 1;
        return n;
    }

    // Written immediately before the sendRequests call: its absence after an intent means the call was not made.
    attempt(n) {
        this._usable('attempt');
        if (this.pendingIntent !== n) { this._fail(`attempt ${n} without its intent`); throw new IssueLogError('attempt_ordering', `${n}`); }
        this._write('attempt', { n, issue_id: `${this.runId}:${this.roundIndex}:${n}` });
        this.pendingIntent = null;
        this.inFlight.add(n);
        this.counts.attempts += 1;
    }

    // sendRequests completed from the generator's side. Not a ledger commit, not a recorder event.
    outcome(n, result, details) {
        this._usable('outcome');
        if (!this.inFlight.has(n)) { this._fail(`outcome ${n} without an open attempt`); throw new IssueLogError('outcome_ordering', `${n}`); }
        const d = details || {};
        this._write('outcome', {
            n, issue_id: `${this.runId}:${this.roundIndex}:${n}`, result, error: text(d.error, 200),
            client_tx_id: text(d.clientTxId, 128), client_status: text(d.clientStatus, 40),
        });
        this.inFlight.delete(n);
        this.counts.outcomes += 1;
    }

    // A failed outcome write must not change what the caller sees; the writer is already marked failed.
    outcomeBestEffort(n, result, details) {
        try { this.outcome(n, result, details); } catch (err) { /* the writer is failed; later issuance is refused */ }
    }

    tick() {
        if (this.failed || this.closed || this.fd === null) { return false; }
        try {
            this._write('tick', { attempted: this.counts.attempts, intents: this.counts.intents, outcomes: this.counts.outcomes });
            this.counts.ticks += 1;
            if (this.fsyncPolicy !== 'none') { this.fs.fsyncSync(this.fd); }
            return true;
        } catch (err) {
            this._fail(`tick: ${err.message}`);
            return false;
        }
    }

    close(reason) {
        if (this.fd === null || this.closed) { return false; }
        let ok = false;
        if (!this.failed) {
            try {
                this._write('stop', {
                    reason: reason || 'cleanup', intents: this.counts.intents, attempted: this.counts.attempts,
                    outcomes: this.counts.outcomes, in_flight: this.inFlight.size,
                });
                if (this.fsyncPolicy !== 'none') { this.fs.fsyncSync(this.fd); }
                ok = true;
            } catch (err) { this._fail(`stop: ${err.message}`); }
        }
        try { this.fs.closeSync(this.fd); } catch (err) { this._fail(`close: ${err.message}`); }
        this.closed = true;
        return ok;
    }

    status() {
        return {
            failed: this.failed, failure: this.failure, closed: this.closed, rseq: this.rseq, intents: this.counts.intents,
            attempts: this.counts.attempts, outcomes: this.counts.outcomes, in_flight: this.inFlight.size, ticks: this.counts.ticks,
            pending_intent: this.pendingIntent, path: this.path,
        };
    }
}

function clientTxId(result) {
    try {
        if (result && typeof result.GetID === 'function') { return result.GetID(); }
        if (result && typeof result.id === 'string') { return result.id; }
    } catch (err) { return null; }
    return null;
}

function clientStatus(result) {
    try {
        if (result && typeof result.GetStatus === 'function') { return result.GetStatus(); }
        if (result && typeof result.status === 'string') { return result.status; }
    } catch (err) { return null; }
    return null;
}

// Wrap a Caliper sutAdapter so every sendRequests call is logged before and after. Everything else is forwarded unchanged.
// nextTxSeq(request) returns the workload's transaction sequence for the request about to be sent. The wrapper body runs
// synchronously up to the call, so intent and attempt are written with no await in between.
function wrapSutAdapter(sut, writer, nextTxSeq, expectedKeyOf) {
    return new Proxy(sut, {
        get(target, prop) {
            if (prop === 'sendRequests') {
                return async function sendRequests(request, ...rest) {
                    const txSeq = nextTxSeq(request);
                    const op = request && request.contractFunction;
                    const assetKey = op === 'CreateAsset' && request.contractArguments ? request.contractArguments[0] : null;
                    if (expectedKeyOf && assetKey !== null && expectedKeyOf(txSeq) !== assetKey) {
                        writer._fail(`asset key does not match txSeq ${txSeq}`);
                        throw new IssueLogError('tx_seq_mismatch', `request key ${assetKey} does not belong to txSeq ${txSeq}`);
                    }
                    const n = writer.intent({ txSeq, op, assetKey });
                    writer.attempt(n);
                    let result;
                    try {
                        result = await target.sendRequests(request, ...rest);
                    } catch (err) {
                        writer.outcomeBestEffort(n, 'rejected', { error: err && err.message ? err.message : String(err) });
                        throw err;
                    }
                    writer.outcomeBestEffort(n, 'returned', { clientTxId: clientTxId(result), clientStatus: clientStatus(result) });
                    return result;
                };
            }
            const value = Reflect.get(target, prop, target);
            return typeof value === 'function' ? value.bind(target) : value;
        },
    });
}

module.exports = {
    ISSUE_LOG_SCHEMA, WRITER_VERSION, GENESIS_SHA256, KINDS, FSYNC_POLICIES, IssueLogError, IssueLogWriter, canonical, digestOf,
    wrapSutAdapter,
};
