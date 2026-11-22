'use strict';

// Versioned successor of e1_create_asset_issue_logged.js that ALSO records write-completion and sync evidence (task A0p, 2026-11-22).
//
// e1_create_asset.js and e1_create_asset_issue_logged.js are left byte-identical. This module is selected explicitly: it reads the round argument
// `issueLogTimed` (a different key from the A0l `issueLog`), so a configuration written for the A0l workload is never silently upgraded and one written
// for this module is never silently downgraded. A round that carries BOTH keys, or only `issueLog`, is refused here.
//
// issueLogTimed = { path (absolute, new file), timingPath (absolute, new file, different from path), workerId ('w<workerIndex>'),
//                   expectedWorkers (['w<workerIndex>']), clockDomain, tickIntervalMs (default 1000), fsyncPolicy ('tick' | 'every' | 'none', default 'tick') }
//
// Requests, keys, the settle sleep and the one-worker guard are those of the legacy workload. The log it writes is schema e1_issue_log/1; the timing
// evidence goes to the sidecar (see e1_issue_log_timed.js for what each sidecar row does and does not prove).

const legacy = require('./e1_create_asset.js');
const core = require('./e1_workload_core.js');
const timed = require('./e1_issue_log_timed.js');

const WORKLOAD_ID = 'e1_create_asset_issue_logged_timed/1';
const DEFAULT_TICK_MS = 1000;
const { IssueLogError } = timed;

function timedConfig(roundArguments) {
    if (roundArguments && roundArguments.issueLog !== undefined && roundArguments.issueLog !== null) {
        throw new IssueLogError('mixed_issue_log_schema', 'the timed workload reads issueLogTimed; an issueLog (A0l) argument is a different writer version and is refused, not converted');
    }
    const cfg = roundArguments && roundArguments.issueLogTimed;
    if (cfg === undefined || cfg === null) {
        throw new IssueLogError('issue_log_timed_missing', 'the timed workload requires round argument issueLogTimed; it has no legacy path (select e1_create_asset.js for that)');
    }
    return cfg;
}

function validateTimed(cfg, workerIndex, totalWorkers) {
    if (typeof cfg !== 'object' || Array.isArray(cfg)) { throw new IssueLogError('config_invalid', 'issueLogTimed must be an object'); }
    if (totalWorkers !== 1) { throw new IssueLogError('worker_count_unsupported', `exactly 1 worker is supported, got ${totalWorkers}`); }
    if (typeof cfg.workerId !== 'string' || cfg.workerId === '') { throw new IssueLogError('worker_identity_missing', 'issueLogTimed.workerId is required'); }
    if (cfg.workerId !== `w${workerIndex}`) { throw new IssueLogError('worker_identity_mismatch', `issueLogTimed.workerId ${cfg.workerId} is not w${workerIndex}`); }
    if (!Array.isArray(cfg.expectedWorkers) || cfg.expectedWorkers.length !== 1 || cfg.expectedWorkers[0] !== cfg.workerId) {
        throw new IssueLogError('worker_roster_mismatch', 'issueLogTimed.expectedWorkers must be exactly [workerId]');
    }
    if (typeof cfg.timingPath !== 'string' || cfg.timingPath === '') { throw new IssueLogError('timing_path_missing', 'issueLogTimed.timingPath is required'); }
    return cfg;
}

function build(Base, sleep, deps) {
    const d = deps || {};
    const Legacy = legacy.build(Base, sleep);
    return class E1CreateAssetIssueLoggedTimed extends Legacy {
        constructor() {
            super();
            this.issueWriter = null;
            this.issueTimer = null;
        }

        async initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext) {
            this._stopIssueLog('reinitialized');
            const cfg = timedConfig(roundArguments);
            validateTimed(cfg, workerIndex, totalWorkers);                   // fail before the (possibly long) settle sleep
            await super.initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext);
            const args = this.roundArguments;
            const writer = new timed.TimedIssueLogWriter({
                path: cfg.path, timingPath: cfg.timingPath, runId: args.runId, workerIndex, workerId: cfg.workerId, totalWorkers, expectedWorkers: cfg.expectedWorkers,
                clockDomain: cfg.clockDomain, roundIndex, tickIntervalMs: cfg.tickIntervalMs === undefined ? DEFAULT_TICK_MS : cfg.tickIntervalMs,
                fsyncPolicy: cfg.fsyncPolicy, workloadId: WORKLOAD_ID,
                config: { mode: args.mode, value_bytes: args.valueBytes, fail_every: args.failEvery === undefined ? null : args.failEvery },
                fsImpl: d.fsImpl, clock: d.clock, mono: d.mono, timingClock: d.timingClock, monoUs: d.monoUs,
            });
            writer.open();                                                   // refuses an existing log or sidecar
            this.issueWriter = writer;
            this.sutAdapter = timed.wrapSutAdapter(this.sutAdapter, writer, () => this.seq - 1, (txSeq) => core.keyFor(args.runId, this.roundIndex, txSeq));
            const setTimer = d.setInterval || setInterval;
            this.issueTimer = setTimer(() => { writer.tick(); }, writer.tickIntervalMs);
            if (this.issueTimer && typeof this.issueTimer.unref === 'function') { this.issueTimer.unref(); }
            this._clearTimer = d.clearInterval || clearInterval;
            return undefined;
        }

        _stopIssueLog(reason) {
            if (this.issueTimer !== null) { this._clearTimer(this.issueTimer); this.issueTimer = null; }
            if (this.issueWriter !== null) { this.issueWriter.close(reason); }
        }

        issueLogStatus() { return this.issueWriter === null ? null : this.issueWriter.status(); }

        async cleanupWorkloadModule() {
            this._stopIssueLog('cleanup');
            if (typeof super.cleanupWorkloadModule === 'function') { await super.cleanupWorkloadModule(); }
        }
    };
}

function createWorkloadModule() {
    // lazy: the Caliper base class is only resolvable inside a Caliper worker
    const Base = (() => {
        try { return require('@hyperledger/caliper-core').WorkloadModuleBase; } catch (err) {
            return require.main.require('@hyperledger/caliper-core').WorkloadModuleBase;
        }
    })();
    return new (build(Base))();
}

module.exports = { createWorkloadModule, build, validateTimed, timedConfig, WORKLOAD_ID };
