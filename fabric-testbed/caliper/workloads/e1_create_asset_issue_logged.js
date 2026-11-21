'use strict';

// Versioned successor of e1_create_asset.js with OPT-IN generator-side issuance logging (task A0l, 2026-11-21).
//
// e1_create_asset.js is left byte-identical. This module extends its class and changes nothing unless the round arguments
// carry an `issueLog` object. Without it the behaviour is exactly the legacy workload (same requests, same keys, same settle
// sleep, same one-worker guard).
//
// With `issueLog` the sutAdapter is wrapped so each sendRequests call writes an intent and an attempt record BEFORE the call and
// an outcome record after it (see e1_issue_log.js for what each record does and does not prove). A periodic tick record proves
// the writer was alive. The workload never reads the log, never talks to the recorder, and never changes the request it sends.
//
// issueLog = { path (absolute, new file), workerId ('w<workerIndex>'), expectedWorkers (['w<workerIndex>']), clockDomain,
//              tickIntervalMs (default 1000), fsyncPolicy ('tick' | 'every' | 'none', default 'tick') }
//
// One worker only (inherited guard); the asset keys carry no worker index, so two workers would collide.

const legacy = require('./e1_create_asset.js');
const core = require('./e1_workload_core.js');
const issue = require('./e1_issue_log.js');

const WORKLOAD_ID = 'e1_create_asset_issue_logged/1';
const DEFAULT_TICK_MS = 1000;

function issueLogConfig(roundArguments) {
    const cfg = roundArguments && roundArguments.issueLog;
    return cfg === undefined || cfg === null ? null : cfg;
}

function validateIssueLog(cfg, workerIndex, totalWorkers) {
    if (typeof cfg !== 'object' || Array.isArray(cfg)) { throw new issue.IssueLogError('config_invalid', 'issueLog must be an object'); }
    if (totalWorkers !== 1) {
        throw new issue.IssueLogError('worker_count_unsupported', `exactly 1 worker is supported, got ${totalWorkers}`);
    }
    if (typeof cfg.workerId !== 'string' || cfg.workerId === '') {
        throw new issue.IssueLogError('worker_identity_missing', 'issueLog.workerId is required (no worker identity is assumed)');
    }
    if (cfg.workerId !== `w${workerIndex}`) {
        throw new issue.IssueLogError('worker_identity_mismatch', `issueLog.workerId ${cfg.workerId} is not w${workerIndex}`);
    }
    if (!Array.isArray(cfg.expectedWorkers) || cfg.expectedWorkers.length !== 1 || cfg.expectedWorkers[0] !== cfg.workerId) {
        throw new issue.IssueLogError('worker_roster_mismatch', 'issueLog.expectedWorkers must be exactly [workerId]');
    }
    return cfg;
}

function build(Base, sleep, deps) {
    const d = deps || {};
    const Legacy = legacy.build(Base, sleep);
    return class E1CreateAssetIssueLogged extends Legacy {
        constructor() {
            super();
            this.issueWriter = null;
            this.issueTimer = null;
        }

        async initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext) {
            this._stopIssueLog('reinitialized');
            const cfg = issueLogConfig(roundArguments);
            if (cfg === null) {
                // legacy path: nothing about this call differs from e1_create_asset.js
                return super.initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext);
            }
            validateIssueLog(cfg, workerIndex, totalWorkers);               // fail before the (possibly long) settle sleep
            await super.initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext);
            const args = this.roundArguments;
            const writer = new issue.IssueLogWriter({
                path: cfg.path, runId: args.runId, workerIndex, workerId: cfg.workerId, totalWorkers, expectedWorkers: cfg.expectedWorkers,
                clockDomain: cfg.clockDomain, roundIndex, tickIntervalMs: cfg.tickIntervalMs === undefined ? DEFAULT_TICK_MS : cfg.tickIntervalMs,
                fsyncPolicy: cfg.fsyncPolicy, workloadId: WORKLOAD_ID,
                config: { mode: args.mode, value_bytes: args.valueBytes, fail_every: args.failEvery === undefined ? null : args.failEvery },
                fsImpl: d.fsImpl, clock: d.clock, mono: d.mono,
            });
            writer.open();                                                   // refuses an existing file
            this.issueWriter = writer;
            this.sutAdapter = issue.wrapSutAdapter(this.sutAdapter, writer, () => this.seq - 1,
                (txSeq) => core.keyFor(args.runId, this.roundIndex, txSeq));
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

module.exports = { createWorkloadModule, build, validateIssueLog, WORKLOAD_ID };
