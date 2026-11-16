'use strict';

// Caliper workload adapter for a bounded, deterministic Flashbots-bin replay.
// The source is an arrival-count series, not Ethereum transaction payloads.
// One worker is required so the schedule and dispatch log have one clock/order.

const fs = require('fs');
const path = require('path');
const core = require('./e1_workload_core.js');

function fail(message) { throw new Error(`e1 trace replay: ${message}`); }

function validateSchedule(schedule) {
    if (!Array.isArray(schedule) || schedule.length < 1) { fail('schedule must be a non-empty array'); }
    let last = -1;
    for (let i = 0; i < schedule.length; i += 1) {
        const value = Number(schedule[i]);
        if (!Number.isFinite(value) || value < 0 || value < last) {
            fail(`schedule[${i}] must be finite, non-negative and chronological`);
        }
        schedule[i] = value;
        last = value;
    }
    return schedule;
}

function validateArgs(a) {
    core.validateArgs(a);
    if (!Array.isArray(a.scheduleMs)) { fail('scheduleMs is required'); }
    validateSchedule(a.scheduleMs);
    if (typeof a.dispatchLog !== 'string' || !path.isAbsolute(a.dispatchLog)) {
        fail('dispatchLog must be an absolute path');
    }
    return a;
}

function loadBase() {
    try {
        return require('@hyperledger/caliper-core').WorkloadModuleBase;
    } catch (err) {
        return require.main.require('@hyperledger/caliper-core').WorkloadModuleBase;
    }
}

function build(Base, clock = () => Date.now(), sleeper = (ms) => new Promise((resolve) => setTimeout(resolve, ms)), fsImpl = fs) {
    return class E1TraceReplay extends Base {
        constructor() {
            super();
            this.seq = 0;
            this.schedule = [];
            this.startedMs = null;
            this.dispatchLog = null;
            this.overflowLog = null;
        }

        async initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext) {
            await super.initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter,
                sutContext);
            validateArgs(roundArguments);
            if (totalWorkers !== 1) { fail(`exactly 1 worker required, got ${totalWorkers}`); }
            this.seq = 0;
            this.roundArguments = roundArguments;
            this.roundIndex = roundIndex;
            this.sutAdapter = sutAdapter;
            this.schedule = roundArguments.scheduleMs.slice();
            this.startedMs = clock();
            this.dispatchLog = roundArguments.dispatchLog;
            this.overflowLog = `${this.dispatchLog}.overflow.csv`;
            const parent = path.dirname(this.dispatchLog);
            fsImpl.mkdirSync(parent, { recursive: true });
            if (fsImpl.existsSync(this.dispatchLog)) { fail(`refusing to overwrite ${this.dispatchLog}`); }
            if (fsImpl.existsSync(this.overflowLog)) { fail(`refusing to overwrite ${this.overflowLog}`); }
            fsImpl.writeFileSync(this.dispatchLog, 'seq,intended_offset_ms,intended_ms,dispatch_ms,lag_ms\n', { flag: 'wx' });
            fsImpl.writeFileSync(this.overflowLog, 'requested_seq,observed_ms\n', { flag: 'wx' });
        }

        async submitTransaction() {
            if (this.seq >= this.schedule.length) {
                // Caliper 0.6 fixed-rate scheduling can issue one trailing
                // callback while draining its timer. It must not become a
                // Fabric submission or contaminate the replay count.
                fsImpl.appendFileSync(this.overflowLog, `${this.seq},${clock()}\n`);
                this.seq += 1;
                return;
            }
            const seq = this.seq;
            // Caliper's fixed-rate controller may have several in-flight
            // workload calls. Reserve the sequence before the first await so
            // concurrent calls cannot reuse one schedule entry.
            this.seq += 1;
            const intendedOffsetMs = this.schedule[seq];
            // When Caliper's built-in replay-rate controller is active it owns
            // the inter-arrival sleep.  Keeping the workload gate disabled in
            // that mode avoids a second scheduler and the callback flood that
            // occurs when fixed-rate keeps invoking a gated workload.
            if (!this.roundArguments.rateControlled) {
                const waitMs = intendedOffsetMs - (clock() - this.startedMs);
                if (waitMs > 0) { await sleeper(waitMs); }
            }
            const intendedMs = this.startedMs + intendedOffsetMs;
            const dispatchMs = clock();
            const lagMs = dispatchMs - intendedMs;
            fsImpl.appendFileSync(this.dispatchLog,
                `${seq},${intendedOffsetMs},${intendedMs},${dispatchMs},${lagMs}\n`);
            const request = core.requestFor(this.roundArguments, this.roundIndex, seq);
            await this.sutAdapter.sendRequests(request);
        }
    };
}

function createWorkloadModule() { return new (build(loadBase()))(); }

module.exports = { build, createWorkloadModule, validateSchedule, validateArgs };
