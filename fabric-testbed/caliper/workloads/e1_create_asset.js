'use strict';
/*
 * Caliper 0.6.0 workload module for E0/E1 (LA-2). PROPOSED: never loaded by a real Caliper worker (E0-0
 * "stack/load" verifies it). All request logic is in e1_workload_core.js; this file only adapts it to
 * Caliper's WorkloadModuleBase (initializeWorkloadModule / submitTransaction).
 * - Exactly one worker (protocol section 10: deterministic arguments from the sequence number only).
 * - settleSeconds: the protocol section 9 step 4 idle period, slept in initializeWorkloadModule of the
 *   measured round, i.e. before the transaction observer activates.
 * - One sendRequests call with a single request per transaction.
 * The module is copied into the run's workspace by scripts/e1/e1_la2_render.py (SHA-256 recorded), so
 * '@hyperledger/caliper-core' resolves from the workspace (after `caliper bind`) or, failing that, from the
 * Caliper CLI that loaded it (require.main).
 */
const core = require('./e1_workload_core.js');

function loadBase() {
    try {
        return require('@hyperledger/caliper-core').WorkloadModuleBase;
    } catch (err) {
        return require.main.require('@hyperledger/caliper-core').WorkloadModuleBase;
    }
}

function build(Base, sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))) {
    return class E1CreateAsset extends Base {
        constructor() {
            super();
            this.seq = 0;
        }

        async initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext) {
            await super.initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter,
                sutContext);
            core.validateArgs(roundArguments);
            if (totalWorkers !== 1) {
                throw new Error(`e1 workload: exactly 1 worker required, got ${totalWorkers}`);
            }
            this.seq = 0;
            const settle = roundArguments.settleSeconds || 0;
            if (settle > 0) {
                await sleep(settle * 1000);
            }
        }

        async submitTransaction() {
            const request = core.requestFor(this.roundArguments, this.roundIndex, this.seq);
            this.seq += 1;
            await this.sutAdapter.sendRequests(request);
        }
    };
}

function createWorkloadModule() {
    const E1CreateAsset = build(loadBase());
    return new E1CreateAsset();
}

module.exports = { createWorkloadModule, build };
