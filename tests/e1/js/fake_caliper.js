'use strict';
/*
 * Test doubles for the E1 recorder. Synthetic only; no Caliper, Docker or Fabric is involved.
 * FakeTxStatus mirrors the accessor names of Caliper's TxStatus (transaction-status.js:
 * GetID, GetStatus, GetTimeCreate, GetTimeFinal, IsVerified, GetErrMsg). FakeDispatch replays
 * the call order of tx-observer-dispatch.js (spec file 03 section 1): activate each observer in
 * list order, forward events only while active, deactivate each in list order, awaited.
 *
 * CLI: node synthetic_round.js is not needed; run this file directly to write one synthetic round:
 *   node fake_caliper.js <absoluteOutputDir> <runId> <n> [batchEvery]
 */
const path = require('path');

class FakeTxStatus {
    constructor(id, create, final, status, errs) {
        this.status = { id, time_create: create, time_final: final, status, verified: false,
            error_messages: errs || [] };
    }
    GetID() { return this.status.id; }
    GetStatus() { return this.status.status; }
    GetTimeCreate() { return this.status.time_create; }
    GetTimeFinal() { return this.status.time_final; }
    IsVerified() { return this.status.verified; }
    GetErrMsg() { return this.status.error_messages; }
}

class FakeInternalObserver {
    constructor() { this.deactivated = false; this.activated = false; }
    async activate() { this.activated = true; }
    txSubmitted() {}
    txFinished() {}
    async deactivate() { this.deactivated = true; }
}

class FakeDispatch {
    constructor(observers) { this.observers = observers; this.active = false; }
    async activate(roundIndex, roundLabel) {
        this.active = true;
        for (const o of this.observers) { await o.activate(roundIndex, roundLabel); }
    }
    txSubmitted(count) { if (this.active) { for (const o of this.observers) { o.txSubmitted(count); } } }
    txFinished(results) { if (this.active) { for (const o of this.observers) { o.txFinished(results); } } }
    async deactivate() {
        this.active = false;
        for (const o of this.observers) { await o.deactivate(); } // a throw here skips later observers
    }
}

/** Replays one fixed-count round: n submits, n finishes (every batchEvery-th as a 2-element array). */
async function playRound(dispatch, { roundIndex, label, n, t0 = 1700000000000, lam = 10, prefix = 'tx',
    finishCount = n, failEvery = 0, batchEvery = 0 }) {
    await dispatch.activate(roundIndex, label);
    const pending = [];
    for (let i = 0; i < n; i++) {
        dispatch.txSubmitted(1);
        const create = t0 + Math.round((i * 1000) / lam);
        const failed = failEvery && (i + 1) % failEvery === 0;
        pending.push(new FakeTxStatus(`${prefix}${String(i).padStart(4, '0')}`, create, create + 450,
            failed ? 'failed' : 'success', failed ? ['endorsement failure, with "quotes", commas'] : []));
    }
    let done = 0;
    while (done < finishCount) {
        if (batchEvery && done % batchEvery === 0 && done + 2 <= finishCount) {
            dispatch.txFinished(pending.slice(done, done + 2));
            done += 2;
        } else {
            dispatch.txFinished(pending[done]);
            done += 1;
        }
    }
    await dispatch.deactivate();
}

module.exports = { FakeTxStatus, FakeInternalObserver, FakeDispatch, playRound };

if (require.main === module) {
    const [outputDir, runId, n, batchEvery] = process.argv.slice(2);
    const { createTxObserver } = require(path.resolve(__dirname, '../../../scripts/e1/caliper/e1_per_tx_recorder.js'));
    global.caliper = { config: { get: (k) => ({ 'caliper-fabric-gateway-eventstrategy': 'msp_any',
        'caliper-fabric-timeout-invokeorquery': '60', 'caliper-workspace': outputDir })[k] } };
    const rec = createTxObserver({ runId, outputDir, binding: 'fabric:2.5',
        eventSemantics: 'gateway_peer_commit_status', caliperVersion: '0.6.0' }, null, 0);
    const dispatch = new FakeDispatch([rec, new FakeInternalObserver()]);
    playRound(dispatch, { roundIndex: 1, label: `m-${runId}`, n: Number(n), batchEvery: Number(batchEvery || 0) })
        .then(() => { console.log(`wrote ${outputDir}`); })
        .catch((err) => { console.error(err); process.exit(1); });
}
