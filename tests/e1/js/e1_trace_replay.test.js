'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { build, validateSchedule } = require('../../../fabric-testbed/caliper/workloads/e1_trace_replay.js');

class Base {
    async initializeWorkloadModule() {}
}

function fakeSut(sent) { return { sendRequests: async (r) => { sent.push(r); } }; }

test('trace replay validates chronological schedules', () => {
    assert.deepEqual(validateSchedule([0, 1.5, 2]), [0, 1.5, 2]);
    assert.throws(() => validateSchedule([0, -1]), /chronological/);
    assert.throws(() => validateSchedule([1, 0]), /chronological/);
});

test('trace replay preserves deterministic asset requests and records dispatch lag', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'e1trace-'));
    const log = path.join(dir, 'dispatch.csv');
    let now = 1000;
    const sleeps = [];
    const sent = [];
    const W = build(Base, () => now, async (ms) => { sleeps.push(ms); now += ms; }, fs);
    const w = new W();
    const args = { runId: 'trace1', contractId: 'e1asset', invokerIdentity: 'User1', valueBytes: 32,
        mode: 'create', scheduleMs: [0, 25, 50], dispatchLog: log };
    await w.initializeWorkloadModule(0, 1, 2, args, fakeSut(sent), {});
    await w.submitTransaction();
    await w.submitTransaction();
    await w.submitTransaction();
    assert.deepEqual(sent.map((r) => r.contractArguments[0]), ['trace1_2_0000000', 'trace1_2_0000001', 'trace1_2_0000002']);
    assert.deepEqual(sleeps, [25, 25]);
    const rows = fs.readFileSync(log, 'utf8').trim().split('\n');
    assert.equal(rows.length, 4);
    assert.match(rows[1], /^0,0,1000,1000,0$/);
    assert.match(rows[2], /^1,25,1025,1025,0$/);
});

test('rate-controlled mode delegates timing to Caliper without a second workload gate', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'e1trace-rate-'));
    const log = path.join(dir, 'dispatch.csv');
    let now = 1000;
    const sleeps = [];
    const sent = [];
    const W = build(Base, () => now, async (ms) => { sleeps.push(ms); now += ms; }, fs);
    const w = new W();
    const args = { runId: 'rate1', contractId: 'e1asset', invokerIdentity: 'User1', valueBytes: 32,
        mode: 'create', scheduleMs: [0, 25], dispatchLog: log, rateControlled: true };
    await w.initializeWorkloadModule(0, 1, 0, args, fakeSut(sent), {});
    await w.submitTransaction();
    await w.submitTransaction();
    assert.deepEqual(sleeps, []);
    assert.equal(sent.length, 2);
    assert.equal(fs.readFileSync(`${log}.overflow.csv`, 'utf8').trim(), 'requested_seq,observed_ms');
});
