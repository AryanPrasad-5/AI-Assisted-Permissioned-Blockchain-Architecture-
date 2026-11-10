'use strict';
                                                                                                                   
const test = require('node:test');
const assert = require('node:assert/strict');
const path = require('path');

const WL = path.resolve(__dirname, '../../../fabric-testbed/caliper/workloads');
const core = require(path.join(WL, 'e1_workload_core.js'));
const { build } = require(path.join(WL, 'e1_create_asset.js'));

const ARGS = { runId: 'r001', contractId: 'e1asset', invokerIdentity: 'User1', valueBytes: 128, mode: 'create',
    failEvery: 25, rmwKey: 'hot', settleSeconds: 0 };
const CHAINCODE_KEY = /^[A-Za-z0-9._-]{1,128}$/;                                  

class FakeBase {
    async initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext) {
        Object.assign(this, { workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext });
    }
}

function fakeSut() {
    const sent = [];
    return { sent, sendRequests: async (r) => { sent.push(r); return { status: 'success' }; } };
}

test('keys are fixed width, unique, and satisfy the chaincode key rule', () => {
    const keys = Array.from({ length: 1000 }, (_, i) => core.keyFor('r001', 1, i));
    assert.equal(new Set(keys).size, 1000);
    assert.equal(new Set(keys.map((k) => k.length)).size, 1);
    assert.equal(keys[7], 'r001_1_0000007');
    assert.ok(keys.every((k) => CHAINCODE_KEY.test(k)));
    assert.throws(() => core.keyFor('r001', 1, 10000000), /seq/);
    assert.throws(() => core.keyFor('r001', -1, 0), /roundIndex/);
});

test('values are fixed-size printable ASCII within the chaincode limit', () => {
    const v = core.valueOf(128);
    assert.equal(Buffer.byteLength(v, 'utf8'), 128);
    assert.ok(/^[\x20-\x7e]+$/.test(v));
    assert.throws(() => core.validateArgs({ ...ARGS, valueBytes: 4097 }), /valueBytes/);
});

test('create mode: one CreateAsset(id, value) request per transaction, constant size', () => {
    const reqs = Array.from({ length: 5 }, (_, i) => core.requestFor(ARGS, 1, i));
    assert.ok(reqs.every((r) => r.contractFunction === 'CreateAsset' && r.contractArguments.length === 2
        && r.readOnly === false && r.contractId === 'e1asset' && r.invokerIdentity === 'User1'));
    assert.equal(new Set(reqs.map((r) => JSON.stringify(r).length)).size, 1);
});

test('E0-2a: every 25th transaction calls FailAlways() with no arguments (8 of 200)', () => {
    const reqs = Array.from({ length: 200 }, (_, i) => core.requestFor({ ...ARGS, mode: 'fail-every' }, 1, i));
    const fails = reqs.map((r, i) => [r, i]).filter(([r]) => r.contractFunction === 'FailAlways');
    assert.equal(fails.length, 8);
    assert.deepEqual(fails.map(([, i]) => i), [24, 49, 74, 99, 124, 149, 174, 199]);
    assert.ok(fails.every(([r]) => r.contractArguments.length === 0));
});

test('E0-2b: ReadModifyWrite("hot") on every transaction', () => {
    const r = core.requestFor({ ...ARGS, mode: 'rmw' }, 1, 3);
    assert.deepEqual([r.contractFunction, r.contractArguments], ['ReadModifyWrite', ['hot']]);
});

test('bad arguments fail loudly', () => {
    for (const bad of [{ ...ARGS, runId: 'has space' }, { ...ARGS, mode: 'random' }, { ...ARGS, contractId: '' },
        { ...ARGS, mode: 'fail-every', failEvery: 1 }, { ...ARGS, mode: 'rmw', rmwKey: 'a b' },
        { ...ARGS, settleSeconds: -1 }, null]) {
        assert.throws(() => core.validateArgs(bad), /e1 workload/);
    }
});

test('module adapter: settles before the round, sends one request per call, numbers from 0', async () => {
    const slept = [];
    const W = build(FakeBase, async (ms) => { slept.push(ms); });
    const w = new W();
    const sut = fakeSut();
    await w.initializeWorkloadModule(0, 1, 1, { ...ARGS, settleSeconds: 10 }, sut, {});
    assert.deepEqual(slept, [10000]);
    for (let i = 0; i < 3; i++) { await w.submitTransaction(); }
    assert.deepEqual(sut.sent.map((r) => r.contractArguments[0]), ['r001_1_0000000', 'r001_1_0000001', 'r001_1_0000002']);
    await w.initializeWorkloadModule(0, 1, 2, ARGS, sut, {});                                         
    await w.submitTransaction();
    assert.equal(sut.sent[3].contractArguments[0], 'r001_2_0000000');
    assert.deepEqual(slept, [10000]);                                                           
});

test('module adapter refuses more than one worker and bad arguments', async () => {
    const W = build(FakeBase, async () => {});
    await assert.rejects(new W().initializeWorkloadModule(0, 2, 1, ARGS, fakeSut(), {}), /exactly 1 worker/);
    await assert.rejects(new W().initializeWorkloadModule(0, 1, 1, { ...ARGS, mode: 'x' }, fakeSut(), {}), /mode/);
});
