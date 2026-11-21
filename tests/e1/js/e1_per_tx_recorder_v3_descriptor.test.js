'use strict';
/* A0k: the v3 recorder copies the operator's sourceCount.descriptor into the start row and never judges it. SYNTHETIC: a scripted wall clock and a
 * fake Caliper dispatch; no Caliper, Docker, Fabric or network. Date.now is replaced for the duration of each test and restored afterwards. */

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const v3 = require('../../../scripts/e1/caliper/e1_per_tx_recorder_v3.js');
const { FakeDispatch } = require('./fake_caliper.js');

const T0 = Date.UTC(2026, 8, 21, 0, 0, 0);
const realNow = Date.now;
const created = [];
function tmpDir() {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'e1v3d-'));
    created.push(dir);
    return dir;
}
test.after(() => { Date.now = realNow; for (const dir of created) { fs.rmSync(dir, { recursive: true, force: true }); } });

const DESCRIPTOR = { counter_id: 'synthetic-generator-issue-counter', kind: 'generator_issue_counter', independent_of_recorder: true,
    derived_from: 'synthetic_workload_generator_counter', timestamp_semantics: 'generator_issue_time', resolution: 'interval' };

async function startRow(sourceCount) {
    const dir = tmpDir();
    const clock = { now: T0 - 500 };
    Date.now = () => clock.now;
    try {
        const health = { sourceId: 'synthetic-test-day', collectorId: 'rec', evidenceOrigin: 'synthetic_fixture', heartbeatIntervalMs: 1000,
            autoTimer: false, nowMs: () => clock.now, monoMs: () => clock.now - T0 };
        if (sourceCount !== undefined) { health.sourceCount = sourceCount; }
        const rec = v3.createTxObserver({ runId: 'run-a0k', outputDir: dir, health }, null, 0);
        const dispatch = new FakeDispatch([rec]);
        await dispatch.activate(0, 'synth');
        clock.now = T0 + 1000;
        rec.tick();
        clock.now = T0 + 1300;
        await dispatch.deactivate();
    } finally { Date.now = realNow; }
    const file = fs.readdirSync(dir).find((f) => f.endsWith('.health.jsonl'));
    return fs.readFileSync(path.join(dir, file), 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l))[0];
}

test('the descriptor is copied into the start row unchanged', async () => {
    const row = await startRow({ provider: () => 0, basis: 'synthetic_independent_source_counter', independenceClaim: true, descriptor: DESCRIPTOR });
    assert.equal(row.kind, 'start');
    assert.equal(row.source_count_declared, true);
    assert.deepEqual(row.source_counter, DESCRIPTOR);
});

test('a source count without a descriptor declares the count and no descriptor (the Python contract then certifies nothing)', async () => {
    const row = await startRow({ provider: () => 0, basis: 'synthetic_independent_source_counter', independenceClaim: true });
    assert.equal(row.source_count_declared, true);
    assert.equal(row.source_counter, null);
});

test('no source count: no descriptor and no declaration', async () => {
    const row = await startRow(undefined);
    assert.equal(row.source_count_declared, false);
    assert.equal(row.source_counter, null);
});

test('the recorder never judges the descriptor: even a recorder-derived one is copied as declared', async () => {
    const claim = Object.assign({}, DESCRIPTOR, { kind: 'recorder_derived', independent_of_recorder: false });
    const row = await startRow({ provider: () => 0, basis: 'synthetic_independent_source_counter', independenceClaim: true, descriptor: claim });
    assert.deepEqual(row.source_counter, claim);
});

test('a descriptor that is not an object is refused before anything is created', async () => {
    const dir = tmpDir();
    for (const bad of ['text', 7, ['x']]) {
        assert.throws(() => v3.createTxObserver({ runId: 'r', outputDir: dir, health: { sourceId: 's', collectorId: 'c', evidenceOrigin: 'synthetic_fixture',
            heartbeatIntervalMs: 1000, sourceCount: { provider: () => 0, basis: 'b', descriptor: bad } } }, null, 0), /descriptor/);
    }
});
