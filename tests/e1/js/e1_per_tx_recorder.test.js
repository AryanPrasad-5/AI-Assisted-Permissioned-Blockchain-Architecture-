'use strict';
// Synthetic lifecycle tests of the E1 per-transaction recorder (spec file 03 section 9).
// Run: node --test tests/e1/js/    (no Caliper, Docker or Fabric involved)
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const { createTxObserver } = require('../../../scripts/e1/caliper/e1_per_tx_recorder.js');
const { FakeTxStatus, FakeInternalObserver, FakeDispatch, playRound } = require('./fake_caliper.js');

const created = [];
function tmpDir() {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'e1rec-'));
    created.push(dir);
    return dir;
}

// Remove only the temporary directories this test file created (like pytest's tmp_path).
test.after(() => {
    for (const dir of created) { fs.rmSync(dir, { recursive: true, force: true }); }
});

function readCsv(file) {
    const [header, ...rows] = fs.readFileSync(file, 'utf8').trimEnd().split('\n');
    return { header: header.split(','), rows };
}

function readManifest(dir, base) {
    return JSON.parse(fs.readFileSync(path.join(dir, `${base}.manifest.json`), 'utf8'));
}

test.beforeEach(() => {
    global.caliper = { config: { get: (k) => ({ 'caliper-fabric-gateway-eventstrategy': 'msp_any',
        'caliper-fabric-timeout-invokeorquery': '60', 'caliper-workspace': '/ws' })[k] } };
});

test('R2: construction fails loudly without runId or with a relative or unwritable outputDir', () => {
    const dir = tmpDir();
    assert.throws(() => createTxObserver({ outputDir: dir }, null, 0), /runId/);
    assert.throws(() => createTxObserver({ runId: 'r', outputDir: 'relative/dir' }, null, 0), /absolute/);
    const blocker = path.join(dir, 'blocker');
    fs.writeFileSync(blocker, 'x');
    assert.throws(() => createTxObserver({ runId: 'r', outputDir: path.join(blocker, 'sub') }, null, 0));
    assert.deepEqual(fs.readdirSync(dir), ['blocker']); // the probe file is removed after the check
});

test('200-tx round: counts reconcile, files are complete, manifest records config in force', async () => {
    const dir = tmpDir();
    const rec = createTxObserver({ runId: 'RUN1', outputDir: dir, binding: 'fabric:2.5',
        eventSemantics: 'gateway_peer_commit_status', caliperVersion: '0.6.0' }, null, 0);
    const internal = new FakeInternalObserver();
    await playRound(new FakeDispatch([rec, internal]), { roundIndex: 1, label: 'm-RUN1', n: 200, batchEvery: 50 });
    const base = 'RUN1_w0_r1_m-RUN1';
    const { header, rows } = readCsv(path.join(dir, `${base}.csv`));
    assert.deepEqual(header, ['tx_id', 'seq_finish', 'create_ms', 'final_ms', 'status', 'verified', 'err',
        'obs_hr_ns', 'worker', 'round', 'label']);
    assert.equal(rows.length, 200);
    const m = readManifest(dir, base);
    assert.equal(m.counts.submitted, 200);
    assert.equal(m.counts.finished, 200);
    assert.equal(m.counts.pending_at_deactivate, 0);
    assert.equal(m.counts.duplicate_ids, 0);
    assert.equal(m.counts.no_id_records, 0);
    assert.equal(m.counts.array_events, 4); // R10: arrays at finish positions 0, 50, 100, 150
    assert.equal(m.counts.observer_errors, 0);
    assert.equal(m.config_in_force.event_strategy, 'msp_any');
    assert.equal(m.config_in_force.invoke_timeout_s, 60);
    assert.equal(m.config_in_force.binding, 'fabric:2.5');
    assert.equal(m.versions.caliper.value, '0.6.0');
    assert.equal(readCsv(path.join(dir, `${base}.submits.csv`)).rows.length, 200);
    assert.ok(internal.deactivated);
    assert.ok(!fs.readdirSync(dir).some((f) => f.endsWith('.tmp') || f.endsWith('.FAILED')));
});

test('O-6: warm-up and measured rounds write distinct files (no per-round overwrite)', async () => {
    const dir = tmpDir();
    const rec = createTxObserver({ runId: 'RUN2', outputDir: dir }, null, 0);
    const dispatch = new FakeDispatch([rec, new FakeInternalObserver()]);
    await playRound(dispatch, { roundIndex: 0, label: 'wu-RUN2', n: 30, prefix: 'wu' });
    await playRound(dispatch, { roundIndex: 1, label: 'm-RUN2', n: 200 });
    assert.equal(readCsv(path.join(dir, 'RUN2_w0_r0_wu-RUN2.csv')).rows.length, 30);
    assert.equal(readCsv(path.join(dir, 'RUN2_w0_r1_m-RUN2.csv')).rows.length, 200);
});

test('O-15: error path (deactivate with transactions in flight) records pending > 0', async () => {
    const dir = tmpDir();
    const rec = createTxObserver({ runId: 'RUN3', outputDir: dir }, null, 0);
    await playRound(new FakeDispatch([rec]), { roundIndex: 1, label: 'm', n: 200, finishCount: 150 });
    const m = readManifest(dir, 'RUN3_w0_r1_m');
    assert.equal(m.counts.pending_at_deactivate, 50);
    assert.equal(m.counts.rows, 150);
});

test('failures and no-ID records are counted; error text is CSV-escaped', async () => {
    const dir = tmpDir();
    const rec = createTxObserver({ runId: 'RUN4', outputDir: dir }, null, 0);
    const dispatch = new FakeDispatch([rec]);
    await dispatch.activate(1, 'm');
    dispatch.txSubmitted(2);
    dispatch.txFinished(new FakeTxStatus('a1', 1, 2, 'failed', ['bad, "x"']));
    dispatch.txFinished(new FakeTxStatus(undefined, 3, 3, 'failed', []));
    await dispatch.deactivate();
    const m = readManifest(dir, 'RUN4_w0_r1_m');
    assert.equal(m.counts.failed, 2);
    assert.equal(m.counts.no_id_records, 1);
    const text = fs.readFileSync(path.join(dir, 'RUN4_w0_r1_m.csv'), 'utf8');
    assert.match(text, /"bad, ""x"""/);
});

test('duplicate tx_id is reported in the manifest', async () => {
    const dir = tmpDir();
    const rec = createTxObserver({ runId: 'RUN5', outputDir: dir }, null, 0);
    const dispatch = new FakeDispatch([rec]);
    await dispatch.activate(1, 'm');
    dispatch.txSubmitted(2);
    dispatch.txFinished([new FakeTxStatus('dup', 1, 2, 'success'), new FakeTxStatus('dup', 1, 2, 'success')]);
    await dispatch.deactivate();
    assert.equal(readManifest(dir, 'RUN5_w0_r1_m').counts.duplicate_ids, 1);
});

test('events outside the active window are not recorded (dispatch drops them)', async () => {
    const dir = tmpDir();
    const rec = createTxObserver({ runId: 'RUN6', outputDir: dir }, null, 0);
    const dispatch = new FakeDispatch([rec]);
    dispatch.txSubmitted(1); // before activate: dropped by the dispatch
    await dispatch.activate(1, 'm');
    dispatch.txSubmitted(1);
    dispatch.txFinished(new FakeTxStatus('t', 1, 2, 'success'));
    await dispatch.deactivate();
    assert.equal(readManifest(dir, 'RUN6_w0_r1_m').counts.submitted, 1);
});

test('O-8/O-9: a write failure in deactivate never throws, so the internal observer still cleans up', async () => {
    const dir = tmpDir();
    const out = path.join(dir, 'out');
    const rec = createTxObserver({ runId: 'RUN7', outputDir: out }, null, 0);
    const internal = new FakeInternalObserver();
    const dispatch = new FakeDispatch([rec, internal]);
    await dispatch.activate(1, 'm');
    dispatch.txSubmitted(1);
    dispatch.txFinished(new FakeTxStatus('t', 1, 2, 'success'));
    fs.rmSync(out, { recursive: true });
    fs.writeFileSync(out, 'not a directory'); // every write under out/ now fails
    const errors = [];
    const orig = console.error;
    console.error = (msg) => errors.push(String(msg));
    try {
        await dispatch.deactivate(); // must resolve
    } finally {
        console.error = orig;
    }
    assert.ok(internal.deactivated, 'internal observer cleanup was skipped');
    assert.ok(errors.some((e) => e.startsWith('E1_OBSERVER_WRITE_FAILED')));
});

test('R4: an existing round file is never overwritten; a FAILED marker is written instead', async () => {
    const dir = tmpDir();
    const first = createTxObserver({ runId: 'RUN8', outputDir: dir }, null, 0);
    await playRound(new FakeDispatch([first]), { roundIndex: 1, label: 'm', n: 5 });
    const before = fs.readFileSync(path.join(dir, 'RUN8_w0_r1_m.csv'), 'utf8');
    const second = createTxObserver({ runId: 'RUN8', outputDir: dir }, null, 0);
    const orig = console.error;
    console.error = () => {};
    try {
        await playRound(new FakeDispatch([second]), { roundIndex: 1, label: 'm', n: 7, prefix: 'zz' });
    } finally {
        console.error = orig;
    }
    assert.equal(fs.readFileSync(path.join(dir, 'RUN8_w0_r1_m.csv'), 'utf8'), before);
    assert.ok(fs.existsSync(path.join(dir, 'RUN8_w0_r1_m.FAILED')));
});

test('R9: config values are UNAVAILABLE when the Caliper config singleton is absent', async () => {
    delete global.caliper;
    const dir = tmpDir();
    const rec = createTxObserver({ runId: 'RUN9', outputDir: dir }, null, 0);
    await playRound(new FakeDispatch([rec]), { roundIndex: 1, label: 'm', n: 3 });
    assert.equal(readManifest(dir, 'RUN9_w0_r1_m').config_in_force.event_strategy, 'UNAVAILABLE');
});

test('R7: the hot path does no I/O (no file appears before deactivate)', async () => {
    const dir = tmpDir();
    const rec = createTxObserver({ runId: 'RUN10', outputDir: dir }, null, 0);
    const dispatch = new FakeDispatch([rec]);
    await dispatch.activate(1, 'm');
    for (let i = 0; i < 1000; i++) {
        dispatch.txSubmitted(1);
        dispatch.txFinished(new FakeTxStatus(`t${i}`, i, i + 1, 'success'));
    }
    assert.deepEqual(fs.readdirSync(dir), []);
    await dispatch.deactivate();
    assert.equal(fs.readdirSync(dir).length, 3);
});
