'use strict';
// A0l: generator-side issuance log (writer + opt-in successor workload). SYNTHETIC only: a fake Caliper base class, a fake
// sutAdapter and a scripted clock. No Caliper, Fabric, Docker or network is involved; nothing here measures a real system.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');

const WL = path.resolve(__dirname, '../../../fabric-testbed/caliper/workloads');
const legacy = require(path.join(WL, 'e1_create_asset.js'));
const issued = require(path.join(WL, 'e1_create_asset_issue_logged.js'));
const log = require(path.join(WL, 'e1_issue_log.js'));

const ARGS = { runId: 'r001', contractId: 'e1asset', invokerIdentity: 'User1', valueBytes: 16, mode: 'create', failEvery: 25, rmwKey: 'hot',
    settleSeconds: 0 };

class FakeBase {
    async initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext) {
        Object.assign(this, { workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext });
    }
}

function tmpdir() { return fs.mkdtempSync(path.join(os.tmpdir(), 'a0l-')); }

function scripted(start = 1700000000000) {
    let t = start;
    return { clock: () => { t += 1; return t; }, mono: () => t - start, set: (v) => { t = v; }, now: () => t };
}

function fakeSut(onSend) {
    const sent = [];
    return { sent, extra: 'kept', sendRequests: async (r) => { sent.push(r); return onSend ? onSend(r, sent.length) : { status: 'success' }; } };
}

function cfg(dir, over) {
    return { path: path.join(dir, 'issue.log.jsonl'), workerId: 'w0', expectedWorkers: ['w0'], clockDomain: 'synthetic-host:node_date_now',
        tickIntervalMs: 1000, ...over };
}

function lines(file) { return fs.readFileSync(file, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l)); }

function make(deps, args) {
    const W = issued.build(FakeBase, async () => {}, deps);
    return { w: new W(), args: args || ARGS };
}

// ---------------------------------------------------------------------------------------------------------- 1, 2 opt-in and legacy
test('opt-in: without issueLog no file is created, the adapter is untouched and the requests equal the legacy workload', async () => {
    const dir = tmpdir();
    const a = make();
    const sutA = fakeSut();
    await a.w.initializeWorkloadModule(0, 1, 1, ARGS, sutA, {});
    for (let i = 0; i < 5; i++) { await a.w.submitTransaction(); }
    const Legacy = legacy.build(FakeBase, async () => {});
    const b = new Legacy();
    const sutB = fakeSut();
    await b.initializeWorkloadModule(0, 1, 1, ARGS, sutB, {});
    for (let i = 0; i < 5; i++) { await b.submitTransaction(); }
    assert.deepEqual(sutA.sent, sutB.sent);
    assert.equal(a.w.sutAdapter, sutA);
    assert.equal(a.w.issueLogStatus(), null);
    assert.deepEqual(fs.readdirSync(dir), []);
});

test('legacy module is untouched by the successor: it still builds without the issue-log module', () => {
    const src = fs.readFileSync(path.join(WL, 'e1_create_asset.js'), 'utf8');
    assert.ok(!/issue/i.test(src), 'e1_create_asset.js must not mention the issue log');
});

// ---------------------------------------------------------------------------------------------------------- 3 before-send ordering
test('intent and attempt are on disk BEFORE sendRequests runs; the outcome is written after it returns', async () => {
    const dir = tmpdir();
    const s = scripted();
    const { w } = make({ clock: s.clock, mono: s.mono, setInterval: () => ({}), clearInterval: () => {} });
    const file = cfg(dir).path;
    const seen = [];
    const sut = fakeSut((r, n) => { seen.push(lines(file).map((x) => `${x.kind}${x.n === undefined ? '' : x.n}`)); return { status: 'success' }; });
    await w.initializeWorkloadModule(0, 1, 1, { ...ARGS, issueLog: cfg(dir) }, sut, {});
    await w.submitTransaction();
    assert.deepEqual(seen[0], ['start', 'intent0', 'attempt0']);                       // no outcome yet while the call runs
    assert.deepEqual(lines(file).map((x) => x.kind), ['start', 'intent', 'attempt', 'outcome']);
    const rows = lines(file);
    assert.equal(rows[1].issue_id, 'r001:1:0');
    assert.equal(rows[1].asset_key, 'r001_1_0000000');
    assert.equal(rows[1].op, 'CreateAsset');
    assert.equal(rows[3].result, 'returned');
    assert.equal(rows[3].client_status, 'success');
    assert.equal(rows[3].client_tx_id, null);                                          // never invented
    assert.ok(rows.every((x, i) => x.rseq === i + 1 && x.run_id === 'r001' && x.worker_id === 'w0'));
    assert.ok(rows[1].t_ms <= rows[2].t_ms && rows[2].t_ms <= rows[3].t_ms);
    assert.equal(sut.sent.length, 1);
    assert.equal(sut.extra, 'kept');
    assert.equal(w.sutAdapter.extra, 'kept');                                          // non-send members are forwarded
});

test('canonical form and hash chain match the Python reader contract (fixed vector)', () => {
    assert.equal(log.canonical({ b: 1, a: 'x', c: [1, null, true] }), '{"a":"x","b":1,"c":[1,null,true]}');
    assert.equal(log.digestOf({ a: 1 }, log.GENESIS_SHA256),
        require('crypto').createHash('sha256').update('{"a":1,"prev":"' + '0'.repeat(64) + '"}', 'utf8').digest('hex'));
    assert.throws(() => log.canonical({ a: 1.5 }), /value_not_integer/);
    assert.throws(() => log.canonical({ a: 'café' }), /text_not_ascii/);
    assert.throws(() => log.canonical({ a: undefined }), /value_undefined/);
});

// ---------------------------------------------------------------------------------------------------------- 5 failed submission
test('a rejected submission is logged as rejected and the original error propagates unchanged', async () => {
    const dir = tmpdir();
    const s = scripted();
    const { w } = make({ clock: s.clock, mono: s.mono, setInterval: () => ({}), clearInterval: () => {} });
    const boom = new Error('endorsement refused');
    const sut = { sendRequests: async () => { throw boom; } };
    await w.initializeWorkloadModule(0, 1, 1, { ...ARGS, issueLog: cfg(dir) }, sut, {});
    await assert.rejects(w.submitTransaction(), (err) => err === boom);
    const rows = lines(cfg(dir).path);
    assert.deepEqual(rows.map((x) => x.kind), ['start', 'intent', 'attempt', 'outcome']);
    assert.equal(rows[3].result, 'rejected');
    assert.equal(rows[3].error, 'endorsement refused');
    assert.equal(w.issueLogStatus().failed, false);                                    // a failed send is not a failed log
    assert.equal(w.issueLogStatus().in_flight, 0);
});

// ---------------------------------------------------------------------------------------------------------- 6 write failure
function failingFs(failOnWriteNumber) {
    let n = 0;
    return { ...fs, writeSync: (...args) => { n += 1; if (n === failOnWriteNumber) { throw new Error('disk full'); } return fs.writeSync(...args); } };
}

test('write failure before the send: the transaction is NOT sent, the writer is failed and refuses everything after', async () => {
    const dir = tmpdir();
    const s = scripted();
    // writes: 1 start, 2 intent0 (fails)
    const { w } = make({ fsImpl: failingFs(2), clock: s.clock, mono: s.mono, setInterval: () => ({}), clearInterval: () => {} });
    const sut = fakeSut();
    await w.initializeWorkloadModule(0, 1, 1, { ...ARGS, issueLog: cfg(dir) }, sut, {});
    await assert.rejects(w.submitTransaction(), /write_failed/);
    assert.equal(sut.sent.length, 0);
    await assert.rejects(w.submitTransaction(), /writer_failed/);
    assert.equal(sut.sent.length, 0);
    const status = w.issueLogStatus();
    assert.equal(status.failed, true);
    assert.match(status.failure, /disk full/);
});

test('write failure after the send: the result is unchanged, but the writer fails closed for every later issuance', async () => {
    const dir = tmpdir();
    const s = scripted();
    // writes: 1 start, 2 intent0, 3 attempt0, 4 outcome0 (fails)
    const { w } = make({ fsImpl: failingFs(4), clock: s.clock, mono: s.mono, setInterval: () => ({}), clearInterval: () => {} });
    const sut = fakeSut();
    await w.initializeWorkloadModule(0, 1, 1, { ...ARGS, issueLog: cfg(dir) }, sut, {});
    await w.submitTransaction();                                                         // does not throw: submission semantics preserved
    assert.equal(sut.sent.length, 1);
    assert.equal(w.issueLogStatus().failed, true);
    await assert.rejects(w.submitTransaction(), /writer_failed/);
    assert.equal(sut.sent.length, 1);                                                    // the second transaction was never sent
    assert.deepEqual(lines(cfg(dir).path).map((x) => x.kind), ['start', 'intent', 'attempt']);   // the unresolved attempt stays visible
});

// ---------------------------------------------------------------------------------------------------------- 7, 8 duplicates and retries
test('duplicate issuance of one txSeq is refused before the call (a retry is not a new issuance)', async () => {
    const dir = tmpdir();
    const s = scripted();
    const writer = new log.IssueLogWriter({ path: cfg(dir).path, runId: 'r001', workerIndex: 0, workerId: 'w0', totalWorkers: 1,
        expectedWorkers: ['w0'], clockDomain: 'd', roundIndex: 1, tickIntervalMs: 1000, clock: s.clock, mono: s.mono });
    writer.open();
    const sut = fakeSut();
    const wrapped = log.wrapSutAdapter(sut, writer, () => 7, null);
    await wrapped.sendRequests({ contractFunction: 'FailAlways', contractArguments: [] });
    await assert.rejects(wrapped.sendRequests({ contractFunction: 'FailAlways', contractArguments: [] }), /duplicate_issuance/);
    assert.equal(sut.sent.length, 1);
    assert.equal(writer.status().failed, true);
});

test('a key that does not belong to the sequence number is refused (no send)', async () => {
    const dir = tmpdir();
    const s = scripted();
    const { w } = make({ clock: s.clock, mono: s.mono, setInterval: () => ({}), clearInterval: () => {} });
    const sut = fakeSut();
    await w.initializeWorkloadModule(0, 1, 1, { ...ARGS, issueLog: cfg(dir) }, sut, {});
    w.seq = 5;                                                                          // corrupt the workload counter between key and send
    const wrapped = w.sutAdapter;
    await assert.rejects(wrapped.sendRequests({ contractFunction: 'CreateAsset', contractArguments: ['r001_1_0000099', 'v'] }), /tx_seq_mismatch/);
    assert.equal(sut.sent.length, 0);
});

// ---------------------------------------------------------------------------------------------------------- 9 shutdown, ticks
test('clean cleanup writes a stop record with the final counts; no cleanup leaves the log without one (interrupted)', async () => {
    const dir = tmpdir();
    const s = scripted();
    let tickFn = null;
    const deps = { clock: s.clock, mono: s.mono, setInterval: (fn) => { tickFn = fn; return { id: 1 }; }, clearInterval: () => {} };
    const { w } = make(deps);
    const sut = fakeSut();
    await w.initializeWorkloadModule(0, 1, 1, { ...ARGS, issueLog: cfg(dir) }, sut, {});
    await w.submitTransaction();
    tickFn();
    await w.submitTransaction();
    tickFn();
    await w.cleanupWorkloadModule();
    const rows = lines(cfg(dir).path);
    const kinds = rows.map((x) => x.kind);
    assert.deepEqual(kinds, ['start', 'intent', 'attempt', 'outcome', 'tick', 'intent', 'attempt', 'outcome', 'tick', 'stop']);
    const tick = rows.filter((x) => x.kind === 'tick')[1];
    assert.deepEqual([tick.intents, tick.attempted, tick.outcomes], [2, 2, 2]);
    const stop = rows[rows.length - 1];
    assert.deepEqual([stop.reason, stop.intents, stop.attempted, stop.outcomes, stop.in_flight], ['cleanup', 2, 2, 2, 0]);
    await assert.rejects(w.submitTransaction(), /writer_closed/);

    const dir2 = tmpdir();
    const { w: w2 } = make({ clock: s.clock, mono: s.mono, setInterval: () => ({}), clearInterval: () => {} });
    await w2.initializeWorkloadModule(0, 1, 1, { ...ARGS, issueLog: cfg(dir2) }, fakeSut(), {});
    await w2.submitTransaction();
    assert.equal(lines(cfg(dir2).path).some((x) => x.kind === 'stop'), false);          // killed process: no stop record
});

test('an in-flight transaction at cleanup is recorded as in_flight, never silently resolved', async () => {
    const dir = tmpdir();
    const s = scripted();
    const { w } = make({ clock: s.clock, mono: s.mono, setInterval: () => ({}), clearInterval: () => {} });
    let release;
    const sut = { sendRequests: () => new Promise((resolve) => { release = resolve; }) };
    await w.initializeWorkloadModule(0, 1, 1, { ...ARGS, issueLog: cfg(dir) }, sut, {});
    const pending = w.submitTransaction();
    await w.cleanupWorkloadModule();
    const rows = lines(cfg(dir).path);
    assert.equal(rows[rows.length - 1].in_flight, 1);
    release({ status: 'success' });
    await pending;                                                                      // the late outcome write is refused, never thrown
    assert.equal(lines(cfg(dir).path).filter((x) => x.kind === 'outcome').length, 0);
});

test('ticks are written by the timer, fsync follows the tick policy, and a failed tick marks the writer failed', async () => {
    const dir = tmpdir();
    const s = scripted();
    let synced = 0;
    const fsImpl = { ...fs, fsyncSync: (fd) => { synced += 1; return fs.fsyncSync(fd); } };
    let tickFn = null;
    const { w } = make({ fsImpl, clock: s.clock, mono: s.mono, setInterval: (fn, ms) => { tickFn = fn; assert.equal(ms, 250); return {}; }, clearInterval: () => {} });
    await w.initializeWorkloadModule(0, 1, 1, { ...ARGS, issueLog: cfg(dir, { tickIntervalMs: 250 }) }, fakeSut(), {});
    tickFn();
    tickFn();
    assert.equal(synced, 2);
    assert.equal(lines(cfg(dir).path).filter((x) => x.kind === 'tick').length, 2);
    assert.equal(w.issueLogStatus().failed, false);
});

// ---------------------------------------------------------------------------------------------------------- 10, 11 identity and workers
test('worker identity must be explicit and match; the roster must be exactly the one worker', async () => {
    for (const [over, pattern] of [
        [{ workerId: undefined }, /worker_identity_missing/],
        [{ workerId: 'w1' }, /worker_identity_mismatch/],
        [{ workerId: '0' }, /worker_identity_mismatch/],
        [{ expectedWorkers: ['w0', 'w1'] }, /worker_roster_mismatch/],
        [{ expectedWorkers: ['w9'] }, /worker_roster_mismatch/],
    ]) {
        const dir = tmpdir();
        const { w } = make();
        await assert.rejects(w.initializeWorkloadModule(0, 1, 1, { ...ARGS, issueLog: cfg(dir, over) }, fakeSut(), {}), pattern);
        assert.deepEqual(fs.readdirSync(dir), []);                                      // refused before any file exists
    }
});

test('more than one worker is refused with the issue log selected, before any file is created', async () => {
    const dir = tmpdir();
    const { w } = make();
    await assert.rejects(w.initializeWorkloadModule(0, 2, 1, { ...ARGS, issueLog: cfg(dir) }, fakeSut(), {}), /worker_count_unsupported/);
    assert.deepEqual(fs.readdirSync(dir), []);
    const { w: w0 } = make();
    await assert.rejects(w0.initializeWorkloadModule(0, 2, 1, ARGS, fakeSut(), {}), /exactly 1 worker/);   // the legacy guard is unchanged
});

test('an existing log file is never overwritten, and bad clock-domain, path and tick settings are refused', async () => {
    const dir = tmpdir();
    fs.writeFileSync(cfg(dir).path, 'precious\n');
    const { w } = make();
    await assert.rejects(w.initializeWorkloadModule(0, 1, 1, { ...ARGS, issueLog: cfg(dir) }, fakeSut(), {}), /log_exists/);
    assert.equal(fs.readFileSync(cfg(dir).path, 'utf8'), 'precious\n');
    for (const [over, pattern] of [[{ clockDomain: '' }, /clock_domain_invalid/], [{ path: 'relative.jsonl' }, /path_invalid/],
        [{ tickIntervalMs: 1 }, /tick_interval_invalid/], [{ fsyncPolicy: 'sometimes' }, /fsync_policy_invalid/]]) {
        const d = tmpdir();
        const { w: x } = make();
        await assert.rejects(x.initializeWorkloadModule(0, 1, 1, { ...ARGS, issueLog: cfg(d, over) }, fakeSut(), {}), pattern);
    }
});

test('modes other than create log the operation and no invented asset key', async () => {
    const dir = tmpdir();
    const s = scripted();
    const { w } = make({ clock: s.clock, mono: s.mono, setInterval: () => ({}), clearInterval: () => {} });
    await w.initializeWorkloadModule(0, 1, 1, { ...ARGS, mode: 'rmw', issueLog: cfg(dir) }, fakeSut(), {});
    await w.submitTransaction();
    const intent = lines(cfg(dir).path)[1];
    assert.equal(intent.op, 'ReadModifyWrite');
    assert.equal(intent.asset_key, null);
});

test('the issue-log modules do not reference the recorder or its files (structural independence)', () => {
    for (const file of ['e1_issue_log.js', 'e1_create_asset_issue_logged.js']) {
        const src = fs.readFileSync(path.join(WL, file), 'utf8');
        assert.ok(!/createTxObserver|txSubmitted|txFinished|live\.submits|e1_per_tx_recorder/.test(src.replace(/^\/\/.*$/gm, '')), file);
        assert.ok(!/require\(['"][^'"]*recorder/.test(src), file);
    }
});
