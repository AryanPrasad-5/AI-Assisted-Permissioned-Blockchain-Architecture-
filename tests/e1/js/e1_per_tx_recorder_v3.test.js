'use strict';
/* A0j: e1_per_tx_recorder_v3 (recorder-health spool). SYNTHETIC: a scripted wall clock and a fake Caliper dispatch; no Caliper, Docker, Fabric
 * or network. Date.now is replaced for the duration of each test and restored afterwards. */

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const v2 = require('../../../scripts/e1/caliper/e1_per_tx_recorder_v2.js');
const v3 = require('../../../scripts/e1/caliper/e1_per_tx_recorder_v3.js');
const { FakeTxStatus, FakeDispatch } = require('./fake_caliper.js');

const T0 = Date.UTC(2026, 8, 21, 0, 0, 0);
const realNow = Date.now;
const created = [];
function tmpDir() {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'e1v3-'));
    created.push(dir);
    return dir;
}
test.after(() => { Date.now = realNow; for (const dir of created) { fs.rmSync(dir, { recursive: true, force: true }); } });

function readSpool(dir) {
    const file = fs.readdirSync(dir).find((f) => f.endsWith('.health.jsonl'));
    if (!file) { return []; }
    return fs.readFileSync(path.join(dir, file), 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
}

function healthOptions(clock, extra = {}) {
    return Object.assign({ sourceId: 'synthetic-test-day', collectorId: 'rec', evidenceOrigin: 'synthetic_fixture', heartbeatIntervalMs: 1000,
        autoTimer: false, nowMs: () => clock.now, monoMs: () => clock.now - T0 }, extra);
}

/** Replays `seconds` of scripted traffic. `plan(s)` returns the units the callback sees in second s (0 = none). */
async function round(dir, { seconds = 8, plan = () => 3, health = true, factory = v3, clock = { now: T0 - 500 }, hook = () => {}, healthExtra = {}, finish = true, endAt = null, flushRows = 1 } = {}) {
    Date.now = () => clock.now;
    try {
        const options = { runId: 'run-a0j', outputDir: dir, liveFlushRows: flushRows };
        if (health) { options.health = healthOptions(clock, healthExtra); }
        const rec = factory.createTxObserver(options, null, 0);
        const dispatch = new FakeDispatch([rec]);
        await dispatch.activate(0, 'synth');
        for (let s = 0; s < seconds; s++) {
            clock.now = T0 + s * 1000 + 250;
            const units = plan(s);
            if (units > 0) { dispatch.txSubmitted(units); }
            clock.now = T0 + (s + 1) * 1000;
            hook(rec, s);
            if (rec.tick) { rec.tick(); }
        }
        clock.now = endAt === null ? T0 + seconds * 1000 + 300 : endAt;
        if (finish) {
            dispatch.txFinished([new FakeTxStatus('tx0', T0 + 100, T0 + 550, 'success', [])]);
            await dispatch.deactivate();
        }
        return rec;
    } finally { Date.now = realNow; }
}

function submitUnits(dir) {
    const file = fs.readdirSync(dir).find((f) => f.endsWith('.live.submits.csv'));
    return fs.readFileSync(path.join(dir, file), 'utf8').trim().split('\n').slice(1).map((l) => l.split(',').map(Number));
}

test('health options are explicit and validated; there are no defaults for timing or origin', () => {
    const dir = tmpDir();
    const make = (health) => () => v3.createTxObserver({ runId: 'r', outputDir: dir, health }, null, 0);
    assert.throws(make({}), /sourceId/);
    assert.throws(make({ sourceId: 's', collectorId: 'c', heartbeatIntervalMs: 1000, evidenceOrigin: 'live' }), /evidenceOrigin/);
    assert.throws(make({ sourceId: 's', collectorId: 'c', heartbeatIntervalMs: 1500, evidenceOrigin: 'live_recorder' }), /heartbeatIntervalMs/);
    assert.throws(make({ sourceId: 's', collectorId: 'c', heartbeatIntervalMs: 500, evidenceOrigin: 'live_recorder' }), /heartbeatIntervalMs/);
    assert.throws(make({ sourceId: 's', collectorId: 'c', heartbeatIntervalMs: 1000, evidenceOrigin: 'live_recorder', sourceCount: { provider: () => 0 } }), /basis/);
    assert.throws(make({ sourceId: 's', collectorId: 'c', heartbeatIntervalMs: 1000, evidenceOrigin: 'live_recorder', sourceCount: { basis: 'x' } }), /provider/);
});

test('without a health block v3 behaves as v2 and writes no health evidence', async () => {
    const dir = tmpDir();
    await round(dir, { health: false });
    assert.equal(fs.readdirSync(dir).filter((f) => f.includes('.health')).length, 0);
    assert.ok(fs.readdirSync(dir).some((f) => f.endsWith('.live.final.json')));
});

test('v1 and v2 outputs are byte-identical whether or not the health spool runs', async () => {
    const a = tmpDir();
    const b = tmpDir();
    await round(a, { factory: v2, health: false });
    await round(b, { factory: v3, health: true });
    // wall-clock-dependent columns (obs_hr_ns, obs_wall_ms) legitimately differ between two runs; every other column must be identical
    const drop = { '.csv': [7], '.live.csv': [9, 10], '.submits.csv': [], '.live.submits.csv': [] };
    for (const [suffix, skip] of Object.entries(drop)) {
        const norm = (dirName) => fs.readFileSync(path.join(dirName, `run-a0j_w0_r0_synth${suffix}`), 'utf8').trimEnd().split('\n')
            .map((line) => line.split(',').filter((_, i) => !skip.includes(i)).join(','));
        assert.deepEqual(norm(a), norm(b), suffix);
    }
});

test('a healthy round writes start, whole-second heartbeats and a clean stop; counts equal the durable submit rows', async () => {
    const dir = tmpDir();
    await round(dir, { seconds: 8, plan: (s) => (s === 3 ? 0 : 2 + (s % 3)) });
    const rows = readSpool(dir);
    assert.deepEqual(rows.map((r) => r.n), rows.map((_, i) => i + 1));
    assert.equal(rows[0].kind, 'start');
    assert.equal(rows[0].coverage_scope, 'recorder_callback');
    assert.equal(rows[0].evidence_origin, 'synthetic_fixture');
    assert.ok(Date.parse(rows[0].observation_start_utc) > Date.parse(rows[0].t_utc), 'observation never starts before the start row is written');
    assert.equal(rows[0].lifecycle.window_open_utc, rows[0].observation_start_utc);
    assert.equal(rows[rows.length - 1].kind, 'stop');
    assert.equal(rows[rows.length - 1].reason, 'clean');
    const submits = submitUnits(dir);
    for (const r of rows.filter((x) => x.kind === 'heartbeat' || x.kind === 'stop')) {
        const wm = Date.parse(r.observed_through_utc);
        assert.equal(wm % 1000, 0);
        assert.ok(wm <= Date.parse(r.t_utc), 'a watermark never exceeds the time the row was written');
        const expected = submits.filter(([, ts]) => ts >= T0 && ts < wm).reduce((a, [, , c]) => a + c, 0);
        assert.equal(r.events_received, expected, `row ${r.n}`);
    }
    assert.ok(rows.some((r) => r.phase === 'window_close'));
});

test('every heartbeat flushes the live submits file first, so a certified count is always durable', async () => {
    const dir = tmpDir();
    await round(dir, { flushRows: 1000, plan: () => 4 });            // the buffer alone would never flush in this round
    const rows = readSpool(dir);
    const submits = submitUnits(dir);
    for (const r of rows.filter((x) => x.kind === 'heartbeat')) {
        const wm = Date.parse(r.observed_through_utc);
        assert.equal(r.events_received, submits.filter(([, ts]) => ts >= T0 && ts < wm).reduce((a, [, , c]) => a + c, 0), `row ${r.n}`);
    }
    assert.ok(rows.filter((x) => x.kind === 'heartbeat').every((x) => x.health === 'healthy'));
});

test('a source count is reported only when the operator supplies one, and its basis is declared', async () => {
    const withDir = tmpDir();
    const truth = (through, start) => { let t = 0; for (let s = 0; s < 8; s++) { const ts = T0 + s * 1000 + 250; if (ts >= start && ts < through) { t += 3; } } return t; };
    await round(withDir, { healthExtra: { sourceCount: { provider: truth, basis: 'synthetic_independent_source_counter', independenceClaim: true } } });
    const beats = readSpool(withDir).filter((r) => r.kind === 'heartbeat');
    assert.ok(beats.every((r) => r.source_events_expected === r.events_received && r.source_basis === 'synthetic_independent_source_counter'));
    const noDir = tmpDir();
    await round(noDir);
    assert.ok(readSpool(noDir).every((r) => r.source_events_expected === undefined));
    assert.equal(readSpool(noDir)[0].source_count_declared, false);
});

test('missed callbacks make the source count exceed the recorder count', async () => {
    const dir = tmpDir();
    const truth = (through, start) => { let t = 0; for (let s = 0; s < 8; s++) { const ts = T0 + s * 1000 + 250; if (ts >= start && ts < through) { t += 3; } } return t; };
    await round(dir, { plan: (s) => (s === 4 ? 0 : 3), healthExtra: { sourceCount: { provider: truth, basis: 'synthetic_independent_source_counter', independenceClaim: true } } });
    const beats = readSpool(dir).filter((r) => r.kind === 'heartbeat');
    const last = beats[beats.length - 1];
    assert.equal(last.source_events_expected - last.events_received, 3);
});

test('a spool write failure ends health output, leaves a marker, never reaches Caliper and leaves the v1/v2 files intact', async () => {
    const dir = tmpDir();
    await round(dir, { hook: (rec, s) => { if (s === 4 && rec.hfd !== null) { fs.closeSync(rec.hfd); } } });
    const rows = readSpool(dir);
    assert.ok(rows.length >= 4 && rows.every((r) => r.kind !== 'stop'), 'no stop is written once the spool cannot be written');
    assert.ok(fs.readdirSync(dir).some((f) => f.endsWith('.health.FAILED')));
    assert.ok(fs.existsSync(path.join(dir, 'run-a0j_w0_r0_synth.manifest.json')) && fs.existsSync(path.join(dir, 'run-a0j_w0_r0_synth.live.final.json')));
});

test('a failed live submit stream opens an error, degrades every later heartbeat and aborts the stop', async () => {
    const dir = tmpDir();
    await round(dir, { hook: (rec, s) => { if (s === 4 && rec.live) { rec.live.sub.failed = true; } } });
    const rows = readSpool(dir);
    assert.ok(rows.some((r) => r.kind === 'error' && r.error_code === 'live_submit_stream_failed'));
    assert.ok(rows.filter((r) => r.kind === 'heartbeat' && r.n > rows.findIndex((x) => x.kind === 'error')).every((r) => r.health === 'degraded'));
    assert.equal(rows[rows.length - 1].reason, 'abort');
});

test('a killed worker leaves no stop row', async () => {
    const dir = tmpDir();
    await round(dir, { finish: false });
    const rows = readSpool(dir);
    assert.ok(rows.length > 2 && rows.every((r) => r.kind !== 'stop'));
});

test('a round that ends inside its first second never opens its window and writes no stop', async () => {
    const dir = tmpDir();
    await round(dir, { seconds: 0, endAt: T0 - 200 });
    const rows = readSpool(dir);
    assert.deepEqual(rows.map((r) => r.kind), ['start']);
});

test('a wall-clock regression is flagged, degrades the heartbeat and aborts the stop', async () => {
    const dir = tmpDir();
    const clock = { now: T0 - 500 };
    await round(dir, { clock, hook: (rec, s) => {
        if (s === 4) { const saved = clock.now; clock.now = T0 + 1500; rec.txSubmitted(2); clock.now = saved; }   // stamped before the watermark that already passed
    } });
    const rows = readSpool(dir);
    assert.ok(rows.some((r) => r.kind === 'heartbeat' && r.health === 'degraded' && r.errors_since_last >= 1));
    const stop = rows[rows.length - 1];
    assert.equal(stop.reason, 'abort');
    assert.ok(stop.health_anomaly_codes.includes('wall_clock_regression'));
});

test('the spool is never overwritten', async () => {
    const dir = tmpDir();
    const clock = { now: T0 - 500 };
    await round(dir, { clock });
    const before = fs.readFileSync(path.join(dir, 'run-a0j_w0_r0_synth.health.jsonl'), 'utf8');
    Date.now = () => T0 - 500;
    const rec = v3.createTxObserver({ runId: 'run-a0j', outputDir: dir, health: healthOptions({ now: T0 - 500 }) }, null, 0);
    await rec.activate(0, 'synth').catch(() => {});
    Date.now = realNow;
    assert.equal(fs.readFileSync(path.join(dir, 'run-a0j_w0_r0_synth.health.jsonl'), 'utf8'), before);
});
