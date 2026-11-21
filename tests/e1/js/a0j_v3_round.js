'use strict';
/*
 * Offline driver for e1_per_tx_recorder_v3 (A0j). SYNTHETIC: a scripted wall clock replaces Date.now, a fake Caliper dispatch replays the
 * observer call order, and the "source" is a closure. No Caliper, Docker, Fabric or network is involved.
 *
 *   node a0j_v3_round.js <absoluteOutputDir> <scenario> [seconds]
 *
 * scenarios: healthy | zeros | missed_callbacks | no_source | write_failure | killed | stream_failure | clock_step | no_health
 * Prints one JSON line: { base, spool, submits, files }.
 */
const path = require('path');
const fs = require('fs');
const { FakeDispatch, FakeTxStatus } = require('./fake_caliper.js');

const T0 = Date.UTC(2026, 8, 21, 0, 0, 0);            // 2026-09-21T00:00:00Z (the synthetic UTC day)
const outDir = process.argv[2];
const scenario = process.argv[3] || 'healthy';
const seconds = Number(process.argv[4] || 12);
if (!outDir || !path.isAbsolute(outDir)) { throw new Error('absolute output directory required'); }

let now = T0 - 500;
Date.now = () => now;                                   // the scripted wall clock (v1 and v2 read Date.now)

const zeroSeconds = new Set(scenario === 'zeros' || scenario === 'missed_callbacks' ? [3, 7] : []);
const missed = new Set(scenario === 'missed_callbacks' ? [5] : []);
const truth = new Map();                                // second -> units the synthetic SOURCE issued

function sourceThrough(throughMs, startMs) {
    let total = 0;
    for (const [sec, units] of truth) {
        const ts = T0 + sec * 1000 + 250;
        if (ts >= startMs && ts < throughMs) { total += units; }
    }
    return total;
}

const recorderModule = require('../../../scripts/e1/caliper/e1_per_tx_recorder_v3.js');
const health = scenario === 'no_health' ? undefined : {
    sourceId: 'synthetic-test-day', collectorId: 'rec', evidenceOrigin: 'synthetic_fixture', heartbeatIntervalMs: 1000,
    autoTimer: false, nowMs: () => now, monoMs: () => (scenario === 'clock_step' && now > T0 + 5000 ? now - T0 + 40000 : now - T0),
    // A0k: a source count is believed only with a descriptor. SYNTHETIC; it classifies as source-independent and interval-suitable by declaration only.
    sourceCount: scenario === 'no_source' ? undefined : { provider: sourceThrough, basis: 'synthetic_independent_source_counter', independenceClaim: true,
        descriptor: { counter_id: 'synthetic-generator-issue-counter', kind: 'generator_issue_counter', independent_of_recorder: true,
            derived_from: 'synthetic_workload_generator_counter', timestamp_semantics: 'generator_issue_time', resolution: 'interval' } },
};

(async () => {
    const rec = recorderModule.createTxObserver({ runId: 'run-a0j', outputDir: outDir, health, liveFlushRows: 1, liveFlushIntervalMs: 500 }, null, 0);
    const dispatch = new FakeDispatch([rec]);
    await dispatch.activate(0, 'synth');
    for (let s = 0; s < seconds; s++) {
        now = T0 + s * 1000 + 250;
        if (!zeroSeconds.has(s)) {
            const units = 10 + (s % 3);
            truth.set(s, units);
            if (!missed.has(s)) { dispatch.txSubmitted(units); }
        }
        now = T0 + (s + 1) * 1000;
        if (scenario === 'write_failure' && s === 4 && rec.hfd !== null) { fs.closeSync(rec.hfd); }
        if (scenario === 'stream_failure' && s === 4 && rec.live) { rec.live.sub.failed = true; }
        rec.tick();
    }
    now = T0 + seconds * 1000 + 300;
    if (scenario !== 'killed') {
        const results = [];
        for (let i = 0; i < 3; i++) { results.push(new FakeTxStatus(`tx${i}`, T0 + 100, T0 + 550, 'success', [])); }
        dispatch.txFinished(results);
        await dispatch.deactivate();
    }
    const base = path.join(outDir, 'run-a0j_w0_r0_synth');
    process.stdout.write(`${JSON.stringify({ base, spool: `${base}.health.jsonl`, submits: `${base}.live.submits.csv`, files: fs.readdirSync(outDir).sort() })}\n`);
    process.exit(0);
})().catch((err) => { process.stderr.write(`${err && err.stack || err}\n`); process.exit(1); });
