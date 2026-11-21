'use strict';
/*
 * Test driver for scripts/e1/caliper/e1_per_tx_recorder_v2.js (task A0c). Synthetic only: no Caliper, Docker or Fabric.
 * It reuses FakeTxStatus from fake_caliper.js (Caliper's TxStatus accessor names).
 *
 *   node a0c_live_round.js <absoluteOutputDir> <runId> <scenario>
 *
 * Scenarios (one JSON object is printed on stdout):
 *   midround   10 submits, 7 finishes (one failed, with a multi-line quoted error), wait for the timer flush, report
 *              what is on disk BEFORE deactivate; then 3 more finishes, deactivate, report what is on disk after.
 *   interrupt  5 submits, 5 finishes, flushLive(), report, then exit WITHOUT deactivate (a killed worker).
 *   edge       malformed timestamps: missing create, final before create, NaN, zero, create == final.
 *   nolive     liveFlushRows = 1000 and a long timer: rows must still be bounded by the final flush at deactivate.
 */
const fs = require('fs');
const path = require('path');
const { FakeTxStatus, FakeDispatch, FakeInternalObserver } = require('./fake_caliper.js');
const { createTxObserver } = require(path.resolve(__dirname, '../../../scripts/e1/caliper/e1_per_tx_recorder_v2.js'));

// Event-time origin. A0C_T0 (epoch ms) lets the end-to-end test keep create/final times in the same clock domain as the
// recorder's own submit-event times (Date.now); A0C_SUBMIT_GAP_MS spaces the submits so their time span is non-zero.
const T0 = process.env.A0C_T0 ? Number(process.env.A0C_T0) : 1700000000000;
const SUBMIT_GAP_MS = process.env.A0C_SUBMIT_GAP_MS ? Number(process.env.A0C_SUBMIT_GAP_MS) : 0;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function read(file) { return fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : null; }
function lines(text) { return text === null ? null : text.split('\n').filter((l) => l.length > 0); }

function snapshot(base) {
    return {
        live_csv_lines: lines(read(`${base}.live.csv`)),
        live_submits_lines: lines(read(`${base}.live.submits.csv`)),
        open_marker: fs.existsSync(`${base}.live.open.json`),
        final_marker: fs.existsSync(`${base}.live.final.json`),
        final_csv: fs.existsSync(`${base}.csv`),
        final_manifest: fs.existsSync(`${base}.manifest.json`),
    };
}

async function main() {
    const [outputDir, runId, scenario] = process.argv.slice(2);
    global.caliper = { config: { get: (k) => ({ 'caliper-fabric-gateway-eventstrategy': 'msp_any',
        'caliper-fabric-timeout-invokeorquery': '60', 'caliper-workspace': outputDir })[k] } };
    const options = { runId, outputDir, binding: 'fabric:2.5', eventSemantics: 'gateway_peer_commit_status',
        caliperVersion: '0.6.0', liveFlushRows: 1000, liveFlushIntervalMs: 20 };
    if (scenario === 'nolive') { options.liveFlushIntervalMs = 60000; }
    const rec = createTxObserver(options, null, 0);
    const dispatch = new FakeDispatch([rec, new FakeInternalObserver()]);
    const base = path.join(outputDir, `${runId}_w0_r1_m-${runId}`);
    await dispatch.activate(1, `m-${runId}`);
    const out = { scenario };

    if (scenario === 'midround' || scenario === 'nolive') {
        const txs = [];
        for (let i = 0; i < 10; i++) {
            dispatch.txSubmitted(1);
            if (SUBMIT_GAP_MS) { await sleep(SUBMIT_GAP_MS); }
            const failed = i === 3;
            txs.push(new FakeTxStatus(`tx${i}`, T0 + i * 100, T0 + i * 100 + 450 + i, failed ? 'failed' : 'success',
                failed ? ['endorsement failure,\nwith "quotes", commas and a second line'] : []));
        }
        for (let i = 0; i < 7; i++) { dispatch.txFinished(txs[i]); }
        await sleep(scenario === 'nolive' ? 120 : 250);          // longer than the 20 ms timer, shorter than 60 s
        out.before_deactivate = snapshot(base);
        for (let i = 7; i < 10; i++) { dispatch.txFinished(txs[i]); }
        await dispatch.deactivate();
        out.after_deactivate = snapshot(base);
        out.final_marker = JSON.parse(read(`${base}.live.final.json`));
        out.final_csv_lines = lines(read(`${base}.csv`));
    } else if (scenario === 'interrupt') {
        for (let i = 0; i < 5; i++) {
            dispatch.txSubmitted(1);
            dispatch.txFinished(new FakeTxStatus(`tx${i}`, T0 + i * 100, T0 + i * 100 + 500, 'success', []));
        }
        rec.flushLive();
        out.before_exit = snapshot(base);
        console.log(JSON.stringify(out));
        process.exit(0);                                          // no deactivate: a killed worker
    } else if (scenario === 'livefail') {
        const finish = (i) => {
            dispatch.txSubmitted(1);
            dispatch.txFinished(new FakeTxStatus(`tx${i}`, T0 + i * 100, T0 + i * 100 + 500, 'success', []));
        };
        for (let i = 0; i < 3; i++) { finish(i); }
        rec.flushLive();
        fs.closeSync(rec.live.tx.fd);                             // the live stream's file handle dies (a disk or handle problem)
        for (let i = 3; i < 6; i++) { finish(i); }
        rec.flushLive();
        await dispatch.deactivate();
        out.final_marker = JSON.parse(read(`${base}.live.final.json`));
        out.final_csv_rows = lines(read(`${base}.csv`)).length - 1;
    } else if (scenario === 'edge') {
        const cases = [
            ['ok', T0, T0 + 250, 'success'],
            ['zero_latency', T0, T0, 'success'],
            ['missing_create', undefined, T0 + 250, 'success'],
            ['final_before_create', T0 + 500, T0 + 250, 'success'],
            ['nan_final', T0, NaN, 'success'],
            ['zero_create', 0, T0 + 250, 'success'],
            ['failed_missing_create', undefined, T0 + 250, 'failed'],
        ];
        for (const [id, create, final, status] of cases) {
            dispatch.txSubmitted(1);
            dispatch.txFinished(new FakeTxStatus(id, create, final, status, status === 'failed' ? ['x'] : []));
        }
        await dispatch.deactivate();
        out.live_csv_lines = lines(read(`${base}.live.csv`));
        out.final_marker = JSON.parse(read(`${base}.live.final.json`));
    } else {
        throw new Error(`unknown scenario ${scenario}`);
    }
    if (scenario !== 'interrupt') { console.log(JSON.stringify(out)); }
}

main().catch((err) => { console.error(err); process.exit(1); });
