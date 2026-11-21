'use strict';
// A0l offline driver: runs the REAL successor workload (e1_create_asset_issue_logged.js) and the REAL issue-log writer under a fake
// Caliper base class, a fake sutAdapter and a scripted clock, and writes the issue log it produces. SYNTHETIC: no Caliper, Fabric, Docker
// or network is involved, and nothing here is a measurement of a real system. This is a driver, not a test file.
//
//   node a0l_issue_round.js <absoluteOutDir> <absoluteConfig.json>
//
// config: { runId, dayMs, workerId, totalWorkers, until, txEvery, tickEveryS, lateTicksFromS, tickIntervalMs, rejectEvery, hangTxSeq,
//           cleanup, clockStepAtS, clockStepMs, logName, mode }
// Output (stdout, one JSON line): { log, bytesAtTick: [[second, bytes]], status, error }
const fs = require('fs');
const path = require('path');

const WL = path.resolve(__dirname, '../../../fabric-testbed/caliper/workloads');
const issued = require(path.join(WL, 'e1_create_asset_issue_logged.js'));

class FakeBase {
    async initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext) {
        Object.assign(this, { workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext });
    }
    async cleanupWorkloadModule() {}
}

async function main() {
    const [outDir, configPath] = process.argv.slice(2);
    const cfg = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    const logName = cfg.logName || 'issue.jsonl';
    const logPath = path.join(outDir, logName);
    let wall = cfg.dayMs - 500;
    let mono = 0;
    const advanceTo = (ms) => { const delta = cfg.dayMs + ms - wall; if (delta > 0) { wall += delta; mono += delta; } };
    let tickFn = null;
    const deps = {
        clock: () => wall, mono: () => mono,
        setInterval: (fn) => { tickFn = fn; return { id: 1 }; }, clearInterval: () => {},
    };
    const W = issued.build(FakeBase, async () => {}, deps);
    const w = new W();
    let sendCount = 0;
    const sut = {
        sendRequests: async (request) => {
            sendCount += 1;
            if (cfg.hangTxSeq !== undefined && cfg.hangTxSeq === sendCount - 1) { return new Promise(() => {}); }
            if (cfg.rejectEvery && sendCount % cfg.rejectEvery === 0) { throw new Error(`rejected tx ${sendCount - 1}`); }
            return { status: 'success' };
        },
    };
    const args = { runId: cfg.runId, contractId: 'e1asset', invokerIdentity: 'User1', valueBytes: 16, mode: cfg.mode || 'create', failEvery: 25,
        rmwKey: 'hot', settleSeconds: 0,
        issueLog: { path: logPath, workerId: cfg.workerId || 'w0', expectedWorkers: [cfg.workerId || 'w0'], clockDomain: cfg.clockDomain || 'synthetic-host:node_date_now',
            tickIntervalMs: cfg.tickIntervalMs || 1000 } };
    const result = { log: logPath, bytesAtTick: [], status: null, error: null };
    try {
        await w.initializeWorkloadModule(cfg.workerIndex === undefined ? 0 : cfg.workerIndex, cfg.totalWorkers || 1, 1, args, sut, {});
        const size = () => fs.statSync(logPath).size;
        result.bytesAtTick.push([-0.5, size()]);
        const pending = [];
        for (let sec = 1; sec <= cfg.until; sec += 1) {
            const s0 = sec - 1;
            if (s0 % cfg.txEvery === 0) {
                advanceTo((s0 * 1000) + 100);
                const call = w.submitTransaction();
                if (cfg.hangTxSeq !== undefined && sendCount - 1 === cfg.hangTxSeq) { pending.push(call.catch(() => {})); } else {
                    try { await call; } catch (err) { /* a rejected send is expected in the rejectEvery scenario */ }
                }
            }
            advanceTo(sec * 1000);
            if (cfg.clockStepAtS === sec) { wall += cfg.clockStepMs; }               // wall clock jumps; the monotonic clock does not
            const regular = sec % cfg.tickEveryS === 0;
            const late = cfg.lateTicksFromS !== undefined && sec > cfg.lateTicksFromS;
            const stopped = cfg.stopTicksAfterS !== undefined && sec > cfg.stopTicksAfterS;      // a killed generator: no tick, no stop, nothing more
            if ((regular || late) && !stopped) { tickFn(); result.bytesAtTick.push([sec, size()]); }
        }
        if (cfg.cleanup) { await w.cleanupWorkloadModule(); result.bytesAtTick.push([cfg.until, size()]); }
        result.status = w.issueLogStatus();
    } catch (err) {
        result.error = { message: String(err.message), code: err.code || null };
        result.status = w.issueLogStatus ? w.issueLogStatus() : null;
    }
    process.stdout.write(`${JSON.stringify(result)}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack}\n`); process.exit(1); });
