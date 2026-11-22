'use strict';
// A0p offline driver: runs the REAL timed successor workload (e1_create_asset_issue_logged_timed.js) and the REAL timed writer under a fake Caliper base class, a fake
// sutAdapter, scripted clocks and an optional failing file system, and writes the issue log and the timing sidecar it produces. SYNTHETIC: no Caliper, Fabric, Docker
// or network is involved, and nothing here is a measurement of a real system. This is a driver, not a test file.
//
//   node a0p_timed_round.js <absoluteOutDir> <absoluteConfig.json>
//
// config: { runId, dayMs, workerId, until, txEvery, tickEveryS, tickIntervalMs, fsyncPolicy, logName, timingName, stepUs, cleanup, issueLogKeyAlso, useLegacyKey,
//           failLogWriteAt, failLogFsyncAt, failSidecarWriteAt, failSidecarKind }
//   stepUs: every call of the scripted monotonic-microsecond clock advances it by this many microseconds (default 40); the sidecar wall clock follows it.
//   failLogWriteAt / failLogFsyncAt: the Nth (1-based) writeSync / fsyncSync on the LOG file throws; failSidecarWriteAt: the Nth writeSync on the sidecar throws.
// Output (stdout, one JSON line): { log, timing, sends, status, error }
const fs = require('fs');
const path = require('path');

const WL = path.resolve(__dirname, '../../../fabric-testbed/caliper/workloads');
const timedWorkload = require(path.join(WL, 'e1_create_asset_issue_logged_timed.js'));

class FakeBase {
    async initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext) {
        Object.assign(this, { workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext });
    }
    async cleanupWorkloadModule() {}
}

async function main() {
    const [outDir, configPath] = process.argv.slice(2);
    const cfg = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    const logPath = path.join(outDir, cfg.logName || 'issue.jsonl');
    const timingPath = path.join(outDir, cfg.timingName || 'issue.timing.jsonl');
    let wall = cfg.dayMs - 500;
    let mono = 0;
    const monoBase = 5_000_123;
    let monoUs = monoBase;
    // the scripted wall, millisecond monotonic and microsecond monotonic clocks all advance together, so the sidecar's clocks stay consistent with the log's
    const advanceTo = (ms) => { const delta = cfg.dayMs + ms - wall; if (delta > 0) { wall += delta; mono += delta; monoUs += delta * 1000; } };
    const stepUs = cfg.stepUs === undefined ? 40 : cfg.stepUs;
    const counters = { logWrite: 0, logFsync: 0, sidecarWrite: 0 };
    const fds = new Map();
    const fsImpl = {
        mkdirSync: (...a) => fs.mkdirSync(...a),
        existsSync: (...a) => fs.existsSync(...a),
        openSync: (p, ...a) => { const fd = fs.openSync(p, ...a); fds.set(fd, p); return fd; },
        closeSync: (fd) => fs.closeSync(fd),
        writeSync: (fd, ...a) => {
            const p = fds.get(fd);
            if (p === logPath) { counters.logWrite += 1; if (cfg.failLogWriteAt === counters.logWrite) { throw new Error('injected log write failure'); } }
            if (p === timingPath) { counters.sidecarWrite += 1; if (cfg.failSidecarWriteAt === counters.sidecarWrite) { throw new Error('injected sidecar write failure'); } }
            return fs.writeSync(fd, ...a);
        },
        fsyncSync: (fd) => {
            if (fds.get(fd) === logPath) { counters.logFsync += 1; if (cfg.failLogFsyncAt === counters.logFsync) { throw new Error('injected log fsync failure'); } }
            return fs.fsyncSync(fd);
        },
    };
    let tickFn = null;
    const deps = {
        clock: () => wall, mono: () => mono, fsImpl,
        timingClock: () => cfg.dayMs - 500 + Math.floor((monoUs - monoBase) / 1000), monoUs: () => { monoUs += stepUs; return monoUs; },
        setInterval: (fn) => { tickFn = fn; return { id: 1 }; }, clearInterval: () => {},
    };
    const W = timedWorkload.build(FakeBase, async () => {}, deps);
    const w = new W();
    let sendCount = 0;
    const sut = { sendRequests: async () => { sendCount += 1; return { status: 'success' }; } };
    const wid = cfg.workerId || 'w0';
    const base = { path: logPath, workerId: wid, expectedWorkers: [wid], clockDomain: cfg.clockDomain || 'synthetic-host:node_date_now', tickIntervalMs: cfg.tickIntervalMs || 1000,
        fsyncPolicy: cfg.fsyncPolicy };
    const args = { runId: cfg.runId, contractId: 'e1asset', invokerIdentity: 'User1', valueBytes: 16, mode: 'create', failEvery: 25, rmwKey: 'hot', settleSeconds: 0 };
    if (cfg.useLegacyKey) { args.issueLog = base; } else { args.issueLogTimed = { ...base, timingPath }; }
    if (cfg.issueLogKeyAlso) { args.issueLog = base; }
    const result = { log: logPath, timing: timingPath, sends: 0, status: null, error: null };
    try {
        await w.initializeWorkloadModule(0, cfg.totalWorkers || 1, 1, args, sut, {});
        for (let sec = 1; sec <= cfg.until; sec += 1) {
            const s0 = sec - 1;
            if (s0 % cfg.txEvery === 0) {
                advanceTo((s0 * 1000) + 100);
                try { await w.submitTransaction(); } catch (err) { result.error = result.error || { message: String(err.message), code: err.code || null, atSec: sec }; }
            }
            advanceTo(sec * 1000);
            if (sec % cfg.tickEveryS === 0) { tickFn(); }
        }
        if (cfg.cleanup !== false) { await w.cleanupWorkloadModule(); }
        result.status = w.issueLogStatus();
    } catch (err) {
        result.error = { message: String(err.message), code: err.code || null };
        result.status = w.issueLogStatus ? w.issueLogStatus() : null;
    }
    result.sends = sendCount;
    process.stdout.write(`${JSON.stringify(result)}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack}\n`); process.exit(1); });
