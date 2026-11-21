'use strict';
/*
 * A0m offline driver: loads the modules a DRY-RENDERED run directory would give Caliper, from THAT directory's modules/ folder only (never from the repository),
 * builds the generated round arguments and monitor options into a fake Caliper round, and writes what the issue-logged workload and recorder v3 produce.
 * SYNTHETIC: a scripted wall clock replaces Date.now, a fake Caliper base class and sutAdapter replace Caliper, no Fabric, Docker, network or process is involved.
 * Nothing here is a measurement and nothing here shows that the modules load inside a real Caliper worker. This is a driver, not a test file.
 *
 *   node a0m_generated_run.js <absolute rendered run dir (the wired_v3/<runId> folder)> <absolute empty output dir> [round index (default: last)] [submits]
 *
 * The container paths in the generated configuration (/hyperledger/caliper/workspace/runs/<runId>/...) are mapped to <output dir>/...; every other value is used as
 * generated. Only the clock functions of the recorder health block are injected (autoTimer false), because a real timer would make the output non-deterministic.
 * Prints one JSON line: { loaded, roundIndex, label, issueLog, recorderBase, files, assetKeys, submitTimes }.
 */
const fs = require('fs');
const path = require('path');

const [runDir, outDir, roundArg, submitsArg] = process.argv.slice(2);
if (!runDir || !path.isAbsolute(runDir) || !outDir || !path.isAbsolute(outDir)) { throw new Error('absolute run directory and output directory are required'); }
const modulesDir = path.join(runDir, 'modules');
const bench = JSON.parse(fs.readFileSync(path.join(runDir, 'benchmark.yaml'), 'utf8'));
const monitor = bench.monitors.transaction[0];
const containerRun = path.posix.dirname(monitor.options.outputDir);
const remap = (p) => {
    if (typeof p !== 'string' || !p.startsWith(`${containerRun}/`)) { throw new Error(`path ${p} is not under ${containerRun}`); }
    return path.join(outDir, ...p.slice(containerRun.length + 1).split('/'));
};
const roundIndex = roundArg === undefined ? bench.test.rounds.length - 1 : Number(roundArg);
const submits = submitsArg === undefined ? 5 : Number(submitsArg);
const round = bench.test.rounds[roundIndex];

const T0 = Date.UTC(2026, 10, 21, 0, 0, 0);
let now = T0 - 500;
Date.now = () => now;

// The modules come from the rendered directory. (The fake dispatch is test scaffolding and lives with the tests, not in the rendered run.)
const { FakeDispatch, FakeTxStatus } = require('./fake_caliper.js');
const recorderModule = require(path.join(modulesDir, path.basename(monitor.module)));
const workloadModule = require(path.join(modulesDir, path.basename(round.workload.module)));

const loaded = { inModules: [], outside: [] };
for (const file of Object.keys(require.cache)) {
    if (file.startsWith(modulesDir + path.sep)) { loaded.inModules.push(path.basename(file)); }
    else if (!file.startsWith(__dirname + path.sep)) { loaded.outside.push(file); }
}
loaded.inModules.sort();

class FakeBase {
    async initializeWorkloadModule(workerIndex, totalWorkers, rIndex, roundArguments, sutAdapter, sutContext) {
        Object.assign(this, { workerIndex, totalWorkers, roundIndex: rIndex, roundArguments, sutAdapter, sutContext });
    }
    async cleanupWorkloadModule() {}
}

(async () => {
    const args = JSON.parse(JSON.stringify(round.workload.arguments));
    if (args.issueLog) { args.issueLog.path = remap(args.issueLog.path); }      // a round without issueLog is the legacy (opt-out) case
    const recorderOptions = JSON.parse(JSON.stringify(monitor.options));
    recorderOptions.outputDir = remap(recorderOptions.outputDir);
    recorderOptions.health = { ...recorderOptions.health, autoTimer: false, nowMs: () => now, monoMs: () => now - T0 };
    const rec = recorderModule.createTxObserver(recorderOptions, null, 0);
    const dispatch = new FakeDispatch([rec]);

    let tickFn = null;
    const deps = { clock: () => now, mono: () => now - T0, setInterval: (fn) => { tickFn = fn; return { id: 1 }; }, clearInterval: () => {} };
    const Workload = workloadModule.build(FakeBase, async () => {}, deps);
    const w = new Workload();
    const submitTimes = [];
    const assetKeys = [];
    const sut = {
        sendRequests: async (request) => {
            assetKeys.push(request.contractArguments ? request.contractArguments[0] : null);
            now += 7;                                     // Caliper's submit event is seen a moment AFTER the generator's attempt record
            submitTimes.push(now);
            dispatch.txSubmitted(1);
            return { status: 'success' };
        },
    };
    now = T0;
    await dispatch.activate(roundIndex, round.label);
    await w.initializeWorkloadModule(0, 1, roundIndex, args, sut, {});
    for (let s = 0; s < submits; s += 1) {
        now = T0 + (s * 1000) + 100;
        await w.submitTransaction();
        now = T0 + ((s + 1) * 1000);
        rec.tick();
        if (tickFn) { tickFn(); }
    }
    now = T0 + (submits * 1000) + 200;
    await w.cleanupWorkloadModule();
    dispatch.txFinished([new FakeTxStatus('tx0', T0 + 100, T0 + 550, 'success', [])]);
    await dispatch.deactivate();
    const recorderDir = recorderOptions.outputDir;
    const logDir = remap(`${containerRun}/issuelog`);
    const files = fs.existsSync(recorderDir) ? fs.readdirSync(recorderDir).sort() : [];
    process.stdout.write(`${JSON.stringify({ loaded, roundIndex, label: round.label, issueLog: args.issueLog ? args.issueLog.path : null, recorderDir, files,
        issueLogFiles: fs.existsSync(logDir) ? fs.readdirSync(logDir).sort() : [], assetKeys, submitTimes })}
`);
    process.exit(0);
})().catch((err) => { process.stderr.write(`${err && err.stack || err}\n`); process.exit(1); });
