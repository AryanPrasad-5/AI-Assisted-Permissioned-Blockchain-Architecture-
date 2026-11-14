'use strict';
// Offline harness for the REAL e1_trace_replay.js / e1_workload_core.js. It does not start Caliper, a network or a container.
// usage: node harness.js <repoRoot> <argsJson>   (argsJson: {mode, roundIndex, totalWorkers, arguments})
// mode "controller": emulates Caliper replay-rate by advancing a virtual clock to each intended offset (arguments.rateControlled true)
// mode "self":       rateControlled false, so the module's own gate sleeps on the virtual clock
// mode "validate":   only runs validateArgs / initializeWorkloadModule and reports the error text

const fs = require('fs');
const path = require('path');

const repoRoot = process.argv[2];
const spec = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
const mod = require(path.join(repoRoot, 'fabric-testbed', 'caliper', 'workloads', 'e1_trace_replay.js'));

let now = 1_700_000_000_000;
const clock = () => now;
const sleeper = (ms) => { now += ms; return Promise.resolve(); };
const files = new Map();
const fsImpl = {
    mkdirSync() {},
    existsSync(p) { return files.has(p); },
    writeFileSync(p, data, opts) {
        if (opts && opts.flag === 'wx' && files.has(p)) { throw new Error(`EEXIST ${p}`); }
        files.set(p, String(data));
    },
    appendFileSync(p, data) { files.set(p, (files.get(p) || '') + String(data)); },
};
class FakeBase { async initializeWorkloadModule() {} }

(async () => {
    const out = { ok: true, error: null };
    try {
        const requests = [];
        const sut = { sendRequests: async (req) => { requests.push(req); } };
        const Workload = mod.build(FakeBase, clock, sleeper, fsImpl);
        const w = new Workload();
        const args = spec.arguments;
        await w.initializeWorkloadModule(0, spec.totalWorkers === undefined ? 1 : spec.totalWorkers, spec.roundIndex, args, sut, {});
        if (spec.mode !== 'validate') {
            const start = now;
            for (let i = 0; i < args.scheduleMs.length; i += 1) {
                if (spec.mode === 'controller') { now = start + args.scheduleMs[i]; }   // Caliper's replay-rate sleeps until the offset
                await w.submitTransaction();
            }
            await w.submitTransaction();                                                // one trailing callback: must NOT become a submission
        }
        out.requests = requests.map((r) => ({ fn: r.contractFunction, args: r.contractArguments, readOnly: r.readOnly,
                                              contractId: r.contractId, identity: r.invokerIdentity }));
        out.dispatchLog = files.get(args.dispatchLog) || null;
        out.overflowLog = files.get(`${args.dispatchLog}.overflow.csv`) || null;
    } catch (err) {
        out.ok = false;
        out.error = String(err && err.message ? err.message : err);
    }
    process.stdout.write(JSON.stringify(out));
})();
