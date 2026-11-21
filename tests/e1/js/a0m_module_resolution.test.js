'use strict';
// A0m: the seven modules an A0m dry render ships resolve from ONE directory, and removing any dependency breaks loading with MODULE_NOT_FOUND.
// SYNTHETIC and offline: the directory is a temporary copy of the repository sources; nothing runs Caliper, Fabric or a network.
const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawnSync } = require('child_process');

const REPO = path.resolve(__dirname, '../../..');
const SOURCES = {
    'e1_per_tx_recorder.js': 'scripts/e1/caliper', 'e1_per_tx_recorder_v2.js': 'scripts/e1/caliper', 'e1_per_tx_recorder_v3.js': 'scripts/e1/caliper',
    'e1_create_asset.js': 'fabric-testbed/caliper/workloads', 'e1_create_asset_issue_logged.js': 'fabric-testbed/caliper/workloads',
    'e1_issue_log.js': 'fabric-testbed/caliper/workloads', 'e1_workload_core.js': 'fabric-testbed/caliper/workloads',
};

function stage(except) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'a0m-mod-'));
    for (const [name, rel] of Object.entries(SOURCES)) {
        if (name !== except) { fs.copyFileSync(path.join(REPO, rel, name), path.join(dir, name)); }
    }
    return dir;
}

// each load runs in a fresh process so the module cache of this test process cannot hide a missing file
function load(dir, entry) {
    const code = `const m = require(${JSON.stringify(path.join(dir, entry))}); const loaded = Object.keys(require.cache).filter((f) => f.startsWith(${JSON.stringify(dir)})).map((f) => require('path').basename(f)).sort(); console.log(JSON.stringify({ keys: Object.keys(m).sort(), loaded }));`;
    return spawnSync(process.execPath, ['-e', code], { encoding: 'utf8' });
}

test('both entry modules load from the staged directory and pull in only their siblings', () => {
    const dir = stage(null);
    const rec = load(dir, 'e1_per_tx_recorder_v3.js');
    assert.strictEqual(rec.status, 0, rec.stderr);
    const r = JSON.parse(rec.stdout);
    assert.ok(r.keys.includes('createTxObserver'));
    assert.deepStrictEqual(r.loaded, ['e1_per_tx_recorder.js', 'e1_per_tx_recorder_v2.js', 'e1_per_tx_recorder_v3.js']);
    const wl = load(dir, 'e1_create_asset_issue_logged.js');
    assert.strictEqual(wl.status, 0, wl.stderr);
    const w = JSON.parse(wl.stdout);
    assert.ok(w.keys.includes('build') && w.keys.includes('createWorkloadModule'));
    assert.deepStrictEqual(w.loaded, ['e1_create_asset.js', 'e1_create_asset_issue_logged.js', 'e1_issue_log.js', 'e1_workload_core.js']);
});

test('removing a dependency makes the dependent module fail to load', () => {
    for (const [missing, entry] of [['e1_per_tx_recorder.js', 'e1_per_tx_recorder_v3.js'], ['e1_per_tx_recorder_v2.js', 'e1_per_tx_recorder_v3.js'],
        ['e1_issue_log.js', 'e1_create_asset_issue_logged.js'], ['e1_create_asset.js', 'e1_create_asset_issue_logged.js'],
        ['e1_workload_core.js', 'e1_create_asset_issue_logged.js']]) {
        const out = load(stage(missing), entry);
        assert.notStrictEqual(out.status, 0, `${entry} loaded without ${missing}`);
        assert.match(out.stderr, /MODULE_NOT_FOUND|Cannot find module/);
    }
});

test('the legacy workload loads without the issue-log modules (opt-in: it does not depend on them)', () => {
    const dir = stage('e1_issue_log.js');
    const out = load(dir, 'e1_create_asset.js');
    assert.strictEqual(out.status, 0, out.stderr);
    assert.deepStrictEqual(JSON.parse(out.stdout).loaded, ['e1_create_asset.js', 'e1_workload_core.js']);
});
