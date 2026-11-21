'use strict';
// A0m follow-up to the A0l issue-log tests (which are not edited). SYNTHETIC and offline.
// M44 (the A0l mutation "a worker id that does not match its index is accepted") survived because the successor workload's own validator rejects a
// mismatching worker id BEFORE the writer is built, so no A0l test reached the writer's own check. The writer is a public module; its identity rules
// must hold when it is used directly.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');

const log = require(path.resolve(__dirname, '../../../fabric-testbed/caliper/workloads/e1_issue_log.js'));

const base = (dir) => ({ path: path.join(dir, 'x.jsonl'), runId: 'r001', workerIndex: 0, workerId: 'w0', totalWorkers: 1, expectedWorkers: ['w0'],
    clockDomain: 'd', roundIndex: 1, tickIntervalMs: 1000 });
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'a0m-w-'));

test('the writer itself refuses a worker id that is not w<workerIndex>, even with a matching roster', () => {
    const dir = tmp();
    for (const [index, id] of [[0, 'w1'], [1, 'w0'], [2, 'w3']]) {
        assert.throws(() => new log.IssueLogWriter({ ...base(dir), workerIndex: index, workerId: id, expectedWorkers: [id] }), /worker_identity_mismatch/);
    }
    assert.ok(new log.IssueLogWriter(base(dir)));                                   // the matching identity is accepted
});

test('the writer itself refuses a roster that is not exactly its own worker, and a worker count other than 1', () => {
    const dir = tmp();
    assert.throws(() => new log.IssueLogWriter({ ...base(dir), expectedWorkers: ['w0', 'w1'] }), /worker_roster_mismatch/);
    assert.throws(() => new log.IssueLogWriter({ ...base(dir), expectedWorkers: ['w1'] }), /worker_roster_mismatch/);
    assert.throws(() => new log.IssueLogWriter({ ...base(dir), totalWorkers: 2 }), /worker_count_unsupported/);
    assert.deepEqual(fs.readdirSync(dir), []);                                      // nothing was created by a refused construction
});
