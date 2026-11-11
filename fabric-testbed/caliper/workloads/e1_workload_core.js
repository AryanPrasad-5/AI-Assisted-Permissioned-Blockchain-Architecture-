'use strict';
  
                                                                                                            
                                                                                                        
                                                                                                       
                                                                           
                                                                                                       
                                                       
   
const KEY_RE = /^[A-Za-z0-9._-]+$/;
const MAX_KEY = 128;
const MAX_VALUE = 4096;
const MAX_SEQ = 9999999;
const MODES = new Set(['create', 'fail-every', 'rmw']);

function fail(msg) { throw new Error(`e1 workload: ${msg}`); }

function validateArgs(a) {
    if (!a || typeof a !== 'object') { fail('round arguments missing'); }
    if (typeof a.runId !== 'string' || !/^[A-Za-z0-9_.-]{1,40}$/.test(a.runId)) { fail(`bad runId ${a.runId}`); }
    if (typeof a.contractId !== 'string' || !a.contractId) { fail('contractId missing'); }
    if (typeof a.invokerIdentity !== 'string' || !a.invokerIdentity) { fail('invokerIdentity missing'); }
    if (!Number.isInteger(a.valueBytes) || a.valueBytes < 1 || a.valueBytes > MAX_VALUE) {
        fail(`valueBytes ${a.valueBytes} outside 1..${MAX_VALUE}`);
    }
    if (!MODES.has(a.mode)) { fail(`mode ${a.mode} not in ${[...MODES]}`); }
    if (a.mode === 'fail-every' && (!Number.isInteger(a.failEvery) || a.failEvery < 2)) { fail('failEvery >= 2'); }
    if (a.mode === 'rmw' && (typeof a.rmwKey !== 'string' || !KEY_RE.test(a.rmwKey))) { fail('bad rmwKey'); }
    if (a.settleSeconds !== undefined && (!Number.isFinite(a.settleSeconds) || a.settleSeconds < 0)) {
        fail('settleSeconds >= 0');
    }
    return a;
}

function keyFor(runId, roundIndex, seq) {
    if (!Number.isInteger(roundIndex) || roundIndex < 0) { fail(`roundIndex ${roundIndex}`); }
    if (!Number.isInteger(seq) || seq < 0 || seq > MAX_SEQ) { fail(`seq ${seq} outside 0..${MAX_SEQ}`); }
    const key = `${runId}_${roundIndex}_${String(seq).padStart(7, '0')}`;
    if (!KEY_RE.test(key) || key.length > MAX_KEY) { fail(`key ${key} violates the chaincode key rule`); }
    return key;
}

function valueOf(bytes) { return 'v'.repeat(bytes); }

                                                                                  
function requestFor(a, roundIndex, seq) {
    const base = { contractId: a.contractId, invokerIdentity: a.invokerIdentity, readOnly: false };
    if (a.mode === 'rmw') {
        return { ...base, contractFunction: 'ReadModifyWrite', contractArguments: [a.rmwKey] };
    }
    if (a.mode === 'fail-every' && (seq + 1) % a.failEvery === 0) {
        return { ...base, contractFunction: 'FailAlways', contractArguments: [] };
    }
    return { ...base, contractFunction: 'CreateAsset',
        contractArguments: [keyFor(a.runId, roundIndex, seq), valueOf(a.valueBytes)] };
}

module.exports = { validateArgs, keyFor, valueOf, requestFor, MAX_KEY, MAX_VALUE };
