'use strict';
// PROPOSED, NOT RUN. Confirm the interface against the pinned Caliper before use.
// Writes one row per finished transaction at the end of the round.
const fs = require('fs');
const { TxObserverInterface } = require('@hyperledger/caliper-core/lib/worker/tx-observers/tx-observer-interface');

class PerTxObserver extends TxObserverInterface {
    constructor(options, messenger, workerIndex) {
        super(messenger, workerIndex);
        this.outputPath = options.outputPath;
        this.rows = [];
    }

    async activate(roundIndex, roundLabel) {
        await super.activate(roundIndex, roundLabel);
        this.rows = ['create_ms,final_ms,status'];
    }

    txSubmitted(_count) {}

    txFinished(results) {
        for (const result of Array.isArray(results) ? results : [results]) {
            this.rows.push(`${result.GetTimeCreate()},${result.GetTimeFinal()},${result.GetStatus()}`);
        }
    }

    async deactivate() {
        await super.deactivate();
        fs.writeFileSync(this.outputPath, `${this.rows.join('\n')}\n`);
    }
}

function createTxObserver(options, messenger, workerIndex) {
    return new PerTxObserver(options, messenger, workerIndex);
}

module.exports.createTxObserver = createTxObserver;
