'use strict';
  
                                                                                                        
                                                                                                      
                                                                               
                                                                                                     
                                                                                                       
                                                                    
                                                                 
                                                                                                      
                                                                                                            
                                             
   
const core = require('./e1_workload_core.js');

function loadBase() {
    try {
        return require('@hyperledger/caliper-core').WorkloadModuleBase;
    } catch (err) {
        return require.main.require('@hyperledger/caliper-core').WorkloadModuleBase;
    }
}

function build(Base, sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))) {
    return class E1CreateAsset extends Base {
        constructor() {
            super();
            this.seq = 0;
        }

        async initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter, sutContext) {
            await super.initializeWorkloadModule(workerIndex, totalWorkers, roundIndex, roundArguments, sutAdapter,
                sutContext);
            core.validateArgs(roundArguments);
            if (totalWorkers !== 1) {
                throw new Error(`e1 workload: exactly 1 worker required, got ${totalWorkers}`);
            }
            this.seq = 0;
            const settle = roundArguments.settleSeconds || 0;
            if (settle > 0) {
                await sleep(settle * 1000);
            }
        }

        async submitTransaction() {
            const request = core.requestFor(this.roundArguments, this.roundIndex, this.seq);
            this.seq += 1;
            await this.sutAdapter.sendRequests(request);
        }
    };
}

function createWorkloadModule() {
    const E1CreateAsset = build(loadBase());
    return new E1CreateAsset();
}

module.exports = { createWorkloadModule, build };
