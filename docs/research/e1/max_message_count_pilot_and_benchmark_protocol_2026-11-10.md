# MaxMessageCount pilot and benchmark protocol — 2026-11-10

## Final verdict

**PILOT INCONCLUSIVE — INSTRUMENTATION REQUIRED**

The bounded pilot completed successfully for both verified channels and all
five offered-rate steps. The results are sufficient to design the controlled
comparison, but not sufficient to make a definitive causal claim about the
orderer block-cutter reason because the pilot did not capture a direct
block-cutter event/reason or an orderer-side block timestamp for each block.

No channel configuration was changed, no rollback was performed, and the
final five-repetition comparison was not started.

## Scope and implementation

The existing Caliper workload was inspected and used unchanged:

- Caliper `0.6.0`, Fabric binding `fabric:2.5`;
- one worker, `CreateAsset`, 128-byte value;
- deterministic unique keys derived from run ID, round and sequence;
- exactly 60 measured transactions per arm;
- fixed offered-rate schedule: 10, 20, 30, 40 and 50 TPS;
- `BatchTimeout=2s`, `PreferredMaxBytes=10,485,760`, same peer/orderer topology;
- no warm-up round, matching the completed B=45 arm.

The standard orchestrator was not used because it creates a fresh channel. A
bounded existing-channel runner was used instead. It performs read-only
configuration and height checks, runs only the finite Caliper workload, fetches
the new ledger range, and analyzes the raw recorder and ledger artifacts. It
does not invoke channel creation, genesis generation, orderer joining, peer
joining or chaincode lifecycle operations.

Added/used implementation:

- [existing-channel pilot runner](C:/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/scripts/e1/e1_existing_channel_pilot.py)
- [existing-channel renderer override](C:/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/scripts/e1/e1_la2_render.py)
- [benchmark analyzer](C:/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/scripts/e1/e1_benchmark_analyze.py)
- [ledger/block parser](C:/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/scripts/e1/e1_ledger_blocks.py)

An initial pilot attempt using the stale container name `caliper` failed before
submission. It was corrected to the operational `caliper-benchmark-001`
container; that failed attempt produced no workload transactions. The actual
10-TPS B=45 arm is `p10r002b`, not the failed `p10r002` attempt.

## Run-level results

All completed runs had 60 submitted, 60 finished, 60 VALID ledger transactions,
zero failed transactions, zero invalid transactions, zero duplicates and zero
pending transactions at deactivation.

| Run | Channel | B | Target TPS | Observed offered TPS | VALID TPS | Mean ms | p95 ms | Max backlog | Before→after height |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| p10r002b | e1-r002 | 45 | 10 | 10.17 | 9.52 | 1088.4 | 1966 | 21 | 10→13 |
| p20r002 | e1-r002 | 45 | 20 | 20.35 | 14.44 | 1230.9 | 1982 | 42 | 13→15 |
| p30r002 | e1-r002 | 45 | 30 | 30.53 | 16.78 | 1027.7 | 1938 | 45 | 15→17 |
| p40r002 | e1-r002 | 45 | 40 | 40.73 | 18.87 | 904.2 | 1962 | 46 | 17→19 |
| p50r002 | e1-r002 | 45 | 50 | 50.89 | 20.10 | 839.4 | 1973 | 47 | 19→21 |
| p10c001 | e1-cfg001 | 40 | 10 | 10.17 | 9.52 | 1086.7 | 1966 | 21 | 5→8 |
| p20c001 | e1-cfg001 | 40 | 20 | 20.35 | 14.80 | 1190.7 | 1931 | 40 | 8→10 |
| p30c001 | e1-cfg001 | 40 | 30 | 30.55 | 17.27 | 1043.2 | 1942 | 41 | 10→12 |
| p40c001 | e1-cfg001 | 40 | 40 | 40.73 | 19.47 | 960.4 | 1977 | 41 | 12→14 |
| p50c001 | e1-cfg001 | 40 | 50 | 50.93 | 20.80 | 902.7 | 1975 | 41 | 14→16 |

The B=40 and B=45 channels remained unchanged before and after every arm:

| Channel | MaxMessageCount | BatchTimeout | Preferred bytes | Absolute bytes | Orderers |
|---|---:|---|---:|---:|---:|
| e1-r002 | 45 | 2s | 10,485,760 | 103,809,024 | 3 |
| e1-cfg001 | 40 | 2s | 10,485,760 | 103,809,024 | 3 |

The recorded policy facts, Orderer group version, BatchSize version and other
configuration fields also matched before and after each arm. Configuration
fetch/decode commands exited successfully for all ten runs.

Per-run configuration verification is preserved in each run's `before_config.json` and `after_config.json`; all ten before/after comparisons were identical.

## Block-level evidence

The following table reports each measured block’s transaction count and the
repository parser’s inferred cause. The timestamps are the first and last
transaction `ch_ts_ms` values in the block; the span is therefore a
transaction-timestamp proxy, not an orderer block-creation timestamp.

| Run | Blocks: count / inferred cause | First→last transaction timestamp span (ms) |
|---|---|---|
| p10r002b | 21 timer; 21 timer; 18 timer | 1997; 1999; 1700 |
| p20r002 | 41 timer; 19 timer | 1998; 900 |
| p30r002 | 45 count; 15 timer | 1466; 463 |
| p40r002 | 45 count; 15 timer | 1098; 349 |
| p50r002 | 45 count; 15 timer | 878; 281 |
| p10c001 | 21 timer; 21 timer; 18 timer | 1996; 1999; 1701 |
| p20c001 | 40 count; 20 timer | 1947; 949 |
| p30c001 | 40 count; 20 timer | 1299; 631 |
| p40c001 | 40 count; 20 timer | 973; 474 |
| p50c001 | 40 count; 20 timer | 779; 379 |

The parser’s classification is defined in `e1_reconcile.infer_cut_causes`:
an exact B-sized block is labeled `count`; otherwise, with no byte overflow,
it is labeled `timer`. This is a deterministic inference from ledger contents
and envelope sizes, not an observed orderer event.

### Paired behavior

- At 10 TPS, both channels produced the same `21,21,18` pattern. The 2-second
  timer was reached before either count threshold.
- At 20 TPS, B=40 produced `40,20`, while B=45 produced `41,19`. This is the
  first rate where the lower threshold was reached in the observed ledger.
- At 30, 40 and 50 TPS, B=40 consistently produced `40,20`; B=45 consistently
  produced `45,15`.
- The paired count patterns are consistent with MaxMessageCount controlling
  the first block once enough transactions arrive before the timeout.
- The evidence is not sufficient to claim direct causal attribution because no
  block-cutter reason metric/event and no orderer-side block timestamp was
  captured. The `count`/`timer` labels must therefore remain “inferred”.

## Workload saturation assessment

The workload was not capacity-neutral at the upper rates. At 50 TPS offered,
the observed delivered rate was approximately 20.10 TPS for B=45 and 20.80 TPS
for B=40, with maximum in-flight backlog 47 and 41 respectively. Thus the
pilot successfully exercised the message-count thresholds, but it also entered
a saturated/backlogged regime. The pilot is suitable for selecting candidate
rates, not for estimating final performance superiority.

## Validity and safety checks

- 10 completed arms; 5 per channel.
- 600 submitted and 600 committed VALID workload transactions overall.
- 0 failed, invalid, duplicate or foreign workload transactions in completed
  arms.
- Unique asset IDs were generated from each run ID.
- Pre/post active configuration matched the expected channel configuration for
  every run.
- No configuration update, rollback, channel recreation, Docker reset or
  volume deletion was performed.
- Raw recorder, ledger, block and pilot records were preserved under:
  `/home/mailf18/e1/pilot-runs/<run-id>/` and
  `/home/mailf18/e1/caliper-ws-benchmark/runs/<run-id>/`.

## Instrumentation limitations and required additions

The current evidence does not directly record:

1. orderer block-cutter reason events;
2. orderer-side block creation timestamps per block;
3. a direct block-fill-duration field from the orderer;
4. a live backlog gauge independent of Caliper submit/finish events;
5. orderer telemetry correlated to each measured block.

Before the final comparison, add an orderer-side block observer or equivalent
telemetry correlation that records block number, creation time, transaction
count, and cut reason. Keep the ledger-derived count classification as a
cross-check, not as the primary causal evidence.

## Recommended final comparison protocol

Do not begin it from this report. After instrumentation is added, preregister:

- paired channels `e1-r002` and `e1-cfg001`;
- rates 20, 30, 40 and 50 TPS as the primary threshold-exercising cells;
- 10 TPS as an optional timeout-only control;
- 10-second warm-up, then a bounded 120-transaction measurement window;
- a 60-second hard timeout and a drain timeout;
- stop if backlog exceeds `2 × B`, failure/invalid rate exceeds 1%, or the
  Caliper worker reports an error;
- at least five repetitions per channel/rate, alternating channel order;
- identical workload modules, transaction mix, worker count, Docker resources
  and telemetry settings;
- configuration verification before and after each run;
- paired medians/IQR, per-run differences, percentage changes and bootstrap
  intervals where justified.

The 120-transaction cap is intentionally finite and should be validated with
the new backlog guard before the final experiment. No superiority claim should
be made from the pilot means or from the inferred cut labels.

The consolidated read-only post-run audit, including exact first/last transaction timestamps for each fetched block, is `/home/mailf18/e1/pilot-audit-2026-11-10/summary.json`.

## Evidence index

Each run’s complete record is in:

`/home/mailf18/e1/pilot-runs/<run-id>/pilot_run_record.json`

Each run contains `before_config.json`, `after_config.json`, recorder/Caliper
artifacts, fetched raw blocks, `ledger/ledger_tx.csv`,
`ledger/blocks.csv`, and `analysis/summary.json`. The renderer plans are in:

`/home/mailf18/e1/pilot-plans/`

The failed pre-workload container-name attempt is retained at
`/home/mailf18/e1/pilot-runs/p10r002-retry/`; it is not counted as a completed
pilot arm. The completed 10-TPS B=45 evidence is `p10r002b`.
