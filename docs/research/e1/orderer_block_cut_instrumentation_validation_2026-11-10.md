# Orderer block-cut instrumentation validation — 2026-11-10

## Final verdict

**INSTRUMENTATION VALIDATED — READY FOR FINAL COMPARISON**

Fabric v2.5.16 emitted direct, source-defined block-cut reason events when the
two relevant orderer loggers were enabled at runtime. A bounded high-rate run
produced a direct `MESSAGE_COUNT` event followed by a 45-transaction block. A
bounded low-rate run produced direct `TIMEOUT` events followed by 21-, 20-,
and 19-transaction blocks. The original logging state was restored and both
channels remained unchanged.

This validates the instrumentation method for the final controlled comparison.
It does not claim that the two bounded runs are the final performance result,
and it does not replace the required repeated benchmark protocol.

## Scope and preservation

The validation used the existing Fabric environment only:

- Fabric image: `hyperledger/fabric-orderer:2.5.16`
- Fabric source revision: `f871cf92a026aba7b12e6f06d71ded3e6e659d71`
- Channels: `e1-r002` (`MaxMessageCount=45`) and `e1-cfg001`
  (`MaxMessageCount=40`)
- Both channels: `BatchTimeout=2s`, `PreferredMaxBytes=10485760`,
  `AbsoluteMaxBytes=103809024`
- Three Raft orderers, three active nodes per channel

No channel configuration update, rollback, orderer restart, Docker reset,
volume operation, chaincode change, or final benchmark was performed.

The pre-change environment snapshot is recorded in
`orderer_block_cut_instrumentation_original_2026-11-10.json`, with SHA-256
recorded in the adjacent `.sha256` file. The compose-file hash in that record
was unchanged by this validation.

## 1. Logging change and readiness

The running orderers initially reported `{"spec":"info"}` at their Operations
Service `/logspec` endpoints. The image, version, health, leadership and
three-node quorum were checked before the change. The containers had been
running since approximately `2026-11-10T15:28:59Z` with restart count `0`.

The temporary runtime-only specification was applied independently to ports
9443, 9446 and 9447:

```text
info:orderer.common.blockcutter,orderer.consensus.etcdraft=debug
```

Each `PUT /logspec` returned HTTP 204. A subsequent `GET /logspec` on all
three orderers returned the normalized equivalent:

```text
orderer.common.blockcutter=debug:orderer.consensus.etcdraft=debug:info
```

This used the Fabric Operations Service and did not require a restart. The
compose environment remained `FABRIC_LOGGING_SPEC=INFO`; the debug setting was
therefore temporary and non-persistent.

## 2. Bounded validation runs

Both runs used the existing read-only channel pilot runner, unique run IDs,
exactly 60 transactions, the existing workload and Caliper container, and
captured orderer logs with `e1_log_collector.py`.

| Run | Channel | B | Target TPS | Observed offered TPS | Delivered TPS | Valid / submitted | Mean latency | p95 latency | Max backlog | Ledger blocks |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| `iv-count-r002` | `e1-r002` | 45 | 50 | 50.977 | 20.107 | 60 / 60 | 838.267 ms | 1978 ms | 47 | 21:45, 22:15 |
| `iv-timeout-c001` | `e1-cfg001` | 40 | 10 | 10.176 | 9.671 | 60 / 60 | 1091.233 ms | 1964 ms | 21 | 16:21, 17:20, 18:19 |

Both run records report `COMPLETE`, analysis verdict `VALID`, zero failures,
zero duplicates and zero pending transactions. The runner fetched the channel
configuration before and after each run:

| Run | Height before → after | MaxMessageCount before → after | Other configuration changes |
|---|---:|---:|---|
| `iv-count-r002` | 21 → 23 | 45 → 45 | none; timeout and byte limits unchanged |
| `iv-timeout-c001` | 16 → 19 | 40 → 40 | none; timeout and byte limits unchanged |

Evidence records:

- `/home/mailf18/e1/pilot-runs/iv-count-r002/pilot_run_record.json`
- `/home/mailf18/e1/pilot-runs/iv-count-r002/ledger/ledger/blocks.csv`
- `/home/mailf18/e1/pilot-runs/iv-count-r002/logs/`
- `/home/mailf18/e1/pilot-runs/iv-timeout-c001/pilot_run_record.json`
- `/home/mailf18/e1/pilot-runs/iv-timeout-c001/ledger/ledger/blocks.csv`
- `/home/mailf18/e1/pilot-runs/iv-timeout-c001/logs/`

These are WSL paths used by the runner. The collector manifests in each log
directory preserve the capture windows and hashes.

## 3. Direct cut-reason evidence

The logger names and messages are source-compatible with Fabric v2.5.16:

- `orderer.common.blockcutter`: `Batch size met, cutting batch`
- `orderer.consensus.etcdraft`: `Batch timer expired, creating block`

### Confirmed message-count cut

In `iv-count-r002`, the leader was `orderer2.example.com` for `e1-r002`.
The raw log contains:

```text
.../iv-count-r002/logs/orderer2.example.com.stderr.log:314
2026-11-10T17:43:58.164884947Z ... [orderer.common.blockcutter] ... -> Batch size met, cutting batch

.../iv-count-r002/logs/orderer2.example.com.stderr.log:315
2026-11-10T17:43:58.165198972Z ... [orderer.consensus.etcdraft] ... -> Created block [21], there are 0 blocks in flight channel=e1-r002 node=2
```

The independently fetched ledger record for block 21 contains exactly 45
valid workload transactions. The next block-cutter event is a timer event and
block 22 contains the remaining 15 transactions:

```text
.../iv-count-r002/logs/orderer2.example.com.stderr.log:409
2026-11-10T17:44:00.190285685Z ... [orderer.consensus.etcdraft] ... -> Batch timer expired, creating block channel=e1-r002 node=2

.../iv-count-r002/logs/orderer2.example.com.stderr.log:410
2026-11-10T17:44:00.190312508Z ... [orderer.consensus.etcdraft] ... -> Created block [22], there are 0 blocks in flight channel=e1-r002 node=2
```

The `MESSAGE_COUNT` classification is therefore direct at the blockcutter
event and unambiguous for block 21 in this isolated run: the event is followed
immediately by the same-channel leader's block-creation event and the ledger
block has exactly the configured count. The reason log itself does not carry a
block number, so the association is a validated temporal/logical join rather
than a single record containing both fields.

### Confirmed timeout cuts

In `iv-timeout-c001`, the leader was `orderer.example.com` for `e1-cfg001`.
The raw log contains three direct timer events, each followed immediately by a
same-channel block-creation event:

| Raw log lines | Timer event | Created block | Ledger transaction count |
|---|---|---:|---:|
| 178, 181 | 17:46:08.875218956Z | 16 | 21 |
| 282, 283 | 17:46:10.942455124Z | 17 | 20 |
| 385, 386 | 17:46:12.945714805Z | 18 | 19 |

The source log is
`/home/mailf18/e1/pilot-runs/iv-timeout-c001/logs/orderer.example.com.stderr.log`.
No count-trigger event appears in this bounded run. Since the offered rate
was approximately 10 TPS and each batch remained well below 40 transactions,
the direct timer event and immediate block join provide the confirmed
`TIMEOUT` evidence requested for this validation.

## 4. Validation of the instrumentation method

The method passed the required checks:

| Check | Result |
|---|---|
| Direct message-count event captured | PASS — `Batch size met` |
| Direct timeout event captured | PASS — `Batch timer expired` |
| Event includes channel and orderer context | PASS for the timer event; count event joined to the same leader/channel block event |
| Block lifecycle event captured | PASS — `Created block` and `Writing block` |
| Independent block transaction count | PASS — fetched blocks CSV |
| Configuration stable before/after run | PASS for both channels |
| All validation transactions valid | PASS — 120/120 valid across the two bounded runs |
| Three-node Raft quorum maintained | PASS — active nodes 3, cluster size 3 |
| Logging state restored | PASS — all three `/logspec` endpoints report `info` |

The count run also demonstrates that a single run can contain different cut
reasons: the full 45-transaction block is followed by a timeout-cut tail
block. This is why transaction counts alone were insufficient in the earlier
pilot and why direct reason events must be captured for the final comparison.

## 5. Post-validation restoration and health

The original logging state was restored with `PUT /logspec` body
`{"spec":"info"}` on all three orderers. Each request returned HTTP 204. At
`2026-11-10T17:48:11.401952300Z`:

- port 9443: `{"spec":"info"}`, health HTTP 200, Fabric v2.5.16;
- port 9446: `{"spec":"info"}`, health HTTP 200, Fabric v2.5.16;
- port 9447: `{"spec":"info"}`, health HTTP 200, Fabric v2.5.16.

Post-restore Prometheus checks on all three endpoints reported, for both
channels, `active_nodes=3` and `cluster_size=3`. The observed leaders remained
`orderer.example.com` for `e1-cfg001` and `orderer2.example.com` for
`e1-r002`; `orderer3.example.com` remained a follower. The committed block
gauges were 18 for `e1-cfg001` and 22 for `e1-r002`, matching the validation
records' last fetched block numbers.

## 6. Timestamp and operational limitations

- Fabric application timestamps in the log lines are displayed to millisecond
  precision. The Docker capture prefix has finer formatting, but it is a log
  transport/capture timestamp and must not be treated as nanosecond event
  precision.
- The direct reason and block-creation lines were correlated on the same
  orderer and channel. Cross-orderer line arrival times are not used to infer
  consensus timing.
- No separate clock-offset/uncertainty handshake was needed for the local
  same-orderer joins, but the final benchmark should retain a clock-check
  artifact if it compares events across hosts.
- Targeted DEBUG logging increases log volume. The two captures were finite
  and small (approximately 128–152 KB per orderer stderr file); CPU, disk-I/O
  and latency overhead were not separately benchmarked. The final comparison
  must use the same logging procedure for every configuration arm and preserve
  the raw logs.
- The reason messages do not contain the final block number. The validated join
  is reliable for these isolated runs, but a reusable parser should reject
  ambiguous joins caused by interleaved same-channel activity, missing log
  lines or leader changes.
- Prometheus blockcutter histograms remain aggregate process-lifetime metrics;
  they are useful as cross-checks but do not independently identify a cut
  reason or block number.

## 7. Reproducible procedure for the final comparison

For each final run:

1. Record the active `/logspec`, channel configuration, Fabric version, image
   digest, leadership and quorum.
2. Apply the same temporary module-specific DEBUG specification to all three
   orderers through `/logspec`.
3. Run one finite benchmark arm and capture all three raw orderer logs over the
   exact run window.
4. Fetch the resulting blocks and preserve the transaction-count CSV,
   transaction measurements, run record, capture manifest and raw-log hashes.
5. Join direct reason events to same-channel block lifecycle events. Emit
   `UNKNOWN` for missing or ambiguous joins; do not infer a reason from count
   alone.
6. Restore `{"spec":"info"}` and verify health, version, quorum, leadership
   and unchanged channel configuration before the next approved arm.

The final five-repetition comparison has not been started.

## 8. Raw evidence checksums

The capture manifests contain the complete file inventories. Key SHA-256
values are listed here to make the direct evidence auditable:

| Artifact | SHA-256 |
|---|---|
| `iv-count-r002` orderer leader stderr log | `23e32c4dc3fecc5313090daad2d97d571a2a2559c3e9d462dc702f14a1e9fcd2` |
| `iv-count-r002` block distribution CSV | `8113a9a1750accea6590f50467874576bacb2d9d1ffac5526d192bdbd190365b` |
| `iv-count-r002` pilot record | `3c858059c453d459d5370f5b17a1289d965f7d3dd25c356ea306cc1c34b55a84` |
| `iv-timeout-c001` orderer leader stderr log | `5a37ab9a5bff0ff56f153d53e9ca50df7579186368847a90fcb9e5bc4dfe2b15` |
| `iv-timeout-c001` block distribution CSV | `5ec30ccf7d1be6c7ae8931c077823a63a8e8ca457f2bb42c29008adf01603d2c` |
| `iv-timeout-c001` pilot record | `7ad579d792887c50915d31a114f90379c56dd6b4fe4460cbd9df29d515142c39` |

These validation artifacts are additive. Existing pilot, baseline, channel,
chaincode and dataset evidence was not overwritten.
