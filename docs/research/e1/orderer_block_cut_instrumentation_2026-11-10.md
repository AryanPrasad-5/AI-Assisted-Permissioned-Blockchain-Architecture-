# Orderer block-cut instrumentation audit — 2026-11-10

## Final verdict

**INSTRUMENTATION REQUIRES APPROVED ORDERER CHANGE**

Fabric 2.5.16 contains direct debug log statements that distinguish
message-count and timeout cuts, but the running orderers use
`FABRIC_LOGGING_SPEC=INFO`. Prometheus exposes block-fill duration, but not a
block number or cut-reason label. The current read-only telemetry therefore
supports only inferred classifications. No workload was executed, no orderer
was restarted, and neither channel was modified during this audit.

## 1. Deployed version and topology

Read-only inspection found three running orderers:

| Orderer | Image | Operations endpoint | Observed leader at audit time |
|---|---|---|---|
| `orderer.example.com` | `hyperledger/fabric-orderer:2.5.16` | `http://127.0.0.1:9443/metrics` | `e1-cfg001` |
| `orderer2.example.com` | `hyperledger/fabric-orderer:2.5.16` | `http://127.0.0.1:9446/metrics` | `e1-r002` |
| `orderer3.example.com` | `hyperledger/fabric-orderer:2.5.16` | `http://127.0.0.1:9447/metrics` | none at snapshot |

All three endpoints reported `fabric_version{version="v2.5.16"} 1`, and both
channels reported three active nodes and a three-node Raft cluster. The compose
overlay pins the orderer image to 2.5.16, enables Prometheus, publishes the
three operations ports, and sets INFO logging. The image metadata identifies
source revision `f871cf92a026aba7b12e6f06d71ded3e6e659d71`, the v2.5.16 release
revision.

## 2. Version-pinned blockcutter findings

The inspected Fabric source confirms:

- the pending-batch clock starts when the first message enters an empty batch;
- preferred-byte overflow cuts before adding the overflowing message;
- `MaxMessageCount` cuts after the pending count reaches the threshold;
- `Cut()` observes fill duration and resets the pending batch;
- the Raft path starts/stops the channel `BatchTimeout` timer and calls
  `BlockCutter().Cut()` when that timer fires.

The exact source is [Fabric v2.5.16 blockcutter](https://github.com/hyperledger/fabric/blob/f871cf92a026aba7b12e6f06d71ded3e6e659d71/orderer/common/blockcutter/blockcutter.go#L48-L129),
[Raft timer setup](https://github.com/hyperledger/fabric/blob/f871cf92a026aba7b12e6f06d71ded3e6e659d71/orderer/consensus/etcdraft/chain.go#L569-L588),
and [timer-triggered cut](https://github.com/hyperledger/fabric/blob/f871cf92a026aba7b12e6f06d71ded3e6e659d71/orderer/consensus/etcdraft/chain.go#L778-L788).

The source’s reason-bearing debug events map as follows:

| Source event | Proposed reason |
|---|---|
| `Batch size met, cutting batch` | `MESSAGE_COUNT` |
| `Batch timer expired, creating block` | `TIMEOUT` |
| preferred-byte overflow cut message | `PREFERRED_BYTES` |
| oversized-message isolation path | `OVERSIZED_MESSAGE` |

These meanings are source-confirmed, but not runtime-confirmed in the current
deployment because DEBUG output is suppressed.

## 3. Live logs and current metrics

INFO logs do expose `Created block [n] ... channel=... node=...` and
`Writing block [n] ... to ledger` events. They provide channel, orderer node
and block number, but not the cut reason or per-block fill duration. A
read-only scan of all three current logs found no `Batch size met`, `Batch
timer expired`, or byte-overflow reason messages. This is expected at INFO and
does not show that those paths were unused.

The exact v2.5.16 metrics reference documents
`blockcutter_block_fill_duration` as a channel-labelled histogram in seconds,
plus Raft leader/cluster/block gauges and broadcast counters. See the
[v2.5.16 metrics reference](https://github.com/hyperledger/fabric/blob/f871cf92a026aba7b12e6f06d71ded3e6e659d71/docs/source/metrics_reference.rst#L9-L33)
and [blockcutter metric definition](https://github.com/hyperledger/fabric/blob/f871cf92a026aba7b12e6f06d71ded3e6e659d71/orderer/common/blockcutter/metrics.go#L10-L24).

Observed cumulative leader metrics at the audit snapshot:

| Leader endpoint | Channel | Fill count | Fill sum (s) |
|---|---|---:|---:|
| 9443 / `orderer.example.com` | `e1-cfg001` | 14 | 24.908978669 |
| 9446 / `orderer2.example.com` | `e1-r002` | 20 | 37.383591889 |

These are process-lifetime counters, not pilot-only values. The histogram is
useful for per-run aggregate deltas but has no block number, cut reason,
orderer identity, or event timestamp. It cannot distinguish timeout,
message-count, and byte-overflow cuts by itself.

The existing [E1 metrics scraper](C:/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/scripts/e1/e1_metrics_scraper.py)
already preserves raw Prometheus bodies, host wall/monotonic scrape times,
histogram deltas, leader identity, and the orderer reporting blockcutter
activity. It is suitable as the metrics component of the proposed method.

## 4. Evidence classification

### Confirmed

- Fabric 2.5.16 is running on all three orderers.
- Prometheus is enabled and reachable on 9443, 9446 and 9447.
- Both target channels have three active Raft nodes at the sampled instant.
- Leader-specific blockcutter metrics exist for both target channels.
- The source has distinct count and timer cut paths.
- INFO logs expose orderer-side block creation and ledger-write events.
- No direct cut-reason event is exposed by the current runtime configuration.

### Inferred only

- An exactly B-sized block was cut by `MaxMessageCount`.
- A fill duration near 2 seconds was cut by `BatchTimeout`.
- A metric delta maps one-to-one to a particular block.
- A Fabric ChannelHeader timestamp is an orderer block-creation timestamp.

The pilot’s ledger-derived `count`/`timer` labels remain inferred cross-checks;
this audit does not upgrade them to confirmed causes.

## 5. Proposed instrumentation architecture

The preferred first implementation is an approved operational logging change,
not a source fork:

1. Capture all three orderer logs with application timestamps and Docker
   capture timestamps.
2. Scrape all three operations endpoints at a fixed interval and preserve raw
   bodies, endpoint, container, image digest, and host wall/monotonic times.
3. Parse the version-pinned DEBUG reason events by `(orderer, channel)` and
   parse the INFO `Created block` and `Writing block` events.
4. Fetch the resulting block and join by channel and orderer block number.
5. Mark a reason `CONFIRMED` only when the direct reason event and block
   lifecycle join are unambiguous. Otherwise emit `UNKNOWN`.
6. Use histogram deltas only as an aggregate fill-duration cross-check; do not
   fabricate per-block durations from cumulative samples.

Each event record should include: event ID, channel ID, orderer identity, block
number, reason, reason/created/written timestamps, transaction count, all three
BatchSize thresholds, BatchTimeout, event source, timestamp precision and
confidence. The reason vocabulary is `MESSAGE_COUNT`, `TIMEOUT`,
`PREFERRED_BYTES`, `OVERSIZED_MESSAGE`, and `UNKNOWN`.

## 6. Required code changes after approval

No code was changed in the running deployment. The repository implementation
should add:

- a read-only orderer-log parser preserving raw lines and parse failures;
- a join/validation module rejecting ambiguous joins, channel mismatches,
  duplicate event IDs, missing block numbers and impossible event ordering;
- tests for count, timeout, byte-overflow, interleaved channels, leader
  changes, duplicate/missing log lines and cumulative metric resets;
- a run manifest recording image digest, source revision, compose hash, logging
  level, scrape interval, clock-check result and channel thresholds.

The existing metrics scraper can be reused. A source-level instrumentation fork
is not required if DEBUG logs are accepted, but would be a separate approved
change with a separate reproducibility plan.

## 7. Bounded validation plan

This plan was not executed. After the logging change and capture process are
approved:

1. Record channel configuration and leadership before each bounded run.
2. Use a finite low-rate control where B cannot be reached before 2 seconds;
   require direct `TIMEOUT` events.
3. Use a finite higher-rate control where B is reached before 2 seconds;
   require direct `MESSAGE_COUNT` events.
4. Collect logs, metrics, blocks and transaction timestamps, and reject any
   missing or ambiguous join as `UNKNOWN`.
5. Record natural leader changes if they occur; do not force failover.

The completed ten-arm pilot is not to be rerun as part of this audit.

## 8. Timestamp, clock and operational limitations

- Orderer application log timestamps are process/container clock time.
- Docker log timestamps are daemon/capture time and may differ from them.
- Prometheus scrape timestamps are collector host time.
- ChannelHeader timestamps are client transaction timestamps, not orderer block
  creation timestamps.
- A clock-handshake artifact with offset and uncertainty is required before a
  validation run. Millisecond log precision must not be reported as
  sub-millisecond cut precision.
- Enabling DEBUG increases log volume and may perturb a high-rate benchmark.
- Changing `FABRIC_LOGGING_SPEC` requires an orderer service change/restart;
  it is therefore not performed here.

## Preservation and status

- No MaxMessageCount, BatchTimeout or byte limit changed.
- No channel update or rollback was submitted.
- No orderer, peer, Docker or Caliper service was restarted.
- No workload or completed pilot arm was rerun.
- No Docker volume, chaincode, dataset or prior evidence was modified.

Status: **DESIGNED BUT NOT EXECUTED** for the proposed instrumentation and
validation. The current environment is **VERIFIED INCOMPLETE** for direct cut
reason capture until the required orderer logging change is explicitly
approved.
