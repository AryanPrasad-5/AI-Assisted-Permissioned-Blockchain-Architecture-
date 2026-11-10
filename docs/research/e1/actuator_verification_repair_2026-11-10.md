# E1 actuator verification repair — 2026-11-10

## Verdict

**VERIFICATION REPAIR COMPLETE**

The verifier was repaired and regression-tested. The existing committed update
was not resubmitted, rolled back, or otherwise changed. The repaired logic
accepts the observed `e1-cfg001` transition and rejects unexpected version,
value, policy, stale-state, target, and incomplete-evidence cases.

## Root cause

The original `execute_prepared` path compared a reduced policy dictionary as if
it had to remain byte-for-byte identical. That dictionary included the
`BatchSize` leaf version, so the legitimate `0 -> 1` version increment was
classified as an unrelated policy change. The committed block itself was
correct; the actuator record was the false negative.

The repair does not remove version checking. It makes the expected transition
explicit:

- `common.Config.sequence`: exactly `0 -> 1` for this update;
- `channel_group.version`: unchanged;
- `Orderer.version`: unchanged;
- `Orderer/BatchSize.version`: exactly prior version plus one;
- `Orderer/BatchTimeout.version`: unchanged;
- `MaxMessageCount`: exactly the requested target;
- all other values, policies, groups, byte limits, timeout and consenters:
  unchanged.

## Code changes

### Shared transition validator

Added [e1_config_transition.py](C:/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/scripts/e1/e1_config_transition.py), a pure validator used by both the actuator and independent verifier. It returns auditable checks and raises categorized failures for:

- unexpected configuration, channel-group, Orderer-group, BatchSize or unrelated versions;
- wrong target value;
- unrelated values, policies, groups or other configuration fields;
- incomplete configuration-block evidence.

The structural comparison normalizes only the approved `MaxMessageCount`,
its expected leaf version, and Fabric's expected config sequence increment.
Nothing else is ignored.

### Actuator

[e1_config_actuator.py](C:/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/scripts/e1/e1_config_actuator.py) now:

- validates the complete decoded before/after block transition in both
  `execute-prepared` and direct `apply` paths;
- records the individual verification checks in `UpdateRecord`;
- fails immediately on a detected safety violation instead of swallowing it
  and later reporting only a timeout;
- restores `_utc_delta_ms` execution so commit-latency evidence is calculated;
- audits rollback preconditions against the exact recorded post-update
  configuration and constructs the rollback target only from the recorded
  pre-update configuration.

Rollback still requires both `--execute` and the matching
`--approved-target`. A mismatched current configuration is refused.

### Independent verifier

[e1_independent_config_verify.py](C:/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-/scripts/e1/e1_independent_config_verify.py) now records explicit version facts and can validate the committed transition against the prepared pre-update block using `--before-block`. It continues to fetch both channels independently through the Fabric CLI.

## Regression coverage

Focused actuator tests: **20 passed**.

Coverage includes:

- valid `45 -> 40`, BatchSize `0 -> 1` and sequence `0 -> 1`;
- valid transition from a nonzero starting BatchSize version;
- unexpected BatchSize version;
- unexpected Orderer group version;
- unrelated value and policy changes;
- wrong target;
- stale read set;
- missing configuration block;
- independent cross-channel verification failure;
- rollback refusal for a changed live state and wrong approval target.

The complete repository suite passed: **498 tests collected and passed**.

## Independent live verification

The verifier fetched and decoded both channels read-only at
`2026-11-10T16:57:43Z`.

Evidence directory:

`/home/mailf18/e1/cfg001-live-update-2026-11-10-repair/independent-verification-20261110T165743560025Z/`

The observed facts were:

| Field | `e1-cfg001` | `e1-r002` |
|---|---:|---:|
| config block | 4 | 0 |
| `MaxMessageCount` | 40 | 45 |
| `BatchSize.version` | 1 | 0 |
| `Orderer.version` | 0 | 0 |
| `BatchTimeout` | 2s | 2s |
| preferred bytes | 10,485,760 | 10,485,760 |
| absolute bytes | 103,809,024 | 103,809,024 |
| Raft consenters | 3 | 3 |
| consensus | etcdraft | etcdraft |
| application orgs | Org1MSP, Org2MSP | Org1MSP, Org2MSP |
| Orderer Admins policy | `MAJORITY Admins` | `MAJORITY Admins` |

The prepared-before to committed-after transition passed all checks,
including sequence increment, expected BatchSize version increment, stable
Orderer version, stable timeout version, unchanged values/policies/groups, and
target value 40.

The independent block evidence retains the previously recorded hashes:

- `e1-cfg001`: data hash `HLw3IoSiR8GAOxSF4AoNmzH9Wvoi1FqbfR8hXvmSqr8=`;
  block SHA-256 `2025bc4e5f0511339bfd41942005fce0c7bdd21d780d4254466e2f04017b9067`.
- `e1-r002`: block SHA-256
  `c6c9482c2ac5988cac737dec1d35f78d1f5d6a09c4fd6c61073eba8e9293443c`.

As a separate read-only check, `peer lifecycle chaincode querycommitted` was
run for `e1asset` on both channels. Both returned sequence `1`, version `1.0`,
the same `escc`/`vscc` plugins, the same channel-application endorsement
parameter, and approvals `Org1MSP=true`, `Org2MSP=true`. No chaincode or
endorsement definition changed.

## Rollback readiness

Rollback is **ready for a separately authorized operation**, but was not
executed here.

The repaired path now requires:

1. a successful update record;
2. complete exact `before_config` and `after_config` records;
3. explicit `--execute` and matching `--approved-target`;
4. a fresh read-only inspection whose complete active configuration equals the
   recorded `after_config`;
5. construction of the target from the exact recorded `before_config`.

Any intervening configuration change, missing evidence, unsuccessful record or
approval mismatch is rejected before `apply` is called. There is no automatic
rollback or silent execution.

## Remaining risks

- The configuration-block channel-header timestamp has one-second resolution;
  it cannot by itself provide sub-second commit latency. The actuator's
  monotonic timing remains the appropriate high-resolution measurement for a
  future approved update.
- The current actuator supports only the approved BatchSize leaf. Other
  configuration fields require separate read/write-set rules and tests.
- The independent verifier's live comparison is intentionally focused on the
  E1 invariants; future changes to channel topology or policy should add
  explicit expected facts rather than broadening comparisons implicitly.
- No new live update, rollback, benchmark or controller experiment was run in
  this task.
