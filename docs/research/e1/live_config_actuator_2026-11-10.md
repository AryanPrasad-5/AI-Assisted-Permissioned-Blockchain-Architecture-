# Live Fabric configuration actuator — 2026-11-10

## Current channel inspection

Read-only inspection of `e1-r002` used the existing Fabric 2.5.16 binaries,
the first orderer endpoint and the explicit OrdererMSP admin material. The
active configuration was:

| Field | Active value |
|---|---:|
| `Orderer.BatchSize.MaxMessageCount` | 45 |
| `Orderer.BatchSize.AbsoluteMaxBytes` | 103,809,024 bytes |
| `Orderer.BatchSize.PreferredMaxBytes` | 10,485,760 bytes |
| `Orderer.BatchTimeout` | 2s |
| channel config block | 0 |

The decoded policy facts are:

- `BatchSize.mod_policy = Admins`;
- `Orderer/Admins = MAJORITY Admins`;
- one ordering organization is present: `OrdererOrg`;
- its admin identity is `OrdererMSP.admin`;
- therefore the current E1 policy requires the OrdererMSP admin signature.

The inspection artifacts are under:

```text
/home/mailf18/e1/r002-config-actuator/update-20261110T162242925001Z-f2763262/
```

## Actuator behavior

`[e1_config_actuator.py](../../../scripts/e1/e1_config_actuator.py)` supports
the approved parameter `Orderer.BatchSize.MaxMessageCount` only. It:

1. obtains the current config block;
2. decodes and records the active value and policy;
3. validates the target against the approved range 10–800;
4. rejects changes to `BatchTimeout`, byte limits, orderer count or any
   unchanged target;
5. constructs a deep-copy config with only `max_message_count` changed;
6. uses `configtxlator proto_encode` and `compute_update`;
7. verifies the decoded read/write set;
8. wraps the update in a `common.Envelope` with type `CONFIG_UPDATE`;
9. submits through `peer channel update`, which signs using the explicit
   OrdererMSP admin environment; and
10. polls a newly fetched config block until a later block number contains the
    requested value.

Every attempt has a unique artifact directory, command stdout/stderr files,
decoded configs, update envelope, policy record and `update_record.json`.
The actuator uses an inter-process lock, a persisted success-based cooldown,
command timeouts and fail-closed error handling. A failed callback leaves the
previous active configuration unchanged in the actuator state.

## Prepare-only verification

The following real, non-mutating preparation was executed for target `B=40`:

```bash
python3 -B scripts/e1/e1_config_actuator.py \
  --network fabric-testbed/network/network-e1-recovery.json \
  --channel e1-r002 \
  --orderer-admin-msp /home/mailf18/fabric/fabric-samples/test-network/organizations/ordererOrganizations/example.com/users/Admin@example.com/msp \
  --out-dir /home/mailf18/e1/r002-config-actuator \
  prepare --target-b 40
```

The preparation succeeded with:

- current `B=45`, target `B=40`;
- read set: `Orderer` version `0`, with no changed leaf;
- write set: `Orderer` version `0`, `BatchSize` version `1`, target
  `max_message_count=40`;
- envelope encoding succeeded;
- no `peer channel update` command was executed.

The prepare artifacts are under:

```text
/home/mailf18/e1/r002-config-actuator/update-20261110T162335161265Z-8d396657/
```

## Live submission gate

No live update has been submitted in this milestone. The exact target value
was not supplied as an explicit experimental approval, so the actuator was
not allowed to mutate `e1-r002`.

After an explicit approval of a specific target, the guarded command is:

```bash
python3 -B scripts/e1/e1_config_actuator.py \
  --network fabric-testbed/network/network-e1-recovery.json \
  --channel e1-r002 \
  --orderer-admin-msp /home/mailf18/fabric/fabric-samples/test-network/organizations/ordererOrganizations/example.com/users/Admin@example.com/msp \
  --out-dir /home/mailf18/e1/r002-config-actuator \
  apply --target-b <APPROVED_B> --approved-target <APPROVED_B> --execute
```

Both `--execute` and the matching `--approved-target` are required. Submission
does not count as success: success requires a later active configuration block
with the target value.

Rollback uses the successful record's previous value and is also approval- and
cooldown-gated:

```bash
python3 -B scripts/e1/e1_config_actuator.py \
  --network fabric-testbed/network/network-e1-recovery.json \
  --channel e1-r002 \
  --orderer-admin-msp /home/mailf18/fabric/fabric-samples/test-network/organizations/ordererOrganizations/example.com/users/Admin@example.com/msp \
  --out-dir /home/mailf18/e1/r002-config-actuator \
  rollback --record <SUCCESSFUL_UPDATE_RECORD> \
  --approved-target <PREVIOUS_B> --execute
```

## Controller integration

The actuator can be connected to the existing `CallableActuator` without
changing any controller:

```python
live = FabricChannelConfigActuator(...)
actuator = CallableActuator(initial_config, live.apply)
harness = ControllerExperimentHarness(controller, actuator, run_id="...")
```

The callable wrapper measures the complete actuator call. The actuator's
record additionally separates decision time, update construction, submission,
the channel-header configuration-block timestamp, local active verification,
and total adaptation latency.

Static and Reactive should be tested first on a dedicated channel or an
explicitly authorized E1 update. Predictive integration remains blocked until
one manual update and rollback have both been commit-verified.

## Blockers and research limits

- No live adaptation latency exists yet because no update was submitted.
- `BatchTimeout` is intentionally not mutable by this first actuator.
- No concurrent update is permitted.
- Existing baseline artifacts and `e1-r002` were not modified.
- No HFBTP or Flashbots pipeline was changed.
