import json
import copy
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "e1"))

import e1_config_actuator as ca  # noqa: E402
import e1_independent_config_verify as independent  # noqa: E402


def block_doc(max_count=45, timeout="2s", number="7", batch_version="0", orderer_version="3", timeout_version="0", sequence="0"):
    orderer = {
        "version": orderer_version,
        "mod_policy": "Admins",
        "values": {
            "BatchSize": {"version": batch_version, "mod_policy": "Admins", "value": {
                "max_message_count": max_count,
                "absolute_max_bytes": 103809024,
                "preferred_max_bytes": 10485760,
            }},
            "BatchTimeout": {"version": timeout_version, "mod_policy": "Admins", "value": {"timeout": timeout}},
            "ConsensusType": {"version": "0", "mod_policy": "Admins", "value": {"metadata": {"consenters": [{}, {}, {}]}}},
        },
        "policies": {"Admins": {"policy": {"type": 3, "value": {"rule": "MAJORITY", "sub_policy": "Admins"}}}},
        "groups": {"OrdererOrg": {"policies": {"Admins": {"policy": {"type": 1, "value": {"identities": [{"principal": {"msp_identifier": "OrdererMSP"}}]}}}}}},
    }
    config = {"sequence": sequence, "channel_group": {"version": "4", "groups": {"Orderer": orderer}}}
    return {"header": {"number": number}, "data": {"data": [{"payload": {"header": {"channel_header": {"timestamp": "2026-11-10T15:00:00Z"}}, "data": {"config": config}}}]}}


def test_extracts_active_config_and_required_policy():
    active, policy = ca.config_from_block(block_doc())
    assert active.max_message_count == 45
    assert active.batch_timeout_ms == 2000
    assert active.preferred_max_bytes == 10485760
    assert active.orderers == 3
    assert policy["batch_size_mod_policy"] == "Admins"
    assert policy["required_signer_scope"] == "Orderer/Admins"
    assert policy["required_orderer_admin_msp"] == ["OrdererMSP"]
    assert ca.block_timestamp_utc(block_doc()) == "2026-11-10T15:00:00Z"


def test_updated_config_changes_only_approved_leaf_and_does_not_mutate_original():
    original = ca.config_from_block(block_doc())[0]
    raw = block_doc()["data"]["data"][0]["payload"]["data"]["config"]
    updated = ca.updated_config(raw, 60)
    assert raw["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]["value"]["max_message_count"] == 45
    assert updated["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]["value"]["max_message_count"] == 60
    assert updated["channel_group"]["groups"]["Orderer"]["values"]["BatchTimeout"]["value"]["timeout"] == "2s"
    assert original.batch_timeout_ms == 2000


@pytest.mark.parametrize("bad", [0, 9, 801, -1, True])
def test_updated_config_rejects_unsafe_target(bad):
    raw = block_doc()["data"]["data"][0]["payload"]["data"]["config"]
    with pytest.raises(ca.SafetyError):
        ca.updated_config(raw, bad)


def test_actuator_target_validation_rejects_timeout_or_byte_changes(tmp_path):
    actuator = ca.FabricChannelConfigActuator(
        channel="e1-r002", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"),
        fabric_cfg_path=tmp_path, orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com",
        orderer_ca=tmp_path / "ca.pem", orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out",
    )
    current = ca.FabricConfig()
    with pytest.raises(ca.SafetyError):
        actuator._validate_target(current, ca.FabricConfig(max_message_count=60, batch_timeout_ms=1000))
    with pytest.raises(ca.SafetyError):
        actuator._validate_target(current, ca.FabricConfig(max_message_count=60, preferred_max_bytes=524288))
    with pytest.raises(ca.SafetyError):
        actuator._validate_target(current, current)


def transition_after(before, *, max_count=40, batch_version="1", sequence="1"):
    after = copy.deepcopy(before)
    config = after["data"]["data"][0]["payload"]["data"]["config"]
    config["sequence"] = sequence
    batch = config["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]
    batch["version"] = batch_version
    batch["value"]["max_message_count"] = max_count
    return after


def test_valid_45_to_40_transition_accepts_expected_batch_and_sequence_versions():
    before = block_doc(max_count=45, batch_version="0", sequence="0")
    after = transition_after(before)
    checks = ca._validate_committed_transition(before, after, 40)
    assert checks["batch_size_version_expected_increment"]
    assert checks["orderer_group_version_unchanged"]
    assert checks["unrelated_configuration_unchanged"]


def test_valid_transition_accepts_a_different_starting_batch_version():
    before = block_doc(max_count=55, batch_version="7", sequence="10")
    after = transition_after(before, max_count=50, batch_version="8", sequence="11")
    assert ca._validate_committed_transition(before, after, 50)["batch_size_version_expected_increment"]


@pytest.mark.parametrize(
    ("mutation", "category"),
    [
        (lambda d: d["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"].update(version="2"), "unexpected_batch_size_version"),
        (lambda d: d["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"].update(version="4"), "unexpected_orderer_group_version"),
        (lambda d: d["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["BatchTimeout"]["value"].update(timeout="3s"), "unexpected_unrelated_value"),
        (lambda d: d["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["policies"]["Admins"]["policy"]["value"].update(rule="ANY"), "unexpected_unrelated_policy"),
    ],
)
def test_unexpected_committed_changes_are_rejected(mutation, category):
    before = block_doc()
    after = transition_after(before)
    mutation(after)
    with pytest.raises(ca.SafetyError, match=category):
        ca._validate_committed_transition(before, after, 40)


def test_wrong_target_is_rejected():
    before = block_doc()
    after = transition_after(before, max_count=39)
    with pytest.raises(ca.SafetyError, match="wrong_target_value"):
        ca._validate_committed_transition(before, after, 40)


def test_missing_configuration_block_is_rejected_as_incomplete_evidence():
    with pytest.raises(ca.SafetyError, match="incomplete_evidence"):
        ca._validate_committed_transition(block_doc(), {}, 40)


def test_stale_read_set_is_rejected_before_submission(tmp_path):
    actuator = ca.FabricChannelConfigActuator(
        channel="e1-r002", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"),
        fabric_cfg_path=tmp_path, orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com",
        orderer_ca=tmp_path / "ca.pem", orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out",
    )
    before_block = block_doc()
    before, policy = ca.config_from_block(before_block)
    target = ca.FabricConfig(max_message_count=40, batch_timeout_ms=before.batch_timeout_ms,
                             preferred_max_bytes=before.preferred_max_bytes,
                             absolute_max_bytes=before.absolute_max_bytes, orderers=before.orderers)
    update = {
        "channel_id": "e1-r002",
        "read_set": {"groups": {"Orderer": {"version": policy["orderer_group_version"], "values": {}, "policies": {}, "groups": {}}}, "values": {}, "policies": {}},
        "write_set": {"groups": {"Orderer": {"version": policy["orderer_group_version"], "values": {"BatchSize": {
            "version": "1", "mod_policy": "Admins", "value": {"max_message_count": 40, "preferred_max_bytes": before.preferred_max_bytes, "absolute_max_bytes": before.absolute_max_bytes}
        }}, "policies": {}, "groups": {}}}, "values": {}, "policies": {}},
    }
    prepared = tmp_path / "prepared"
    prepared.mkdir()
    (prepared / "prepare_record.json").write_text(json.dumps({"channel": "e1-r002", "parameter": "Orderer.BatchSize.MaxMessageCount", "signer_msp_id": "OrdererMSP", "before_config": before.as_dict(), "requested_config": target.as_dict(), "before_block_number": 7}), encoding="utf-8")
    (prepared / "before.json").write_text(json.dumps(before_block), encoding="utf-8")
    (prepared / "original_config.json").write_text(json.dumps(ca.config_from_block_document(before_block)), encoding="utf-8")
    (prepared / "config_update.json").write_text(json.dumps(update), encoding="utf-8")
    (prepared / "config_update_in_envelope.json").write_text(json.dumps({"payload": {"header": {"channel_header": {"channel_id": "e1-r002", "type": 2}}, "data": {"config_update": update}}}), encoding="utf-8")
    (prepared / "config_update_in_envelope.pb").write_bytes(b"fixture")
    update["read_set"]["groups"]["Orderer"]["version"] = "2"
    (prepared / "config_update.json").write_text(json.dumps(update), encoding="utf-8")
    (prepared / "config_update_in_envelope.json").write_text(json.dumps({"payload": {"header": {"channel_header": {"channel_id": "e1-r002", "type": 2}}, "data": {"config_update": update}}}), encoding="utf-8")
    with pytest.raises(ca.SafetyError, match="read set version"):
        actuator._validate_prepared_artifact(prepared, before_block, before, policy, target)


def test_wrong_channel_prepared_artifact_is_rejected(tmp_path):
    actuator = ca.FabricChannelConfigActuator(
        channel="e1-r002", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"),
        fabric_cfg_path=tmp_path, orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com",
        orderer_ca=tmp_path / "ca.pem", orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out",
    )
    before_block = block_doc()
    before = ca.config_from_block(before_block)[0]
    target = ca.FabricConfig(max_message_count=40, batch_timeout_ms=before.batch_timeout_ms,
                             preferred_max_bytes=before.preferred_max_bytes,
                             absolute_max_bytes=before.absolute_max_bytes, orderers=before.orderers)
    prepared = tmp_path / "prepared-wrong-channel"
    prepared.mkdir()
    for name in ("before.json", "original_config.json", "config_update.json", "config_update_in_envelope.json", "config_update_in_envelope.pb"):
        (prepared / name).write_text("{}", encoding="utf-8")
    (prepared / "prepare_record.json").write_text(json.dumps({"channel": "other-channel", "parameter": "Orderer.BatchSize.MaxMessageCount", "signer_msp_id": "OrdererMSP", "before_config": before.as_dict(), "requested_config": target.as_dict(), "before_block_number": 7}), encoding="utf-8")
    with pytest.raises(ca.SafetyError, match="channel does not match"):
        actuator._validate_prepared_artifact(prepared, before_block, before, ca.config_from_block(before_block)[1], target)


def test_independent_verification_failure_is_not_accepted():
    cfg = {"max_message_count": 40, "batch_size_version": "1", "orderer_group_version": "0", "batch_timeout_version": "0", "batch_timeout": "2s", "preferred_max_bytes": 10485760, "absolute_max_bytes": 103809024, "consenters": 3, "consensus_type": "etcdraft", "application_orgs": ["Org1", "Org2"], "orderer_admin_policy": {"rule": "MAJORITY"}}
    baseline = dict(cfg, max_message_count=44)
    checks = independent.verification_checks(cfg, baseline)
    assert not all(checks.values())


def test_rollback_requires_exact_recorded_post_update_state_and_explicit_target():
    before = ca.FabricConfig(max_message_count=45, batch_timeout_ms=2000, preferred_max_bytes=10485760, absolute_max_bytes=103809024, orderers=3).as_dict()
    after = dict(before, max_message_count=40)
    record = {"success": True, "before_config": before, "after_config": after}
    target = ca.validate_rollback_preconditions(record, after, 45)
    assert target.as_dict() == before
    with pytest.raises(ca.SafetyError, match="current configuration differs"):
        ca.validate_rollback_preconditions(record, dict(after, max_message_count=41), 45)
    with pytest.raises(ca.SafetyError, match="approved-target"):
        ca.validate_rollback_preconditions(record, after, 40)
