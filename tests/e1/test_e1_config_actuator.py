import json
import copy
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "e1"))

import e1_config_actuator as ca              
import e1_independent_config_verify as independent              


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
    before_block = block_doc(max_count=45, batch_version="0", sequence="0", number="7")
    before_block["data"]["data"][0]["payload"]["header"]["channel_header"]["channel_id"] = "e1-cfg001"
    after_block = transition_after(before_block, max_count=40, batch_version="1", sequence="1")
    after_block["header"]["number"] = "8"
    before = ca.config_from_block(before_block)[0].as_dict()
    after = ca.config_from_block(after_block)[0].as_dict()
    record = {"success": True, "attempt_id": "a1", "channel": "e1-cfg001", "before_config": before,
              "after_config": after, "before_config_document": ca.config_from_block_document(before_block),
              "after_config_document": ca.config_from_block_document(after_block), "after_block_number": 8,
              "verification_checks": {"independent_verification": True}}
    auth = "ROLLBACK:e1-cfg001:a1"
    target = ca.validate_rollback_preconditions(record, after_block, 45, authorization=auth)
    assert target.as_dict() == before
    with pytest.raises(ca.SafetyError, match="full configuration differs"):
        changed = copy.deepcopy(after_block)
        changed["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["BatchTimeout"]["value"]["timeout"] = "3s"
        ca.validate_rollback_preconditions(record, changed, 45, authorization=auth)
    with pytest.raises(ca.SafetyError, match="approved-target"):
        ca.validate_rollback_preconditions(record, after_block, 40, authorization=auth)
    with pytest.raises(ca.SafetyError, match="explicit transition-specific"):
        ca.validate_rollback_preconditions(record, after_block, 45)


@pytest.mark.parametrize("mutation, message", [
    (lambda b: b["data"]["data"][0]["payload"]["data"]["config"].update(sequence="2"), "sequence differs"),
    (lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["ConsensusType"]["value"]["metadata"].update(consenters=[{}, {}]), "full configuration differs"),
    (lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]["value"].update(absolute_max_bytes=77), "full configuration differs"),
    (lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["BatchTimeout"]["value"].update(timeout="3s"), "full configuration differs"),
    (lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["policies"]["Admins"]["policy"]["value"].update(rule="ANY"), "full configuration differs"),
])
def test_rollback_rejects_changed_full_state(mutation, message):
    before = block_doc(number="7")
    before["data"]["data"][0]["payload"]["header"]["channel_header"]["channel_id"] = "e1-cfg001"
    after = transition_after(before)
    after["header"]["number"] = "8"
    record = {"success": True, "attempt_id": "a1", "channel": "e1-cfg001",
              "before_config": ca.config_from_block(before)[0].as_dict(), "after_config": ca.config_from_block(after)[0].as_dict(),
              "before_config_document": ca.config_from_block_document(before), "after_config_document": ca.config_from_block_document(after),
              "after_block_number": 8, "verification_checks": {"independent_verification": True}}
    current = copy.deepcopy(after); mutation(current)
    with pytest.raises(ca.SafetyError, match=message):
        ca.validate_rollback_preconditions(record, current, 45, authorization="ROLLBACK:e1-cfg001:a1")


def test_rollback_rejects_legacy_incomplete_or_unverified_records():
    record = {"success": True, "channel": "e1-cfg001", "attempt_id": "legacy"}
    with pytest.raises(ca.SafetyError, match="full pre-state"):
        ca.validate_rollback_preconditions(record, block_doc(), 45, authorization="ROLLBACK:e1-cfg001:legacy")


def test_rollback_rejects_missing_independent_verification_and_wrong_channel():
    before = block_doc(); before["data"]["data"][0]["payload"]["header"]["channel_header"]["channel_id"] = "e1-cfg001"
    after = transition_after(before); after["header"]["number"] = "8"
    record = {"success": True, "attempt_id": "a1", "channel": "e1-cfg001",
              "before_config": ca.config_from_block(before)[0].as_dict(), "after_config": ca.config_from_block(after)[0].as_dict(),
              "before_config_document": ca.config_from_block_document(before), "after_config_document": ca.config_from_block_document(after),
              "after_block_number": 8, "verification_checks": {}}
    with pytest.raises(ca.SafetyError, match="independent post-state"):
        ca.validate_rollback_preconditions(record, after, 45, authorization="ROLLBACK:e1-cfg001:a1")
    record["verification_checks"]["independent_verification"] = True
    wrong_channel = copy.deepcopy(after)
    wrong_channel["data"]["data"][0]["payload"]["header"]["channel_header"]["channel_id"] = "e1-r002"
    with pytest.raises(ca.SafetyError, match="different channel"):
        ca.validate_rollback_preconditions(record, wrong_channel, 45, authorization="ROLLBACK:e1-cfg001:a1")


def test_live_actuator_construction_requires_independent_verifier(tmp_path):
    with pytest.raises(ca.SafetyError, match="concrete IndependentFabricConfigVerifier"):
        ca.FabricChannelConfigActuator(
            channel="e1-cfg001", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"),
            fabric_cfg_path=tmp_path, orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com",
            orderer_ca=tmp_path / "ca.pem", orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out",
            live_enabled=True,
        )


def test_live_actuator_rejects_arbitrary_verifier_callback(tmp_path):
    with pytest.raises(ca.SafetyError, match="concrete IndependentFabricConfigVerifier"):
        ca.FabricChannelConfigActuator(
            channel="e1-cfg001", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"),
            fabric_cfg_path=tmp_path, orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com",
            orderer_ca=tmp_path / "ca.pem", orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out",
            live_enabled=True, independent_verifier=lambda *_: {"success": True, "independent": True, "complete": True},
        )


def test_config_update_submission_gateway_fails_closed_before_external_command(tmp_path):
    calls = []
    def reject_runner(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 2, b'{"valid":false}', b'Fabric source not configured')
    actuator = ca.FabricChannelConfigActuator(
        channel="e1-cfg001", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"),
        fabric_cfg_path=tmp_path, orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com",
        orderer_ca=tmp_path / "ca.pem", orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out",
        runner=reject_runner,
    )
    with pytest.raises(ca.ActuatorError, match="inspect_config_update_before_signing failed"):
        actuator._submit_config_update("test", tmp_path / "unsigned.pb", tmp_path / "block.pb",
                                       ca.FabricConfig(40, 2000, 10485760, 103809024, 3), tmp_path, [])
    assert len(calls) == 1
    assert "run_fabric_config_verifier.sh" in calls[0][1]
    assert "channel" not in calls[0] or "update" not in calls[0]
    source = (ROOT / "scripts/e1/e1_config_actuator.py").read_text(encoding="utf-8")
    assert source.count('"channel", "update"') == 0
    assert source.count("self._submit_config_update(") == 2


def test_native_verification_success_still_cannot_broadcast_before_review(tmp_path, monkeypatch):
    calls = []
    key_dir = tmp_path / "msp" / "keystore"
    key_dir.mkdir(parents=True)
    key_meta = key_dir / "fixture_key_metadata_only"
    key_meta.write_bytes(b"not-a-private-key-test-metadata")
    key_meta.chmod(0o600)
    update_hash = "a" * 64
    responses = [
        {"valid": True, "channel_id": "e1-cfg001", "update_channel_id": "e1-cfg001",
         "signature_count": 0, "outer_signature_present": False, "config_update_sha256": update_hash,
         "fabric_source_commit": "f871cf92a026aba7b12e6f06d71ded3e6e659d71"},
        None,
        {"valid": True, "channel_id": "e1-cfg001", "update_channel_id": "e1-cfg001",
         "signature_count": 1, "outer_signature_present": True, "config_update_sha256": update_hash,
         "fabric_source_commit": "f871cf92a026aba7b12e6f06d71ded3e6e659d71"},
        {"valid": True, "config_update_sha256": update_hash,
         "fabric_source_commit": "f871cf92a026aba7b12e6f06d71ded3e6e659d71"},
    ]
    def successful_offline_verifier(argv, **kwargs):
        calls.append(argv)
        response = responses.pop(0)
        stdout = b"" if response is None else json.dumps(response).encode()
        return subprocess.CompletedProcess(argv, 0, stdout, b"")
    actuator = ca.FabricChannelConfigActuator(
        channel="e1-cfg001", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"),
        fabric_cfg_path=tmp_path, orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com",
        orderer_ca=tmp_path / "ca.pem", orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out",
        runner=successful_offline_verifier,
    )
    monkeypatch.setattr(actuator, "_assert_signing_msp_key_permissions", lambda: None)
    monkeypatch.setattr(ca.os, "fstat", lambda _fd: type("Stat", (), {
        "st_mode": 0o100600, "st_uid": os.geteuid() if hasattr(os, "geteuid") else 0,
    })())
    original = tmp_path / "unsigned.pb"
    original.write_bytes(b"test envelope bytes")
    work = tmp_path / "work"
    work.mkdir()
    with pytest.raises(ca.SafetyError, match="offline Fabric-native signing and verification passed"):
        actuator._submit_config_update("test", original, tmp_path / "block.pb",
                                       ca.FabricConfig(41, 2000, 10485760, 103809024, 3), work, [])
    assert len(calls) == 4
    assert calls[1][1:4] == ["channel", "signconfigtx", "-f"]
    signed_copy = work / "offline_signed_config_update.pb"
    assert signed_copy.read_bytes() == original.read_bytes()
    if os.name == "posix":
        assert signed_copy.stat().st_mode & 0o777 == 0o600
    assert original.read_bytes() == b"test envelope bytes"
    assert not any("channel" in argv and "update" in argv for argv in calls)


def test_signer_fails_closed_on_insecure_or_unverifiable_key_permissions(tmp_path):
    actuator = ca.FabricChannelConfigActuator(
        channel="e1-cfg001", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"),
        fabric_cfg_path=tmp_path, orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com",
        orderer_ca=tmp_path / "ca.pem", orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out",
    )
    key_dir = tmp_path / "msp" / "keystore"
    key_dir.mkdir(parents=True)
    key_file = key_dir / "metadata_only"
    key_file.write_bytes(b"not-a-private-key-test-metadata")
    key_file.chmod(0o640)
    expected = "group or other permissions" if os.name == "posix" else "POSIX private-key permissions cannot be verified"
    with pytest.raises(ca.SafetyError, match=expected):
        actuator._assert_signing_msp_key_permissions()


def test_signing_copy_rejects_filesystem_without_private_mode_support(tmp_path, monkeypatch):
    calls = []
    response = {"valid": True, "channel_id": "e1-cfg001", "update_channel_id": "e1-cfg001",
                "signature_count": 0, "outer_signature_present": False, "config_update_sha256": "a" * 64,
                "fabric_source_commit": "f871cf92a026aba7b12e6f06d71ded3e6e659d71"}
    def inspect_only(argv, **kwargs):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, json.dumps(response).encode(), b"")
    actuator = ca.FabricChannelConfigActuator(
        channel="e1-cfg001", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"),
        fabric_cfg_path=tmp_path, orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com",
        orderer_ca=tmp_path / "ca.pem", orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out",
        runner=inspect_only,
    )
    monkeypatch.setattr(actuator, "_assert_signing_msp_key_permissions", lambda: None)
    # Simulate WSL on a Windows-backed mount, where requested 0600 is reported
    # as group/other accessible. This must fail before peer signconfigtx.
    monkeypatch.setattr(ca.os, "fstat", lambda _fd: type("Stat", (), {"st_mode": 0o100666, "st_uid": 0})())
    original = tmp_path / "unsigned.pb"
    original.write_bytes(b"test envelope bytes")
    work = tmp_path / "work"
    work.mkdir()
    with pytest.raises(ca.SafetyError, match="cannot enforce private artifact mode 0600"):
        actuator._submit_config_update("test", original, tmp_path / "block.pb",
                                       ca.FabricConfig(41, 2000, 10485760, 103809024, 3), work, [])
    assert len(calls) == 1
    assert "signconfigtx" not in calls[0]


@pytest.mark.parametrize("command", ["apply", "execute-prepared", "rollback"])
def test_production_cli_factory_injects_concrete_org_verifier(tmp_path, command):
    network = {
        "bin_dir": str(tmp_path / "bin"), "fabric_cfg_path": str(tmp_path / "fabric-cfg"),
        "orderer_ca": str(tmp_path / "orderer-ca.pem"),
        "orderers": [{"address": "localhost:7050", "tls_hostname": "orderer.example.com"}],
        "peers": [{"msp_id": "Org1MSP", "admin_msp_dir": str(tmp_path / "org1-msp"),
                   "address": "localhost:7051", "tls_root_cert": str(tmp_path / "org1-tls.pem")}],
    }
    network_file = tmp_path / "network.json"
    network_file.write_text(json.dumps(network), encoding="utf-8")
    args = type("Args", (), {"network": network_file, "channel": "e1-cfg001",
        "orderer_admin_msp": tmp_path / "orderer-msp", "out_dir": tmp_path / "out",
        "fence_dir": tmp_path / "fence", "min_b": 10, "max_b": 800,
        "cooldown_s": 30, "poll_s": 0.01, "commit_timeout_s": 1,
        "command": command, "execute": True})()
    actuator = ca.actuator_from_args(args)
    assert actuator.live_enabled is True
    assert isinstance(actuator.independent_verifier, independent.IndependentFabricConfigVerifier)
    assert actuator.independent_verifier.msp_id == "Org1MSP"


@pytest.mark.parametrize("path", ["apply", "execute_prepared"])
def test_all_submission_methods_fail_closed_when_live_mode_is_not_verified(tmp_path, path):
    calls = []
    actuator = ca.FabricChannelConfigActuator(
        channel="e1-cfg001", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"),
        fabric_cfg_path=tmp_path, orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com",
        orderer_ca=tmp_path / "ca.pem", orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out",
        runner=lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    target = ca.FabricConfig(max_message_count=40)
    with pytest.raises(ca.SafetyError, match="live submission disabled"):
        if path == "apply":
            actuator.apply(target)
        else:
            actuator.execute_prepared(tmp_path / "prepared", target)
    assert calls == []


def test_independent_verification_records_only_matching_observed_config(tmp_path):
    target = ca.FabricConfig(max_message_count=40)
    before = block_doc()
    expected = independent.expected_batch_size_config(before, 40)
    evidence = {"success": True, "independent": True, "complete": True,
                "outcome": "VERIFIED", "source": "peer channel fetch config + configtxlator proto_decode",
                "observed_config": target.as_dict(), "expected_config_document": expected,
                "transition_checks": {"full_transition": True}, "timestamp_utc": "2026-11-11T00:00:00Z"}
    verifier = independent.IndependentFabricConfigVerifier(
        peer=Path("peer"), configtxlator=Path("configtxlator"), fabric_cfg=tmp_path,
        msp_id="Org1MSP", msp_path=tmp_path / "org1msp", peer_address="localhost:7051",
        peer_tls_root_cert=tmp_path / "tls.pem", orderer="localhost:7050",
        orderer_tls_hostname="orderer.example.com", orderer_ca=tmp_path / "ca.pem",
        output_dir=tmp_path / "verify",
    )
    verifier.verify_transition = lambda *_: evidence
    actuator = ca.FabricChannelConfigActuator(
        channel="e1-cfg001", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"),
        fabric_cfg_path=tmp_path, orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com",
        orderer_ca=tmp_path / "ca.pem", orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out",
        fence_dir=tmp_path / "shared-state", live_enabled=True,
        independent_verifier=verifier,
    )
    fence = actuator.transition_fence
    with fence.channel_lock():
        fence.begin("attempt-1", target.as_dict(), context={"expected_config_document": expected})
        fence.mark_submitting("attempt-1")
        actuator._verify_and_reconcile("attempt-1", block_doc(), target, block_doc(max_count=40))
        assert fence.read()["status"] == "VERIFIED"


def test_failed_independent_verification_leaves_durable_unknown_fence(tmp_path):
    target = ca.FabricConfig(max_message_count=40)
    verifier = independent.IndependentFabricConfigVerifier(
        peer=Path("peer"), configtxlator=Path("configtxlator"), fabric_cfg=tmp_path,
        msp_id="Org1MSP", msp_path=tmp_path / "org1msp", peer_address="localhost:7051",
        peer_tls_root_cert=tmp_path / "tls.pem", orderer="localhost:7050",
        orderer_tls_hostname="orderer.example.com", orderer_ca=tmp_path / "ca.pem",
        output_dir=tmp_path / "verify",
    )
    verifier.verify_transition = lambda *_: {"success": False, "independent": True, "complete": True}
    actuator = ca.FabricChannelConfigActuator(
        channel="e1-cfg001", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"),
        fabric_cfg_path=tmp_path, orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com",
        orderer_ca=tmp_path / "ca.pem", orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out",
        fence_dir=tmp_path / "shared-state", live_enabled=True,
        independent_verifier=verifier,
    )
    fence = actuator.transition_fence
    with fence.channel_lock():
        fence.begin("attempt-1", target.as_dict())
        fence.mark_submitting("attempt-1")
        with pytest.raises(ca.SafetyError, match="did not establish"):
            actuator._verify_and_reconcile("attempt-1", block_doc(), target, block_doc(max_count=40))
        actuator._record_transition_failure("attempt-1", RuntimeError("verify failure"))
        assert fence.read()["status"] == "UNKNOWN"


def _pending_actuator(tmp_path, before, target, response):
    expected = independent.expected_batch_size_config(before, target.max_message_count)
    verifier = independent.IndependentFabricConfigVerifier(
        peer=Path("peer"), configtxlator=Path("configtxlator"), fabric_cfg=tmp_path,
        msp_id="Org1MSP", msp_path=tmp_path / "org1msp", peer_address="localhost:7051",
        peer_tls_root_cert=tmp_path / "tls.pem", orderer="localhost:7050",
        orderer_tls_hostname="orderer.example.com", orderer_ca=tmp_path / "ca.pem",
        output_dir=tmp_path / "verify",
    )
    verifier.verify_transition = lambda *_: response
    actuator = ca.FabricChannelConfigActuator(
        channel="e1-cfg001", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"),
        fabric_cfg_path=tmp_path, orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com",
        orderer_ca=tmp_path / "ca.pem", orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out",
        fence_dir=tmp_path / "shared-state", live_enabled=True, independent_verifier=verifier,
    )
    context = {"pre_config": ca.config_from_block(before)[0].as_dict(), "before_block": before,
               "expected_config_document": expected}
    with actuator.transition_fence.channel_lock():
        actuator.transition_fence.begin("crashed-attempt", target.as_dict(), context=context)
        actuator.transition_fence.mark_submitting("crashed-attempt")
        actuator.transition_fence.mark_unknown("crashed-attempt", "simulated process restart")
    return actuator, expected


def test_restart_reconciliation_finalizes_independently_observed_poststate(tmp_path):
    before = block_doc()
    target = ca.FabricConfig(max_message_count=40, batch_timeout_ms=2000,
                             preferred_max_bytes=10485760, absolute_max_bytes=103809024, orderers=3)
    expected = independent.expected_batch_size_config(before, 40)
    response = {"success": True, "independent": True, "complete": True, "outcome": "VERIFIED",
                "source": "peer channel fetch config + configtxlator proto_decode",
                "observed_config": target.as_dict(), "expected_config_document": expected,
                "transition_checks": {"all": True}}
    actuator, _ = _pending_actuator(tmp_path, before, target, response)
    result = actuator.reconcile_pending()
    assert result["resolved"] and result["status"] == "VERIFIED"
    assert actuator.transition_fence.read()["reconciliation"]["outcome"] == "VERIFIED"


def test_restart_prestate_requires_explicit_resolution_and_never_retries(tmp_path):
    before = block_doc()
    pre = ca.config_from_block(before)[0].as_dict()
    target = ca.FabricConfig(max_message_count=40, batch_timeout_ms=2000,
                             preferred_max_bytes=10485760, absolute_max_bytes=103809024, orderers=3)
    response = {"success": False, "independent": True, "complete": True, "outcome": "PRE_STATE",
                "source": "peer channel fetch config + configtxlator proto_decode",
                "observed_config": pre,
                "expected_config_document": independent.expected_batch_size_config(before, 40)}
    actuator, _ = _pending_actuator(tmp_path, before, target, response)
    blocked = actuator.reconcile_pending()
    assert not blocked["resolved"] and blocked["classification"] == "PRE_STATE"
    pending_state = actuator.transition_fence.read()
    assert pending_state["status"] == "UNKNOWN"
    assert pending_state["observations"][-1]["outcome"] == "PRE_STATE"
    resolved = actuator.reconcile_pending(explicit_not_submitted_authorization="RESOLVE-NOT-SUBMITTED:crashed-attempt")
    assert resolved["resolved"] and resolved["status"] == "NOT_SUBMITTED"


def test_restart_third_party_state_remains_fenced(tmp_path):
    before = block_doc()
    target = ca.FabricConfig(max_message_count=40, batch_timeout_ms=2000,
                             preferred_max_bytes=10485760, absolute_max_bytes=103809024, orderers=3)
    response = {"success": False, "independent": True, "complete": True, "outcome": "UNEXPECTED_STATE",
                "error": "test fixture contains an unexpected config"}
    actuator, _ = _pending_actuator(tmp_path, before, target, response)
    result = actuator.reconcile_pending()
    assert not result["resolved"] and result["classification"] == "UNEXPECTED_STATE"
    pending_state = actuator.transition_fence.read()
    assert pending_state["status"] == "UNKNOWN"
    assert pending_state["observations"][-1]["outcome"] == "UNEXPECTED_STATE"
