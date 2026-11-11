import copy
import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "e1"))

from e1_config_actuator import FabricConfig
from e1_config_transition import expected_batch_size_config
from e1_independent_config_verify import IndependentFabricConfigVerifier, IndependentVerificationError


def config_block(*, channel="e1-cfg001", sequence="7", batch_version="3", b=45):
    return {
        "header": {"number": "12"},
        "data": {"data": [{"payload": {"header": {"channel_header": {"channel_id": channel}},
            "data": {"config": {"sequence": sequence, "channel_group": {
                "version": "4", "mod_policy": "Admins", "policies": {"Admins": {"policy": {"type": 3, "value": {"rule": "MAJORITY"}}}},
                "groups": {
                    "Orderer": {"version": "8", "mod_policy": "Admins", "policies": {"Admins": {"policy": {"type": 3, "value": {"rule": "MAJORITY"}}}},
                        "groups": {"OrdererOrg": {"version": "0", "policies": {"Admins": {"policy": {"type": 1, "value": {"identities": [{"principal": {"msp_identifier": "OrdererMSP"}}]}}}}}},
                        "values": {
                            "BatchSize": {"version": batch_version, "mod_policy": "Admins", "value": {"max_message_count": b, "absolute_max_bytes": 103809024, "preferred_max_bytes": 10485760}},
                            "BatchTimeout": {"version": "1", "mod_policy": "Admins", "value": {"timeout": "2s"}},
                            "ConsensusType": {"version": "0", "mod_policy": "Admins", "value": {"type": "etcdraft", "metadata": {"consenters": [{"host": "o1"}, {"host": "o2"}, {"host": "o3"}]}}}
                        }},
                    "Application": {"version": "2", "mod_policy": "Admins", "policies": {"Admins": {"policy": {"type": 3, "value": {"rule": "MAJORITY"}}}},
                        "groups": {"Org1MSP": {"version": "0", "policies": {"Admins": {"policy": {"type": 1, "value": {"identities": []}}}}},
                                   "Org2MSP": {"version": "0", "policies": {"Admins": {"policy": {"type": 1, "value": {"identities": []}}}}}}, "values": {}}
                }}}}}}]}
    }


def verifier(tmp_path, *, runner=None):
    return IndependentFabricConfigVerifier(
        peer=Path("peer"), configtxlator=Path("configtxlator"), fabric_cfg=tmp_path,
        msp_id="Org1MSP", msp_path=tmp_path / "org1-admin-msp",
        peer_address="localhost:7051", peer_tls_root_cert=tmp_path / "org1-tls.pem",
        orderer="localhost:7050", orderer_tls_hostname="orderer.example.com",
        orderer_ca=tmp_path / "orderer-ca.pem", output_dir=tmp_path / "verification",
        timeout_s=0.01, runner=runner or subprocess.run,
    )


def project(block):
    cfg = block["data"]["data"][0]["payload"]["data"]["config"]
    orderer = cfg["channel_group"]["groups"]["Orderer"]
    size = orderer["values"]["BatchSize"]["value"]
    return {"max_message_count": size["max_message_count"], "batch_timeout_ms": 2000,
            "preferred_max_bytes": size["preferred_max_bytes"],
            "absolute_max_bytes": size["absolute_max_bytes"], "orderers": 3}


def test_independent_verification_accepts_exact_post_state_and_writes_provenance(tmp_path, monkeypatch):
    before = config_block()
    after = copy.deepcopy(before)
    cfg = after["data"]["data"][0]["payload"]["data"]["config"]
    expected = expected_batch_size_config(before, 40)
    cfg.clear()
    cfg.update(expected)
    target = FabricConfig(**{**project(before), "max_message_count": 40})
    obj = verifier(tmp_path)
    monkeypatch.setattr(obj, "retrieve", lambda _channel: (after, {"evidence_dir": str(tmp_path), "source": "fixture", "observation_timestamp_utc": "now"}))
    result = obj.verify_transition("e1-cfg001", before, target)
    assert result["success"] is True and result["independent"] is True
    assert result["outcome"] == "VERIFIED"
    assert result["observed_config"] == target.as_dict()
    assert (tmp_path / "transition_verification.json").is_file()


@pytest.mark.parametrize("mutation", [
    lambda b: b["data"]["data"][0]["payload"]["data"]["config"].update(sequence="9"),
    lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"].update(version="5"),
    lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]["value"].update(max_message_count=39),
    lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["BatchTimeout"]["value"].update(timeout="3s"),
    lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]["value"].update(absolute_max_bytes=999),
    lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]["value"].update(preferred_max_bytes=999),
    lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["policies"]["Admins"]["policy"]["value"].update(rule="ANY"),
    lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Application"]["groups"].pop("Org2MSP"),
    lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["ConsensusType"]["value"]["metadata"]["consenters"].pop(),
])
def test_unexpected_post_configuration_is_rejected(tmp_path, monkeypatch, mutation):
    before = config_block()
    after = copy.deepcopy(before)
    cfg = after["data"]["data"][0]["payload"]["data"]["config"]
    cfg.clear(); cfg.update(expected_batch_size_config(before, 40))
    mutation(after)
    target = FabricConfig(**{**project(before), "max_message_count": 40})
    obj = verifier(tmp_path)
    monkeypatch.setattr(obj, "retrieve", lambda _channel: (after, {"evidence_dir": str(tmp_path)}))
    result = obj.verify_transition("e1-cfg001", before, target)
    assert result["success"] is False
    assert result["outcome"] == "UNEXPECTED_STATE"


def test_wrong_channel_is_rejected(tmp_path, monkeypatch):
    before = config_block()
    after = copy.deepcopy(before)
    cfg = after["data"]["data"][0]["payload"]["data"]["config"]
    cfg.clear(); cfg.update(expected_batch_size_config(before, 40))
    after["data"]["data"][0]["payload"]["header"]["channel_header"]["channel_id"] = "wrong"
    obj = verifier(tmp_path)
    monkeypatch.setattr(obj, "retrieve", lambda _channel: (after, {"evidence_dir": str(tmp_path)}))
    with pytest.raises(IndependentVerificationError, match="retrieved config channel"):
        obj.verify_transition("e1-cfg001", before, FabricConfig(**{**project(before), "max_message_count": 40}))


def test_malformed_decoded_configuration_is_rejected_and_saved(tmp_path, monkeypatch):
    before = config_block()
    malformed = {"header": {"number": "13"}, "data": {"data": [{"payload": {"header": {
        "channel_header": {"channel_id": "e1-cfg001"}}, "data": {"not_config": {}}}}]}}
    obj = verifier(tmp_path)
    monkeypatch.setattr(obj, "retrieve", lambda _channel: (malformed, {"evidence_dir": str(tmp_path), "source": "fixture"}))
    with pytest.raises(IndependentVerificationError):
        obj.verify_transition("e1-cfg001", before, FabricConfig(**{**project(before), "max_message_count": 40}))
    assert json.loads((tmp_path / "transition_verification.json").read_text(encoding="utf-8"))["success"] is False


def test_pre_state_is_classified_without_claiming_safe_retry(tmp_path, monkeypatch):
    before = config_block()
    obj = verifier(tmp_path)
    monkeypatch.setattr(obj, "retrieve", lambda _channel: (before, {"evidence_dir": str(tmp_path)}))
    result = obj.verify_transition("e1-cfg001", before, FabricConfig(**{**project(before), "max_message_count": 40}))
    assert result["outcome"] == "PRE_STATE"
    assert result["safe_to_retry"] is False


def test_concrete_retriever_uses_read_only_fetch_decode_and_captures_tool_versions(tmp_path):
    block = config_block()
    calls = []
    def runner(argv, **kwargs):
        calls.append(argv)
        if argv[-1] == "version":
            return subprocess.CompletedProcess(argv, 0, b" Version: v2.5.16\n", b"")
        if argv[1:3] == ["channel", "fetch"]:
            Path(argv[4]).write_bytes(b"configuration-block-pb")
        else:
            Path(argv[argv.index("--output") + 1]).write_text(json.dumps(block), encoding="utf-8")
        return subprocess.CompletedProcess(argv, 0, b"", b"")
    observed, evidence = verifier(tmp_path, runner=runner).retrieve("e1-cfg001")
    assert observed["header"]["number"] == "12"
    assert evidence["tool_versions"] == {"peer": "v2.5.16", "configtxlator": "v2.5.16"}
    assert any(argv[1:4] == ["channel", "fetch", "config"] for argv in calls)
    assert any(argv[1:3] == ["proto_decode", "--input"] or "proto_decode" in argv for argv in calls)
    assert all("channel" not in argv or "update" not in argv for argv in calls)


@pytest.mark.parametrize(("failure", "category"), [
    ("timeout", "retrieval_timeout"), ("unavailable", "verifier_unavailable"),
    ("missing", "incomplete_response"), ("malformed", "malformed_decoded_configuration"),
])
def test_retrieval_failures_are_classified(tmp_path, failure, category):
    def runner(argv, **kwargs):
        if failure == "timeout":
            raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
        if failure == "unavailable":
            raise FileNotFoundError("peer binary missing")
        if argv[-1] == "version":
            return subprocess.CompletedProcess(argv, 0, b" Version: v2.5.16\n", b"")
        if failure == "missing":
            if argv[1:3] == ["channel", "fetch"]:
                return subprocess.CompletedProcess(argv, 0, b"", b"")
        if argv[1:3] == ["channel", "fetch"]:
            Path(argv[4]).write_bytes(b"fixture block bytes")
        else:
            Path(argv[argv.index("--output") + 1]).write_text("{ malformed", encoding="utf-8")
        return subprocess.CompletedProcess(argv, 0, b"", b"")
    obj = verifier(tmp_path, runner=runner)
    with pytest.raises(IndependentVerificationError) as exc:
        obj.retrieve("e1-cfg001")
    assert exc.value.category == category
