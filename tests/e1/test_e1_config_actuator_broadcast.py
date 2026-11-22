"""Owner-authorized broadcast path of e1_config_actuator (2026-11-22). Offline: a fake runner stands in for peer/configtxlator."""
import copy
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "e1"))

import e1_config_actuator as ca  # noqa: E402
import e1_independent_config_verify as independent  # noqa: E402

NOW = dt.datetime(2026, 11, 23, 12, 0, 0, tzinfo=dt.timezone.utc)
COMMIT = "f871cf92a026aba7b12e6f06d71ded3e6e659d71"


def auth_text(**over):
    kv = {"approved_by": "owner", "approved_utc": "2026-11-23T10:00:00Z", "expires_utc": "2026-11-25T18:00:00Z",
          "broadcast_channels": "e1-cl01,e1-cl02", "broadcast_targets": "10,40"}
    kv.update(over)
    return "".join(f"{k}={v}\n" for k, v in kv.items() if v is not None)


def write_auth(tmp_path, text, name="auth.txt"):
    p = tmp_path / name
    p.write_bytes(text.encode("ascii") if isinstance(text, str) else text)
    return p


def test_authorization_parses_and_covers_exact_channel_and_target(tmp_path):
    a = ca.BroadcastAuthorization.load(write_auth(tmp_path, auth_text()), now=NOW)
    assert a.channels == ("e1-cl01", "e1-cl02") and a.targets == (10, 40)
    a.require("e1-cl01", 40, now=NOW)
    with pytest.raises(ca.SafetyError, match="does not cover channel"):
        a.require("e1-cl03", 40, now=NOW)
    with pytest.raises(ca.SafetyError, match="does not cover MaxMessageCount"):
        a.require("e1-cl01", 45, now=NOW)
    with pytest.raises(ca.SafetyError, match="expired"):
        a.require("e1-cl01", 40, now=dt.datetime(2026, 11, 26, tzinfo=dt.timezone.utc))


@pytest.mark.parametrize("text,message", [
    (auth_text(approved_by="assistant"), "approved_by=owner"),
    (auth_text(approved_by=None), "lacks"),
    (auth_text(broadcast_targets=None), "lacks"),
    (auth_text(expires_utc="2026-11-23T09:00:00Z"), "expires before"),
    (auth_text(expires_utc="2026-11-23T11:00:00Z"), "expired"),
    (auth_text(approved_utc="2026-11-24T10:00:00Z", expires_utc="2026-11-25T10:00:00Z"), "future"),
    (auth_text(broadcast_channels="mychannel"), "exact e1-"),
    (auth_text(broadcast_channels="e1-cl01,e1-cl01"), "twice"),
    (auth_text(broadcast_targets="10,4000"), "outside"),
    (auth_text(broadcast_targets="ten"), "integers"),
    (auth_text().replace("\n", "\r\n"), "LF"),
    (auth_text() + "approved_by=owner\n", "twice"),
    (auth_text() + "bad line\n", "key=value"),
    (auth_text() + "x = y\n", "key=value"),
    (auth_text(approved_utc="2026-11-23 10:00:00"), "YYYY"),
])
def test_authorization_negatives(tmp_path, text, message):
    with pytest.raises(ca.SafetyError, match=message):
        ca.BroadcastAuthorization.load(write_auth(tmp_path, text), now=NOW)


@pytest.mark.parametrize("channels", ["e1-cfg001", "e1-r002", "e1-cs01,e1-cfg001", "E1-R002", "e1-b001", "e1-e00s",
                                      "e1-ql01-b10-l5", "e1-cp03-b40-l10", "e1-rp01-b10-r1"])
def test_authorization_refuses_protected_and_historical_channels(tmp_path, channels):
    with pytest.raises(ca.SafetyError, match="broadcast_channels"):
        ca.BroadcastAuthorization.load(write_auth(tmp_path, auth_text(broadcast_channels=channels)), now=NOW)


def test_authorization_accepts_the_closed_loop_channels(tmp_path):
    a = ca.BroadcastAuthorization.load(write_auth(tmp_path, auth_text(broadcast_channels="e1-cs01,e1-cl12")), now=NOW)
    assert a.channels == ("e1-cs01", "e1-cl12")


def test_authorization_rejects_non_ascii(tmp_path):
    with pytest.raises(ca.SafetyError, match="ASCII"):
        ca.BroadcastAuthorization.load(write_auth(tmp_path, auth_text().encode("ascii") + "note=é\n".encode("utf-8")), now=NOW)


def test_runner_keys_are_ignored_by_the_actuator_parser(tmp_path):
    a = ca.BroadcastAuthorization.load(write_auth(tmp_path, auth_text() + "runs=cl01,cl02\nstage_sha256=" + "a" * 64 + "\n"), now=NOW)
    assert a.channels == ("e1-cl01", "e1-cl02")


def test_actuator_refuses_authorization_for_another_channel(tmp_path):
    a = ca.BroadcastAuthorization.load(write_auth(tmp_path, auth_text()), now=NOW)
    with pytest.raises(ca.SafetyError, match="does not cover channel"):
        ca.FabricChannelConfigActuator(
            channel="e1-cfg001", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"), fabric_cfg_path=tmp_path,
            orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com", orderer_ca=tmp_path / "ca.pem",
            orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out", broadcast_authorization=a)


def test_cli_refuses_authorization_without_live_execute(tmp_path):
    network = {"bin_dir": str(tmp_path), "fabric_cfg_path": str(tmp_path), "orderer_ca": str(tmp_path / "ca.pem"),
               "orderers": [{"address": "localhost:7050", "tls_hostname": "orderer.example.com"}],
               "peers": [{"msp_id": "Org1MSP", "admin_msp_dir": str(tmp_path), "address": "localhost:7051", "tls_root_cert": str(tmp_path)}]}
    nf = tmp_path / "network.json"
    nf.write_text(json.dumps(network), encoding="utf-8")
    args = ca.build_parser().parse_args(["--network", str(nf), "--channel", "e1-cl01", "--orderer-admin-msp", str(tmp_path),
                                         "--out-dir", str(tmp_path / "o"), "--broadcast-authorization", str(tmp_path / "a.txt"),
                                         "apply", "--target-b", "40"])
    with pytest.raises(ca.SafetyError, match="only accepted with a live --execute"):
        ca.actuator_from_args(args)


# --------------------------------------------------------------------------------------------- end-to-end apply() with a fake world
def block_doc(max_count=10, number="7", batch_version="0", sequence="0", channel="e1-cl01"):
    orderer = {
        "version": "3", "mod_policy": "Admins",
        "values": {
            "BatchSize": {"version": batch_version, "mod_policy": "Admins", "value": {
                "max_message_count": max_count, "absolute_max_bytes": 103809024, "preferred_max_bytes": 10485760}},
            "BatchTimeout": {"version": "0", "mod_policy": "Admins", "value": {"timeout": "2s"}},
            "ConsensusType": {"version": "0", "mod_policy": "Admins", "value": {"metadata": {"consenters": [{}, {}, {}]}}},
        },
        "policies": {"Admins": {"policy": {"type": 3, "value": {"rule": "MAJORITY", "sub_policy": "Admins"}}}},
        "groups": {"OrdererOrg": {"policies": {"Admins": {"policy": {"type": 1, "value": {"identities": [{"principal": {"msp_identifier": "OrdererMSP"}}]}}}}}},
    }
    config = {"sequence": sequence, "channel_group": {"version": "4", "groups": {"Orderer": orderer}}}
    return {"header": {"number": number}, "data": {"data": [{"payload": {"header": {"channel_header": {
        "timestamp": "2026-11-23T12:00:00Z", "channel_id": channel}}, "data": {"config": config}}}]}}


class FakeWorld:
    """peer / configtxlator / native verifier stand-in. The ledger moves to the target only after `peer channel update`."""

    def __init__(self, target=40, broadcast_exit=0):
        self.calls, self.target, self.broadcast_exit = [], target, broadcast_exit
        self.before = block_doc()
        after = copy.deepcopy(self.before)
        after["header"]["number"] = "8"
        cfg = after["data"]["data"][0]["payload"]["data"]["config"]
        cfg["sequence"] = "1"
        cfg["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]["version"] = "1"
        cfg["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]["value"]["max_message_count"] = target
        self.after, self.committed = after, False
        self.verifier_responses = [
            {"valid": True, "signature_count": 0, "outer_signature_present": False, "config_update_sha256": "c" * 64, "fabric_source_commit": COMMIT},
            {"valid": True, "signature_count": 1, "outer_signature_present": True, "config_update_sha256": "c" * 64, "fabric_source_commit": COMMIT},
            {"valid": True, "config_update_sha256": "c" * 64, "fabric_source_commit": COMMIT},
        ]

    def __call__(self, argv, **kwargs):
        argv = [str(a) for a in argv]
        self.calls.append(argv)
        out = b""
        if argv[0] == "bash":
            out = json.dumps(self.verifier_responses.pop(0)).encode()
        elif argv[1:4] == ["channel", "fetch", "config"]:
            Path(argv[4]).write_bytes(b"pb")
        elif argv[1:3] == ["channel", "update"]:
            if self.broadcast_exit == 0:
                self.committed = True
            return subprocess.CompletedProcess(argv, self.broadcast_exit, b"", b"")
        elif argv[1:3] == ["channel", "signconfigtx"]:
            pass
        elif argv[1] == "proto_decode":
            kind, output = argv[argv.index("--type") + 1], Path(argv[argv.index("--output") + 1])
            if kind == "common.Block":
                output.write_text(json.dumps(self.after if self.committed else self.before), encoding="utf-8")
            else:
                output.write_text(json.dumps({"read_set": {"groups": {"Orderer": {"values": {}}}},
                                              "write_set": {"groups": {"Orderer": {"values": {"BatchSize": {"value": {"max_message_count": self.target}}}}}}}), encoding="utf-8")
        elif argv[1] in ("proto_encode", "compute_update"):
            Path(argv[argv.index("--output") + 1]).write_bytes(b"x")
        return subprocess.CompletedProcess(argv, 0, out, b"")


def live_actuator(tmp_path, monkeypatch, world, authorization):
    verifier = independent.IndependentFabricConfigVerifier(
        peer=Path("peer"), configtxlator=Path("configtxlator"), fabric_cfg=tmp_path, msp_id="Org1MSP",
        msp_path=tmp_path / "org1msp", peer_address="localhost:7051", peer_tls_root_cert=tmp_path / "tls.pem",
        orderer="localhost:7050", orderer_tls_hostname="orderer.example.com", orderer_ca=tmp_path / "ca.pem",
        output_dir=tmp_path / "verify")
    target = ca.FabricConfig(world.target, 2000, 10485760, 103809024, 3)
    verifier.verify_transition = lambda *_: {
        "success": True, "independent": True, "complete": True, "outcome": "VERIFIED",
        "source": "peer channel fetch config + configtxlator proto_decode", "observed_config": target.as_dict(),
        "expected_config_document": independent.expected_batch_size_config(world.before, world.target),
        "transition_checks": {"full_transition": True}, "timestamp_utc": "2026-11-23T12:00:01Z"}
    act = ca.FabricChannelConfigActuator(
        channel="e1-cl01", peer_bin=Path("peer"), configtxlator_bin=Path("configtxlator"), fabric_cfg_path=tmp_path,
        orderer_address="localhost:7050", orderer_tls_hostname="orderer.example.com", orderer_ca=tmp_path / "ca.pem",
        orderer_admin_msp=tmp_path / "msp", output_dir=tmp_path / "out", fence_dir=tmp_path / "fence",
        runner=world, live_enabled=True, independent_verifier=verifier, broadcast_authorization=authorization,
        poll_s=0.001, commit_timeout_s=2)
    monkeypatch.setattr(act, "_assert_signing_msp_key_permissions", lambda: None)
    monkeypatch.setattr(ca.os, "fstat", lambda _fd: type("Stat", (), {
        "st_mode": 0o100600, "st_uid": os.geteuid() if hasattr(os, "geteuid") else 0})())
    return act, target


def test_apply_without_authorization_never_broadcasts(tmp_path, monkeypatch):
    world = FakeWorld()
    act, target = live_actuator(tmp_path, monkeypatch, world, None)
    with pytest.raises(ca.ActuatorError, match="remains disabled"):
        act.apply(target)
    assert not any(c[1:3] == ["channel", "update"] for c in world.calls)
    assert act.transition_fence.read()["status"] == "NOT_SUBMITTED"


def test_apply_with_authorization_broadcasts_once_after_verification_and_verifies_commit(tmp_path, monkeypatch):
    world = FakeWorld()
    a = ca.BroadcastAuthorization.load(write_auth(tmp_path, auth_text(approved_utc="2026-11-22T00:00:00Z", expires_utc="2099-01-01T00:00:00Z")))
    act, target = live_actuator(tmp_path, monkeypatch, world, a)
    record = act.apply(target)
    updates = [i for i, c in enumerate(world.calls) if c[1:3] == ["channel", "update"]]
    assert len(updates) == 1
    verify_idx = max(i for i, c in enumerate(world.calls) if c[0] == "bash")
    assert updates[0] > verify_idx                      # broadcast only after the native verification
    u = world.calls[updates[0]]
    assert u == ["peer", "channel", "update", "-f", u[4], "-c", "e1-cl01", "-o", "localhost:7050",
                 "--ordererTLSHostnameOverride", "orderer.example.com", "--tls", "--cafile", str(tmp_path / "ca.pem")]
    assert u[4].endswith("config_update_in_envelope.pb")
    assert record.success is True and record.after_config["max_message_count"] == 40
    assert record.verification_checks["independent_verification"] is True
    assert act.transition_fence.read()["status"] == "VERIFIED"
    attempt = Path(record.artifacts_dir)
    assert json.loads((attempt / "broadcast_authorization.json").read_text())["sha256"] == a.sha256


def test_apply_with_authorization_for_other_target_refuses_before_broadcast(tmp_path, monkeypatch):
    world = FakeWorld(target=45)
    a = ca.BroadcastAuthorization.load(write_auth(tmp_path, auth_text(approved_utc="2026-11-22T00:00:00Z", expires_utc="2099-01-01T00:00:00Z")))
    act, target = live_actuator(tmp_path, monkeypatch, world, a)
    with pytest.raises(ca.ActuatorError, match="does not cover MaxMessageCount 45"):
        act.apply(target)
    assert not any(c[1:3] == ["channel", "update"] for c in world.calls)


def test_native_verification_defaults_to_required_and_rejects_other_values(tmp_path):
    assert ca.BroadcastAuthorization.load(write_auth(tmp_path, auth_text()), now=NOW).native_verification == "required"
    with pytest.raises(ca.SafetyError, match="required or waived"):
        ca.BroadcastAuthorization.load(write_auth(tmp_path, auth_text() + "native_verification=skip\n", "b.txt"), now=NOW)


def test_owner_waived_native_verification_broadcasts_proven_path_without_go_verifier(tmp_path, monkeypatch):
    world = FakeWorld()
    world.verifier_responses = []                      # any call to the Go verifier would raise IndexError
    a = ca.BroadcastAuthorization.load(write_auth(tmp_path, auth_text(approved_utc="2026-11-22T00:00:00Z", expires_utc="2099-01-01T00:00:00Z")
                                                  + "native_verification=waived\n"))
    act, target = live_actuator(tmp_path, monkeypatch, world, a)
    record = act.apply(target)
    assert not any(c[0] == "bash" or "signconfigtx" in c for c in world.calls)
    assert sum(c[1:3] == ["channel", "update"] for c in world.calls) == 1
    assert record.success is True and record.verification_checks["independent_verification"] is True
    evidence = json.loads((Path(record.artifacts_dir) / "broadcast_authorization.json").read_text())
    assert evidence["native_envelope_verification"] == "WAIVED_BY_OWNER"


def test_failed_broadcast_leaves_fence_unknown_and_no_success(tmp_path, monkeypatch):
    world = FakeWorld(broadcast_exit=1)
    a = ca.BroadcastAuthorization.load(write_auth(tmp_path, auth_text(approved_utc="2026-11-22T00:00:00Z", expires_utc="2099-01-01T00:00:00Z")))
    act, target = live_actuator(tmp_path, monkeypatch, world, a)
    with pytest.raises(ca.ActuatorError, match="broadcast_update failed"):
        act.apply(target)
    assert act.transition_fence.read()["status"] == "UNKNOWN"     # outcome unknown -> independent reconciliation required


def test_committed_actuator_is_still_the_a0b_start_version():
    """A0b (2026-11-20) left e1_config_actuator.py alone: the committed blob still has the A0b-start hash. The working-tree
    revision (owner-authorized broadcast + channel guard, 2026-11-22) is uncommitted and is recorded in the cs01 evidence."""
    import hashlib
    blob = subprocess.run(["git", "-C", str(ROOT), "cat-file", "-p", "HEAD:scripts/e1/e1_config_actuator.py"], capture_output=True)
    if blob.returncode != 0:
        pytest.skip("git history unavailable")
    assert hashlib.sha256(blob.stdout).hexdigest().startswith("f4b979535b9abb4c")