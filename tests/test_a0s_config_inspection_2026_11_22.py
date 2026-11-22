"""A0s workstream C: read-only channel-configuration inspection. Offline: stored/synthetic decoded blocks and a mock runner. Nothing starts a process."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from tests import a0s_support as s
from tests.a0s_support import CELL, CHANNEL, REPO, decoded_block

from fabric_perf.controllers import config_inspection as ci      # noqa: E402

KW = dict(peer="/opt/fabric/bin/peer", configtxlator="/opt/fabric/bin/configtxlator", orderer="localhost:7050", orderer_tls_hostname="orderer.example.com", orderer_ca="/opt/ca.pem")
CAPTURED = "2026-11-22T10:00:00Z"


def plan(**kw):
    return ci.build_inspection_plan(kw.pop("channel", CHANNEL), kw.pop("expected", CELL), out_dir=kw.pop("out_dir", "/tmp/a0s/inspect"), **{**KW, **kw})


def test_01_plan_builds_only_read_only_vectors():
    p = plan()
    assert [st["step"] for st in p["steps"]] == ["peer_version", "configtxlator_version", "peer_fetch_config", "configtxlator_decode"]
    assert all(st["read_only"] and st["mutates_fabric"] is False for st in p["steps"])
    for st in p["steps"]:
        ci.validate_read_only_argv(st["argv"])
    assert p["live_inspection_enabled"] is False and p["cell_id"] == "b40-t2000ms-p10485760-a103809024-o3"


def test_02_plan_vectors_equal_the_existing_independent_verifier_vectors():
    """The fetch and decode vectors are the ones scripts/e1/e1_independent_config_verify.py builds (exercised with a recording fake runner, no process)."""
    import e1_independent_config_verify as v

    seen = []

    class Done:
        returncode, stdout, stderr = 0, b"Version: 2.5.0\n", b""

    def runner(argv, **kwargs):
        seen.append(list(argv))
        return Done()

    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        verifier = v.IndependentFabricConfigVerifier(peer=Path(KW["peer"]), configtxlator=Path(KW["configtxlator"]), fabric_cfg=Path("/cfg"), msp_id="Org1MSP", msp_path=Path("/msp"),
                                                     peer_address="localhost:7051", peer_tls_root_cert=Path("/tls.pem"), orderer=KW["orderer"],
                                                     orderer_tls_hostname=KW["orderer_tls_hostname"], orderer_ca=Path(KW["orderer_ca"]), output_dir=Path(tmp), runner=runner)
        with pytest.raises(Exception):
            verifier.retrieve(CHANNEL)                # the fake produces no block file, so it stops after the fetch; the vectors are what matters
    norm = lambda argv: [a.replace("\\", "/") for a in argv]       # the existing verifier builds paths with pathlib (backslashes on Windows)
    fetch = norm(next(a for a in seen if a[1:4] == ["channel", "fetch", "config"]))
    mine = norm(next(st["argv"] for st in plan()["steps"] if st["step"] == "peer_fetch_config"))
    assert fetch[:4] == mine[:4] and fetch[5:] == mine[5:]          # position 4 is the output file path (differs by directory)
    assert [a for a in fetch if a.startswith("-")] == [a for a in mine if a.startswith("-")]


@pytest.mark.parametrize("argv", [
    ["peer", "channel", "create", "-c", "x", "-o", "o"], ["peer", "channel", "join", "-b", "x.block"], ["peer", "channel", "update", "-c", "x", "-f", "u.pb"],
    ["peer", "channel", "signconfigtx", "-f", "u.pb"], ["peer", "lifecycle", "chaincode", "install", "cc.tgz"], ["peer", "chaincode", "invoke", "-C", "x"],
    ["peer", "node", "start"], ["peer", "node", "rebuild-dbs"], ["configtxlator", "compute_update", "--channel_id", "x"], ["configtxlator", "proto_encode", "--input", "a"],
    ["orderer", "start"], ["docker", "exec", "c", "peer", "version"], ["wsl.exe", "--shutdown"], ["sh", "-c", "peer channel update"],
    ["peer", "channel", "fetch", "config", "f.pb", "-c", "x", "--evil", "1"], ["peer", "channel", "fetch", "config", "-c", "x"],
    ["peer", "channel", "fetch", "config", "f.pb", "-c"], ["peer", "version", "--anything"], []])
def test_03_every_mutation_capable_or_unknown_vector_is_refused(argv):
    with pytest.raises(ci.MutationRefused):
        ci.validate_read_only_argv(argv)


def test_04_a_forbidden_word_anywhere_in_an_otherwise_allowed_vector_is_refused():
    with pytest.raises(ci.MutationRefused):
        ci.validate_read_only_argv(["peer", "channel", "fetch", "config", "f.pb", "-c", "x", "-o", "create"])


@pytest.mark.parametrize("channel", ["e1-r002", "e1-cfg001", "e1-b001", "e1-ql01-b10-l15", "e1-cp03-b40-l20", "", None, "UPPER", "e1-rp01-b10-r1"])
def test_05_protected_historical_reserved_or_malformed_channel_is_refused_everywhere(channel):
    with pytest.raises(ci.InspectionError) as e:
        plan(channel=channel)
    assert e.value.code == "channel_refused"
    with pytest.raises(ci.InspectionError):
        ci.inspect_decoded_block(decoded_block(), channel=channel, expected=CELL, captured_utc=CAPTURED)


def test_06_correct_configuration_matches_and_reports_every_required_fact():
    ev = ci.inspect_decoded_block(decoded_block(sequence=7, number=3), channel=CHANNEL, expected=CELL, captured_utc=CAPTURED, block_sha256="a" * 64)
    assert ev["verdict"] == "MATCH" and ev["differing_fields"] == []
    assert (ev["channel"], ev["config_block_number"], ev["config_sequence"], ev["captured_utc"]) == (CHANNEL, 3, 7, "2026-11-22T10:00:00.000+00:00")
    assert ev["observed"] == CELL and ev["batch_timeout"] == "2s"
    assert len(ev["channel_config_sha256"]) == 64 and len(ev["orderer_batch_projection_sha256"]) == 64 and ev["raw_block_sha256"] == "a" * 64
    assert ev["cell_id"] == "b40-t2000ms-p10485760-a103809024-o3" and ev["read_only"] is True


@pytest.mark.parametrize("kwargs, field", [({"B": 10}, "max_message_count"), ({"timeout": "1s"}, "batch_timeout_ms"), ({"preferred": 5242880}, "preferred_max_bytes"),
                                           ({"absolute": 52428800}, "absolute_max_bytes"), ({"consenters": 5}, "orderers")])
def test_07_wrong_cell_is_a_mismatch_naming_the_field(kwargs, field):
    ev = ci.inspect_decoded_block(decoded_block(**kwargs), channel=CHANNEL, expected=CELL, captured_utc=CAPTURED)
    assert ev["verdict"] == "MISMATCH" and field in ev["differing_fields"]


def test_08_block_of_another_channel_is_rejected():
    with pytest.raises(ci.InspectionError) as e:
        ci.inspect_decoded_block(decoded_block("e1-other-channel"), channel=CHANNEL, expected=CELL, captured_utc=CAPTURED)
    assert e.value.code == "wrong_channel"


@pytest.mark.parametrize("mutate", [
    lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"].pop("BatchSize"),
    lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"].pop("BatchTimeout"),
    lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]["value"].pop("preferred_max_bytes"),
    lambda b: b["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]["value"].pop("max_message_count"),
    lambda b: b["data"]["data"].clear(), lambda b: b.pop("header"), lambda b: b["data"].pop("data")])
def test_09_missing_configuration_fields_are_rejected_not_defaulted(mutate):
    block = decoded_block()
    mutate(block)
    with pytest.raises(ci.InspectionError):
        ci.inspect_decoded_block(block, channel=CHANNEL, expected=CELL, captured_utc=CAPTURED)


def test_10_a_non_config_block_is_rejected():
    with pytest.raises(ci.InspectionError) as e:
        ci.inspect_decoded_block(decoded_block(header_type=3), channel=CHANNEL, expected=CELL, captured_utc=CAPTURED)
    assert e.value.code in ("malformed_block", "config_fields_invalid")


@pytest.mark.parametrize("sequence", [None, "x", -1, 1.5, True, "", [], {"a": 1}, 2 ** 64])
def test_11_invalid_configuration_sequence_is_rejected(sequence):
    block = decoded_block()
    config = block["data"]["data"][0]["payload"]["data"]["config"]
    config["sequence"] = sequence
    if sequence is None:
        config.pop("sequence")
    with pytest.raises(ci.InspectionError) as e:
        ci.inspect_decoded_block(block, channel=CHANNEL, expected=CELL, captured_utc=CAPTURED)
    assert e.value.code in ("config_sequence_missing", "config_sequence_invalid")


def test_12_string_sequence_from_protobuf_json_is_accepted_and_an_expected_sequence_is_enforced():
    ev = ci.inspect_decoded_block(decoded_block(sequence="4"), channel=CHANNEL, expected=CELL, captured_utc=CAPTURED, expected_sequence=4)
    assert ev["config_sequence"] == 4
    with pytest.raises(ci.InspectionError) as e:
        ci.inspect_decoded_block(decoded_block(sequence="4"), channel=CHANNEL, expected=CELL, captured_utc=CAPTURED, expected_sequence=5)
    assert e.value.code == "config_sequence_unexpected"


@pytest.mark.parametrize("bad", [None, "", "2026-11-22T10:00:00", "yesterday", 123])
def test_13_capture_timestamp_must_be_explicit_and_timezone_aware(bad):
    with pytest.raises(ci.InspectionError):
        ci.inspect_decoded_block(decoded_block(), channel=CHANNEL, expected=CELL, captured_utc=bad)


@pytest.mark.parametrize("drop", ["max_message_count", "batch_timeout_ms", "preferred_max_bytes", "absolute_max_bytes", "orderers"])
def test_14_every_expected_field_is_required(drop):
    cell = {k: v for k, v in CELL.items() if k != drop}
    with pytest.raises(ci.InspectionError) as e:
        plan(expected=cell)
    assert e.value.code == "expected_incomplete"


@pytest.mark.parametrize("raw", [b"", b"not json", b"[1,2,3]", b"\xff\xfe"])
def test_15_malformed_command_output_is_rejected(raw):
    p = plan()
    runner = s.MockRunner(s.FakeClock())
    adapter = lambda argv, timeout: (0, b"", b"")
    adapter.kind = "mock"
    with pytest.raises(ci.InspectionError) as e:
        ci.execute_inspection(p, adapter, read_file=lambda path: raw, captured_utc=CAPTURED)
    assert e.value.code in ("malformed_block",)


def test_16_failed_command_is_reported_and_nothing_after_it_runs():
    p = plan()
    calls = []

    def runner(argv, timeout):
        calls.append(list(argv))
        return (1, b"", b"boom") if "fetch" in argv else (0, b"Version: 2\n", b"")

    runner.kind = "fixture"
    with pytest.raises(ci.InspectionError) as e:
        ci.execute_inspection(p, runner, read_file=lambda path: b"{}", captured_utc=CAPTURED)
    assert e.value.code == "command_failed" and not any("proto_decode" in c for c in calls)


def test_17_injected_runner_path_returns_evidence_and_every_vector_was_validated():
    p = plan()
    calls = []

    def runner(argv, timeout):
        calls.append(list(argv))
        return 0, b"Version: 2.5.0\n", b""

    runner.kind = "mock"
    ev = ci.execute_inspection(p, runner, read_file=lambda path: json.dumps(decoded_block(sequence=2)).encode(), captured_utc=CAPTURED)
    assert ev["verdict"] == "MATCH" and ev["config_sequence"] == 2 and [c["step"] for c in ev["commands"]] == [st["step"] for st in p["steps"]]
    assert calls == [st["argv"] for st in p["steps"]]
    for argv in calls:
        ci.validate_read_only_argv(argv)


@pytest.mark.parametrize("kind", ["process", None, "live", "real"])
def test_18_live_inspection_is_disabled_a_process_runner_is_refused(kind):
    p = plan()

    def runner(argv, timeout):                       # pragma: no cover - must never be called
        raise AssertionError("a refused runner must not be called")

    runner.kind = kind
    with pytest.raises(ci.MutationRefused) as e:
        ci.execute_inspection(p, runner, read_file=lambda path: b"{}", captured_utc=CAPTURED)
    assert (kind != "process") or "live inspection is disabled in this version" in str(e.value)
    assert ci.LIVE_INSPECTION_ENABLED is False


def test_19_tampered_plan_vector_is_refused_at_execution_time():
    p = copy.deepcopy(plan())
    p["steps"][2]["argv"][1:3] = ["channel", "update"]
    runner = lambda argv, timeout: (0, b"", b"")
    runner.kind = "mock"
    with pytest.raises(ci.MutationRefused):
        ci.execute_inspection(p, runner, read_file=lambda path: b"{}", captured_utc=CAPTURED)


def test_20_comparison_detects_a_sequence_block_or_digest_change_and_accepts_identical_evidence():
    a = ci.inspect_decoded_block(decoded_block(sequence=1, number=1), channel=CHANNEL, expected=CELL, captured_utc=CAPTURED)
    same = ci.inspect_decoded_block(decoded_block(sequence=1, number=1), channel=CHANNEL, expected=CELL, captured_utc="2026-11-22T10:05:00Z")
    assert ci.compare_inspections(a, same)["unchanged"] is True
    for changed in (decoded_block(sequence=2, number=1), decoded_block(sequence=1, number=2), decoded_block(sequence=1, number=1, preferred=5242880)):
        later = ci.inspect_decoded_block(changed, channel=CHANNEL, expected=CELL, captured_utc="2026-11-22T10:05:00Z")
        assert ci.compare_inspections(a, later)["unchanged"] is False


def test_21_cli_plan_parse_and_refusal(tmp_path, capsys):
    import e1_a0s_config_inspect as cli

    expect = tmp_path / "cell.json"
    expect.write_text(json.dumps(CELL), encoding="utf-8")
    block = tmp_path / "decoded.json"
    block.write_text(json.dumps(decoded_block(sequence=1)), encoding="utf-8")
    out = tmp_path / "evidence.json"
    assert cli.main(["parse", "--channel", CHANNEL, "--expect-json", str(expect), "--block", str(block), "--captured-utc", CAPTURED, "--out", str(out)]) == 0
    assert json.loads(out.read_text(encoding="utf-8"))["verdict"] == "MATCH"
    assert cli.main(["parse", "--channel", CHANNEL, "--expect-json", str(expect), "--block", str(block), "--captured-utc", CAPTURED, "--out", str(out)]) == 2      # never overwritten
    assert cli.main(["parse", "--channel", "e1-r002", "--expect-json", str(expect), "--block", str(block), "--captured-utc", CAPTURED, "--out", str(tmp_path / "x.json")]) == 2
    assert cli.main(["plan", "--channel", CHANNEL, "--expect-json", str(expect), "--peer", "/x/peer", "--configtxlator", "/x/configtxlator", "--out-dir", str(tmp_path), "--orderer", "o:1",
                     "--orderer-tls-hostname", "h", "--orderer-ca", "ca"]) == 0
    assert cli.main(["inspect", "--anything"]) == 5
    assert "REFUSED" in capsys.readouterr().out
