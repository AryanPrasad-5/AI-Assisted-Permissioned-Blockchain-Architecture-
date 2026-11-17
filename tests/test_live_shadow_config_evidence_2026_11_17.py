"""Live-shadow fix 1 (2026-11-17): no hard-coded block size on the live-shadow path.

The Phase 2A.3 frame builder wrote ``max_message_count: 40`` into every frame. A frame's block size must
come from validated evidence (a decoded CONFIG block or an explicit complete input), carry provenance,
and never default. Fabric MaxMessageCount is carried as such; it is not assumed equal to HFBTP Block Size.
"""

import hashlib
import importlib.util
import json
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from fabric_perf.controllers import (
    ConfigEvidenceError,
    FabricConfig,
    ProductionTelemetryProvider,
    PrometheusTelemetryAdapter,
)
from fabric_perf.controllers import config_evidence as ce
from fabric_perf.controllers.base import PerformancePrediction
from fabric_perf.controllers.live_integration import IntegrationSafetyError, load_jsonl_frames

REPO = Path(__file__).resolve().parents[1]
BUILDER = REPO / "scripts" / "e1" / "build_contemporaneous_transaction_evidence.py"
INTEGRATED_CLI = REPO / "Integration" / "system_integration" / "run_integrated_controller.py"
SHADOW_CLI = REPO / "Fabric_Live_Testbed" / "controllers" / "run_production_shadow_validation.py"
STORED_R002 = sorted((REPO / "Integration/system_integration/observability_recovery_2026-11-17/raw/05_config_retrieval"
                      ).glob("verify-e1-r002-*/committed_config.json"))
HISTORICAL_B40 = {"max_message_count": 40, "batch_timeout_ms": 2000, "batch_timeout": "2s",
                  "preferred_max_bytes": 10485760, "absolute_max_bytes": 103809024, "orderers": 3}


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def block_doc(channel="e1-new", number=0, B=45, timeout="2s", P=10_485_760, A=103_809_024, consenters=3, header_type=1):
    return {"header": {"number": str(number)}, "data": {"data": [{"payload": {
        "header": {"channel_header": {"channel_id": channel, "type": header_type}},
        "data": {"config": {"channel_group": {"groups": {"Orderer": {"values": {
            "BatchSize": {"value": {"max_message_count": B, "preferred_max_bytes": P, "absolute_max_bytes": A}},
            "BatchTimeout": {"value": {"timeout": timeout}},
            "ConsensusType": {"value": {"type": "etcdraft", "metadata": {"consenters": [{} for _ in range(consenters)]}}},
        }}}}}}}}]}}


def explicit(B=45, **over):
    return {"max_message_count": B, "batch_timeout_ms": 2000, "preferred_max_bytes": 10_485_760,
            "absolute_max_bytes": 103_809_024, "orderers": 3, **over}


# ------------------------------------------------------------- parsing and validation
@pytest.mark.parametrize("text, ms", [("2s", 2000), ("500ms", 500), ("1.5s", 1500), ("2000ms", 2000), ("1m", 60000),
                                      (" 2s ", 2000)])
def test_go_durations_are_parsed_in_whole_milliseconds(text, ms):
    assert ce.parse_batch_timeout_ms(text) == ms


@pytest.mark.parametrize("bad", ["", "2", "fast", "0.0005s", "-1s", "1h", 2, True, None])
def test_other_durations_are_refused(bad):
    with pytest.raises(ConfigEvidenceError):
        ce.parse_batch_timeout_ms(bad)


@pytest.mark.parametrize("B", [10, 40, 45, 800])
def test_canonical_config_has_exactly_the_shape_of_the_controller_configuration(B):
    assert ce.canonical_config(explicit(B)) == FabricConfig(max_message_count=B).as_dict()
    assert list(ce.canonical_config(explicit(B))) == list(FabricConfig(max_message_count=B).as_dict())


@pytest.mark.parametrize("over", [{"max_message_count": 0}, {"max_message_count": 2 ** 32}, {"max_message_count": True},
                                  {"max_message_count": 45.5}, {"max_message_count": "forty"},
                                  {"batch_timeout_ms": 0}, {"batch_timeout_ms": 60_001}, {"orderers": 0},
                                  {"preferred_max_bytes": 0}, {"preferred_max_bytes": 200_000_000}])
def test_out_of_range_or_mistyped_values_are_refused_as_the_controller_configuration_refuses_them(over):
    values = explicit(**over)
    with pytest.raises(ConfigEvidenceError):
        ce.canonical_config(values)
    with pytest.raises(ValueError):                                        # same rule set as FabricConfig
        FabricConfig(**{k: v for k, v in values.items()})


def test_missing_or_partial_values_are_never_filled_in():
    for missing in ce.CANONICAL_FIELDS:
        partial = {k: v for k, v in explicit().items() if k != missing}
        with pytest.raises(ConfigEvidenceError, match=missing):
            ce.canonical_config(partial)
    with pytest.raises(ConfigEvidenceError, match="no default block size is substituted"):
        ce.build_evidence(channel="e1-new")
    with pytest.raises(ConfigEvidenceError, match="incomplete"):
        ce.build_evidence(channel="e1-new", explicit={"max_message_count": 45})


# ---------------------------------------------------------------- decoded blocks
@pytest.mark.parametrize("B", [40, 45])
def test_block_size_comes_from_the_block_and_propagates(B):
    evidence = ce.build_evidence(channel="e1-new", block=block_doc(B=B))
    assert evidence.config["max_message_count"] == B
    assert evidence.provenance["kind"] == "config_block" and evidence.provenance["config_block_number"] == 0
    assert "not asserted equal to the HFBTP Block Size" in evidence.provenance["max_message_count_semantics"]
    assert evidence.provenance["no_default_substituted"] is True


def test_millisecond_timeouts_are_read_correctly():
    assert ce.build_evidence(channel="e1-new", block=block_doc(timeout="2000ms")).config["batch_timeout_ms"] == 2000
    assert ce.build_evidence(channel="e1-new", block=block_doc(timeout="1500ms")).config["batch_timeout"] == "1500ms"


@pytest.mark.parametrize("doc, match", [
    (block_doc(channel="e1-other"), "not 'e1-new'"),
    (block_doc(header_type=3), "not a CONFIG block"),
    (block_doc(consenters=0), "no consenters"),
    ({"header": {"number": "0"}}, "not a decoded CONFIG block"),
    (block_doc(timeout="soon"), "unsupported BatchTimeout"),
    (block_doc(B=0), "outside"),
])
def test_only_a_valid_config_block_for_this_channel_is_accepted(doc, match):
    with pytest.raises(ConfigEvidenceError, match=match):
        ce.build_evidence(channel="e1-new", block=doc)


@pytest.mark.skipif(not STORED_R002, reason="stored e1-r002 configuration evidence not present")
def test_the_stored_r002_block_gives_b45_and_it_is_not_the_old_constant():
    block, digest = ce.load_block_file(STORED_R002[0])
    evidence = ce.build_evidence(channel="e1-r002", block=block, block_path=STORED_R002[0], block_sha256=digest)
    assert evidence.config["max_message_count"] == 45 and evidence.config["batch_timeout_ms"] == 2000
    assert evidence.config["orderers"] == 3 and evidence.provenance["source_sha256"] == digest
    with pytest.raises(ConfigEvidenceError, match="not 'e1-cfg001'"):
        ce.build_evidence(channel="e1-cfg001", block=block)


# ------------------------------------------------------ ambiguous and stale evidence
def test_block_and_explicit_input_must_agree():
    ok = ce.build_evidence(channel="e1-new", block=block_doc(B=45), explicit=explicit(45))
    assert ok.provenance["cross_checked_explicit_input"] is True
    with pytest.raises(ConfigEvidenceError, match=r"ambiguous.*max_message_count"):
        ce.build_evidence(channel="e1-new", block=block_doc(B=45), explicit=explicit(40))


def test_the_operators_expectation_is_checked_not_assumed():
    assert ce.build_evidence(channel="e1-new", block=block_doc(B=45), expect_max_message_count=45)
    with pytest.raises(ConfigEvidenceError, match="operator expected 40"):
        ce.build_evidence(channel="e1-new", block=block_doc(B=45), expect_max_message_count=40)


def test_stale_or_ambiguous_evidence_is_refused():
    with pytest.raises(ConfigEvidenceError, match="postdates the first run block"):
        ce.build_evidence(channel="e1-new", block=block_doc(number=20), run_first_block=10)
    ce.build_evidence(channel="e1-new", block=block_doc(number=10), run_first_block=10)         # equal is fine
    with pytest.raises(ConfigEvidenceError, match=r"CONFIG block\(s\) \[12\]"):
        ce.build_evidence(channel="e1-new", block=block_doc(number=0), run_first_block=10, config_blocks_in_run=[12])
    ce.build_evidence(channel="e1-new", block=block_doc(number=0), run_first_block=10, config_blocks_in_run=[0])
    with pytest.raises(ConfigEvidenceError, match=r"CONFIG block\(s\) \[12\]"):
        ce.build_evidence(channel="e1-new", explicit=explicit(), run_first_block=10, config_blocks_in_run=[12])
    with pytest.raises(ConfigEvidenceError, match="appear in the run"):
        ce.build_evidence(channel="e1-new", explicit=explicit(), config_blocks_in_run=[3])       # run start unknown
    ce.build_evidence(channel="e1-new", explicit=explicit(), run_first_block=10, config_blocks_in_run=[10])


# ------------------------------------------------------- the Phase 2A.3 frame builder
BASE_MS = 1_790_000_000_000


def iso_ms(offset_ms):
    return datetime.fromtimestamp((BASE_MS + offset_ms) / 1000, tz=timezone.utc).isoformat(timespec="milliseconds")


def write_run(tmp_path, channel="e1-new", config_row_block=None):
    folder = tmp_path / "inputs"
    folder.mkdir()
    (folder / "recorder.csv").write_text(
        "tx_id,seq_finish,create_ms,final_ms,status,verified,err,obs_hr_ns,worker,round,label\n" + "".join(
            f"{tx},{i},{BASE_MS + 200 * i},{BASE_MS + 1000 + 200 * i},success,,,{i},0,1,m-test\n"
            for i, tx in enumerate("abc")), encoding="utf-8")
    (folder / "submits.csv").write_text("seq_submit,t_submit_evt_ms,count\n" + "".join(
        f"{i},{BASE_MS + 200 * i},1\n" for i in range(3)), encoding="utf-8")
    (folder / "manifest.json").write_text(json.dumps(
        {"clock": {"wall_elapsed_ms": 1500}, "counts": {"pending_at_deactivate": 0}}), encoding="utf-8")
    rows = [f"{tx},{10 if i < 2 else 11},{i % 2},3,0,100,{channel},{BASE_MS + 900 + 200 * i}" for i, tx in enumerate("abc")]
    if config_row_block is not None:
        rows.append(f"cfg,{config_row_block},0,1,0,100,{channel},{BASE_MS + 3000}")
    (folder / "ledger_tx.csv").write_text("tx_id,block,index,header_type,validation_code,env_bytes,channel_id,ch_ts_ms\n"
                                          + "\n".join(rows) + "\n", encoding="utf-8")
    (folder / "observations.jsonl").write_text("".join(json.dumps(
        {"block_number": number, "observed_timestamp_utc": iso_ms(offset), "status": "ok"}) + "\n"
        for number, offset in ((10, 1800), (11, 2200))), encoding="utf-8")
    return folder


def build(tmp_path, channel, *extra, folder=None):
    folder = folder or write_run(tmp_path, channel)
    module = load_module(BUILDER, "builder_under_test")
    out = tmp_path / "out"
    argv = ["--run-id", "t1", "--channel", channel, "--recorder-csv", str(folder / "recorder.csv"),
            "--submits-csv", str(folder / "submits.csv"), "--manifest", str(folder / "manifest.json"),
            "--ledger-tx", str(folder / "ledger_tx.csv"), "--ledger-observations", str(folder / "observations.jsonl"),
            "--out-dir", str(out), *extra]
    return module.main(argv), out


def frames_of(out, name="controller_frames_all.jsonl"):
    return [json.loads(line) for line in (out / name).read_text(encoding="utf-8").splitlines() if line.strip()]


def explicit_args(B=45, **over):
    values = explicit(B, **over)
    return ["--max-message-count", str(values["max_message_count"]), "--batch-timeout-ms", str(values["batch_timeout_ms"]),
            "--preferred-max-bytes", str(values["preferred_max_bytes"]),
            "--absolute-max-bytes", str(values["absolute_max_bytes"]), "--orderers", str(values["orderers"])]


@pytest.mark.parametrize("B", [45, 40, 10])
def test_builder_carries_the_supplied_block_size_into_every_frame(tmp_path, B):
    rc, out = build(tmp_path, "e1-new", *explicit_args(B), "--expect-max-message-count", str(B))
    assert rc == 0
    frames = frames_of(out)
    assert len(frames) == 2
    for frame in frames:
        assert frame["state"]["current_config"]["max_message_count"] == B
        source = frame["provenance"]["configuration_source"]
        assert source["kind"] == "explicit_operator_input" and source["channel_id"] == "e1-new"
        assert source["expected_max_message_count"] == B and source["validated_against_run"] is True
    summary = json.loads((out / "evidence_summary.json").read_text(encoding="utf-8"))
    assert summary["configuration"]["max_message_count"] == B and summary["tool"].startswith(
        "build_contemporaneous_transaction_evidence 1.1")
    assert summary["configuration_evidence"]["no_default_substituted"] is True


def test_builder_with_explicit_b40_reproduces_the_historical_configuration_exactly(tmp_path):
    rc, out = build(tmp_path, "e1-cfg001", *explicit_args(40))
    assert rc == 0
    assert all(frame["state"]["current_config"] == HISTORICAL_B40 for frame in frames_of(out))


def test_builder_reads_the_configuration_from_a_decoded_block(tmp_path):
    block_path = tmp_path / "committed_config.json"
    block_path.write_text(json.dumps(block_doc(channel="e1-new", number=0, B=45)), encoding="utf-8")
    rc, out = build(tmp_path, "e1-new", "--config-block-json", str(block_path))
    assert rc == 0
    for frame in frames_of(out):
        source = frame["provenance"]["configuration_source"]
        assert frame["state"]["current_config"]["max_message_count"] == 45
        assert source["kind"] == "config_block" and source["config_block_number"] == 0
        assert source["source_sha256"] == hashlib.sha256(block_path.read_bytes()).hexdigest()
        assert source["run_first_block"] == 10 and source["validated_against_run"] is True


def refused(tmp_path, channel, *extra, folder=None):
    with pytest.raises(SystemExit) as caught:
        build(tmp_path, channel, *extra, folder=folder)
    assert caught.value.code == 2
    assert not (tmp_path / "out").exists()                    # a refused configuration writes nothing at all


def test_builder_refuses_to_run_without_configuration_evidence(tmp_path, capsys):
    refused(tmp_path, "e1-new")
    assert "no configuration evidence supplied" in capsys.readouterr().err


def test_builder_refuses_partial_explicit_configuration(tmp_path, capsys):
    refused(tmp_path, "e1-new", "--max-message-count", "45")
    assert "incomplete" in capsys.readouterr().err


@pytest.mark.parametrize("over, why", [({"max_message_count": 0}, "outside"), ({"batch_timeout_ms": 0}, "outside"),
                                       ({"orderers": 0}, "outside"), ({"preferred_max_bytes": 10 ** 9}, "exceed")])
def test_builder_refuses_invalid_values(tmp_path, capsys, over, why):
    refused(tmp_path, "e1-new", *explicit_args(**over))
    assert why in capsys.readouterr().err


def test_builder_refuses_a_block_of_another_channel(tmp_path, capsys):
    block_path = tmp_path / "block.json"
    block_path.write_text(json.dumps(block_doc(channel="e1-r002", B=45)), encoding="utf-8")
    refused(tmp_path, "e1-new", "--config-block-json", str(block_path))
    assert "e1-r002" in capsys.readouterr().err


def test_builder_refuses_a_conflicting_operator_expectation(tmp_path, capsys):
    good = tmp_path / "good.json"
    good.write_text(json.dumps(block_doc(channel="e1-new", B=45)), encoding="utf-8")
    refused(tmp_path, "e1-new", "--config-block-json", str(good), "--expect-max-message-count", "40")
    assert "operator expected 40" in capsys.readouterr().err


def test_builder_refuses_block_and_explicit_input_that_disagree(tmp_path, capsys):
    block_path = tmp_path / "block.json"
    block_path.write_text(json.dumps(block_doc(channel="e1-new", B=45)), encoding="utf-8")
    refused(tmp_path, "e1-new", "--config-block-json", str(block_path), *explicit_args(40))
    assert "ambiguous" in capsys.readouterr().err


def test_builder_refuses_stale_evidence_and_a_configuration_change_inside_the_run(tmp_path, capsys):
    late = tmp_path / "late.json"
    late.write_text(json.dumps(block_doc(channel="e1-new", number=30, B=45)), encoding="utf-8")
    refused(tmp_path, "e1-new", "--config-block-json", str(late))
    assert "postdates the first run block 10" in capsys.readouterr().err
    changed = tmp_path / "changed"
    changed.mkdir()
    refused(changed, "e1-new", *explicit_args(45), folder=write_run(changed, "e1-new", config_row_block=11))
    assert "appear in the run" in capsys.readouterr().err


def test_no_block_size_is_written_in_the_builder_or_defaulted_in_the_cli():
    builder = BUILDER.read_text(encoding="utf-8")
    assert '"max_message_count": 40' not in builder and "max_message_count=40" not in builder
    cli = INTEGRATED_CLI.read_text(encoding="utf-8")
    assert "default=40" not in cli and '"40,45"' not in cli and "default=2000" not in cli


# ------------------------------------------------------- the integrated controller CLI
class FakePredictor:
    def __init__(self, artifact_dir, *, candidate_space):
        self.candidate_space = candidate_space

    def predict(self, arrival, config):
        return PerformancePrediction(10.0, 100.0)


class FakeForecaster:
    def __init__(self, series, baseline=None):
        pass

    def predict(self, history, horizon):
        return 10.0


def write_frames(path, B, count=3):
    config = FabricConfig(max_message_count=B).as_dict()
    rows = []
    for i in range(count):
        stamp = f"2026-11-17T12:00:0{i}Z"
        rows.append({"observation_timestamp_utc": stamp,
                     "state": {"timestamp_utc": stamp, "current_config": config, "observed_latency_ms": 100.0,
                               "observed_throughput_tps": 10.0, "observed_arrival_rate_tps": 10.0, "queue_depth": 0.0,
                               "telemetry_age_s": 0.5, "arrival_history_tps": [10.0], "arrival_history_timestamps_utc": [stamp]},
                     "source_timestamps_utc": {"s": stamp}, "freshness_seconds": {"s": 0.1}})
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    return path


def cli_args(tmp_path, snapshots, *extra, out="out"):
    return ["--run-id", "cli", "--snapshots", str(snapshots), "--out-dir", str(tmp_path / out),
            "--hfbtp-artifact-dir", str(tmp_path), "--flashbots-series", str(tmp_path / "series.csv"), *extra]


@pytest.mark.parametrize("B", [45, 40])
def test_integrated_cli_propagates_the_recorded_block_size_to_every_controller(tmp_path, monkeypatch, B):
    module = load_module(INTEGRATED_CLI, "integrated_cli_under_test")
    monkeypatch.setattr(module, "HFBTPArtifactPerformancePredictor", FakePredictor)
    monkeypatch.setattr(module, "FlashbotsSeriesForecaster", FakeForecaster)
    snapshots = write_frames(tmp_path / "frames.jsonl", B)
    assert module.main(cli_args(tmp_path, snapshots, "--batch-size", str(B), "--candidate-batch-sizes", "40,45",
                                "--hfbtp-block-map", "40:40,45:45")) == 0
    records = [json.loads(line) for line in (tmp_path / "out" / "controller_decisions.jsonl").read_text().splitlines()]
    assert len(records) == 9
    for record in records:
        assert record["state"]["current_config"]["max_message_count"] == B
        assert record["decision"]["config"]["max_message_count"] == B
        assert record["configuration_state"]["after"]["max_message_count"] == B
        assert record["application"] is None or record["application"]["error"] == "shadow_mode_no_submission"
    static = next(r for r in records if r["decision"]["controller"] == "static")
    assert static["decision"]["config"]["max_message_count"] == B


def test_integrated_cli_has_no_block_size_default(tmp_path, capsys):
    module = load_module(INTEGRATED_CLI, "integrated_cli_under_test")
    snapshots = write_frames(tmp_path / "frames.jsonl", 45)
    for missing in (["--candidate-batch-sizes", "40,45", "--hfbtp-block-map", "40:40,45:45"],       # no --batch-size
                    ["--batch-size", "45", "--hfbtp-block-map", "40:40,45:45"],                      # no candidates
                    ["--batch-size", "45", "--candidate-batch-sizes", "40,45"]):                     # no block map
        with pytest.raises(SystemExit) as caught:
            module.main(cli_args(tmp_path, snapshots, *missing))
        assert caught.value.code == 2
    assert not (tmp_path / "out").exists()


def test_integrated_cli_refuses_an_asserted_value_that_differs_from_the_recorded_configuration(tmp_path, capsys):
    module = load_module(INTEGRATED_CLI, "integrated_cli_under_test")
    snapshots = write_frames(tmp_path / "frames.jsonl", 45)
    base = ["--candidate-batch-sizes", "40,45", "--hfbtp-block-map", "40:40,45:45"]
    for wrong in (["--batch-size", "40"], ["--batch-size", "45", "--batch-timeout-ms", "1000"],
                  ["--batch-size", "45", "--preferred-max-bytes", "1"]):
        with pytest.raises(SystemExit) as caught:
            module.main(cli_args(tmp_path, snapshots, *base, *wrong))
        assert caught.value.code == 2
        assert "differs from the first frame's recorded configuration" in capsys.readouterr().err
    assert not (tmp_path / "out").exists()


def test_integrated_cli_refuses_frames_that_carry_no_complete_configuration(tmp_path, capsys):
    module = load_module(INTEGRATED_CLI, "integrated_cli_under_test")
    path = tmp_path / "bare.jsonl"
    stamp = "2026-11-17T12:00:00Z"
    path.write_text(json.dumps({"observation_timestamp_utc": stamp, "state": {"timestamp_utc": stamp}}) + "\n", encoding="utf-8")
    with pytest.raises(SystemExit) as caught:
        module.main(cli_args(tmp_path, path, "--batch-size", "45", "--candidate-batch-sizes", "40,45",
                             "--hfbtp-block-map", "40:40,45:45"))
    assert caught.value.code == 2 and "no default is substituted" in capsys.readouterr().err


def test_strict_loading_refuses_what_the_default_loader_silently_defaults(tmp_path):
    path = tmp_path / "bare.jsonl"
    stamp = "2026-11-17T12:00:00Z"
    path.write_text(json.dumps({"observation_timestamp_utc": stamp, "state": {"timestamp_utc": stamp}}) + "\n", encoding="utf-8")
    assert load_jsonl_frames(path, run_id="r")[0].state.current_config.max_message_count == 45     # the silent default
    with pytest.raises(IntegrationSafetyError, match="no complete current_config"):
        load_jsonl_frames(path, run_id="r", strict_config=True)
    partial = tmp_path / "partial.jsonl"
    partial.write_text(json.dumps({"observation_timestamp_utc": stamp, "state": {
        "timestamp_utc": stamp, "current_config": {"max_message_count": 45}}}) + "\n", encoding="utf-8")
    with pytest.raises(IntegrationSafetyError, match="no complete current_config"):
        load_jsonl_frames(partial, run_id="r", strict_config=True)


# --------------------------------------------- provenance in provider frames (live path)
GOOD = 'consensus_etcdraft_is_leader{channel="ch"} 1\n'


def provider_frame(config_reader):
    adapter = PrometheusTelemetryAdapter({"o": "http://o/metrics"}, fetch=lambda *_: GOOD)
    provider = ProductionTelemetryProvider(adapter, "ch", config_reader=config_reader, required_fields=())
    return provider.collect(run_id="r", decision_index=0, observation_timestamp_utc="2026-11-17T12:00:00.000+00:00")


def test_provider_frames_record_where_the_configuration_came_from(tmp_path):
    shadow = load_module(SHADOW_CLI, "shadow_cli_under_test")
    path = tmp_path / "block.json"
    path.write_text(json.dumps(block_doc(channel="ch", number=0, B=45, timeout="2000ms")), encoding="utf-8")
    verified = provider_frame(shadow.VerifiedConfigReader(path, "ch"))
    assert verified.state.current_config.max_message_count == 45 and verified.state.current_config.batch_timeout_ms == 2000
    source = verified.provenance["configuration_source"]
    assert source["kind"] == "config_block" and source["channel_id"] == "ch" and source["no_default_substituted"] is True
    unverified = provider_frame(lambda: FabricConfig(max_message_count=40))
    assert unverified.provenance["configuration_source"]["status"] == "unverified_reader"
    none = provider_frame(None)
    assert none.provenance["configuration_source"]["status"] == "unavailable" and "current_config" in none.critical_missing
    with pytest.raises(ConfigEvidenceError, match="not 'other'"):
        shadow.VerifiedConfigReader(path, "other")


# ------------------------------------- MaxMessageCount is not silently the HFBTP Block Size
def test_unsupported_block_sizes_fail_safely_and_the_mapping_is_explicit():
    from fabric_perf.controllers.adapters import ApprovedMaxMessageCountSpace
    with pytest.raises(ValueError, match="approved live candidates"):
        ApprovedMaxMessageCountSpace(frozenset({10, 40}), {10: 10, 40: 40}, FabricConfig(max_message_count=45))
    with pytest.raises(ValueError, match="cover exactly"):
        ApprovedMaxMessageCountSpace(frozenset({10, 45}), {10: 10}, FabricConfig(max_message_count=45))
    space = ApprovedMaxMessageCountSpace(frozenset({10, 45}), {10: 10, 45: 50}, FabricConfig(max_message_count=45))
    assert space.hfbtp_block_size(FabricConfig(max_message_count=45)) == 50          # Fabric B=45, HFBTP Block Size 50
    with pytest.raises(ValueError, match="not approved"):
        space.validate(FabricConfig(max_message_count=40))
    with pytest.raises(ValueError, match="live policy permits only MaxMessageCount"):
        space.validate(replace(FabricConfig(max_message_count=45), batch_timeout_ms=1000))


def test_the_shadow_foundation_carries_b45_end_to_end(tmp_path, monkeypatch):
    from fabric_perf.controllers import live_foundation as lf
    monkeypatch.setattr(lf, "HFBTPArtifactPerformancePredictor",
                        lambda artifact_dir, *, candidate_space: _FoundationPredictor(candidate_space))
    stamp0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    source = lf.StreamingWorkload("fixture workload")
    for i in range(600):
        source.add((stamp0 + timedelta(seconds=i)).isoformat(), 12, collected_at_utc=(stamp0 + timedelta(seconds=i + 1)).isoformat())
    config = FabricConfig(max_message_count=45)
    provider, runner = lf.make_shadow_foundation(
        root=REPO, manifest_path=REPO / "Integration/system_integration/offline_replay_inputs.json",
        endpoints={"orderer": "fixture"}, channel_id="channel", current_config=config, config_reader=lambda: config,
        workload=source, candidate_map={10: 10, 45: 45}, latency_high_ms=1500, latency_low_ms=1200,
        congested_max_message_count=10, consecutive_samples=2,
        fetch=lambda *_: 'consensus_etcdraft_is_leader{channel="channel"} 1\n')
    frame = provider.collect(run_id="live-foundation", decision_index=0,
                             observation_timestamp_utc=(stamp0 + timedelta(seconds=600)).isoformat())
    assert frame.state.current_config.max_message_count == 45
    result = runner.run([frame])
    assert result["record_count"] == 3 and result["mode"] == "shadow" and not result["live_submission_enabled"]
    assert runner.controllers["static"].config.max_message_count == 45
    assert all(r["state"]["current_config"]["max_message_count"] == 45 for r in result["records"])


class _FoundationPredictor:
    def __init__(self, candidate_space):
        self.candidate_space = candidate_space

    def validate_config(self, config):
        return self.candidate_space.hfbtp_block_size(config)

    def predict(self, arrival, config):
        return PerformancePrediction(10.0, 100.0)


# ------------------------- the two existing-channel runners that carried protected-channel defaults
RUNNERS = REPO / "scripts" / "e1"


@pytest.mark.parametrize("script, given, missing", [
    ("run_contemporaneous_capture.py", [], "--channel"),
    ("run_contemporaneous_capture.py", ["--channel", "e1-new"], "--max-message-count"),
    ("synchronized_shadow_replay.py", ["--run-id", "t"], "--channel"),
])
def test_the_capture_runners_no_longer_default_to_a_protected_channel_or_block_size(script, given, missing, capsys):
    """They stop at argument parsing, before any file, Docker or Fabric access."""
    sys.path.insert(0, str(RUNNERS))
    module = load_module(RUNNERS / script, f"runner_under_test_{script[:-3]}")
    with pytest.raises(SystemExit) as caught:
        module.main(given)
    assert caught.value.code == 2 and missing in capsys.readouterr().err


def synthetic_run(tmp_path, B):
    run_dir = tmp_path / f"run{B}"
    (run_dir / "analysis").mkdir(parents=True)
    (run_dir / "analysis" / "transaction_measurements.csv").write_text(
        "tx_id,seq_finish,final_ms,status,latency_ms\nt0,0,1000100,success,100\nt1,1,1001100,success,120\n", encoding="utf-8")
    (run_dir / "replay_dispatch.csv").write_text(
        "seq,dispatch_ms,intended_offset_ms,lag_ms\n0,1000000,0,0\n1,1001000,1000,0\n", encoding="utf-8")
    trace = [{"timestamp_utc": f"2026-09-21T00:00:0{i}+00:00", "arrival_count": 1} for i in range(2)]
    summary = {"channel": "e1-x", "trace": {"series_path": "series.csv.gz"},
               "before": {"config": {"active_config": {"max_message_count": B}}}}
    return run_dir, trace, summary


@pytest.mark.parametrize("B", [45, 40])
def test_shadow_replay_snapshots_carry_the_block_size_fetched_from_the_channel(tmp_path, B):
    """Before: every snapshot said B=40 (the constant of e1-cfg001) whatever the channel."""
    sys.path.insert(0, str(RUNNERS))
    module = load_module(RUNNERS / "synchronized_shadow_replay.py", "shadow_replay_under_test")
    run_dir, trace, summary = synthetic_run(tmp_path, B)
    _events, snapshots = module.build_timeline(run_dir, trace, summary)
    rows = [json.loads(line) for line in snapshots.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 2 and {row["current_config"]["max_message_count"] for row in rows} == {B}
    assert 'CHANNELS["e1-cfg001"]' not in (RUNNERS / "synchronized_shadow_replay.py").read_text(encoding="utf-8").split(
        "def build_timeline", 1)[1].split("def ", 1)[0]
