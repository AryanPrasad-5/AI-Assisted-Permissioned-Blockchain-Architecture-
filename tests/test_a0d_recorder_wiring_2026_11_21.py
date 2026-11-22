"""Task A0d (2026-11-21), blocker B-wire: selecting recorder v2 for a run, offline.

The run directories used here are produced by the REAL pinned renderer function (``e1_la2_render.caliper_files``), so the
wrapper is tested on the structure the pinned tools actually write. Nothing runs Caliper, Docker, WSL or a network; the only
subprocess is local Node.js resolving a ``require`` (one test), with the process APIs patched out everywhere else.
"""

import ast
import hashlib
import json
import posixpath
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from fabric_perf.controllers import channel_selection as cs

REPO = Path(__file__).resolve().parents[1]
E1 = REPO / "scripts" / "e1"
sys.path.insert(0, str(E1))
sys.path.insert(0, str(REPO))

import e1_la2_render as lr  # noqa: E402  (the pinned renderer, imported not edited)
import e1_recorder_v2_wire as wire  # noqa: E402

NODE = shutil.which("node")
CHANNEL = "e1-offline.a0d"
RUN_ID = "a0d-wire-001"
ENV = {"chaincode": {"name": "e1asset", "version": "1.0"}, "protocol": {"binding": "gateway", "event_strategy": "peer", "invoke_timeout_s": 30}}
V1, V2 = wire.V1_NAME, wire.V2_NAME


def sha(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def render_source_run(root: Path, *, channel: str = CHANNEL, run_id: str = RUN_ID) -> Path:
    """What the pinned renderer writes (benchmark, network configuration, connection profile, modules)."""
    run = root / "runs" / run_id
    run.mkdir(parents=True)
    files = lr.caliper_files(ENV, run_id, channel, None, 5.0, 0, "create", duration_s=10)
    for name, doc in files.items():
        (run / name).write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    (run / "modules").mkdir()
    for name, rel in lr.MODULES.items():
        shutil.copyfile(REPO / rel, run / "modules" / name)
    return run


def tree_hashes(directory: Path) -> dict:
    return {p.relative_to(directory).as_posix(): sha(p) for p in sorted(directory.rglob("*")) if p.is_file()}


# ============================================================ 18. explicit v2 selection, proven in the generated file
def test_the_wrapper_writes_a_run_whose_monitor_loads_v2_and_the_source_run_is_untouched(tmp_path):
    source = render_source_run(tmp_path / "src")
    before = tree_hashes(source)
    record = wire.wire_run(source, tmp_path / "out" / "runs", channel=CHANNEL)
    target = Path(record["target_run_dir"])
    assert target == tmp_path / "out" / "runs" / RUN_ID and tree_hashes(source) == before                  # the pinned renderer's output is not modified
    benchmark = json.loads((target / "benchmark.yaml").read_text(encoding="utf-8"))
    monitor = benchmark["monitors"]["transaction"][0]
    assert posixpath.basename(monitor["module"]) == V2 and monitor["module"].endswith(f"/runs/{RUN_ID}/modules/{V2}")
    old = json.loads((source / "benchmark.yaml").read_text(encoding="utf-8"))["monitors"]["transaction"][0]
    assert posixpath.basename(old["module"]) == V1                                                           # the source still says v1
    assert {k: v for k, v in monitor["options"].items()} == old["options"]                                   # options untouched (no live options given)
    assert benchmark["test"] == json.loads((source / "benchmark.yaml").read_text(encoding="utf-8"))["test"]  # rounds and workload unchanged
    assert record["status"] == "RENDERED_OFFLINE_NOT_EXECUTED" and record["caliper_invoked"] is False
    assert record["monitor"]["before"]["module"] != record["monitor"]["after"]["module"]
    assert record["files"]["benchmark.yaml"]["target_sha256"] == sha(target / "benchmark.yaml")
    assert json.loads((target / "recorder_v2_wire_record.json").read_text(encoding="utf-8"))["run_id"] == RUN_ID


def test_live_stream_options_are_passed_through_only_when_given_and_only_the_two_v2_knows(tmp_path):
    source = render_source_run(tmp_path / "src")
    record = wire.wire_run(source, tmp_path / "out", channel=CHANNEL, live_options={"liveFlushRows": 20, "liveFlushIntervalMs": 250})
    options = json.loads((Path(record["target_run_dir"]) / "benchmark.yaml").read_text(encoding="utf-8"))["monitors"]["transaction"][0]["options"]
    assert options["liveFlushRows"] == 20 and options["liveFlushIntervalMs"] == 250 and options["runId"] == RUN_ID


@pytest.mark.parametrize("bad", [{"liveFlushRows": 0}, {"liveFlushRows": True}, {"liveFlushIntervalMs": 9}, {"liveFlushRows": 1.5},
                                 {"unknownOption": 1}, {"liveFlushRows": "5"}])
def test_invalid_live_options_are_refused_before_anything_is_written(tmp_path, bad):
    source = render_source_run(tmp_path / "src")
    with pytest.raises(wire.WireError):
        wire.wire_run(source, tmp_path / "out", channel=CHANNEL, live_options=bad)
    assert not (tmp_path / "out").exists()


# ============================================================ 19. the v1 dependency resolves
def test_v1_is_present_in_the_wired_run_and_is_the_pinned_file(tmp_path):
    record = wire.wire_run(render_source_run(tmp_path / "src"), tmp_path / "out", channel=CHANNEL)
    modules = Path(record["target_run_dir"]) / "modules"
    assert sha(modules / V1) == wire.PINNED_V1_SHA256 == sha(wire.V1_SOURCE) and sha(modules / V2) == sha(wire.V2_SOURCE)
    assert {p.name for p in modules.iterdir()} >= {V1, V2, "e1_create_asset.js", "e1_workload_core.js"}


def test_v1_is_supplied_by_the_wrapper_when_the_source_run_does_not_carry_it(tmp_path):
    source = render_source_run(tmp_path / "src")
    (source / "modules" / V1).unlink()                                                                       # e.g. a template run that lacks it
    record = wire.wire_run(source, tmp_path / "out", channel=CHANNEL)
    assert sha(Path(record["target_run_dir"]) / "modules" / V1) == wire.PINNED_V1_SHA256
    assert not (source / "modules" / V1).exists()                                                            # still not added to the source


@pytest.mark.skipif(NODE is None, reason="Node.js not on PATH: module resolution NOT VERIFIED")
def test_node_resolves_v2_and_its_v1_dependency_from_the_wired_modules_directory_and_not_without_v1(tmp_path):
    record = wire.wire_run(render_source_run(tmp_path / "src"), tmp_path / "out", channel=CHANNEL)
    modules = Path(record["target_run_dir"]) / "modules"
    probe = "const m=require(process.argv[1]); console.log(typeof m.createTxObserver, typeof m.LiveStreamRecorder)"
    ok = subprocess.run([NODE, "-e", probe, str(modules / V2)], capture_output=True, text=True, timeout=30)
    assert ok.returncode == 0 and ok.stdout.strip() == "function function", ok.stderr
    broken = tmp_path / "no-v1"
    broken.mkdir()
    shutil.copyfile(modules / V2, broken / V2)                                                               # v2 alone: its dependency is real
    fail = subprocess.run([NODE, "-e", probe, str(broken / V2)], capture_output=True, text=True, timeout=30)
    assert fail.returncode != 0 and "e1_per_tx_recorder.js" in fail.stderr


# ============================================================ 20/21/22. the channel is explicit, validated, and never implicit
def test_a_missing_channel_is_refused_by_the_library_and_the_command_line(tmp_path):
    source = render_source_run(tmp_path / "src")
    for missing in (None, "", "   "):
        with pytest.raises(cs.ChannelSelectionError) as err:
            wire.wire_run(source, tmp_path / "out", channel=missing)
        assert err.value.reason in {cs.MISSING, cs.WRONG_TYPE, cs.EMPTY}
    with pytest.raises(SystemExit) as exit_info:
        wire.main(["--source-run-dir", str(source), "--target-runs-root", str(tmp_path / "out")])
    assert exit_info.value.code == 2 and not (tmp_path / "out").exists()


@pytest.mark.parametrize("channel", sorted(cs.PROTECTED_REFERENCE_CHANNELS))
def test_a_protected_reference_channel_cannot_be_selected_even_when_the_run_names_it(tmp_path, channel):
    source = render_source_run(tmp_path / "src", channel=channel)                                            # a run that really targets it
    with pytest.raises(cs.ChannelSelectionError) as err:
        wire.wire_run(source, tmp_path / "out", channel=channel)
    assert err.value.reason == cs.PROTECTED and not (tmp_path / "out").exists()
    with pytest.raises(SystemExit) as exit_info:
        wire.main(["--source-run-dir", str(source), "--target-runs-root", str(tmp_path / "out"), "--channel", channel])
    assert exit_info.value.code == 2


@pytest.mark.parametrize("channel", ["E1-Upper", "e1-", "e1 space", "e1-" + "x" * 41, "other-channel", "e1-bad_underscore", "e1-é"])
def test_a_malformed_channel_is_refused(tmp_path, channel):
    source = render_source_run(tmp_path / "src")
    with pytest.raises(cs.ChannelSelectionError) as err:
        wire.wire_run(source, tmp_path / "out", channel=channel)
    assert err.value.reason in {cs.MALFORMED, cs.HISTORICAL}


def test_the_command_line_has_no_default_channel_and_uses_the_shared_validator():
    tree = ast.parse((E1 / "e1_recorder_v2_wire.py").read_text(encoding="utf-8"))
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and getattr(n.func, "attr", "") == "add_argument"
             and any(isinstance(a, ast.Constant) and a.value == "--channel" for a in n.args)]
    assert len(calls) == 1
    keywords = {k.arg: k.value for k in calls[0].keywords}
    assert "default" not in keywords and "choices" not in keywords and keywords["required"].value is True
    assert keywords["type"].attr == "experiment_channel_arg" and keywords["type"].value.id == "cs"


def test_a_run_whose_network_names_another_channel_is_refused(tmp_path):
    source = render_source_run(tmp_path / "src", channel="e1-someone-else")
    with pytest.raises(cs.ChannelSelectionError) as err:
        wire.wire_run(source, tmp_path / "out", channel=CHANNEL)
    assert err.value.reason == cs.MISMATCH and not (tmp_path / "out").exists()


def test_a_protected_name_left_anywhere_in_the_run_is_refused(tmp_path):
    source = render_source_run(tmp_path / "src")
    leftover = sorted(cs.PROTECTED_REFERENCE_CHANNELS)[0]
    (source / "ccp-org1.json").write_text(json.dumps({"note": f"copied from {leftover}"}), encoding="utf-8")
    with pytest.raises(cs.ChannelSelectionError) as err:
        wire.wire_run(source, tmp_path / "out", channel=CHANNEL)
    assert err.value.reason == cs.PROTECTED_REMAINS and not (tmp_path / "out").exists()


# ============================================================ 23. invalid or unapproved configurations fail closed
def edit_benchmark(source: Path, mutate) -> None:
    path = source / "benchmark.yaml"
    doc = json.loads(path.read_text(encoding="utf-8"))
    mutate(doc)
    path.write_text(json.dumps(doc), encoding="utf-8")


@pytest.mark.parametrize("name,mutate,message", [
    ("no_monitor", lambda d: d["monitors"]["transaction"].clear(), "exactly one transaction monitor"),
    ("two_monitors", lambda d: d["monitors"]["transaction"].append(dict(d["monitors"]["transaction"][0])), "exactly one"),
    ("other_module", lambda d: d["monitors"]["transaction"][0].update(module="/x/modules/some_other.js"), "not the v1 recorder"),
    ("already_v2", lambda d: d["monitors"]["transaction"][0].update(module="/x/modules/" + V2), "already names recorder v2"),
    ("no_module", lambda d: d["monitors"]["transaction"][0].pop("module"), "names no module"),
    ("no_run_id", lambda d: d["monitors"]["transaction"][0]["options"].pop("runId"), "no runId"),
    ("no_monitors_key", lambda d: d.pop("monitors"), "exactly one transaction monitor"),
])
def test_a_benchmark_that_is_not_exactly_one_v1_monitor_is_refused(tmp_path, name, mutate, message):
    source = render_source_run(tmp_path / "src")
    edit_benchmark(source, mutate)
    with pytest.raises(wire.WireError, match=message):
        wire.wire_run(source, tmp_path / "out", channel=CHANNEL)
    assert not (tmp_path / "out").exists()


def test_a_directory_that_is_not_a_rendered_run_is_refused(tmp_path):
    (tmp_path / "empty").mkdir()
    with pytest.raises(wire.WireError, match="not a rendered run directory"):
        wire.wire_run(tmp_path / "empty", tmp_path / "out", channel=CHANNEL)


def test_unreadable_json_is_refused(tmp_path):
    source = render_source_run(tmp_path / "src")
    (source / "benchmark.yaml").write_text("{ not json", encoding="utf-8")
    with pytest.raises(wire.WireError, match="cannot read"):
        wire.wire_run(source, tmp_path / "out", channel=CHANNEL)


def test_an_existing_target_is_never_overwritten(tmp_path):
    source = render_source_run(tmp_path / "src")
    wire.wire_run(source, tmp_path / "out", channel=CHANNEL)
    with pytest.raises(wire.WireError, match="refusing to overwrite"):
        wire.wire_run(source, tmp_path / "out", channel=CHANNEL)


def test_a_v1_that_differs_from_the_pinned_hash_is_refused_in_the_repository_and_in_the_run(tmp_path, monkeypatch):
    source = render_source_run(tmp_path / "src")
    (source / "modules" / V1).write_text("// tampered\n", encoding="utf-8")
    with pytest.raises(wire.WireError, match="differs from the pinned v1"):
        wire.wire_run(source, tmp_path / "out", channel=CHANNEL)
    clean = render_source_run(tmp_path / "src2", run_id="a0d-wire-002")
    fake = tmp_path / "fake_v1.js"
    fake.write_text("// not the pinned file\n", encoding="utf-8")
    monkeypatch.setattr(wire, "V1_SOURCE", fake)
    with pytest.raises(wire.WireError, match="no longer matches the pinned v1 hash"):
        wire.wire_run(clean, tmp_path / "out2", channel=CHANNEL)


def test_verify_wired_rejects_a_run_that_still_selects_v1(tmp_path):
    source = render_source_run(tmp_path / "src")
    with pytest.raises(wire.WireError, match="does not select recorder v2"):
        wire.verify_wired(source, channel=CHANNEL)


# ============================================================ 24. nothing selects v2 implicitly
def test_the_pinned_renderer_still_writes_v1_by_default():
    benchmark = lr.caliper_files(ENV, RUN_ID, CHANNEL, None, 5.0, 0, "create", duration_s=10)["benchmark.yaml"]
    module = benchmark["monitors"]["transaction"][0]["module"]
    assert posixpath.basename(module) == V1 and V2 not in json.dumps(benchmark)
    assert set(lr.MODULES) == {"e1_per_tx_recorder.js", "e1_create_asset.js", "e1_workload_core.js"}


def test_no_script_or_module_other_than_the_wrapper_and_v2_itself_names_v2():
    """Search sources the way a launcher would pull them in. Tests, reports and verification harnesses are excluded."""
    # A0j (2026-11-21): recorder v3 EXTENDS v2 (it must require it as a sibling) and is itself selected by nothing; the matching guard for v3 is
    # tests/test_a0j_integrity_2026_11_21.py::test_42_nothing_selects_recorder_v3_implicitly. No other file may name v2.
    allowed = {"scripts/e1/e1_recorder_v2_wire.py", "scripts/e1/caliper/e1_per_tx_recorder_v2.js", "scripts/e1/caliper/e1_per_tx_recorder_v3.js"}
    # A0r (2026-11-22): two files added by A0o name v2 WITHOUT selecting it and made this guard fail from then on. Exactly these two paths are registered, each with its role;
    # any other file naming v2 is still a failure, and a registered path that stops naming v2 also fails (equality below).
    allowed |= {"src/fabric_perf/controllers/launch_variant.py",                                            # VARIANT CLASSIFIER (A0o): derives the variant of a generated benchmark; selects nothing
                "Integration/system_integration/a0o_2026-11-21/collision_reproduction_before_change.json"}  # RECORDED EVIDENCE (A0o): a pre-change reproduction record, not a selector
    found = set()
    for root in ("src", "scripts", "Fabric_Live_Testbed", "Integration", "p4_replay", "fabric-testbed"):
        for path in (REPO / root).rglob("*"):
            if not path.is_file() or path.suffix not in {".py", ".js", ".yaml", ".yml", ".json", ".sh"}:
                continue
            rel = path.relative_to(REPO).as_posix()
            if "/tests/" in "/" + rel or "a0c_2026-11-21/" in rel or "__pycache__" in rel or "node_modules" in rel:
                continue
            if "e1_per_tx_recorder_v2" in path.read_text(encoding="utf-8", errors="replace"):
                found.add(rel)
    assert found == allowed, sorted(found ^ allowed)


def test_the_default_capture_runner_still_selects_v1_in_its_template_rewrite():
    text = (E1 / "run_contemporaneous_capture.py").read_text(encoding="utf-8")
    assert "modules/e1_per_tx_recorder.js" in text and "e1_per_tx_recorder_v2" not in text


# ============================================================ 25. preparing a run never starts anything
def test_the_wrapper_imports_no_process_network_or_container_api():
    tree = ast.parse((E1 / "e1_recorder_v2_wire.py").read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert not imported & {"subprocess", "socket", "urllib", "http", "requests", "docker", "multiprocessing", "asyncio", "ssl"}
    called = {getattr(n.func, "attr", getattr(n.func, "id", "")) for n in ast.walk(tree) if isinstance(n, ast.Call)}
    assert not called & {"run", "Popen", "system", "check_output", "call", "execv", "spawn"}


def test_preparing_a_run_does_not_touch_any_process_api_even_when_they_are_armed(tmp_path, monkeypatch):
    def boom(*args, **kwargs):
        raise AssertionError("an external process was started while preparing a run")
    for name in ("run", "Popen", "call", "check_call", "check_output"):
        monkeypatch.setattr(subprocess, name, boom)
    import os
    monkeypatch.setattr(os, "system", boom)
    record = wire.wire_run(render_source_run(tmp_path / "src"), tmp_path / "out", channel=CHANNEL)
    assert record["caliper_invoked"] is False
    assert wire.main(["--source-run-dir", str(render_source_run(tmp_path / "src2", run_id="a0d-wire-003")),
                      "--target-runs-root", str(tmp_path / "out2"), "--channel", CHANNEL]) == 0


def test_the_command_line_reports_and_exits_nonzero_on_a_refusal(tmp_path, capsys):
    source = render_source_run(tmp_path / "src")
    edit_benchmark(source, lambda d: d["monitors"]["transaction"][0].update(module="/x/modules/" + V2))
    code = wire.main(["--source-run-dir", str(source), "--target-runs-root", str(tmp_path / "out"), "--channel", CHANNEL])
    assert code == 2 and "REFUSED" in capsys.readouterr().out and not (tmp_path / "out").exists()


# ============================================================ 26. the historical tools are byte-identical to the start of A0d
# SHA-256 taken at the start of task A0d, before any A0d edit (the same files are untouched at the end of the task).
PINNED_AT_A0D_START = {
    "scripts/e1/e1_la2_render.py": "f77a83e7de106b3c1dac28f93333e8dbaf67ea869836944b906f9bc1aabe3d83",
    "p4_replay/render.py": "f8683663675ae6cee5d857d9f06c31a64ea51f206036c82c308acdff733e1e52",
    "scripts/e1/caliper/e1_per_tx_recorder.js": "b5496af3ca038728ff7490b45db726f27ebf0f7efe09b36f6d9330a8cbd1b0a8",
    "scripts/e1/caliper/e1_per_tx_recorder_v2.js": "7ed795b314931c63db1e1ed90e48569901173729ab174097f1a8b370e7ede8f0",
    "scripts/e1/e1_block_fetch.py": "08eedd2ea373dfa1aa9ce1567eca2de78fb3ab77462ac333208c02b6008670bc",
    "scripts/e1/e1_ledger_blocks.py": "d9889fd8421d6f5085b3325e56d1877e925beb7e03ac3611bdcce4762a2db320",
    "src/fabric_perf/controllers/channel_selection.py": "7c59a9bd7ddcc2b536461c307ec1936ee78ad37444a0ad09fdba94417e2df6bc",
    "scripts/e1/run_contemporaneous_capture.py": "81dc1f4f77f85387bf9578e4293f9793051b93d5724496586878797ef904bd54",
    "scripts/e1/synchronized_shadow_replay.py": "35b6e15c22c18b80903a61ceaf7f3eb6ccb9a89803cce1e0b27df4632376acf3",
}


@pytest.mark.parametrize("rel", sorted(PINNED_AT_A0D_START))
def test_historical_and_pinned_files_are_byte_identical_to_the_start_of_a0d(rel):
    assert sha(REPO / rel) == PINNED_AT_A0D_START[rel], rel
