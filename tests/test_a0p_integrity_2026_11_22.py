"""A0p integrity (checklist items 48-50 and the offline-only guarantees): protected channels, the A0p candidate manifest, the nine earlier manifests, frozen and pinned files, the command-line
tools, and the absence of any process, network, container or model API in the new sources."""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from tests.a0p_support import REPO, run_timed_node, spool_recorder, wc

from fabric_perf.controllers import channel_selection as cs         # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi         # noqa: E402

MANIFEST_DIR = REPO / "docs" / "research" / "e1"
A0P = MANIFEST_DIR / "prelive_candidate_manifest_2026-11-22_a0p.json"
A0O = MANIFEST_DIR / "prelive_candidate_manifest_2026-11-21_a0o.json"
# SHA-256 of the nine earlier candidate manifests, recorded at the start of A0p (before any A0p file existed)
HISTORICAL = {
    "prelive_candidate_manifest_2026-11-21.json": "13640a7f683adcfec96f26f9060b05f29306bb2ebcfeb4c4b90c5fc43fa2b984",
    "prelive_candidate_manifest_2026-11-21_a0g.json": "491c7d0c296515bf24cd2a8d4462be3f34a81926f933e5d7542a45105eb5a5e7",
    "prelive_candidate_manifest_2026-11-21_a0h.json": "d6c451cfae089642d8c5c6be2c80d1fbdef7f718d1d2888c95d86275c1fda689",
    "prelive_candidate_manifest_2026-11-21_a0i.json": "7bc73bc7f648c92e3d515e4e9e98f41df1a552d2c9bd8bad204f78761d2a7c66",
    "prelive_candidate_manifest_2026-11-21_a0j.json": "831cb76ecdbd75318ebc7d4fe9be0d0c3eedfe7873a9a2190fa3446dd736c594",
    "prelive_candidate_manifest_2026-11-21_a0k.json": "d648a7151bf7d2688d2f658594a4d5adae4d08201b4428de5924372688ff756c",
    "prelive_candidate_manifest_2026-11-21_a0l.json": "b256ba9e04e699be412d362c3d167a7c5b58204c442cb3bb17afac5c2b0efcb4",
    "prelive_candidate_manifest_2026-11-21_a0m.json": "64687487752fc097f4d18e5e86593936421b0738dc967d0c23f06b3b3ccdd0fd",
    "prelive_candidate_manifest_2026-11-21_a0o.json": "e152ebae4d5f35cbea36acac3ca8d1854cf1899f1aa3ccac616c2a44e7d8ecf7",
}
NEW_PY = ["src/fabric_perf/controllers/issue_log_timing.py", "src/fabric_perf/controllers/recorder_health_incremental.py", "src/fabric_perf/controllers/timing_durable.py",
          "src/fabric_perf/controllers/timed_variant.py", "scripts/e1/e1_a0p_launch_variant.py", "scripts/e1/e1_a0p_timing_tools.py"]
NEW_JS = ["fabric-testbed/caliper/workloads/e1_issue_log_timed.js", "fabric-testbed/caliper/workloads/e1_create_asset_issue_logged_timed.js"]
NEW_RUNTIME = sorted(NEW_PY + NEW_JS)
FORBIDDEN_IMPORTS = {"subprocess", "socket", "requests", "http", "urllib", "docker", "asyncio", "multiprocessing", "ssl", "ftplib", "telnetlib", "paramiko", "pickle", "joblib",
                     "sklearn", "torch", "tensorflow", "xgboost", "lightgbm"}
FORBIDDEN_CALLS = {"system", "popen", "Popen", "run", "check_output", "check_call", "startfile", "spawn", "execv", "execve", "fork", "urlopen", "connect"}
FORBIDDEN_JS_REQUIRES = {"child_process", "net", "http", "https", "http2", "dgram", "dns", "tls", "worker_threads", "cluster", "vm"}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ----------------------------------------------------------------------------------------------------------------------------- 48 protected channels
def test_48_protected_and_earlier_channels_are_rejected_everywhere_the_new_tools_take_a_channel(tmp_path):
    from tests.a0p_support import bridge_config
    rec = spool_recorder(tmp_path)
    for channel in sorted(cs.PROTECTED_REFERENCE_CHANNELS):
        with pytest.raises(cs.ChannelSelectionError):
            bridge_config(rec, tmp_path, channel_id=channel)
    import e1_a0p_launch_variant as launcher
    with pytest.raises(SystemExit) as refused:
        launcher.main(["render", "--variant", "a0p_recorder_v3_timed_issue_log", "--params", "p", "--a0m-params", "q", "--channel", "e1-r002", "--out-root", str(tmp_path / "x"),
                       "--render-id", "r"])
    assert refused.value.code == 2 and not (tmp_path / "x").exists()
    assert not (tmp_path / "b.journal.jsonl").exists()


# ----------------------------------------------------------------------------------------------------------------------------- 49 manifest verification
def test_49_the_a0p_candidate_manifest_verifies_and_stays_a_candidate():
    manifest = json.loads(A0P.read_text(encoding="utf-8"))
    assert pi.verify_candidate_manifest(manifest, REPO) == []
    assert manifest["status"] == "CANDIDATE_NOT_APPROVED" and manifest["approved"] is False and manifest["approval_record"] is None
    assert len(manifest["entries"]) == 93 and manifest["changed_since_a0o"] == [] and manifest["removed_since_a0o"] == [] and manifest["superseded_entries"] == []
    assert manifest["added_since_a0o"] == NEW_RUNTIME
    by_path = {e["path"]: e for e in manifest["entries"]}
    for path in NEW_RUNTIME:
        assert by_path[path]["sha256"] == sha(REPO / path) and by_path[path]["approval_status"] == pi.PENDING_OWNER_APPROVAL and by_path[path]["pinned_sha256"] is None
    assert manifest["supersedes"]["sha256"] == sha(A0O) and manifest["launch_configuration"]["approved"] is False and manifest["launch_configuration"]["live_run"] is False
    assert {e["approval_status"] for e in manifest["entries"]} <= {pi.PENDING_OWNER_APPROVAL, pi.PINNED_ELSEWHERE_UNCHANGED, pi.EXCLUDED_NOT_APPROVED_ENTRY_POINT}
    assert any("eight A0p additions has not been given" in item for item in manifest["open_items"])
    assert manifest["schema_versions"]["e1_issue_log"] == "e1_issue_log/1 (unchanged)" and "proposed" in manifest["schema_versions"]["ingest_ledger"]
    assert any("fsync" in item and "not a guarantee" in item for item in manifest["unresolved_dependencies"])


def test_49b_the_a0o_entries_are_repeated_with_unchanged_hashes():
    a0p = {e["path"]: e["sha256"] for e in json.loads(A0P.read_text(encoding="utf-8"))["entries"]}
    a0o = {e["path"]: e["sha256"] for e in json.loads(A0O.read_text(encoding="utf-8"))["entries"]}
    assert len(a0o) == 85 and all(a0p[p] == h for p, h in a0o.items())
    assert pi.verify_candidate_manifest(json.loads(A0O.read_text(encoding="utf-8")), REPO) == []


def test_49c_a_tampered_manifest_or_candidate_file_is_detected(tmp_path):
    manifest = json.loads(A0P.read_text(encoding="utf-8"))
    manifest["entries"][-1]["sha256"] = "0" * 64
    assert [f.code for f in pi.verify_candidate_manifest(manifest, REPO)] == ["modified"]
    approved = json.loads(A0P.read_text(encoding="utf-8"))
    approved["approved"] = True
    assert "status" in {f.code for f in pi.verify_candidate_manifest(approved, REPO)}
    sandbox = tmp_path / "repo"
    copy = json.loads(A0P.read_text(encoding="utf-8"))
    copy["entries"] = [e for e in copy["entries"] if e["path"] in NEW_RUNTIME]
    for e in copy["entries"]:
        (sandbox / e["path"]).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO / e["path"], sandbox / e["path"])
    assert pi.verify_candidate_manifest(copy, sandbox) == []
    victim = sandbox / "src/fabric_perf/controllers/recorder_health_incremental.py"
    victim.write_text(victim.read_text(encoding="utf-8") + "\n# edited\n", encoding="utf-8")
    assert [f.path for f in pi.verify_candidate_manifest(copy, sandbox)] == ["src/fabric_perf/controllers/recorder_health_incremental.py"]


# ----------------------------------------------------------------------------------------------------------------------------- 50 earlier manifests and frozen files
def test_50_the_nine_earlier_candidate_manifests_are_byte_identical_to_their_starting_hashes():
    for name, digest in HISTORICAL.items():
        assert sha(MANIFEST_DIR / name) == digest, name


def test_50b_pinned_frozen_and_earlier_candidate_sources_are_unchanged():
    manifest = json.loads(A0P.read_text(encoding="utf-8"))
    a0o = json.loads(A0O.read_text(encoding="utf-8"))
    pinned = {e["path"]: e["pinned_sha256"] for e in manifest["entries"] if e["pinned_sha256"] is not None}
    assert pinned == {e["path"]: e["pinned_sha256"] for e in a0o["entries"] if e["pinned_sha256"] is not None}
    assert {"scripts/e1/e1_la2_render.py", "scripts/e1/caliper/e1_per_tx_recorder.js"} <= set(pinned) and all(sha(REPO / p) == h for p, h in pinned.items())
    assert sha(REPO / "scripts/e1/e1_la2_render.py").startswith("f77a83e7") and sha(REPO / "scripts/e1/caliper/e1_per_tx_recorder.js").startswith("b5496af3")
    expected = {e["path"]: e["sha256"] for e in a0o["entries"]}
    for path in ("fabric-testbed/caliper/workloads/e1_issue_log.js", "fabric-testbed/caliper/workloads/e1_create_asset_issue_logged.js", "src/fabric_perf/controllers/issue_log_reader.py",
                 "src/fabric_perf/controllers/issue_log_coverage.py", "src/fabric_perf/controllers/recorder_health_bridge.py", "src/fabric_perf/controllers/collector_health.py",
                 "src/fabric_perf/controllers/sourcecount_reader.py", "src/fabric_perf/controllers/timing_sampler.py", "src/fabric_perf/controllers/launch_variant.py",
                 "scripts/e1/e1_a0o_launch_variant.py", "scripts/e1/e1_a0m_dry_render.py", "scripts/e1/e1_prelive_validation.py", "scripts/e1/caliper/e1_per_tx_recorder_v3.js"):
        assert sha(REPO / path) == expected[path], path
    stored = REPO / "docs/research/e1/a0m_dry_render_evidence_2026-11-21/render/a0m-evidence-001/a0m_config_manifest.json"
    recorded = json.loads(stored.read_text(encoding="utf-8"))["files"]
    assert all(sha(stored.parent / name) == digest for name, digest in recorded.items())          # the stored historical evidence is untouched


# ----------------------------------------------------------------------------------------------------------------------------- offline only
def imports_and_calls(rel: str):
    tree = ast.parse((REPO / rel).read_text(encoding="utf-8"))
    names, calls = set(), []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
        elif isinstance(node, ast.Call):
            func = node.func
            calls.append(func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", ""))
    return names, calls


@pytest.mark.parametrize("rel", NEW_PY)
def test_51_no_new_python_file_imports_or_calls_a_process_network_container_or_model_api(rel):
    names, calls = imports_and_calls(rel)
    assert not names & FORBIDDEN_IMPORTS, (rel, names & FORBIDDEN_IMPORTS)
    assert not set(calls) & FORBIDDEN_CALLS, (rel, set(calls) & FORBIDDEN_CALLS)
    text = (REPO / rel).read_text(encoding="utf-8")
    for token in ("os.environ", "getenv", "shutil.which", "wsl.exe", "docker run", "docker exec", "npx ", "caliper launch", "caliper-cli", "joblib", ".joblib"):
        assert token not in text, (rel, token)


@pytest.mark.parametrize("rel", NEW_JS)
def test_52_no_new_javascript_file_requires_a_process_network_or_vm_module(rel):
    text = (REPO / rel).read_text(encoding="utf-8")
    required = {target for _quote, target in re.findall(r"""require\(\s*(['"])([^'"]+)\1\s*\)""", text)}
    assert required and not {r.replace("node:", "") for r in required} & FORBIDDEN_JS_REQUIRES
    assert required <= {"fs", "path", "./e1_issue_log.js", "./e1_create_asset.js", "./e1_workload_core.js", "./e1_issue_log_timed.js", "@hyperledger/caliper-core"}
    for token in ("process.env", "child_process", "fetch(", "XMLHttpRequest", "WebSocket", "eval(", "new Function"):
        assert token not in text, (rel, token)


def test_53_every_execute_path_is_refused_and_no_new_source_enables_real_inference_or_a_live_run():
    for rel, argv in {"scripts/e1/e1_a0p_launch_variant.py": ["execute"]}.items():
        spec = importlib.util.spec_from_file_location(Path(rel).stem, REPO / rel)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        assert module.main(argv) == 5
    for rel in NEW_RUNTIME:
        text = (REPO / rel).read_text(encoding="utf-8")
        assert "REAL_INFERENCE_AUTHORIZED = True" not in text and "live_run = True" not in text and "LIVE_FOLLOW_ENABLED = True" not in text
    assert '"caliper_invoked": False' in (REPO / "scripts/e1/e1_a0p_launch_variant.py").read_text(encoding="utf-8")


# ----------------------------------------------------------------------------------------------------------------------------- command line
def tools():
    spec = importlib.util.spec_from_file_location("e1_a0p_timing_tools", REPO / "scripts/e1/e1_a0p_timing_tools.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_54_the_command_line_verifies_bridges_and_samples_and_never_overwrites(tmp_path, capsys):
    t = tools()
    node = run_timed_node(tmp_path / "n", stepUs=200)
    out = tmp_path / "verify.json"
    assert t.main(["verify-write-timing", "--log", node["log"], "--sidecar", node["timing"], "--out", str(out)]) == 0
    assert json.loads(out.read_text())["status"] == "VERIFIED"
    assert t.main(["verify-write-timing", "--log", node["log"], "--sidecar", node["timing"], "--out", str(out)]) == 2            # an existing output is refused
    rec = spool_recorder(tmp_path / "s")
    spool = rec.spool_path
    base = ["bridge", "--spool", str(spool), "--journal", str(tmp_path / "j.jsonl"), "--ledger", str(tmp_path / "l.jsonl"), "--checkpoint", str(tmp_path / "c.json"),
            "--source-id", rec.source_id, "--run-id", "run-a0j", "--spool-clock-domain", "recorder-host", "--bridge-clock-domain", "bridge-host", "--heartbeat-interval-s", "1",
            "--max-heartbeat-gap-s", "5", "--freshness-limit-s", "10", "--clock-tolerance-ms", "250", "--max-rows-per-poll", "4"]
    assert t.main(base + ["--source-ended", "--out", str(tmp_path / "report.json")]) == 0
    report = json.loads((tmp_path / "report.json").read_text())
    assert report["complete"] is True and report["journal_records"] == 22
    assert t.main(base + ["--out", str(tmp_path / "again.json")]) == 2                                                          # the journal exists: not overwritten
    sampled = tmp_path / "sample.json"
    assert t.main(["sample", "--origin", "synthetic_fixture", "--issue-log", node["log"], "--issue-log-timing", node["timing"], "--spool", str(spool), "--ingest-ledger",
                   str(tmp_path / "l.jsonl"), "--out", str(sampled)]) == 0
    doc = json.loads(sampled.read_text())
    assert doc["schema"] == "a0p_timing_results/1" and doc["measurements"]["T01"]["status"] == "MEASURED_FROM_OFFLINE_EVIDENCE"
    assert doc["measurements"]["T06"]["status"] == "NOT_MEASURABLE_FROM_EXISTING_EVIDENCE" and doc["measurements"]["T06"]["bridge_internal"]["eligible_samples"] == 22
    assert t.main(["sample", "--origin", "live_recorder", "--out", str(tmp_path / "live.json")]) == 2


# ----------------------------------------------------------------------------------------------------------------------------- git
def test_55_nothing_is_staged_and_head_is_the_starting_commit():
    git = shutil.which("git")
    if git is None:
        pytest.skip("git is not available")
    staged = subprocess.run([git, "-C", str(REPO), "diff", "--cached", "--name-only"], capture_output=True, text=True, check=False, timeout=60)
    head = subprocess.run([git, "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True, check=False, timeout=60)
    assert staged.returncode == 0 and staged.stdout.strip() == ""
    assert head.stdout.strip() == "8c67a4678b3901c8d601fb1a33210d7d8c84cf78"
