"""A0o workstream D (checklist items 29-37): launch-variant isolation between the A0e (recorder v2 + legacy workload) and A0m (recorder v3 + issue-logged workload) runs.

Real renders into temporary directories (the pinned renderer, the A0e prepare and the A0m dry render all run); nothing starts Caliper, Fabric, Docker, WSL or a network."""

from __future__ import annotations

import ast
import hashlib
import json
import os
import posixpath
import shutil
import socket
import subprocess
from pathlib import Path

import pytest

from tests import a0m_support as m
from tests.a0o_support import REPO

import e1_a0o_launch_variant as v    # noqa: E402  (scripts/e1 is on sys.path through a0m_support)
import e1_prelive_validation as pv   # noqa: E402

from fabric_perf.controllers import channel_selection as cs      # noqa: E402
from fabric_perf.controllers import launch_variant as lv         # noqa: E402

A0E, A0M = lv.VARIANT_A0E, lv.VARIANT_A0M


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a process, container or network use is forbidden")
    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(os, "system", refuse)


def inputs(tmp: Path, **a0e):
    return m.write(tmp / "a0e_params.json", m.a0e_params(**a0e)), m.write(tmp / "a0m_params.json", m.a0m_params())


def historical(tmp: Path):
    """The pre-A0o situation: the A0e prepare and the unmodified A0m dry render for the SAME parameter file."""
    p1, p2 = inputs(tmp)
    a0e = pv.prepare(p1, tmp / "a0e_out", "p1")
    legacy_root, legacy = m.render(tmp, render_id="legacy")
    return tmp / "a0e_out" / "p1", a0e, legacy_root, legacy


def successor(tmp: Path, render_id="s1", out="v_out", **kw):
    p1, p2 = inputs(tmp)
    return v.render(p1, p2, tmp / out, render_id, variant=kw.pop("variant", A0M), channel=kw.pop("channel", m.CHANNEL))


def benchmark_path(root: Path, manifest_wired: str) -> Path:
    return Path(root) / manifest_wired / "benchmark.yaml"


def clone(tmp: Path, root: Path, name: str) -> Path:
    dest = tmp / name / "x"
    shutil.copytree(root, dest)
    return dest


# ------------------------------------------------------------------------------------------------------------------------------- 29 collision reproduction
def test_29_the_historical_a0e_and_a0m_renders_share_a_run_id_and_a_container_path_and_nothing_says_which_is_which(tmp_path):
    a0e_dir, a0e, legacy_root, legacy = historical(tmp_path)
    assert a0e["run_id"] == legacy["run_id"] == m.RUN_ID
    facts_e = lv.classify_directory(a0e_dir)
    facts_m = lv.classify_directory(legacy_root)
    assert facts_e["variant"] == A0E and facts_m["variant"] == A0M
    assert facts_e["facts"]["container_run_dir"] == facts_m["facts"]["container_run_dir"] == f"/hyperledger/caliper/workspace/runs/{m.RUN_ID}"
    assert not any(k in a0e or k in legacy for k in ("variant", "launch_variant")) and not lv.marker_path(legacy_root).exists()           # neither manifest names a variant
    report = lv.check_collision([a0e_dir, legacy_root])
    assert report["ok"] is False and len(report["collisions"]) == 1 and {e["variant"] for e in report["collisions"][0]["entries"]} == {A0E, A0M}
    # the A0e launch manifest's step R09 names the same benchmark path for both
    r09 = next(s for s in a0e["future_execution"]["steps"] if s["step"] == "R09_caliper_smoke")
    assert f"runs/{m.RUN_ID}/benchmark.yaml" in r09["argv"]
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.verify_marker(legacy_root, A0M)
    assert exc.value.code == "variant_marker_missing"                                      # a historical A0m render is never accepted as the isolated variant


# ------------------------------------------------------------------------------------------------------------------------------- 30 distinct identities
def test_30_the_successor_gives_the_same_base_run_id_a_distinct_id_directory_and_container_path(tmp_path):
    a0e_dir, a0e, legacy_root, legacy = historical(tmp_path)
    out = successor(tmp_path)
    root, marker = Path(out["root"]), out["marker"]
    assert marker["base_run_id"] == m.RUN_ID and marker["qualified_run_id"] == m.RUN_ID + "-v3il" and out["manifest"]["run_id"] == m.RUN_ID + "-v3il"
    assert root == tmp_path / "v_out" / "a0m-v3il" / "s1" and lv.marker_path(root).is_file() and not (root / "LAUNCH_VARIANT.json").exists()
    report = lv.check_collision([a0e_dir, root])
    assert report["ok"] is True and report["collisions"] == []
    facts = lv.verify_marker(root, A0M)
    assert facts["container_run_dir"] == f"/hyperledger/caliper/workspace/runs/{m.RUN_ID}-v3il" != lv.classify_directory(a0e_dir)["facts"]["container_run_dir"]
    for path_text in lv.classify_directory(root)["facts"]["issue_log_paths"]:
        assert path_text.startswith(f"/hyperledger/caliper/workspace/runs/{m.RUN_ID}-v3il/issuelog/")
    assert marker["fallback"] == "none" and marker["execution"] == "NOT_AUTHORIZED" and marker["not_for_launch"] and marker["launch_authorizing_directory"].startswith("wired_v3/")
    assert lv.variant_directory(tmp_path, A0E, "s1") != lv.variant_directory(tmp_path, A0M, "s1")
    preflight = v.preflight(root, A0M)
    assert preflight["offline_preflight"] == "PASS" and preflight["live_ready"] is False and preflight["counts"]["FAIL"] == 0
    assert [c["id"] for c in preflight["checks"]] == ["V01_manifest_belongs_to_variant", "V02_marker_matches_generated_files", "V03_run_id_is_qualified",
                                                       "V04_modules_match_variant", "V05_channel_not_protected", "V06_a0m_preflight"]


def test_30b_run_id_qualification_never_truncates_and_never_double_qualifies():
    assert lv.qualified_run_id("abc", A0M) == "abc-v3il" and lv.qualified_run_id("abc", A0E) == "abc"
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.qualified_run_id("x" * 36, A0M)
    assert exc.value.code == "run_id_too_long"
    assert lv.qualified_run_id("x" * 35, A0M) == "x" * 35 + "-v3il"
    with pytest.raises(lv.LaunchVariantError) as exc2:
        lv.qualified_run_id("abc-v3il", A0M)
    assert exc2.value.code == "run_id_already_qualified"
    for bad in ("", "has space", None):
        with pytest.raises(lv.LaunchVariantError):
            lv.qualified_run_id(bad, A0M)
    with pytest.raises(lv.LaunchVariantError) as exc3:
        lv.qualified_run_id("abc", "a0z_unknown")
    assert exc3.value.code == "variant_unknown"


# ------------------------------------------------------------------------------------------------------------------------------- 31 conflicting directory
def test_31_an_occupied_directory_is_refused_and_the_message_says_whose_it_is(tmp_path):
    a0e_dir, a0e, legacy_root, legacy = historical(tmp_path)
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.reject_if_occupied(a0e_dir, A0M)
    assert exc.value.code == "directory_belongs_to_other_variant" and A0E in str(exc.value)
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.reject_if_occupied(legacy_root, A0E)
    assert exc.value.code == "directory_belongs_to_other_variant"
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.reject_if_occupied(legacy_root, A0M)
    assert exc.value.code == "directory_exists_same_variant"
    stray = tmp_path / "stray"
    stray.mkdir()
    (stray / "notes.txt").write_text("not a run")
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.reject_if_occupied(stray, A0M)
    assert exc.value.code == "directory_unclassified"
    lv.reject_if_occupied(tmp_path / "does_not_exist", A0M)                                # a free path is accepted
    out = successor(tmp_path)
    with pytest.raises(lv.LaunchVariantError) as exc:                                       # a second render into the same place
        successor(tmp_path)
    assert exc.value.code == "directory_exists_same_variant"
    shutil.rmtree(out["root"])
    with pytest.raises(lv.LaunchVariantError) as exc:                                       # the marker outlived its directory
        lv.reject_if_occupied(Path(out["root"]), A0M)
    assert exc.value.code == "stale_marker_without_directory"


def test_31b_a_render_into_an_other_variants_directory_changes_nothing_and_exits_2(tmp_path):
    out_root = tmp_path / "v_out"
    squat = out_root / "a0m-v3il" / "s1"
    shutil.copytree(historical(tmp_path)[0], squat)                                        # an A0e preparation sitting where the A0m variant would go
    before = sorted(p.relative_to(out_root).as_posix() for p in out_root.rglob("*"))
    p1, p2 = inputs(tmp_path, run_id="other-001")
    code = v.main(["render", "--variant", A0M, "--params", str(p1), "--a0m-params", str(p2), "--channel", m.CHANNEL, "--out-root", str(out_root), "--render-id", "s1"])
    assert code == 2 and sorted(p.relative_to(out_root).as_posix() for p in out_root.rglob("*")) == before


# ------------------------------------------------------------------------------------------------------------------------------- 32-34 wrong selections
def damaged(tmp: Path, mutate, name="d"):
    out = successor(tmp, render_id=f"{name}1", out=f"{name}_out")
    root = clone(tmp, Path(out["root"]), name)
    lv.marker_path(root).write_text(lv.marker_path(Path(out["root"])).read_text())
    path = benchmark_path(root, out["manifest"]["wired_v3_dir"])
    doc = json.loads(path.read_text())
    mutate(doc)
    path.write_text(json.dumps(doc, indent=2) + "\n")
    return root, doc


def failed_checks(result):
    return {c["id"] for c in result["checks"] if c["status"] == "FAIL"}


def test_32_a_recorder_v2_configuration_cannot_pass_as_recorder_v3(tmp_path):
    def swap(doc):
        mon = doc["monitors"]["transaction"][0]
        mon["module"] = posixpath.join(posixpath.dirname(mon["module"]), lv.RECORDER_V2)
    root, doc = damaged(tmp_path, swap)
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.derive_variant(doc)
    assert exc.value.code == "variant_mixed_or_unknown" and lv.RECORDER_V2 in str(exc.value)
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.verify_marker(root, A0M)
    assert exc.value.code == "generated_files_do_not_match_variant"
    result = v.preflight(root, A0M)
    assert result["offline_preflight"] == "FAIL" and {"V02_marker_matches_generated_files", "V06_a0m_preflight"} <= failed_checks(result)


def test_32b_a_whole_a0e_configuration_dropped_into_the_a0m_slot_is_rejected_whatever_its_manifest_says(tmp_path):
    a0e_dir, a0e, *_ = historical(tmp_path)
    out = successor(tmp_path)
    root = clone(tmp_path, Path(out["root"]), "swap")
    lv.marker_path(root).write_text(lv.marker_path(Path(out["root"])).read_text())
    shutil.copyfile(a0e_dir / a0e["wired_run_dir"] / "benchmark.yaml", benchmark_path(root, out["manifest"]["wired_v3_dir"]))
    assert lv.classify_directory(root)["variant"] == A0E
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.verify_marker(root, A0M)
    assert exc.value.code == "generated_files_do_not_match_variant"
    assert "V02_marker_matches_generated_files" in failed_checks(v.preflight(root, A0M))


def test_33_the_legacy_workload_cannot_pass_as_the_issue_logged_workload(tmp_path):
    def legacy_workload(doc):
        for rnd in doc["test"]["rounds"]:
            rnd["workload"]["module"] = posixpath.join(posixpath.dirname(rnd["workload"]["module"]), lv.WORKLOAD_LEGACY)
    root, doc = damaged(tmp_path, legacy_workload, "wl")
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.derive_variant(doc)
    assert exc.value.code == "variant_mixed_or_unknown" and lv.WORKLOAD_LEGACY in str(exc.value)
    assert "V02_marker_matches_generated_files" in failed_checks(v.preflight(root, A0M))

    def drop_issue_log(doc):
        del doc["test"]["rounds"][0]["workload"]["arguments"]["issueLog"]
    _root, doc2 = damaged(tmp_path, drop_issue_log, "il")
    with pytest.raises(lv.LaunchVariantError):
        lv.derive_variant(doc2)

    def drop_health(doc):
        del doc["monitors"]["transaction"][0]["options"]["health"]
    _root, doc3 = damaged(tmp_path, drop_health, "hl")
    with pytest.raises(lv.LaunchVariantError):
        lv.derive_variant(doc3)


def test_34_a_manifest_of_the_other_variant_never_authorizes_a_run(tmp_path):
    a0e_dir, a0e, legacy_root, legacy = historical(tmp_path)
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.authorize_manifest(a0e, A0M)
    assert exc.value.code == "stale_manifest_wrong_variant" and A0E in str(exc.value)
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.authorize_manifest(legacy, A0E)
    assert exc.value.code == "stale_manifest_wrong_variant"
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.authorize_manifest({"schema": "something/9"}, A0M)
    assert exc.value.code == "manifest_schema_unknown"
    lv.authorize_manifest(a0e, A0E)
    lv.authorize_manifest(legacy, A0M)
    out = successor(tmp_path)
    root = Path(out["root"])
    stale = clone(tmp_path, root, "stale")
    lv.marker_path(stale).write_text(lv.marker_path(root).read_text())
    (stale / "a0m_config_manifest.json").write_text(json.dumps(a0e))                       # the A0e launch manifest placed where the A0m manifest belongs
    result = v.preflight(stale, A0M)
    assert result["offline_preflight"] == "FAIL" and failed_checks(result) == {"V01_manifest_belongs_to_variant"}
    wrong_id = clone(tmp_path, root, "wrongid")
    mf = json.loads((wrong_id / "a0m_config_manifest.json").read_text())
    mf["run_id"] = m.RUN_ID                                                                 # the unqualified id of the A0e run
    (wrong_id / "a0m_config_manifest.json").write_text(json.dumps(mf))
    lv.marker_path(wrong_id).write_text(lv.marker_path(root).read_text())
    assert "V03_run_id_is_qualified" in failed_checks(v.preflight(wrong_id, A0M))


def test_34b_the_selected_pair_is_read_from_the_generated_files_and_matches_the_markers_claim(tmp_path):
    out = successor(tmp_path)
    root = Path(out["root"])
    bench = json.loads(benchmark_path(root, out["manifest"]["wired_v3_dir"]).read_text())
    facts = lv.derive_variant(bench)
    assert facts["variant"] == A0M and facts["monitor_module"] == lv.RECORDER_V3 and facts["workload_modules"] == [lv.WORKLOAD_LOGGED]
    assert out["marker"]["selected_recorder_file"] == facts["monitor_module"] and out["marker"]["selected_workload_file"] == facts["workload_modules"][0]
    tampered = json.loads(lv.marker_path(root).read_text())
    tampered["selected_recorder_file"] = lv.RECORDER_V2
    lv.marker_path(root).write_text(json.dumps(tampered))
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.verify_marker(root, A0M)
    assert exc.value.code == "marker_selection_mismatch"
    tampered.update(selected_recorder_file=lv.RECORDER_V3, variant=A0E)
    lv.marker_path(root).write_text(json.dumps(tampered))
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.verify_marker(root, A0M)
    assert exc.value.code == "variant_marker_mismatch"


# ------------------------------------------------------------------------------------------------------------------------------- 35 protected channel
@pytest.mark.parametrize("channel", ["e1-r002", "E1-CFG001", "e1-b001"])
def test_35_a_protected_reference_channel_is_refused_before_anything_is_created(tmp_path, channel):
    p1, p2 = inputs(tmp_path)
    out_root = tmp_path / "v_out"
    with pytest.raises(cs.ChannelSelectionError):
        v.render(p1, p2, out_root, "s1", variant=A0M, channel=channel)
    assert not out_root.exists()
    for bad in (None, "", " "):
        with pytest.raises(cs.ChannelSelectionError):
            v.render(p1, p2, out_root, "s1", variant=A0M, channel=bad)
        assert not out_root.exists()
    with pytest.raises(SystemExit) as parse_exit:                                           # the command line refuses a protected name before any code runs
        v.main(["render", "--variant", A0M, "--params", str(p1), "--a0m-params", str(p2), "--channel", channel, "--out-root", str(out_root), "--render-id", "s1"])
    assert parse_exit.value.code == 2 and not out_root.exists()


def test_35b_a_manifest_naming_a_protected_channel_fails_the_variant_preflight(tmp_path):
    out = successor(tmp_path)
    root = clone(tmp_path, Path(out["root"]), "prot")
    lv.marker_path(root).write_text(lv.marker_path(Path(out["root"])).read_text())
    mf = json.loads((root / "a0m_config_manifest.json").read_text())
    mf["channel"] = "e1-r002"
    (root / "a0m_config_manifest.json").write_text(json.dumps(mf))
    assert "V05_channel_not_protected" in failed_checks(v.preflight(root, A0M))
    # no channel name is invented anywhere: the successor takes the name only from --channel and the parameter file, and the two must agree
    (tmp_path / "x").mkdir()
    p1, p2 = inputs(tmp_path / "x")
    with pytest.raises(cs.ChannelSelectionError):
        v.render(p1, p2, tmp_path / "x" / "o", "s1", variant=A0M, channel="e1-offline.other")


# ------------------------------------------------------------------------------------------------------------------------------- 36 no fallback
def test_36_there_is_no_default_variant_and_no_implicit_fallback(tmp_path):
    p1, p2 = inputs(tmp_path)
    with pytest.raises(lv.LaunchVariantError) as exc:
        v.render(p1, p2, tmp_path / "o", "s1", variant=None, channel=m.CHANNEL)
    assert exc.value.code == "variant_missing"
    with pytest.raises(lv.LaunchVariantError) as exc:
        v.render(p1, p2, tmp_path / "o", "s1", variant=A0E, channel=m.CHANNEL)
    assert exc.value.code == "variant_not_rendered_here"
    with pytest.raises(lv.LaunchVariantError) as exc:
        v.render(p1, p2, tmp_path / "o", "s1", variant="recorder_v2", channel=m.CHANNEL)
    assert exc.value.code == "variant_unknown" and not (tmp_path / "o").exists()
    with pytest.raises(SystemExit):
        v.main(["render", "--params", str(p1), "--a0m-params", str(p2), "--channel", m.CHANNEL, "--out-root", str(tmp_path / "o"), "--render-id", "s1"])     # --variant is required
    out = successor(tmp_path)
    root = Path(out["root"])
    marker = json.loads(lv.marker_path(root).read_text())
    marker["fallback"] = "recorder_v2"
    lv.marker_path(root).write_text(json.dumps(marker))
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.verify_marker(root, A0M)
    assert exc.value.code == "fallback_declared"
    wired = root / out["manifest"]["wired_v3_dir"]
    shutil.copytree(wired, tmp_path / "w2")
    (tmp_path / "w2" / "modules" / lv.RECORDER_V3).unlink()                                  # recorder v3 absent: v2 is present, and it is NOT substituted
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.verify_modules(tmp_path / "w2", A0M)
    assert exc.value.code == "modules_do_not_match_variant" and lv.RECORDER_V3 in str(exc.value)
    assert (tmp_path / "w2" / "modules" / lv.RECORDER_V2).is_file()
    assert lv.verify_modules(wired, A0M)["missing"] == []
    assert v.main(["execute"]) == 5


# ------------------------------------------------------------------------------------------------------------------------------- 37 legacy preserved
def test_37_the_historical_a0e_and_a0m_tools_behave_exactly_as_before(tmp_path):
    a0e_dir, a0e, legacy_root, legacy = historical(tmp_path)
    assert legacy["run_id"] == m.RUN_ID and legacy["wired_v3_dir"] == f"wired_v3/{m.RUN_ID}"                # unqualified, as it always was
    assert pv.preflight(a0e_dir, write=False)["offline_preflight"] == "PASS"
    import e1_a0m_dry_render as dr
    assert dr.preflight(legacy_root, write=False)["offline_preflight"] == "PASS"
    before = {p: hashlib.sha256((REPO / p).read_bytes()).hexdigest() for p in ("scripts/e1/e1_a0m_dry_render.py", "scripts/e1/e1_prelive_validation.py", "scripts/e1/e1_la2_render.py")}
    successor(tmp_path)
    assert before == {p: hashlib.sha256((REPO / p).read_bytes()).hexdigest() for p in before}
    # the stored A0m evidence render (run id a0m-dry-001, no marker) is still a valid A0m render of the historical kind
    stored = REPO / "docs/research/e1/a0m_dry_render_evidence_2026-11-21/render/a0m-evidence-001"
    assert lv.classify_directory(stored)["variant"] == A0M and not lv.marker_path(stored).exists()


# ------------------------------------------------------------------------------------------------------------------------------- no live service
def test_38_prepare_and_preflight_cannot_start_a_live_service():
    forbidden = {"subprocess", "socket", "requests", "http", "urllib", "docker", "asyncio", "multiprocessing", "ssl", "ftplib", "telnetlib", "paramiko"}
    for rel in ("scripts/e1/e1_a0o_launch_variant.py", "src/fabric_perf/controllers/launch_variant.py"):
        tree = ast.parse((REPO / rel).read_text(encoding="utf-8"))
        names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, ast.ImportFrom) and node.module:
                names.add(node.module.split(".")[0])
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr in {"system", "popen", "Popen", "run", "check_output", "startfile"}:
                pytest.fail(f"{rel} calls {node.func.attr}")
        assert not names & forbidden, (rel, names & forbidden)
    assert v.refuse_execute() == 5
