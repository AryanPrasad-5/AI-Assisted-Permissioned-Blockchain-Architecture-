"""A0p workstream F: how the timed writer and the incremental bridge are SELECTED in a future authorized validation, and what is refused.

Real renders into temporary directories (the A0m dry render runs unchanged); nothing starts Caliper, Fabric, Docker, WSL or a network. Node is not used here."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import shutil
import socket
import subprocess
from pathlib import Path

import pytest

from tests import a0m_support as m
from tests.a0p_support import REPO, SOURCE_ID, bridge_config, create_bridge, j, lines_of, resume_bridge, rhi, spool_recorder, wc, write_lines

import e1_a0o_launch_variant as v0   # noqa: E402  (scripts/e1 is on sys.path through a0m_support)
import e1_a0p_launch_variant as v    # noqa: E402

from fabric_perf.controllers import channel_selection as cs      # noqa: E402
from fabric_perf.controllers import launch_variant as lv         # noqa: E402
from fabric_perf.controllers import timed_variant as tv          # noqa: E402

A0P = tv.VARIANT_A0P


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a process, container or network use is forbidden")
    monkeypatch.setattr(subprocess, "Popen", refuse)
    monkeypatch.setattr(socket, "socket", refuse)
    monkeypatch.setattr(os, "system", refuse)


def inputs(tmp: Path, **a0e):
    return m.write(tmp / "a0e_params.json", m.a0e_params(**a0e)), m.write(tmp / "a0m_params.json", m.a0m_params())


def render(tmp: Path, render_id="p1", out="out", **kw):
    p1, p2 = inputs(tmp)
    return v.render(p1, p2, tmp / out, render_id, variant=kw.pop("variant", A0P), channel=kw.pop("channel", m.CHANNEL))


def overlay_bench_path(out: dict) -> Path:
    root = Path(out["root"])
    manifest = json.loads((root / "a0m_config_manifest.json").read_text(encoding="utf-8"))
    return Path(out["overlay"]) / "wired_v3_timed" / Path(manifest["wired_v3_dir"]).name / "benchmark.yaml"


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def save(path: Path, doc) -> None:
    path.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")


# ----------------------------------------------------------------------------------------------------------------------------- selection and identity
def test_c01_the_timed_variant_is_selected_by_name_under_a_qualified_run_id_and_directory(tmp_path):
    out = render(tmp_path)
    marker = out["marker"]
    assert marker["variant"] == A0P and marker["qualified_run_id"] == m.RUN_ID + "-v3ilt" and marker["base_run_id"] == m.RUN_ID and marker["fallback"] == "none"
    assert marker["selected_workload_file"] == "e1_create_asset_issue_logged_timed.js" and marker["selected_writer_file"] == "e1_issue_log_timed.js"
    assert Path(out["root"]).parent.name == "a0p-v3ilt" and Path(out["overlay"]).name.endswith(".timed_overlay") and tv.marker_path(Path(out["root"])).is_file()
    bench = load(overlay_bench_path(out))
    facts = tv.derive_timed_variant(bench)
    assert facts["run_id"] == marker["qualified_run_id"] and facts["container_run_dir"].endswith("/runs/" + marker["qualified_run_id"])
    assert marker["execution"] == "NOT_AUTHORIZED" and marker["caliper_invoked"] is False and marker["fabric_contacted"] is False and marker["docker_or_wsl_started"] is False


def test_c02_no_variant_is_assumed_and_the_other_variants_have_their_own_unchanged_tools(tmp_path):
    p1, p2 = inputs(tmp_path)
    with pytest.raises(tv.TimedVariantError) as none:
        v.render(p1, p2, tmp_path / "o", "x", variant=None, channel=m.CHANNEL)
    assert none.value.code == "variant_missing"
    for other in (lv.VARIANT_A0M, lv.VARIANT_A0E, "something_else"):
        with pytest.raises(tv.TimedVariantError) as refused:
            v.render(p1, p2, tmp_path / "o", "x", variant=other, channel=m.CHANNEL)
        assert refused.value.code == "variant_not_rendered_here"
    assert not (tmp_path / "o").exists()
    assert sorted(lv.VARIANTS) == [lv.VARIANT_A0E, lv.VARIANT_A0M]                                  # A0o's variant table was not extended or edited


def test_c03_the_validated_a0o_variant_still_renders_and_the_a0o_classifier_refuses_the_timed_benchmark(tmp_path):
    p1, p2 = inputs(tmp_path)
    a0o = v0.render(p1, p2, tmp_path / "a0o_out", "s1", variant=lv.VARIANT_A0M, channel=m.CHANNEL)
    assert a0o["marker"]["qualified_run_id"] == m.RUN_ID + "-v3il" and lv.verify_marker(Path(a0o["root"]), lv.VARIANT_A0M)["variant"] == lv.VARIANT_A0M
    out = render(tmp_path, "p1", out="a0p_out")
    with pytest.raises(lv.LaunchVariantError) as exc:
        lv.derive_variant(load(overlay_bench_path(out)))                                           # the A0o derivation does not accept it: no silent upgrade in either direction
    assert exc.value.code == "variant_mixed_or_unknown"
    with pytest.raises(tv.TimedVariantError) as back:
        tv.derive_timed_variant(load(Path(a0o["root"]) / json.loads((Path(a0o["root"]) / "a0m_config_manifest.json").read_text())["wired_v3_dir"] / "benchmark.yaml"))
    assert back.value.code == "not_the_timed_workload"                                              # ... and the timed one does not accept an A0m benchmark either
    assert lv.verify_marker(Path(a0o["root"]), lv.VARIANT_A0M)["run_id"] != tv.qualified_run_id(m.RUN_ID)


def test_c04_run_ids_are_qualified_never_truncated_and_never_qualified_twice():
    assert tv.qualified_run_id("abc") == "abc-v3ilt"
    for bad, code in (("abc-v3ilt", "run_id_already_qualified"), ("abc-v3il", "run_id_already_qualified"), ("x" * 35, "run_id_too_long"), ("bad id", "run_id_invalid")):
        with pytest.raises(tv.TimedVariantError) as exc:
            tv.qualified_run_id(bad)
        assert exc.value.code == code
    assert tv.qualified_run_id("x" * 34) == "x" * 34 + "-v3ilt" and len(tv.qualified_run_id("x" * 34)) == 40


# ----------------------------------------------------------------------------------------------------------------------------- preflight and dependencies
def test_c05_the_preflight_passes_on_the_untouched_a0m_render_and_the_overlay_and_is_never_live_ready(tmp_path):
    out = render(tmp_path)
    result = v.preflight(Path(out["root"]), A0P)
    assert result["offline_preflight"] == "PASS" and result["live_ready"] is False and [c["id"] for c in result["checks"]] == [
        "W01_base_render_is_an_a0m_render", "W02_overlay_matches_marker_and_render", "W03_module_requires_resolve", "W04_channel_not_protected", "W05_a0m_preflight_on_untouched_render"]
    assert "not an authorization" in result["statement"]
    assert v.preflight(Path(out["root"]), lv.VARIANT_A0M)["offline_preflight"] == "FAIL"                # no fallback to another variant


def test_c06_dependencies_resolve_from_the_generated_modules_directory_only_and_match_the_repository(tmp_path):
    out = render(tmp_path)
    overlay_wired = overlay_bench_path(out).parent
    modules = overlay_wired / "modules"
    import e1_a0m_dry_render as dr
    graph = dr.resolve_requires(modules, tv.ENTRY_MODULES)["graph"]
    assert graph["e1_create_asset_issue_logged_timed.js"] == ["e1_create_asset.js", "e1_issue_log_timed.js", "e1_workload_core.js"]
    assert graph["e1_issue_log_timed.js"] == ["e1_issue_log.js"]
    for name, rel in tv.SOURCES.items():
        assert (modules / name).read_bytes() == (REPO / rel).read_bytes()
    for name in ("e1_issue_log.js", "e1_create_asset.js", "e1_workload_core.js", "e1_per_tx_recorder_v3.js"):
        assert (modules / name).read_bytes() == ((overlay_wired.parent.parent.parent / Path(out["root"]).name / json.loads((Path(out["root"]) / "a0m_config_manifest.json").read_text())["wired_v3_dir"] / "modules" / name).read_bytes())
    pinned = hashlib.sha256((modules / "e1_per_tx_recorder.js").read_bytes()).hexdigest()
    assert pinned.startswith("b5496af3")                                                              # the pinned recorder v1 reaches the overlay unchanged


def test_c07_every_round_has_its_own_log_and_sidecar_inside_the_run_directory_and_one_worker(tmp_path):
    out = render(tmp_path)
    facts = tv.derive_timed_variant(load(overlay_bench_path(out)))
    logs, timings = facts["issue_log_paths"], facts["timing_paths"]
    assert len(logs) == len(timings) >= 2 and len(set(logs + timings)) == len(logs) * 2
    assert all(p.startswith(facts["container_run_dir"] + "/") for p in logs + timings) and all(t.endswith(".issuelog.timing.jsonl") for t in timings)
    for log, timing in zip(logs, timings):
        assert timing == log.replace(".issuelog.jsonl", ".issuelog.timing.jsonl") and "_r" in log                    # the round index is in the name
    bench = load(overlay_bench_path(out))
    assert {rnd["workload"]["arguments"]["issueLogTimed"]["workerId"] for rnd in bench["test"]["rounds"]} == {"w0"}
    assert {tuple(rnd["workload"]["arguments"]["issueLogTimed"]["expectedWorkers"]) for rnd in bench["test"]["rounds"]} == {("w0",)}


# ----------------------------------------------------------------------------------------------------------------------------- mixed versions
@pytest.mark.parametrize("name,change,code", [
    ("issue_log_alongside", lambda b: b["test"]["rounds"][0]["workload"]["arguments"].update(issueLog={"path": "/x"}), "mixed_issue_log_schema"),
    ("timed_arg_missing", lambda b: b["test"]["rounds"][0]["workload"]["arguments"].pop("issueLogTimed"), "issue_log_timed_missing"),
    ("timing_equals_log", lambda b: b["test"]["rounds"][0]["workload"]["arguments"]["issueLogTimed"].update(
        timingPath=b["test"]["rounds"][0]["workload"]["arguments"]["issueLogTimed"]["path"]), "timing_path_invalid"),
    ("timing_outside_run_dir", lambda b: b["test"]["rounds"][0]["workload"]["arguments"]["issueLogTimed"].update(timingPath="/tmp/elsewhere.jsonl"), "issue_log_outside_run_directory"),
    ("timing_reused", lambda b: b["test"]["rounds"][1]["workload"]["arguments"]["issueLogTimed"].update(
        timingPath=b["test"]["rounds"][0]["workload"]["arguments"]["issueLogTimed"]["timingPath"]), "log_path_reused"),
    ("two_workers", lambda b: b["test"]["rounds"][0]["workload"]["arguments"]["issueLogTimed"].update(expectedWorkers=["w0", "w1"]), "worker_restriction"),
    ("unknown_key", lambda b: b["test"]["rounds"][0]["workload"]["arguments"]["issueLogTimed"].update(fallback="legacy"), "issue_log_timed_keys"),
    ("recorder_v2", lambda b: b["monitors"]["transaction"][0].update(module=b["monitors"]["transaction"][0]["module"].replace("recorder_v3", "recorder_v2")), "recorder_mismatch"),
    ("no_health", lambda b: b["monitors"]["transaction"][0]["options"].pop("health"), "recorder_mismatch"),
    ("a0m_workload", lambda b: b["test"]["rounds"][0]["workload"].update(module=b["test"]["rounds"][0]["workload"]["module"].replace("_timed", "")), "not_the_timed_workload"),
])
def test_c08_a_mixed_or_incomplete_configuration_is_refused_not_repaired(tmp_path, name, change, code):
    bench = load(overlay_bench_path(render(tmp_path)))
    broken = copy.deepcopy(bench)
    change(broken)
    with pytest.raises(tv.TimedVariantError) as exc:
        tv.derive_timed_variant(broken)
    assert exc.value.code == code, name
    assert tv.derive_timed_variant(bench)["variant"] == A0P                                          # the unbroken one is still accepted


def test_c09_the_conversion_only_accepts_the_a0m_variant_and_is_deterministic(tmp_path):
    out = render(tmp_path)
    a0m_bench = load(Path(out["root"]) / json.loads((Path(out["root"]) / "a0m_config_manifest.json").read_text())["wired_v3_dir"] / "benchmark.yaml")
    assert tv.timed_benchmark(a0m_bench) == load(overlay_bench_path(out)) and tv.timed_benchmark(a0m_bench) == tv.timed_benchmark(copy.deepcopy(a0m_bench))
    with pytest.raises(tv.TimedVariantError) as again:
        tv.timed_benchmark(load(overlay_bench_path(out)))                                            # converting a converted benchmark is refused
    assert again.value.code == "not_the_a0m_variant"


def test_c10_the_incompatible_bridge_checkpoint_or_journal_version_is_rejected(tmp_path):
    rec = spool_recorder(tmp_path)
    cfg = bridge_config(rec, tmp_path, max_rows_per_poll=6)
    create_bridge(cfg)[0].poll()
    body = {k: v_ for k, v_ in json.loads(cfg.checkpoint_path.read_text(encoding="ascii")).items() if k != "self_sha256"}
    body["bridge_version"] = "recorder_health_incremental 0.0 (older)"
    cfg.checkpoint_path.write_text(wc.canonical({**body, "self_sha256": hashlib.sha256(wc.canonical(body).encode("utf-8")).hexdigest()}) + "\n", encoding="ascii")
    with pytest.raises(rhi.BridgeError) as old:
        resume_bridge(cfg)
    assert old.value.code == "bridge_version_mismatch"
    other = tmp_path / "j"
    other.mkdir()
    rec2 = spool_recorder(other)
    cfg2 = bridge_config(rec2, other, max_rows_per_poll=6)
    create_bridge(cfg2)[0].poll()
    rows = [json.loads(line) for line in lines_of(cfg2.journal_path)]
    for row in rows:
        row["schema"] = "collector_health_event/2"
    write_lines(cfg2.journal_path, [wc.canonical(r).encode("utf-8") for r in rows])
    with pytest.raises(rhi.BridgeError) as mixed:
        resume_bridge(cfg2)
    assert mixed.value.code in ("journal_schema_mismatch", "journal_not_verified")


# ----------------------------------------------------------------------------------------------------------------------------- tampering and refusals
def test_c11_a_tampered_overlay_or_a_foreign_marker_is_detected(tmp_path):
    out = render(tmp_path)
    root, overlay = Path(out["root"]), Path(out["overlay"])
    wired = overlay_bench_path(out).parent
    module = wired / "modules" / "e1_issue_log_timed.js"
    original = module.read_bytes()
    module.write_bytes(original + b"\n// edited\n")
    assert v.preflight(root, A0P)["offline_preflight"] == "FAIL"
    module.write_bytes(original)
    bench = load(wired / "benchmark.yaml")
    bench["test"]["rounds"][0]["workload"]["arguments"]["issueLogTimed"]["tickIntervalMs"] = 7
    save(wired / "benchmark.yaml", bench)
    with pytest.raises(tv.TimedVariantError) as derived:
        tv.verify_overlay(root, overlay, REPO)
    assert derived.value.code == "overlay_not_derived_from_render"
    assert v.preflight(root, A0P)["checks"][1]["status"] == "FAIL"
    marker = tv.marker_path(root)
    doc = json.loads(marker.read_text(encoding="utf-8"))
    doc["schema"] = lv.MARKER_SCHEMA
    save(marker, doc)
    with pytest.raises(tv.TimedVariantError) as foreign:
        tv.verify_overlay(root, overlay, REPO)
    assert foreign.value.code in ("variant_marker_schema", "overlay_not_derived_from_render")
    marker.unlink()
    with pytest.raises(tv.TimedVariantError) as missing:
        tv.verify_overlay(root, overlay, REPO)
    assert missing.value.code == "variant_marker_missing"


def test_c12_a_historical_a0m_or_a0o_render_is_never_accepted_as_the_timed_variant(tmp_path):
    p1, p2 = inputs(tmp_path)
    a0o = v0.render(p1, p2, tmp_path / "a0o_out", "s1", variant=lv.VARIANT_A0M, channel=m.CHANNEL)
    result = v.preflight(Path(a0o["root"]), A0P)
    assert result["offline_preflight"] == "FAIL" and result["checks"][1]["status"] == "FAIL"             # the A0o marker is another variant's
    historical_root, historical = m.render(tmp_path, render_id="legacy")
    assert v.preflight(Path(historical_root), A0P)["offline_preflight"] == "FAIL"


def test_c13_a_protected_channel_an_occupied_directory_and_the_execute_phase_are_refused(tmp_path):
    p1, p2 = inputs(tmp_path)
    for channel in ("e1-r002", "e1-cfg001"):
        with pytest.raises(cs.ChannelSelectionError):
            v.render(p1, p2, tmp_path / "o", "x", variant=A0P, channel=channel)
    assert not (tmp_path / "o").exists()
    with pytest.raises(SystemExit) as argparse_refusal:
        v.main(["render", "--variant", A0P, "--params", str(p1), "--a0m-params", str(p2), "--channel", "e1-r002", "--out-root", str(tmp_path / "o2"), "--render-id", "x"])
    assert argparse_refusal.value.code == 2 and not (tmp_path / "o2").exists()
    render(tmp_path, "dup", out="d")
    with pytest.raises(tv.TimedVariantError) as occupied:
        render(tmp_path, "dup", out="d")
    assert occupied.value.code == "output_occupied"
    assert v.main(["execute"]) == 5
    assert v.main(["render", "--variant", A0P, "--params", str(p1), "--a0m-params", str(p2), "--channel", m.CHANNEL, "--out-root", str(tmp_path / "d"), "--render-id", "dup"]) == 2
    assert v.main(["preflight", "--variant", lv.VARIANT_A0M, "--run-dir", str(tmp_path / "d" / "a0p-v3ilt" / "dup")]) == 1
