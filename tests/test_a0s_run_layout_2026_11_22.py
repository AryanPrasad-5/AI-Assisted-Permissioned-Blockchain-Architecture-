"""A0s workstream E: the unified run layout and instrumentation selection, built from a REAL offline A0p render. Nothing starts a process or Fabric."""

from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path

import pytest

from tests import a0m_support as m
from tests import a0s_support as s
from tests.a0s_support import REPO

import e1_a0m_dry_render as dr      # noqa: E402

from fabric_perf.controllers import run_layout as rl            # noqa: E402
from fabric_perf.controllers import timed_variant as tv         # noqa: E402


@pytest.fixture(scope="module")
def rendered(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("a0s_layout")
    return tmp, s.render_a0p(tmp)


def layout(rendered, mode="deferred", out="run_out"):
    tmp, root = rendered
    return rl.build_layout(root, tmp / out, repo_root=REPO, prometheus_mode=mode)


def test_01_layout_comes_from_the_rendered_benchmark_not_from_invention(rendered):
    L = layout(rendered)
    bench = json.loads((Path(L["launch_authorizing_directory"]) / "benchmark.yaml").read_text(encoding="utf-8"))
    assert L["variant"] == tv.VARIANT_A0P and L["run_id"] == "a0m-test-001-v3ilt"
    assert [r["label"] for r in L["rounds"]] == [r["label"] for r in bench["test"]["rounds"]]
    for rnd, source in zip(L["rounds"], bench["test"]["rounds"]):
        args = source["workload"]["arguments"]["issueLogTimed"]
        assert rnd["issue_log"]["container"] == args["path"] and rnd["timing_sidecar"]["container"] == args["timingPath"]
        assert rnd["rate_tps"] == source["rateControl"]["opts"]["tps"] and rnd["tx_duration_s"] == source["txDuration"]
    assert [r["role"] for r in L["rounds"]] == ["warmup", "measured"] and L["nothing_started"] is True
    assert L["caliper_argv"][:5] == ["docker", "exec", "-w", "/hyperledger/caliper/workspace", "caliper"] and L["caliper_argv"][-1] == "--caliper-flow-only-test"


def test_02_per_round_files_carry_the_round_index_and_label_and_never_collide(rendered):
    L = layout(rendered)
    assert rl.verify_layout(L) == []
    paths = []
    for rnd in L["rounds"]:
        i = rnd["round_index"]
        assert f"_w0_r{i}_" in rnd["base"] and rnd["base"].endswith(rnd["label"])
        for entry in (rnd["issue_log"], rnd["timing_sidecar"]):
            assert f"_r{i}_" in entry["container"] and f"_r{i}_" in entry["host"]
            paths += [entry["container"], entry["host"]]
        assert set(rnd["recorder_files"]) == set(rl.RECORDER_SUFFIXES) and all(f"_r{i}_" in p for p in rnd["recorder_files"].values())
        paths += list(rnd["recorder_files"].values())
    assert len(paths) == len(set(paths))
    assert L["rounds"][0]["issue_log"]["host"] != L["rounds"][1]["issue_log"]["host"]


def test_03_host_evidence_slots_cover_every_required_artifact(rendered):
    L = layout(rendered)
    need = {"execution_plan", "approval_reference", "execution_log", "run_status", "process_logs", "config_initial", "config_final", "clock_pre", "clock_post", "clock_verdict",
            "ledger_journal", "ledger_blocks_raw", "measurement_summary", "integrity_manifest", "reader_checkpoints", "generator_coverage_journal", "recorder_health_journal", "timing_results"}
    assert need <= set(L["host_evidence"])
    assert L["host_evidence"]["prometheus_before"] is None and L["host_evidence"]["prometheus_after"] is None
    on = layout(rendered, "before_after", "run_out2")
    assert on["host_evidence"]["prometheus_before"] == "prometheus/before/" and on["host_evidence"]["prometheus_after"] == "prometheus/after/"


def test_04_prometheus_has_no_default_and_a_bad_value_is_refused(rendered):
    tmp, root = rendered
    for bad in (None, "", "latest", "after_only"):
        with pytest.raises(rl.LayoutError) as e:
            rl.build_layout(root, tmp / "x", repo_root=REPO, prometheus_mode=bad)
        assert e.value.code == "prometheus_choice_missing"


def test_05_selection_is_explicit_and_every_deferred_component_states_its_effect(rendered):
    sel = rl.SELECTION
    assert {k: v["state"] for k, v in sel.items()} == {
        "workload": "RUN", "issue_log_writer": "RUN", "recorder": "RUN", "ledger_capture": "RUN", "clock_probe": "RUN", "config_inspection": "RUN", "sourcecount_reader": "POST_RUN",
        "generator_route_recovery": "DEFERRED", "recorder_health_bridge": "POST_RUN", "timing_sampler": "POST_RUN", "prometheus_capture": "OWNER_CHOICE", "stage3_shadow": "DEFERRED",
        "real_inference": "DEFERRED"}
    for name, spec in sel.items():
        if spec["state"] == "DEFERRED":
            assert spec["effect"], name
    assert sel["workload"]["file"] == "e1_create_asset_issue_logged_timed.js" and sel["recorder"]["file"] == "e1_per_tx_recorder_v3.js"
    assert layout(rendered)["selection"] == sel


def test_06_component_schemas_agree():
    assert rl.check_component_compatibility() == []


def test_07_a_schema_disagreement_is_reported(monkeypatch):
    monkeypatch.setattr(rl.ilt, "TIMING_SCHEMA", "e1_issue_log_timing/2")
    assert any(p["item"] == "timing sidecar schema" for p in rl.check_component_compatibility())


def test_08_only_the_selected_variant_is_accepted_other_renders_are_refused(tmp_path):
    p1, p2 = m.write(tmp_path / "a.json", m.a0e_params()), m.write(tmp_path / "b.json", m.a0m_params())
    plain = dr.dry_render(p1, p2, tmp_path / "plain", "r1", channel=m.CHANNEL)         # an A0m render without the timed overlay
    root = tmp_path / "plain" / "r1"
    with pytest.raises(rl.LayoutError) as e:
        rl.build_layout(root, tmp_path / "o", repo_root=REPO, prometheus_mode="deferred")
    assert e.value.code == "variant_not_verified" and plain


def test_09_unmarked_or_tampered_overlay_is_refused(tmp_path):
    root = s.render_a0p(tmp_path)
    marker = tv.marker_path(root)
    saved = marker.read_text(encoding="utf-8")
    marker.write_text(saved.replace(tv.VARIANT_A0P, "a0m_recorder_v3_issue_logged"), encoding="utf-8")
    with pytest.raises(rl.LayoutError):
        rl.build_layout(root, tmp_path / "o", repo_root=REPO, prometheus_mode="deferred")
    marker.write_text(saved, encoding="utf-8")
    bench = tv.overlay_directory(root) / "wired_v3_timed" / "a0m-test-001-v3ilt" / "benchmark.yaml"
    doc = json.loads(bench.read_text(encoding="utf-8"))
    doc["test"]["rounds"][1]["workload"]["arguments"]["issueLogTimed"]["path"] = doc["test"]["rounds"][0]["workload"]["arguments"]["issueLogTimed"]["path"]
    bench.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(rl.LayoutError):
        rl.build_layout(root, tmp_path / "o", repo_root=REPO, prometheus_mode="deferred")


def test_10_module_dependencies_resolve_and_equal_the_repository_sources(rendered):
    L = layout(rendered)
    modules = Path(L["launch_authorizing_directory"]) / "modules"
    present = {p.name for p in modules.glob("*.js")}
    assert set(tv.REQUIRED_MODULES) <= present
    for name, rel in tv.SOURCES.items():
        assert (modules / name).read_bytes() == (REPO / rel).read_bytes()


def test_11_a_path_collision_is_reported(rendered):
    L = copy.deepcopy(layout(rendered))
    L["rounds"][1]["timing_sidecar"]["host"] = L["rounds"][1]["issue_log"]["host"]
    assert any(p["code"] == "path_collision" for p in rl.verify_layout(L))
    L = copy.deepcopy(layout(rendered))
    L["host_evidence"]["clock_post"] = L["host_evidence"]["clock_pre"]
    assert any(p["code"] == "path_collision" for p in rl.verify_layout(L))


def test_12_cross_round_consumption_is_reported(rendered):
    L = copy.deepcopy(layout(rendered))
    first_log = L["rounds"][0]["issue_log"]["host"]
    step = next(x for x in L["post_run_steps"] if x["round"] == 1 and "sourcecount" in x["tool"])
    step["inputs"] = [first_log]
    assert any(p["code"] == "cross_round_input" for p in rl.verify_layout(L))
    L = copy.deepcopy(layout(rendered))
    step = next(x for x in L["post_run_steps"] if x["round"] == 1 and "verify-write-timing" in x["tool"])
    step["output"] = "post_run/write_timing_shared.json"
    assert any(p["code"] == "output_not_bound_to_round" for p in rl.verify_layout(L))


def test_13_every_post_run_tool_input_is_bound_to_its_own_round(rendered):
    L = layout(rendered)
    assert len(L["post_run_steps"]) == 3 * len(L["rounds"])
    for step in L["post_run_steps"]:
        assert all(f"_r{step['round']}_" in path for path in step["inputs"])


@pytest.mark.parametrize("prefix", ["experiments/results", "data/raw", "data/processed", "Integration/system_integration/observability_recovery_2026-11-17"])
def test_14_an_output_directory_inside_protected_evidence_is_refused(rendered, prefix):
    tmp, root = rendered
    with pytest.raises(rl.LayoutError) as e:
        rl.build_layout(root, tmp / prefix / "run1", repo_root=REPO, prometheus_mode="deferred")
    assert e.value.code == "layout_invalid" and "protected_path" in str(e.value)


def test_15_protected_channel_in_a_layout_is_reported(rendered):
    L = copy.deepcopy(layout(rendered))
    L["channel"] = "e1-r002"
    assert any(p["code"] == "channel_refused" for p in rl.verify_layout(L))


def test_16_selection_state_and_variant_are_validated(rendered):
    L = copy.deepcopy(layout(rendered))
    L["selection"]["stage3_shadow"].pop("effect")
    assert any(p["code"] == "deferred_without_effect" for p in rl.verify_layout(L))
    L = copy.deepcopy(layout(rendered))
    L["variant"] = "a0m_recorder_v3_issue_logged"
    assert any(p["code"] == "variant_not_selected" for p in rl.verify_layout(L))


def test_17_sourcecount_reader_and_bridge_compatibility_with_the_selected_files(rendered):
    """The post-run tools named in the layout exist, and the readers they use accept the schemas the selected writers produce."""
    for tool in {step["tool"].split()[0] for step in layout(rendered)["post_run_steps"]}:
        assert (REPO / "scripts" / "e1" / tool).is_file(), tool
    from fabric_perf.controllers import sourcecount_reader as sr
    assert rl.SELECTION["sourcecount_reader"]["evidence_schema"] == sr.EVIDENCE_SCHEMA
    assert rl.SELECTION["ledger_capture"]["evidence_schema"] == "fabric_ledger_evidence/1" and (REPO / rl.SELECTION["ledger_capture"]["file"]).is_file()


def test_18_the_render_is_not_modified_by_building_a_layout(rendered):
    tmp, root = rendered
    before = {p.relative_to(root.parent).as_posix(): p.read_bytes() for p in root.parent.rglob("*") if p.is_file()}
    layout(rendered, "deferred", "run_out_ro")
    after = {p.relative_to(root.parent).as_posix(): p.read_bytes() for p in root.parent.rglob("*") if p.is_file()}
    assert before == after and not (tmp / "run_out_ro").exists()
