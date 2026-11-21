"""A0m (2026-11-21): the versioned dry-render workflow (issue-logged workload + recorder v3), offline. SYNTHETIC; nothing here ran Caliper, Fabric, Docker, WSL or a
network. Node ran only the offline driver tests/e1/js/a0m_generated_run.js, which loads the modules of the RENDERED directory under a fake Caliper."""

from __future__ import annotations

import ast
import hashlib
import json
import posixpath
import shutil
import socket
import subprocess
import sys
from pathlib import Path

import pytest

from tests import a0e_support as e
from tests import a0m_support as m

dr = m.dr
REPO = m.REPO
cs, pi, pc = dr.cs, dr.pi, dr.pc
T0 = 1795219200000                                  # 2026-11-21T00:00:00Z, the driver's scripted clock origin


@pytest.fixture(scope="module")
def good(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("a0m_good")
    root, manifest = m.render(tmp)
    return root, manifest


def tree(directory: Path) -> dict:
    return {p.relative_to(directory).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(Path(directory).rglob("*")) if p.is_file()}


def sha(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def bench(root, manifest) -> dict:
    return m.read_json(m.wired(root, manifest) / "benchmark.yaml")


# ============================================================================================================================ renderer
def test_01_dry_render_is_explicit_offline_and_not_executed(good):
    root, manifest = good
    assert manifest["status"] == "RENDERED_OFFLINE_NOT_EXECUTED" and manifest["phase"] == "dry-render"
    for flag in ("caliper_invoked", "fabric_contacted", "docker_or_wsl_started", "prometheus_contacted", "model_inference_enabled", "actuator_invoked"):
        assert manifest[flag] is False
    assert manifest["transactions_submitted"] == 0 and manifest["execution"]["phase_implemented"] is False
    assert not (root / dr.INCOMPLETE_MARKER).exists()
    assert manifest["owner_approval_of_channel"] == "NOT_GRANTED" and manifest["channel_exists"] == "UNVERIFIED"
    assert all(g["state"] in {"NOT_GRANTED", "UNVERIFIED"} for g in manifest["live_gates"])


def test_02_a_missing_channel_is_refused_and_nothing_is_created(tmp_path):
    for value in (None, "", "   "):
        with pytest.raises(cs.ChannelSelectionError):
            m.render(tmp_path, channel=value, render_id=f"x{abs(hash(value))}")
    assert not (tmp_path / "out").exists()
    with pytest.raises(SystemExit):                                  # the CLI requires --channel
        dr.main(["dry-render", "--params", "a", "--a0m-params", "b", "--out-root", str(tmp_path / "o"), "--render-id", "x"])


@pytest.mark.parametrize("name", ["e1-r002", "e1-cfg001", "E1-R002", "e1-b001", "e1-ql01-b40-l50", "e1-cp01-b40-l50", "e1-rp01-b40-r1"])
def test_03_protected_and_historical_channels_are_refused_before_anything_is_created(tmp_path, name):
    with pytest.raises(cs.ChannelSelectionError):
        m.render(tmp_path, channel=name)
    with pytest.raises((cs.ChannelSelectionError, pc.PreliveParameterError)):
        m.render(tmp_path, a0e=m.a0e_params(channel=name), channel=name, render_id="r2")
    assert not (tmp_path / "out").exists()


def test_03b_a_channel_that_differs_from_the_parameter_file_is_refused(tmp_path):
    with pytest.raises(cs.ChannelSelectionError) as info:
        m.render(tmp_path, channel="e1-other.a0m")
    assert info.value.reason == cs.MISMATCH and not (tmp_path / "out").exists()


@pytest.mark.parametrize("breaker", [
    lambda c: c.pop("duration_s"), lambda c: c.update(tx_count=10), lambda c: c.update(rate_tps=-1), lambda c: c.update(workload="bogus"),
    lambda c: c.pop("max_message_count")])
def test_04_an_invalid_experimental_cell_is_refused(tmp_path, breaker):
    doc = m.a0e_params()
    breaker(doc["cell"])
    with pytest.raises(pc.PreliveParameterError):
        m.render(tmp_path, a0e=doc)
    doc = m.a0e_params()
    doc.pop("cell")
    with pytest.raises(pc.PreliveParameterError):
        m.render(tmp_path, a0e=doc, render_id="r2")
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("key", ["selected_workload", "selected_recorder"])
def test_05_06_a_missing_or_unsupported_selection_is_refused(tmp_path, key):
    for value, code in ((e.DROP, "selection_missing"), ("", "selection_missing"), ("e1_create_asset/1", "selection_unsupported"),
                        ("e1_per_tx_recorder_v2", "selection_unsupported"), ("e1_per_tx_recorder", "selection_unsupported")):
        with pytest.raises(dr.A0mParameterError) as info:
            m.render(tmp_path, a0m=m.a0m_params(**{key: value}), render_id="x")
        assert code in {i.code for i in info.value.issues}, (value, info.value.issues)
    assert not (tmp_path / "out").exists()


def test_06b_every_field_is_required_and_unknown_keys_are_refused(tmp_path):
    for section, field in (("issue_log", "tick_interval_ms"), ("issue_log", "clock_domain"), ("issue_log", "fsync_policy"), ("recorder_health", "source_id"),
                           ("recorder_health", "evidence_origin"), ("recorder_health", "heartbeat_interval_ms")):
        doc = m.a0m_params()
        doc[section].pop(field)
        with pytest.raises(dr.A0mParameterError) as info:
            dr.validate_a0m_parameters(doc)
        assert any(i.code == "missing" and i.field == f"{section}.{field}" for i in info.value.issues)
    for doc in (m.a0m_params(extra=1), m.a0m_params(issue_log={**m.GOOD_A0M["issue_log"], "default": 1}), m.a0m_params(worker_count=e.DROP)):
        with pytest.raises(dr.A0mParameterError):
            dr.validate_a0m_parameters(doc)
    for field, value in (("tick_interval_ms", 5), ("tick_interval_ms", True), ("fsync_policy", "never"), ("clock_domain", " x"), ("clock_domain", "x" * 81)):
        doc = m.a0m_params()
        doc["issue_log"][field] = value
        with pytest.raises(dr.A0mParameterError):
            dr.validate_a0m_parameters(doc)
    for field, value in (("heartbeat_interval_ms", 1500), ("heartbeat_interval_ms", 500), ("evidence_origin", "live"), ("checkpoint_every_heartbeats", 0)):
        doc = m.a0m_params()
        doc["recorder_health"][field] = value
        with pytest.raises(dr.A0mParameterError):
            dr.validate_a0m_parameters(doc)


@pytest.mark.parametrize("workers", [2, 3, 0, -1, True, "1", 1.0, None])
def test_07_an_unsupported_worker_count_is_refused_before_final_artifacts_and_never_replaced_by_one(tmp_path, workers):
    with pytest.raises(dr.A0mParameterError) as info:
        m.render(tmp_path, a0m=m.a0m_params(worker_count=workers))
    assert {i.field for i in info.value.issues} == {"worker_count"}
    assert not (tmp_path / "out").exists()


def test_07b_the_generated_benchmark_declares_exactly_one_worker(good):
    root, manifest = good
    assert bench(root, manifest)["test"]["workers"] == {"number": 1}
    assert manifest["worker"]["count"] == 1 and manifest["worker"]["worker_id"] == "w0"
    doc = bench(root, manifest)
    doc["test"]["workers"]["number"] = 2
    with pytest.raises(dr.A0mError, match="exactly 1 is supported"):
        dr.select_v3(doc, dr.validate_a0m_parameters(m.a0m_params()), m.RUN_ID)


def test_08_output_is_isolated_new_and_never_reused(tmp_path):
    sources = {name: sha(REPO / rel) for name, (rel, _r) in dr.MODULE_SOURCES.items()}
    root, manifest = m.render(tmp_path)
    assert [p.name for p in (tmp_path / "out").iterdir()] == ["r1"]
    resolved = root.resolve()
    assert all(str(p.resolve()).startswith(str(resolved)) for p in root.rglob("*"))
    assert {name: sha(REPO / rel) for name, (rel, _r) in dr.MODULE_SOURCES.items()} == sources          # the repository sources were only read
    before = tree(root)
    with pytest.raises(dr.A0mError, match="refusing to reuse"):
        m.render(tmp_path)
    assert tree(root) == before
    root2, _ = m.render(tmp_path, render_id="r2")
    assert root2 != root and not set(tree(root2)) - set(tree(root)) - {"a0m_preflight_results"}


def test_09_a_missing_source_is_refused_before_anything_is_created(tmp_path, monkeypatch):
    for name in (dr.ISSUE_LOG_MODULE, dr.RECORDER_V3, dr.WORKLOAD_V3):
        monkeypatch.setitem(dr.MODULE_SOURCES, name, ("does/not/exist.js", "x"))
        with pytest.raises(dr.A0mError, match="is missing; nothing was created"):
            m.render(tmp_path, render_id=f"x-{name}")
        monkeypatch.undo()
    assert not (tmp_path / "out").exists()


def test_09b_a_module_deleted_from_the_rendered_directory_fails_preflight_and_resolution(good, tmp_path):
    root, manifest = good
    for name in (dr.ISSUE_LOG_MODULE, dr.RECORDER_V2, dr.RECORDER_V1, dr.WORKLOAD_LEGACY, dr.CORE_MODULE):
        copy = m.clone(root, tmp_path / f"c-{name}")
        (m.wired(copy, manifest) / "modules" / name).unlink()
        with pytest.raises(dr.A0mError):
            dr.verify_v3(copy)
        result = dr.preflight(copy, write=False)
        assert result["offline_preflight"] == "FAIL"
        assert {c["id"] for c in result["checks"] if c["status"] == "FAIL"} >= {"A02_file_inventory", "A04_selections_and_modules"}


def test_09c_a_require_that_leaves_the_directory_or_is_unknown_is_refused(tmp_path):
    mod = tmp_path / "modules"
    mod.mkdir()
    for name, body in (("e1_per_tx_recorder_v3.js", "require('./../x.js');"), ("e1_create_asset_issue_logged.js", "require('left-pad');")):
        (mod / name).write_text(body, encoding="utf-8")
        with pytest.raises(dr.A0mError):
            dr.resolve_requires(mod, (name,))
    (mod / "e1_create_asset_issue_logged.js").write_text("require('./nope.js'); require('fs');", encoding="utf-8")
    with pytest.raises(dr.A0mError, match="not in the modules directory"):
        dr.resolve_requires(mod, ("e1_create_asset_issue_logged.js",))


def test_10_a_wrong_module_reference_in_the_generated_configuration_is_detected(good, tmp_path):
    root, manifest = good
    for edit, text in (
            (lambda b: b["monitors"]["transaction"][0].update(module=b["monitors"]["transaction"][0]["module"].replace("_v3", "_v2")), "does not select recorder v3"),
            (lambda b: b["monitors"]["transaction"][0].update(module=b["monitors"]["transaction"][0]["module"].replace("_v3", "")), "does not select recorder v3"),
            (lambda b: b["test"]["rounds"][1]["workload"].update(module=b["test"]["rounds"][1]["workload"]["module"].replace("_issue_logged", "")), "does not select"),
            (lambda b: b["test"]["rounds"][0]["workload"]["arguments"].pop("issueLog"), "issueLog"),
            (lambda b: b["test"]["rounds"][1]["workload"]["arguments"]["issueLog"].update(workerId="w1"), "issueLog"),
            (lambda b: b["test"]["rounds"][1]["workload"]["arguments"]["issueLog"].update(path=b["test"]["rounds"][0]["workload"]["arguments"]["issueLog"]["path"]), "issueLog"),
            (lambda b: b["monitors"]["transaction"][0]["options"]["health"].update(heartbeatIntervalMs=2000), "health block"),
            (lambda b: b["monitors"]["transaction"][0]["options"].pop("health"), "health block")):
        copy = m.clone(root, tmp_path / f"c{abs(hash(text)) % 10**6}{id(edit) % 10**5}")
        path = m.wired(copy, manifest) / "benchmark.yaml"
        doc = m.read_json(path)
        edit(doc)
        path.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
        with pytest.raises(dr.A0mError, match=text):
            dr.verify_v3(copy)
        assert dr.preflight(copy, write=False)["offline_preflight"] == "FAIL"


def test_10b_an_issue_log_path_inside_the_recorder_directory_is_refused(good, tmp_path):
    root, manifest = good
    copy = m.clone(root, tmp_path / "c")
    path = m.wired(copy, manifest) / "benchmark.yaml"
    doc = m.read_json(path)
    log = doc["test"]["rounds"][1]["workload"]["arguments"]["issueLog"]
    log["path"] = log["path"].replace("/issuelog/", "/recorder/")
    path.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(dr.A0mError):
        dr.verify_v3(copy)


def test_10c_a_recorder_output_directory_that_is_the_issue_log_directory_is_refused(good, tmp_path):
    root, manifest = good
    copy = m.clone(root, tmp_path / "c")
    path = m.wired(copy, manifest) / "benchmark.yaml"
    doc = m.read_json(path)
    options = doc["monitors"]["transaction"][0]["options"]
    options["outputDir"] = posixpath.dirname(options["outputDir"]) + "/issuelog"       # the recorder would write beside the issue logs
    path.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(dr.A0mError, match="collides with the recorder output directory"):
        dr.verify_v3(copy)


def test_10d_a_shipped_module_that_differs_from_its_repository_source_is_refused_by_the_verifier_itself(good, tmp_path):
    root, manifest = good
    for name in (dr.ISSUE_LOG_MODULE, dr.RECORDER_V3, dr.WORKLOAD_V3):
        copy = m.clone(root, tmp_path / f"c-{name}")
        target = m.wired(copy, manifest) / "modules" / name
        target.write_bytes(target.read_bytes() + b"\n// edited after rendering\n")
        with pytest.raises(dr.A0mError, match=f"modules/{name} differs from"):
            dr.verify_v3(copy)


def test_10e_a_v1_recorder_that_is_not_the_pinned_file_is_refused_even_if_repository_and_run_agree(good, tmp_path):
    root, manifest = good
    fake = tmp_path / "repo"
    for name, (rel, _role) in dr.MODULE_SOURCES.items():
        (fake / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO / rel, fake / rel)
    v1_rel = dr.MODULE_SOURCES[dr.RECORDER_V1][0]
    copy = m.clone(root, tmp_path / "c")
    for target in (fake / v1_rel, m.wired(copy, manifest) / "modules" / dr.RECORDER_V1):
        target.write_bytes(target.read_bytes() + b"\n// not the pinned recorder\n")
    with pytest.raises(dr.A0mError, match="not the pinned v1 recorder"):
        dr.verify_v3(copy, repo_root=fake)
    ok = m.clone(root, tmp_path / "ok")
    assert dr.verify_v3(ok, repo_root=REPO)["workers"] == 1                                # the unmodified pair still verifies


def test_10f_the_preflight_fails_when_the_manifest_claims_any_execution(good, tmp_path):
    root, manifest = good
    for edit in (lambda d: d.update(caliper_invoked=True), lambda d: d.update(fabric_contacted=True), lambda d: d.update(docker_or_wsl_started=True),
                 lambda d: d.update(prometheus_contacted=True), lambda d: d.update(model_inference_enabled=True), lambda d: d.update(actuator_invoked=True),
                 lambda d: d.update(transactions_submitted=1), lambda d: d["execution"].update(phase_implemented=True)):
        copy = m.clone(root, tmp_path / f"c{id(edit) % 10**6}")
        path = copy / dr.MANIFEST_NAME
        doc = m.read_json(path)
        edit(doc)
        path.write_text(json.dumps(doc), encoding="utf-8")
        result = dr.preflight(copy, write=False)
        assert result["offline_preflight"] == "FAIL" and "A06_nothing_executed" in {c["id"] for c in result["checks"] if c["status"] == "FAIL"}


def test_11_the_configuration_manifest_lists_every_file_and_any_change_is_detected(good, tmp_path):
    root, manifest = good
    listed = manifest["files"]
    on_disk = {k: v for k, v in tree(root).items() if k != dr.MANIFEST_NAME and not k.startswith("a0m_preflight_results/")}
    assert listed == on_disk and len(listed) >= 25
    for name, entry in manifest["modules"].items():
        assert sha(m.wired(root, manifest) / "modules" / name) == entry["sha256"] == sha(REPO / entry["source"])
    victims = ["wired_v3/{r}/benchmark.yaml", "wired_v3/{r}/modules/e1_issue_log.js", "a0m_params.validated.json", "a0e_base/prepared/launch_manifest.json"]
    for index, victim in enumerate(victims):
        copy = m.clone(root, tmp_path / f"t{index}")
        target = copy / victim.format(r=manifest["run_id"])
        target.write_bytes(target.read_bytes() + b"\n")
        failed = {c["id"] for c in dr.preflight(copy, write=False)["checks"] if c["status"] == "FAIL"}
        assert "A02_file_inventory" in failed, victim
    extra = m.clone(root, tmp_path / "extra")
    (extra / "stray.txt").write_text("x")
    assert dr.preflight(extra, write=False)["offline_preflight"] == "FAIL"
    marker = m.clone(root, tmp_path / "marker")
    (marker / dr.INCOMPLETE_MARKER).write_text("{}")
    assert dr.preflight(marker, write=False)["checks"][0]["status"] == "FAIL"


def test_12_no_execution_side_effects_no_process_no_network(tmp_path, monkeypatch):
    def refuse(*args, **kwargs):
        raise AssertionError("a process or network use is forbidden")
    for obj, attr in ((subprocess, "Popen"), (subprocess, "run"), (socket, "socket"), (socket, "create_connection"), (socket, "getaddrinfo")):
        monkeypatch.setattr(obj, attr, refuse)
    import os
    monkeypatch.setattr(os, "system", refuse)
    root, manifest = m.render(tmp_path)
    assert dr.preflight(root)["offline_preflight"] == "PASS"
    assert dr.main(["execute"]) == dr.EXIT_EXECUTE_REFUSED == 5
    tree_before = tree(root)
    assert dr.main(["execute"]) == 5 and tree(root) == tree_before            # refusing writes nothing


def test_12b_the_tool_imports_no_process_network_model_or_actuator_api():
    forbidden = {"subprocess", "socket", "http", "urllib", "requests", "asyncio", "multiprocessing", "ssl", "joblib", "sklearn", "pickle", "docker", "paramiko"}
    tree_ = ast.parse((REPO / "scripts" / "e1" / "e1_a0m_dry_render.py").read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree_):
        if isinstance(node, ast.Import):
            imported |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert not imported & forbidden, imported & forbidden
    assert not {"e1_config_actuator", "e1_channel_run", "e1_run_orchestrator", "run_contemporaneous_capture"} & imported
    text = (REPO / "scripts" / "e1" / "e1_a0m_dry_render.py").read_text(encoding="utf-8")
    for needle in ("os.system", "Popen", "os.popen", "exec(", "eval("):
        assert needle not in text


def test_12c_no_model_or_forecast_library_is_loaded_by_a_dry_render(tmp_path):
    code = ("import sys; sys.path[:0]=[r'%s', r'%s']\n"
            "from tests import a0m_support as m\nfrom pathlib import Path\nroot, man = m.render(Path(sys.argv[1]))\n"
            "bad = [n for n in ('joblib','sklearn','torch','tensorflow','lightgbm','xgboost') if n in sys.modules]\nprint(bad)\n") % (REPO, REPO / "src")
    done = subprocess.run([sys.executable, "-I", "-c", code, str(tmp_path)], capture_output=True, text=True, cwd=REPO, timeout=300, check=False)
    assert done.returncode == 0, done.stderr[-600:]
    assert done.stdout.strip().splitlines()[-1] == "[]"


# ============================================================================================================================ workload and recorder
def test_13_14_the_issue_logged_workload_and_recorder_v3_are_selected_in_the_generated_configuration(good):
    root, manifest = good
    doc = bench(root, manifest)
    modules = [posixpath.basename(r["workload"]["module"]) for r in doc["test"]["rounds"]]
    assert modules == ["e1_create_asset_issue_logged.js"] * 2
    monitors = doc["monitors"]["transaction"]
    assert len(monitors) == 1 and posixpath.basename(monitors[0]["module"]) == "e1_per_tx_recorder_v3.js"
    assert monitors[0]["options"]["health"] == {"sourceId": "a0m-test-recorder", "collectorId": "a0m-test-collector", "evidenceOrigin": "synthetic_fixture",
                                                "heartbeatIntervalMs": 1000, "checkpointEveryHeartbeats": 60}
    assert "sourceCount" not in monitors[0]["options"]["health"]                   # a JSON file cannot carry a provider function
    assert manifest["selections"]["workload_file"] == "e1_create_asset_issue_logged.js" and manifest["selections"]["recorder_file"] == "e1_per_tx_recorder_v3.js"
    assert "NOT_CONFIGURED" in manifest["source_count"]["recorder_health_sourceCount"]


def test_15_16_recorder_v2_and_v1_dependencies_resolve_from_the_rendered_directory_itself(good):
    root, manifest = good
    graph = dr.resolve_requires(m.wired(root, manifest) / "modules")["graph"]
    assert graph["e1_per_tx_recorder_v3.js"] == ["e1_per_tx_recorder_v2.js"]
    assert graph["e1_per_tx_recorder_v2.js"] == ["e1_per_tx_recorder.js"]
    assert graph["e1_create_asset_issue_logged.js"] == ["e1_create_asset.js", "e1_issue_log.js", "e1_workload_core.js"]
    assert sha(m.wired(root, manifest) / "modules" / "e1_per_tx_recorder.js") == dr.wire.PINNED_V1_SHA256


def test_15b_actual_node_module_resolution_uses_only_the_rendered_modules_directory(good, tmp_path):
    root, manifest = good
    result = m.run_driver(m.wired(root, manifest), tmp_path / "js")
    assert result["loaded"]["outside"] == []
    assert result["loaded"]["inModules"] == sorted(dr.MODULE_SOURCES)


def test_17_issue_log_output_is_configured_per_round_and_the_driver_writes_it_where_configured(good, tmp_path):
    root, manifest = good
    paths = [r["workload"]["arguments"]["issueLog"]["path"] for r in bench(root, manifest)["test"]["rounds"]]
    assert len(set(paths)) == 2 and all(posixpath.dirname(p).endswith("/issuelog") for p in paths)
    assert [r["issue_log_path"] for r in manifest["rounds"]] == paths
    result = m.run_driver(m.wired(root, manifest), tmp_path / "js")
    assert result["issueLogFiles"] == [posixpath.basename(paths[-1])]
    assert Path(result["issueLog"]).is_file() and Path(result["issueLog"]).parent != Path(result["recorderDir"])


def test_17b_the_recorder_file_names_in_the_manifest_are_the_names_the_recorder_really_writes(driven_files):
    manifest, result = driven_files
    r = manifest["rounds"][1]
    written = set(result["files"])
    for key in ("recorder_health_spool", "recorder_live_submits", "recorder_live_csv"):
        assert posixpath.basename(r[key]) in written, (key, sorted(written))
    assert posixpath.basename(r["issue_log_path"]) in result["issueLogFiles"]
    assert posixpath.basename(r["issue_log_path"]).startswith(r["recorder_base"] + ".")


def _issue_rows(path):
    return [json.loads(line) for line in Path(path).read_text(encoding="ascii").splitlines()]


def _spool_rows(recorder_dir):
    (spool,) = list(Path(recorder_dir).glob("*.health.jsonl"))
    return [json.loads(line) for line in spool.read_text(encoding="utf-8").splitlines()]


@pytest.fixture(scope="module")
def driven_files(good, tmp_path_factory):
    root, manifest = good
    return manifest, m.run_driver(m.wired(root, manifest), tmp_path_factory.mktemp("a0m_files"))


@pytest.fixture(scope="module")
def driven(good, tmp_path_factory):
    root, manifest = good
    out = tmp_path_factory.mktemp("a0m_driven")
    return root, manifest, m.run_driver(m.wired(root, manifest), out)


def test_18_19_run_and_worker_identity_agree_between_the_generator_log_and_the_recorder_spool(driven):
    _root, manifest, result = driven
    issue, spool = _issue_rows(result["issueLog"]), _spool_rows(result["recorderDir"])
    assert {r["run_id"] for r in issue} == {r["run_id"] for r in spool if "run_id" in r} == {manifest["run_id"]}
    start_i, start_s = issue[0], spool[0]
    assert start_i["kind"] == start_s["kind"] == "start"
    assert start_i["worker_id"] == "w0" and start_i["worker_index"] == 0 and start_i["total_workers"] == 1 and start_i["worker_roster"] == ["w0"]
    assert start_s["worker"] == 0
    assert start_i["round_index"] == start_s["round"] == manifest["rounds"][1]["round"]
    assert start_s["label"] == manifest["rounds"][1]["label"]
    assert start_i["clock_domain"] == manifest["issue_log"]["clock_domain"]
    assert start_s["source_id"] == manifest["recorder_health"]["source_id"] and start_s["collector_id"] == manifest["recorder_health"]["collector_id"]
    assert posixpath.basename(manifest["rounds"][1]["issue_log_path"]).startswith(manifest["rounds"][1]["recorder_base"])


def test_20_transaction_identifiers_reconcile_by_asset_key_and_by_count_never_by_timestamp(driven):
    _root, manifest, result = driven
    from fabric_perf.controllers import issue_log_reader as ilr
    pol = ilr.IssueLogPolicy(run_id=manifest["run_id"], worker_id="w0", clock_domain=manifest["issue_log"]["clock_domain"], max_tick_gap_ms=5000)
    tail = ilr.IssueLogTail(Path(result["issueLog"]), pol)
    assert tail.poll(ilr.ms_to_utc(T0 + 9000)).status == "progress"
    keys = result["assetKeys"]
    assert keys == [f"{manifest['run_id']}_1_{i:07d}" for i in range(5)]                 # core.keyFor(runId, round, seq)
    rec = ilr.reconcile_counts(tail, recorder_submit_units=5, ledger_committed_keys=keys[:3] + ["elsewhere"])
    assert rec["generator"]["submission_attempts"] == 5 and rec["generator"]["unresolved_attempts"] == 0
    assert rec["recorder_submit_callbacks"]["minus_generator_attempts"] == 0 and "counts only" in rec["recorder_submit_callbacks"]["join"]
    assert rec["ledger_committed"]["matched_by_asset_key"] == 3 and rec["ledger_committed"]["committed_keys_not_issued_here"] == 1
    done = ilr.reconcile_counts(tail, recorder_completed_ids=["tx0"])
    assert done["recorder_completed"]["matched_by_client_tx_id"] == 0                 # the fake send exposed no id: no join is invented


def test_21_issuance_time_and_recorder_callback_time_are_different_quantities(driven):
    _root, manifest, result = driven
    attempts = [r for r in _issue_rows(result["issueLog"]) if r["kind"] == "attempt"]
    submits = [line.split(",") for line in (next(Path(result["recorderDir"]).glob("*.live.submits.csv"))).read_text().splitlines()[1:]]
    assert len(attempts) == len(submits) == 5
    for attempt, submit in zip(attempts, submits):
        assert int(submit[1]) - attempt["t_ms"] == 7                                  # scripted: the observer saw the event 7 ms after the generator's attempt
    assert _issue_rows(result["issueLog"])[0]["event_time_semantics"] == "generator_issue_time"
    assert _spool_rows(result["recorderDir"])[0]["timestamp_provenance"] != _issue_rows(result["issueLog"])[0]["event_time_semantics"]


def test_21b_the_two_evidence_streams_never_share_a_directory_or_a_suffix(good):
    _root, manifest = good
    for r in manifest["rounds"]:
        assert posixpath.dirname(r["issue_log_path"]) != posixpath.dirname(r["recorder_health_spool"])
        assert r["issue_log_path"].endswith(".issuelog.jsonl") and not r["recorder_health_spool"].endswith(".issuelog.jsonl")
        assert len({r["issue_log_path"], r["recorder_health_spool"], r["recorder_live_submits"], r["recorder_live_csv"]}) == 4


def test_22_issue_logging_stays_opt_in_and_the_legacy_workload_is_unchanged(good, tmp_path):
    root, manifest = good
    copy = m.clone(root, tmp_path / "c")
    path = m.wired(copy, manifest) / "benchmark.yaml"
    doc = m.read_json(path)
    for rnd in doc["test"]["rounds"]:
        rnd["workload"]["arguments"].pop("issueLog")
    path.write_text(json.dumps(doc), encoding="utf-8")
    result = m.run_driver(m.wired(copy, manifest), tmp_path / "js")
    assert result["issueLog"] is None and result["issueLogFiles"] == []                 # no issueLog argument: no log is written
    assert len(result["assetKeys"]) == 5 and result["assetKeys"][0] == f"{manifest['run_id']}_1_0000000"
    legacy = REPO / "fabric-testbed" / "caliper" / "workloads" / "e1_create_asset.js"
    a0l = m.read_json(REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21_a0l.json")
    assert {e_["path"]: e_["sha256"] for e_ in a0l["entries"]}["fabric-testbed/caliper/workloads/e1_create_asset.js"] == sha(legacy)


def test_22b_the_historical_a0e_preparation_inside_the_render_is_still_recorder_v2(good):
    root, manifest = good
    base = root / manifest["a0e_base_dir"]
    selected = dr.wire.verify_wired(base / "wired" / manifest["run_id"], channel=manifest["channel"])
    assert posixpath.basename(selected["selected_module"]) == "e1_per_tx_recorder_v2.js"
    bench_v2 = m.read_json(base / "wired" / manifest["run_id"] / "benchmark.yaml")
    assert all(posixpath.basename(r["workload"]["module"]) == "e1_create_asset.js" for r in bench_v2["test"]["rounds"])
    assert "issueLog" not in json.dumps(bench_v2) and "health" not in bench_v2["monitors"]["transaction"][0]["options"]


def test_23_reader_lag_is_reported_as_a_failed_attestation_not_as_missing_or_zero_activity():
    """The A0l limitation is kept (see test_a0m_followup): nothing in the dry render sets or measures a poll cadence."""
    from fabric_perf.controllers import issue_log_reader as ilr
    assert ilr.IssueLogPolicy(run_id="r", worker_id="w0", clock_domain="d").max_tick_gap_ms == 5000      # a placeholder default, never chosen by the renderer
    assert not [k for k in dr.TOP_KEYS + dr.ISSUE_LOG_KEYS + dr.HEALTH_KEYS if "poll" in k or "cadence" in k]        # no reader cadence is invented


# ============================================================================================================================ integration
def test_24_dry_render_to_offline_preflight_passes_writes_only_its_results_and_never_claims_live_readiness(tmp_path):
    root, manifest = m.render(tmp_path)
    before = tree(root)
    result = dr.preflight(root)
    assert result["offline_preflight"] == "PASS" and result["live_ready"] is False and result["counts"]["FAIL"] == 0
    assert [c["id"] for c in result["checks"]] == ["A01_manifest", "A02_file_inventory", "A03_a0e_base_preflight", "A04_selections_and_modules",
                                                   "A05_identities_and_paths", "A06_nothing_executed", "A07_candidate_manifest"]
    after = tree(root)
    assert {k for k in after if k not in before} == set(result["written_to"])
    assert dr.preflight(root, write=False)["offline_preflight"] == "PASS"          # the results directory is not part of the inventory
    assert dr.main(["preflight", "--run-dir", str(root), "--no-write"]) == 0
    assert dr.main(["preflight", "--run-dir", str(tmp_path / "nope"), "--no-write"]) == 1


def test_24b_the_a0e_base_preflight_still_passes_on_its_own_and_reports_the_open_gates(good):
    root, manifest = good
    base = dr.pv.preflight(root / manifest["a0e_base_dir"], write=False)
    assert base["offline_preflight"] == "PASS" and base["live_ready"] is False
    states = {g["gate"]: g["state"] for g in base["live_gates"]}
    assert states["authorization_to_execute_any_live_step"] == "NOT_GRANTED" and states["recorder_v2_inside_a_real_caliper_worker"] == "UNVERIFIED"


def test_25_the_a0b_channel_protections_hold_on_the_rendered_files(good, tmp_path):
    root, manifest = good
    copy = m.clone(root, tmp_path / "c")
    net = m.wired(copy, manifest) / "networkconfig.yaml"
    doc = m.read_json(net)
    doc["channels"][0]["channelName"] = "e1-r002"
    net.write_text(json.dumps(doc), encoding="utf-8")
    with pytest.raises(cs.ChannelSelectionError):
        dr.verify_v3(copy)
    copy2 = m.clone(root, tmp_path / "c2")
    bench_path = m.wired(copy2, manifest) / "benchmark.yaml"
    bench_path.write_text(bench_path.read_text(encoding="utf-8").replace("e1-a0m", "e1-cfg001", 1) if "e1-a0m" in bench_path.read_text(encoding="utf-8")
                          else bench_path.read_text(encoding="utf-8") + "\n e1-cfg001", encoding="utf-8")
    with pytest.raises(cs.ChannelSelectionError):
        dr.verify_v3(copy2)
    assert dr.preflight(copy2, write=False)["offline_preflight"] == "FAIL"
    assert manifest["channel_selection"]["role"] == "experiment" and bench(root, manifest)["test"]["workers"]["number"] == 1


def test_26_the_a0e_authorization_gates_are_untouched(good):
    root, manifest = good
    assert dr.pv.main(["execute"]) == 5 and dr.main(["execute"]) == 5
    assert "execute" not in {name for name in dir(dr) if name.startswith("run_") or name == "execute"}
    states = {g["gate"]: g["state"] for g in manifest["live_gates"]}
    assert states["authorization_to_execute_any_live_step"] == "NOT_GRANTED"
    assert states["recorder_v3_inside_a_real_caliper_worker"] == states["issue_logged_workload_inside_a_real_caliper_worker"] == "UNVERIFIED"
    assert dr.preflight(root, write=False)["live_ready"] is False


def test_27_the_generated_issue_log_is_consumable_by_the_a0l_reader_bridge_and_a0b_channel_rule(driven):
    _root, manifest, result = driven
    from fabric_perf.controllers import issue_log_coverage as gc
    from fabric_perf.controllers import issue_log_reader as ilr
    pol = ilr.IssueLogPolicy(run_id=manifest["run_id"], worker_id="w0", clock_domain=manifest["issue_log"]["clock_domain"],
                             expected_roster=("w0",), max_tick_gap_ms=5000)
    tail = ilr.IssueLogTail(Path(result["issueLog"]), pol)
    tail.poll(ilr.ms_to_utc(T0 + 9000))
    assert tail.counts()["attempted"] == 5 and tail.counts()["unresolved"] == 0 and tail.error_read_ms is None
    bridge = gc.IssueLogJournalBridge(tail, Path(result["issueLog"]).with_name("journal.jsonl"), source_id=manifest["recorder_health"]["source_id"],
                                      origin=gc.ORIGIN_SYNTHETIC, channel_id=manifest["channel"])
    assert bridge is not None
    with pytest.raises(Exception):                                                    # a protected channel is refused by the bridge (A0b)
        gc.IssueLogJournalBridge(tail, Path(result["issueLog"]).with_name("journal2.jsonl"), source_id="s", origin=gc.ORIGIN_SYNTHETIC, channel_id="e1-r002")
    wrong = ilr.IssueLogPolicy(run_id=manifest["run_id"], worker_id="w0", clock_domain="another-domain", max_tick_gap_ms=5000)
    refused = ilr.IssueLogTail(Path(result["issueLog"]), wrong).poll(ilr.ms_to_utc(T0 + 9000))          # the configured clock domain is what the reader insists on
    assert refused.status != "progress" and refused.error_code is not None


def test_28_no_real_inference_and_no_actuator_in_the_rendered_directory(good):
    root, manifest = good
    from fabric_perf.controllers import stage3_predictive as ris
    assert ris.REAL_INFERENCE_AUTHORIZED is False
    run = m.wired(root, manifest)
    text = " ".join((run / n).read_text(encoding="utf-8") for n in ("benchmark.yaml", "networkconfig.yaml", "ccp-org1.json"))     # the files Caliper would read
    for needle in ("joblib", ".pkl", "e1_config_actuator", "UpdateConfig", "channel update", "peer channel", "hfbtp", "model"):
        assert needle not in text, needle
    assert not [p.name for p in (run / "modules").iterdir() if "actuator" in p.name or "model" in p.name]
    assert manifest["model_inference_enabled"] is False and manifest["actuator_invoked"] is False
