"""A0s workstream B: the bounded, gated execution entry point. Plans are built from a real offline A0p render; execution runs ONLY through a mock runner.
No process, Caliper, Fabric, Docker, WSL or network is used, and the real execute path stays refused."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from tests import a0s_support as s
from tests.a0s_support import CELL, CHANNEL, REPO, FakeClock, MockRunner, ProcessRunner, approved_auth, build_plan, decoded_block, deps_for, ok

from fabric_perf.controllers import bounded_execution as be      # noqa: E402
from fabric_perf.controllers import config_inspection as ci      # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi      # noqa: E402


@pytest.fixture()
def built(tmp_path):
    plan, req, root = build_plan(tmp_path)
    return tmp_path, plan, req, root


def run(built, *, rules=None, raise_on=None, finish_timed_out=False, initial=None, final=None, auth=None, runner_cls=MockRunner, allow_synthetic=True):
    tmp, plan, req, root = built
    clock = FakeClock()
    runner = runner_cls(clock, rules, raise_on=raise_on, finish_timed_out=finish_timed_out)
    deps = deps_for(runner, clock, initial=initial, final=final)
    return runner, be.execute_bounded(plan, auth or approved_auth(plan, tmp), deps, repo_root=REPO, allow_synthetic=allow_synthetic)


def caliper(argv):
    return argv[:1] == ["docker"] and "launch" in argv


def status_of(plan):
    return json.loads((Path(plan["layout"]["output_dir"]) / "run_status.json").read_text(encoding="utf-8"))


# ------------------------------------------------------------------------------------------------ plan and request
def test_01_plan_is_inspectable_and_grounded_in_the_render(built):
    tmp, plan, req, root = built
    assert plan["status"] == "PLANNED_NOT_AUTHORIZED_NOT_EXECUTED" and plan["real_launch_enabled"] is False and len(plan["plan_sha256"]) == 64
    steps = {st["step"]: st for st in plan["steps"]}
    assert list(steps) == ["X01_verify_artifacts", "X02_config_initial", "X03_clock_pre", "X04_run_dir_absent", "X05_stage_rendered_run", "X06_ledger_follower", "X08_caliper",
                           "X09_collectors_stop", "X11_clock_post", "X12_config_final", "X13_integrity"]
    plan_json = json.loads((Path(root) / "a0e_base" / "prepared" / "rendered" / "plan.json").read_text(encoding="utf-8"))
    assert steps["X08_caliper"]["argv"] == plan_json["caliper"]["argv"] and steps["X08_caliper"]["class"] == "transaction_producing"
    assert steps["X06_ledger_follower"]["argv"][:3] == ["python3", "scripts/e1/e1_ledger_follow.py", "--channel"] and steps["X06_ledger_follower"]["argv"][-1] == "--execute"
    assert "--max-polls" in steps["X06_ledger_follower"]["argv"] and steps["X04_run_dir_absent"]["argv"][0] == "mkdir" and "-p" not in steps["X04_run_dir_absent"]["argv"]
    assert plan["permitted_transaction_producing_operations"] == ["X08_caliper"]
    assert set(plan["permitted_local_staging_operations"]) == {"X04_run_dir_absent", "X05_stage_rendered_run"}
    assert plan["mounts"]["container_workspace"] == "/hyperledger/caliper/workspace" and plan["envelope"]["planned_transactions"] == 360 and plan["retry_policy"].startswith("none")
    assert be.verify_plan(plan, repo_root=REPO) == []


def test_02_every_read_only_vector_in_the_plan_is_on_the_allow_list_and_nothing_mutates_a_channel(built):
    _, plan, _, _ = built
    for stage in ("initial", "final"):
        for step in plan["config_inspection"][stage]["steps"]:
            ci.validate_read_only_argv(step["argv"])
    flat = [word for st in plan["steps"] if st["argv"] for word in st["argv"]]
    for forbidden in ("create", "join", "update", "install", "approveformyorg", "commit", "invoke"):
        assert forbidden not in flat


def test_03_prometheus_deferred_has_no_scrape_step_and_before_after_adds_two(tmp_path):
    root = s.render_a0p(tmp_path)
    req = s.request_for(root, tmp_path, prometheus={"mode": "before_after", "interval_s": 1, "max_rounds": 3, "timeout_s": 2})
    plan = be.build_execution_plan(req, repo_root=REPO, created_utc="2026-11-22T09:00:00Z")
    scrapes = [st for st in plan["steps"] if "scrape" in (st["argv"] or [])]
    assert [st["step"] for st in scrapes] == ["X07_prometheus_before", "X10_prometheus_after"] and all(st["class"] == "read_only_observation" for st in scrapes)
    assert "scripts/e1/e1_metrics_scraper.py" in plan["artifact_hashes"]


@pytest.mark.parametrize("override, code", [
    ({"worker_count": 2}, "worker_count"), ({"worker_count": True}, "worker_count"), ({"variant": "a0m_recorder_v3_issue_logged"}, "variant_not_executable"),
    ({"variant": "latest"}, "variant_not_executable"), ({"channel": "e1-r002"}, "channel_refused"), ({"channel": "e1-cfg001"}, "channel_refused"), ({"channel": "e1-cp01-b10-l15"}, "channel_refused"),
    ({"cell_id": "b10-wrong"}, "cell_id_mismatch"), ({"recorder_version": "e1_per_tx_recorder_v2"}, "recorder_not_selected"),
    ({"workload": {"file": "e1_create_asset.js", "identity": "e1_create_asset/1"}}, "workload_not_selected"), ({"run_id": "bad id!"}, "run_id_invalid"),
    ({"rounds": []}, "rounds"), ({"limits": {"max_rate_tps": 5}}, "limits"), ({"prometheus": {"mode": "maybe"}}, "prometheus"), ({"clock": {"tolerance_ms": 5}}, "clock")])
def test_04_request_validation_rejects_each_unsupported_or_unsafe_value(tmp_path, override, code):
    root = s.render_a0p(tmp_path)
    req = s.request_for(root, tmp_path, **override)
    assert code in {i["code"] for i in be.validate_request(req)}
    with pytest.raises(be.RequestInvalid):
        be.build_execution_plan(req, repo_root=REPO, created_utc="2026-11-22T09:00:00Z")


@pytest.mark.parametrize("key", list(be.REQUEST_KEYS))
def test_05_no_research_sensitive_value_has_a_default(tmp_path, key):
    root = s.render_a0p(tmp_path)
    req = s.request_for(root, tmp_path)
    del req[key]
    assert {"code": "missing", "field": key, "message": "no value was supplied; there is no default"} in be.validate_request(req)


def test_06_unbounded_or_oversized_parameters_are_rejected(tmp_path):
    root = s.render_a0p(tmp_path)
    for limits_patch, code in (({"max_transactions": 100}, "transactions_above_limit"), ({"max_rate_tps": 3}, "rate_above_limit"), ({"max_round_duration_s": 40}, "round_duration_above_limit"),
                               ({"max_total_duration_s": 60, "caliper_timeout_s": 60}, "total_duration_above_limit"), ({"caliper_timeout_s": 50}, "caliper_timeout")):
        req = s.request_for(root, tmp_path)
        req["limits"].update(limits_patch)
        with pytest.raises(be.RequestInvalid) as e:
            be.build_execution_plan(req, repo_root=REPO, created_utc="2026-11-22T09:00:00Z")
        assert code in {i["code"] for i in e.value.issues}, limits_patch
    for bad in (0, -1, float("inf"), float("nan"), True, None, "5"):
        req = s.request_for(root, tmp_path)
        req["limits"]["max_transactions"] = bad
        assert any(i["code"] == "limits" for i in be.validate_request(req))


def test_07_rendered_values_must_equal_the_request(tmp_path):
    root = s.render_a0p(tmp_path)
    for patch, code in (({"cell": {**CELL, "max_message_count": 10}, "cell_id": "b10-t2000ms-p10485760-a103809024-o3"}, "cell_mismatch"),
                        ({"operator_timing": {"tick_interval_ms": 500, "fsync_policy": "tick", "clock_domain": "caliper-worker-host:node_date_now", "heartbeat_interval_ms": 1000,
                                              "checkpoint_every_heartbeats": 60}}, "operator_timing_mismatch"),
                        ({"channel": "e1-other"}, "channel_mismatch")):
        req = s.request_for(root, tmp_path, **patch)
        with pytest.raises(be.RequestInvalid) as e:
            be.build_execution_plan(req, repo_root=REPO, created_utc="2026-11-22T09:00:00Z")
        assert code in {i["code"] for i in e.value.issues}, patch
    req = s.request_for(root, tmp_path)
    req["rounds"][1]["rate_tps"] = 3.0
    with pytest.raises(be.RequestInvalid) as e:
        be.build_execution_plan(req, repo_root=REPO, created_utc="2026-11-22T09:00:00Z")
    assert "rounds_mismatch" in {i["code"] for i in e.value.issues}


def test_08_an_occupied_output_path_is_refused_at_plan_time_and_at_execution_time(built):
    tmp, plan, req, root = built
    out = Path(plan["layout"]["output_dir"])
    req2 = copy.deepcopy(req)
    out.mkdir()
    with pytest.raises(be.RequestInvalid) as e:
        be.build_execution_plan(req2, repo_root=REPO, created_utc="2026-11-22T09:00:00Z")
    assert "output_occupied" in {i["code"] for i in e.value.issues}
    with pytest.raises(be.LaunchRefused) as e2:
        run(built)
    assert "already exists" in str(e2.value) and list(out.iterdir()) == []


def test_09_a_modified_artifact_or_manifest_hash_stops_the_plan(tmp_path):
    root = s.render_a0p(tmp_path)
    req = s.request_for(root, tmp_path)
    req["candidate_manifest"]["sha256"] = "0" * 64
    with pytest.raises(be.RequestInvalid) as e:
        be.build_execution_plan(req, repo_root=REPO, created_utc="2026-11-22T09:00:00Z")
    assert "manifest_hash" in {i["code"] for i in e.value.issues}
    req = s.request_for(root, tmp_path)
    manifest = json.loads(Path(req["candidate_manifest"]["path"]).read_text(encoding="utf-8"))
    manifest["entries"][0]["sha256"] = "f" * 64
    Path(req["candidate_manifest"]["path"]).write_text(json.dumps(manifest), encoding="utf-8")
    req["candidate_manifest"]["sha256"] = pi.sha256_file(Path(req["candidate_manifest"]["path"]))
    with pytest.raises(be.RequestInvalid) as e:
        be.build_execution_plan(req, repo_root=REPO, created_utc="2026-11-22T09:00:00Z")
    assert "artifact_hash" in {i["code"] for i in e.value.issues}


def test_10_a_tool_the_plan_uses_must_be_listed_in_the_manifest(tmp_path):
    root = s.render_a0p(tmp_path)
    req = s.request_for(root, tmp_path)
    manifest = json.loads(Path(req["candidate_manifest"]["path"]).read_text(encoding="utf-8"))
    manifest["entries"] = [e for e in manifest["entries"] if e["path"] != "src/fabric_perf/controllers/config_inspection.py"]
    Path(req["candidate_manifest"]["path"]).write_text(json.dumps(manifest), encoding="utf-8")
    req["candidate_manifest"]["sha256"] = pi.sha256_file(Path(req["candidate_manifest"]["path"]))
    with pytest.raises(be.RequestInvalid) as e:
        be.build_execution_plan(req, repo_root=REPO, created_utc="2026-11-22T09:00:00Z")
    assert "tool_not_in_manifest" in {i["code"] for i in e.value.issues}


def test_11_a_tampered_plan_fails_preflight(built):
    _, plan, _, _ = built
    for edit in (lambda p: p["steps"][6]["argv"].append("--extra"), lambda p: p["envelope"].__setitem__("planned_transactions", 1), lambda p: p.__setitem__("channel", "e1-other"),
                 lambda p: p["config_inspection"]["initial"]["steps"][2]["argv"].__setitem__(2, "update")):
        tampered = copy.deepcopy(plan)
        edit(tampered)
        assert be.verify_plan(tampered, repo_root=REPO)
    flagged = copy.deepcopy(plan)
    flagged["real_launch_enabled"] = True
    assert any(p["code"] == "real_launch_flag" for p in be.verify_plan(flagged, repo_root=REPO))


# ------------------------------------------------------------------------------------------------ authorization
def test_12_template_has_every_approval_field_unset_and_never_verifies(built):
    tmp, plan, _, _ = built
    template = be.authorization_template(plan)
    assert template["approval"] == {k: None for k in be.APPROVAL_KEYS} and template["synthetic"] is False
    verdict = be.verify_authorization(template, plan, repo_root=REPO)
    assert verdict["valid"] is False and "approval_unset" in {f["code"] for f in verdict["findings"]} and verdict["real_launch_permitted"] is False


@pytest.mark.parametrize("auth", [None, {}, [], "approved", 5])
def test_13_missing_or_malformed_authorization_is_rejected(built, auth):
    _, plan, _, _ = built
    verdict = be.verify_authorization(auth, plan, repo_root=REPO)
    assert verdict["valid"] is False and verdict["real_launch_permitted"] is False


def test_14_a_synthetic_authorization_verifies_only_with_allow_synthetic_and_is_flagged(built):
    tmp, plan, _, _ = built
    auth = approved_auth(plan, tmp)
    assert be.verify_authorization(auth, plan, repo_root=REPO)["valid"] is False
    assert "synthetic_authorization" in {f["code"] for f in be.verify_authorization(auth, plan, repo_root=REPO)["findings"]}
    verdict = be.verify_authorization(auth, plan, repo_root=REPO, allow_synthetic=True)
    assert verdict["valid"] is True and verdict["synthetic"] is True and verdict["real_launch_permitted"] is False


@pytest.mark.parametrize("edit, code", [
    (lambda a: a["artifact_hashes"].__setitem__("src/fabric_perf/controllers/bounded_execution.py", "0" * 64), "artifact_hash_mismatch"),
    (lambda a: a["artifact_hashes"].pop("scripts/e1/e1_ledger_follow.py"), "artifact_hash_mismatch"),
    (lambda a: a["manifest"].__setitem__("sha256", "1" * 64), "manifest_mismatch"), (lambda a: a.__setitem__("plan_sha256", "2" * 64), "plan_digest_mismatch"),
    (lambda a: a.__setitem__("run_id", "other-run"), "run_id_mismatch"), (lambda a: a.__setitem__("cell_id", "b10-t2000ms-p1-a1-o3"), "cell_id_mismatch"),
    (lambda a: a.__setitem__("channel", "e1-other"), "channel_mismatch"), (lambda a: a.__setitem__("channel", "e1-r002"), "channel_refused"),
    (lambda a: a.__setitem__("variant", "a0m_recorder_v3_issue_logged"), "variant_not_executable"), (lambda a: a.__setitem__("worker_count", 2), "worker_count_unsupported"),
    (lambda a: a.__setitem__("recorder_version", "e1_per_tx_recorder_v2"), "recorder_version_mismatch"), (lambda a: a["workload"].__setitem__("file", "e1_create_asset.js"), "workload_mismatch"),
    (lambda a: a["rounds"][1].__setitem__("rate_tps", 40.0), "rounds_mismatch"), (lambda a: a["operator_timing"].__setitem__("tick_interval_ms", 7), "operator_timing_mismatch"),
    (lambda a: a.__setitem__("output_dir", "/elsewhere"), "output_dir_mismatch"), (lambda a: a["limits"].pop("max_transactions"), "limits_unbounded"),
    (lambda a: a["limits"].__setitem__("max_transactions", 10 ** 9), "limits_differ_from_plan"), (lambda a: a["limits"].__setitem__("max_transactions", 100), "planned_exceeds_authorized_limit"),
    (lambda a: a["permitted_transaction_producing_operations"].append("X06_ledger_follower"), "transaction_operations_mismatch"),
    (lambda a: a["permitted_read_only_operations"].pop(), "read_only_operations_mismatch"), (lambda a: a["permitted_local_staging_operations"].clear(), "local_staging_operations_mismatch"),
    (lambda a: a["stop_conditions"].pop(), "stop_conditions"), (lambda a: a.__setitem__("prohibited_configuration_changes", "NONE"), "configuration_changes_not_prohibited"),
    (lambda a: a["approval"].__setitem__("status", "PROPOSED"), "approval_status"), (lambda a: a["approval"].__setitem__("approved_by", None), "approval_unset"),
    (lambda a: a["approval"].__setitem__("approved_utc", "2026-11-22T09:30:00"), "approval_time"), (lambda a: a["approval"].__setitem__("record_sha256", "3" * 64), "approval_record_hash"),
    (lambda a: a["approval"].__setitem__("record_path", "/nonexistent/approval.txt"), "approval_record_unreadable"), (lambda a: a.__setitem__("schema", "x/1"), "authorization_schema")])
def test_15_each_binding_mismatch_is_rejected_with_its_own_code(built, edit, code):
    tmp, plan, _, _ = built
    auth = approved_auth(plan, tmp)
    edit(auth)
    verdict = be.verify_authorization(auth, plan, repo_root=REPO, allow_synthetic=True)
    assert verdict["valid"] is False and code in {f["code"] for f in verdict["findings"]}


def test_16_an_unverified_approval_document_is_not_an_approval(built):
    tmp, plan, _, _ = built
    auth = approved_auth(plan, tmp)
    Path(auth["approval"]["record_path"]).write_text("edited after approval\n", encoding="utf-8")
    assert "approval_record_hash" in {f["code"] for f in be.verify_authorization(auth, plan, repo_root=REPO, allow_synthetic=True)["findings"]}


def test_17_no_flag_environment_variable_manifest_or_preflight_substitutes_for_an_authorization(built, monkeypatch):
    tmp, plan, req, root = built
    monkeypatch.setenv("EXECUTE", "1")
    monkeypatch.setenv("A0S_REAL_LAUNCH", "true")
    assert be.verify_plan(plan, repo_root=REPO) == []                          # a passing preflight ...
    template = be.authorization_template(plan)                                  # ... and an assistant-generated template ...
    clock = FakeClock()
    deps = deps_for(MockRunner(clock), clock)
    with pytest.raises(be.LaunchRefused):                                       # ... authorize nothing
        be.execute_bounded(plan, template, deps, repo_root=REPO, allow_synthetic=True)
    with pytest.raises(be.LaunchRefused):
        be.execute_bounded(plan, None, deps, repo_root=REPO, allow_synthetic=True)
    assert not Path(plan["layout"]["output_dir"]).exists()
    assert be.REAL_LAUNCH_ENABLED is False


# ------------------------------------------------------------------------------------------------ execution with a mock runner
def test_18_a_complete_mock_run_executes_the_planned_vectors_in_order_once(built):
    tmp, plan, _, _ = built
    runner, result = run(built)
    assert result["status"] == "COMPLETED" and result["halted_at"] is None and result["transaction_attempts"] == 1 and result["real_process_started"] is False
    cal = runner.calls_matching("launch")
    assert cal == [next(st["argv"] for st in plan["steps"] if st["step"] == "X08_caliper")] and len(cal) == 1
    assert cal[0][:6] == ["docker", "exec", "-w", "/hyperledger/caliper/workspace", "caliper", "caliper"] and "--caliper-flow-only-test" in cal[0]
    assert runner.background == [next(st["argv"] for st in plan["steps"] if st["step"] == "X06_ledger_follower")] and runner.finished == [1]
    order = [("mkdir" if a[0] == "mkdir" else "cp" if a[0] == "cp" else "caliper" if caliper(a) else "wsl" if a[0] == "wsl.exe" else "cfg") for a, _, _ in runner.calls]
    assert order.index("mkdir") < order.index("cp") < order.index("caliper")
    timeouts = {tuple(a): t for a, t, _ in runner.calls}
    assert timeouts[tuple(cal[0])] == 100
    out = Path(plan["layout"]["output_dir"])
    for name in ("00_plan/execution_plan.json", "00_plan/approval_reference.json", "00_plan/execution_log.jsonl", "run_status.json", "integrity_manifest.json",
                 "config_inspection/initial/evidence.json", "config_inspection/final/evidence.json", "clock/pre_run.json", "clock/post_run.json", "clock/clock_verdict.json"):
        assert (out / name).is_file(), name
    assert json.loads((out / "clock/clock_verdict.json").read_text(encoding="utf-8"))["verdict"] == "AGREE"


def test_19_the_expected_caliper_argument_vector_comes_from_the_rendered_plan_not_from_the_executor(built):
    tmp, plan, _, _ = built
    runner, _ = run(built)
    argv = runner.calls_matching("launch")[0]
    assert argv[-5:] == ["--caliper-benchconfig", f"runs/{plan['run_id']}/benchmark.yaml", "--caliper-networkconfig", f"runs/{plan['run_id']}/networkconfig.yaml", "--caliper-flow-only-test"]


def test_20_a_caliper_timeout_preserves_the_partial_evidence_and_terminates_the_collector(built):
    tmp, plan, _, _ = built
    runner, result = run(built, rules=lambda argv: be.RunResult(None, b"partial", b"", True) if caliper(argv) else None, finish_timed_out=True)
    assert result["status"] == "TIMED_OUT" and result["halted_at"] == "X08_caliper" and result["partial_evidence_preserved"] is True and result["transaction_attempts"] == 1
    assert runner.terminated == [1]                                              # the still-running ledger follower was terminated
    out = Path(plan["layout"]["output_dir"])
    assert (out / "process_logs/X08_caliper.stdout").read_bytes() == b"partial" and (out / "integrity_manifest.json").is_file() and (out / "run_status.json").is_file()
    assert (out / "config_inspection/final/evidence.json").is_file() and (out / "clock/post_run.json").is_file()        # read-only post-run observation still ran
    assert len(runner.calls_matching("launch")) == 1


def test_21_a_nonzero_caliper_exit_is_never_retried(built):
    runner, result = run(built, rules=lambda argv: be.RunResult(1, b"", b"worker crashed") if caliper(argv) else None)
    assert result["status"] == "FAILED_DURING_TRANSACTIONS" and result["transaction_attempts"] == 1
    assert len(runner.calls_matching("launch")) == 1


def test_22_a_second_transaction_producing_step_is_refused(built):
    tmp, plan, _, _ = built
    doubled = copy.deepcopy(plan)
    cal = next(st for st in doubled["steps"] if st["step"] == "X08_caliper")
    doubled["steps"].insert(doubled["steps"].index(cal) + 1, {**cal, "step": "X08b_caliper_again"})
    assert any(p["code"] in ("plan_digest", "transaction_step") for p in be.verify_plan(doubled, repo_root=REPO))
    with pytest.raises(be.LaunchRefused):
        clock = FakeClock()
        be.execute_bounded(doubled, approved_auth(doubled, tmp), deps_for(MockRunner(clock), clock), repo_root=REPO, allow_synthetic=True)


def test_23_a_failed_pre_run_step_stops_before_any_transaction(built):
    runner, result = run(built, initial=decoded_block(B=10))                      # the channel does not carry the approved cell
    assert result["status"] == "HALTED_BEFORE_TRANSACTIONS" and result["halted_at"] == "X02_config_initial" and result["transaction_attempts"] == 0
    assert runner.calls_matching("launch") == [] and runner.background == [] and not any(c[0] in ("mkdir", "cp") for c, _, _ in runner.calls)


def test_24_a_clock_that_disagrees_halts_before_any_transaction(built):
    tmp, plan, _, _ = built
    clock = FakeClock(offset_ns=900 * s.MS)
    runner = MockRunner(clock)
    result = be.execute_bounded(plan, approved_auth(plan, tmp), deps_for(runner, clock), repo_root=REPO, allow_synthetic=True)
    assert result["status"] == "HALTED_BEFORE_TRANSACTIONS" and result["halted_at"] == "X03_clock_pre" and runner.calls_matching("launch") == []


def test_25_a_staging_failure_blocks_the_transaction_step(built):
    runner, result = run(built, rules=lambda argv: be.RunResult(1, b"", b"exists") if argv[0] == "mkdir" else None)
    assert result["status"] == "HALTED_BEFORE_TRANSACTIONS" and result["halted_at"] == "X04_run_dir_absent" and runner.calls_matching("launch") == []


def test_26_a_configuration_change_during_the_run_is_detected(built):
    runner, result = run(built, final=decoded_block(sequence=1, number=1))
    assert result["status"] == "CONFIGURATION_CHANGED" and result["halted_at"] == "X12_config_final" and result["transaction_attempts"] == 1


def test_27_a_final_configuration_that_differs_from_the_cell_is_reported(built):
    runner, result = run(built, final=decoded_block(B=10))
    assert result["status"] == "FINAL_CHECK_FAILED"


def test_28_interruption_terminates_collectors_writes_status_and_is_reraised(built):
    tmp, plan, _, _ = built
    with pytest.raises(KeyboardInterrupt):
        run(built, raise_on=lambda argv: KeyboardInterrupt() if caliper(argv) else None)
    out = Path(plan["layout"]["output_dir"])
    status = json.loads((out / "run_status.json").read_text(encoding="utf-8"))
    assert status["status"] == "INTERRUPTED" and status["transaction_attempts"] == 1 and status["partial_evidence_preserved"] is True
    assert (out / "integrity_manifest.json").is_file() and (out / "00_plan/execution_log.jsonl").is_file()
    log = [json.loads(line) for line in (out / "00_plan/execution_log.jsonl").read_text(encoding="utf-8").splitlines()]
    assert any(row["event"] == "interrupt" for row in log) and any(row["event"] == "collector_terminated" for row in log)


def test_29_a_runner_error_is_a_recorded_halt_not_a_retry(built):
    runner, result = run(built, raise_on=lambda argv: OSError("docker not found") if caliper(argv) else None)
    assert result["status"] == "ERROR" and result["halted_at"] == "X08_caliper" and len(runner.calls_matching("launch")) == 1


def test_30_evidence_files_are_created_exclusively_and_a_second_run_in_the_same_directory_is_refused(built):
    tmp, plan, _, _ = built
    run(built)
    clock = FakeClock()
    with pytest.raises(be.LaunchRefused):
        be.execute_bounded(plan, approved_auth(plan, tmp), deps_for(MockRunner(clock), clock), repo_root=REPO, allow_synthetic=True)
    with pytest.raises(FileExistsError):
        be._create_new(Path(plan["layout"]["output_dir"]) / "run_status.json", b"overwrite")


def test_31_the_execution_log_is_append_only_and_records_every_step(built):
    tmp, plan, _, _ = built
    run(built)
    log = [json.loads(line) for line in (Path(plan["layout"]["output_dir"]) / "00_plan/execution_log.jsonl").read_text(encoding="utf-8").splitlines()]
    begun = [row["step"] for row in log if row["event"] == "step_begin"]
    assert begun == [st["step"] for st in plan["steps"]] and log[0]["event"] == "start" and log[0]["synthetic_authorization"] is True


# ------------------------------------------------------------------------------------------------ real launch stays disabled
@pytest.mark.parametrize("runner_cls", [ProcessRunner])
def test_32_a_process_runner_is_refused_even_with_a_verifying_authorization_and_nothing_is_created(built, runner_cls):
    tmp, plan, _, _ = built
    with pytest.raises(be.LaunchRefused) as e:
        run(built, runner_cls=runner_cls)
    assert "real launch is disabled in this version" in str(e.value) and not Path(plan["layout"]["output_dir"]).exists()


@pytest.mark.parametrize("kind", [None, "live", "real", "docker", ""])
def test_33_unknown_runner_kinds_are_refused(built, kind):
    tmp, plan, _, _ = built
    clock = FakeClock()
    runner = MockRunner(clock)
    runner.kind = kind
    with pytest.raises(be.LaunchRefused):
        be.execute_bounded(plan, approved_auth(plan, tmp), deps_for(runner, clock), repo_root=REPO, allow_synthetic=True)
    assert runner.calls == [] and not Path(plan["layout"]["output_dir"]).exists()


def test_34_a_synthetic_authorization_without_allow_synthetic_is_refused_for_execution(built):
    with pytest.raises(be.LaunchRefused):
        run(built, allow_synthetic=False)


def test_35_the_cli_execute_is_refused_whatever_follows(capsys):
    import e1_a0s_bounded_execute as cli

    for extra in ([], ["--execute"], ["--plan", "p.json", "--authorization", "a.json"], ["--force", "--yes"]):
        assert cli.main(["execute", *extra]) == 5
    assert "REFUSED" in capsys.readouterr().out


def test_36_cli_prepare_preflight_inspect_template_and_verify(built, capsys):
    tmp, plan, req, root = built
    import e1_a0s_bounded_execute as cli

    request_file, plan_file, template = tmp / "request.json", tmp / "plan.json", tmp / "template.json"
    request_file.write_text(json.dumps(req), encoding="utf-8")
    assert cli.main(["prepare", "--request", str(request_file), "--out-plan", str(plan_file), "--created-utc", "2026-11-22T09:00:00Z"]) == 0
    assert cli.main(["prepare", "--request", str(request_file), "--out-plan", str(plan_file), "--created-utc", "2026-11-22T09:00:00Z"]) == 2        # never overwritten
    assert cli.main(["preflight", "--plan", str(plan_file)]) == 0
    assert cli.main(["inspect-plan", "--plan", str(plan_file)]) == 0
    assert cli.main(["authorization-template", "--plan", str(plan_file), "--out", str(template)]) == 0
    assert cli.main(["verify-authorization", "--plan", str(plan_file), "--authorization", str(template)]) == 1
    text = capsys.readouterr().out
    assert "X08_caliper" in text and "claims not made" in text and "approval_unset" in text
    bad = dict(req, worker_count=3)
    request_file2 = tmp / "request2.json"
    request_file2.write_text(json.dumps(bad), encoding="utf-8")
    assert cli.main(["prepare", "--request", str(request_file2), "--out-plan", str(tmp / "plan2.json"), "--created-utc", "2026-11-22T09:00:00Z"]) == 2
    assert not (tmp / "plan2.json").exists()
