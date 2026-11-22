"""A0t workstreams A and C: the plan-bound, WSL-aware process-runner adapter and its binding to the A0s execution gate.

Plans come from a real offline A0p render. Execution goes ONLY through ``MockTransport`` (starts nothing) or ``FixtureTransport`` (harmless local Python one-liners; never wsl.exe, Docker,
Caliper, Fabric or a network client). The real transport is disabled and asserted to be."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests import a0t_support as t
from tests.a0t_support import (DISTRO, LIFETIME_S, MOUNT_ROOT, REPO, FakeClock, FixtureTransport, MockTransport, a0t_auth, build_a0t_plan, caliper_inv, decoded_block, description,
                               path_record, run_adapter)

from fabric_perf.controllers import bounded_execution as be      # noqa: E402
from fabric_perf.controllers import config_inspection as ci      # noqa: E402
from fabric_perf.controllers import path_mapping as pm           # noqa: E402
from fabric_perf.controllers import process_runner as pr         # noqa: E402


@pytest.fixture()
def built(tmp_path):
    plan, req, root = build_a0t_plan(tmp_path)
    return tmp_path, plan, req, root, path_record(plan)


def status_of(plan):
    return json.loads((Path(plan["layout"]["output_dir"]) / "run_status.json").read_text(encoding="utf-8"))


def refused(built, **kw):
    tmp, plan, req, root, record = built
    transport = kw.pop("transport", MockTransport())
    with pytest.raises(be.LaunchRefused) as info:
        run_adapter(tmp, plan, record, transport, **kw)
    assert getattr(transport, "invocations", []) == [], "nothing may be handed to the transport"
    assert not Path(plan["layout"]["output_dir"]).exists(), "a refused launch creates nothing"
    return info.value


def finding_codes(exc):
    return {f["code"] for f in exc.findings}


def bound_runner(built, transport=None, *, lifetime=LIFETIME_S):
    tmp, plan, req, root, record = built
    return pr.PlanBoundRunner(plan, transport or MockTransport(), distro=DISTRO, mount_root=MOUNT_ROOT, max_process_lifetime_s=lifetime)


def step(plan, step_id):
    return next(s for s in plan["steps"] if s["step"] == step_id)


# ---- 1: structured argument vectors are preserved
def test_01_the_plan_vectors_reach_the_transport_unchanged_and_structured(built):
    tmp, plan, req, root, record = built
    transport = MockTransport()
    status = run_adapter(tmp, plan, record, transport)
    assert status["status"] == "COMPLETED"
    wanted = {tuple(s["argv"]) for s in plan["steps"] if s["argv"]} | {tuple(v["argv"]) for stage in ("initial", "final") for v in plan["config_inspection"][stage]["steps"]}
    seen = {inv.plan_argv for inv in transport.invocations}
    assert seen == wanted
    for inv in transport.invocations:
        assert isinstance(inv.os_argv, tuple) and all(isinstance(x, str) for x in inv.os_argv)
        if inv.domain == pr.DOMAIN_WSL:
            head = ("wsl.exe", "-d", DISTRO) + (("--cd", inv.wsl_cwd) if inv.wsl_cwd else ()) + ("--exec",)
            assert inv.os_argv[: len(head)] == head
            assert "--exec" in inv.os_argv and inv.os_argv[inv.os_argv.index("--exec") + 1:] == inv.plan_argv      # no path in this plan needed translation: the tail is the plan vector
        else:
            assert inv.os_argv == inv.plan_argv


def test_01_a_token_with_spaces_stays_one_token():
    inv = pr.build_invocation("S", ["tool", "--name", "a b  c", "x"], "/home/u/dir with space", 5, launch_mode=pr.LAUNCH_FOREGROUND, distro="Ubuntu", mount_root="/mnt")
    assert inv.os_argv == ("wsl.exe", "-d", "Ubuntu", "--cd", "/home/u/dir with space", "--exec", "tool", "--name", "a b  c", "x")


# ---- 2: no shell injection
@pytest.mark.parametrize("token", ["a;b", "a|b", "a&b", "a`b`", "$(x)", "$HOME", "a>b", "a<b", "a\nb", "a\rb", "it's", 'say "x"', "a\x00b"])
def test_02_metacharacters_and_quotes_are_refused(token):
    with pytest.raises(pr.ArgvRefused) as info:
        pr.validate_argv(["tool", token])
    assert info.value.code == "shell_metacharacter_or_quote"


@pytest.mark.parametrize("argv,code", [
    ("rm -rf /", "argv_not_a_vector"), (b"x", "argv_not_a_vector"), ([], "argv_length"), (["tool", 1], "token_not_text"), ([""], "empty_executable"),
    (["bash", "-c", "x"], "shell_executable"), (["/bin/sh", "x"], "shell_executable"), (["cmd.exe", "/c", "dir"], "shell_executable"), (["tool", "powershell.exe"], "shell_executable"),
    (["python3", "-c", "import os"], "inline_code"), (["node", "-e", "1"], "inline_code"),
    (["wsl.exe", "-d", "Ubuntu", "date"], "wsl_without_exec"), (["wsl.exe", "-d", "Ubuntu", "-e", "date", "--exec"], "ambiguous_wsl_flag"),
    (["tool", "../secret"], "path_traversal"), (["tool", "x" * 5000], "token_too_long"),
])
def test_02_other_ambiguous_or_shell_shaped_vectors_are_refused(argv, code):
    with pytest.raises(pr.ArgvRefused) as info:
        pr.validate_argv(argv)
    assert info.value.code == code


def test_02_legitimate_tokens_pass_including_parentheses_and_percent():
    assert pr.validate_argv(["wsl.exe", "-d", "Ubuntu", "--exec", "date", "+%s%N"])[-1] == "+%s%N"
    assert pr.validate_argv([r"C:\Program Files (x86)\Tool\t.exe", "--x", "1.5"])[0].endswith("t.exe")
    assert pr.validate_argv(["peer", "channel", "fetch", "config", "f", "-c", "e1-ch", "-o", "localhost:7050"])[5] == "-c"


def test_02_the_runner_refuses_an_injected_vector_before_the_transport(built):
    transport = MockTransport()
    runner = bound_runner(built, transport)
    for argv in (["bash", "-c", "docker exec caliper x"], step(built[1], "X08_caliper")["argv"] + [";", "rm", "x"], "docker exec caliper"):
        with pytest.raises(pr.RunnerRefused):
            runner.run(argv, 10, None)
    assert transport.invocations == []


# ---- 3: wrong transport
def test_03_unknown_and_mismatched_transport_labels_are_rejected():
    for label in (None, "", "ssh", "docker", "process", "subprocess"):
        with pytest.raises(pr.RunnerRefused) as info:
            pr.make_transport(label, synthetic_authorization=True, fixture_factory=MockTransport)
        assert info.value.code == "transport_unknown"
    with pytest.raises(pr.RunnerRefused) as info:
        pr.make_transport(pr.TRANSPORT_FIXTURE, synthetic_authorization=False, fixture_factory=MockTransport)
    assert info.value.code == "fixture_transport_needs_synthetic_authorization"
    with pytest.raises(pr.RunnerRefused) as info:
        pr.make_transport(pr.TRANSPORT_FIXTURE, synthetic_authorization=True, fixture_factory=None)
    assert info.value.code == "fixture_transport_needs_synthetic_authorization"

    class Process:
        kind = "process"
    with pytest.raises(pr.RunnerRefused) as info:
        pr.make_transport(pr.TRANSPORT_FIXTURE, synthetic_authorization=True, fixture_factory=Process)
    assert info.value.code == "fixture_transport_kind"


def test_03_a_transport_of_an_unrecognised_kind_cannot_back_a_runner(built):
    class Odd:
        kind = "docker"
    with pytest.raises(pr.RunnerRefused) as info:
        pr.PlanBoundRunner(built[1], Odd(), distro=DISTRO, mount_root=MOUNT_ROOT, max_process_lifetime_s=60)
    assert info.value.code == "transport_kind"


def test_03_the_authorization_must_name_the_transport_and_the_environment(built):
    tmp, plan, req, root, record = built
    for field, value, code in (("process_transport", "ssh", "process_transport"), ("process_transport", None, "a0t_field_unset"),
                               ("execution_environment", "wsl_orchestrator", "execution_environment"), ("execution_environment", None, "a0t_field_unset")):
        auth = a0t_auth(plan, record, tmp)
        auth["a0t"][field] = value
        assert code in finding_codes(refused(built, auth=auth)), (field, value)


# ---- 4: missing approval
def test_04_an_unapproved_template_is_refused_and_nothing_starts(built):
    tmp, plan, req, root, record = built
    auth = be.authorization_template(plan)
    auth["a0t"] = pr.a0t_authorization_extension(plan, record, description(plan, record), category=pr.OP_SMOKE)
    codes = finding_codes(refused(built, auth=auth))
    assert {"approval_unset", "a0t_field_unset"} <= codes


def test_04_missing_a0t_block_missing_record_and_blank_fields_are_refused(built):
    tmp, plan, req, root, record = built
    auth = a0t_auth(plan, record, tmp)
    no_ext = {k: v for k, v in auth.items() if k != "a0t"}
    assert "a0t_extension_missing" in finding_codes(refused(built, auth=no_ext))
    blank = copy.deepcopy(auth)
    blank["approval"]["approved_by"] = ""
    assert "approval_unset" in finding_codes(refused(built, auth=blank))
    edited = copy.deepcopy(auth)
    Path(edited["approval"]["record_path"]).write_text("edited after approval\n", encoding="utf-8")
    assert "approval_record_hash" in finding_codes(refused(built, auth=edited))


def test_04_a_file_called_approved_json_is_not_an_approval(built, tmp_path):
    tmp, plan, req, root, record = built
    fake = tmp / "approved.json"
    fake.write_text(json.dumps({"status": "APPROVED_BY_OWNER"}), encoding="utf-8")
    auth = a0t_auth(plan, record, tmp)
    auth["approval"]["record_path"] = str(fake)                      # a record whose hash was not recorded for this file
    assert "approval_record_hash" in finding_codes(refused(built, auth=auth))
    auth["synthetic"] = True
    plain = copy.deepcopy(auth)
    plain["approval"]["status"] = "approved"
    assert "approval_status" in finding_codes(refused(built, auth=plain))


def test_04_a_synthetic_authorization_without_the_allow_flag_is_refused(built):
    assert "synthetic_authorization" in finding_codes(refused(built, allow_synthetic=False))


# ---- 5-8: wrong manifest, channel, cell, variant
@pytest.mark.parametrize("mutate,code", [
    (lambda a: a["manifest"].update(sha256="0" * 64), "manifest_mismatch"),
    (lambda a: a["manifest"].update(path="docs/research/e1/other.json"), "manifest_mismatch"),
    (lambda a: a["artifact_hashes"].update({next(iter(a["artifact_hashes"])): "1" * 64}), "artifact_hash_mismatch"),
    (lambda a: a.update(channel="e1-some-other-channel"), "channel_mismatch"),
    (lambda a: a.update(channel="e1-r002"), "channel_refused"),
    (lambda a: a.update(channel="e1-cfg001"), "channel_refused"),
    (lambda a: a.update(channel="e1-b001"), "channel_refused"),
    (lambda a: a.update(cell_id="b10-t2000ms-p10485760-a103809024-o3"), "cell_id_mismatch"),
    (lambda a: a.update(variant="a0m_recorder_v3_issue_logged"), "variant_mismatch"),
    (lambda a: a.update(variant="a0e_recorder_v2_legacy_workload"), "variant_not_executable"),
    (lambda a: a.update(run_id="another-run"), "run_id_mismatch"),
    (lambda a: a.update(worker_count=2), "worker_count_unsupported"),
    (lambda a: a.update(plan_sha256="2" * 64), "plan_digest_mismatch"),
    (lambda a: a.update(output_dir="C:\\somewhere\\else"), "output_dir_mismatch"),
    (lambda a: a.update(recorder_version="e1_per_tx_recorder"), "recorder_version_mismatch"),
    (lambda a: a.update(workload={"file": "e1_create_asset.js", "identity": "x"}), "workload_mismatch"),
])
def test_05_to_08_binding_mismatches_are_refused(built, mutate, code):
    tmp, plan, req, root, record = built
    auth = a0t_auth(plan, record, tmp)
    mutate(auth)
    assert code in finding_codes(refused(built, auth=auth))


# ---- 9, 10, 11: bounded load, duration, transactions
def test_09_to_11_a_plan_cannot_be_built_beyond_its_limits(tmp_path):
    for field, value, code in (("max_rate_tps", 3, "rate_above_limit"), ("max_total_duration_s", 80, "total_duration_above_limit"),
                               ("max_round_duration_s", 40, "round_duration_above_limit"), ("max_transactions", 300, "transactions_above_limit")):
        (tmp_path / f"p_{field}").mkdir()
        probe, _req, root = build_a0t_plan(tmp_path / f"p_{field}")
        limits = dict(_req["limits"])
        limits[field] = value
        if field == "max_total_duration_s":
            limits["caliper_timeout_s"] = 80
        req = copy.deepcopy(_req)
        req["limits"] = limits
        with pytest.raises(be.RequestInvalid) as info:
            be.build_execution_plan(req, repo_root=REPO, created_utc="2026-11-22T09:00:00Z")
        assert code in {i["code"] for i in info.value.issues}, field


def test_09_to_11_the_authorization_cannot_loosen_or_omit_the_limits(built):
    tmp, plan, req, root, record = built
    for key in be.LIMIT_KEYS:
        auth = a0t_auth(plan, record, tmp)
        del auth["limits"][key]
        assert "limits_unbounded" in finding_codes(refused(built, auth=auth)), key
    auth = a0t_auth(plan, record, tmp)
    auth["limits"]["max_transactions"] = 100
    assert {"planned_exceeds_authorized_limit", "limits_differ_from_plan"} <= finding_codes(refused(built, auth=auth))
    auth = a0t_auth(plan, record, tmp)
    auth["limits"]["max_rate_tps"] = 0
    assert "limits_unbounded" in finding_codes(refused(built, auth=auth))


def test_10_the_process_lifetime_limit_is_required_positive_and_enforced(built):
    tmp, plan, req, root, record = built
    for value, code in ((None, "a0t_field_unset"), (0, "lifetime_unbounded"), (-5, "lifetime_unbounded"), (True, "lifetime_unbounded"), ("120", "lifetime_unbounded"),
                        (plan["envelope"]["limits"]["caliper_timeout_s"] - 1, "lifetime_below_caliper_timeout")):
        auth = a0t_auth(plan, record, tmp)
        auth["a0t"]["max_process_lifetime_s"] = value
        assert code in finding_codes(refused(built, auth=auth)), value
    with pytest.raises(pr.RunnerRefused) as info:
        pr.PlanBoundRunner(plan, MockTransport(), distro=DISTRO, mount_root=MOUNT_ROOT, max_process_lifetime_s=None)
    assert info.value.code == "lifetime_unbounded"
    runner = bound_runner(built, lifetime=50)
    with pytest.raises(pr.RunnerRefused) as info:
        runner.run(step(plan, "X08_caliper")["argv"], 100, None)                         # the step allows 100 s; the lifetime limit allows 50 s
    assert info.value.code == "timeout_exceeds_bound"


def test_10_timeouts_above_the_step_bound_or_missing_are_refused(built):
    runner = bound_runner(built)
    argv = step(built[1], "X04_run_dir_absent")["argv"]
    for timeout, code in ((11, "timeout_exceeds_bound"), (0, "timeout_invalid"), (-1, "timeout_invalid"), (True, "timeout_invalid"), (None, "timeout_missing")):
        with pytest.raises(pr.RunnerRefused) as info:
            runner.run(argv, timeout, None)
        assert info.value.code == code, timeout


def test_the_inspection_timeout_constant_equals_the_one_inside_config_inspection(built):
    tmp, plan, req, root, record = built
    seen = []

    def recording(argv, timeout_s):
        seen.append(timeout_s)
        return 0, b"", b""
    recording.kind = "mock"
    try:
        ci.execute_inspection(plan["config_inspection"]["initial"], recording, read_file=lambda p: json.dumps(decoded_block()).encode(), captured_utc="2026-11-22T10:00:00Z")
    except Exception:                                                  # only the timeout argument matters here
        pass
    assert seen and set(seen) == {pr.INSPECTION_TIMEOUT_S}


# ---- 12: no overwrite
def test_12_an_existing_output_directory_is_never_overwritten(built):
    tmp, plan, req, root, record = built
    out = Path(plan["layout"]["output_dir"])
    out.mkdir(parents=True)
    (out / "evidence.txt").write_text("keep", encoding="utf-8")
    transport = MockTransport()
    with pytest.raises(be.LaunchRefused) as info:
        run_adapter(tmp, plan, record, transport)
    assert "already exists" in str(info.value) and transport.invocations == []
    assert (out / "evidence.txt").read_text(encoding="utf-8") == "keep" and sorted(p.name for p in out.iterdir()) == ["evidence.txt"]


def test_12_a_plan_for_an_occupied_directory_cannot_even_be_built(tmp_path):
    probe, req, root = build_a0t_plan(tmp_path)
    Path(req["output_dir"]).mkdir(parents=True)
    with pytest.raises(be.RequestInvalid) as info:
        be.build_execution_plan(req, repo_root=REPO, created_utc="2026-11-22T09:00:00Z")
    assert "output_occupied" in {i["code"] for i in info.value.issues}


def test_12_a_planned_write_outside_the_run_directories_blocks_the_launch(built):
    tmp, plan, req, root, record = built
    bad = copy.deepcopy(plan)
    bad["steps"][3]["writes"] = ["/home/mailf18/e1/elsewhere"]
    bad["plan_sha256"] = be._digest(bad, drop=("plan_sha256", "created_utc"))                         # an edited plan with a valid digest: only the isolation check can catch it
    auth = a0t_auth(bad, record, tmp)
    transport = MockTransport()
    with pytest.raises(be.LaunchRefused) as info:
        run_adapter(tmp, bad, record, transport, auth=auth)
    assert "outside the approved run directories" in str(info.value) and transport.invocations == []


# ---- 13: timeouts
def test_13_a_timeout_stops_only_the_authorized_process_and_is_recorded(built):
    tmp, plan, req, root, record = built
    bystander = subprocess.Popen([sys.executable, "-I", "-c", "import time; time.sleep(60)"])
    transport = FixtureTransport(lambda inv: "import time; time.sleep(60)" if caliper_inv(inv) else "print('ok')")
    try:
        runner = pr.PlanBoundRunner(plan, transport, distro=DISTRO, mount_root=MOUNT_ROOT, max_process_lifetime_s=LIFETIME_S)
        started = time.monotonic()
        result = runner.run(step(plan, "X08_caliper")["argv"], 0.5, None)
        assert result.timed_out is True and time.monotonic() - started < 20
        assert transport.spawned[-1].poll() is not None, "the timed-out process was killed"
        assert bystander.poll() is None, "an unrelated process must not be touched"
    finally:
        bystander.kill()
        bystander.communicate()
        transport.cleanup()


def test_13_a_caliper_timeout_halts_with_status_timed_out_and_preserves_evidence(built):
    tmp, plan, req, root, record = built
    transport = MockTransport(rules=lambda inv: be.RunResult(None, b"partial", b"", True) if caliper_inv(inv) else None)
    status = run_adapter(tmp, plan, record, transport)
    assert status["status"] == "TIMED_OUT" and status["halted_at"] == "X08_caliper" and status["transaction_attempts"] == 1 and status["partial_evidence_preserved"] is True
    out = Path(plan["layout"]["output_dir"])
    assert (out / "process_logs" / "X08_caliper.stdout").read_bytes() == b"partial" and (out / "integrity_manifest.json").is_file()
    assert transport.finished or transport.terminated, "the collector was stopped"
    assert len(transport.matching("launch")) == 1


# ---- 14, 15: interruption and partial evidence
def test_14_an_interrupt_preserves_partial_evidence_terminates_collectors_and_is_reraised(built):
    tmp, plan, req, root, record = built
    transport = MockTransport(raise_on=lambda inv: KeyboardInterrupt() if caliper_inv(inv) else None)
    with pytest.raises(KeyboardInterrupt):
        run_adapter(tmp, plan, record, transport)
    status = status_of(plan)
    assert status["status"] == "INTERRUPTED" and status["partial_evidence_preserved"] is True and transport.terminated
    out = Path(plan["layout"]["output_dir"])
    assert (out / "integrity_manifest.json").is_file() and (out / "00_plan" / "execution_log.jsonl").is_file() and (out / "clock" / "pre_run.json").is_file()
    assert len(transport.matching("launch")) == 1


def test_15_a_failed_gate_before_transactions_keeps_what_was_collected_and_starts_no_caliper(built):
    tmp, plan, req, root, record = built
    wrong = decoded_block(B=10)
    transport = MockTransport()
    status = run_adapter(tmp, plan, record, transport, initial=wrong)
    assert status["status"] == "HALTED_BEFORE_TRANSACTIONS" and status["transaction_attempts"] == 0 and transport.matching("launch") == []
    out = Path(plan["layout"]["output_dir"])
    assert (out / "config_inspection" / "initial" / "evidence.json").is_file() and (out / "integrity_manifest.json").is_file() and (out / "run_status.json").is_file()
    assert not (out / "clock").exists(), "the clock gate comes after the failed configuration gate and never ran"


# ---- 16: no automatic retry
def test_16_a_failed_caliper_step_is_attempted_once_and_never_retried(built):
    tmp, plan, req, root, record = built
    transport = MockTransport(rules=lambda inv: be.RunResult(1, b"", b"boom") if caliper_inv(inv) else None)
    status = run_adapter(tmp, plan, record, transport)
    assert status["status"] == "FAILED_DURING_TRANSACTIONS" and len(transport.matching("launch")) == 1
    assert [e for e in status["runner_log"] if e["event"] == "refused"] == []


def test_16_a_second_launch_or_any_repeat_is_refused_by_the_runner_itself(built):
    transport = MockTransport()
    runner = bound_runner(built, transport)
    tx = step(built[1], "X08_caliper")["argv"]
    runner.run(tx, 100, None)
    with pytest.raises(pr.RunnerRefused) as info:
        runner.run(tx, 100, None)
    assert info.value.code == "second_transaction_attempt"
    mkdir = step(built[1], "X04_run_dir_absent")["argv"]
    runner.run(mkdir, 10, None)
    with pytest.raises(pr.RunnerRefused) as info:
        runner.run(mkdir, 10, None)
    assert info.value.code == "no_retry"
    assert len(transport.matching("launch")) == 1 and len(transport.matching("mkdir")) == 1


def test_16_a_vector_that_is_not_in_the_plan_is_never_run_and_nothing_falls_back(built):
    tmp, plan, req, root, record = built
    transport = MockTransport()
    runner = bound_runner(built, transport)
    tx = list(step(plan, "X08_caliper")["argv"])
    for argv, cwd, code in ((tx + ["--extra"], None, "vector_not_in_plan"), (tx[:-1], None, "vector_not_in_plan"), (["docker", "ps"], None, "vector_not_in_plan"),
                            (["docker", "exec", "caliper", "node", "x.js"], None, "vector_not_in_plan"), (tx, "/tmp", "cwd_not_approved")):
        with pytest.raises(pr.RunnerRefused) as info:
            runner.run(argv, 100, cwd)
        assert info.value.code == code
    assert transport.invocations == []


def test_the_runner_keeps_the_approved_working_directory_and_launch_mode(built):
    tmp, plan, req, root, record = built
    transport = MockTransport()
    runner = bound_runner(built, transport)
    follower = step(plan, "X06_ledger_follower")
    with pytest.raises(pr.RunnerRefused) as info:
        runner.start_background(follower["argv"], None)
    assert info.value.code == "cwd_missing"
    with pytest.raises(pr.RunnerRefused) as info:
        runner.start_background(follower["argv"], "/mnt/c/elsewhere")
    assert info.value.code == "cwd_not_approved"
    with pytest.raises(pr.RunnerRefused) as info:
        runner.run(follower["argv"], 10, follower["cwd"])                    # the follower is background-only
    assert info.value.code == "launch_mode_not_approved"
    handle = runner.start_background(follower["argv"], follower["cwd"])
    assert transport.started[0].wsl_cwd == follower["cwd"] and transport.started[0].os_argv[3:6] == ("--cd", follower["cwd"], "--exec")
    with pytest.raises(pr.RunnerRefused) as info:
        runner.terminate("not-a-handle", 5)
    assert info.value.code == "unknown_handle"
    assert runner.terminate(handle, 5).returncode == -15 and transport.terminated == [handle]
    with pytest.raises(pr.RunnerRefused):
        runner.finish(handle, LIFETIME_S + 1)


def test_inspection_vectors_run_in_the_repository_directory_even_when_the_caller_passes_none(built):
    tmp, plan, req, root, record = built
    transport = MockTransport()
    runner = bound_runner(built, transport)
    vector = plan["config_inspection"]["initial"]["steps"][0]["argv"]
    runner.run(vector, 30.0, None)
    assert transport.invocations[0].wsl_cwd == step(plan, "X02_config_initial")["cwd"]


# ---- 17: the real transport is refused
def test_17_the_real_transport_is_disabled_and_every_method_refuses():
    assert pr.REAL_TRANSPORT_ENABLED is False
    transport = pr.make_transport(pr.TRANSPORT_WSL, synthetic_authorization=False)
    assert isinstance(transport, pr.DisabledWslTransport) and transport.kind == "process"
    for call in (lambda: transport.run(None), lambda: transport.start(None), lambda: transport.finish(1, 1), lambda: transport.terminate(1, 1)):
        with pytest.raises(pr.TransportDisabled):
            call()
    with pytest.raises(pr.RunnerRefused) as info:
        pr.make_transport(pr.TRANSPORT_WSL, synthetic_authorization=True)
    assert info.value.code == "synthetic_authorization_for_real_transport"


def test_17_a_fully_valid_non_synthetic_authorization_still_cannot_start_the_real_transport(built, monkeypatch):
    tmp, plan, req, root, record = built
    for name in ("A0S_REAL_LAUNCH", "A0T_REAL_TRANSPORT", "EXECUTE", "REAL_LAUNCH_ENABLED", "FABRIC_PERF_EXECUTE"):
        monkeypatch.setenv(name, "1")
    auth = a0t_auth(plan, record, tmp, transport=pr.TRANSPORT_WSL, synthetic=False)
    auth["a0t"]["real_transport_authorized"] = True
    description_ = description(plan, record)
    verdict = pr.verify_a0t_authorization(auth, plan, record, description_, repo_root=REPO, allow_synthetic=False, expected_category=pr.OP_SMOKE)
    assert verdict["valid"] is True and verdict["real_transport_permitted"] is False
    transport = pr.make_transport(auth["a0t"]["process_transport"], synthetic_authorization=False)
    exc = refused(built, auth=auth, transport=transport, allow_synthetic=False)
    assert "disabled in this version" in str(exc)


def test_17_a_process_runner_is_refused_by_the_a0s_gate_too(built):
    tmp, plan, req, root, record = built
    runner = pr.PlanBoundRunner(plan, pr.DisabledWslTransport(), distro=DISTRO, mount_root=MOUNT_ROOT, max_process_lifetime_s=LIFETIME_S)
    assert runner.kind == "process"
    clock = FakeClock()
    parts = t.deps_parts(clock)
    with pytest.raises(be.LaunchRefused) as info:
        be.execute_bounded(plan, a0t_auth(plan, record, tmp), be.Deps(runner=runner, **parts), repo_root=REPO, allow_synthetic=True)
    assert "real launch is disabled" in str(info.value)
    assert not Path(plan["layout"]["output_dir"]).exists()


def test_a_valid_synthetic_record_verifies_but_permits_no_real_transport(built):
    tmp, plan, req, root, record = built
    verdict = pr.verify_a0t_authorization(a0t_auth(plan, record, tmp), plan, record, description(plan, record), repo_root=REPO, allow_synthetic=True, expected_category=pr.OP_SMOKE)
    assert verdict["valid"] is True and verdict["synthetic"] is True and verdict["real_transport_permitted"] is False


@pytest.mark.parametrize("field,value,code", [
    ("operation_category", pr.OP_CLOCK_ONLY, "operation_category_mismatch"), ("operation_category", "anything", "operation_category"),
    ("automount_root", "/wslmnt", "automount_root_mismatch"), ("path_mapping_record_sha256", "3" * 64, "path_mapping_record_mismatch"),
    ("invocation_description_sha256", "4" * 64, "invocation_description_mismatch"), ("real_transport_authorized", "yes", "real_transport_authorized"),
    ("schema", "a0t_process_authorization/2", "a0t_schema"),
])
def test_the_a0t_extension_binds_category_root_record_and_intended_vectors(built, field, value, code):
    tmp, plan, req, root, record = built
    auth = a0t_auth(plan, record, tmp)
    auth["a0t"][field] = value
    assert code in finding_codes(refused(built, auth=auth))


def test_a_fixture_transport_label_is_not_operational_without_a_synthetic_authorization(built):
    tmp, plan, req, root, record = built
    auth = a0t_auth(plan, record, tmp, synthetic=False)                        # non-synthetic approval naming the fixture transport
    codes = finding_codes(refused(built, auth=auth, allow_synthetic=False))
    assert "fixture_transport_not_operational" in codes


# ---- end to end with harmless local processes
class Hybrid(FixtureTransport):
    """Harmless local one-liners for every step except the clock probe, which the scripted FakeClock answers (so the A0e verdict can be AGREE)."""

    def __init__(self, clock):
        super().__init__(lambda inv: "import sys; sys.stdout.write('step ok')")
        self.clock = clock

    def run(self, inv):
        if inv.domain == pr.DOMAIN_WINDOWS:
            return t.ok(self.clock.remote())
        return super().run(inv)


def test_end_to_end_with_harmless_local_processes_completes_without_wsl_or_caliper(built):
    tmp, plan, req, root, record = built
    clock = FakeClock()
    transport = Hybrid(clock)
    try:
        status = run_adapter(tmp, plan, record, transport, clock=clock)
    finally:
        transport.cleanup()
    assert status["status"] == "COMPLETED" and status["transaction_attempts"] == 1
    out = Path(plan["layout"]["output_dir"])
    assert (out / "process_logs" / "X08_caliper.stdout").read_bytes() == b"step ok"
    assert (out / "process_logs" / "X06_ledger_follower.stdout").read_bytes() == b"step ok"
    assert len(transport.spawned) >= 4


def test_a_background_collector_is_terminated_without_touching_other_processes(built):
    tmp, plan, req, root, record = built
    bystander = subprocess.Popen([sys.executable, "-I", "-c", "import time; time.sleep(60)"])
    transport = FixtureTransport(lambda inv: "import time; time.sleep(60)")
    try:
        runner = pr.PlanBoundRunner(plan, transport, distro=DISTRO, mount_root=MOUNT_ROOT, max_process_lifetime_s=LIFETIME_S)
        follower = step(plan, "X06_ledger_follower")
        handle = runner.start_background(follower["argv"], follower["cwd"])
        assert runner.finish(handle, 0.3).timed_out is True
        result = runner.terminate(handle, 5)
        assert transport.procs[handle].poll() is not None and result.timed_out is False
        assert bystander.poll() is None
    finally:
        bystander.kill()
        bystander.communicate()
        transport.cleanup()


# ---- dry inspection
def test_dry_inspection_shows_every_vector_and_starts_nothing(built):
    tmp, plan, req, root, record = built
    d = description(plan, record)
    assert d["nothing_started"] is True and d["real_transport_enabled"] is False and d["transaction_producing_vectors"] == 1
    assert d["path_mapping_record_sha256"] == record["record_sha256"]
    by_step = {r["step"]: r for r in d["invocations"]}
    probe = by_step["X03_clock_pre"]
    assert probe["domain"] == "windows_host" and probe["os_argv"] == ["wsl.exe", "-d", DISTRO, "--exec", "date", "+%s%N"] and probe["max_uses"] == 10
    caliper = by_step["X08_caliper"]
    assert caliper["domain"] == "wsl" and caliper["max_uses"] == 1 and caliper["os_argv"][:5] == ["wsl.exe", "-d", DISTRO, "--exec", "docker"] and caliper["approved_cwd"] is None
    assert by_step["X06_ledger_follower"]["launch_mode"] == "background"
    assert by_step["X06_ledger_follower"]["os_argv"][3:6] == ["--cd", t.repo_wsl(), "--exec"]
    assert by_step["initial:peer_version"]["max_uses"] == 2
    json.dumps(d)                                                           # serialisable evidence


def test_a_windows_form_output_path_in_the_plan_is_translated_in_the_vector(tmp_path):
    probe, _req, root = build_a0t_plan(tmp_path)
    tools = dict(_req["tools"])
    tools["output_dir_wsl"] = _req["output_dir"].replace("\\", "/")           # the operator typed the Windows form with forward slashes
    req = copy.deepcopy(_req)
    req["tools"] = tools
    plan = be.build_execution_plan(req, repo_root=REPO, created_utc="2026-11-22T09:00:00Z")
    d = pr.describe_plan_invocations(plan, distro=DISTRO, mount_root=MOUNT_ROOT)
    row = next(r for r in d["invocations"] if r["step"] == "X06_ledger_follower")
    assert row["path_translations"] and row["path_translations"][0][1].startswith("/mnt/c/")
    assert all(not tok[1:3] == ":/" for tok in row["os_argv"])
