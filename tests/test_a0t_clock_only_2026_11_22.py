"""A0t workstream E: the clock-only round. The plan is transaction-free; execution here uses a scripted FakeClock and MockTransport only (no wsl.exe, no clock read, no process)."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from tests import a0t_support as t
from tests.a0t_support import FakeClock, MockTransport, MS

from fabric_perf.controllers import bounded_execution as be      # noqa: E402
from fabric_perf.controllers import clock_only_round as co        # noqa: E402
from fabric_perf.controllers import process_runner as pr          # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi       # noqa: E402

CREATED = "2026-11-22T09:00:00Z"


def request(tmp: Path, **over):
    req = {"schema": co.REQUEST_SCHEMA, "round_id": "clk-test-001", "distro": "Ubuntu", "samples_per_phase": 5, "min_valid_samples": 3, "tolerance_ms": 50,        # SYNTHETIC test values
           "max_wall_monotonic_disagreement_ms": 5, "remote_resolution_ns": 1000, "probe_timeout_s": 5, "phase_gap_s": 2, "reference_domain": "windows-host:time_ns",
           "remote_domain": "wsl:Ubuntu:date_ns", "max_process_lifetime_s": 30, "output_dir": str(tmp / "clock_out"), "automount_root": "/mnt"}
    req.update(over)
    return req


def approved(plan, tmp, *, synthetic=True, transport=pr.TRANSPORT_FIXTURE):
    record = tmp / "clock_approval.txt"
    record.write_text("SYNTHETIC approval document for a test; not an approval\n", encoding="utf-8")
    auth = co.authorization_template(plan)
    auth.update(synthetic=synthetic, process_transport=transport, execution_environment=pr.ENV_WINDOWS_ORCHESTRATOR, real_transport_authorized=False)
    auth["approval"] = {"status": "APPROVED_BY_OWNER", "approved_by": "synthetic-test-fixture", "approved_utc": "2026-11-22T09:30:00Z", "record_path": str(record),
                        "record_sha256": pi.sha256_file(record)}
    return auth


def execute(plan, auth, transport, *, clock=None, sleeps=None, allow_synthetic=True):
    clock = clock or getattr(transport, "clock", FakeClock())
    parts = t.deps_parts(clock)
    sleeps = sleeps if sleeps is not None else []
    return co.execute_clock_only(plan, auth, transport, wall_ns=clock.wall, mono_ns=clock.mono, sleep=sleeps.append, now_utc=parts["now_utc"], allow_synthetic=allow_synthetic,
                                 hash_tree=parts["hash_tree"])


@pytest.fixture()
def plan(tmp_path):
    return co.build_clock_only_plan(request(tmp_path), created_utc=CREATED)


def codes(exc):
    return {f["code"] for f in exc.findings}


# ---- the request has no defaults
@pytest.mark.parametrize("key", co.REQUEST_KEYS)
def test_every_request_field_is_required(tmp_path, key):
    req = request(tmp_path)
    del req[key]
    with pytest.raises(be.RequestInvalid) as info:
        co.build_clock_only_plan(req, created_utc=CREATED)
    assert {"missing"} <= {i["code"] for i in info.value.issues} and any(i["field"] == key for i in info.value.issues)


@pytest.mark.parametrize("over", [{"tolerance_ms": 0}, {"tolerance_ms": -1}, {"tolerance_ms": True}, {"samples_per_phase": 2}, {"min_valid_samples": 0}, {"remote_resolution_ns": 1.5},
                                  {"phase_gap_s": 0}, {"probe_timeout_s": "5"}, {"max_process_lifetime_s": 1}, {"remote_domain": "windows-host:time_ns"}, {"distro": "bad distro;rm"},
                                  {"output_dir": "relative/out"}, {"output_dir": r"\\server\share\x"}, {"automount_root": "mnt"}, {"round_id": "a b"}, {"schema": "x"}, {"unknown": 1}])
def test_invalid_request_values_are_refused(tmp_path, over):
    with pytest.raises(be.RequestInvalid):
        co.build_clock_only_plan(request(tmp_path, **over), created_utc=CREATED)


def test_an_occupied_output_directory_is_refused(tmp_path):
    (tmp_path / "clock_out").mkdir()
    with pytest.raises(be.RequestInvalid) as info:
        co.build_clock_only_plan(request(tmp_path), created_utc=CREATED)
    assert "output_occupied" in {i["code"] for i in info.value.issues}


# ---- the plan is transaction-free
def test_the_plan_has_no_transaction_channel_configuration_or_model_step(plan):
    assert plan["transactions_planned"] == 0 and plan["channel_created"] is False and plan["configuration_changed"] is False and plan["model_inference"] is False
    assert plan["real_launch_enabled"] is False and plan["status"] == "PLANNED_NOT_AUTHORIZED_NOT_EXECUTED"
    assert {s["class"] for s in plan["steps"]} <= {"read_only_observation", "offline_internal"}
    vectors = [s["argv"] for s in plan["steps"] if s["argv"]]
    assert vectors == [["wsl.exe", "-d", "Ubuntu", "--exec", "date", "+%s%N"]] * 2
    text = json.dumps(plan["steps"])
    for forbidden in ("docker", "caliper", "peer", "configtxlator", "channel", "invoke", "python3", "mkdir", "cp"):
        assert forbidden not in text.replace("channel_created", ""), forbidden
    assert set(plan["prohibited_operations"]) == set(co.PROHIBITED) and plan["permitted_operations"] == ["clock_probe_wsl_date"]
    assert plan["plan_sha256"] == be._digest(plan, drop=("plan_sha256", "created_utc"))


def test_the_only_vector_the_runner_admits_is_the_probe(plan):
    runner = pr.PlanBoundRunner(plan, MockTransport(), distro="Ubuntu", mount_root="/mnt", max_process_lifetime_s=30, vectors=co.clock_only_vectors(plan))
    for argv in (["docker", "ps"], ["docker", "exec", "caliper", "caliper", "launch", "manager"], ["peer", "channel", "list"], ["wsl.exe", "-d", "Ubuntu", "--exec", "mkdir", "/x"]):
        with pytest.raises(pr.RunnerRefused) as info:
            runner.run(argv, 5, None)
        assert info.value.code == "vector_not_in_plan"
    for _ in range(10):
        runner.run(plan["probe"]["argv"], 5, None)
    with pytest.raises(pr.RunnerRefused) as info:
        runner.run(plan["probe"]["argv"], 5, None)                       # the 11th probe is a retry beyond 2 x samples_per_phase
    assert info.value.code == "no_retry"


# ---- the authorization record
def test_the_template_has_every_owner_field_unset_and_fails_verification(plan):
    tpl = co.authorization_template(plan)
    assert tpl["approval"] == {k: None for k in be.APPROVAL_KEYS} and tpl["process_transport"] is None and tpl["execution_environment"] is None and tpl["real_transport_authorized"] is None
    assert tpl["transactions_authorized"] == 0 and tpl["synthetic"] is False
    verdict = co.verify_authorization(tpl, plan)
    assert verdict["valid"] is False and {"approval_unset", "process_transport", "execution_environment", "real_transport_authorized"} <= {f["code"] for f in verdict["findings"]}


@pytest.mark.parametrize("mutate,code", [
    (lambda a: a.update(plan_sha256="0" * 64), "plan_digest_mismatch"), (lambda a: a.update(operation_category=pr.OP_SMOKE), "operation_category_mismatch"),
    (lambda a: a.update(transactions_authorized=1), "transactions_authorized"), (lambda a: a.update(permitted_operations=["clock_probe_wsl_date", "caliper"]), "permitted_operations"),
    (lambda a: a.update(prohibited_operations=[]), "prohibited_operations"), (lambda a: a["clock"].update(tolerance_ms=1000), "clock_value_mismatch"),
    (lambda a: a["clock"].update(samples_per_phase=99), "clock_value_mismatch"), (lambda a: a.update(output_dir="C:\\elsewhere"), "output_dir_mismatch"),
    (lambda a: a.update(max_process_lifetime_s=999), "max_process_lifetime_s_mismatch"), (lambda a: a.update(process_transport="ssh"), "process_transport"),
    (lambda a: a.update(execution_environment="wsl"), "execution_environment"), (lambda a: a.update(real_transport_authorized=None), "real_transport_authorized"),
    (lambda a: a["approval"].update(status="PENDING"), "approval_status"), (lambda a: a["approval"].update(approved_utc="2026-11-22T09:30:00"), "approval_time"),
])
def test_binding_mismatches_are_refused(plan, tmp_path, mutate, code):
    auth = approved(plan, tmp_path)
    mutate(auth)
    verdict = co.verify_authorization(auth, plan, allow_synthetic=True)
    assert verdict["valid"] is False and code in {f["code"] for f in verdict["findings"]}


def test_an_edited_approval_document_a_missing_one_and_a_synthetic_record_without_the_flag_fail(plan, tmp_path):
    auth = approved(plan, tmp_path)
    assert co.verify_authorization(auth, plan, allow_synthetic=True)["valid"] is True
    assert "synthetic_authorization" in {f["code"] for f in co.verify_authorization(auth, plan, allow_synthetic=False)["findings"]}
    Path(auth["approval"]["record_path"]).write_text("changed\n", encoding="utf-8")
    assert "approval_record_hash" in {f["code"] for f in co.verify_authorization(auth, plan, allow_synthetic=True)["findings"]}
    auth["approval"]["record_path"] = str(tmp_path / "does_not_exist.txt")
    assert "approval_record_unreadable" in {f["code"] for f in co.verify_authorization(auth, plan, allow_synthetic=True)["findings"]}


def test_a_valid_record_permits_no_real_transport(plan, tmp_path):
    verdict = co.verify_authorization(approved(plan, tmp_path), plan, allow_synthetic=True)
    assert verdict["valid"] is True and verdict["real_transport_permitted"] is False and pr.REAL_TRANSPORT_ENABLED is False


# ---- execution through fixtures only
def test_a_pass_round_writes_raw_samples_the_offset_interval_and_a_verdict(plan, tmp_path):
    transport, sleeps = MockTransport(FakeClock(offset_ns=0, rtt_ns=2 * MS)), []
    status = execute(plan, approved(plan, tmp_path), transport, sleeps=sleeps)
    assert status["status"] == "COMPLETED" and status["verdict"] == "AGREE" and status["round_label"] == "PASS" and status["transactions_attempted"] == 0
    assert sleeps == [2.0] and len(transport.invocations) == 10 and all(i.os_argv == tuple(plan["probe"]["argv"]) for i in transport.invocations)
    out = Path(plan["request"]["output_dir"])
    pre = json.loads((out / "clock" / "pre_run.json").read_text(encoding="utf-8"))
    post = json.loads((out / "clock" / "post_run.json").read_text(encoding="utf-8"))
    assert pre["count_collected"] == 5 and post["count_collected"] == 5 and pre["phase"] == "pre_run" and post["phase"] == "post_run"
    assert {"ref_wall_before_ns", "remote_wall_ns", "ref_wall_after_ns", "ref_mono_before_ns", "ref_mono_after_ns", "remote_resolution_ns", "round_trip_ns"} <= set(pre["samples"][0])
    verdict = json.loads((out / "clock" / "clock_verdict.json").read_text(encoding="utf-8"))
    assert verdict["verdict"] == "AGREE" and verdict["round_label"] == "PASS" and set(verdict["phases"]) == {"pre_run", "post_run"}
    assert (out / "integrity_manifest.json").is_file() and (out / "round_status.json").is_file()
    assert sorted(p.name for p in out.iterdir()) == ["clock", "integrity_manifest.json", "round_status.json"]
    assert "lower_ms" in json.dumps(verdict) or "offset" in json.dumps(verdict)


def test_a_large_offset_is_a_fail_and_a_slow_round_trip_is_inconclusive(tmp_path):
    for sub, clock, expected_verdict, label in (("fail", FakeClock(offset_ns=500 * MS), "DISAGREE", "FAIL"), ("slow", FakeClock(rtt_ns=400 * MS), "INCONCLUSIVE", "INCONCLUSIVE")):
        (tmp_path / sub).mkdir()
        plan = co.build_clock_only_plan(request(tmp_path / sub), created_utc=CREATED)
        status = execute(plan, approved(plan, tmp_path / sub), MockTransport(clock), clock=clock)
        assert status["verdict"] == expected_verdict and status["round_label"] == label


def test_the_stored_phases_are_accepted_by_the_unchanged_a0e_evaluator_cli(plan, tmp_path):
    import e1_a0s_clock_probe as cli
    clock = FakeClock()
    execute(plan, approved(plan, tmp_path), MockTransport(clock), clock=clock)
    out = Path(plan["request"]["output_dir"])
    code = cli.main(["evaluate", "--phases", str(out / "clock" / "pre_run.json"), str(out / "clock" / "post_run.json"), "--tolerance-ms", "50", "--min-valid-samples", "3",
                     "--max-wall-monotonic-disagreement-ms", "5"])
    assert code == 0


def test_the_real_transport_and_every_process_kind_are_refused_and_nothing_is_created(plan, tmp_path):
    out = Path(plan["request"]["output_dir"])
    with pytest.raises(be.LaunchRefused):
        execute(plan, approved(plan, tmp_path, synthetic=False, transport=pr.TRANSPORT_WSL), pr.DisabledWslTransport(), allow_synthetic=False)
    with pytest.raises(be.LaunchRefused):
        execute(plan, approved(plan, tmp_path), pr.DisabledWslTransport())                       # a process transport with an otherwise valid synthetic record
    with pytest.raises(be.LaunchRefused):
        execute(plan, co.authorization_template(plan), MockTransport())                           # the template
    assert not out.exists()


def test_an_existing_output_directory_is_never_reused_at_execution_time(plan, tmp_path):
    Path(plan["request"]["output_dir"]).mkdir()
    with pytest.raises(be.LaunchRefused):
        execute(plan, approved(plan, tmp_path), MockTransport())


def test_an_interrupt_preserves_the_partial_evidence_and_is_reraised(plan, tmp_path):
    calls = {"n": 0}

    def boom(inv):
        calls["n"] += 1
        return KeyboardInterrupt() if calls["n"] == 7 else None                                   # during the post-run phase

    with pytest.raises(KeyboardInterrupt):
        execute(plan, approved(plan, tmp_path), MockTransport(FakeClock(), raise_on=boom))
    out = Path(plan["request"]["output_dir"])
    status = json.loads((out / "round_status.json").read_text(encoding="utf-8"))
    assert status["status"] == "INTERRUPTED" and (out / "clock" / "pre_run.json").is_file() and (out / "integrity_manifest.json").is_file()
    assert not (out / "clock" / "clock_verdict.json").exists()


def test_a_failing_probe_is_recorded_as_invalid_samples_not_retried(plan, tmp_path):
    transport = MockTransport(FakeClock(), rules=lambda inv: be.RunResult(1, b"", b"wsl failed"))
    status = execute(plan, approved(plan, tmp_path), transport)
    assert len(transport.invocations) == 10, "one attempt per requested sample; no extra attempt"
    assert status["verdict"] == "INCONCLUSIVE" and status["round_label"] == "INCONCLUSIVE"
    pre = json.loads((Path(plan["request"]["output_dir"]) / "clock" / "pre_run.json").read_text(encoding="utf-8"))
    assert pre["count_collected"] == 0 and len(pre["invalid"]) == 5 and pre["invalid"][0]["code"] == "remote_command_failed"
