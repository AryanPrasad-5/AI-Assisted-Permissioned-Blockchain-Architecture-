"""A0u Workstreams A, B and C: the X13 defect, its WSL-side repair, the addendum, the authorization gate and the fail-closed verdict. Plans come from a real offline A0p render; the worker root is a
TEMPORARY directory standing in for the distribution's file system; no process, WSL, Docker, Caliper or Fabric is started and the real transport stays disabled."""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from tests import a0t_support as t
from tests import a0u_support as u
from tests.a0u_support import WslSimTransport, a0u_auth, make_addendum, run_with_integrity

from fabric_perf.controllers import bounded_execution as be      # noqa: E402
from fabric_perf.controllers import process_runner as pr         # noqa: E402
from fabric_perf.controllers import tree_digest as td            # noqa: E402
from fabric_perf.controllers import wsl_integrity as wi          # noqa: E402

REPO = u.REPO


@pytest.fixture()
def built(tmp_path):
    plan, req, root = u.build_plan(tmp_path)
    record = t.path_record(plan)
    addendum = make_addendum(plan, record)
    wsl = tmp_path / "wsl"
    return tmp_path, plan, record, addendum, wsl


def out_dir(plan) -> Path:
    return Path(plan["layout"]["output_dir"])


def verdict_of(plan) -> dict:
    return json.loads((out_dir(plan) / wi.VERDICT_FILE).read_text(encoding="utf-8"))


def go(built, **kw):
    tmp, plan, record, addendum, wsl = built
    transport = kw.pop("transport", None) or WslSimTransport(wsl, plan, **kw)
    status = run_with_integrity(tmp, plan, record, addendum, transport)
    return status, transport


def caliper_runs(transport) -> list:
    return [i for i in transport.invocations if t.caliper_inv(i)]


# ---- root cause (characterisation of the A0s/A0t behaviour; it does not edit it)
def test_x13_defect_reproduced_the_worker_root_is_hashed_from_windows_and_the_manifest_is_empty_yet_the_run_reads_completed(tmp_path):
    plan, req, root = t.build_a0t_plan(tmp_path)
    assert plan["mounts"]["run_dir_host"].startswith("/home/") and pr.pm.classify(plan["mounts"]["run_dir_host"]) == "wsl_posix"
    assert next(s for s in plan["steps"] if s["step"] == "X13_integrity")["argv"] is None            # no vector: the host hashes it
    status = t.run_adapter(tmp_path, plan, t.path_record(plan), t.MockTransport())
    manifest = json.loads((out_dir(plan) / "integrity_manifest.json").read_text(encoding="utf-8"))
    assert status["status"] == "COMPLETED" and manifest["files"]["worker_evidence_root"] == {}
    assert manifest["files"]["output_dir"] != {}
    assert {p["code"] for p in wi.audit_integrity_manifest(manifest)} == {"worker_evidence_root_empty"}


def test_x13_a_windows_path_check_of_a_wsl_native_directory_cannot_see_it():
    assert pr.pm.classify("/home/mailf18/e1/caliper-ws-benchmark/runs/x") == "wsl_posix"
    with pytest.raises(pr.pm.PathMappingError) as info:
        pr.pm.wsl_to_windows("/home/mailf18/e1/caliper-ws-benchmark/runs/x", mount_root="/mnt")
    assert info.value.code == "wsl_native_path_has_no_drive_equivalent"


# ---- 1, 9: environment assignment, addendum and historical-plan preservation
def test_01_x13_is_assigned_to_the_wsl_distribution_for_the_worker_root_and_to_the_host_for_the_output_directory(built):
    tmp, plan, record, addendum, wsl = built
    assert addendum["environment"]["digest_environment"] == "wsl_distribution" and addendum["environment"]["orchestrator"] == "windows_host"
    assert addendum["supersedes_step"]["reassignment"] == {"worker_evidence_root": "wsl_distribution (structured wsl.exe --exec vector, read-only)", "output_directory": "windows_host (local, fail-closed)"}
    pre, post, host = addendum["steps"]
    assert (pre["phase"], post["phase"], host["kind"]) == ("pre_workload", "post_workload", "integrity_host_digest")
    for step in (pre, post):
        assert step["argv"][:3] == ["python3", wi.DIGEST_TOOL, "digest"] and step["cwd"] == plan["request"]["tools"]["wsl_repo_path"].rstrip("/") and step["class"] == "read_only_observation"
        assert step["request"]["target"] == plan["mounts"]["run_dir_host"] and step["request"]["target"].startswith(step["request"]["approved_root"] + "/")
    inv = {r["step"]: r for r in addendum["invocations"]}
    assert inv["I1_worker_root_pre"]["os_argv"][:5] == ["wsl.exe", "-d", t.DISTRO, "--cd", plan["request"]["tools"]["wsl_repo_path"].rstrip("/")] and "--exec" in inv["I2_worker_root_post"]["os_argv"]
    assert addendum["real_transport_enabled"] is False and addendum["status"] == "PLANNED_NOT_AUTHORIZED_NOT_EXECUTED"


def test_01_the_command_is_structured_inspectable_and_carries_no_shell(built):
    tmp, plan, record, addendum, wsl = built
    for step in addendum["steps"][:2]:
        assert pr.validate_argv(step["argv"]) == step["argv"] and not any(tok in ("sh", "bash", "-c") for tok in step["argv"])
        flags = dict(zip(step["argv"][3::2], step["argv"][4::2]))
        assert flags["--plan-sha256"] == plan["plan_sha256"] and flags["--run-id"] == plan["run_id"] and flags["--variant"] == plan["variant"] and flags["--distro"] == t.DISTRO
        assert flags["--path-record-sha256"] == record["record_sha256"] and flags["--expect"] == "directory" and flags["--operation"] == td.OPERATION


def test_02_no_implicit_distribution_limits_symlink_policy_or_timeout(built):
    tmp, plan, record, addendum, wsl = built
    for override in ({"distro": None}, {"distro": "bad name"}, {"symlink_policy": None}, {"symlink_policy": "follow"}, {"limits": None}, {"limits": {"max_entries": 5}}, {"digest_timeout_s": None},
                     {"digest_timeout_s": 0}, {"digest_timeout_s": True}, {"mount_root": None}):
        with pytest.raises(wi.IntegrityRefused):
            make_addendum(plan, record, **override)


def test_03_no_implicit_container_identity_the_authorization_must_name_the_plans_container(built):
    tmp, plan, record, addendum, wsl = built
    assert "caliper_container" in wi.A0U_OWNER_KEYS and wi.a0u_authorization_extension(addendum)["caliper_container"] is None
    verdict = wi.verify_a0u_authorization(a0u_auth(plan, record, addendum, tmp, caliper_container="caliper-benchmark-001"), plan, addendum, allow_synthetic=True)
    assert {f["code"] for f in verdict["findings"]} == {"caliper_container_mismatch"}
    assert wi.verify_a0u_authorization(a0u_auth(plan, record, addendum, tmp, caliper_container=None), plan, addendum, allow_synthetic=True)["findings"][0]["code"] == "a0u_field_unset"


def test_04_the_wrong_run_variant_plan_or_path_record_is_rejected(built):
    tmp, plan, record, addendum, wsl = built
    assert wi.verify_addendum(addendum, plan, record, repo_root=REPO) == []
    for key, value in (("run_id", "other-run"), ("variant", "other_variant"), ("plan_sha256", "0" * 64), ("path_record_sha256", "0" * 64), ("distro", "Debian")):
        tampered = copy.deepcopy(addendum)
        tampered["binding"][key] = value
        tampered["addendum_sha256"] = be._digest(tampered, drop=("addendum_sha256",))              # a re-hashed forgery still disagrees with the plan
        assert wi.verify_addendum(tampered, plan, record, repo_root=REPO), key
    other_record = copy.deepcopy(record)
    other_record["run_id"] = "other-run"
    with pytest.raises(wi.IntegrityRefused) as info:
        make_addendum(plan, other_record)
    assert info.value.code in ("path_record_invalid", "path_record_for_another_plan")


def test_05_an_edited_addendum_or_a_changed_digest_tool_is_detected(built, tmp_path):
    tmp, plan, record, addendum, wsl = built
    edited = copy.deepcopy(addendum)
    edited["worker_root"]["limits"]["max_entries"] = 10 ** 6
    assert {p["code"] for p in wi.verify_addendum(edited, plan, record, repo_root=REPO)} >= {"addendum_digest"}
    forged = copy.deepcopy(addendum)
    forged["tool"]["script_sha256"] = "0" * 64
    forged["addendum_sha256"] = be._digest(forged, drop=("addendum_sha256",))
    assert {p["code"] for p in wi.verify_addendum(forged, plan, record, repo_root=REPO)} >= {"digest_tool_hash"}


def test_06_the_digest_tool_must_be_in_the_candidate_manifest_the_plan_names(tmp_path):
    plan, req, root = t.build_a0t_plan(tmp_path)                                        # built against the plain A0s tool list: no digest tool in its manifest
    with pytest.raises(wi.IntegrityRefused) as info:
        make_addendum(plan, t.path_record(plan))
    assert info.value.code == "digest_tool_not_in_candidate_manifest"


def test_07_wrong_artifact_hash_missing_approval_and_synthetic_authorization_are_refused(built):
    tmp, plan, record, addendum, wsl = built
    auth = a0u_auth(plan, record, addendum, tmp)
    for mutate, code in ((lambda a: a["artifact_hashes"].__setitem__(next(iter(a["artifact_hashes"])), "0" * 64), "artifact_hash_mismatch"),
                         (lambda a: a.__setitem__("approval", {k: None for k in be.APPROVAL_KEYS}), "approval_unset")):
        bad = copy.deepcopy(auth)
        mutate(bad)
        with pytest.raises(be.LaunchRefused) as info:
            run_with_integrity(tmp, plan, record, addendum, WslSimTransport(wsl, plan), auth=bad)
        assert code in {f["code"] for f in info.value.findings}
    with pytest.raises(be.LaunchRefused):
        run_with_integrity(tmp, plan, record, addendum, WslSimTransport(wsl, plan), allow_synthetic=False)
    assert not out_dir(plan).exists()


def test_08_the_authorization_needs_the_a0u_block_every_owner_field_and_the_addendum_hash(built):
    tmp, plan, record, addendum, wsl = built
    template = {**a0u_auth(plan, record, addendum, tmp), "a0u": wi.a0u_authorization_extension(addendum)}
    codes = {f["code"] for f in wi.verify_a0u_authorization(template, plan, addendum, allow_synthetic=True)["findings"]}
    assert codes >= {"a0u_field_unset", "integrity_environment", "authorization_reference", "real_integrity_transport_authorized", "distro_mismatch", "caliper_container_mismatch"}
    missing = {k: v for k, v in a0u_auth(plan, record, addendum, tmp).items() if k != "a0u"}
    assert wi.verify_a0u_authorization(missing, plan, addendum, allow_synthetic=True)["findings"][0]["code"] == "a0u_extension_missing"
    for key, value in (("integrity_addendum_sha256", "0" * 64), ("worker_root", "/elsewhere"), ("approved_root", "/"), ("digest_limits", {"max_entries": 1, "max_total_bytes": 1, "max_depth": 1}), ("phases", ["pre_workload"])):
        found = wi.verify_a0u_authorization(a0u_auth(plan, record, addendum, tmp, **{key: value}), plan, addendum, allow_synthetic=True)
        assert "a0u_copied_field_mismatch" in {f["code"] for f in found["findings"]}, key
    synthetic_off = wi.verify_a0u_authorization(a0u_auth(plan, record, addendum, tmp), plan, addendum, allow_synthetic=False)
    assert "synthetic_authorization" in {f["code"] for f in synthetic_off["findings"]} and synthetic_off["real_transport_permitted"] is False


# ---- the complete simulated flow
def test_a_complete_simulated_run_digests_before_and_after_and_is_labelled_simulated(built):
    tmp, plan, record, addendum, wsl = built
    status, transport = go(built)
    assert status["status"] == "COMPLETED" and status["transaction_attempts"] == 1
    assert [c["phase"] for c in transport.digest_calls] == ["pre_workload", "post_workload"] and [c["returncode"] for c in transport.digest_calls] == [0, 0]
    order = [("digest", i.plan_argv[argv_phase(i)]) if i.plan_argv[:3] == ("python3", wi.DIGEST_TOOL, "digest") else ("other", i.plan_argv[0]) for i in transport.invocations]
    pre_at = order.index(("digest", "pre_workload"))
    assert [kind for kind, _ in order[:pre_at]].count("digest") == 0 and ("other", "cp") in order[:pre_at] and not any(t.caliper_inv(i) for i in transport.invocations[:pre_at + 1])
    assert order.index(("digest", "post_workload")) > max(n for n, i in enumerate(transport.invocations) if t.caliper_inv(i))
    verdict = verdict_of(plan)
    assert verdict["verdict"] == wi.V_SIM and verdict["evidence_class"] == "SIMULATED" and verdict["gate_satisfied"] is False and verdict["problems"] == []
    assert status["integrity"] == {"verdict": wi.V_SIM, "evidence_class": "SIMULATED", "gate_satisfied": False, "file": wi.VERDICT_FILE} and status["live_completion"] is False
    manifest = json.loads((out_dir(plan) / "integrity_manifest.json").read_text(encoding="utf-8"))
    assert {"benchmark.yaml", "issuelog/r0.jsonl", "recorder/r0.csv"} <= set(manifest["files"]["worker_evidence_root"]) and wi.audit_integrity_manifest(manifest) == []
    pre = json.loads((out_dir(plan) / wi.PRE_FILE).read_text(encoding="utf-8"))
    assert pre["binding"]["plan_sha256"] == plan["plan_sha256"] and td.verify_evidence(pre) == [] and pre["observation"]["platform"] == "linux"
    assert verdict["phases"]["pre_workload"]["document_sha256"] == pre["document_sha256"] and verdict["authorization_reference"] == "SYNTHETIC-TEST-REFERENCE"


def argv_phase(inv) -> int:
    return inv.plan_argv.index("--phase") + 1


def test_the_staged_run_directory_matches_the_authorized_overlay_exactly(built):
    tmp, plan, record, addendum, wsl = built
    status, transport = go(built)
    pre = json.loads((out_dir(plan) / wi.PRE_FILE).read_text(encoding="utf-8"))
    assert td.flat_file_map(pre) == addendum["expected_pre_workload"]["files"] and "modules" in addendum["expected_pre_workload"]["directories"]


# ---- pre-workload failures halt before any transaction
@pytest.mark.parametrize("name", ["tampered", "extra_file", "missing_file", "extra_directory"])
def test_a_staged_directory_that_differs_from_the_authorized_overlay_halts_before_caliper(built, name):
    def after_stage(run_dir: Path):
        if name == "tampered":
            (run_dir / "benchmark.yaml").write_bytes(b"tampered")
        elif name == "extra_file":
            (run_dir / "extra.sh").write_bytes(b"echo")
        elif name == "missing_file":
            (run_dir / "ccp-org1.json").unlink()
        else:
            (run_dir / "unexpected").mkdir()
    status, transport = go(built, after_stage=after_stage)
    assert status["status"] == "HALTED_BEFORE_TRANSACTIONS" and status["transaction_attempts"] == 0 and caliper_runs(transport) == []
    verdict = verdict_of(built[1])
    assert verdict["verdict"] == wi.V_FAILED and verdict["gate_satisfied"] is False and verdict["phases"]["pre_workload"]["ok"] is False
    assert status["live_completion"] is False
    assert "pre_workload_evidence_missing" in {p["code"] for p in verdict["phases"]["post_workload"]["problems"]}      # a post digest is never accepted without a verified pre-workload baseline


def test_a_failed_staging_step_is_not_followed_by_a_pre_workload_digest(built):
    status, transport = go(built, fail_stage=True)
    assert status["status"] == "HALTED_BEFORE_TRANSACTIONS" and transport.digest_calls[0]["phase"] == "post_workload" and caliper_runs(transport) == []
    assert verdict_of(built[1])["verdict"] == wi.V_FAILED


# ---- post-workload failures never produce a verified manifest
def test_a_staged_file_changed_during_the_run_fails_integrity_writes_no_manifest_and_is_not_live_complete(built):
    status, transport = go(built, after_caliper=lambda run_dir: (run_dir / "benchmark.yaml").write_bytes(b"edited during the run"))
    plan = built[1]
    assert status["status"] == "COMPLETED"                                              # the A0s status alone cannot see it: that is why the verdict is the gate
    assert not (out_dir(plan) / "integrity_manifest.json").exists()
    verdict = verdict_of(plan)
    assert verdict["verdict"] == wi.V_FAILED and {p["code"] for p in verdict["problems"]} >= {"staged_entry_changed", "integrity_step_error", "integrity_manifest_missing"}
    assert status["live_completion"] is False and status["integrity"]["verdict"] == wi.V_FAILED


def test_a_removed_staged_file_fails_integrity(built):
    status, transport = go(built, after_caliper=lambda run_dir: (run_dir / "ccp-org1.json").unlink())
    assert "staged_entry_removed" in {p["code"] for p in verdict_of(built[1])["problems"]}


def test_files_added_by_the_workload_are_expected_and_do_not_fail_integrity(built):
    status, transport = go(built, after_caliper=lambda run_dir: (run_dir / "recorder" / "extra.csv").write_bytes(b"more"))
    assert verdict_of(built[1])["verdict"] == wi.V_SIM


@pytest.mark.parametrize("kind", ["distro_env_mismatch", "distro_env_absent", "windows_platform", "garbage", "two_documents", "empty", "refusal_exit_zero", "tool_hash", "other_run", "nonzero_exit", "edited"])
def test_forged_missing_or_refused_digest_output_fails_closed(built, kind):
    tmp, plan, record, addendum, wsl = built

    def hook(inv, text, code):
        doc = json.loads(text) if text.strip().startswith(b"{") else None
        if kind == "garbage":
            return b"not json\n", 0
        if kind == "two_documents":
            return text + text, 0
        if kind == "empty":
            return b"", 0
        if kind == "refusal_exit_zero":
            refused = td.build_evidence({k: doc["binding"][k] for k in td.REQUEST_KEYS}, u.FakeFs({}), observed_distro="Ubuntu", observed_utc=u.NOW, platform="linux")
            return (json.dumps(refused) + "\n").encode(), 0
        if kind == "tool_hash":
            doc["observation"]["tool_sha256"] = "0" * 64
            doc["document_sha256"] = td._document_digest(doc)
            return (json.dumps(doc) + "\n").encode(), 0
        if kind == "other_run":
            other = td.build_evidence({**doc["binding"], "run_id": "other-run"}, u.FakeFs({}), observed_distro="Ubuntu", observed_utc=u.NOW, platform="linux")
            return (json.dumps(other) + "\n").encode(), 0
        if kind == "nonzero_exit":
            return text, 3
        if kind == "edited":
            doc["tree"]["entries"][0]["sha256"] = "0" * 64
            return (json.dumps(doc) + "\n").encode(), 0
        return text, code
    kw = {"distro_env": {"distro_env_mismatch": "Debian", "distro_env_absent": None}.get(kind, "Ubuntu"), "platform": "win32" if kind == "windows_platform" else "linux"}
    transport = WslSimTransport(wsl, plan, digest_hook=hook, **kw)
    status = run_with_integrity(tmp, plan, record, addendum, transport)
    verdict = verdict_of(plan)
    assert verdict["verdict"] == wi.V_FAILED and verdict["gate_satisfied"] is False and verdict["problems"], kind
    assert status["status"] == "HALTED_BEFORE_TRANSACTIONS" and caliper_runs(transport) == [] and status["live_completion"] is False


@pytest.mark.parametrize("edit,code", [("platform", "not_a_linux_process"), ("binding_run", "binding_mismatch"), ("binding_plan", "binding_mismatch"), ("binding_target", "binding_mismatch"),
                                       ("observed_distro", "distro_mismatch"), ("library_hash", "tool_hash_mismatch")])
def test_an_internally_consistent_forgery_is_caught_by_the_orchestrator_side_checks_alone(built, edit, code):
    """The document is edited AND re-hashed, so its own digests verify; only the orchestrator's comparison with what it asked for, the attested platform, the distribution and the authorized tool
    hashes can reject it."""
    tmp, plan, record, addendum, wsl = built

    def hook(inv, text, rc):
        doc = json.loads(text)
        if edit == "platform":
            doc["observation"]["platform"] = "win32"
        elif edit == "binding_run":
            doc["binding"]["run_id"] = "other-run"
        elif edit == "binding_plan":
            doc["binding"]["plan_sha256"] = "0" * 64
        elif edit == "binding_target":
            doc["binding"]["target"] = doc["binding"]["target"] + "x"
        elif edit == "observed_distro":
            doc["observation"]["observed_distro"] = "Debian"
        else:
            doc["observation"]["library_sha256"] = "0" * 64
        doc["document_sha256"] = td._document_digest(doc)
        assert td.verify_evidence(doc) == [], "the forgery must be internally consistent for this test to mean anything"
        return (json.dumps(doc) + "\n").encode(), 0
    transport = WslSimTransport(wsl, plan, digest_hook=hook)
    run_with_integrity(tmp, plan, record, addendum, transport)
    verdict = verdict_of(plan)
    assert verdict["verdict"] == wi.V_FAILED and code in {p["code"] for p in verdict["problems"]} and caliper_runs(transport) == []


def test_a_post_workload_refusal_document_is_never_accepted_as_a_digest(built):
    tmp, plan, record, addendum, wsl = built

    def hook(inv, text, code):
        if "post_workload" in inv.plan_argv:
            return (json.dumps(td.build_evidence(json.loads(text)["binding"], u.FakeFs({}), observed_distro="Ubuntu", observed_utc=u.NOW, platform="linux")) + "\n").encode(), 3
        return text, code
    transport = WslSimTransport(wsl, plan, digest_hook=hook)
    status = run_with_integrity(tmp, plan, record, addendum, transport)
    verdict = verdict_of(plan)
    assert verdict["verdict"] == wi.V_FAILED and "not_digested" in {p["code"] for p in verdict["problems"]} and not (out_dir(plan) / "integrity_manifest.json").exists()


# ---- nothing staged, interruption, dispatch
def test_a_run_halted_before_staging_has_nothing_to_verify_and_is_not_a_pass(built):
    tmp, plan, record, addendum, wsl = built
    wrong = t.decoded_block(B=999)
    parts = t.deps_parts(t.FakeClock(), initial=wrong)
    parts.pop("hash_tree")
    transport = WslSimTransport(wsl, plan)
    status = wi.execute_with_integrity(plan, a0u_auth(plan, record, addendum, tmp), record, addendum, transport, parts, repo_root=REPO, expected_category=pr.OP_SMOKE, allow_synthetic=True)
    verdict = verdict_of(plan)
    assert status["status"] == "HALTED_BEFORE_TRANSACTIONS" and transport.digest_calls == []
    assert verdict["verdict"] == wi.V_NA and verdict["gate_satisfied"] is False and status["live_completion"] is False


def test_an_interruption_still_leaves_a_verdict_and_is_reraised(built):
    tmp, plan, record, addendum, wsl = built
    transport = WslSimTransport(wsl, plan, raise_on=lambda inv: KeyboardInterrupt() if t.caliper_inv(inv) else None)
    with pytest.raises(KeyboardInterrupt):
        run_with_integrity(tmp, plan, record, addendum, transport)
    assert (out_dir(plan) / wi.VERDICT_FILE).is_file() and (out_dir(plan) / wi.PRE_FILE).is_file()


def test_the_hash_tree_dispatch_knows_only_the_two_planned_roots_and_fail_closes_the_output_directory(built, tmp_path):
    tmp, plan, record, addendum, wsl = built
    runner = wi.IntegrityBoundRunner(plan, WslSimTransport(wsl, plan), addendum=addendum, distro=t.DISTRO, mount_root=t.MOUNT_ROOT, max_process_lifetime_s=t.LIFETIME_S)
    dispatch = wi.make_hash_tree(runner, plan)
    with pytest.raises(wi.IntegrityRefused) as info:
        dispatch("/somewhere/else")
    assert info.value.code == "unplanned_root"
    assert dispatch(plan["mounts"]["run_dir_host"]) == {}                                   # nothing staged yet: explicitly nothing to verify, labelled V_NA by the verdict
    with pytest.raises(wi.IntegrityRefused) as info:
        dispatch(str(out_dir(plan)))                                                         # missing output directory: never {}
    assert info.value.code == "output_dir_missing"
    empty = tmp_path / "empty_out"
    empty.mkdir()
    with pytest.raises(wi.IntegrityRefused) as info:
        wi.host_hash_tree(str(empty))
    assert info.value.code == "output_dir_empty"


def test_a_caller_cannot_supply_its_own_hash_tree(built):
    tmp, plan, record, addendum, wsl = built
    with pytest.raises(be.LaunchRefused):
        wi.execute_with_integrity(plan, a0u_auth(plan, record, addendum, tmp), record, addendum, WslSimTransport(wsl, plan), t.deps_parts(t.FakeClock()), repo_root=REPO,
                                  expected_category=pr.OP_SMOKE, allow_synthetic=True)


# ---- mock evidence never satisfies a live gate; the real transport stays disabled
def test_simulated_evidence_never_satisfies_a_live_gate_even_when_it_claims_everything(built):
    tmp, plan, record, addendum, wsl = built
    status, transport = go(built)
    verdict = verdict_of(plan)
    assert transport.kind == "mock" and verdict["evidence_class"] == "SIMULATED" and verdict["gate_satisfied"] is False
    for kind in ("mock", "fixture", None, "unknown"):
        assert wi.evidence_class(kind) == "SIMULATED"
    assert wi.evidence_class("process") == "SIMULATED" and pr.REAL_TRANSPORT_ENABLED is False


def test_the_classification_is_by_transport_kind_not_by_the_documents_own_claims(built, monkeypatch):
    monkeypatch.setattr(pr, "REAL_TRANSPORT_ENABLED", True)                                  # a hypothetical future version
    assert wi.evidence_class("process") == "REAL_WSL_OBSERVATION" and wi.evidence_class("mock") == "SIMULATED"
    status, transport = go(built)                                                            # even then a mock transport's forged-perfect documents stay simulated
    assert verdict_of(built[1])["evidence_class"] == "SIMULATED" and status["live_completion"] is False


def test_the_real_transport_is_refused_and_nothing_is_created(built):
    tmp, plan, record, addendum, wsl = built
    for transport in (pr.DisabledWslTransport(), WslSimTransport(wsl, plan)):
        auth = a0u_auth(plan, record, addendum, tmp)
        if transport.kind == "process":
            with pytest.raises(be.LaunchRefused):
                run_with_integrity(tmp, plan, record, addendum, transport, auth=auth)
            assert not out_dir(plan).exists()
    auth = a0u_auth(plan, record, addendum, tmp)
    auth["a0t"]["process_transport"] = pr.TRANSPORT_WSL
    with pytest.raises(be.LaunchRefused):
        run_with_integrity(tmp, plan, record, addendum, WslSimTransport(wsl, plan), auth=auth)
    assert not out_dir(plan).exists()


def test_the_runner_still_refuses_vectors_outside_the_plan_and_a_second_digest(built):
    tmp, plan, record, addendum, wsl = built
    transport = WslSimTransport(wsl, plan)
    runner = wi.IntegrityBoundRunner(plan, transport, addendum=addendum, distro=t.DISTRO, mount_root=t.MOUNT_ROOT, max_process_lifetime_s=t.LIFETIME_S)
    step = addendum["steps"][0]
    with pytest.raises(pr.RunnerRefused) as info:
        runner.run([*step["argv"][:-1], "999"], 10, step["cwd"])
    assert info.value.code == "vector_not_in_plan"
    runner.run(step["argv"], step["timeout_s"], step["cwd"])
    with pytest.raises(pr.RunnerRefused) as info:
        runner.run(step["argv"], step["timeout_s"], step["cwd"])
    assert info.value.code == "no_retry"


# ---- historical plans and files are not rewritten
def test_building_the_addendum_does_not_modify_the_plan_and_the_a0s_plan_still_verifies(built):
    tmp, plan, record, addendum, wsl = built
    before = json.dumps(plan, sort_keys=True)
    again = make_addendum(plan, record)
    assert json.dumps(plan, sort_keys=True) == before and again == addendum and be.verify_plan(plan, repo_root=REPO) == []
    assert next(s for s in plan["steps"] if s["step"] == "X13_integrity")["kind"] == "gate_integrity"


def test_the_stored_a0t_example_plan_and_its_manifest_are_byte_identical_to_what_a0t_recorded():
    examples = REPO / "docs/research/e1/a0t_example_artifacts_2026-11-22"
    assert hashlib.sha256((REPO / "docs/research/e1/prelive_candidate_manifest_2026-11-22_a0t.json").read_bytes()).hexdigest() == "529e5d5ed35ef7dd0cb74534db3088c08627d09b674ea2bbda23bd3227a013f4"
    stored = json.loads((examples / "synthetic_smoke_plan.json").read_text(encoding="utf-8"))
    assert be._digest(stored, drop=("plan_sha256", "created_utc")) == stored["plan_sha256"] and next(s for s in stored["steps"] if s["step"] == "X13_integrity")["argv"] is None


def test_the_a0t_authorization_flow_is_unchanged_without_the_addendum(tmp_path):
    plan, req, root = t.build_a0t_plan(tmp_path)
    status = t.run_adapter(tmp_path, plan, t.path_record(plan), t.MockTransport())
    assert status["status"] == "COMPLETED" and "integrity" not in status
