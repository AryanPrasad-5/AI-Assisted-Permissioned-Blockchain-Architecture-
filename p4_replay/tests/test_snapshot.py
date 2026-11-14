"""S1 snapshot integration and the 'frozen things stay frozen' checks. Read-only on the real snapshot (ARCHIVE_20261114T113430Z) and the frozen P1b files; skipped when the
snapshot folder is not on this machine. Nothing here re-hashes the 59,000 archived files: that is the S1 verify, which needs WSL and is LIVE_UNTESTED here."""

import dataclasses
import hashlib
import json
from pathlib import Path

import pytest

from p4_replay import REPO_ROOT, driver as dr, preflight as pf, snapshot as sn

p1b = dr.p1b_module()
SNAP = sn.default_snapshot_path()
needs_snapshot = pytest.mark.skipif(not (SNAP / "SHA256SUMS.txt").is_file(), reason="the S1 snapshot folder is not on this machine")
SECOND = r"D:\PLAN_BLOCK18\evidence_archive\ARCHIVE_20261112T213119Z"


# ----------------------------------------------------------------------------------------------- the snapshot is a parameter, and it is the third one
def test_replay_ctx_uses_the_pinned_third_snapshot_and_nothing_else():
    ctx = sn.replay_ctx("S")
    assert ctx.win_snapshot == sn.SNAPSHOT_PATH_WIN and sn.SNAPSHOT_ID in ctx.win_snapshot and ctx.root == dr.posix_ctx("S").root
    sn.assert_ctx_snapshot(ctx)
    for bad in (SECOND, r"D:\PLAN_BLOCK18\evidence_archive\ARCHIVE_20261114T113430Z_copy", "", "<S1 route not chosen>"):
        with pytest.raises(sn.SnapshotError, match="not the pinned"):
            sn.replay_ctx("S", snapshot=bad)
    with pytest.raises(sn.SnapshotError, match="P1b default"):
        sn.replay_ctx("S", snapshot=SECOND)
    with pytest.raises(sn.SnapshotError):
        sn.assert_ctx_snapshot(dataclasses.replace(ctx, win_snapshot=SECOND))


def test_the_frozen_p1b_default_still_names_the_second_snapshot_and_is_not_used():
    assert p1b.Ctx.default().win_snapshot == SECOND                                        # the trap: unchanged, because P1b is frozen
    assert sn.SECOND_SNAPSHOT_ID in SECOND and sn.SNAPSHOT_ID != sn.SECOND_SNAPSHOT_ID
    assert sn.replay_ctx("S").win_snapshot != p1b.Ctx.default().win_snapshot


def test_the_s1_command_is_built_from_the_pinned_snapshot():
    ctx = sn.replay_ctx("S")
    argv = sn.s1_verify_argv(ctx)
    assert argv[2:] == ["verify", "--snapshot", sn.SNAPSHOT_PATH_WIN, "--check-sources"] and argv[1].endswith(r"scripts\research_audit\archive_evidence.py")
    with pytest.raises(sn.SnapshotError):
        sn.s1_verify_argv(dataclasses.replace(ctx, win_snapshot=SECOND))


# ----------------------------------------------------------------------------------------------- identity (read-only)
@needs_snapshot
def test_the_snapshot_folder_is_the_one_the_owner_accepted():
    r = sn.verify_identity()
    assert r["ok"], [k for k, v in r["checks"].items() if not v]
    d = r["detail"]
    assert d["sums_sha256"] == sn.SNAPSHOT_SUMS_SHA256 == "40303028db1f826a3b1db7199b477f729062fe245dbfface17199da0f681faf9"
    assert d["manifest_result"] == "PASS" and d["problem_files"] == 0 and d["group_count"] == 26 and d["git_head"] == sn.SNAPSHOT_GIT_HEAD and d["missing_required_groups"] == []


@needs_snapshot
@pytest.mark.parametrize("kwargs,failed", [({"expected_sums": "0" * 64}, "sums_sha256_equals_owner_recorded"), ({"expected_manifest": "0" * 64}, "manifest_sha256_equals_pinned"),
                                           ({"expected_head": "0" * 40}, "git_head_equals_pinned")])
def test_a_snapshot_that_differs_from_the_pins_fails_its_named_check(kwargs, failed):
    r = sn.verify_identity(**kwargs)
    assert not r["ok"] and [k for k, v in r["checks"].items() if not v] == [failed]


def test_a_folder_that_is_not_the_snapshot_fails_identity(tmp_path):
    fake = tmp_path / "ARCHIVE_FAKE"
    (fake / "00_manifest").mkdir(parents=True)
    (fake / "SHA256SUMS.txt").write_text("x", encoding="utf-8")
    (fake / "00_manifest" / "archive_manifest.json").write_text(json.dumps({"result": "FAIL", "groups": [{"name": "repo_src"}], "totals": {"problem_files": 3}, "git_head": "abc"}), encoding="utf-8")
    r = sn.verify_identity(fake)
    assert not r["ok"] and not any(r["checks"][k] for k in ("folder_name_is_pinned_snapshot", "sums_sha256_equals_owner_recorded", "manifest_result_PASS", "required_groups_present"))
    assert not sn.verify_identity(tmp_path / "missing")["ok"]


# ----------------------------------------------------------------------------------------------- the strict S1 evaluation
def verify_text(events, checked=100, mism=0, miss=0, unl=0, drift=None):
    return "\n".join(events) + f"\nverify: checked={checked} mismatched={mism} missing={miss} unlisted={unl} source_drift={len(events) if drift is None else drift}\n"


def test_s1_passes_with_no_drift_and_fails_on_any_drift_without_an_allowance():
    assert sn.evaluate_s1_replay(verify_text([]))["verdict"] == "PASS"
    assert sn.evaluate_s1_replay(verify_text(["SOURCE_ADDED repo_p4_replay/new.py"]))["verdict"] == "FAIL"        # a file added under p4_replay/ after the snapshot is drift
    for kind in ("SOURCE_CHANGED", "SOURCE_REMOVED"):
        r = sn.evaluate_s1_replay(verify_text([f"{kind} repo_p4_replay/driver.py"]), allowed={"repo_p4_replay/driver.py": "a" * 64})
        assert r["verdict"] == "FAIL"                                                       # a changed or removed file is never allowed, whatever the allowance says


def test_s1_allows_an_added_file_only_while_it_still_has_the_signed_hash(tmp_path):
    (tmp_path / "p4_replay").mkdir()
    f = tmp_path / "p4_replay" / "new.py"
    f.write_text("print(1)\n", encoding="utf-8")
    digest = hashlib.sha256(f.read_bytes()).hexdigest()
    v = verify_text(["SOURCE_ADDED repo_p4_replay/new.py"])
    assert sn.evaluate_s1_replay(v, allowed={"repo_p4_replay/new.py": digest}, repo_root=tmp_path)["verdict"] == "PASS"
    f.write_text("print(2)\n", encoding="utf-8")                                            # edited after the owner signed
    r = sn.evaluate_s1_replay(v, allowed={"repo_p4_replay/new.py": digest}, repo_root=tmp_path)
    assert r["verdict"] == "FAIL" and r["allowed_hash_mismatch"] == ["repo_p4_replay/new.py"]
    unmappable = sn.evaluate_s1_replay(verify_text(["SOURCE_ADDED wsl_e1/x"]), allowed={"wsl_e1/x": digest}, repo_root=tmp_path)
    assert unmappable["verdict"] == "FAIL" and unmappable["allowed_unverifiable"] == ["wsl_e1/x"]    # a path the repo cannot hash is not silently accepted


@pytest.mark.parametrize("text", ["", "no summary here", verify_text([], checked=0), verify_text([], mism=1), verify_text([], miss=1), verify_text([], unl=1),
                                  verify_text(["SOURCE_ADDED a/b"], drift=3)])
def test_s1_fails_on_missing_summary_integrity_errors_or_a_miscount(text):
    assert sn.evaluate_s1_replay(text, allowed={"a/b": "a" * 64})["verdict"] == "FAIL"


def test_signed_additions_parse_and_reject_malformed_input():
    t = sn.signed_additions_text({"repo_p4_replay/b.py": "b" * 64, "repo_p4_replay/a.py": "a" * 64})
    assert t.splitlines()[0].startswith("# PROPOSED") and sn.parse_signed_additions(t) == {"repo_p4_replay/a.py": "a" * 64, "repo_p4_replay/b.py": "b" * 64}
    with pytest.raises(sn.SnapshotError, match="repeated"):
        sn.parse_signed_additions("x " + "a" * 64 + "\nx " + "b" * 64)
    with pytest.raises(sn.SnapshotError, match="not '<path>  <sha256>'"):
        sn.parse_signed_additions("only-a-path")


def test_the_s1_gate_refuses_saved_output_from_another_snapshot_and_a_missing_backend():
    ctx = sn.replay_ctx("S")
    ok_identity = {"ok": True, "checks": {}, "detail": {}}
    clean = verify_text([])
    assert sn.gate_s1_replay(ctx, evidence_text=clean, evidence_snapshot=sn.SNAPSHOT_PATH_WIN, identity=ok_identity).status == "PASS"
    assert sn.gate_s1_replay(ctx, evidence_text=clean, evidence_snapshot=SECOND, identity=ok_identity).status == "FAIL"        # the output never names its snapshot, so the caller must
    assert sn.gate_s1_replay(ctx, evidence_text=clean, evidence_snapshot=None, identity=ok_identity).status == "FAIL"
    assert sn.gate_s1_replay(ctx, None, identity=ok_identity).status == "FAIL"
    assert sn.gate_s1_replay(dataclasses.replace(ctx, win_snapshot=SECOND), evidence_text=clean, evidence_snapshot=SECOND, identity=ok_identity).status == "FAIL"
    assert sn.gate_s1_replay(ctx, evidence_text=clean, evidence_snapshot=sn.SNAPSHOT_PATH_WIN, identity={"ok": False, "checks": {"x": False}, "detail": {}}).status == "FAIL"


# ----------------------------------------------------------------------------------------------- what Phase 2 adds to the snapshot groups
@needs_snapshot
def test_phase_2_added_files_and_changed_none_of_the_snapshot_files():
    d = sn.added_since_snapshot()
    assert d["changed"] == [] and d["removed"] == []                                        # every Phase 1 file is byte-identical to what the snapshot recorded
    assert d["recorded_files"] == 16 and all(k.startswith("repo_p4_replay/") for k in d["added"])
    assert {"repo_p4_replay/series.py", "repo_p4_replay/classify.py", "repo_p4_replay/snapshot.py", "repo_p4_replay/caliper_check.py", "repo_p4_replay/preflight.py",
            "repo_p4_replay/series_approval.py", "repo_p4_replay/keepawake.py"} <= set(d["added"])                         # these ARE S1 SOURCE_ADDED drift until a snapshot or a signed list covers them
    assert not {f"repo_p4_replay/{n}" for n in ("driver.py", "fidelity.py", "offsets.py", "prereg.py", "render.py", "__init__.py", "README.md")} & set(d["added"])


# ----------------------------------------------------------------------------------------------- frozen things stay frozen
def test_the_frozen_p1b_files_equal_their_pinned_hashes():
    for rel, digest in pf.FROZEN_P1B_SHA256.items():
        assert hashlib.sha256((REPO_ROOT / rel).read_bytes()).hexdigest() == digest, rel
    assert pf.FROZEN_P1B_SHA256["scripts/e1/e1_quicklook_p1b.py"] == dr.P1B_DRIVER_SHA256


@pytest.mark.skipif(not Path(r"D:\PLAN_BLOCK18\p1b_source_bundle_2026-11-13\SHA256SUMS.txt").is_file(), reason="the P1b source bundle is not on this machine")
def test_the_pins_equal_the_p1b_source_bundle_record():
    sums = {}
    for line in Path(r"D:\PLAN_BLOCK18\p1b_source_bundle_2026-11-13\SHA256SUMS.txt").read_text(encoding="utf-8").splitlines():
        digest, _, name = line.strip().partition("  ")
        if digest:
            sums[name.strip().lstrip("*")] = digest
    for rel, digest in pf.FROZEN_P1B_SHA256.items():
        assert sums.get(rel, sums.get(Path(rel).name)) == digest, rel


def test_importing_the_frozen_scripts_cannot_write_bytecode_into_their_folders():
    """`scripts/e1/__pycache__` already holds .pyc files from 9-11 Nov (not from Phase 2), so absence cannot be asserted; what is asserted is that this package switches
    bytecode writing off before it imports anything, and the before/after comparison in the Phase 2 report shows no .pyc newer than the session."""
    import sys
    assert sys.dont_write_bytecode is True


def test_an_s1_allowance_must_hash_to_what_the_owner_signed():
    text = sn.signed_additions_text({"repo_p4_replay/a.py": "a" * 64})
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    assert sn.bind_allowance(text, route="SIGNED_DRIFT_UPDATE", evidence_sha256=digest) == {"repo_p4_replay/a.py": "a" * 64}
    assert sn.bind_allowance(text.encode("utf-8"), route="SIGNED_DRIFT_UPDATE", evidence_sha256=digest.upper()) == {"repo_p4_replay/a.py": "a" * 64}
    assert sn.bind_allowance(None, route="THIRD_SNAPSHOT", evidence_sha256=digest) == {}                      # no list: no allowance, whatever the route
    with pytest.raises(sn.SnapshotError, match="not what the owner signed"):
        sn.bind_allowance(text + "\n", route="SIGNED_DRIFT_UPDATE", evidence_sha256=digest)
    with pytest.raises(sn.SnapshotError, match="not SIGNED_DRIFT_UPDATE"):
        sn.bind_allowance(text, route="THIRD_SNAPSHOT", evidence_sha256=digest)
