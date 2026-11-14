"""Phase 2C, Package A: external pins (no snapshot or source hash inside the archived tree), disjoint launch identifiers, and the two owner-written artifacts. Synthetic files; the one
read of the real third snapshot is read-only and skipped when the folder is absent."""

import dataclasses
import hashlib
import json
import re
from pathlib import Path

import pytest

from p4_replay import REPO_ROOT, driver as dr, launch_approval as la, launch_ids as li, pins as pn, prereg as pr, snapshot as sn
from p4_replay.tests import support_2c as s2
from p4_replay.tests.conftest import ENV

TWO_C = ("clock_evidence", "replay_fidelity", "replay_classify", "gate_r4", "duration_model", "keepawake_v2", "analysis_primary", "pins", "launch_ids", "launch_approval", "launch_series", "live_entry")
PREREG = "ab" * 32


# ----------------------------------------------------------------------------------------------- problem 3: the circularity is resolved by construction
def test_no_phase_2c_module_contains_a_snapshot_id_or_a_64_hex_literal():
    for name in TWO_C:
        text = (REPO_ROOT / "p4_replay" / f"{name}.py").read_text(encoding="utf-8")
        assert not re.search(r"ARCHIVE_\d{8}T\d{6}Z", text), f"{name} names a snapshot"
        assert not re.search(r"(?<![0-9a-fA-F])[0-9a-fA-F]{64}(?![0-9a-fA-F])", text), f"{name} holds a 64-hex literal"


def test_the_third_snapshot_can_be_pinned_from_outside_and_verifies_with_the_same_checks():
    third = sn.default_snapshot_path()
    if not third.is_dir():
        pytest.skip("the third snapshot folder is not on this machine")
    manifest = json.loads((third / "00_manifest" / "archive_manifest.json").read_text(encoding="utf-8"))
    pin = pn.SnapshotPin(sn.SNAPSHOT_ID, sn.SNAPSHOT_PATH_WIN, sn.SNAPSHOT_SUMS_SHA256, sn.SNAPSHOT_MANIFEST_SHA256, sn.SNAPSHOT_GIT_HEAD, sn.SNAPSHOT_GROUP_COUNT,
                         tuple(sn.SNAPSHOT_REQUIRED_GROUPS), manifest["finished_utc"])
    ident = pn.verify_snapshot_identity(pin)
    assert ident["ok"], ident["checks"]


def good_pin_bytes(tmp_path):
    return s2.make_snapshot(tmp_path)


def test_a_pins_file_parses_and_exposes_the_snapshot(tmp_path):
    snap = good_pin_bytes(tmp_path)
    p = pn.parse_pins(snap["pins_bytes"], expected_sha256=snap["pins_sha256"])
    assert p.snapshot.id == s2.SNAP_ID and p.snapshot.group_count == len(s2.GROUPS) and "repo_p4_replay" in p.snapshot.required_groups and p.sha256 == snap["pins_sha256"]


def test_the_pins_file_must_hash_to_the_named_value(tmp_path):
    snap = good_pin_bytes(tmp_path)
    with pytest.raises(pn.PinError, match="hashes to"):
        pn.parse_pins(snap["pins_bytes"], expected_sha256="0" * 64)
    with pytest.raises(pn.PinError, match="hashes to"):
        pn.parse_pins(snap["pins_bytes"] + b" ", expected_sha256=snap["pins_sha256"])


@pytest.mark.parametrize("mutate,fragment", [
    (lambda d: d.update(schema="x/1"), "schema"),
    (lambda d: d.update(extra=1), "schema"),
    (lambda d: d["snapshot"].update(id="ARCHIVE_bad"), "id is not"),
    (lambda d: d["snapshot"].update(path_win=r"D:\x\ARCHIVE_20200101T000000Z"), "path_win"),
    (lambda d: d["snapshot"].update(sums_sha256="AB" * 32), "sums_sha256"),
    (lambda d: d["snapshot"].update(manifest_sha256="short"), "manifest_sha256"),
    (lambda d: d["snapshot"].update(git_head="abc"), "git_head"),
    (lambda d: d["snapshot"].update(group_count=0), "group_count"),
    (lambda d: d["snapshot"].update(group_count=True), "group_count"),
    (lambda d: d["snapshot"].update(required_groups=["repo_src"]), "required_groups"),
    (lambda d: d["snapshot"].update(manifest_finished_utc="yesterday"), "manifest_finished_utc"),
    (lambda d: d["snapshot"].pop("git_head"), "keys must be exactly"),
])
def test_a_malformed_pins_file_is_refused(tmp_path, mutate, fragment):
    snap = good_pin_bytes(tmp_path)
    doc = json.loads(snap["pins_bytes"])
    mutate(doc)
    data = json.dumps(doc).encode()
    with pytest.raises(pn.PinError, match=fragment):
        pn.parse_pins(data, expected_sha256=hashlib.sha256(data).hexdigest())


def test_a_non_json_pins_file_is_refused():
    with pytest.raises(pn.PinError, match="not JSON"):
        pn.parse_pins(b"nope", expected_sha256=hashlib.sha256(b"nope").hexdigest())


def identity(tmp_path, **kw):
    snap = s2.make_snapshot(tmp_path, **kw)
    pin = pn.parse_pins(snap["pins_bytes"], expected_sha256=snap["pins_sha256"]).snapshot
    return pin, snap["root"]


def test_the_snapshot_identity_passes_for_the_pinned_folder(tmp_path):
    pin, root = identity(tmp_path)
    r = pn.verify_snapshot_identity(pin, root)
    assert r["ok"] and all(r["checks"].values()) and len(r["checks"]) == 8


@pytest.mark.parametrize("tamper,failed", [
    (lambda root: (root / "SHA256SUMS.txt").write_text("changed\n"), "sums_sha256_equals_pin"),
    (lambda root: (root / "00_manifest" / "archive_manifest.json").write_text((root / "00_manifest" / "archive_manifest.json").read_text() + " "), "manifest_sha256_equals_pin"),
    (lambda root: None, "folder_name_is_pinned_snapshot"),
])
def test_each_identity_mismatch_fails_its_own_check(tmp_path, tamper, failed):
    pin, root = identity(tmp_path)
    if failed == "folder_name_is_pinned_snapshot":
        root = root.rename(root.parent / "ARCHIVE_20200101T000000Z")
    else:
        tamper(root)
    r = pn.verify_snapshot_identity(pin, root)
    assert r["ok"] is False and r["checks"][failed] is False


@pytest.mark.parametrize("field,value,failed", [("git_head", "d" * 40, "git_head_equals_pin"), ("group_count", 99, "group_count_equals_pin"), ("manifest_finished_utc", "2030-01-01T00:00:00Z", "finish_time_equals_pin"),
                                                  ("required_groups", ("repo_src", "repo_p4_replay", "wsl_e1", "absent_group"), "required_groups_present")])
def test_each_pinned_fact_that_differs_from_the_manifest_fails(tmp_path, field, value, failed):
    pin, root = identity(tmp_path)
    r = pn.verify_snapshot_identity(dataclasses.replace(pin, **{field: value}), root)
    assert r["ok"] is False and r["checks"][failed] is False


def test_a_snapshot_whose_manifest_result_is_not_pass_fails(tmp_path):
    pin, root = identity(tmp_path, result="FAIL")
    r = pn.verify_snapshot_identity(pin, root)
    assert r["ok"] is False and r["checks"]["manifest_result_PASS_no_problem_files"] is False


# ----------------------------------------------------------------------------------------------- the source manifest
def tiny_repo(tmp_path):
    root = tmp_path / "repo"
    (root / "p4_replay" / "tests").mkdir(parents=True)
    (root / "p4_replay" / "a.py").write_text("a = 1\n")
    (root / "p4_replay" / "tests" / "t.py").write_text("t = 1\n")
    (root / "p4_replay" / "__pycache__").mkdir()
    (root / "p4_replay" / "__pycache__" / "a.pyc").write_bytes(b"\0")
    (root / "scripts").mkdir()
    (root / "scripts" / "tool.py").write_text("x\n")
    return root


def manifest_for(root, rels):
    text = pn.build_source_manifest(root, rels)
    data = text.encode()
    return pn.parse_source_manifest(data, expected_sha256=hashlib.sha256(data).hexdigest())


def test_a_manifest_is_exact_changes_additions_and_removals_all_fail(tmp_path):
    root = tiny_repo(tmp_path)
    m = manifest_for(root, ["p4_replay/a.py", "p4_replay/tests/t.py", "scripts/tool.py"])
    assert pn.verify_source_manifest(root, m)["ok"] is True
    (root / "p4_replay" / "a.py").write_text("a = 2\n")
    assert pn.verify_source_manifest(root, m)["mismatched"] == ["p4_replay/a.py"]
    (root / "p4_replay" / "a.py").write_text("a = 1\n")
    (root / "p4_replay" / "new.py").write_text("n = 1\n")
    assert pn.verify_source_manifest(root, m)["unlisted"] == ["p4_replay/new.py"]
    (root / "p4_replay" / "new.py").unlink()
    (root / "scripts" / "tool.py").unlink()
    r = pn.verify_source_manifest(root, m)
    assert r["ok"] is False and r["missing"] == ["scripts/tool.py"]


def test_pycache_is_not_part_of_the_tree_exactly_as_in_the_archive_manifest(tmp_path):
    root = tiny_repo(tmp_path)
    assert pn.tree_files(root) == ["p4_replay/a.py", "p4_replay/tests/t.py"]


def test_a_manifest_refuses_a_missing_file_a_bad_hash_a_bad_line_and_a_repeat(tmp_path):
    root = tiny_repo(tmp_path)
    with pytest.raises(pn.PinError, match="not a file"):
        pn.build_source_manifest(root, ["p4_replay/nope.py"])
    good = pn.build_source_manifest(root, ["p4_replay/a.py"]).encode()
    with pytest.raises(pn.PinError, match="hashes to"):
        pn.parse_source_manifest(good, expected_sha256="0" * 64)
    for bad in (b"not a line\n", good + good, b"\n\n"):
        with pytest.raises(pn.PinError):
            pn.parse_source_manifest(bad, expected_sha256=hashlib.sha256(bad).hexdigest())


def test_the_reviewed_set_covers_the_whole_p4_replay_tree_the_frozen_files_and_the_invoked_tools():
    paths = set(pn.reviewed_source_paths(REPO_ROOT))
    assert set(pn.tree_files(REPO_ROOT)) <= paths
    from p4_replay import preflight as pf
    assert set(pf.FROZEN_P1B_SHA256) <= paths and {f"scripts/e1/{n}" for n in s2.p1b.INVOKED_TOOLS} <= paths
    assert {"p4_replay/live_entry.py", "p4_replay/launch_series.py", "p4_replay/pins.py", "fabric-testbed/caliper/workloads/e1_trace_replay.js"} <= paths


# ----------------------------------------------------------------------------------------------- problem 1: identifiers
def test_the_namespaces_are_disjoint_and_phase_1s_dry_run_id_is_retired():
    assert li.namespaces_disjoint() and li.DRY_RUN_ID == "rp90" and li.MEASURED_RUN_IDS == ("rp01", "rp02", "rp03", "rp04", "rp05", "rp06") and li.RETIRED_RUN_IDS == ("rp00",)
    dry, meas = li.dry_rows()[0], li.measured_rows(7)
    assert dry.run_id not in {r.run_id for r in meas} and dry.channel not in {r.channel for r in meas}
    phase1 = dr.build_matrix(7)
    assert phase1[0].run_id == "rp00" and phase1[0].channel == "e1-rp00-b10-r0", "this is the pair of identifiers that a dry run then a measured series would collide on"


def test_a_dry_launch_and_a_measured_launch_each_validate_and_nothing_else_does():
    li.validate_launch_rows(li.DRY, li.dry_rows())
    li.validate_launch_rows(li.MEASURED, li.measured_rows(7))
    with pytest.raises(li.LaunchIdError):
        li.validate_launch_rows(li.DRY, li.measured_rows(7)[:1])
    with pytest.raises(li.LaunchIdError):
        li.validate_launch_rows(li.MEASURED, li.dry_rows())
    with pytest.raises(li.LaunchIdError, match="unknown launch kind"):
        li.validate_launch_rows("OTHER", li.dry_rows())


@pytest.mark.parametrize("kind", [li.DRY, li.MEASURED])
def test_a_phase_1_matrix_can_never_be_mixed_into_a_launch(kind):
    phase1 = dr.build_matrix(7)
    for rows in (phase1, phase1[:1], phase1[:1] + li.measured_rows(7)):                       # anything containing Phase 1's dry run rp00
        with pytest.raises(li.LaunchIdError):
            li.validate_launch_rows(kind, rows)
    with pytest.raises(li.LaunchIdError, match="retired"):
        li.validate_launch_rows(kind, phase1[:1])
    if kind == li.DRY:
        with pytest.raises(li.LaunchIdError):
            li.validate_launch_rows(kind, phase1[1:])
    else:
        li.validate_launch_rows(kind, phase1[1:])                                              # by design: rp01..rp06 of Phase 1 ARE the measured rows; only rp00 was retired
        assert phase1[1:] == li.measured_rows(7)


def test_each_defect_in_a_row_is_refused():
    base = li.measured_rows(7)
    for mutate in (lambda r: dataclasses.replace(r[0], channel="e1-rp01-b99-r1"), lambda r: dataclasses.replace(r[0], B=20), lambda r: dataclasses.replace(r[0], tau="3s"),
                   lambda r: dataclasses.replace(r[0], window="D_33000"), lambda r: dataclasses.replace(r[0], kind="dry_run"), lambda r: dataclasses.replace(r[0], seq=0),
                   lambda r: dataclasses.replace(r[0], channel="e1-r002")):
        rows = list(base)
        rows[0] = mutate(rows)
        with pytest.raises(li.LaunchIdError):
            li.validate_launch_rows(li.MEASURED, rows)
    with pytest.raises(li.LaunchIdError):
        li.validate_launch_rows(li.MEASURED, base[::-1])
    dupe = list(base)
    dupe[1] = dataclasses.replace(dupe[1], channel=dupe[0].channel)
    with pytest.raises(li.LaunchIdError):
        li.validate_launch_rows(li.MEASURED, dupe)


@pytest.mark.parametrize("change", [dict(window="F_82800"), dict(B=40, channel="e1-rp90-b40-r0"), dict(replicate=1, channel="e1-rp90-b10-r1"), dict(seq=1), dict(kind="measured")])
def test_each_defect_in_the_dry_row_is_refused(change):
    row = dataclasses.replace(li.dry_rows()[0], **change)
    with pytest.raises(li.LaunchIdError):
        li.validate_launch_rows(li.DRY, [row])


def test_a_dry_launch_has_exactly_one_row():
    with pytest.raises(li.LaunchIdError):
        li.validate_launch_rows(li.DRY, li.dry_rows() * 2)
    with pytest.raises(li.LaunchIdError):
        li.validate_launch_rows(li.DRY, [])


def test_the_measured_order_is_the_seeded_r2_order_and_deterministic():
    a, b = li.measured_rows(7), li.measured_rows(7)
    assert a == b and [(x.B, x.replicate) for x in a] == [(i["B"], i["replicate"]) for i in pr.run_order(7)]
    assert any([x.B for x in li.measured_rows(s)] != [x.B for x in a] for s in range(8, 40)), "the seed changes the order"
    for s in range(30):
        bs = [x.B for x in li.measured_rows(s)]
        assert max(len(g) for g in re.findall(r"(1+|4+)", "".join("1" if x == 10 else "4" for x in bs))) <= 2, "never three of one configuration in a row"


def test_a_measured_launch_needs_the_owners_seed_and_the_dry_run_none():
    with pytest.raises(li.LaunchIdError, match="seed"):
        li.rows_for(li.MEASURED, None)
    assert li.rows_for(li.DRY, None)[0].run_id == "rp90"
    with pytest.raises(li.LaunchIdError):
        li.rows_for("OTHER", 1)
    with pytest.raises(ValueError):
        pr.run_order(True)


@pytest.mark.parametrize("launch_id,kind,ok", [("p4r-dry-01", li.DRY, True), ("p4r-meas-12", li.MEASURED, True), ("p4r-dry-01", li.MEASURED, False), ("p4r-meas-01", li.DRY, False),
                                               ("p4r-dry-1", li.DRY, False), ("rp90", li.DRY, False), ("", li.DRY, False), ("p4r-dry-01/../x", li.DRY, False), ("P4R-DRY-01", li.DRY, False)])
def test_the_launch_id_names_its_kind(launch_id, kind, ok):
    if ok:
        assert li.check_launch_id(launch_id, kind) == launch_id
    else:
        with pytest.raises(li.LaunchIdError):
            li.check_launch_id(launch_id, kind)


def test_staging_a_launch_uses_the_same_renderer_and_refuses_an_existing_root(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY)
    assert sorted(p.name for p in (L.staged.stage_dir / "runs").iterdir() if p.is_dir()) == ["rp90"] and L.staged.manifest["launch_kind"] == li.DRY
    assert (L.staged.stage_dir / "replay_matrix.csv").read_bytes() == dr.matrix_csv(L.rows) and dr.verify_stage(L.staged.stage_dir)["ok"]
    with pytest.raises(FileExistsError):
        li.stage_launch(li.DRY, L.rows, dict(ENV), str(tmp_path / "network.json"), s2.processed_dir(tmp_path), L.staged.stage_dir, network_doc=s2.NETWORK_DOC)
    with pytest.raises(li.LaunchIdError):
        li.stage_launch(li.DRY, dr.build_matrix(7)[:1], dict(ENV), str(tmp_path / "network.json"), s2.processed_dir(tmp_path), tmp_path / "other_stage", network_doc=s2.NETWORK_DOC)
    M = s2.prepare_launch(tmp_path, li.MEASURED)
    assert sorted(p.name for p in (M.staged.stage_dir / "runs").iterdir() if p.is_dir()) == list(li.MEASURED_RUN_IDS)
    assert not ({"rp90"} & {p.name for p in (M.staged.stage_dir / "runs").iterdir()}) and not ({"rp01"} & {p.name for p in (L.staged.stage_dir / "runs").iterdir()})


# ----------------------------------------------------------------------------------------------- the series approval
def approval(kind=li.MEASURED, status="APPROVED", **mutate):
    text = la.example_approval_text(kind=kind, thresholds_status=status, prereg_sha256=PREREG)
    for k, v in mutate.items():
        text = re.sub(rf"^{k}=.*$", f"{k}={v}", text, flags=re.M)
    return text.encode()


def check(data, kind=li.MEASURED):
    return la.check_launch_approval(data, expected_kind=kind, prereg_sha256=PREREG)


def test_a_complete_measured_and_a_complete_dry_approval_validate():
    m, d = check(approval()), check(approval(li.DRY, "PROPOSED"), li.DRY)
    assert m.kind == li.MEASURED and m.thresholds.status == "APPROVED" and m.run_order_seed == 7 and m.mapping == "M3_p95ofintervals_1s" and m.sha256 == hashlib.sha256(approval()).hexdigest()
    assert d.kind == li.DRY and d.thresholds.status == "PROPOSED" and d.run_order_seed is None and d.continue_after_limited is False
    assert m.thresholds.n1_lag_p99_ms == 20 and m.thresholds.n3_missed_ms == 500 and m.thresholds.n4_count_tv == 0.02 and m.thresholds.n5_drain_s == 10
    assert check(approval(li.DRY, "APPROVED"), li.DRY).thresholds.status == "APPROVED"


@pytest.mark.parametrize("mutate,fragment", [
    ({"launch_kind": "DRY_RUN"}, "launch_kind"), ({"approved_by": "me"}, "owner"), ({"approved_utc": "today"}, "approved_utc"), ({"prereg_sha256": "cd" * 32}, "prereg_sha256"),
    ({"window": "F_1"}, "only scope"), ({"replicates": "4"}, "only scope"), ({"configs": "10,20"}, "only scope"), ({"schedule": "BO"}, "only scope"),
    ({"law_p95_mapping": "M1_mean_1s"}, "M3"), ({"law_p95_mapping": "M4_p95ofintervals_60s"}, "M3"),
    ({"m3_convention_review": "PENDING"}, "OWNER_REVIEWED"), ({"lp1_mode": "MAGNITUDE_0.6S"}, "DIRECTIONAL_ONLY"), ({"lp4_resolution": "NONE"}, "LP4"),
    ({"thresholds_status": "PROPOSED"}, "APPROVED thresholds"), ({"thresholds_status": "MAYBE"}, "PROPOSED or APPROVED"),
    ({"run_order_seed": "NOT_USED"}, "integer"), ({"run_order_seed": "seven"}, "integer"), ({"run_order_seed": "-1"}, "integer"),
    ({"n1_lag_p99_ms": "0"}, "n1_lag_p99_ms"), ({"n3_missed_ms": "-5"}, "n3_missed_ms"), ({"n4_count_tv": "1.5"}, "n4_count_tv"), ({"n5_drain_s": "x"}, "n5_drain_s"),
    ({"diag_n6_burst_dev": "0"}, "diag_n6_burst_dev"), ({"caliper_timeout_s": "700"}, "cannot fit"), ({"host_max_seconds": "1300"}, "outlive"),
    ({"stop_conditions_sha256": "00" * 32}, "stop_conditions_sha256"), ({"continue_after_limited_measured": "maybe"}, "continue_after_limited_measured"),
])
def test_every_defect_in_a_measured_approval_is_refused(mutate, fragment):
    with pytest.raises(pr.ApprovalRefused, match=fragment):
        check(approval(**mutate))


def test_a_dry_approval_cannot_name_a_seed_and_cannot_be_used_for_the_measured_launch():
    with pytest.raises(pr.ApprovalRefused, match="NOT_USED"):
        check(approval(li.DRY, "PROPOSED", run_order_seed="7"), li.DRY)
    with pytest.raises(pr.ApprovalRefused, match="launch_kind"):
        check(approval(li.DRY, "PROPOSED"), li.MEASURED)


def test_missing_and_unknown_keys_and_the_withdrawn_lp1_threshold_key_are_refused():
    text = approval().decode()
    for key in ("m3_convention_review", "lp1_mode", "n1_lag_p99_ms"):
        with pytest.raises(pr.ApprovalRefused, match="lacks"):
            check(re.sub(rf"^{key}=.*\n", "", text, flags=re.M).encode())
    with pytest.raises(pr.ApprovalRefused, match="unknown keys"):
        check((text + "lp1_min_gap_ms=100\n").encode())
    with pytest.raises(pr.ApprovalRefused, match="unknown keys"):
        check((text + "extra=1\n").encode())
    with pytest.raises(pr.ApprovalRefused, match="repeated"):
        check((text + "window=F_82800\n").encode())


def test_the_stop_conditions_text_covers_every_owner_listed_halt_and_is_hash_bound():
    text = "\n".join(la.STOP_CONDITIONS_V2)
    for needed in ("no retry, no resume and no replacement", "never replaced", "CLOCK_FAILURE", "network", "configuration", "integrity", "reconciled", "preserved in place", "nothing is deleted",
                   "R6", "FAILED_INTEGRITY", "UNDETERMINED_REPLAY"):
        assert needed in text, needed
    assert la.stop_conditions_sha256_v2() == hashlib.sha256((text + "\n").encode("ascii")).hexdigest()


# ----------------------------------------------------------------------------------------------- the entry authorization
def auth(kind=li.MEASURED, launch_id="p4r-meas-01", **kw):
    return la.example_authorization_text(kind=kind, launch_id=launch_id, **({"dry_run_summary_sha256": "e" * 64} if kind == li.MEASURED else {}) | kw).encode()


def test_a_complete_authorization_validates_for_each_kind():
    m = la.check_entry_authorization(auth())
    assert m.kind == li.MEASURED and m.dry_run_summary_sha256 == "e" * 64 and m.s1_route == "SNAPSHOT_EXACT" and m.s1_evidence_sha256 is None
    d = la.check_entry_authorization(auth(li.DRY, "p4r-dry-01"))
    assert d.dry_run_summary_sha256 is None
    s = la.check_entry_authorization(auth(s1_route="SIGNED_DRIFT_UPDATE", s1_evidence_sha256="a" * 64))
    assert s.s1_evidence_sha256 == "a" * 64


@pytest.mark.parametrize("data,fragment", [
    (auth(launch_id="p4r-dry-01"), "belongs to a DRY_RUN"), (auth(li.DRY, "p4r-meas-01"), "belongs to a MEASURED"), (auth(li.DRY, "p4r-dry-01", dry_run_summary_sha256="e" * 64), "no earlier dry run"),
    (auth(dry_run_summary_sha256="NONE"), "must name the SHA-256"), (auth(s1_route="SNAPSHOT_EXACT", s1_evidence_sha256="a" * 64), "NONE"),
    (auth(s1_route="SIGNED_DRIFT_UPDATE"), "signed drift list"), (auth(s1_route="THIRD"), "s1_route"), (auth(pins_sha256="xyz"), "pins_sha256"), (auth(entry_sha256="0" * 63), "entry_sha256"),
    (auth().replace(b"authorized_by=owner", b"authorized_by=agent"), "owner"), (auth().replace(b"2026-11-15T10:00:00Z", b"soon"), "authorized_utc"),
    (auth().replace(b"launch_kind=MEASURED_SERIES", b"launch_kind=BOTH"), "launch_kind"), (auth() + b"x=1\n", "unknown keys"),
])
def test_every_defect_in_an_authorization_is_refused(data, fragment):
    with pytest.raises(pr.ApprovalRefused, match=fragment):
        la.check_entry_authorization(data)


def test_a_missing_authorization_key_is_refused():
    with pytest.raises(pr.ApprovalRefused, match="lacks"):
        la.check_entry_authorization(re.sub(rb"^entry_sha256=.*\n", b"", auth(), flags=re.M))
