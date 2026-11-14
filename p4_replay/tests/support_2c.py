"""Test support for the Phase 2C suite: a fake backend that also answers the keep-awake identity query and `taskkill`, and builders for a complete launch (staged tree, pins,
source manifest, series approval, entry authorization). Nothing here starts a process or a network connection (conftest's guard enforces it). Every seed, hash and time is a TEST VALUE.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

from p4_replay import REPO_ROOT, driver as dr, launch_approval as la, launch_ids as li, launch_series as ls, live_entry as le, pins as pn
from p4_replay.tests import support as sp
from p4_replay.tests.conftest import ENV
from p4_replay.tests.support import FakeSeriesBackend, KEEP_PID, NETWORK_DOC, TEST_SEED, World

p1b = dr.p1b_module()
SNAP_ID = "ARCHIVE_20261120T100000Z"
SNAP_WIN = r"D:\PLAN_BLOCK18\evidence_archive" + "\\" + SNAP_ID
PREREG_TEXT = "TEST preregistration text (hash only)\n"
GROUPS = ["repo_src", "repo_scripts", "repo_tests", "repo_data", "repo_results", "repo_fabric_testbed", "repo_p4_replay", "wsl_e1"]
_SOURCE_MANIFEST: dict = {}


def world(**kw) -> World:
    """A World whose idle Caliper container shows the documented keep-alive process."""
    return World(**{"caliper_top": ["1 sleep infinity"], **kw})


class FakeLaunchBackend(FakeSeriesBackend):
    """FakeSeriesBackend plus the Windows process queries the identity-checked cleanup makes. Knobs mutate one behaviour each."""

    def __init__(self, w: World | None = None):
        super().__init__(w or world())
        self.keep_cmdline = ""
        self.started = False
        self.survives_terminate = False          # the launcher handle dies on terminate, the Windows process does not (interop)
        self.pid_reused = False                  # after start, the pid belongs to another program
        self.query_fails = False
        self.query_garbage = False
        self.taskkill_fails = False
        self.identity_calls = 0

    def spawn(self, argv, env=None, stdout_path=None, stderr_path=None):
        a = [str(x) for x in argv]
        proc = super().spawn(argv, env, stdout_path, stderr_path)
        if a[0] == "powershell.exe":
            self.keep_cmdline = " ".join(a)
            self.started = True
            if self.survives_terminate and self.proc_keep is not None:
                self.proc_keep.on_terminate = None
        return proc

    def _identity_output(self, pid: int) -> bytes:
        if pid != KEEP_PID:
            return b"ABSENT"
        if self.pid_reused and self.started:
            return json.dumps({"Name": "notepad.exe", "CommandLine": "notepad.exe C:\\notes.txt", "Created": "2026-11-15T10:00:05.0000000Z"}).encode()
        if self.keep_alive and not self.keep_killed:
            return json.dumps({"Name": "powershell.exe", "CommandLine": self.keep_cmdline, "Created": "2026-11-15T10:00:01.0000000Z"}).encode()
        return b"ABSENT"

    def run(self, argv, env=None, timeout=None):
        a = [str(x) for x in argv]
        if a[0] == "powershell.exe" and "-Command" in a and "Win32_Process" in a[-1]:
            self.calls.append(a)
            self.identity_calls += 1
            if self.query_fails:
                return p1b.CmdResult(code=1, stdout=b"", stderr=b"query failed")
            if self.query_garbage:
                return p1b.CmdResult(code=0, stdout=b"???", stderr=b"")
            return p1b.CmdResult(code=0, stdout=self._identity_output(int(re.search(r"ProcessId=(\d+)", a[-1]).group(1))), stderr=b"")
        if a[0] == "taskkill.exe":
            self.calls.append(a)
            self.taskkills.append(a)
            if self.taskkill_fails:
                return p1b.CmdResult(code=1, stdout=b"", stderr=b"access denied")
            self.keep_killed = True
            return p1b.CmdResult(code=0, stdout=b"", stderr=b"")
        return super().run(argv, env, timeout)


# ----------------------------------------------------------------------------------------------- pins and source manifest
def make_snapshot(tmp_path: Path, *, snap_id: str = SNAP_ID, finished_utc: str = "2026-11-20T10:30:00.000000Z", sums_text: str = "TEST SHA256SUMS\n", groups=None, result="PASS") -> dict:
    """A synthetic snapshot folder plus the pins file describing it. Returns {root, pins_bytes, pins_sha256, manifest}."""
    root = tmp_path / "snapshots" / snap_id
    (root / "00_manifest").mkdir(parents=True, exist_ok=True)
    (root / "SHA256SUMS.txt").write_text(sums_text, encoding="utf-8")
    names = list(groups or GROUPS)
    manifest = {"result": result, "git_head": "c" * 40, "finished_utc": finished_utc, "groups": [{"name": n} for n in names], "totals": {"problem_files": 0}}
    (root / "00_manifest" / "archive_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    doc = {"schema": pn.PIN_SCHEMA, "created_utc": "2026-11-20T11:00:00Z",
           "snapshot": {"id": snap_id, "path_win": r"D:\PLAN_BLOCK18\evidence_archive" + "\\" + snap_id, "sums_sha256": pn.sha256_file(root / "SHA256SUMS.txt"),
                        "manifest_sha256": pn.sha256_file(root / "00_manifest" / "archive_manifest.json"), "git_head": "c" * 40, "group_count": len(names),
                        "required_groups": names, "manifest_finished_utc": finished_utc}}
    data = json.dumps(doc, indent=1).encode()
    return {"root": root, "pins_bytes": data, "pins_sha256": pn.sha256_bytes(data), "doc": doc}


def source_manifest() -> tuple[bytes, str, dict]:
    """The reviewed source manifest of the real tree, built once per test session (the tests never write into p4_replay)."""
    if not _SOURCE_MANIFEST:
        text = pn.build_source_manifest(REPO_ROOT, pn.reviewed_source_paths(REPO_ROOT))
        data = text.encode("utf-8")
        _SOURCE_MANIFEST.update(data=data, sha=pn.sha256_bytes(data), parsed=pn.parse_source_manifest(data, expected_sha256=pn.sha256_bytes(data)))
    return _SOURCE_MANIFEST["data"], _SOURCE_MANIFEST["sha"], _SOURCE_MANIFEST["parsed"]


def processed_dir(tmp_path: Path) -> Path:
    return tmp_path / "processed" if (tmp_path / "processed").exists() else sp.make_processed(tmp_path)


# ----------------------------------------------------------------------------------------------- a complete launch
@dataclass
class Launch:
    tmp: Path
    kind: str
    launch_id: str
    be: FakeLaunchBackend
    w: World
    ctx: object
    staged: sp.Staged
    rows: list
    approval: la.LaunchApproval
    approval_bytes: bytes
    auth: la.EntryAuthorization
    auth_bytes: bytes
    pins: pn.Pins
    pins_bytes: bytes
    snapshot: dict
    prereg_path: Path
    source_manifest_path: Path
    source_manifest: dict
    session: object
    helpers: dict
    clock_dir: Path
    preflight_dir: Path
    env_doc: dict
    files: dict = field(default_factory=dict)
    dry_summary_path: Path | None = None

    def args(self, **over) -> dict:
        a = dict(mode=ls.MODE_OFFLINE, own_pid=300, clock_dir=self.clock_dir, env_doc=self.env_doc, preflight_dir=self.preflight_dir, helpers=self.helpers, node=False,
                 series_root_prefix=str(self.ctx.root.parent).replace("\\", "/"), snapshot_path=self.snapshot["root"], dry_summary_path=self.dry_summary_path)
        a.update(over)
        return a

    def run(self, **over) -> dict:
        return ls.run_launch(self.session, self.rows, self.staged.stage_dir, self.approval, self.auth, self.pins, self.source_manifest, **self.args(**over))

    def entry_kwargs(self) -> dict:
        return dict(authorization=self.files["authorization"], approval=self.files["approval"], pins=self.files["pins"], source_manifest=self.source_manifest_path,
                    prereg=self.prereg_path, stage_root=self.staged.stage_dir, snapshot_path=self.snapshot["root"], dry_summary=self.dry_summary_path)


def prepare_launch(tmp_path: Path, kind: str, *, w: World | None = None, seed: int = TEST_SEED, thresholds_status: str | None = None, launch_n: int = 1,
                   snap_finished: str = "2026-11-20T10:30:00.000000Z", dry_summary: Path | None = None, s1_route: str = "SNAPSHOT_EXACT", s1_list_text: str | None = None,
                   approval_mutate: dict | None = None, auth_mutate: dict | None = None, snap_id: str = SNAP_ID, tag: str = "") -> Launch:
    """`tag` keeps a second launch's staged tree, artifacts and preflight folder apart from the first's while both share the caliper workspace and the series-root parent."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    w = w or world()
    prefix = "dry" if kind == li.DRY else "meas"
    launch_id = f"p4r-{prefix}-{launch_n:02d}"
    rows = li.rows_for(kind, seed if kind == li.MEASURED else None)
    ctx = dataclasses.replace(sp.make_ctx(tmp_path, launch_id), win_snapshot=r"D:\PLAN_BLOCK18\evidence_archive" + "\\" + snap_id)
    processed = processed_dir(tmp_path)
    stage = tmp_path / f"stage_{prefix}{tag}"
    manifest = li.stage_launch(kind, rows, dict(ENV), str(tmp_path / "network.json"), processed, stage, network_doc=NETWORK_DOC)
    staged = sp.Staged(stage, rows, manifest, li.stage_sums_sha256(stage), seed)
    prereg = tmp_path / "prereg.md"
    prereg.write_text(PREREG_TEXT, encoding="utf-8")
    status = thresholds_status or ("PROPOSED" if kind == li.DRY else "APPROVED")
    atext = la.example_approval_text(kind=kind, thresholds_status=status, prereg_sha256=pn.sha256_file(prereg), seed=seed)
    for key, value in (approval_mutate or {}).items():
        atext = re.sub(rf"^{key}=.*$", f"{key}={value}", atext, flags=re.M)
    approval_bytes = atext.encode("utf-8")
    snap = make_snapshot(tmp_path, snap_id=snap_id, finished_utc=snap_finished)
    sm_bytes, sm_sha, sm = source_manifest()
    sm_path = tmp_path / "source_manifest.txt"
    sm_path.write_bytes(sm_bytes)
    files = {"approval": tmp_path / f"approval_{prefix}{tag}.txt", "pins": tmp_path / f"pins{tag}.json", "authorization": tmp_path / f"authorization_{prefix}{tag}.txt"}
    files["approval"].write_bytes(approval_bytes)
    files["pins"].write_bytes(snap["pins_bytes"])
    hashes = dict(series_approval_sha256=pn.sha256_bytes(approval_bytes), pins_sha256=snap["pins_sha256"], source_manifest_sha256=sm_sha, stage_sums_sha256=staged.sums_sha256,
                  entry_sha256=pn.sha256_file(Path(le.__file__)), s1_route=s1_route)
    if kind == li.MEASURED:
        hashes["dry_run_summary_sha256"] = pn.sha256_file(dry_summary) if dry_summary else "f" * 64
    if s1_list_text is not None:
        hashes["s1_evidence_sha256"] = hashlib.sha256(s1_list_text.encode()).hexdigest()
    auth_text = la.example_authorization_text(kind=kind, launch_id=launch_id, **hashes)
    for key, value in (auth_mutate or {}).items():
        auth_text = re.sub(rf"^{key}=.*$", f"{key}={value}", auth_text, flags=re.M)
    auth_bytes = auth_text.encode("utf-8")
    files["authorization"].write_bytes(auth_bytes)
    be = FakeLaunchBackend(w)
    return Launch(tmp_path, kind, launch_id, be, w, ctx, staged, rows, la.check_launch_approval(approval_bytes, expected_kind=kind, prereg_sha256=pn.sha256_file(prereg)), approval_bytes,
                  la.check_entry_authorization(auth_bytes), auth_bytes, pn.parse_pins(snap["pins_bytes"], expected_sha256=snap["pins_sha256"]), snap["pins_bytes"], snap, prereg, sm_path, sm,
                  p1b.Session(ctx=ctx, be=be, thr=dr.replay_thresholds()), sp.helpers_for(be, w, ctx, staged), sp.write_clock_dir(tmp_path), tmp_path / f"preflight_{prefix}{tag}", dict(ENV),
                  files, dry_summary)


def tree_digest(root: Path) -> dict[str, str]:
    """{relative path: sha256} of every file under root: to prove that evidence is untouched."""
    root = Path(root)
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(root.rglob("*")) if p.is_file()} if root.exists() else {}


def cmd_text(be) -> list[str]:
    return [" ".join(c) for c in be.calls]
