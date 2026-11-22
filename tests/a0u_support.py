"""Shared fixtures for the A0u tests (2026-11-22). EVERYTHING is SYNTHETIC. No process, WSL, Docker, Caliper, Fabric, network or clock is touched: ``WslSimTransport`` (kind 'mock') simulates the few
vectors that act on the worker root inside a TEMPORARY directory that stands in for the WSL file system, and runs the real digest-tool code on it; ``FakeFs`` is an in-memory file system for the
cases a real one cannot reproduce portably (permission errors, special files, a file that changes while it is read). Nothing here is a measurement, a setting or an approval."""

from __future__ import annotations

import contextlib
import io
import shutil
import stat
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

from tests import a0t_support as t
from tests.a0s_support import REPO, ok  # noqa: F401

import e1_wsl_tree_digest as tool      # noqa: E402  (scripts/e1 is on sys.path through a0m_support)

from fabric_perf.controllers import bounded_execution as be      # noqa: E402
from fabric_perf.controllers import path_mapping as pm           # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi      # noqa: E402
from fabric_perf.controllers import process_runner as pr         # noqa: E402
from fabric_perf.controllers import tree_digest as td            # noqa: E402
from fabric_perf.controllers import wsl_integrity as wi          # noqa: E402

LIMITS = {"max_entries": 200, "max_total_bytes": 10_000_000, "max_depth": 8}       # synthetic test values, not proposals
DIGEST_TIMEOUT_S = 30.0
NOW = "2026-11-22T10:00:00.000+00:00"
REGULAR, DIRECTORY, LINK, FIFO = stat.S_IFREG | 0o644, stat.S_IFDIR | 0o755, stat.S_IFLNK | 0o777, stat.S_IFIFO | 0o644


# ---- in-memory file system
class _Handle:
    def __init__(self, data: bytes, on_read: Callable[[], None] | None):
        self._data, self._pos, self._on_read = data, 0, on_read

    def read(self, n: int) -> bytes:
        if self._on_read is not None:
            self._on_read()
            self._on_read = None
        chunk = self._data[self._pos:self._pos + n]
        self._pos += len(chunk)
        return chunk

    def close(self) -> None:
        pass


class FakeFs:
    """``nodes``: POSIX path -> ('dir',) | ('file', bytes) | ('link', target) | ('fifo',). Parents are implied."""

    def __init__(self, nodes: dict[str, tuple]):
        self.nodes = dict(nodes)
        self.mtime: dict[str, int] = {}
        self.denied_stat: set[str] = set()
        self.denied_list: set[str] = set()
        self.denied_open: set[str] = set()
        self.on_read: dict[str, Callable[[], None]] = {}
        self.list_script: dict[str, list[list[str]]] = {}
        self.reads: list[str] = []

    def _kind(self, path: str):
        if path in self.nodes:
            return self.nodes[path]
        if any(p.startswith(path.rstrip("/") + "/") for p in self.nodes) or path == "/":
            return ("dir",)
        raise FileNotFoundError(path)

    def lstat(self, path: str):
        if path in self.denied_stat:
            raise PermissionError(path)
        node = self._kind(path)
        mode = {"dir": DIRECTORY, "file": REGULAR, "link": LINK, "fifo": FIFO}[node[0]]
        size = len(node[1]) if node[0] == "file" else 0
        return SimpleNamespace(st_mode=mode, st_size=size, st_mtime_ns=self.mtime.get(path, 1_000), st_ino=hash(path) & 0xFFFF, st_dev=1)

    def listdir(self, path: str):
        if path in self.denied_list:
            raise PermissionError(path)
        if path in self.list_script and self.list_script[path]:
            return list(self.list_script[path].pop(0))
        if self._kind(path)[0] != "dir":
            raise NotADirectoryError(path)
        prefix = path.rstrip("/") + "/"
        return sorted({p[len(prefix):].split("/")[0] for p in self.nodes if p.startswith(prefix)})

    def open_read(self, path: str):
        if path in self.denied_open:
            raise PermissionError(path)
        self.reads.append(path)
        return _Handle(self.nodes[path][1], self.on_read.get(path))

    def readlink(self, path: str) -> str:
        return self.nodes[path][1]


def digest(fs: Any, target: str = "/w/runs/r1", **overrides: Any) -> dict[str, Any]:
    args = {"approved_root": "/w/runs", "expected_type": "directory", "symlink_policy": "refuse", "max_entries": 100, "max_total_bytes": 10_000, "max_depth": 6}
    args.update(overrides)
    return td.digest_tree(fs, target, **args)


def request(**overrides: Any) -> dict[str, Any]:
    base = {"run_id": "r1", "variant": "a0p_recorder_v3_timed_issue_log", "plan_sha256": "a" * 64, "distro": "Ubuntu", "phase": "pre_workload", "operation": td.OPERATION,
            "approved_root": "/w/runs", "target": "/w/runs/r1", "expected_type": "directory", "symlink_policy": "refuse", "max_entries": 100, "max_total_bytes": 10_000, "max_depth": 6,
            "path_record_sha256": "b" * 64}
    base.update(overrides)
    return base


# ---- plans and the addendum
def write_digest_manifest(tmp: Path) -> tuple[Path, str]:
    """A candidate manifest listing the A0s tool files AND the two A0u digest files (status CANDIDATE_NOT_APPROVED)."""
    paths = list(be.REQUIRED_TOOL_FILES) + [wi.DIGEST_TOOL, wi.DIGEST_LIB]
    manifest = pi.build_candidate_manifest(REPO, [pi.FileSpec(p, "synthetic test entry", "test") for p in paths], created_utc="2026-11-22T00:00:00+00:00")
    target = tmp / "a0u_candidate_manifest.json"
    return target, pi.write_new_json(target, manifest)


def build_plan(tmp: Path, **overrides: Any):
    manifest, digest_ = write_digest_manifest(tmp)
    return t.build_a0t_plan(tmp, candidate_manifest={"path": str(manifest), "sha256": digest_}, **overrides)


def make_addendum(plan: dict[str, Any], record: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    args = dict(distro=t.DISTRO, mount_root=t.MOUNT_ROOT, symlink_policy="refuse", limits=dict(LIMITS), digest_timeout_s=DIGEST_TIMEOUT_S, repo_root=REPO)
    args.update(overrides)
    return wi.build_integrity_addendum(plan, record=record, **args)


def a0u_auth(plan: dict[str, Any], record: dict[str, Any], addendum: dict[str, Any], tmp: Path, **ext_overrides: Any) -> dict[str, Any]:
    auth = t.a0t_auth(plan, record, tmp)
    auth["a0u"] = {**wi.a0u_authorization_extension(addendum), "integrity_environment": wi.ENV_WSL_SIDE_INTEGRITY, "distro": t.DISTRO,
                   "caliper_container": plan["mounts"]["caliper_container"], "authorization_reference": "SYNTHETIC-TEST-REFERENCE", "real_integrity_transport_authorized": False}
    auth["a0u"].update(ext_overrides)
    return auth


# ---- a simulated WSL file system and transport
class WslSimTransport(t.MockTransport):
    """kind 'mock'. ``wsl_root`` (a temporary directory) stands in for the distribution's file system. Simulated: ``mkdir`` (no -p), ``cp -r SRC/. DST``, the Caliper step (writes an issue log and
    recorder file into the run directory) and the digest tool (the REAL tool code, on the simulated root, with the attested distribution and platform). Everything else is the MockTransport default."""

    def __init__(self, wsl_root: Path, plan: dict[str, Any], *, clock=None, distro_env: str | None = "Ubuntu", platform: str = "linux", after_stage: Callable[[Path], None] | None = None,
                 after_caliper: Callable[[Path], None] | None = None, digest_hook: Callable[[Any, bytes, int], tuple[bytes, int]] | None = None, fail_stage: bool = False, **kw: Any):
        super().__init__(clock, **kw)
        self.wsl_root, self.plan = Path(wsl_root), plan
        self.distro_env, self.platform = distro_env, platform
        self.after_stage, self.after_caliper, self.digest_hook, self.fail_stage = after_stage, after_caliper, digest_hook, fail_stage
        self.digest_calls: list[dict[str, Any]] = []
        (self.real(plan["mounts"]["host_workspace"]) / "runs").mkdir(parents=True, exist_ok=True)

    def real(self, posix: str) -> Path:
        return self.wsl_root / posix.lstrip("/")

    def run(self, inv: pr.Invocation) -> be.RunResult:
        argv = list(inv.plan_argv)
        run_dir = self.real(self.plan["mounts"]["run_dir_host"])
        if self.raise_on is not None:
            exc = self.raise_on(inv)
            if exc is not None:
                self.invocations.append(inv)
                raise exc
        if argv[:1] == ["mkdir"]:
            self.invocations.append(inv)
            try:
                self.real(argv[1]).mkdir()
            except FileExistsError:
                return be.RunResult(1, b"", b"File exists")
            return ok(b"")
        if argv[:2] == ["cp", "-r"]:
            self.invocations.append(inv)
            if self.fail_stage:
                return be.RunResult(1, b"", b"cp failed")
            source = pm.wsl_to_windows(argv[2][:-2], mount_root=t.MOUNT_ROOT)
            shutil.copytree(source, self.real(argv[3]), dirs_exist_ok=True)
            if self.after_stage:
                self.after_stage(run_dir)
            return ok(b"")
        if argv[:3] == ["python3", wi.DIGEST_TOOL, "digest"]:
            self.invocations.append(inv)
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                code = tool.main(argv[2:], fs=td.RealFs(self.real), env_distro=self.distro_env, platform=self.platform, now_utc=NOW)
            text = buffer.getvalue().encode("utf-8")
            self.digest_calls.append({"phase": argv[argv.index("--phase") + 1], "returncode": code})
            if self.digest_hook:
                text, code = self.digest_hook(inv, text, code)
            return be.RunResult(code, text, b"", False, NOW, NOW)
        if t.caliper_inv(inv):
            self.invocations.append(inv)
            (run_dir / "issuelog").mkdir(exist_ok=True)
            (run_dir / "recorder").mkdir(exist_ok=True)
            (run_dir / "issuelog" / "r0.jsonl").write_bytes(b'{"synthetic": true}\n')
            (run_dir / "recorder" / "r0.csv").write_bytes(b"synthetic\n")
            if self.after_caliper:
                self.after_caliper(run_dir)
            return ok(b"")
        return super().run(inv)


def run_with_integrity(tmp: Path, plan: dict[str, Any], record: dict[str, Any], addendum: dict[str, Any], transport: Any, *, auth: dict[str, Any] | None = None,
                       allow_synthetic: bool = True) -> dict[str, Any]:
    parts = t.deps_parts(getattr(transport, "clock", None) or t.FakeClock())
    parts.pop("hash_tree")
    return wi.execute_with_integrity(plan, auth or a0u_auth(plan, record, addendum, tmp), record, addendum, transport, parts, repo_root=REPO, expected_category=pr.OP_SMOKE,
                                     allow_synthetic=allow_synthetic)
