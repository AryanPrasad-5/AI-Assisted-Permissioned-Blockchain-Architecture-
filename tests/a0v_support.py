"""Shared fixtures for the A0v tests (2026-11-22). EVERYTHING is SYNTHETIC.

The only processes ever started are HARMLESS LOCAL Python one-liners (the running interpreter, ``-I``), through ``WslProcessTransport`` in FIXTURE mode. The ``guard`` fixture replaces
``subprocess.Popen`` with a subclass that raises for ``wsl.exe``, Docker, Caliper, Fabric tools, git, curl and ssh, and replaces ``os.system``; a test that tried to start any of them would fail.
Real-mode transports in these tests receive an injected ``FakeProc`` factory and never start anything.
"""

from __future__ import annotations

import io
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import pytest

from tests import a0t_support as t
from tests import a0u_support as u
from tests.a0s_support import REPO  # noqa: F401

from fabric_perf.controllers import bounded_execution as be      # noqa: E402
from fabric_perf.controllers import clock_only_round as co       # noqa: E402
from fabric_perf.controllers import path_mapping as pm           # noqa: E402,F401
from fabric_perf.controllers import prelive_integrity as pi      # noqa: E402,F401
from fabric_perf.controllers import process_runner as pr         # noqa: E402
from fabric_perf.controllers import runtime_discovery as rd      # noqa: E402
from fabric_perf.controllers import wsl_transport as wt          # noqa: E402

DISTRO = "Ubuntu"
MOUNT = "/mnt"
EXE = "C:\\Windows\\System32\\wsl.exe"
MANIFEST_SHA = "c" * 64                                           # a synthetic candidate-manifest hash used only to test the binding
CREATED = "2026-11-22T09:00:00Z"
FORBIDDEN = frozenset({"wsl.exe", "wsl", "docker", "docker.exe", "caliper", "peer", "configtxlator", "orderer", "git", "git.exe", "curl", "ssh", "kubectl", "cmd.exe", "powershell.exe", "bash", "sh"})


@pytest.fixture()
def guard(monkeypatch):
    """Fail any attempt to start a forbidden program. Records the base names of everything started."""
    started: list[str] = []
    real = subprocess.Popen

    class GuardedPopen(real):                                      # type: ignore[misc, valid-type]
        def __init__(self, args, *a, **k):
            first = args[0] if isinstance(args, (list, tuple)) and args else args
            base = re.split(r"[\\/]", str(first))[-1].lower()
            if base in FORBIDDEN:
                raise AssertionError(f"a test tried to start {base!r}")
            started.append(base)
            super().__init__(args, *a, **k)

    def no_system(*_a, **_k):
        raise AssertionError("os.system is not allowed in tests")

    monkeypatch.setattr(subprocess, "Popen", GuardedPopen)
    monkeypatch.setattr(os, "system", no_system)
    return started


# ---- clock-only plan
def clock_request(tmp: Path, **over: Any) -> dict[str, Any]:
    req = {"schema": co.REQUEST_SCHEMA, "round_id": "clk-test-001", "distro": DISTRO, "samples_per_phase": 3, "min_valid_samples": 2, "tolerance_ms": 5000,       # SYNTHETIC test values, not proposals
           "max_wall_monotonic_disagreement_ms": 5000, "remote_resolution_ns": 1000, "probe_timeout_s": 20, "phase_gap_s": 1, "reference_domain": "windows-host:time_ns",
           "remote_domain": "wsl:Ubuntu:date_ns", "max_process_lifetime_s": 60, "output_dir": str(tmp / "clock_out"), "automount_root": MOUNT}
    req.update(over)
    return req


def clock_plan(tmp: Path, **over: Any) -> dict[str, Any]:
    return co.build_clock_only_plan(clock_request(tmp, **over), created_utc=CREATED)


def clock_allowed(plan: dict[str, Any]) -> list[wt.AllowedVector]:
    return wt.vectors_for_category(wt.OPC_CLOCK, plan, distro=DISTRO, mount_root=MOUNT)


def clock_binding(plan: dict[str, Any]) -> dict[str, Any]:
    return {"round_id": plan["request"]["round_id"], "plan_sha256": plan["plan_sha256"], "operation": wt.OPC_CLOCK}


# ---- invocations and transports
def invocation(v: wt.AllowedVector, **over: Any) -> pr.Invocation:
    fields: dict[str, Any] = dict(step_id=v.step_id, domain="wsl", os_argv=v.os_argv, wsl_cwd=v.wsl_cwd, timeout_s=min(v.timeout_s, 5.0), launch_mode=v.launch_mode, plan_argv=v.os_argv)
    fields.update(over)
    return pr.Invocation(**fields)


def py(code: str) -> list[str]:
    return [sys.executable, "-I", "-c", code]


def local_vector(step: str = "L1", *, timeout_s: float = 5.0, uses: int = 1, mode: str = pr.LAUNCH_FOREGROUND, klass: str = "read_only_observation", cwd: str | None = None) -> wt.AllowedVector:
    """A vector that LOOKS like a wsl.exe vector (so the real validation applies) but is only ever mapped to a harmless local command by a fixture transport."""
    return wt.AllowedVector(step, ("wsl.exe", "-d", DISTRO, "--exec", "fixture_step", step), cwd, float(timeout_s), klass, uses, mode)


def fixture_transport(category: str, allowed: list[wt.AllowedVector], script_for: Callable[[pr.Invocation], str], *, lifetime: float = 30.0, max_active: int | None = None,
                      cap: int = 1 << 20, popen: Callable[..., Any] | None = None) -> wt.WslProcessTransport:
    permit = wt.issue_fixture_permit(category, allowed, executable=sys.executable, max_lifetime_s=lifetime, max_active=max_active, max_capture_bytes=cap, acknowledge_fixture_only=True)
    return wt.WslProcessTransport(permit, command_for=lambda inv: py(script_for(inv)), popen=popen)


class FakeProc:
    """Stands in for ``subprocess.Popen`` in real-mode tests: exits at once with the given bytes. Starts nothing."""

    def __init__(self, stdout: bytes = b"", stderr: bytes = b"", code: int = 0):
        self.stdout, self.stderr = io.BytesIO(stdout), io.BytesIO(stderr)
        self._code, self.returncode, self.pid = code, None, 999_999

    def poll(self):
        return self.returncode

    def wait(self, timeout=None):
        self.returncode = self._code
        return self._code

    def send_signal(self, *_a):
        self.returncode = -15

    def terminate(self):
        self.returncode = -15

    def kill(self):
        self.returncode = -9


class FakePopen:
    """A callable replacing ``Popen``; records every argv and keyword set; the reply comes from ``reply(argv)``."""

    def __init__(self, reply: Callable[[list[str]], FakeProc] | None = None):
        self.reply = reply or (lambda argv: FakeProc(b"", b"", 0))
        self.calls: list[tuple[list[str], dict[str, Any]]] = []

    def __call__(self, argv, **kwargs):
        self.calls.append((list(argv), dict(kwargs)))
        return self.reply(list(argv))


# ---- activation records
def real_record(category: str, allowed: list[wt.AllowedVector], binding: dict[str, Any], tmp: Path, *, manifest_sha: str = MANIFEST_SHA, pin: bool = True, **over: Any) -> dict[str, Any]:
    """A record that LOOKS fully owner-approved (every field set, approval document pinning it). It is SYNTHETIC: it exists to test the verifier, and nothing here is an approval."""
    rec = wt.activation_template(category, allowed, binding)
    rec.update(distro=DISTRO, executable=EXE, max_process_lifetime_s=max(60, max(a.timeout_s for a in allowed)), max_active_processes=1, max_capture_bytes=1 << 20, candidate_manifest_sha256=manifest_sha,
               activation_reference="SYNTHETIC-TEST-REFERENCE", real_spawn_authorized=True)
    if wt.CATEGORY_RULES[category]["transaction_producing"]:
        rec["max_active_processes"] = 2
        rec["transaction_authorization"] = {"authorized": True, "max_transactions": 10, "authorization_reference": "SYNTHETIC-TX-REF"}
    rec.update({k: v for k, v in over.items() if k != "approval"})
    rec["record_sha256"] = be._digest(rec, drop=("record_sha256", "approval"))
    tmp.mkdir(parents=True, exist_ok=True)
    doc = tmp / f"approval_{category}.txt"
    doc.write_text(("SYNTHETIC approval document for a test; not an approval. pins " + rec["record_sha256"] + "\n") if pin else "SYNTHETIC document that pins nothing\n", encoding="utf-8")
    rec["approval"] = {"status": "APPROVED_BY_OWNER", "approved_by": "synthetic-test-fixture", "approved_utc": "2026-11-22T09:30:00Z", "record_path": str(doc), "record_sha256": pi.sha256_file(doc)}
    rec["approval"].update(over.get("approval", {}))
    return rec


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def wall_ns() -> int:
    return time.time_ns()


def build_world(tmp: Path) -> SimpleNamespace:
    """A real offline A0p render turned into the plan, path record, integrity addendum and discovery specification the categories derive their vectors from."""
    plan, _req, _root = u.build_plan(tmp)
    record = t.path_record(plan)
    addendum = u.make_addendum(plan, record)
    spec = rd.discovery_spec(plan, rd.scan_container_declarations(REPO), distro=DISTRO)
    return SimpleNamespace(tmp=tmp, plan=plan, record=record, addendum=addendum, spec=spec)
