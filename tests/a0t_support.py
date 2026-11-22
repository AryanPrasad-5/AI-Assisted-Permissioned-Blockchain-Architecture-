"""Shared fixtures for the A0t tests (2026-11-22). EVERYTHING is SYNTHETIC. The only processes ever started are HARMLESS LOCAL Python one-liners run by ``FixtureTransport`` (never wsl.exe, Docker,
Caliper, Fabric or a network client); ``MockTransport`` starts nothing. No value here is a measurement, a recommended setting or an approval."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

from tests import a0s_support as s
from tests.a0s_support import CELL, CHANNEL, MS, REPO, FakeClock, approved_auth, decoded_block, ok  # noqa: F401

from fabric_perf.controllers import bounded_execution as be      # noqa: E402
from fabric_perf.controllers import path_mapping as pm            # noqa: E402
from fabric_perf.controllers import process_runner as pr          # noqa: E402
from fabric_perf.controllers import timed_variant as tv           # noqa: E402

MOUNT_ROOT = "/mnt"
DISTRO = "Ubuntu"
ENV_JSON = REPO / "fabric-testbed" / "network" / "env-e1-caliper-benchmark.json"
REPO_SOURCE = "fabric-testbed/network/env-e1-caliper-benchmark.json: \"repo\""
WS_SOURCE = "scripts/e1/e1_la2_render.py:233 docker run -v {ws}:{CAL_WS}; fabric-testbed/network/env-e1-caliper-benchmark.json: \"caliper_workspace\""
CONTAINER_MOUNT = pm.ContainerMount(wsl_root="/home/mailf18/e1/caliper-ws-benchmark", container_root="/hyperledger/caliper/workspace", container_name="caliper", source=WS_SOURCE)
LIFETIME_S = 120


def repo_wsl() -> str:
    return json.loads(ENV_JSON.read_text(encoding="utf-8"))["repo"]


class MockTransport:
    """kind 'mock': records every Invocation, starts nothing. Behaviour comes from ``rules`` (Invocation -> RunResult | None); the clock probe answers from a scripted FakeClock."""

    kind = "mock"

    def __init__(self, clock: FakeClock | None = None, rules: Callable[[pr.Invocation], be.RunResult | None] | None = None, *, raise_on: Callable[[pr.Invocation], BaseException | None] | None = None,
                 finish_timed_out: bool = False):
        self.clock = clock or FakeClock()
        self.rules, self.raise_on, self.finish_timed_out = rules, raise_on, finish_timed_out
        self.invocations: list[pr.Invocation] = []
        self.started: list[pr.Invocation] = []
        self.terminated: list[Any] = []
        self.finished: list[Any] = []

    def run(self, inv: pr.Invocation) -> be.RunResult:
        self.invocations.append(inv)
        if self.raise_on is not None:
            exc = self.raise_on(inv)
            if exc is not None:
                raise exc
        if self.rules is not None:
            result = self.rules(inv)
            if result is not None:
                return result
        if inv.domain == pr.DOMAIN_WINDOWS and "date" in inv.os_argv:
            return ok(self.clock.remote())
        return ok(b"")

    def start(self, inv: pr.Invocation) -> int:
        self.invocations.append(inv)
        self.started.append(inv)
        return len(self.started)

    def finish(self, handle: Any, timeout_s: float) -> be.RunResult:
        self.finished.append(handle)
        return be.RunResult(None, b"", b"", self.finish_timed_out)

    def terminate(self, handle: Any, grace_s: float) -> be.RunResult:
        self.terminated.append(handle)
        return be.RunResult(-15, b"", b"terminated")

    def matching(self, *needles: str) -> list[pr.Invocation]:
        return [i for i in self.invocations if all(n in i.os_argv for n in needles)]


class FixtureTransport:
    """kind 'fixture': runs a HARMLESS LOCAL Python one-liner chosen by ``script_for(invocation)`` instead of the invocation's real vector. It never runs wsl.exe or anything else.
    Timeouts kill only the process this transport started."""

    kind = "fixture"

    def __init__(self, script_for: Callable[[pr.Invocation], str]):
        self.script_for = script_for
        self.procs: dict[int, subprocess.Popen] = {}
        self.spawned: list[subprocess.Popen] = []
        self._n = 0

    def _popen(self, inv: pr.Invocation) -> subprocess.Popen:
        code = self.script_for(inv)
        flags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        kwargs: dict[str, Any] = {"creationflags": flags} if flags else {"start_new_session": True}
        proc = subprocess.Popen([sys.executable, "-I", "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
        self.spawned.append(proc)
        return proc

    def run(self, inv: pr.Invocation) -> be.RunResult:
        proc = self._popen(inv)
        try:
            out, err = proc.communicate(timeout=inv.timeout_s)
            return be.RunResult(proc.returncode, out, err, False)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
            return be.RunResult(proc.returncode, out, err, True)

    def start(self, inv: pr.Invocation) -> int:
        self._n += 1
        self.procs[self._n] = self._popen(inv)
        return self._n

    def finish(self, handle: int, timeout_s: float) -> be.RunResult:
        proc = self.procs[handle]
        try:
            out, err = proc.communicate(timeout=timeout_s)
            return be.RunResult(proc.returncode, out, err, False)
        except subprocess.TimeoutExpired:
            return be.RunResult(None, b"", b"", True)

    def terminate(self, handle: int, grace_s: float) -> be.RunResult:
        proc = self.procs[handle]
        proc.terminate()
        try:
            out, err = proc.communicate(timeout=grace_s)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
        return be.RunResult(proc.returncode, out, err, False)

    def cleanup(self) -> None:
        for proc in self.spawned:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()


# ---- plans, records, authorizations
def a0t_tools(plan_probe: dict[str, Any], tmp: Path) -> dict[str, str]:
    out = tmp / "run_out"
    return {"peer": "/opt/fabric/bin/peer", "configtxlator": "/opt/fabric/bin/configtxlator", "orderer": "localhost:7050", "orderer_tls_hostname": "orderer.example.com",
            "orderer_ca": "/opt/ca.pem", "wsl_repo_path": repo_wsl(), "network_description_wsl": pm.windows_to_wsl(plan_probe["layout"]["network_description"], mount_root=MOUNT_ROOT),
            "rendered_dir_wsl": pm.windows_to_wsl(plan_probe["layout"]["launch_authorizing_directory"], mount_root=MOUNT_ROOT),
            "output_dir_wsl": pm.windows_to_wsl(str(out), mount_root=MOUNT_ROOT)}


def build_a0t_plan(tmp: Path, **overrides: Any) -> tuple[dict[str, Any], dict[str, Any], Path]:
    """A plan whose WSL-form tool paths agree with the drive-mount rule (two builds: the first learns the rendered paths, the second uses them)."""
    probe_plan, _req, root = s.build_plan(tmp)
    tools = a0t_tools(probe_plan, tmp)
    req = s.request_for(root, tmp, tools=tools, **overrides)
    return be.build_execution_plan(req, repo_root=REPO, created_utc="2026-11-22T09:00:00Z"), req, root


def path_record(plan: dict[str, Any], *, container_mount: pm.ContainerMount | None = CONTAINER_MOUNT, mount_root: str | None = None) -> dict[str, Any]:
    return pm.build_path_record(plan, repo_root_windows=str(REPO), repo_declared_wsl=repo_wsl(), repo_declaration_source=REPO_SOURCE, mount_root=mount_root,
                                container_mount=container_mount, workspace_declaration_source=WS_SOURCE)


def description(plan: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    return pr.describe_plan_invocations(plan, distro=DISTRO, mount_root=MOUNT_ROOT, path_record=record)


def a0t_auth(plan: dict[str, Any], record: dict[str, Any], tmp: Path, *, category: str = pr.OP_SMOKE, transport: str = pr.TRANSPORT_FIXTURE, synthetic: bool = True,
             lifetime: float = LIFETIME_S) -> dict[str, Any]:
    auth = approved_auth(plan, tmp, synthetic=synthetic)
    ext = pr.a0t_authorization_extension(plan, record, description(plan, record), category=category)
    ext.update(process_transport=transport, execution_environment=pr.ENV_WINDOWS_ORCHESTRATOR, distro=DISTRO, max_process_lifetime_s=lifetime, real_transport_authorized=False)
    auth["a0t"] = ext
    return auth


def deps_parts(clock: FakeClock, *, initial: dict | None = None, final: dict | None = None) -> dict[str, Any]:
    d = s.deps_for(s.MockRunner(clock), clock, initial=initial, final=final)
    blocks = {"initial": initial or decoded_block(), "final": final or decoded_block()}

    def read_file(path: str) -> bytes:                      # the stage comes from the directory segment, not from any substring of a temporary directory name
        stage = "initial" if "/config_inspection/initial/" in path.replace("\\", "/") else "final"
        return json.dumps(blocks[stage]).encode("utf-8")

    return {"now_utc": d.now_utc, "wall_ns": d.wall_ns, "mono_ns": d.mono_ns, "read_file": read_file, "hash_tree": d.hash_tree}


def run_adapter(tmp: Path, plan: dict, record: dict, transport: Any, *, auth: dict | None = None, initial=None, final=None, clock: FakeClock | None = None, allow_synthetic: bool = True,
                expected_category: str = pr.OP_SMOKE) -> dict[str, Any]:
    clock = clock or getattr(transport, "clock", None) or FakeClock()
    return pr.execute_through_adapter(plan, auth or a0t_auth(plan, record, tmp), record, transport, deps_parts(clock, initial=initial, final=final), repo_root=REPO,
                                      expected_category=expected_category, allow_synthetic=allow_synthetic)


def caliper_inv(inv: pr.Invocation) -> bool:
    return "launch" in inv.os_argv and "docker" in inv.os_argv
