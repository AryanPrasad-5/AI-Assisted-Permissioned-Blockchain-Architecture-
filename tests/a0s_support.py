"""Shared fixtures for the A0s tests (2026-11-22). EVERYTHING is SYNTHETIC: a real offline A0p render into a temporary directory, a hand-built decoded config block,
scripted clocks and a mock runner. Nothing here is a measurement, nothing starts a process, Caliper, Fabric, Docker, WSL or a network connection, and no value is a
recommended or approved setting."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Callable

from tests import a0m_support as m
from tests.a0m_support import CHANNEL, REPO  # noqa: F401

import e1_a0p_launch_variant as v   # noqa: E402  (scripts/e1 is on sys.path through a0m_support)

from fabric_perf.controllers import bounded_execution as be      # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi       # noqa: E402
from fabric_perf.controllers import timed_variant as tv          # noqa: E402

CELL = {"max_message_count": 40, "batch_timeout_ms": 2000, "preferred_max_bytes": 10485760, "absolute_max_bytes": 103809024, "orderers": 3}
T0 = 1_795_000_000_000_000_000                      # a 19-digit epoch-ns instant (scripted)
MS = 1_000_000


def decoded_block(channel: str = CHANNEL, *, B: int = 40, timeout: str = "2s", preferred: int = 10485760, absolute: int = 103809024, consenters: int = 3,
                  sequence: Any = 0, number: int = 0, header_type: int = 1) -> dict[str, Any]:
    """A decoded CONFIG block in the shape ``configtxlator proto_decode`` prints (only the fields the parser reads)."""
    config = {"sequence": sequence, "channel_group": {"groups": {"Orderer": {"values": {
        "BatchSize": {"value": {"max_message_count": B, "absolute_max_bytes": absolute, "preferred_max_bytes": preferred}},
        "BatchTimeout": {"value": {"timeout": timeout}},
        "ConsensusType": {"value": {"metadata": {"consenters": [{"host": f"orderer{i}"} for i in range(consenters)]}}}}}}}}
    return {"header": {"number": number}, "data": {"data": [{"payload": {"header": {"channel_header": {"type": header_type, "channel_id": channel}}, "data": {"config": config}}}]}}


class FakeClock:
    """Scripted Windows wall and monotonic clocks plus a WSL remote reading. Each call advances time; nothing reads a real clock."""

    def __init__(self, *, offset_ns: int = 0, step_ns: int = 500_000, rtt_ns: int = 2 * MS):
        self.t = T0
        self.offset, self.step, self.rtt = offset_ns, step_ns, rtt_ns

    def wall(self) -> int:
        self.t += self.step
        return self.t

    def mono(self) -> int:
        self.t += self.step
        return self.t - T0 + 10 ** 12                # a different origin: the two clocks share no zero

    def remote(self) -> bytes:
        self.t += self.rtt // 2
        reading = self.t + self.offset
        self.t += self.rtt // 2
        return f"{reading}\n".encode("ascii")


def ok(stdout: bytes = b"", stderr: bytes = b"") -> be.RunResult:
    return be.RunResult(0, stdout, stderr, False, "2026-11-22T10:00:00.000+00:00", "2026-11-22T10:00:01.000+00:00")


class MockRunner:
    """kind 'mock'. ``run(argv, timeout, cwd)`` records the call; behaviour comes from ``rules`` (a function argv -> RunResult | None) or defaults to success.
    The clock probe command returns a remote reading from the shared FakeClock."""

    kind = "mock"

    def __init__(self, clock: FakeClock, rules: Callable[[list[str]], be.RunResult | None] | None = None, *, raise_on: Callable[[list[str]], BaseException | None] | None = None,
                 finish_timed_out: bool = False):
        self.clock, self.rules, self.raise_on, self.finish_timed_out = clock, rules, raise_on, finish_timed_out
        self.calls: list[tuple[list[str], float, Any]] = []
        self.background: list[list[str]] = []
        self.terminated: list[int] = []
        self.finished: list[int] = []

    def run(self, argv, timeout_s, cwd):
        argv = list(argv)
        self.calls.append((argv, timeout_s, cwd))
        if self.raise_on is not None:
            exc = self.raise_on(argv)
            if exc is not None:
                raise exc
        if self.rules is not None:
            result = self.rules(argv)
            if result is not None:
                return result
        if argv[:1] == ["wsl.exe"]:
            return ok(self.clock.remote())
        return ok(b"")

    def start_background(self, argv, cwd):
        self.background.append(list(argv))
        return len(self.background)

    def finish(self, handle, timeout_s):
        self.finished.append(handle)
        return be.RunResult(None, b"", b"", self.finish_timed_out)

    def terminate(self, handle, grace_s):
        self.terminated.append(handle)
        return be.RunResult(-15, b"", b"terminated")

    def calls_matching(self, *needles: str) -> list[list[str]]:
        return [argv for argv, _, _ in self.calls if all(n in argv for n in needles)]


class ProcessRunner(MockRunner):
    kind = "process"


def render_a0p(tmp: Path, render_id: str = "s1") -> Path:
    """One real offline A0p render. Returns the rendered root (``.../a0p-v3ilt/<render_id>``)."""
    p1, p2 = m.write(tmp / "a0e_params.json", m.a0e_params()), m.write(tmp / "a0m_params.json", m.a0m_params())
    return Path(v.render(p1, p2, tmp / "out", render_id, variant=tv.VARIANT_A0P, channel=CHANNEL)["root"])


def write_manifest(tmp: Path, *, prometheus: bool = False) -> tuple[Path, str]:
    """A small candidate manifest that lists exactly the tool files the plan uses, at their current hashes (status CANDIDATE_NOT_APPROVED)."""
    paths = list(be.REQUIRED_TOOL_FILES) + (list(be.OPTIONAL_TOOL_FILES["before_after"]) if prometheus else [])
    manifest = pi.build_candidate_manifest(REPO, [pi.FileSpec(p, "synthetic test entry", "test") for p in paths], created_utc="2026-11-22T00:00:00+00:00")
    target = tmp / f"candidate_manifest_{len(list(tmp.glob('candidate_manifest_*.json')))}.json"        # one new file per call: nothing is overwritten
    digest = pi.write_new_json(target, manifest)
    return target, digest


def request_for(root: Path, tmp: Path, *, prometheus: dict | None = None, **overrides: Any) -> dict[str, Any]:
    manifest, digest = write_manifest(tmp, prometheus=bool(prometheus and prometheus.get("mode") == "before_after"))
    run_id = tv.qualified_run_id(m.RUN_ID)
    out = tmp / "run_out"
    req = {
        "schema": be.REQUEST_SCHEMA, "candidate_manifest": {"path": str(manifest), "sha256": digest}, "variant": tv.VARIANT_A0P, "channel": CHANNEL, "cell": dict(CELL),
        "cell_id": "b40-t2000ms-p10485760-a103809024-o3", "run_id": m.RUN_ID, "workload": dict(be.SELECTED_WORKLOAD), "recorder_version": be.SELECTED_RECORDER, "worker_count": 1,
        "rounds": [{"label": f"wu-{run_id}", "role": "warmup", "tx_duration_s": 30, "rate_tps": 4.0}, {"label": f"m-{run_id}", "role": "measured", "tx_duration_s": 60, "rate_tps": 4.0}],
        "limits": {"max_rate_tps": 5, "max_round_duration_s": 60, "max_total_duration_s": 120, "max_transactions": 400, "caliper_timeout_s": 100, "terminate_grace_s": 5, "collector_wait_s": 10},
        "operator_timing": {"tick_interval_ms": 1000, "fsync_policy": "tick", "clock_domain": "caliper-worker-host:node_date_now", "heartbeat_interval_ms": 1000, "checkpoint_every_heartbeats": 60},
        "clock": {"tolerance_ms": 50, "min_valid_samples": 3, "max_wall_monotonic_disagreement_ms": 5, "samples_per_phase": 5, "distro": "Ubuntu", "remote_resolution_ns": 1000,
                  "probe_timeout_s": 5, "reference_domain": "windows-host:time_ns", "remote_domain": "wsl:Ubuntu:date_ns"},
        "ledger": {"poll_interval_s": 1.0, "max_polls": 130}, "prometheus": prometheus or {"mode": "deferred"},
        "tools": {"peer": "/opt/fabric/bin/peer", "configtxlator": "/opt/fabric/bin/configtxlator", "orderer": "localhost:7050", "orderer_tls_hostname": "orderer.example.com",
                  "orderer_ca": "/opt/ca.pem", "wsl_repo_path": "/home/u/repo", "network_description_wsl": "/home/u/network.json",
                  "rendered_dir_wsl": f"/mnt/c/fixture/wired_v3_timed/{run_id}", "output_dir_wsl": (out).as_posix()},
        "rendered_root": str(root), "output_dir": str(out), "authorization_path": str(tmp / "authorization.json"),
    }
    req.update(copy.deepcopy(overrides))
    return req


def build_plan(tmp: Path, **overrides: Any) -> tuple[dict[str, Any], dict[str, Any], Path]:
    root = render_a0p(tmp)
    req = request_for(root, tmp, **overrides)
    return be.build_execution_plan(req, repo_root=REPO, created_utc="2026-11-22T09:00:00Z"), req, root


def approved_auth(plan: dict[str, Any], tmp: Path, *, synthetic: bool = True) -> dict[str, Any]:
    record = tmp / "approval_record.txt"
    record.write_text("SYNTHETIC approval document for a test; not an approval\n", encoding="utf-8")
    auth = be.authorization_template(plan)
    auth["synthetic"] = synthetic
    auth["approval"] = {"status": "APPROVED_BY_OWNER", "approved_by": "synthetic-test-fixture", "approved_utc": "2026-11-22T09:30:00Z", "record_path": str(record),
                        "record_sha256": pi.sha256_file(record)}
    return auth


def deps_for(runner: MockRunner, clock: FakeClock, *, initial: dict | None = None, final: dict | None = None) -> be.Deps:
    blocks = {"initial": initial or decoded_block(), "final": final or decoded_block()}

    def read_file(path: str) -> bytes:
        stage = "initial" if "initial" in path else "final"
        return json.dumps(blocks[stage]).encode("utf-8")

    def hash_tree(root: str) -> dict[str, str]:
        base = Path(root)
        return {p.relative_to(base).as_posix(): pi.sha256_file(p) for p in sorted(base.rglob("*")) if p.is_file()} if base.is_dir() else {}

    from datetime import datetime, timezone
    return be.Deps(runner=runner, now_utc=lambda: datetime(2026, 11, 22, 10, 0, 0, tzinfo=timezone.utc), wall_ns=clock.wall, mono_ns=clock.mono, read_file=read_file, hash_tree=hash_tree)
