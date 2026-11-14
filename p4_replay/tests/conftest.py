"""Fixtures for the Phase 1 replay tests. Synthetic data only (the real-data checks are skipped when the derived trace is absent).

SAFETY GUARD (autouse): while any test runs, starting a process is refused unless it is `node` (used only to parse and exercise the
Caliper workload JavaScript without Caliper), opening a network connection is refused, and os.system / os.popen are refused. This is
what makes "no Docker, WSL, Fabric or Caliper" a tested property rather than a promise.
"""

from __future__ import annotations

import os
import socket
import subprocess
import zipfile
import hashlib
from pathlib import Path

import numpy as np
import pytest

from p4_replay import REPO_ROOT, ensure_paths

ensure_paths()

ALLOWED_EXECUTABLES = {"node", "node.exe"}
SPAWN_ATTEMPTS: list[list[str]] = []


class ForbiddenProcess(AssertionError):
    pass


@pytest.fixture(autouse=True)
def no_processes_no_network(monkeypatch):
    real_popen = subprocess.Popen

    class GuardedPopen(real_popen):
        def __init__(self, args, *a, **kw):
            argv = [args] if isinstance(args, (str, os.PathLike)) else list(args)
            SPAWN_ATTEMPTS.append([str(x) for x in argv])
            if Path(str(argv[0])).name.lower() not in ALLOWED_EXECUTABLES:
                raise ForbiddenProcess(f"a test tried to start {argv[0]!r}; only node is allowed")
            super().__init__(args, *a, **kw)

    def forbid(*a, **kw):
        raise ForbiddenProcess("process or network use is not allowed in the offline replay tests")

    monkeypatch.setattr(subprocess, "Popen", GuardedPopen)
    monkeypatch.setattr(os, "system", forbid)
    monkeypatch.setattr(os, "popen", forbid)
    monkeypatch.setattr(socket.socket, "connect", forbid)
    monkeypatch.setattr(socket, "create_connection", forbid)
    yield


# ----------------------------------------------------------------------------------------------- a synthetic day
START_S = 1000                    # window start (UTC second of day); pre-roll is seconds 940-999
LO = START_S * 1000


def synthetic_day() -> np.ndarray:
    """A day trace (ms since 00:00:00.000) with deliberate structure around a window starting at second 1000:
      * pre-roll: arrival exactly 60,000 ms before the window (kept), 60,001 ms before (excluded), ties, and one at LO-1 (kept)
      * measured: arrival exactly at offset 0 (kept), a 50-arrival burst inside second 5 with exact ties,
        a zero-arrival stretch over seconds 100-199, a sub-5-ms cluster, the last arrival at 599,999 ms
      * after: arrival exactly at 600,000 ms (excluded) and later
    """
    pre = [LO - 60_001, LO - 60_000, LO - 60_000, LO - 30_250, LO - 30_250, LO - 30_249, LO - 1]
    measured = [LO + 0, LO + 0, LO + 7, LO + 1_200, LO + 1_204, LO + 1_204, LO + 1_209]
    burst = [LO + 5_000 + (i * 19) // 2 for i in range(50)]                    # 50 arrivals in second 5, with ties
    burst[10] = burst[11]                                                      # an exact tie inside the burst
    sparse = [LO + 20_000, LO + 20_001, LO + 99_999, LO + 200_000, LO + 450_123, LO + 599_998, LO + 599_999]   # seconds 100-199 stay empty
    after = [LO + 600_000, LO + 600_000, LO + 601_500]
    before = [1_000, 5_000, LO - 200_000]
    day = np.array(sorted(before + pre + measured + burst + sparse + after), dtype=np.int64)
    return day


@pytest.fixture
def day() -> np.ndarray:
    return synthetic_day()


# ----------------------------------------------------------------------------------------------- synthetic archive (zip + manifest entry)
def make_archive(path: Path, day_offsets: np.ndarray, date: str = "2026-10-12", day_start_ms: int = 1_791_763_200_000) -> dict:
    lines = ["timestamp_ms,hash,chain_id"] + [f"{day_start_ms + int(o)},0x{i:064x},1" for i, o in enumerate(day_offsets)]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(f"{date}.csv", "\n".join(lines) + "\n")
    data = Path(path).read_bytes()
    return {"filename": f"{date}.csv.zip", "sha256": hashlib.sha256(data).hexdigest().upper(), "compressed_bytes": len(data),
            "rows": len(day_offsets)}


ENV = {"test_network": "/home/aryan/fabric/fabric-samples/test-network", "fabric_bin": "/home/aryan/fabric/bin",
       "fabric_cfg": "/home/aryan/fabric/fabric-samples/config", "repo": "/mnt/c/OJT_SEM3/AI-Assisted-Permissioned-Blockchain-Architecture-",
       "caliper_workspace": "/home/aryan/e1/caliper-ws", "chaincode": {"name": "e1asset", "version": "1.0", "sequence": 1},
       "protocol": {"event_strategy": "msp_any", "invoke_timeout_s": 60, "binding": "fabric:2.5"}}
NETWORK_PATH = "/home/aryan/e1/network.json"


PKG = "e1asset_1.0:" + "ab" * 32


@pytest.fixture
def env() -> dict:
    return dict(ENV)


@pytest.fixture(scope="session")
def network_doc() -> dict:
    """The real network description built by the existing renderer (not a bootstrap one); lets ro.Plan validate offline."""
    import e1_la2_render as lr
    return lr.network(ENV, PKG)


@pytest.fixture
def window(day):
    from p4_replay.offsets import build_window
    return build_window(day, "T_SYN", "2026-10-12", START_S, 600, 60)
