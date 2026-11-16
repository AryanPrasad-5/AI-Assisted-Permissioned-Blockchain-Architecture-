"""Process-scoped keep-awake for the replay series (Phase 2). The replacement helper is LIVE_UNTESTED.

The P1b helper (e1_quicklook_p1b.KEEP_AWAKE_PS) calls SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED) on one PowerShell thread and sleeps for a fixed
number of seconds that the driver computes as `540 x runs + 1200`. The project notes record it as ineffective (the A2-R1 clock record shows a standby/resume), and the
duration formula was sized for 150 s runs, so it would end in the middle of a 13-minute replay. Nothing in the repository says WHY it was ineffective, so this module does
not claim to have fixed the cause. It does three things instead:

  1. a different request mechanism (PowerCreateRequest with SYSTEM_REQUIRED and EXECUTION_REQUIRED requests, released in a `finally`), which the OS ties to the helper
     process: it ends when the process ends, however that happens;
  2. a duration that comes from the run-length budget (caliper_check.keep_awake_seconds), not from a literal;
  3. cleanup that always runs (context manager): terminate the launcher, then taskkill the Windows process by its PID, on success and on failure.

What is NOT claimed: that the request prevents standby on this machine (LIVE_UNTESTED; a script parse check is the only offline check possible). A standby is therefore
DETECTED, not trusted away: the series keeps a continuous clock probe, checks its record over every run window, and labels a run CLOCK_FAILURE on a step or a sampling hole.
"""

from __future__ import annotations

import base64
import os
from pathlib import Path

from . import driver as dr
from . import guard

POWER_REQUEST_REASON = "p4_replay series keep-awake"
KEEP_AWAKE_REQUEST_PS = r"""
$ErrorActionPreference = 'Stop'
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class P4Power {
    [StructLayout(LayoutKind.Sequential)]
    public struct POWER_REQUEST_CONTEXT {
        public UInt32 Version;
        public UInt32 Flags;
        [MarshalAs(UnmanagedType.LPWStr)] public string SimpleReasonString;
    }
    [DllImport("kernel32.dll", SetLastError = true)] public static extern IntPtr PowerCreateRequest(ref POWER_REQUEST_CONTEXT Context);
    [DllImport("kernel32.dll", SetLastError = true)] public static extern bool PowerSetRequest(IntPtr PowerRequest, int RequestType);
    [DllImport("kernel32.dll", SetLastError = true)] public static extern bool PowerClearRequest(IntPtr PowerRequest, int RequestType);
}
'@
$ctx = New-Object P4Power+POWER_REQUEST_CONTEXT
$ctx.Version = 0
$ctx.Flags = 1
$ctx.SimpleReasonString = 'p4_replay series keep-awake'
$h = [P4Power]::PowerCreateRequest([ref]$ctx)
if ($h -eq [IntPtr]::Zero) { throw 'PowerCreateRequest failed' }
[void][P4Power]::PowerSetRequest($h, 1)
[void][P4Power]::PowerSetRequest($h, 3)
Set-Content -Path $env:QL_PID_FILE -Value $PID
try { Start-Sleep -Seconds ([int]$env:QL_SECONDS) }
finally { [void][P4Power]::PowerClearRequest($h, 1); [void][P4Power]::PowerClearRequest($h, 3) }
"""


def script_problems(script: str = KEEP_AWAKE_REQUEST_PS) -> list[str]:
    """Static checks of the helper text (the only offline check): it must use the request API, set both requests, release them in a finally, write the pid file the driver
    reads, take its duration from the environment, and must not be the thread-scoped P1b literal."""
    need = ["PowerCreateRequest", "PowerSetRequest($h, 1)", "PowerSetRequest($h, 3)", "PowerClearRequest($h, 1)", "PowerClearRequest($h, 3)", "finally",
            "$env:QL_PID_FILE", "$env:QL_SECONDS", "Start-Sleep"]
    problems = [f"missing {n!r}" for n in need if n not in script]
    if "SetThreadExecutionState" in script:
        problems.append("uses the thread-scoped SetThreadExecutionState (the P1b literal)")
    return problems


def encoded(script: str = KEEP_AWAKE_REQUEST_PS) -> str:
    return base64.b64encode(script.encode("utf-16-le")).decode("ascii")


def helper_argv() -> list[str]:
    return ["powershell.exe", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-EncodedCommand", encoded()]


class KeepAwakeError(RuntimeError):
    pass


class KeepAwake:
    """Context manager around the helper. `be` needs spawn(argv, env), run(argv, env, timeout), sleep(s); with a real p1b.Backend it is refused first
    (guard.require_offline_backend) so no process is started in this build."""

    def __init__(self, be, seconds: int, pid_file: Path, pid_file_win: str, *, wait_s: float = 15.0):
        self.be, self.seconds, self.pid_file, self.pid_file_win, self.wait_s = be, int(seconds), Path(pid_file), pid_file_win, wait_s
        self.helper = None
        self.pid: int | None = None
        self.cleaned = False
        self.cleanup_log: list[str] = []

    def __enter__(self) -> "KeepAwake":
        guard.require_offline_backend(self.be)
        if self.seconds <= 0:
            raise KeepAwakeError("keep-awake duration must be positive")
        p1b = dr.p1b_module()
        env = p1b.with_wslenv(dict(os.environ), QL_SECONDS=str(self.seconds), QL_PID_FILE=self.pid_file_win)
        try:
            self.helper = self.be.spawn(helper_argv(), env)
            waited = 0.0
            while waited < self.wait_s and not self.pid_file.is_file():
                self.be.sleep(0.5)
                waited += 0.5
            if self.pid_file.is_file():
                self.pid = int(self.pid_file.read_text().strip())
            if self.pid is None or self.helper.poll() is not None:
                raise KeepAwakeError(f"the keep-awake helper did not stay up (pid {self.pid}, exit {self.helper.poll()}): nothing may start without it")
        except BaseException:
            self._cleanup()
            raise
        return self

    def _cleanup(self) -> None:
        if self.cleaned:
            return
        self.cleaned = True
        if self.helper is not None:
            try:
                self.helper.terminate()
                self.cleanup_log.append("terminate sent")
            except Exception as exc:                                                    # noqa: BLE001 - cleanup must continue
                self.cleanup_log.append(f"terminate failed: {exc!r}")
        if self.pid:
            try:
                r = self.be.run(["taskkill.exe", "/PID", str(self.pid), "/F"], None, 30)
                self.cleanup_log.append(f"taskkill exit {r.code}")
            except Exception as exc:                                                    # noqa: BLE001
                self.cleanup_log.append(f"taskkill failed: {exc!r}")

    def __exit__(self, exc_type, exc, tb) -> bool:
        self._cleanup()
        return False
