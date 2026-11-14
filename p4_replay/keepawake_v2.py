"""Keep-awake helper lifecycle, version 2C: a duration from the documented budget and a cleanup that never kills a process it has not identified. OFFLINE_TESTED against a
fake backend; the helper itself was NOT executed (its effect on this machine is LIVE_UNTESTED, as in Phase 2).

What Phase 2 `keepawake.KeepAwake._cleanup` does, and why it is replaced: after `helper.terminate()` it runs `taskkill.exe /PID <pid> /F` unconditionally. The pid comes from a file
the helper wrote at start. If the helper has already exited (its own deadline passed, or it died) Windows may have given the same pid to an unrelated process before cleanup
runs, and `taskkill /F` would end THAT process. Here the pid is only a lookup key. Before any kill the process is queried (Win32_Process: image name and command line) and it is
killed only if BOTH are the ones this helper was launched with:
    image name  == powershell.exe
    command line contains this launch's complete -EncodedCommand text, which embeds a random nonce, so no other process (an earlier helper included) can carry it.
A process that is absent is a successful cleanup (already gone). A process that exists but is not ours is left alone and the pid reuse is recorded. A query that fails leaves the
process alone and records a FAILED cleanup (the helper still ends itself at its own deadline, QL_SECONDS, because it sleeps for exactly that long and releases its power requests in
a `finally`). Every outcome is recorded in `cleanup_record`, which the launch writes to disk.

Normal exit, an exception, and KeyboardInterrupt all pass through `__exit__`; an `atexit` hook covers the interpreter ending without it. Cleanup runs once.

The duration comes from duration_model.build_budget (an upper bound that starts where the helper starts, before the pre-flight), not from a literal.
The helper script is Phase 2's PowerCreateRequest script plus one comment line carrying the nonce; `keepawake.script_problems` is applied to it.
"""

from __future__ import annotations

import atexit
import base64
import json
import os
import secrets
from pathlib import Path
from typing import Callable

from . import driver as dr
from . import keepawake as ka

OK_OUTCOMES = ("HELPER_ALREADY_EXITED", "TERMINATED_VERIFIED", "PID_NOT_OURS_LEFT_ALONE", "HELPER_NEVER_STARTED")
FAILED_OUTCOMES = ("IDENTITY_QUERY_FAILED", "TERMINATE_FAILED", "STILL_RUNNING_AFTER_KILL", "NO_PID_RECORDED_COULD_NOT_VERIFY")
IMAGE_NAME = "powershell.exe"


class KeepAwakeError(RuntimeError):
    pass


def new_nonce() -> str:
    return secrets.token_hex(8)


def script_with_nonce(nonce: str, base: str = ka.KEEP_AWAKE_REQUEST_PS) -> str:
    return f"# p4_replay keep-awake launch nonce {nonce}\n{base}"


def encoded(script: str) -> str:
    return base64.b64encode(script.encode("utf-16-le")).decode("ascii")


def helper_argv(script: str) -> list[str]:
    return ["powershell.exe", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-EncodedCommand", encoded(script)]


def identity_query_argv(pid: int) -> list[str]:
    ps = (f"$p = Get-CimInstance Win32_Process -Filter \"ProcessId={int(pid)}\"; "
          "if ($null -eq $p) { 'ABSENT' } else { @{Name=$p.Name; CommandLine=$p.CommandLine; Created=$p.CreationDate.ToUniversalTime().ToString('o')} | ConvertTo-Json -Compress }")
    return ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", ps]


def parse_identity(stdout: bytes) -> dict | None:
    """{"state": "ABSENT"} | {"state": "PRESENT", name, command_line, created} | None when the output is not understood."""
    lines = [ln.strip() for ln in stdout.decode("utf-8", "replace").splitlines() if ln.strip()]
    if not lines:
        return None
    if lines[0] == "ABSENT":
        return {"state": "ABSENT"}
    try:
        doc = json.loads(lines[-1] if len(lines) == 1 else "\n".join(lines))
    except ValueError:
        return None
    if not isinstance(doc, dict) or "Name" not in doc:
        return None
    return {"state": "PRESENT", "name": str(doc.get("Name") or ""), "command_line": str(doc.get("CommandLine") or ""), "created": doc.get("Created")}


def is_our_helper(info: dict, encoded_command: str) -> bool:
    """The identity rule. Both conditions; either alone is not enough."""
    return info.get("state") == "PRESENT" and info["name"].lower() == IMAGE_NAME and encoded_command in info["command_line"]


class KeepAwakeV2:
    """Context manager around the helper. `backend_check(be)` is called first and must raise to refuse a backend (the launch passes the check for its own mode)."""

    def __init__(self, be, seconds: int, pid_file: Path, pid_file_win: str, *, backend_check: Callable, wait_s: float = 15.0, nonce: str | None = None):
        self.be, self.seconds, self.pid_file, self.pid_file_win, self.wait_s = be, int(seconds), Path(pid_file), pid_file_win, wait_s
        self.backend_check = backend_check
        self.nonce = nonce or new_nonce()
        self.script = script_with_nonce(self.nonce)
        self.encoded_command = encoded(self.script)
        self.helper = None
        self.pid: int | None = None
        self.identity_at_start: dict | None = None
        self.cleanup_record: dict | None = None
        self.cleanup_log: list[str] = []
        self._hook = None

    @property
    def cleaned(self) -> bool:
        return self.cleanup_record is not None

    def _query(self, pid: int) -> tuple[dict | None, str]:
        try:
            r = self.be.run(identity_query_argv(pid), None, 60)
        except Exception as exc:                                                    # noqa: BLE001
            return None, f"query raised {exc!r}"
        if r.code != 0:
            return None, f"query exit {r.code}"
        info = parse_identity(r.stdout)
        return info, "ok" if info is not None else "query output not understood"

    def __enter__(self) -> "KeepAwakeV2":
        self.backend_check(self.be)
        problems = ka.script_problems(self.script)
        if problems:
            raise KeepAwakeError(f"the keep-awake helper text is not acceptable: {problems}")
        if self.seconds <= 0:
            raise KeepAwakeError("keep-awake duration must be positive")
        p1b = dr.p1b_module()
        env = p1b.with_wslenv(dict(os.environ), QL_SECONDS=str(self.seconds), QL_PID_FILE=self.pid_file_win)
        self._hook = self.cleanup
        atexit.register(self._hook)
        try:
            self.helper = self.be.spawn(helper_argv(self.script), env)
            waited = 0.0
            while waited < self.wait_s and not self.pid_file.is_file():
                self.be.sleep(0.5)
                waited += 0.5
            if self.pid_file.is_file():
                text = self.pid_file.read_text(encoding="utf-8", errors="replace").strip().lstrip("﻿")
                if not text.isdigit() or int(text) <= 0:
                    raise KeepAwakeError(f"the pid file holds {text[:40]!r}, not a process id")
                self.pid = int(text)
            if self.pid is None or self.helper.poll() is not None:
                raise KeepAwakeError(f"the keep-awake helper did not stay up (pid {self.pid}, exit {self.helper.poll()}): nothing may start without it")
            info, why = self._query(self.pid)
            self.identity_at_start = info
            if info is None or not is_our_helper(info, self.encoded_command):
                raise KeepAwakeError(f"pid {self.pid} could not be identified as this launch's keep-awake helper ({why}; {info and info.get('state')}): nothing may start without it")
        except BaseException:
            self.cleanup()
            raise
        return self

    # ------------------------------------------------------------------------------------------- cleanup (once, identity-checked)
    def cleanup(self) -> dict:
        if self.cleanup_record is not None:
            return self.cleanup_record
        if self._hook is not None:
            try:
                atexit.unregister(self._hook)
            except Exception:                                                         # noqa: BLE001
                pass
        log = self.cleanup_log
        if self.helper is not None:
            try:
                self.helper.terminate()
                log.append("launcher handle: terminate sent")
            except Exception as exc:                                                  # noqa: BLE001 - cleanup must continue
                log.append(f"launcher handle: terminate failed {exc!r}")
        outcome = self._decide()
        self.cleanup_record = {"pid": self.pid, "nonce": self.nonce, "outcome": outcome, "ok": outcome in OK_OUTCOMES, "log": list(log),
                               "note": "the helper ends itself at QL_SECONDS and releases its power requests in a finally, whatever this record says"}
        return self.cleanup_record

    def _decide(self) -> str:
        log = self.cleanup_log
        if self.pid is None:
            exited = self.helper is None or self.helper.poll() is not None
            log.append("no pid was recorded")
            return "HELPER_NEVER_STARTED" if exited else "NO_PID_RECORDED_COULD_NOT_VERIFY"
        info, why = self._query(self.pid)
        if info is None:
            log.append(f"identity query failed ({why}); nothing was killed")
            return "IDENTITY_QUERY_FAILED"
        if info["state"] == "ABSENT":
            log.append(f"pid {self.pid}: no such process (already exited)")
            return "HELPER_ALREADY_EXITED"
        if not is_our_helper(info, self.encoded_command):
            log.append(f"pid {self.pid}: now {info['name']!r} with a different command line (pid reuse); NOT terminated")
            return "PID_NOT_OURS_LEFT_ALONE"
        try:
            r = self.be.run(["taskkill.exe", "/PID", str(self.pid), "/F"], None, 30)
            log.append(f"taskkill exit {r.code}")
        except Exception as exc:                                                      # noqa: BLE001
            log.append(f"taskkill raised {exc!r}")
            return "TERMINATE_FAILED"
        again, why2 = self._query(self.pid)
        if again is not None and again["state"] == "ABSENT":
            return "TERMINATED_VERIFIED"
        if again is not None and not is_our_helper(again, self.encoded_command):
            log.append("after taskkill the pid belongs to another process")
            return "TERMINATED_VERIFIED"
        log.append(f"after taskkill: {why2}; {again and again.get('state')}")
        return "STILL_RUNNING_AFTER_KILL" if again is not None else "IDENTITY_QUERY_FAILED"

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.cleanup()
        return False
