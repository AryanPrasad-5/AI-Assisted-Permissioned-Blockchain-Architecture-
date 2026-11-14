"""Phase 2C, Package B: keep-awake lifecycle with an identity-checked cleanup. The helper is never executed; every process, identity query and `taskkill` is the fake backend's.
The property under test: a process is killed only if it is the helper THIS launch started, and every cleanup outcome is recorded."""

import atexit
import base64
import json

import pytest

from p4_replay import driver as dr, keepawake as ka, keepawake_v2 as k2
from p4_replay.tests import support_2c as s2
from p4_replay.tests.support import KEEP_PID

p1b = dr.p1b_module()
ALLOW = lambda be: None                                                                        # noqa: E731


def make(tmp_path, *, seconds=600, **knobs):
    be = s2.FakeLaunchBackend(s2.world())
    for k, v in knobs.items():
        setattr(be, k, v)
    pid = tmp_path / "keep.pid"
    return be, k2.KeepAwakeV2(be, seconds, pid, str(pid), backend_check=ALLOW, wait_s=3.0), pid


# ----------------------------------------------------------------------------------------------- the helper text
def test_the_helper_text_carries_a_unique_nonce_and_passes_the_static_checks():
    a, b = k2.script_with_nonce(k2.new_nonce()), k2.script_with_nonce(k2.new_nonce())
    assert ka.script_problems(a) == [] and "SetThreadExecutionState" not in a and a != b and k2.encoded(a) != k2.encoded(b)
    assert base64.b64decode(k2.helper_argv(a)[-1]).decode("utf-16-le") == a and a.endswith(ka.KEEP_AWAKE_REQUEST_PS)


def test_the_identity_rule_needs_both_the_image_and_this_launchs_exact_command():
    token = k2.encoded(k2.script_with_nonce("n1"))
    ours = {"state": "PRESENT", "name": "powershell.exe", "command_line": f"powershell.exe -NoProfile -EncodedCommand {token}"}
    assert k2.is_our_helper(ours, token)
    assert not k2.is_our_helper({**ours, "name": "notepad.exe"}, token), "right command line, wrong image"
    assert not k2.is_our_helper({**ours, "command_line": "powershell.exe -Command Get-Date"}, token), "right image, wrong command line"
    other_launch = k2.encoded(k2.script_with_nonce("n2"))
    assert not k2.is_our_helper({**ours, "command_line": f"powershell.exe -EncodedCommand {other_launch}"}, token), "an earlier helper of another launch is not ours"
    assert not k2.is_our_helper({"state": "ABSENT"}, token) and k2.is_our_helper({**ours, "name": "PowerShell.EXE"}, token)


def test_parse_identity_understands_present_absent_and_refuses_garbage():
    assert k2.parse_identity(b"ABSENT\r\n") == {"state": "ABSENT"}
    got = k2.parse_identity(json.dumps({"Name": "powershell.exe", "CommandLine": "x", "Created": "t"}).encode())
    assert got == {"state": "PRESENT", "name": "powershell.exe", "command_line": "x", "created": "t"}
    assert k2.parse_identity(b"") is None and k2.parse_identity(b"???") is None and k2.parse_identity(b"[1,2]") is None


# ----------------------------------------------------------------------------------------------- normal paths
def test_normal_exit_identifies_the_helper_at_start_and_records_a_clean_cleanup(tmp_path):
    be, k, _ = make(tmp_path)
    with k:
        assert k.pid == KEEP_PID and k.identity_at_start["state"] == "PRESENT" and be.identity_calls == 1
    rec = k.cleanup_record
    assert rec["ok"] is True and rec["outcome"] == "HELPER_ALREADY_EXITED" and rec["pid"] == KEEP_PID and rec["nonce"] == k.nonce
    assert be.taskkills == [], "the launcher handle's terminate already ended it: no kill was needed, and none was sent"
    json.dumps(rec)


def test_when_the_windows_process_outlives_the_launcher_handle_it_is_killed_after_identification_and_verified_gone(tmp_path):
    be, k, _ = make(tmp_path, survives_terminate=True)
    with k:
        pass
    rec = k.cleanup_record
    assert rec["outcome"] == "TERMINATED_VERIFIED" and rec["ok"] is True
    assert be.taskkills == [["taskkill.exe", "/PID", str(KEEP_PID), "/F"]] and "/T" not in be.taskkills[0], "no process tree kill"
    assert be.identity_calls == 3, "start, before the kill, after the kill"


def test_cleanup_runs_when_the_body_raises(tmp_path):
    be, k, _ = make(tmp_path, survives_terminate=True)
    with pytest.raises(RuntimeError, match="boom"):
        with k:
            raise RuntimeError("boom")
    assert k.cleaned and k.cleanup_record["outcome"] == "TERMINATED_VERIFIED" and be.keep_killed


def test_cleanup_runs_on_keyboard_interrupt(tmp_path):
    be, k, _ = make(tmp_path, survives_terminate=True)
    with pytest.raises(KeyboardInterrupt):
        with k:
            raise KeyboardInterrupt
    assert k.cleaned and k.cleanup_record["ok"] is True and be.keep_killed


def test_an_atexit_hook_covers_the_interpreter_ending_without_the_context_manager(tmp_path, monkeypatch):
    registered, unregistered = [], []
    monkeypatch.setattr(atexit, "register", lambda fn, *a, **kw: registered.append(fn))
    monkeypatch.setattr(atexit, "unregister", lambda fn: unregistered.append(fn))
    be, k, _ = make(tmp_path, survives_terminate=True)
    with k:
        assert len(registered) == 1 and unregistered == []
        registered[0]()                                                                          # what the interpreter would do at exit, without __exit__
        assert k.cleaned and be.keep_killed
    assert k.cleanup_record["outcome"] == "TERMINATED_VERIFIED" and be.taskkills.__len__() == 1


def test_cleanup_is_idempotent(tmp_path):
    be, k, _ = make(tmp_path, survives_terminate=True)
    with k:
        pass
    calls, kills = be.identity_calls, len(be.taskkills)
    again = k.cleanup()
    assert again is k.cleanup_record and be.identity_calls == calls and len(be.taskkills) == kills


# ----------------------------------------------------------------------------------------------- PID reuse and every failure
def test_a_reused_pid_is_never_killed(tmp_path):
    """The reason this module exists. The helper is gone and Windows gave its pid to another program; Phase 2's unconditional `taskkill /PID /F` would have ended it."""
    be, k, _ = make(tmp_path)
    with k:
        be.keep_alive = False
        be.pid_reused = True
    rec = k.cleanup_record
    assert rec["outcome"] == "PID_NOT_OURS_LEFT_ALONE" and rec["ok"] is True and be.taskkills == []
    assert any("pid reuse" in line and "NOT terminated" in line for line in rec["log"])


def test_phase_2s_cleanup_would_have_killed_the_unrelated_process_in_the_same_scenario(tmp_path):
    be = s2.FakeLaunchBackend(s2.world())
    pid = tmp_path / "keep.pid"
    old = ka.KeepAwake(be, 600, pid, str(pid), wait_s=3.0)
    with old:
        be.keep_alive = False
        be.pid_reused = True
    assert be.taskkills == [["taskkill.exe", "/PID", str(KEEP_PID), "/F"]], "the Phase 2 helper killed pid 4242 without asking who owns it"


def test_a_process_that_already_exited_is_a_successful_cleanup_without_a_kill(tmp_path):
    be, k, _ = make(tmp_path)
    with k:
        be.keep_alive = False
    assert k.cleanup_record["outcome"] == "HELPER_ALREADY_EXITED" and k.cleanup_record["ok"] and be.taskkills == []


@pytest.mark.parametrize("knob", ["query_fails", "query_garbage"])
def test_an_identity_query_that_fails_at_cleanup_kills_nothing_and_records_a_failure(tmp_path, knob):
    be, k, _ = make(tmp_path, survives_terminate=True)
    with k:
        setattr(be, knob, True)
    rec = k.cleanup_record
    assert rec["outcome"] == "IDENTITY_QUERY_FAILED" and rec["ok"] is False and be.taskkills == []
    assert "ends itself" in rec["note"]


@pytest.mark.parametrize("knob", ["query_fails", "query_garbage"])
def test_an_identity_query_that_fails_at_start_refuses_to_start_and_kills_nothing(tmp_path, knob):
    be, k, _ = make(tmp_path, **{knob: True})
    with pytest.raises(k2.KeepAwakeError, match="could not be identified"):
        with k:
            pytest.fail("the body must not run")
    assert k.cleaned and be.taskkills == [] and k.cleanup_record["ok"] is False


def test_a_pid_that_belongs_to_another_program_at_start_refuses_to_start(tmp_path):
    be, k, _ = make(tmp_path, pid_reused=True)
    with pytest.raises(k2.KeepAwakeError, match="could not be identified"):
        with k:
            pass
    assert be.taskkills == [] and k.cleanup_record["outcome"] == "PID_NOT_OURS_LEFT_ALONE"


def test_a_kill_that_fails_is_recorded_as_still_running(tmp_path):
    be, k, _ = make(tmp_path, survives_terminate=True, taskkill_fails=True)
    with k:
        pass
    rec = k.cleanup_record
    assert rec["outcome"] == "STILL_RUNNING_AFTER_KILL" and rec["ok"] is False and len(be.taskkills) == 1


def test_a_helper_that_dies_at_start_is_cleaned_up_and_nothing_is_killed(tmp_path):
    be = s2.FakeLaunchBackend(s2.world(keep_dies_at_start=True))
    pid = tmp_path / "keep.pid"
    k = k2.KeepAwakeV2(be, 600, pid, str(pid), backend_check=ALLOW, wait_s=3.0)
    with pytest.raises(k2.KeepAwakeError, match="did not stay up"):
        with k:
            pytest.fail("the body must not run")
    assert k.cleanup_record["outcome"] == "HELPER_NEVER_STARTED" and k.cleanup_record["ok"] is True and be.taskkills == []


@pytest.mark.parametrize("junk", ["", "abc", "-5", "0", "12 34"])
def test_a_pid_file_that_is_not_a_process_id_is_refused_and_kills_nothing(tmp_path, junk):
    be = s2.FakeLaunchBackend(s2.world())
    pid = tmp_path / "keep.pid"
    real_spawn = be.spawn

    def spawn(argv, env=None, *a, **kw):
        proc = real_spawn(argv, env, *a, **kw)
        pid.write_text(junk, encoding="utf-8")
        return proc

    be.spawn = spawn
    k = k2.KeepAwakeV2(be, 600, pid, str(pid), backend_check=ALLOW, wait_s=3.0)
    with pytest.raises(k2.KeepAwakeError):
        with k:
            pass
    assert be.taskkills == []


def test_a_backend_that_the_check_refuses_is_refused_before_anything_is_spawned(tmp_path):
    real = object.__new__(p1b.Backend)
    pid = tmp_path / "k.pid"

    def refuse(b):
        raise dr.LiveExecutionRefused("no")

    k = k2.KeepAwakeV2(real, 600, pid, str(pid), backend_check=refuse)
    with pytest.raises(dr.LiveExecutionRefused):
        with k:
            pass
    assert k.helper is None and not pid.exists() and k.cleanup_record is None, "nothing was started, so nothing needed cleaning up"


def test_the_duration_and_environment_reach_the_helper_and_zero_is_refused(tmp_path):
    seen = {}
    be, k, pid = make(tmp_path, seconds=4321)
    real_spawn = be.spawn
    be.spawn = lambda argv, env=None, *a, **kw: (seen.update(env=dict(env)), real_spawn(argv, env, *a, **kw))[1]
    with k:
        pass
    assert seen["env"]["QL_SECONDS"] == "4321" and seen["env"]["QL_PID_FILE"] == str(pid) and {"QL_SECONDS", "QL_PID_FILE"} <= set(seen["env"]["WSLENV"].split(":"))
    with pytest.raises(k2.KeepAwakeError, match="positive"):
        with k2.KeepAwakeV2(s2.FakeLaunchBackend(), 0, tmp_path / "z.pid", str(tmp_path / "z.pid"), backend_check=ALLOW):
            pass
