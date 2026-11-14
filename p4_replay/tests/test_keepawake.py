"""Process-scoped keep-awake: lifecycle and cleanup against the fake backend, and the static checks of the replacement helper text. Whether the helper actually prevents standby
on the owner's machine is LIVE_UNTESTED; standby is DETECTED through the clock record (see test_series), not trusted away."""

import base64
import os

import pytest

from p4_replay import driver as dr, keepawake as ka
from p4_replay.tests import support as sp
from p4_replay.tests.support import FakeSeriesBackend, World

p1b = dr.p1b_module()


def make(tmp_path, w=None):
    be = FakeSeriesBackend(w or World())
    pid = tmp_path / "keep.pid"
    return be, ka.KeepAwake(be, 600, pid, str(pid), wait_s=3.0), pid


def test_the_helper_text_passes_its_static_checks_and_is_not_the_p1b_literal():
    assert ka.script_problems() == []
    assert "SetThreadExecutionState" in p1b.KEEP_AWAKE_PS and "SetThreadExecutionState" not in ka.KEEP_AWAKE_REQUEST_PS
    argv = ka.helper_argv()
    assert argv[:2] == ["powershell.exe", "-NoProfile"] and argv[-2] == "-EncodedCommand"
    assert base64.b64decode(argv[-1]).decode("utf-16-le") == ka.KEEP_AWAKE_REQUEST_PS


@pytest.mark.parametrize("token", ["PowerCreateRequest", "PowerSetRequest($h, 1)", "PowerSetRequest($h, 3)", "PowerClearRequest($h, 1)", "finally", "$env:QL_PID_FILE", "$env:QL_SECONDS"])
def test_a_helper_text_missing_a_required_part_is_flagged(token):
    assert any(token in p for p in ka.script_problems(ka.KEEP_AWAKE_REQUEST_PS.replace(token, "XX")))
    assert ka.script_problems(p1b.KEEP_AWAKE_PS)                                            # and the thread-scoped P1b literal fails these checks


def test_cleanup_runs_on_success(tmp_path):
    be, k, pid = make(tmp_path)
    with k:
        assert k.pid == sp.KEEP_PID and be.keep_alive and be.host_power(k.pid)["helper_alive"] is True
        env_seen = be.spawns[0]
    assert k.cleaned and not be.keep_alive and be.taskkills == [["taskkill.exe", "/PID", str(sp.KEEP_PID), "/F"]] and "terminate sent" in k.cleanup_log
    assert env_seen[0] == "powershell.exe"


def test_cleanup_runs_when_the_body_raises(tmp_path):
    be, k, _ = make(tmp_path)
    with pytest.raises(RuntimeError, match="boom"):
        with k:
            raise RuntimeError("boom")
    assert k.cleaned and be.taskkills and not be.keep_alive


def test_a_helper_that_dies_at_start_raises_and_is_cleaned_up(tmp_path):
    be, k, _ = make(tmp_path, World(keep_dies_at_start=True))
    with pytest.raises(ka.KeepAwakeError, match="did not stay up"):
        with k:
            pytest.fail("the body must not run")
    assert k.cleaned


def test_the_duration_and_environment_reach_the_helper(tmp_path, monkeypatch):
    seen = {}
    be, k, pid = make(tmp_path)
    real_spawn = be.spawn
    be.spawn = lambda argv, env=None, *a, **kw: (seen.update(env=dict(env)), real_spawn(argv, env, *a, **kw))[1]
    with k:
        pass
    assert seen["env"]["QL_SECONDS"] == "600" and seen["env"]["QL_PID_FILE"] == str(pid) and "QL_SECONDS" in seen["env"]["WSLENV"].split(":") and "QL_PID_FILE" in seen["env"]["WSLENV"].split(":")


def test_cleanup_is_idempotent_and_a_zero_duration_is_refused(tmp_path):
    be, k, _ = make(tmp_path)
    with k:
        pass
    k._cleanup()
    assert len(be.taskkills) == 1
    with pytest.raises(ka.KeepAwakeError, match="positive"):
        with ka.KeepAwake(FakeSeriesBackend(), 0, tmp_path / "z.pid", str(tmp_path / "z.pid")):
            pass


def test_a_real_backend_is_refused_before_it_spawns_anything(tmp_path):
    real = object.__new__(p1b.Backend)
    k = ka.KeepAwake(real, 600, tmp_path / "k.pid", str(tmp_path / "k.pid"))
    with pytest.raises(dr.LiveExecutionRefused):
        with k:
            pass
    assert not (tmp_path / "k.pid").exists() and k.helper is None
