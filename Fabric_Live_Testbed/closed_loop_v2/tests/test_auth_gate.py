"""The v2 authorization gate (ops/check_auth_v2.py) on temporary stages and owner files; no process, no Fabric."""
import hashlib
import runpy
import sys
from pathlib import Path

import pytest

from Fabric_Live_Testbed.closed_loop.tests.test_runner_fake import auth_file
from Fabric_Live_Testbed.closed_loop_v2 import protocol as P
from Fabric_Live_Testbed.closed_loop_v2 import stage_v2 as ST

GATE = Path(__file__).resolve().parents[1] / "ops" / "check_auth_v2.py"


@pytest.fixture(scope="module")
def stages(tmp_path_factory):
    mp = pytest.MonkeyPatch()
    mp.setattr(P, "X0_RUNG_RATES_TPS", (10, 20))
    x0 = tmp_path_factory.mktemp("g") / "x0"
    m0 = ST.stage_x0(x0)
    mp.undo()
    return x0, m0


def gate(monkeypatch, capsys, phase, auth, stage):
    monkeypatch.setattr(sys, "argv", ["check_auth_v2.py", phase, str(auth), str(stage)])
    with pytest.raises(SystemExit) as e:
        runpy.run_path(str(GATE), run_name="__main__")
    return e.value.code, capsys.readouterr().out


def write_auth(tmp_path, stage, m, **over):
    runs = over.pop("runs", ",".join(r["run_id"] for r in m["runs"])).split(",")
    chans = over.pop("broadcast_channels", ",".join(r["channel"] for r in m["runs"])).split(",")
    return auth_file(tmp_path, over.pop("stage_sha256", hashlib.sha256((stage / "SHA256SUMS.txt").read_bytes()).hexdigest()), runs, chans,
                     **{"max_broadcasts_per_run": "0", "native_verification": "required", **over})


def test_exact_x0_authorization_is_accepted(stages, tmp_path, monkeypatch, capsys):
    stage, m = stages
    code, out = gate(monkeypatch, capsys, "x0", write_auth(tmp_path, stage, m), stage)
    assert code == 0 and "V2_AUTH ACCEPTED for phase x0" in out


@pytest.mark.parametrize("over,needle", [
    ({"max_broadcasts_per_run": "1"}, "max_broadcasts_per_run=0"),
    ({"expires_utc": "2020-01-01T00:00:00Z"}, "REFUSED"),
    ({"stage_sha256": "0" * 64}, "stage SHA256SUMS"),
    ({"runs": "cx01,cx02,cx03"}, "authorized runs"),
    ({"runs": "cx01,cx02,cx03,cx04,cl01"}, "authorized runs"),
    ({"broadcast_channels": "e1-cx01,e1-cx02,e1-cx03"}, "channels differ"),
])
def test_wrong_x0_authorizations_are_refused(stages, tmp_path, monkeypatch, capsys, over, needle):
    stage, m = stages
    code, out = gate(monkeypatch, capsys, "x0", write_auth(tmp_path, stage, m, **over), stage)
    assert code == 1 and needle in out


def test_wrong_phase_is_refused(stages, tmp_path, monkeypatch, capsys):
    stage, m = stages
    code, out = gate(monkeypatch, capsys, "m", write_auth(tmp_path, stage, m), stage)
    assert code == 1 and "is not phase m" in out


def test_absent_authorization_file_is_refused_cleanly(stages, tmp_path, monkeypatch, capsys):
    stage, m = stages
    code, out = gate(monkeypatch, capsys, "x0", tmp_path / "AUTH_X0.txt", stage)
    assert code == 1 and "owner authorization file absent" in out
