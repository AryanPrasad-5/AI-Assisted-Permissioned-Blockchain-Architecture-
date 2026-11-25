"""check_auth_m2.py (owner authorization gate) and static checks of the shell/verify scripts. No process is started."""
import hashlib
import json
import py_compile
import runpy
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
OPS = ROOT / "Fabric_Live_Testbed" / "closed_loop_m2" / "ops"
for p in (ROOT, ROOT / "src", ROOT / "scripts" / "e1"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from Fabric_Live_Testbed.closed_loop_m2 import protocol_m2 as P  # noqa: E402
from Fabric_Live_Testbed.closed_loop_m2 import stage_m2 as ST  # noqa: E402

WINDOW = {"start_s": 20400, "segment_rates_tps": [6.0, 8.0, 10.0], "preroll_rate_tps": 8.0, "warmup_rates_tps": [8.0] * 10}
POINTS = [{"B": b, "rate_tps": float(r), "mean_ms": 100.0, "p95_ms": 200.0, "delivered_fraction": 1.0} for b in (2, 5, 10) for r in (5, 60)]
REACTIVE = {"latency_high_ms": 300.0, "latency_low_ms": 240.0, "consecutive_samples": 2, "cooldown_samples": 0, "max_telemetry_age_s": 5.0,
            "congested_B": 5, "default_B": 2}


def kv(stage_sha, runs, channels, **over):
    d = {"approved_by": "owner", "approved_utc": "2026-11-01T00:00:00Z", "expires_utc": "2099-01-01T00:00:00Z", "stage_sha256": stage_sha,
         "runs": ",".join(runs), "broadcast_channels": ",".join(channels), "broadcast_targets": "2,5,10", "max_broadcasts_per_run": "10",
         "native_verification": "required"}
    d.update(over)
    return "".join(f"{k}={v}\n" for k, v in d.items()).encode("ascii")


def gate(phase, auth: Path, stage: Path) -> int:
    old = sys.argv
    sys.argv = ["check_auth_m2.py", phase, str(auth), str(stage)]
    try:
        runpy.run_path(str(OPS / "check_auth_m2.py"), run_name="__main__")
    except SystemExit as exc:
        return int(exc.code)
    finally:
        sys.argv = old
    return -1


@pytest.fixture(scope="module")
def stages(tmp_path_factory):
    base = tmp_path_factory.mktemp("gate")
    m = ST.stage_m2(base / "m2", window=WINDOW, k=1.0, reactive=REACTIVE, surface_points=POINTS, provenance={}, replicates=1)
    a = ST.stage_act(base / "act")
    return base, m, a


def sha(stage):
    return hashlib.sha256((stage / "SHA256SUMS.txt").read_bytes()).hexdigest()


def test_m2_gate_accepts_exactly_the_staged_runs_channels_and_menu_targets(stages, tmp_path, capsys):
    base, m, _ = stages
    runs, ch = [r["run_id"] for r in m["runs"]], [r["channel"] for r in m["runs"]]
    f = tmp_path / "AUTH_M2.txt"
    f.write_bytes(kv(sha(base / "m2"), runs, ch))
    assert gate("m2", f, base / "m2") == 0 and "M2_AUTH ACCEPTED" in capsys.readouterr().out


@pytest.mark.parametrize("over,why", [({"broadcast_targets": "10,40"}, "targets"), ({"broadcast_targets": "2,5"}, "targets"),
                                      ({"native_verification": "waived"}, "native"), ({"max_broadcasts_per_run": "0"}, "max_broadcasts"),
                                      ({"max_broadcasts_per_run": "11"}, "max_broadcasts"), ({"approved_by": "assistant"}, "owner"),
                                      ({"stage_sha256": "0" * 64}, "stage")])
def test_m2_gate_refuses_every_deviation(stages, tmp_path, over, why, capsys):
    base, m, _ = stages
    runs, ch = [r["run_id"] for r in m["runs"]], [r["channel"] for r in m["runs"]]
    f = tmp_path / "AUTH.txt"
    f.write_bytes(kv(sha(base / "m2"), runs, ch, **over))
    assert gate("m2", f, base / "m2") == 1
    assert "REFUSED" in capsys.readouterr().out


def test_m2_gate_refuses_missing_file_extra_run_wrong_kind_and_earlier_runs(stages, tmp_path, capsys):
    base, m, act = stages
    runs, ch = [r["run_id"] for r in m["runs"]], [r["channel"] for r in m["runs"]]
    assert gate("m2", tmp_path / "absent.txt", base / "m2") == 1
    f = tmp_path / "A.txt"
    f.write_bytes(kv(sha(base / "m2"), runs[:-1], ch[:-1]))
    assert gate("m2", f, base / "m2") == 1                                           # one staged run missing from the authorization
    f.write_bytes(kv(sha(base / "act"), ["ca01"], ["e1-ca01"], max_broadcasts_per_run="4"))
    assert gate("m2", f, base / "act") == 1                                          # an ACT stage under the m2 phase
    f.write_bytes(kv(sha(base / "m2"), runs + ["cm01"], ch + ["e1-cm01"]))
    assert gate("m2", f, base / "m2") == 1
    assert "earlier experiment" in capsys.readouterr().out


def test_act_gate_needs_exactly_the_scheduled_number_of_broadcasts(stages, tmp_path):
    base, _, act = stages
    f = tmp_path / "AUTH_ACT.txt"
    f.write_bytes(kv(sha(base / "act"), ["ca01"], ["e1-ca01"], max_broadcasts_per_run="4"))
    assert gate("act", f, base / "act") == 0
    f.write_bytes(kv(sha(base / "act"), ["ca01"], ["e1-ca01"], max_broadcasts_per_run="10"))
    assert gate("act", f, base / "act") == 1


def test_gate_leaves_the_frozen_actuator_range_untouched():
    import e1_config_actuator as ca
    assert (ca.DEFAULT_MIN_B, ca.DEFAULT_MAX_B) == (10, 800)


def test_scripts_compile_and_shell_scripts_are_lf_only_and_reference_m2_paths():
    py_compile.compile(str(OPS / "verify_m2.py"), cfile=str(Path(__file__).parent / "_verify_m2.pyc"), doraise=True)
    (Path(__file__).parent / "_verify_m2.pyc").unlink()
    for sh in OPS.glob("*.sh"):
        raw = sh.read_bytes()
        assert b"\r" not in raw, f"{sh.name} has CR characters (WSL bash would fail)"
    launch = (OPS / "launch_m2.sh").read_text()
    assert "closed_loop_m2.runner_m2" in launch and "verify_m2.py" in launch and "check_auth_m2.py" in launch
    assert "closed_loop_v2.runner_v2" not in launch and "x0|m" not in launch
    assert "three_controller_comparison_2026-11-25" in launch and "decisions_m2" in launch
    assert "ca01" in launch and "act:ca01" in launch
    freeze = (OPS / "freeze_sources_m2.sh").read_text()
    for must in ("e1_config_actuator_m2.py", "e1_actuator_range_m2.py", "closed_loop_m2/*.py", "closed_loop_m2/ops/*.sh", "e1_config_actuator.py"):
        assert must in freeze


def test_runs_listed_by_the_driver_equal_the_staged_run_ids():
    drv = (OPS / "run_phase_detached_m2.sh").read_text()
    assert "cc01 cc02 cc03 cc04 cc05 cc06 cc07 cc08 cc09 cc10 cc11 cc12" in drv and 'act) RUNS="ca01"' in drv
    assert [f"{P.RUN_PREFIX}{i:02d}" for i in range(1, 13)] == "cc01 cc02 cc03 cc04 cc05 cc06 cc07 cc08 cc09 cc10 cc11 cc12".split()
    assert P.ACT_RUN_ID == "ca01"
    assert json.dumps(P.protocol_doc())
