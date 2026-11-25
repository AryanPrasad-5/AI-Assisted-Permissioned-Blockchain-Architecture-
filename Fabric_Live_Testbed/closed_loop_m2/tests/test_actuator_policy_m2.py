"""Versioned actuator range policy ACT-RANGE-M2-v1: pin, scoped patch, CLI refusal. Never touches the frozen file."""
import hashlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
for p in (ROOT, ROOT / "src", ROOT / "scripts" / "e1"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import e1_actuator_range_m2 as pol  # noqa: E402
import e1_config_actuator as ca  # noqa: E402

CFG = {"channel_group": {"groups": {"Orderer": {"values": {"BatchSize": {"value": {"max_message_count": 10}}}}}}}


def auth_file(tmp_path, targets):
    kv = {"approved_by": "owner", "approved_utc": "2026-11-01T00:00:00Z", "expires_utc": "2099-01-01T00:00:00Z", "stage_sha256": "a" * 64,
          "runs": "cc01", "broadcast_channels": "e1-cc01", "broadcast_targets": targets, "max_broadcasts_per_run": "10"}
    p = tmp_path / "AUTH.txt"
    p.write_bytes("".join(f"{k}={v}\n" for k, v in kv.items()).encode("ascii"))
    return p


def test_the_pin_equals_the_actuator_on_disk_and_the_frozen_file_is_untouched():
    assert hashlib.sha256(Path(ca.__file__).read_bytes()).hexdigest() == pol.FROZEN_ACTUATOR_SHA256
    assert (ca.DEFAULT_MIN_B, ca.DEFAULT_MAX_B) == (10, 800)                       # nothing leaked from another test or import


def test_frozen_range_refuses_2_and_5_then_the_scoped_policy_accepts_exactly_the_menu(tmp_path):
    with pytest.raises(ca.SafetyError):
        ca.updated_config(CFG, 2)
    with pytest.raises(ca.SafetyError):
        ca.BroadcastAuthorization.load(auth_file(tmp_path, "2,5,10"))
    with pol.range_applied(ca):
        assert ca.updated_config(CFG, 2)["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]["value"]["max_message_count"] == 2
        assert ca.updated_config(CFG, 5)["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]["value"]["max_message_count"] == 5
        assert ca.BroadcastAuthorization.load(auth_file(tmp_path, "2,5,10")).targets == (2, 5, 10)
        with pytest.raises(ca.SafetyError):
            ca.updated_config(CFG, 1)
        with pytest.raises(ca.SafetyError):
            ca.updated_config(CFG, 40)                                              # the policy range is tighter than the frozen one
    assert (ca.DEFAULT_MIN_B, ca.DEFAULT_MAX_B) == (10, 800)                        # restored on exit
    with pytest.raises(ca.SafetyError):
        ca.updated_config(CFG, 2)


def test_wrong_pin_refuses_and_changes_nothing():
    with pytest.raises(pol.PolicyError, match="differs from the pinned"):
        pol.apply_range(ca, expected_sha256="0" * 64)
    assert (ca.DEFAULT_MIN_B, ca.DEFAULT_MAX_B) == (10, 800)


def test_cli_checks():
    pol.check_cli(["--min-b", "2", "--max-b", "10", "apply", "--target-b", "5", "--approved-target", "5", "--execute"])
    for bad in (["--min-b", "1"], ["--max-b", "40"], ["apply", "--target-b", "40"], ["apply", "--target-b", "3"], ["apply", "--approved-target", "800"]):
        with pytest.raises(pol.PolicyError):
            pol.check_cli(bad)


def test_wrapper_script_refuses_before_importing_the_actuator_on_a_bad_target(monkeypatch):
    import e1_config_actuator_m2 as w
    with pytest.raises(pol.PolicyError):
        w.main(["--channel", "e1-cc01", "apply", "--target-b", "40"])
    called = {}
    monkeypatch.setattr(ca, "main", lambda argv: called.setdefault("argv", argv) and 0)
    assert w.main(["--min-b", "2", "--max-b", "10", "inspect"]) == 0 and called["argv"][-1] == "inspect"
    assert (ca.DEFAULT_MIN_B, ca.DEFAULT_MAX_B) == (2, 10)                          # the subprocess keeps the policy for its lifetime
    pol.restore_range(ca, {"DEFAULT_MIN_B": 10, "DEFAULT_MAX_B": 800})              # test hygiene: this process is not an actuator subprocess


def test_policy_doc_names_the_pin_and_the_menu():
    d = pol.policy_doc()
    assert d["menu_B"] == [2, 5, 10] and d["frozen_actuator_sha256"] == pol.FROZEN_ACTUATOR_SHA256 and (d["min_b"], d["max_b"]) == (2, 10)
