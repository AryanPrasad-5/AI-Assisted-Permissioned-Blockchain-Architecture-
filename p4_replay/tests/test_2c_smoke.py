"""Smoke: one complete DRY launch on the fake backend (kept as a cheap end-to-end canary)."""

from p4_replay import launch_ids as li
from p4_replay.tests import support_2c as s2


def test_a_dry_launch_completes_offline(tmp_path):
    L = s2.prepare_launch(tmp_path, li.DRY)
    summary = L.run()
    print(summary["status"], summary.get("failed_gates"), summary.get("halted"), [(r["run_id"], r["verdict"], r["class"]) for r in summary["runs"]])
    assert summary["status"] == "COMPLETE", summary
