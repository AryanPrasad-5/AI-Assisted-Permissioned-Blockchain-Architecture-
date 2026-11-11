import multiprocessing
from pathlib import Path

import pytest

from fabric_perf.controllers.transition_fence import ChannelTransitionFence, FenceError


def test_durable_unknown_blocks_duplicates_and_restart(tmp_path):
    first = ChannelTransitionFence(tmp_path, "e1-cfg001")
    with first.channel_lock():
        first.begin("tx-1", {"max_message_count": 45})
        first.mark_submitting("tx-1")
        first.mark_unknown("tx-1", "client timeout after submit began")
    restarted = ChannelTransitionFence(tmp_path, "e1-cfg001")
    assert restarted.read()["status"] == "UNKNOWN"
    with restarted.channel_lock(), pytest.raises(FenceError, match="unresolved"):
        restarted.begin("tx-2", {"max_message_count": 40})


def test_only_complete_independent_reconciliation_resolves_unknown(tmp_path):
    fence = ChannelTransitionFence(tmp_path, "e1-cfg001")
    with fence.channel_lock():
        fence.begin("tx-1", {"B": 45})
        fence.mark_submitting("tx-1")
        fence.mark_unknown("tx-1", "timeout")
        with pytest.raises(FenceError, match="independent"):
            fence.reconcile("tx-1", observed_config={"B": 45}, expected_config={"B": 45}, evidence={})
        result = fence.reconcile("tx-1", observed_config={"B": 45}, expected_config={"B": 45},
                                 evidence={"independent": True, "complete": True, "outcome": "COMMITTED", "block": 5})
    assert result["status"] == "VERIFIED"


def test_corrupt_state_fails_closed_and_channels_are_isolated(tmp_path):
    one = ChannelTransitionFence(tmp_path, "e1-cfg001")
    two = ChannelTransitionFence(tmp_path, "e1-r002")
    with one.channel_lock():
        one.begin("tx-1", {"B": 45})
        one.state_path.write_text("{broken", encoding="utf-8")
        with pytest.raises(FenceError, match="unreadable"):
            one.begin("tx-2", {"B": 40})
    with two.channel_lock():
        assert two.begin("tx-3", {"B": 45})["channel"] == "e1-r002"


def _try_lock(root: str, queue):
    fence = ChannelTransitionFence(Path(root), "e1-cfg001")
    try:
        with fence.channel_lock():
            queue.put("acquired")
    except FenceError:
        queue.put("blocked")


def test_cross_process_lock_is_channel_scoped(tmp_path):
    fence = ChannelTransitionFence(tmp_path, "e1-cfg001")
    queue = multiprocessing.Queue()
    with fence.channel_lock():
        proc = multiprocessing.Process(target=_try_lock, args=(str(tmp_path), queue))
        proc.start()
        proc.join(timeout=5)
    assert proc.exitcode == 0
    assert queue.get(timeout=1) == "blocked"


def test_prepare_failure_can_be_marked_definitely_not_submitted(tmp_path):
    fence = ChannelTransitionFence(tmp_path, "e1-cfg001")
    with fence.channel_lock():
        fence.begin("tx-1", {"B": 45})
        result = fence.mark_not_submitted("tx-1", "preflight rejected before network call")
    assert result["status"] == "NOT_SUBMITTED"


def test_channel_cooldown_persists_across_fence_instances_and_directories(tmp_path):
    first = ChannelTransitionFence(tmp_path / "shared-state", "e1-cfg001", cooldown_s=3600)
    with first.channel_lock():
        first.begin("tx-1", {"B": 40})
        first.mark_submitting("tx-1")
        first.reconcile("tx-1", observed_config={"B": 40}, expected_config={"B": 40},
                        evidence={"independent": True, "complete": True, "outcome": "COMMITTED"})
    second = ChannelTransitionFence(tmp_path / "shared-state", "e1-cfg001", cooldown_s=3600)
    with second.channel_lock(), pytest.raises(FenceError, match="cooldown"):
        second.begin("tx-2", {"B": 45})
    other = ChannelTransitionFence(tmp_path / "shared-state", "e1-r002", cooldown_s=3600)
    with other.channel_lock():
        assert other.begin("other-1", {"B": 45})["channel"] == "e1-r002"


def test_reconciliation_cannot_infer_not_submitted_from_config_mismatch(tmp_path):
    fence = ChannelTransitionFence(tmp_path, "e1-cfg001")
    with fence.channel_lock():
        fence.begin("tx-1", {"B": 40})
        fence.mark_submitting("tx-1")
        with pytest.raises(FenceError, match="must establish VERIFIED"):
            fence.reconcile("tx-1", observed_config={"B": 45}, expected_config={"B": 40},
                            evidence={"independent": True, "complete": True})


def test_explicit_prestate_resolution_requires_exact_operator_token(tmp_path):
    fence = ChannelTransitionFence(tmp_path, "e1-cfg001")
    before = {"B": 45}
    with fence.channel_lock():
        fence.begin("tx-1", {"B": 40}, context={"pre_config": before})
        fence.mark_submitting("tx-1")
        evidence = {"independent": True, "complete": True, "outcome": "NOT_SUBMITTED", "safe_to_retry": True}
        with pytest.raises(FenceError, match="authorization"):
            fence.resolve_not_submitted("tx-1", observed_config=before, evidence=evidence,
                                        explicit_authorization="yes")
        result = fence.resolve_not_submitted("tx-1", observed_config=before, evidence=evidence,
                                             explicit_authorization="RESOLVE-NOT-SUBMITTED:tx-1")
        assert result["status"] == "NOT_SUBMITTED"
        with pytest.raises(FenceError, match="already recorded"):
            fence.begin("tx-1", {"B": 40})


def test_unresolved_observation_is_persisted_without_clearing_fence(tmp_path):
    fence = ChannelTransitionFence(tmp_path, "e1-cfg001")
    with fence.channel_lock():
        fence.begin("tx-1", {"B": 40})
        fence.mark_submitting("tx-1")
        result = fence.record_unresolved_observation("tx-1", {"outcome": "UNEXPECTED_STATE", "block": 9})
    assert result["status"] == "SUBMITTING"
    assert result["observations"] == [{"outcome": "UNEXPECTED_STATE", "block": 9}]
