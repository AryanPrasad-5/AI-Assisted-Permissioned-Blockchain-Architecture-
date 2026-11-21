"""A0m follow-up tests (2026-11-21), SYNTHETIC and offline. The A0l tests are not edited; these add to them.

1. M21 (the A0l mutation "a resume accepts a shorter log than the checkpoint"): the A0l tests only checked the error CODE of a truncated resume. The early size check
   and the later "log ended" check both raise ``log_truncated``, so a code-only assertion cannot tell them apart. They differ in what they say and in what they
   do first: the early check refuses without reading the log, so a log that is both truncated AND damaged in its surviving prefix is reported as truncated.
2. Reader lag versus a genuine surplus (A0l limitation 5): both read as ``recorder_exceeds_source`` AT THE DECISION TIME. They differ once the log has been read:
   lag resolves, a genuine surplus does not. Nothing here sets a polling cadence.
"""

from __future__ import annotations

import json

import pytest

from tests import a0j_support as j
from tests import a0k_support as k
from tests import a0l_support as a
from tests.a0j_support import SHORT, at
from tests.test_a0l_coverage_2026_11_21 import r1_evidence
from tests.test_a0l_reader_2026_11_21 import _consume_in_steps, _lines_of, _tamper, _write_lines

ilr = a.ilr


def _resume_error(log, ckpt):
    with pytest.raises(ilr.IssueLogError) as info:
        ilr.IssueLogTail.resume(log.path, a.policy(max_tick_gap_ms=a.SHORT_GAP_MS), ckpt)
    return info.value


def test_m21_a_truncated_resume_says_the_log_is_shorter_than_the_checkpoint_offset(tmp_path):
    log, _gen, tail, ckpt = _consume_in_steps(tmp_path)
    log.path.write_bytes(log.path.read_bytes()[:-30])
    err = _resume_error(log, ckpt)
    assert err.code == "log_truncated"
    assert "shorter than the checkpoint offset" in str(err) and str(tail.offset) in str(err)     # the early size check, not the late "log ended" check


def test_m21_a_truncated_log_with_a_damaged_surviving_prefix_is_reported_as_truncated_before_the_log_is_read(tmp_path):
    log, _gen, _tail, ckpt = _consume_in_steps(tmp_path)
    lines = _lines_of(log)
    lines[1] = _tamper(lines[1], "t_ms", json.loads(lines[1])["t_ms"] + 1)
    _write_lines(log, lines)
    log.path.write_bytes(log.path.read_bytes()[:-30])
    err = _resume_error(log, ckpt)
    assert err.code == "log_truncated" and "shorter than the checkpoint offset" in str(err)


def test_m21_without_truncation_the_same_damage_is_still_reported_as_damage(tmp_path):
    log, _gen, _tail, ckpt = _consume_in_steps(tmp_path)
    lines = _lines_of(log)
    lines[1] = _tamper(lines[1], "t_ms", json.loads(lines[1])["t_ms"] + 1)
    _write_lines(log, lines)
    assert _resume_error(log, ckpt).code in {"hash_mismatch", "record_not_canonical", "log_replaced"}


def test_lag_reads_as_a_failed_attestation_at_the_decision_time_and_resolves_once_the_record_is_read(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    evidence, _reader, _gen = r1_evidence(tmp_path, rec, latency=3.0)
    _r, cov, _e = k.recorder_coverage(rec, tmp_path, source_evidence=evidence)
    early, late = cov.snapshot(at(8)), cov.snapshot(at(SHORT + 30))
    assert not early.valid and early.failure_code == "recorder_exceeds_source"           # fail closed: not valid, not a verified zero, not "no source activity"
    assert late.valid and late.classify(at(5), 0)[0] == a.C.VERIFIED_ZERO                # temporary: it resolved when the issuance records were read


def test_a_genuine_surplus_gives_the_same_code_early_and_keeps_it_after_every_record_has_been_read(tmp_path):
    rec = j.short_recorder(tmp_path, zero={5}, source_count=False)
    evidence, _reader, _gen = r1_evidence(tmp_path, rec, skip_tx={8, 9}, latency=0.0)    # the recorder counted seconds 8 and 9; the generator never logged them
    _r, cov, _e = k.recorder_coverage(rec, tmp_path, source_evidence=evidence)
    early, late = cov.snapshot(at(12)), cov.snapshot(at(SHORT + 30))
    assert not early.valid and early.failure_code == "recorder_exceeds_source"
    assert not late.valid and late.failure_code == "recorder_exceeds_source"             # persistent: the only observable difference from lag
