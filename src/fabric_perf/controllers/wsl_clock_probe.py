"""Windows <-> WSL clock-offset sample collector, offline and injection-only (task A0s, 2026-11-22).

Why. ``clock_preflight`` (A0e) decides AGREE / DISAGREE / INCONCLUSIVE from round-trip samples but deliberately contains no probe (``ClockProbe`` is an
interface). The older ``scripts/e1/e1_clock_handshake.py`` measures host <-> CONTAINER offsets through ``docker exec`` and writes ``offset_ns`` / ``uncertainty_ns``
rows; those are a different format and a different pair of clocks, so it cannot feed the A0e evaluator. This module is the smallest compatible collector: it
produces samples in the exact ``clock_preflight.parse_sample`` shape (reference = Windows, remote = WSL) and nothing else.

What it records per sample (units: integer nanoseconds unless a name says otherwise)::

    ref_wall_before_ns, ref_wall_after_ns   Windows wall clock (Unix epoch, UTC) just before and just after the request
    ref_mono_before_ns, ref_mono_after_ns   Windows MONOTONIC clock at the same two instants; elapsed time only, never an instant on the other host
    remote_wall_ns                          the WSL wall clock reading (``date +%s%N``), integer nanoseconds since the Unix epoch
    remote_resolution_ns                    operator-declared resolution of that reading (required; no default)
    seq, sampling_id, phase                 sampling identity; ``seq`` increases within a phase and the request/response windows may not overlap
    reference_domain, remote_domain         clock-domain labels; every sample of a phase carries the same pair
    remote_mono_ns (optional)               a WSL monotonic reading when the command returns one; recorded as ``remote_mono_comparable_across_hosts: false`` and
                                            never used: monotonic clocks of two hosts share no origin

Rules enforced (each failure is a stable code, never repaired): the remote reading must be ONLY digits and exactly ``remote_digits`` long (19 for nanoseconds;
a seconds-only ``date`` or a literal ``%N`` is rejected, not rescaled); the Windows wall clock may not go backwards and the monotonic clock must advance
(``ref_mono_after > ref_mono_before``); the order wall_before <= wall_after and the non-overlap of consecutive samples are checked; every sample of a phase
shares one domain-label pair and one ``sampling_id``. A UTC label is not evidence of agreement; the evaluator decides that from the interval.

What this module does NOT do: start WSL, call ``wsl.exe``, read a clock for real (the clock functions are injected), or sleep. ``LIVE_CLOCK_SAMPLING_ENABLED`` is
False; a runner of kind ``process`` is refused. The command line in ``probe_plan`` is TEXT: that ``wsl.exe -d <distro> --exec date +%s%N`` works on the
target machine, and what its round trip is, are UNVERIFIED and unmeasured. A real run would likely be dominated by process start-up (the uncertainty is reported
per sample, and the evaluator returns INCONCLUSIVE when it straddles the tolerance).
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Callable, Mapping, Sequence

from . import clock_preflight as cp

PROBE_VERSION = "wsl_clock_probe/1 (A0s, 2026-11-22)"
PHASE_SCHEMA = "wsl_clock_phase/1"
LIVE_CLOCK_SAMPLING_ENABLED = False
REMOTE_DIGITS_NS = 19                      # nanoseconds since the epoch have 19 digits until the year 2286
_DIGITS = re.compile(r"[0-9]+")
_LABEL = re.compile(r"[A-Za-z0-9_.:/-]{1,80}")
_DISTRO = re.compile(r"[A-Za-z0-9_.-]{1,64}")
SAMPLE_KEYS = ("ref_wall_before_ns", "remote_wall_ns", "ref_wall_after_ns", "ref_mono_before_ns", "ref_mono_after_ns", "remote_resolution_ns")


class ProbeRefused(ValueError):
    """The collector is not permitted to run (real sampling is disabled) or was configured wrongly. Nothing was sampled."""


class SampleInvalid(ValueError):
    """One sample cannot be used. ``code`` is a stable machine-readable cause; the sample is reported, never repaired."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def parse_remote_reading(text: Any, *, remote_digits: int = REMOTE_DIGITS_NS) -> tuple[int, int | None]:
    """Return (remote wall ns, optional remote monotonic ns) from the command's stdout, or raise :class:`SampleInvalid`."""
    if isinstance(text, (bytes, bytearray)):
        try:
            text = bytes(text).decode("ascii")
        except UnicodeDecodeError:
            raise SampleInvalid("remote_reading_malformed", "the remote output is not ASCII") from None
    if not isinstance(text, str):
        raise SampleInvalid("remote_reading_malformed", f"the remote output must be text, got {type(text).__name__}")
    lines = [line.strip() for line in text.replace("\r\n", "\n").split("\n") if line.strip()]
    if not lines:
        raise SampleInvalid("remote_reading_missing", "the remote command printed nothing")
    if len(lines) > 2:
        raise SampleInvalid("remote_reading_malformed", f"expected 1 or 2 lines, got {len(lines)}")
    values = []
    for line in lines:
        if not _DIGITS.fullmatch(line):
            raise SampleInvalid("remote_reading_malformed", f"{line[:40]!r} is not a plain integer (a literal %N or a suffix is refused)")
        values.append(int(line))
    if len(lines[0]) != remote_digits:
        raise SampleInvalid("remote_unit_unexpected", f"the wall reading has {len(lines[0])} digits, expected {remote_digits} (integer nanoseconds); it is not rescaled")
    return values[0], (values[1] if len(values) == 2 else None)


def probe_plan(distro: Any, *, remote_resolution_ns: Any, timeout_s: Any) -> dict[str, Any]:
    """The text of one probe request. Nothing is executed. The command line is UNVERIFIED on the target machine."""
    if not isinstance(distro, str) or not _DISTRO.fullmatch(distro):
        raise ProbeRefused(f"distro {distro!r} must match {_DISTRO.pattern} and is required (no default)")
    for name, value in (("remote_resolution_ns", remote_resolution_ns), ("timeout_s", timeout_s)):
        if value is None or isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise ProbeRefused(f"{name} must be a number greater than zero (operator-supplied, no default)")
    return {"schema": "wsl_clock_probe_plan/1", "version": PROBE_VERSION, "argv": ["wsl.exe", "-d", distro, "--exec", "date", "+%s%N"],
            "remote_digits": REMOTE_DIGITS_NS, "remote_resolution_ns": int(remote_resolution_ns), "timeout_s": float(timeout_s),
            "reference_host": "Windows (time.time_ns / time.monotonic_ns, injected)", "remote_host": f"WSL distro {distro}",
            "verified_on_target": False, "live_sampling_enabled": LIVE_CLOCK_SAMPLING_ENABLED,
            "note": "text only. That this command works, and its round-trip time, are unmeasured. Process start-up is likely to dominate the uncertainty."}


class WslClockProbe:
    """One phase of samples through an injected runner and injected clocks. ``sample()`` matches the ``clock_preflight.ClockProbe`` interface."""

    def __init__(self, runner: Any, *, wall_ns: Callable[[], int], mono_ns: Callable[[], int], argv: Sequence[str], remote_resolution_ns: int,
                 timeout_s: float, phase: str, reference_domain: str, remote_domain: str, sampling_id: str | None = None):
        kind = getattr(runner, "kind", None)
        if kind not in ("mock", "fixture"):
            if kind == "process" and not LIVE_CLOCK_SAMPLING_ENABLED:
                raise ProbeRefused("real clock sampling is disabled in this version; a process runner is refused")
            raise ProbeRefused(f"runner kind {kind!r} is not accepted (only 'mock' or 'fixture' while real sampling is disabled)")
        for name, value in (("phase", phase), ("reference_domain", reference_domain), ("remote_domain", remote_domain)):
            if not isinstance(value, str) or not _LABEL.fullmatch(value):
                raise ProbeRefused(f"{name} {value!r} must match {_LABEL.pattern} (required, no default)")
        if reference_domain == remote_domain:
            raise ProbeRefused("reference_domain and remote_domain must name two different clocks")
        if isinstance(remote_resolution_ns, bool) or not isinstance(remote_resolution_ns, int) or remote_resolution_ns <= 0:
            raise ProbeRefused("remote_resolution_ns must be a positive integer (operator-declared)")
        if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)) or timeout_s <= 0:
            raise ProbeRefused("timeout_s must be greater than zero")
        self.runner, self._wall, self._mono = runner, wall_ns, mono_ns
        self._call = runner if callable(runner) else self._via_run_method(runner)
        self.argv, self.resolution, self.timeout_s = list(argv), int(remote_resolution_ns), float(timeout_s)
        self.phase, self.reference_domain, self.remote_domain = phase, reference_domain, remote_domain
        self.sampling_id = sampling_id or hashlib.sha256(json.dumps([self.argv, phase, reference_domain, remote_domain], sort_keys=True).encode()).hexdigest()[:16]
        self.seq = 0
        self.invalid: list[dict[str, Any]] = []

    @staticmethod
    def _via_run_method(runner: Any) -> Callable[[Sequence[str], float], tuple[Any, Any, Any]]:
        """A runner object with ``run(argv, timeout_s, cwd)`` returning an object with returncode/stdout/stderr/timed_out is used through this adapter."""
        def call(argv: Sequence[str], timeout_s: float) -> tuple[Any, Any, Any]:
            result = runner.run(list(argv), timeout_s, None)
            return (124 if getattr(result, "timed_out", False) else result.returncode), result.stdout, result.stderr
        return call

    def sample(self) -> Mapping[str, Any]:
        """One round trip. Raises :class:`SampleInvalid` (and records it in ``self.invalid``) when the sample cannot be used."""
        self.seq += 1
        seq = self.seq
        try:
            wall_before, mono_before = self._wall(), self._mono()
            code, stdout, stderr = self._call(self.argv, self.timeout_s)
            mono_after, wall_after = self._mono(), self._wall()
            if code != 0:
                raise SampleInvalid("remote_command_failed", f"the remote command exited {code}")
            remote_wall, remote_mono = parse_remote_reading(stdout)
            if wall_after < wall_before:
                raise SampleInvalid("wall_clock_went_backwards", f"Windows wall clock moved back {wall_before - wall_after} ns during the sample")
            if mono_after <= mono_before:
                raise SampleInvalid("monotonic_not_advancing", f"Windows monotonic clock did not advance ({mono_before} -> {mono_after})")
        except SampleInvalid as exc:
            self.invalid.append({"seq": seq, "code": exc.code, "message": str(exc)})
            raise
        row = {"seq": seq, "sampling_id": self.sampling_id, "phase": self.phase, "reference_domain": self.reference_domain, "remote_domain": self.remote_domain,
               "ref_wall_before_ns": wall_before, "remote_wall_ns": remote_wall, "ref_wall_after_ns": wall_after,
               "ref_mono_before_ns": mono_before, "ref_mono_after_ns": mono_after, "remote_resolution_ns": self.resolution,
               "round_trip_ns": wall_after - wall_before, "read_order": "wall_before,mono_before,request,mono_after,wall_after",
               "unit": "ns", "ref_wall_label": "Unix epoch UTC (Windows)", "ref_mono_label": "Windows monotonic: elapsed time only"}
        if remote_mono is not None:
            row.update(remote_mono_ns=remote_mono, remote_mono_comparable_across_hosts=False)
        return row


def collect_phase(probe: WslClockProbe, count: int) -> dict[str, Any]:
    """Take ``count`` samples. A bad sample is recorded with its code and sampling continues; the phase record lists both."""
    if isinstance(count, bool) or not isinstance(count, int) or count < 1:
        raise ProbeRefused("count must be a positive integer")
    samples: list[Mapping[str, Any]] = []
    for _ in range(count):
        try:
            samples.append(probe.sample())
        except SampleInvalid:
            pass
    return {"schema": PHASE_SCHEMA, "version": PROBE_VERSION, "phase": probe.phase, "sampling_id": probe.sampling_id, "count_requested": count,
            "count_collected": len(samples), "unit": "ns", "reference_domain": probe.reference_domain, "remote_domain": probe.remote_domain,
            "remote_resolution_ns": probe.resolution, "samples": samples, "invalid": list(probe.invalid),
            "utc_label_is_not_evidence": True,
            "note": "samples only; agreement is decided by clock_preflight from the offset interval, not from a label"}


def check_phase(record: Mapping[str, Any]) -> list[dict[str, str]]:
    """Structural problems of one stored phase record (also for a record read from a file). An empty list means no structural problem."""
    problems: list[dict[str, str]] = []

    def bad(code: str, message: str) -> None:
        problems.append({"code": code, "message": message})

    if record.get("schema") != PHASE_SCHEMA:
        bad("phase_schema", f"schema is {record.get('schema')!r}")
        return problems
    samples = record.get("samples")
    if not isinstance(samples, list):
        bad("samples_not_a_list", "samples must be a list")
        return problems
    last_after = None
    last_seq = 0
    for index, sample in enumerate(samples):
        if not isinstance(sample, Mapping):
            bad("sample_not_an_object", f"sample {index}")
            continue
        if any(sample.get(k) is None for k in SAMPLE_KEYS):
            bad("missing_sample", f"sample {index} lacks one of {SAMPLE_KEYS}")
            continue
        if (sample.get("reference_domain"), sample.get("remote_domain")) != (record.get("reference_domain"), record.get("remote_domain")):
            bad("clock_domain_mismatch", f"sample {index} carries domains ({sample.get('reference_domain')!r}, {sample.get('remote_domain')!r})")
        if sample.get("sampling_id") != record.get("sampling_id"):
            bad("sampling_identity_mismatch", f"sample {index} has another sampling_id")
        if sample.get("unit") != "ns":
            bad("unit_mismatch", f"sample {index} unit is {sample.get('unit')!r}")
        if sample["ref_wall_after_ns"] < sample["ref_wall_before_ns"]:
            bad("invalid_sample_ordering", f"sample {index}: wall_after < wall_before")
        if sample["ref_mono_after_ns"] <= sample["ref_mono_before_ns"]:
            bad("invalid_sample_ordering", f"sample {index}: monotonic did not advance")
        seq = sample.get("seq")
        if not isinstance(seq, int) or seq <= last_seq:
            bad("sequence_not_increasing", f"sample {index} seq {seq!r} after {last_seq}")
        else:
            last_seq = seq
        if last_after is not None and sample["ref_mono_before_ns"] < last_after:
            bad("overlapping_samples", f"sample {index} began before the previous sample ended (monotonic)")
        last_after = sample["ref_mono_after_ns"]
        if sample.get("remote_mono_ns") is not None and sample.get("remote_mono_comparable_across_hosts") is not False:
            bad("monotonic_cross_host_use", f"sample {index} carries a remote monotonic value that is not labelled non-comparable")
    if record.get("count_collected") != len(samples):
        bad("count_mismatch", f"count_collected {record.get('count_collected')} != {len(samples)} samples")
    return problems


def to_evaluator_phases(records: Sequence[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    """Phase records -> the mapping ``clock_preflight.assess_phases`` takes. Refuses a structural problem or a domain change between phases."""
    if not records:
        raise ProbeRefused("no phase records")
    pair = None
    out: dict[str, list[Mapping[str, Any]]] = {}
    for record in records:
        problems = check_phase(record)
        if problems:
            raise ProbeRefused(f"phase {record.get('phase')!r} has structural problems: {problems}")
        this_pair = (record["reference_domain"], record["remote_domain"])
        if pair is not None and this_pair != pair:
            raise ProbeRefused(f"clock-domain labels differ between phases: {pair} vs {this_pair}")
        pair = this_pair
        if record["phase"] in out:
            raise ProbeRefused(f"phase {record['phase']!r} appears twice")
        out[record["phase"]] = list(record["samples"])
    return out


def evaluate(records: Sequence[Mapping[str, Any]], *, tolerance_ms: Any, min_valid_samples: Any, max_wall_monotonic_disagreement_ms: Any) -> dict[str, Any]:
    """Hand the samples to the A0e evaluator. The verdict is evidence about these samples only."""
    return cp.assess_phases(to_evaluator_phases(records), tolerance_ms=tolerance_ms, min_valid_samples=min_valid_samples,
                            max_wall_monotonic_disagreement_ms=max_wall_monotonic_disagreement_ms)
