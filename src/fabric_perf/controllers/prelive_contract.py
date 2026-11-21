"""One operator-parameter contract for the pre-live validation workflow (task A0e, 2026-11-21).

The future bounded validation needs a handful of numbers and names that only the owner or operator can supply. This module
is the single place that decides whether a complete set of them is acceptable. It reads nothing, writes nothing and
contacts nothing: it takes a mapping (usually a parsed JSON file) and returns a validated record or raises one error that
lists EVERY problem found.

Rules
-----
* Nothing has a default. A missing key, a ``null`` (except where a section says an explicit ``null`` means "disabled"), a
  blank string, a boolean in a numeric field, a string in a numeric field, a non-finite number, zero or a negative number is
  an error. Nothing is repaired, rounded, converted or guessed.
* Units live in the key name (``_s`` seconds, ``_ms`` milliseconds, ``_bytes``). A key that names a known parameter with a
  different unit suffix, or with none, is refused with a message that names the key to use (``unit_mismatch``).
* The channel is validated by the shared A0b rules (``channel_selection``) and is a separate top-level key: the Fabric block
  configuration lives under ``cell`` and may not carry a channel.
* The abstention and initialization limits are validated by ``Stage3FrameQualityContract`` itself (the one validator the
  runner uses), one at a time, so a bad value in one is reported even if the other is fine.
* ``telemetry_adapter.window_s`` must be strictly greater than ``ledger_collector.poll_interval_s``. The margin is reported,
  not judged: how much jitter allowance a run needs is the operator's decision and no value is invented here.
* The ceilings in ``UNIT_SLIP_CEILINGS`` only catch a value typed in the wrong unit. They carry no scientific meaning.

What a passing result means: the values are well formed and mutually consistent. It does not mean they are scientifically
justified, approved by the owner, or that anything has been run.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Mapping, NamedTuple

from . import channel_selection as cs
from .stage3_frame_quality import Stage3FrameQualityContract

CONTRACT_VERSION = "prelive_params/1"

RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9_.-]{1,40}")           # the renderer's RUN_ID rule, written for fullmatch
WORKLOADS = ("create", "fail-every", "rmw")                     # e1_la2_render.render_run
RECORDER_LATENCY_MODES = ("column", "derive_from_timestamps")   # ContinuousCaliperLedgerAdapter.RECORDER_LATENCY_MODES
LIVE_OPTION_MIN = {"live_flush_rows": 1, "live_flush_interval_ms": 10}   # the limits recorder v2 itself enforces
RENDERER_SETTLE_SECONDS_AFTER_WARMUP = 10                        # e1_la2_render.caliper_files: settleSeconds when warm-up > 0

# Unit-slip guards (NOT scientific values): a number above these was almost certainly typed in the wrong unit.
UNIT_SLIP_CEILINGS = {"poll_interval_s": 3600.0, "window_s": 86400.0}

_ENDPOINT = re.compile(r"(?P<name>[A-Za-z0-9_.-]+)=(?P<url>https?://[^\s/]+(?:/\S*)?)")

TOP_KEYS = ("schema", "run_id", "channel", "cell", "recorder", "ledger_collector", "telemetry_adapter", "stage3",
            "telemetry_sources", "environment")
REQUIRED_TOP = tuple(k for k in TOP_KEYS if k != "recorder")      # the recorder's two live options are optional
SECTION_KEYS = {
    "cell": ("max_message_count", "batch_timeout", "preferred_max_bytes", "rate_tps", "workload", "warmup_s",
             "duration_s", "tx_count"),
    "recorder": tuple(LIVE_OPTION_MIN),
    "ledger_collector": ("poll_interval_s", "max_polls"),
    "telemetry_adapter": ("window_s", "recorder_latency"),
    "stage3": ("max_consecutive_abstentions", "max_initialization_frames"),
    "telemetry_sources": ("prometheus_endpoints",),
    "environment": ("renderer_env_path", "network_json_path"),
}
# Keys that must be present (an explicit null is only accepted where noted in the validator).
SECTION_REQUIRED = {
    "cell": ("max_message_count", "batch_timeout", "preferred_max_bytes", "rate_tps", "workload", "warmup_s"),
    "recorder": (),
    "ledger_collector": SECTION_KEYS["ledger_collector"],
    "telemetry_adapter": SECTION_KEYS["telemetry_adapter"],
    "stage3": SECTION_KEYS["stage3"],
    "telemetry_sources": SECTION_KEYS["telemetry_sources"],
    "environment": SECTION_KEYS["environment"],
}
# The parameter families whose unit suffix is checked. base name -> the exact key (with its unit) that is accepted.
UNIT_KEYS = {"poll_interval": "poll_interval_s", "window": "window_s", "warmup": "warmup_s", "duration": "duration_s",
             "live_flush_interval": "live_flush_interval_ms", "preferred_max_bytes": "preferred_max_bytes"}
_UNIT_SUFFIX = re.compile(r"_(?:s|ms|us|ns|sec|secs|seconds|milliseconds|minutes|min|bytes|kb|mb)$")


class ParameterIssue(NamedTuple):
    code: str
    field: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {"code": self.code, "field": self.field, "message": self.message}


class PreliveParameterError(ValueError):
    """The parameter set is not acceptable. ``issues`` lists every problem; nothing was repaired."""

    def __init__(self, issues: list[ParameterIssue]):
        self.issues = list(issues)
        super().__init__("; ".join(f"{i.field}: {i.message} [{i.code}]" for i in self.issues))


@dataclass(frozen=True)
class ValidatedParameters:
    """The accepted parameter set (plain values copied from the input) and the quantities derived from it."""

    values: Mapping[str, Any]
    planned_round_s: float
    collector_span_s: float
    window_margin_s: float

    def as_dict(self) -> dict[str, Any]:
        # ``values`` is exactly what validate_operator_parameters accepts again (it round-trips); derived facts live apart.
        init = self.values["stage3"]["max_initialization_frames"]
        return {"schema": CONTRACT_VERSION, "values": self.values,
                "derived": {"planned_round_s": self.planned_round_s, "collector_span_s": self.collector_span_s,
                            "window_margin_s": self.window_margin_s,
                            "initialization": "disabled_by_operator" if init is None else "bounded_by_operator",
                            "window_margin_note": "window_s - poll_interval_s; reported, not judged. The jitter allowance "
                                                  "a run needs is the operator's decision and no value is assumed."},
                "basis": "operator-supplied; well-formed and mutually consistent; no scientific justification is claimed"}


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _positive_finite(value: Any, name: str, issues: list[ParameterIssue], *, integer: bool = False,
                     allow_zero: bool = False, minimum: float | None = None) -> bool:
    """Append an issue and return False unless ``value`` is a finite number > 0 (or >= 0), an int when ``integer``."""
    if value is None:
        issues.append(ParameterIssue("missing", name, "no value was supplied; there is no default"))
        return False
    if isinstance(value, bool) or not _is_number(value):
        issues.append(ParameterIssue("wrong_type", name, f"must be a number, not {type(value).__name__}"))
        return False
    if isinstance(value, float) and not math.isfinite(value):
        issues.append(ParameterIssue("nonfinite", name, f"must be finite, got {value!r}"))
        return False
    if integer and (isinstance(value, float) and not float(value).is_integer()):
        issues.append(ParameterIssue("not_integer", name, f"must be a whole number, got {value!r}"))
        return False
    if value < 0 or (value == 0 and not allow_zero):
        issues.append(ParameterIssue("not_positive" if not allow_zero else "negative", name,
                                     f"must be {'non-negative' if allow_zero else 'greater than zero'}, got {value!r}"))
        return False
    if minimum is not None and value < minimum:
        issues.append(ParameterIssue("below_minimum", name, f"must be at least {minimum}, got {value!r}"))
        return False
    return True


def _text(value: Any, name: str, issues: list[ParameterIssue]) -> bool:
    if value is None:
        issues.append(ParameterIssue("missing", name, "no value was supplied; there is no default"))
        return False
    if not isinstance(value, str):
        issues.append(ParameterIssue("wrong_type", name, f"must be a string, not {type(value).__name__}"))
        return False
    if not value.strip():
        issues.append(ParameterIssue("empty", name, "must not be blank"))
        return False
    return True


def _section(raw: Mapping[str, Any], name: str, issues: list[ParameterIssue]) -> Mapping[str, Any] | None:
    value = raw.get(name)
    if value is None:
        if name in REQUIRED_TOP:
            issues.append(ParameterIssue("missing", name, "required section is missing"))
        return None
    if not isinstance(value, Mapping):
        issues.append(ParameterIssue("wrong_type", name, f"must be an object, not {type(value).__name__}"))
        return None
    allowed = SECTION_KEYS[name]
    for key in value:
        if key in allowed:
            continue
        if key == "channel":
            issues.append(ParameterIssue(
                "channel_in_block_configuration", f"{name}.{key}",
                "the channel is selected once, by the top-level 'channel' key; it is not a Fabric block-configuration "
                "parameter and may not appear here"))
            continue
        stripped = _UNIT_SUFFIX.sub("", str(key))
        expected = UNIT_KEYS.get(stripped)
        if expected is not None and expected in allowed:
            issues.append(ParameterIssue(
                "unit_mismatch", f"{name}.{key}",
                f"unit does not match the contract: use the key '{expected}' (the unit is part of the key name); "
                f"nothing is converted"))
        else:
            issues.append(ParameterIssue("unknown_key", f"{name}.{key}", f"not a known option; allowed: {sorted(allowed)}"))
    for key in SECTION_REQUIRED[name]:
        if key not in value:
            issues.append(ParameterIssue("missing", f"{name}.{key}", "required key is missing; there is no default"))
    return value


def validate_operator_parameters(raw: Any) -> ValidatedParameters:
    """Validate a complete parameter set. Raises :class:`PreliveParameterError` listing every problem."""
    issues: list[ParameterIssue] = []
    if not isinstance(raw, Mapping):
        raise PreliveParameterError([ParameterIssue("wrong_type", "<root>", "the parameter file must contain a JSON object")])
    if raw.get("schema") != CONTRACT_VERSION:
        issues.append(ParameterIssue("schema", "schema", f"must be {CONTRACT_VERSION!r}, got {raw.get('schema')!r}"))
    for key in raw:
        if key not in TOP_KEYS:
            stripped = _UNIT_SUFFIX.sub("", str(key))
            hint = " (the unit-bearing keys live inside their sections)" if stripped in UNIT_KEYS else ""
            issues.append(ParameterIssue("unknown_key", str(key), f"not a known top-level key; allowed: {list(TOP_KEYS)}{hint}"))

    # ---------------------------------------------------------------- run id and channel (separate from the cell)
    run_id = raw.get("run_id")
    run_id_ok = _text(run_id, "run_id", issues)
    if run_id_ok and not RUN_ID_PATTERN.fullmatch(run_id):
        issues.append(ParameterIssue("malformed", "run_id", f"must match {RUN_ID_PATTERN.pattern}, got {run_id!r}"))
        run_id_ok = False
    if run_id_ok:
        # The pinned renderer writes "e1-<run id>" into the Caliper test name, so a run id such as "cfg001" would put a
        # protected channel name into the generated benchmark. Check the name the renderer will actually write.
        rendered_name = "e1-" + run_id.casefold()
        for protected in sorted(cs.PROTECTED_REFERENCE_CHANNELS):
            if protected in rendered_name:
                issues.append(ParameterIssue("protected_name_in_run_id", "run_id",
                                             f"the run id would put the protected reference channel name {protected!r} into the "
                                             f"generated files (the renderer writes 'e1-<run id>'); choose a run id that does not"))
    try:
        cs.select_experiment_channel(raw.get("channel"), purpose="the pre-live validation", option="channel")
    except cs.ChannelSelectionError as exc:
        issues.append(ParameterIssue(exc.reason, "channel", str(exc)))

    # ---------------------------------------------------------------- cell
    cell = _section(raw, "cell", issues)
    planned_round_s = float("nan")
    if cell is not None:
        if "max_message_count" in cell:
            _positive_finite(cell["max_message_count"], "cell.max_message_count", issues, integer=True)
        if "batch_timeout" in cell:
            _text(cell["batch_timeout"], "cell.batch_timeout", issues)
        if "preferred_max_bytes" in cell:
            _positive_finite(cell["preferred_max_bytes"], "cell.preferred_max_bytes", issues, integer=True)
        rate_ok = _positive_finite(cell["rate_tps"], "cell.rate_tps", issues) if "rate_tps" in cell else False
        if "workload" in cell and cell.get("workload") not in WORKLOADS:
            issues.append(ParameterIssue("not_allowed", "cell.workload", f"must be one of {list(WORKLOADS)}, got {cell.get('workload')!r}"))
        warm_ok = _positive_finite(cell.get("warmup_s"), "cell.warmup_s", issues, integer=True, allow_zero=True) if "warmup_s" in cell else False
        has_duration, has_count = cell.get("duration_s") is not None, cell.get("tx_count") is not None
        if has_duration == has_count:
            issues.append(ParameterIssue(
                "exactly_one_required", "cell.duration_s/cell.tx_count",
                "give exactly one of duration_s (seconds) or tx_count (a transaction count); no run length is assumed"))
        measured_s = None
        if has_duration and _positive_finite(cell.get("duration_s"), "cell.duration_s", issues, integer=True):
            measured_s = float(cell["duration_s"])
        if has_count and _positive_finite(cell.get("tx_count"), "cell.tx_count", issues, integer=True) and rate_ok:
            measured_s = float(cell["tx_count"]) / float(cell["rate_tps"])
        if measured_s is not None and warm_ok:
            warm = float(cell["warmup_s"])
            planned_round_s = warm + measured_s + (RENDERER_SETTLE_SECONDS_AFTER_WARMUP if warm > 0 else 0)

    # ---------------------------------------------------------------- recorder (optional live options)
    recorder = _section(raw, "recorder", issues)
    flush_ms = None
    if recorder is not None:
        for key, floor in LIVE_OPTION_MIN.items():
            if key in recorder and _positive_finite(recorder[key], f"recorder.{key}", issues, integer=True, minimum=floor):
                if key == "live_flush_interval_ms":
                    flush_ms = float(recorder[key])

    # ---------------------------------------------------------------- ledger collector and adapter window
    collector = _section(raw, "ledger_collector", issues)
    poll_s = max_polls = None
    if collector is not None:
        if "poll_interval_s" in collector and _positive_finite(collector["poll_interval_s"], "ledger_collector.poll_interval_s", issues):
            poll_s = float(collector["poll_interval_s"])
            if poll_s > UNIT_SLIP_CEILINGS["poll_interval_s"]:
                issues.append(ParameterIssue("unit_slip_suspected", "ledger_collector.poll_interval_s",
                                             f"{poll_s} s exceeds the unit-slip guard of {UNIT_SLIP_CEILINGS['poll_interval_s']} s; "
                                             f"is this a value in milliseconds? Nothing is converted."))
                poll_s = None
        if "max_polls" in collector and _positive_finite(collector["max_polls"], "ledger_collector.max_polls", issues, integer=True):
            max_polls = int(collector["max_polls"])
    adapter = _section(raw, "telemetry_adapter", issues)
    window_s = None
    if adapter is not None:
        if "window_s" in adapter and _positive_finite(adapter["window_s"], "telemetry_adapter.window_s", issues):
            window_s = float(adapter["window_s"])
            if window_s > UNIT_SLIP_CEILINGS["window_s"]:
                issues.append(ParameterIssue("unit_slip_suspected", "telemetry_adapter.window_s",
                                             f"{window_s} s exceeds the unit-slip guard of {UNIT_SLIP_CEILINGS['window_s']} s; "
                                             f"is this a value in milliseconds? Nothing is converted."))
                window_s = None
        if "recorder_latency" in adapter and adapter["recorder_latency"] not in RECORDER_LATENCY_MODES:
            issues.append(ParameterIssue("not_allowed", "telemetry_adapter.recorder_latency",
                                         f"must be one of {list(RECORDER_LATENCY_MODES)}, got {adapter['recorder_latency']!r}"))
    margin = float("nan")
    if poll_s is not None and window_s is not None:
        margin = window_s - poll_s
        if not window_s > poll_s:
            issues.append(ParameterIssue(
                "window_not_longer_than_poll", "telemetry_adapter.window_s",
                f"window_s ({window_s} s) must be strictly greater than ledger_collector.poll_interval_s ({poll_s} s), "
                f"otherwise the newest ledger interval falls outside the window and the committed rate is missing"))
    if flush_ms is not None and window_s is not None and flush_ms / 1000.0 >= window_s:
        issues.append(ParameterIssue(
            "flush_not_shorter_than_window", "recorder.live_flush_interval_ms",
            f"the recorder flush interval ({flush_ms} ms) must be shorter than telemetry_adapter.window_s "
            f"({window_s} s) or no client row can become visible inside a window"))
    span = float("nan")
    if poll_s is not None and max_polls is not None:
        span = (max_polls - 1) * poll_s
        if not math.isnan(planned_round_s) and span < planned_round_s:
            issues.append(ParameterIssue(
                "collector_stops_before_round_ends", "ledger_collector.max_polls",
                f"(max_polls - 1) * poll_interval_s = {span} s is shorter than the planned round of {planned_round_s} s "
                f"(warm-up + measured round + the renderer's settle time); the ledger journal would go stale mid-run"))

    # ---------------------------------------------------------------- Stage 3 limits, validated separately
    stage3 = _section(raw, "stage3", issues)
    if stage3 is not None:
        if "max_consecutive_abstentions" in stage3:
            try:
                Stage3FrameQualityContract(max_consecutive_abstentions=stage3["max_consecutive_abstentions"])
            except ValueError as exc:
                issues.append(ParameterIssue("invalid", "stage3.max_consecutive_abstentions", str(exc)))
        if "max_initialization_frames" in stage3 and stage3["max_initialization_frames"] is not None:
            try:
                Stage3FrameQualityContract(max_consecutive_abstentions=1,
                                           max_initialization_frames=stage3["max_initialization_frames"])
            except ValueError as exc:
                issues.append(ParameterIssue("invalid", "stage3.max_initialization_frames", str(exc)))
        # an explicit null for max_initialization_frames means "initialization disabled" and is recorded as such

    # ---------------------------------------------------------------- telemetry sources and environment
    sources = _section(raw, "telemetry_sources", issues)
    if sources is not None and "prometheus_endpoints" in sources:
        endpoints = sources["prometheus_endpoints"]
        if not isinstance(endpoints, list) or not endpoints:
            issues.append(ParameterIssue("missing", "telemetry_sources.prometheus_endpoints",
                                         "a non-empty list of 'name=http(s)://host:port/path' strings is required; no default endpoints"))
        else:
            names: list[str] = []
            for index, item in enumerate(endpoints):
                where = f"telemetry_sources.prometheus_endpoints[{index}]"
                match = _ENDPOINT.fullmatch(item) if isinstance(item, str) else None
                if match is None:
                    issues.append(ParameterIssue("malformed", where, f"must be 'name=http(s)://host:port/path', got {item!r}"))
                else:
                    names.append(match.group("name"))
            if len(set(names)) != len(names):
                issues.append(ParameterIssue("duplicate", "telemetry_sources.prometheus_endpoints", "endpoint names must be unique"))
    env = _section(raw, "environment", issues)
    if env is not None:
        for key in SECTION_KEYS["environment"]:
            if key in env:
                _text(env[key], f"environment.{key}", issues)

    if issues:
        raise PreliveParameterError(issues)

    values = {
        "schema": CONTRACT_VERSION, "run_id": run_id, "channel": raw["channel"],
        "cell": {k: cell[k] for k in SECTION_KEYS["cell"] if cell.get(k) is not None},
        "recorder": {k: recorder[k] for k in SECTION_KEYS["recorder"] if recorder and k in recorder},
        "ledger_collector": {k: collector[k] for k in SECTION_KEYS["ledger_collector"]},
        "telemetry_adapter": {k: adapter[k] for k in SECTION_KEYS["telemetry_adapter"]},
        "stage3": {"max_consecutive_abstentions": stage3["max_consecutive_abstentions"],
                   "max_initialization_frames": stage3["max_initialization_frames"]},
        "telemetry_sources": {"prometheus_endpoints": list(sources["prometheus_endpoints"])},
        "environment": {k: env[k] for k in SECTION_KEYS["environment"]},
    }
    return ValidatedParameters(values=values, planned_round_s=planned_round_s, collector_span_s=span, window_margin_s=margin)
