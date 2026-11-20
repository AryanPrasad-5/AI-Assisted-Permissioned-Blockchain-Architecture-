"""Explicit, validated selection of the Fabric channel an experiment uses (task A0b, 2026-11-20).

Owner decision D1 (2026-11-17): ``e1-r002`` and ``e1-cfg001`` stay untouched reference channels, and
the next validation runs on a NEW dedicated channel whose name the owner has not yet approved. Before
this module the tools disagreed about channels: the pilot runner and the shadow replay accepted ONLY
those two channels, the configuration verifier fetched only those two, the capture runner rewrote a
literal inside a template, and the plan renderer could derive a protected name from a run id. This
module is the one place that decides whether a channel name may be used.

Two roles, chosen by what the calling tool can do
-------------------------------------------------
``experiment``
    The tool runs a workload on the channel, verifies live channel state for an experiment, or
    captures telemetry that feeds the controllers. The channel must be named explicitly and must be
    a dedicated channel: the protected reference channels and the channels created by earlier
    experiments are refused. Use :func:`select_experiment_channel` or :func:`experiment_channel_arg`.

``observation``
    The tool only reads: an offline evidence builder, or a one-shot metrics scrape. Any existing channel
    may be observed, including the protected ones, because such a tool cannot change a channel.
    Only the explicit, well-formed name is enforced. Use :func:`select_observed_channel` or
    :func:`observed_channel_arg`.

Rules (both roles)
------------------
* There is no default. ``None``, an empty string or a blank string is an error.
* The name is never repaired: no stripping, no case folding, no guessing.
* ``re.fullmatch`` is used throughout, because ``$`` would accept a trailing newline.

Extra rules for ``experiment``
------------------------------
* The name must be ``e1-`` followed by 1 to 40 characters from ``a-z``, ``0-9``, ``.`` and ``-``. That is
  the project convention (``^e1-[A-Za-z0-9_.-]{1,40}$``, used by the renderer and the series drivers)
  narrowed to the alphabet Fabric itself accepts (``e1_channel_run.CHANNEL_RE``). The wider project
  pattern admits upper case and ``_``, which Fabric rejects.
* ``e1-r002`` and ``e1-cfg001`` are refused, also in upper case or with surrounding blanks.
* The channels of earlier experiments are refused (the literal names the P1b tool already treats as
  existing, and the name families of the P1a, P1b and P4 series), because a dedicated validation channel
  is new by definition and a run on an old one would add blocks to old evidence.
* No future channel name is hard-coded here. The owner's approved name is simply any name that passes.

The module also prints a verdict for one name, for the runbook step that vets the owner's name before
any tool is run: ``python -m fabric_perf.controllers.channel_selection --channel NAME``.

Pure standard library; no input or output other than that command-line entry point, no environment
access, no network.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from typing import Any

POLICY_VERSION = "channel_selection/1 (A0b, 2026-11-20)"

ROLE_EXPERIMENT = "experiment"
ROLE_OBSERVATION = "observation"

# Machine-readable refusal causes (ChannelSelectionError.reason).
MISSING = "missing"
WRONG_TYPE = "wrong_type"
EMPTY = "empty"
MALFORMED = "malformed"
PROTECTED = "protected_reference_channel"
HISTORICAL = "historical_evidence_channel"
MISMATCH = "channel_mismatch"
TEMPLATE_NOT_REWRITTEN = "template_not_rewritten"
PROTECTED_REMAINS = "protected_channel_remains"

# Owner decision D1: untouched reference channels. Never selectable for an experiment.
PROTECTED_REFERENCE_CHANNELS = frozenset({"e1-r002", "e1-cfg001"})

# Channels created by earlier experiments (the 10 names of ``EXISTING_CHANNELS`` in e1_quicklook_p1b.py
# minus the two protected ones). The 38 channels found on the network on 2026-11-17 are these 8, the
# two protected channels, and the series families below.
HISTORICAL_EVIDENCE_CHANNELS = frozenset({
    "e1-b001", "e1-b002", "e1-b003", "e1-e00a", "e1-e00a2", "e1-e00b", "e1-e00s", "e1-r001",
})

# Name families of per-run channels made by the series drivers (``CHANNEL_RE`` in e1_quicklook.py,
# e1_quicklook_p1b.py and p4_replay/driver.py). The P4 family is planned, not yet present on the network.
RESERVED_RUN_CHANNEL_FAMILIES = (
    ("the P1a quick-look series", r"e1-ql[0-9]{2}-b[0-9]+-l[0-9]+"),
    ("the P1b quick-look series", r"e1-cp[0-9]{2}-b[0-9]+-l[0-9]+"),
    ("the P4 replay series", r"e1-rp[0-9]{2}-b[0-9]+-r[0-9]+"),
)
_FAMILIES = tuple((label, re.compile(pattern)) for label, pattern in RESERVED_RUN_CHANNEL_FAMILIES)

# Fabric: lower-case ASCII letters, digits, '.' and '-', letter first, fewer than 250 characters
# (the same rule as ``CHANNEL_RE`` in e1_channel_run.py, written without anchors for ``fullmatch``).
FABRIC_CHANNEL_PATTERN = r"[a-z][a-z0-9.-]{0,248}"
# The E1 convention narrowed to Fabric's alphabet. Every match is also a Fabric channel name.
E1_CHANNEL_PATTERN = r"e1-[a-z0-9.-]{1,40}"
_FABRIC_RE = re.compile(FABRIC_CHANNEL_PATTERN)
_E1_RE = re.compile(E1_CHANNEL_PATTERN)


class ChannelSelectionError(ValueError):
    """A channel is missing, malformed, protected, historical, or inconsistent with another source."""

    def __init__(self, reason: str, message: str, *, value: Any = None):
        super().__init__(message)
        self.reason = reason
        self.value_shown = _shown(value)


def _shown(value: Any) -> str:
    text = repr(value)
    return text if len(text) <= 80 else text[:77] + "..."


def _where(option: str | None) -> str:
    return f" ({option})" if option else ""


def _require_text(value: Any, purpose: str, option: str | None) -> str:
    if value is None:
        raise ChannelSelectionError(
            MISSING, f"no channel was selected for {purpose}{_where(option)}. "
                     f"Name the dedicated validation channel explicitly; no default channel exists.", value=value)
    if not isinstance(value, str):
        raise ChannelSelectionError(
            WRONG_TYPE, f"the channel for {purpose}{_where(option)} must be a string, not {type(value).__name__}.",
            value=value)
    if not value.strip():
        raise ChannelSelectionError(
            EMPTY, f"the channel for {purpose}{_where(option)} is empty. "
                   f"Name the dedicated validation channel explicitly.", value=value)
    return value


def select_experiment_channel(value: Any, *, purpose: str = "an experiment", option: str | None = "--channel") -> str:
    """Return ``value`` if it is an acceptable dedicated validation channel; otherwise raise.

    The returned string is the input, unchanged.
    """
    text = _require_text(value, purpose, option)
    normal = text.strip().casefold()
    if normal in PROTECTED_REFERENCE_CHANNELS:
        raise ChannelSelectionError(
            PROTECTED, f"channel {_shown(text)} is a protected reference channel and cannot be selected for "
                       f"{purpose}{_where(option)}. Name the dedicated validation channel instead.", value=text)
    if normal in HISTORICAL_EVIDENCE_CHANNELS:
        raise ChannelSelectionError(
            HISTORICAL, f"channel {_shown(text)} was created by an earlier experiment and cannot be selected "
                        f"for {purpose}{_where(option)}. Name the dedicated validation channel instead.", value=text)
    for label, family in _FAMILIES:
        if family.fullmatch(normal):
            raise ChannelSelectionError(
                HISTORICAL, f"channel {_shown(text)} belongs to the naming family of {label} and cannot be "
                            f"selected for {purpose}{_where(option)}. Name the dedicated validation channel "
                            f"instead.", value=text)
    if not _E1_RE.fullmatch(text):
        raise ChannelSelectionError(
            MALFORMED, f"channel {_shown(text)} is not a valid E1 channel name for {purpose}{_where(option)}: "
                       f"expected 'e1-' followed by 1 to 40 characters from a-z, 0-9, '.' and '-' "
                       f"(Fabric accepts only lower-case ASCII letters, digits, '.' and '-').", value=text)
    return text


def select_observed_channel(value: Any, *, purpose: str = "a read-only observation",
                            option: str | None = "--channel") -> str:
    """Return ``value`` if it is an explicit, well-formed Fabric channel name; protected channels are allowed.

    For tools that cannot change a channel. It never supplies a default.
    """
    text = _require_text(value, purpose, option)
    if not _FABRIC_RE.fullmatch(text):
        raise ChannelSelectionError(
            MALFORMED, f"channel {_shown(text)} is not a valid Fabric channel name for {purpose}{_where(option)}: "
                       f"expected lower-case ASCII letters, digits, '.' and '-', starting with a letter, "
                       f"at most 249 characters.", value=text)
    return text


def require_same_channel(selected: str, recorded: Any, *, source: str, purpose: str = "an experiment",
                         option: str | None = "--channel") -> str:
    """Refuse when a recorded channel (for example in a rendered plan) differs from the explicit selection."""
    if recorded != selected:
        raise ChannelSelectionError(
            MISMATCH, f"the channel recorded in {source} is {_shown(recorded)} but {_shown(selected)} was "
                      f"selected for {purpose}{_where(option)}; both must name the same dedicated validation "
                      f"channel.", value=recorded)
    return selected


def is_protected_reference_channel(value: Any) -> bool:
    return isinstance(value, str) and value.strip().casefold() in PROTECTED_REFERENCE_CHANNELS


def selection_record(channel: str, *, role: str, option: str) -> dict[str, Any]:
    """What to store beside a result: which channel, how it was selected, and under which policy."""
    return {"channel": channel, "role": role, "selected_by": option, "explicit": True, "policy": POLICY_VERSION,
            "protected_reference_channels_refused": role == ROLE_EXPERIMENT}


def experiment_channel_arg(text: str) -> str:
    """argparse ``type=`` for a runner or any tool bound to an experiment. Raises ``ArgumentTypeError``."""
    try:
        return select_experiment_channel(text, option=None)
    except ChannelSelectionError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None


def observed_channel_arg(text: str) -> str:
    """argparse ``type=`` for a read-only tool. Raises ``ArgumentTypeError``."""
    try:
        return select_observed_channel(text, option=None)
    except ChannelSelectionError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="channel_selection", description="Vet one channel name offline. Contacts no Fabric, Docker or network.")
    parser.add_argument("--channel", required=True, help="the channel name to vet")
    parser.add_argument("--role", choices=(ROLE_EXPERIMENT, ROLE_OBSERVATION), default=ROLE_EXPERIMENT,
                        help="experiment (default) refuses protected and earlier channels; observation checks the name only")
    args = parser.parse_args(argv)
    select = select_experiment_channel if args.role == ROLE_EXPERIMENT else select_observed_channel
    try:
        channel = select(args.channel, option="--channel")
    except ChannelSelectionError as exc:
        print(json.dumps({"valid": False, "role": args.role, "reason": exc.reason, "message": str(exc),
                          "policy": POLICY_VERSION}, indent=2))
        return 2
    print(json.dumps({"valid": True, "role": args.role, "channel": channel, "policy": POLICY_VERSION}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
