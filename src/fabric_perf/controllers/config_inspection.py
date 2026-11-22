"""Read-only inspection of one channel's committed batch configuration (task A0s, 2026-11-22). OFFLINE: it builds argument vectors and parses files.

Why. A bounded live validation has to prove, before and after the run, that the dedicated channel carries the approved batch configuration and that nothing
changed it. ``scripts/e1/e1_independent_config_verify.py`` already fetches and decodes a channel's config block, but it runs the fetch as soon as it is called, it
accepts any channel the experiment-channel validator accepts, and nothing proves that every command it can build is read-only. This module is the designated,
gated, testable counterpart. It reuses (does not copy) the existing parser ``config_evidence.config_from_block`` and the channel validator ``channel_selection``.

What it does.

* ``build_inspection_plan`` returns the exact argument vectors of a read-only inspection: ``peer version``, ``configtxlator version``,
  ``peer channel fetch config``, ``configtxlator proto_decode`` (and, only if asked, ``peer channel getinfo``). Every vector is checked by
  ``validate_read_only_argv``: a fixed allow-list of (program, verb, flags) decides; anything else, in particular any creating, joining, updating, signing,
  chaincode-installing, committing, invoking or node-starting verb, raises ``MutationRefused``. There is no "pass-through" option.
* ``inspect_decoded_block`` parses a decoded CONFIG block (stored file or fixture), checks the channel, the batch fields, the configuration sequence and the
  consenter count against the expected experimental cell, and returns one evidence record with the config-block number, sequence, a digest of the channel
  configuration and the operator-supplied capture timestamp.
* ``compare_inspections`` proves that a later inspection equals an earlier one (same sequence, same block, same digest).
* ``execute_inspection`` is the future adapter. It takes an INJECTED runner. Nothing in this module can start a process: it imports no process, network or
  container API (a test asserts that) and ``LIVE_INSPECTION_ENABLED`` is False, so a runner that declares ``kind == "process"`` is refused.

What it does not do: contact Fabric, start Docker or WSL, create or join a channel, submit a transaction, change a configuration, restart a node, or fall back
to another channel. A passing inspection says the supplied block carries the stated values; it does not show that the block is the channel's current block
unless the evidence was fetched live by an authorized, separate step.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path, PurePath, PurePosixPath
from typing import Any, Callable, Mapping, Sequence

from . import channel_selection as cs
from . import config_evidence as ce

INSPECTION_VERSION = "config_inspection/1 (A0s, 2026-11-22)"
EVIDENCE_SCHEMA = "config_inspection_evidence/1"
PLAN_SCHEMA = "config_inspection_plan/1"
LIVE_INSPECTION_ENABLED = False                      # no authorization record can turn this on in this version

EXPECTED_FIELDS = ("max_message_count", "batch_timeout_ms", "preferred_max_bytes", "absolute_max_bytes", "orderers")

# program -> {verb tuple: (number of leading positional arguments, allowed flags)}. A flag not listed here is refused.
# Values of flags are free text but may not start with "-".
ALLOWED: dict[str, dict[tuple[str, ...], tuple[int, frozenset[str]]]] = {
    "peer": {
        ("version",): (0, frozenset()),
        ("channel", "fetch", "config"): (1, frozenset({"-c", "-o", "--ordererTLSHostnameOverride", "--tls", "--cafile"})),   # one positional: the local output file
        ("channel", "getinfo"): (0, frozenset({"-c"})),
    },
    "configtxlator": {
        ("version",): (0, frozenset()),
        ("proto_decode",): (0, frozenset({"--input", "--type", "--output"})),
    },
}
# Defense in depth: none of these may appear as ANY element of a vector, whatever the allow-list says.
FORBIDDEN_WORDS = frozenset({
    "create", "join", "update", "signconfigtx", "compute_update", "proto_encode", "install", "approveformyorg", "commit", "invoke", "package",
    "queryinstalled", "lifecycle", "chaincode", "start", "stop", "restart", "kill", "rm", "run", "node", "snapshot", "rebuild", "rollback",
    "unjoin", "pause", "resume", "upgrade", "docker", "wsl", "wsl.exe", "sh", "bash", "cmd", "powershell", "-i", "--invoke",
})


class MutationRefused(ValueError):
    """An argument vector is not on the read-only allow-list (or the inspection is not permitted). Nothing was run."""


class InspectionError(ValueError):
    """The evidence cannot be accepted. ``code`` is a stable machine-readable cause."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _program(argv0: str) -> str:
    name = PurePath(str(argv0).replace("\\", "/")).name.lower()
    return name[:-4] if name.endswith(".exe") else name


def validate_read_only_argv(argv: Sequence[str]) -> tuple[str, tuple[str, ...]]:
    """Return (program, verb) for an allowed read-only vector, else raise :class:`MutationRefused`."""
    if not isinstance(argv, (list, tuple)) or not argv or not all(isinstance(a, str) and a for a in argv):
        raise MutationRefused("an argument vector must be a non-empty list of non-empty strings")
    program = _program(argv[0])
    verbs = ALLOWED.get(program)
    if verbs is None:
        raise MutationRefused(f"program {program!r} is not on the read-only allow-list {sorted(ALLOWED)}")
    rest = list(argv[1:])
    for verb, (positional, flags) in sorted(verbs.items(), key=lambda item: -len(item[0])):
        if tuple(rest[:len(verb)]) != verb:
            continue
        tail = rest[len(verb):]
        if len(tail) < positional or any(t.startswith("-") for t in tail[:positional]):
            raise MutationRefused(f"{program} {' '.join(verb)}: expected {positional} leading positional argument(s)")
        tail = tail[positional:]
        index = 0
        while index < len(tail):
            token = tail[index]
            if token not in flags:
                raise MutationRefused(f"{program} {' '.join(verb)}: argument {token!r} is not an allowed flag {sorted(flags)}")
            index += 1
            if token != "--tls":
                if index >= len(tail) or tail[index].startswith("-"):
                    raise MutationRefused(f"{program} {' '.join(verb)}: flag {token} needs a value")
                index += 1
        banned = sorted({a for a in argv[1:] if a in FORBIDDEN_WORDS})
        if banned:
            raise MutationRefused(f"forbidden word(s) {banned} in the vector")
        return program, verb
    raise MutationRefused(f"{program} {' '.join(rest[:3])!r} is not an allowed read-only verb; allowed: {sorted(' '.join(v) for v in verbs)}")


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _aware_utc(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise InspectionError("timestamp_missing", f"{name} must be an ISO-8601 string with a timezone; no clock is read here")
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise InspectionError("timestamp_malformed", f"{name} {value!r} is not ISO-8601") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise InspectionError("timestamp_naive", f"{name} {value!r} has no timezone; it is not assumed to be UTC")
    return parsed.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def expected_cell(values: Mapping[str, Any]) -> dict[str, int]:
    """The five canonical fields of the experimental cell. Every one is required; nothing is defaulted."""
    if not isinstance(values, Mapping):
        raise InspectionError("expected_not_a_mapping", "the expected cell must be a mapping")
    missing = [k for k in EXPECTED_FIELDS if values.get(k) is None]
    if missing:
        raise InspectionError("expected_incomplete", f"the expected cell lacks {missing}; no default is substituted")
    try:
        return ce.canonical_config({**{k: values[k] for k in EXPECTED_FIELDS}})  # type: ignore[return-value]
    except ce.ConfigEvidenceError as exc:
        raise InspectionError("expected_invalid", str(exc)) from exc


def cell_identifier(cell: Mapping[str, Any]) -> str:
    """The canonical experimental-cell identifier used by the execution plan and the authorization record."""
    return f"b{cell['max_message_count']}-t{cell['batch_timeout_ms']}ms-p{cell['preferred_max_bytes']}-a{cell['absolute_max_bytes']}-o{cell['orderers']}"


# ------------------------------------------------------------------------------------------------ plan
def build_inspection_plan(channel: Any, expected: Mapping[str, Any], *, peer: str, configtxlator: str, out_dir: str | Path, orderer: str,
                          orderer_tls_hostname: str, orderer_ca: str, include_getinfo: bool = False, label: str = "inspection") -> dict[str, Any]:
    """The exact read-only vectors for one inspection. Raises for a protected, historical, reserved or malformed channel, and for any vector off the allow-list."""
    try:
        channel = cs.select_experiment_channel(channel, purpose="a read-only configuration inspection", option="channel")
    except cs.ChannelSelectionError as exc:
        raise InspectionError("channel_refused", str(exc)) from exc
    cell = expected_cell(expected)
    for name, value in (("peer", peer), ("configtxlator", configtxlator), ("orderer", orderer), ("orderer_tls_hostname", orderer_tls_hostname), ("orderer_ca", orderer_ca)):
        if not isinstance(value, str) or not value.strip():
            raise InspectionError("plan_input_missing", f"{name} is required (no default)")
    if not isinstance(label, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,40}", label):
        raise InspectionError("label_invalid", "label must match [A-Za-z0-9_.-]{1,40}")
    out = PurePosixPath(str(out_dir)) if str(out_dir).startswith("/") else Path(out_dir)      # a WSL path stays POSIX
    block_path, json_path = out / f"{label}_{channel}.config.pb", out / f"{label}_{channel}.config.json"
    steps = [
        {"step": "peer_version", "argv": [peer, "version"]},
        {"step": "configtxlator_version", "argv": [configtxlator, "version"]},
        {"step": "peer_fetch_config", "argv": [peer, "channel", "fetch", "config", str(block_path), "-c", channel, "-o", orderer,
                                              "--ordererTLSHostnameOverride", orderer_tls_hostname, "--tls", "--cafile", orderer_ca]},
        {"step": "configtxlator_decode", "argv": [configtxlator, "proto_decode", "--input", str(block_path), "--type", "common.Block", "--output", str(json_path)]},
    ]
    if include_getinfo:
        steps.append({"step": "peer_getinfo", "argv": [peer, "channel", "getinfo", "-c", channel]})
    for step in steps:
        step["program"], step["verb"] = (lambda p, v: (p, " ".join(v)))(*validate_read_only_argv(step["argv"]))
        step["read_only"] = True
        step["mutates_fabric"] = False
    return {"schema": PLAN_SCHEMA, "version": INSPECTION_VERSION, "channel": channel, "expected": cell, "cell_id": cell_identifier(cell),
            "steps": steps, "block_path": str(block_path), "decoded_json_path": str(json_path),
            "env_names_required": ["FABRIC_CFG_PATH", "CORE_PEER_TLS_ENABLED", "CORE_PEER_LOCALMSPID", "CORE_PEER_MSPCONFIGPATH", "CORE_PEER_ADDRESS", "CORE_PEER_TLS_ROOTCERT_FILE"],
            "env_values": "operator-supplied at a future authorized run; not constructed or stored here",
            "live_inspection_enabled": LIVE_INSPECTION_ENABLED,
            "note": "text only; nothing was run. No identity (MSP) is chosen here."}


# ------------------------------------------------------------------------------------------------ parsing
def _sequence(block: Mapping[str, Any]) -> int:
    try:
        raw = block["data"]["data"][0]["payload"]["data"]["config"]["sequence"]
    except (KeyError, IndexError, TypeError):
        raise InspectionError("config_sequence_missing", "the decoded block has no config.sequence") from None
    if isinstance(raw, bool) or not isinstance(raw, (int, str)) or (isinstance(raw, str) and not re.fullmatch(r"\d+", raw.strip())):
        raise InspectionError("config_sequence_invalid", f"config.sequence {raw!r} is not a non-negative integer")
    value = int(raw)
    if value < 0 or value > 2 ** 63:
        raise InspectionError("config_sequence_invalid", f"config.sequence {value} is out of range")
    return value


def inspect_decoded_block(block: Any, *, channel: Any, expected: Mapping[str, Any], captured_utc: Any, block_sha256: str | None = None,
                          decoded_sha256: str | None = None, source: str = "stored_file_or_fixture", expected_sequence: int | None = None) -> dict[str, Any]:
    """Parse one decoded CONFIG block and compare it with the expected cell. Raises :class:`InspectionError` for evidence that cannot be accepted;
    a clean parse with different values returns ``verdict == "MISMATCH"`` and names every differing field."""
    try:
        channel = cs.select_experiment_channel(channel, purpose="a read-only configuration inspection", option="channel")
    except cs.ChannelSelectionError as exc:
        raise InspectionError("channel_refused", str(exc)) from exc
    cell = expected_cell(expected)
    captured = _aware_utc(captured_utc, "captured_utc")
    if not isinstance(block, Mapping):
        raise InspectionError("malformed_block", "the decoded block must be a JSON object")
    try:
        config, facts = ce.config_from_block(block, expected_channel=channel)
    except ce.ConfigEvidenceError as exc:
        text = str(exc)
        code = "wrong_channel" if "evidence is for channel" in text else ("malformed_block" if "not a decoded CONFIG block" in text or "not a CONFIG block" in text else "config_fields_invalid")
        raise InspectionError(code, text) from exc
    sequence = _sequence(block)
    if expected_sequence is not None and sequence != expected_sequence:
        raise InspectionError("config_sequence_unexpected", f"config.sequence is {sequence}, expected {expected_sequence}")
    config_object = block["data"]["data"][0]["payload"]["data"]["config"]
    orderer_values = config_object["channel_group"]["groups"]["Orderer"]["values"]
    differing = sorted(k for k in EXPECTED_FIELDS if config[k] != cell[k])
    return {
        "schema": EVIDENCE_SCHEMA, "version": INSPECTION_VERSION, "channel": channel, "config_block_number": facts["config_block_number"],
        "config_sequence": sequence, "observed": {k: config[k] for k in EXPECTED_FIELDS}, "batch_timeout": config["batch_timeout"],
        "expected": cell, "cell_id": cell_identifier(cell), "differing_fields": differing, "verdict": "MATCH" if not differing else "MISMATCH",
        "channel_config_sha256": sha256_hex(canonical_json(config_object)), "orderer_batch_projection_sha256": sha256_hex(canonical_json(
            {"BatchSize": orderer_values["BatchSize"], "BatchTimeout": orderer_values["BatchTimeout"]})),
        "raw_block_sha256": block_sha256, "decoded_json_sha256": decoded_sha256, "captured_utc": captured, "source": source,
        "captured_utc_origin": "operator-supplied; no clock is read by this module",
        "read_only": True, "no_default_substituted": True,
        "semantics": ce.SEMANTICS}


def load_decoded_block(path: Path) -> tuple[dict[str, Any], str]:
    """A stored decoded block and its SHA-256 (the existing loader; malformed JSON becomes an :class:`InspectionError`)."""
    try:
        return ce.load_block_file(Path(path))
    except ce.ConfigEvidenceError as exc:
        raise InspectionError("malformed_block", str(exc)) from exc
    except OSError as exc:
        raise InspectionError("block_unreadable", str(exc)) from exc


def compare_inspections(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    """Prove that the configuration did not change between two inspections of the same channel."""
    problems = []
    for key in ("channel", "config_sequence", "config_block_number", "channel_config_sha256", "orderer_batch_projection_sha256", "observed"):
        if before.get(key) != after.get(key):
            problems.append(f"{key}: {before.get(key)!r} -> {after.get(key)!r}")
    return {"unchanged": not problems, "differences": problems,
            "note": "equal sequence, block number and digest; says nothing about the interval between the two reads"}


# ------------------------------------------------------------------------------------------------ future adapter (injected runner only)
def execute_inspection(plan: Mapping[str, Any], runner: Any, *, read_file: Callable[[str], bytes], captured_utc: Any,
                       expected_sequence: int | None = None) -> dict[str, Any]:
    """Run a plan through an INJECTED runner and parse the decoded block. A runner of kind ``process`` is refused while live inspection is disabled.

    ``runner(argv, timeout_s)`` returns ``(returncode, stdout, stderr)``. Every vector is re-validated immediately before it is handed over.
    """
    kind = getattr(runner, "kind", None)
    if kind not in ("mock", "fixture"):
        if kind == "process" and not LIVE_INSPECTION_ENABLED:
            raise MutationRefused("live inspection is disabled in this version; a process runner is refused")
        raise MutationRefused(f"runner kind {kind!r} is not accepted (only 'mock' or 'fixture' while live inspection is disabled)")
    if plan.get("schema") != PLAN_SCHEMA:
        raise InspectionError("plan_schema", "not a config_inspection_plan/1")
    results = []
    for step in plan["steps"]:
        validate_read_only_argv(step["argv"])
        code, out, err = runner(list(step["argv"]), 30.0)
        results.append({"step": step["step"], "argv": list(step["argv"]), "returncode": code})
        if code != 0:
            raise InspectionError("command_failed", f"{step['step']} exited {code}")
    raw = read_file(plan["decoded_json_path"])
    try:
        block = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise InspectionError("malformed_block", f"decoded block is not valid JSON: {exc}") from exc
    evidence = inspect_decoded_block(block, channel=plan["channel"], expected=plan["expected"], captured_utc=captured_utc,
                                     decoded_sha256=sha256_hex(raw), source="injected_runner", expected_sequence=expected_sequence)
    return {**evidence, "commands": results}
