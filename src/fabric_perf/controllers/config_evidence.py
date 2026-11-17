"""Authoritative or explicitly validated channel-configuration evidence (live-shadow fix 1, 2026-11-17).

The Phase 2A.3 frame builder wrote ``max_message_count: 40`` into every frame regardless of the
channel it was reading. That was true for ``e1-cfg001`` and false for ``e1-r002`` (B=45), and it
would be false again for the dedicated validation channel, whose configuration is its own.
The block size of a frame must come from evidence, never from a constant.

Accepted evidence
-----------------
* a decoded CONFIG block (``configtxlator proto_decode`` output), the authoritative source; or
* an explicit operator input that names all five canonical fields.

Everything is validated. There is no default: missing, partial, ambiguous (block and explicit
input disagree, or a different channel), out-of-range, or stale evidence raises
``ConfigEvidenceError``. Evidence is stale if its config block postdates the first block of the
run, or if a later CONFIG block appears inside the run (the frame's configuration would then be
ambiguous). Provenance travels with the configuration into every frame.

Fabric ``BatchSize.MaxMessageCount`` (B) is carried as exactly that. It is not asserted equal to
the HFBTP ``Block Size`` feature: that mapping is an explicit, separately validated object
(``adapters.ApprovedMaxMessageCountSpace``). Pure standard library; no package-relative imports.
"""

from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal, InvalidOperation
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

CANONICAL_FIELDS = ("max_message_count", "batch_timeout_ms", "preferred_max_bytes", "absolute_max_bytes", "orderers")
UINT32_MAX = 2 ** 32 - 1
MIN_TIMEOUT_MS, MAX_TIMEOUT_MS = 1, 60_000
CONFIG_HEADER_TYPE = 1
SEMANTICS = ("Fabric BatchSize.MaxMessageCount (B) as recorded in the channel configuration; "
             "not asserted equal to the HFBTP Block Size feature (see ApprovedMaxMessageCountSpace)")

_DURATION = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m)\s*$")
_UNIT_MS = {"ms": Decimal(1), "s": Decimal(1000), "m": Decimal(60_000)}


class ConfigEvidenceError(ValueError):
    """Configuration evidence is missing, partial, ambiguous, out of range or stale."""


def parse_batch_timeout_ms(value: Any) -> int:
    """Parse a Go duration such as ``2s``, ``500ms`` or ``1.5s`` into whole milliseconds."""
    if isinstance(value, bool) or not isinstance(value, str):
        raise ConfigEvidenceError(f"BatchTimeout must be a duration string such as '2s', got {value!r}")
    match = _DURATION.match(value)
    if not match:
        raise ConfigEvidenceError(f"unsupported BatchTimeout {value!r}; expected a duration in ms, s or m")
    try:
        milliseconds = Decimal(match.group(1)) * _UNIT_MS[match.group(2)]
    except InvalidOperation as exc:  # pragma: no cover - the regex admits only valid decimals
        raise ConfigEvidenceError(f"unsupported BatchTimeout {value!r}") from exc
    if milliseconds != milliseconds.to_integral_value():
        raise ConfigEvidenceError(f"BatchTimeout {value!r} is not a whole number of milliseconds")
    return int(milliseconds)


def batch_timeout_text(milliseconds: int) -> str:
    return f"{milliseconds // 1000}s" if milliseconds % 1000 == 0 else f"{milliseconds}ms"


def _integer(name: str, value: Any, *, low: int, high: int) -> int:
    if isinstance(value, bool):
        raise ConfigEvidenceError(f"{name} must be an integer, got {value!r}")
    if isinstance(value, str) and re.fullmatch(r"\d+", value.strip()):
        value = int(value.strip())
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    if not isinstance(value, int):
        raise ConfigEvidenceError(f"{name} must be an integer, got {value!r}")
    if not low <= value <= high:
        raise ConfigEvidenceError(f"{name} {value} is outside [{low}, {high}]")
    return value


def canonical_config(values: Mapping[str, Any]) -> dict[str, Any]:
    """Validate all five canonical fields and return them in the frame's ``current_config`` shape."""
    if not isinstance(values, Mapping):
        raise ConfigEvidenceError("configuration evidence must be a mapping")
    missing = [name for name in CANONICAL_FIELDS if values.get(name) is None]
    if missing:
        raise ConfigEvidenceError(f"configuration evidence is incomplete: missing {missing}; no default is substituted")
    config = {
        "max_message_count": _integer("max_message_count", values["max_message_count"], low=1, high=UINT32_MAX),
        "batch_timeout_ms": _integer("batch_timeout_ms", values["batch_timeout_ms"], low=MIN_TIMEOUT_MS, high=MAX_TIMEOUT_MS),
    }
    config["batch_timeout"] = batch_timeout_text(config["batch_timeout_ms"])
    config["preferred_max_bytes"] = _integer("preferred_max_bytes", values["preferred_max_bytes"], low=1, high=UINT32_MAX)
    config["absolute_max_bytes"] = _integer("absolute_max_bytes", values["absolute_max_bytes"], low=1, high=UINT32_MAX)
    config["orderers"] = _integer("orderers", values["orderers"], low=1, high=UINT32_MAX)
    if config["preferred_max_bytes"] > config["absolute_max_bytes"]:
        raise ConfigEvidenceError("preferred_max_bytes cannot exceed absolute_max_bytes")
    return config


def config_from_block(block: Mapping[str, Any], *, expected_channel: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Extract the canonical configuration and block facts from a decoded CONFIG block."""
    try:
        number = _integer("block header number", block["header"]["number"], low=0, high=2 ** 63)
        payload = block["data"]["data"][0]["payload"]
        header = payload["header"]["channel_header"]
        orderer = payload["data"]["config"]["channel_group"]["groups"]["Orderer"]["values"]
        batch = orderer["BatchSize"]["value"]
        timeout = orderer["BatchTimeout"]["value"]["timeout"]
        consenters = orderer["ConsensusType"]["value"]["metadata"]["consenters"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ConfigEvidenceError(f"not a decoded CONFIG block: missing or malformed {exc!r}") from exc
    if header.get("type") != CONFIG_HEADER_TYPE:
        raise ConfigEvidenceError(f"block is not a CONFIG block (channel header type {header.get('type')!r})")
    channel = header.get("channel_id")
    if channel != expected_channel:
        raise ConfigEvidenceError(f"configuration evidence is for channel {channel!r}, not {expected_channel!r}")
    if not isinstance(consenters, list) or not consenters:
        raise ConfigEvidenceError("configuration block lists no consenters")
    config = canonical_config({
        "max_message_count": batch.get("max_message_count"),
        "batch_timeout_ms": parse_batch_timeout_ms(timeout),
        "preferred_max_bytes": batch.get("preferred_max_bytes"),
        "absolute_max_bytes": batch.get("absolute_max_bytes"),
        "orderers": len(consenters),
    })
    return config, {"channel_id": channel, "config_block_number": number}


@dataclass(frozen=True)
class ConfigEvidence:
    """Validated configuration plus the provenance that every frame carries."""

    config: Mapping[str, Any]
    provenance: Mapping[str, Any]

    def as_current_config(self) -> dict[str, Any]:
        return dict(self.config)


def build_evidence(*, channel: str, block: Mapping[str, Any] | None = None, block_path: Path | None = None,
                   block_sha256: str | None = None, explicit: Mapping[str, Any] | None = None,
                   expect_max_message_count: int | None = None, run_first_block: int | None = None,
                   config_blocks_in_run: Iterable[int] = ()) -> ConfigEvidence:
    """Validate configuration evidence for one channel and one run. See the module docstring."""
    if not channel or not isinstance(channel, str):
        raise ConfigEvidenceError("a channel is required to validate configuration evidence")
    if block is None and explicit is None:
        raise ConfigEvidenceError("no configuration evidence supplied: give a decoded CONFIG block or all five "
                                  "canonical fields; no default block size is substituted")
    block_config = block_facts = None
    if block is not None:
        block_config, block_facts = config_from_block(block, expected_channel=channel)
    explicit_config = None if explicit is None else canonical_config(explicit)
    if block_config is not None and explicit_config is not None and block_config != explicit_config:
        differing = sorted(name for name in block_config if block_config[name] != explicit_config[name])
        raise ConfigEvidenceError(f"ambiguous configuration evidence: block and explicit input differ in {differing}")
    config = block_config if block_config is not None else explicit_config
    if expect_max_message_count is not None and config["max_message_count"] != expect_max_message_count:
        raise ConfigEvidenceError(f"configuration evidence has max_message_count {config['max_message_count']}, "
                                  f"operator expected {expect_max_message_count}")
    config_blocks = tuple(sorted({int(n) for n in config_blocks_in_run}))
    # The block that fixes the configuration: the evidence block itself, or (for explicit input,
    # which carries no block number) the first block of the run. A CONFIG block after it means
    # the configuration changed inside the run.
    reference = block_facts["config_block_number"] if block_facts is not None else run_first_block
    later = [n for n in config_blocks if reference is None or n > reference]
    if later:
        raise ConfigEvidenceError(
            f"stale or ambiguous configuration: CONFIG block(s) {later} appear in the run after the evidence "
            "was taken, so one configuration does not describe every frame")
    if block_facts is not None and run_first_block is not None and block_facts["config_block_number"] > run_first_block:
        raise ConfigEvidenceError(
            f"stale configuration: evidence is config block {block_facts['config_block_number']}, which postdates "
            f"the first run block {run_first_block}")
    provenance = {
        "kind": "config_block" if block_config is not None else "explicit_operator_input",
        "channel_id": channel,
        "config_block_number": None if block_facts is None else block_facts["config_block_number"],
        "source_path": None if block_path is None else str(block_path),
        "source_sha256": block_sha256,
        "cross_checked_explicit_input": block_config is not None and explicit_config is not None,
        "expected_max_message_count": expect_max_message_count,
        "validated_against_run": run_first_block is not None,
        "run_first_block": run_first_block,
        "config_blocks_seen_in_run": list(config_blocks),
        "no_default_substituted": True,
        "max_message_count_semantics": SEMANTICS,
    }
    return ConfigEvidence(dict(config), provenance)


def load_block_file(path: Path) -> tuple[dict[str, Any], str]:
    """Read a decoded CONFIG block file and return it with its SHA-256."""
    raw = Path(path).read_bytes()
    try:
        return json.loads(raw.decode("utf-8")), hashlib.sha256(raw).hexdigest()
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ConfigEvidenceError(f"configuration block file is not valid JSON: {Path(path).name}: {exc}") from exc
