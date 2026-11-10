"""Safe, measured Fabric channel configuration actuator for one approved field.

This tool implements the standard channel configuration update sequence for
``Orderer.BatchSize.MaxMessageCount`` only:

1. fetch and decode the current config block;
2. construct a copied config with one bounded value changed;
3. proto-encode both configs and let ``configtxlator compute_update`` calculate
   the versioned read/write set;
4. wrap the update in a common.Envelope;
5. submit with an explicit OrdererMSP admin identity; and
6. poll a newly fetched config block until the target value and a later block
   number are independently verified.

The default CLI mode is read-only. Live submission requires both ``--execute``
and ``--approved-target`` equal to the requested value. Every attempt gets a
new artifact directory and JSON record; existing artifacts are never reused.
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
try:
    import fcntl
except ImportError:  # pragma: no cover - exercised only by Windows-host imports
    fcntl = None
    import msvcrt
import hashlib
import json
import os
import posixpath
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from fabric_perf.controllers.base import FabricConfig  # noqa: E402
from e1_config_transition import (  # noqa: E402
    TransitionValidationError,
    validate_batch_size_transition,
)


TOOL_VERSION = "e1_config_actuator 0.1"
CONFIG_UPDATE_TYPE = 2
DEFAULT_MIN_B, DEFAULT_MAX_B = 10, 800


class ActuatorError(RuntimeError):
    """A configuration update was not completed and verified."""


class SafetyError(ActuatorError):
    """The requested update violates the approved actuator policy."""


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def _utc_delta_ms(start: str | None, end: str | None) -> float | None:
    if not start or not end:
        return None
    try:
        a = dt.datetime.fromisoformat(start.replace("Z", "+00:00"))
        b = dt.datetime.fromisoformat(end.replace("Z", "+00:00"))
        return (b - a).total_seconds() * 1000
    except ValueError:
        return None


def _validate_committed_transition(before_block: dict, after_block: dict, target_b: int) -> dict:
    """Adapt pure transition errors to the actuator's safety-error type."""
    try:
        return validate_batch_size_transition(before_block, after_block, target_b)
    except TransitionValidationError as exc:
        raise SafetyError(f"committed configuration verification failed [{exc.category}]: {exc}") from exc


def validate_rollback_preconditions(record: dict, current_config: dict, approved_target: int | None) -> FabricConfig:
    """Validate a rollback without contacting Fabric or submitting anything."""
    if record.get("success") is not True:
        raise SafetyError("rollback requires a successful update record")
    before = record.get("before_config")
    after = record.get("after_config")
    if not isinstance(before, dict) or not isinstance(after, dict):
        raise SafetyError("rollback record lacks complete before/after configuration evidence")
    try:
        target_b = int(before["max_message_count"])
    except (KeyError, TypeError, ValueError) as exc:
        raise SafetyError("rollback record lacks the exact recorded pre-update MaxMessageCount") from exc
    if approved_target != target_b:
        raise SafetyError("refusing live rollback: --approved-target must equal the recorded previous value")
    if current_config != after:
        raise SafetyError("rollback refused: current configuration differs from the recorded post-update configuration")
    try:
        return FabricConfig(**{k: before[k] for k in ("max_message_count", "batch_timeout_ms", "preferred_max_bytes", "absolute_max_bytes", "orderers")})
    except (KeyError, TypeError, ValueError) as exc:
        raise SafetyError(f"rollback record lacks an exact supported pre-update configuration: {exc}") from exc


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_new(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8", newline="\n") as fh:
        json.dump(value, fh, indent=2, sort_keys=True)
        fh.write("\n")


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def config_from_block(block: dict) -> tuple[FabricConfig, dict]:
    """Extract the active Fabric batch config and policy facts from a block."""
    try:
        config = block["data"]["data"][0]["payload"]["data"]["config"]
        channel_group = config["channel_group"]
        orderer = channel_group["groups"]["Orderer"]
        values = orderer["values"]
        batch = values["BatchSize"]["value"]
        timeout = values["BatchTimeout"]["value"]["timeout"]
        if timeout.endswith("ms"):
            timeout_ms = int(float(timeout[:-2]))
        elif timeout.endswith("s"):
            timeout_ms = int(float(timeout[:-1]) * 1000)
        else:
            raise ValueError(f"unsupported active BatchTimeout {timeout!r}")
        consenters = orderer["values"].get("ConsensusType", {}).get("value", {}).get("metadata", {}).get("consenters", [])
        orderer_admin_policy = orderer["policies"]["Admins"]["policy"]
        orderer_orgs = list(orderer.get("groups", {}).keys())
        policy_facts = {
            "channel_group_version": channel_group.get("version"),
            "orderer_group_version": orderer.get("version"),
            "batch_size_version": values["BatchSize"].get("version"),
            "batch_timeout_version": values["BatchTimeout"].get("version"),
            "batch_size_mod_policy": values["BatchSize"].get("mod_policy"),
            "batch_timeout_mod_policy": values["BatchTimeout"].get("mod_policy"),
            "orderer_group_mod_policy": orderer.get("mod_policy"),
            "orderer_admin_policy": orderer_admin_policy,
            "orderer_orgs": orderer_orgs,
            "required_signer_scope": "Orderer/Admins",
            "required_signer_rule": "MAJORITY Admins",
            "required_orderer_admin_msp": [
                g["policies"]["Admins"]["policy"]["value"]["identities"][0]["principal"]["msp_identifier"]
                for g in (orderer.get("groups", {}).values())
                if "Admins" in g.get("policies", {})
            ],
        }
        active = FabricConfig(
            max_message_count=int(batch["max_message_count"]),
            batch_timeout_ms=timeout_ms,
            preferred_max_bytes=int(batch["preferred_max_bytes"]),
            absolute_max_bytes=int(batch["absolute_max_bytes"]),
            orderers=len(consenters) or len(orderer_orgs),
        )
        return active, policy_facts
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise ActuatorError(f"decoded block does not expose a supported Fabric channel config: {exc}") from exc


def block_number(block: dict) -> int:
    try:
        return int(block["header"]["number"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ActuatorError(f"decoded block has no usable header number: {exc}") from exc


def block_timestamp_utc(block: dict) -> str | None:
    try:
        return block["data"]["data"][0]["payload"]["header"]["channel_header"].get("timestamp")
    except (KeyError, IndexError, TypeError):
        return None


def block_data_hash(block: dict) -> str | None:
    try:
        return block["header"].get("data_hash")
    except (KeyError, TypeError):
        return None


def config_from_block_document(block: dict) -> dict:
    return block["data"]["data"][0]["payload"]["data"]["config"]


def updated_config(original_config: dict, target_max_message_count: int) -> dict:
    """Return a deep copy changing exactly Orderer/BatchSize/max_message_count."""
    if isinstance(target_max_message_count, bool) or not isinstance(target_max_message_count, int):
        raise SafetyError("target MaxMessageCount must be an integer")
    if not DEFAULT_MIN_B <= target_max_message_count <= DEFAULT_MAX_B:
        raise SafetyError(f"target MaxMessageCount must be between {DEFAULT_MIN_B} and {DEFAULT_MAX_B}")
    out = copy.deepcopy(original_config)
    try:
        values = out["channel_group"]["groups"]["Orderer"]["values"]
        values["BatchSize"]["value"]["max_message_count"] = target_max_message_count
    except (KeyError, TypeError) as exc:
        raise ActuatorError(f"cannot locate Orderer/BatchSize/max_message_count: {exc}") from exc
    return out


def _find_max_message_count(update: dict, key: str) -> int | None:
    """Read the changed BatchSize value from a decoded ConfigUpdate read/write set."""
    try:
        values = update[key]["groups"]["Orderer"]["values"]
        return int(values["BatchSize"]["value"]["max_message_count"])
    except (KeyError, TypeError, ValueError):
        return None


@dataclass
class CommandRecord:
    label: str
    argv: list[str]
    start_utc: str
    end_utc: str
    elapsed_ms: float
    exit_code: int | None
    stdout_file: str
    stderr_file: str


@dataclass
class UpdateRecord:
    attempt_id: str
    channel: str
    parameter: str
    before_config: dict
    requested_config: dict
    after_config: dict | None
    decision_timestamp_utc: str
    config_fetch_timestamp_utc: str | None = None
    update_constructed_timestamp_utc: str | None = None
    submission_timestamp_utc: str | None = None
    config_block_commit_timestamp_utc: str | None = None
    active_verified_timestamp_utc: str | None = None
    total_adaptation_latency_ms: float | None = None
    success: bool = False
    error: str | None = None
    before_block_number: int | None = None
    after_block_number: int | None = None
    policy: dict | None = None
    artifacts_dir: str = ""
    commands: list[dict] | None = None
    prepared_artifacts_dir: str | None = None
    preparation_completion_timestamp_utc: str | None = None
    submission_start_timestamp_utc: str | None = None
    submission_completion_timestamp_utc: str | None = None
    submission_duration_ms: float | None = None
    commit_latency_ms: float | None = None
    independent_verification_timestamp_utc: str | None = None
    before_block_data_hash: str | None = None
    after_block_data_hash: str | None = None
    before_block_sha256: str | None = None
    after_block_sha256: str | None = None
    verification_checks: dict | None = None

    def as_dict(self) -> dict:
        return self.__dict__.copy()


class FabricChannelConfigActuator:
    """One-parameter, serialized, commit-verified Fabric config actuator."""

    def __init__(
        self,
        *,
        channel: str,
        peer_bin: Path,
        configtxlator_bin: Path,
        fabric_cfg_path: Path,
        orderer_address: str,
        orderer_tls_hostname: str,
        orderer_ca: Path,
        orderer_admin_msp: Path,
        orderer_msp_id: str = "OrdererMSP",
        output_dir: Path,
        min_b: int = DEFAULT_MIN_B,
        max_b: int = DEFAULT_MAX_B,
        cooldown_s: float = 30.0,
        poll_s: float = 1.0,
        commit_timeout_s: float = 60.0,
        command_timeout_s: float = 30.0,
        runner: Callable[..., subprocess.CompletedProcess] = subprocess.run,
    ):
        if min_b < 1 or max_b < min_b:
            raise SafetyError("invalid allowed MaxMessageCount range")
        if cooldown_s < 0 or poll_s <= 0 or commit_timeout_s <= 0 or command_timeout_s <= 0:
            raise SafetyError("cooldown and timeouts must be positive, cooldown may be zero")
        self.channel = channel
        self.peer_bin = Path(peer_bin)
        self.configtxlator_bin = Path(configtxlator_bin)
        self.fabric_cfg_path = Path(fabric_cfg_path)
        self.orderer_address = orderer_address
        self.orderer_tls_hostname = orderer_tls_hostname
        self.orderer_ca = Path(orderer_ca)
        self.orderer_admin_msp = Path(orderer_admin_msp)
        self.orderer_msp_id = orderer_msp_id
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.output_dir / ".config-actuator.lock"
        self.min_b, self.max_b = min_b, max_b
        self.cooldown_s, self.poll_s = cooldown_s, poll_s
        self.commit_timeout_s, self.command_timeout_s = commit_timeout_s, command_timeout_s
        self.runner = runner
        self._last_active: FabricConfig | None = None

    def _env(self) -> dict[str, str]:
        env = os.environ.copy()
        env.update({
            "FABRIC_CFG_PATH": str(self.fabric_cfg_path),
            "CORE_PEER_TLS_ENABLED": "true",
            "CORE_PEER_LOCALMSPID": self.orderer_msp_id,
            "CORE_PEER_MSPCONFIGPATH": str(self.orderer_admin_msp),
        })
        return env

    @contextmanager
    def _exclusive_lock(self) -> Iterator[None]:
        with self.lock_path.open("a+", encoding="utf-8") as lock:
            if fcntl is not None:
                try:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise SafetyError("another configuration update is in progress") from exc
            else:
                try:
                    lock.seek(0)
                    lock.write("0")
                    lock.flush()
                    msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
                except OSError as exc:
                    raise SafetyError("another configuration update is in progress") from exc
            try:
                yield
            finally:
                if fcntl is not None:
                    fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
                else:
                    lock.seek(0)
                    msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)

    def _new_attempt_dir(self) -> Path:
        path = self.output_dir / f"update-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:8]}"
        path.mkdir(parents=True, exist_ok=False)
        return path

    def _run(self, label: str, argv: list[str], work: Path, commands: list[CommandRecord]) -> None:
        stdout_path, stderr_path = work / f"{len(commands):02d}_{label}.stdout", work / f"{len(commands):02d}_{label}.stderr"
        start = time.monotonic()
        started = utc_now()
        try:
            result = self.runner(argv, capture_output=True, timeout=self.command_timeout_s, check=False, env=self._env())
            code = result.returncode
            out, err = result.stdout or b"", result.stderr or b""
        except (OSError, subprocess.SubprocessError) as exc:
            code, out, err = None, b"", repr(exc).encode()
        stdout_path.write_bytes(out)
        stderr_path.write_bytes(err)
        commands.append(CommandRecord(label, argv, started, utc_now(), (time.monotonic() - start) * 1000, code,
                                       stdout_path.name, stderr_path.name))
        if code != 0:
            raise ActuatorError(f"{label} failed with exit code {code}; see {stderr_path}")

    def _fetch_decode(self, work: Path, stem: str, commands: list[CommandRecord]) -> tuple[dict, FabricConfig, dict]:
        block_path, json_path = work / f"{stem}.pb", work / f"{stem}.json"
        self._run(f"fetch_config_{stem}", [str(self.peer_bin), "channel", "fetch", "config", str(block_path),
                                            "-c", self.channel, "-o", self.orderer_address,
                                            "--ordererTLSHostnameOverride", self.orderer_tls_hostname,
                                            "--tls", "--cafile", str(self.orderer_ca)], work, commands)
        self._run(f"decode_config_{stem}", [str(self.configtxlator_bin), "proto_decode", "--input", str(block_path),
                                             "--type", "common.Block", "--output", str(json_path)], work, commands)
        block = _load(json_path)
        active, policy = config_from_block(block)
        return block, active, policy

    def inspect_current(self) -> dict:
        """Fetch and decode the active config without submitting anything."""
        work = self._new_attempt_dir()
        commands: list[CommandRecord] = []
        block, active, policy = self._fetch_decode(work, "inspect", commands)
        self._last_active = active
        facts = {
            "tool": TOOL_VERSION,
            "timestamp_utc": utc_now(),
            "channel": self.channel,
            "block_number": block_number(block),
            "active_config": active.as_dict(),
            "policy": policy,
            "artifacts_dir": str(work),
            "commands": [c.__dict__ for c in commands],
        }
        _write_new(work / "inspection.json", facts)
        return facts

    def _check_cooldown(self, now_epoch: float) -> None:
        latest = None
        for path in self.output_dir.glob("update-*/update_record.json"):
            try:
                rec = _load(path)
                if rec.get("success") and rec.get("active_verified_timestamp_utc"):
                    epoch = dt.datetime.fromisoformat(rec["active_verified_timestamp_utc"].replace("Z", "+00:00")).timestamp()
                    latest = max(latest or epoch, epoch)
            except (OSError, ValueError, TypeError, KeyError):
                continue
        if latest is not None and now_epoch - latest < self.cooldown_s:
            raise SafetyError(f"cooldown active for {self.cooldown_s - (now_epoch - latest):.3f}s")

    def _validate_target(self, current: FabricConfig, target: FabricConfig) -> None:
        if not self.min_b <= target.max_message_count <= self.max_b:
            raise SafetyError(f"target MaxMessageCount {target.max_message_count} outside approved range {self.min_b}..{self.max_b}")
        if target.batch_timeout_ms != current.batch_timeout_ms:
            raise SafetyError("this actuator only permits MaxMessageCount; BatchTimeout must remain unchanged")
        if target.preferred_max_bytes != current.preferred_max_bytes or target.absolute_max_bytes != current.absolute_max_bytes:
            raise SafetyError("this actuator only permits MaxMessageCount; byte limits must remain unchanged")
        if target.orderers != current.orderers:
            raise SafetyError("orderer count cannot be changed by this actuator")
        if target.max_message_count == current.max_message_count:
            raise SafetyError("target MaxMessageCount equals the active value")

    def prepare(self, target: FabricConfig) -> dict:
        """Construct and verify the update envelope without submitting it."""
        with self._exclusive_lock():
            work = self._new_attempt_dir()
            commands: list[CommandRecord] = []
            before_block, before, policy = self._fetch_decode(work, "before", commands)
            self._validate_target(before, target)
            original = config_from_block_document(before_block)
            changed = updated_config(original, target.max_message_count)
            _write_new(work / "original_config.json", original)
            _write_new(work / "updated_config.json", changed)
            original_pb, changed_pb, update_pb = work / "original_config.pb", work / "updated_config.pb", work / "config_update.pb"
            self._run("encode_original", [str(self.configtxlator_bin), "proto_encode", "--input", str(work / "original_config.json"), "--type", "common.Config", "--output", str(original_pb)], work, commands)
            self._run("encode_updated", [str(self.configtxlator_bin), "proto_encode", "--input", str(work / "updated_config.json"), "--type", "common.Config", "--output", str(changed_pb)], work, commands)
            self._run("compute_update", [str(self.configtxlator_bin), "compute_update", "--channel_id", self.channel, "--original", str(original_pb), "--updated", str(changed_pb), "--output", str(update_pb)], work, commands)
            update_json = work / "config_update.json"
            self._run("decode_update", [str(self.configtxlator_bin), "proto_decode", "--input", str(update_pb), "--type", "common.ConfigUpdate", "--output", str(update_json)], work, commands)
            update = _load(update_json)
            if _find_max_message_count(update, "write_set") != target.max_message_count:
                raise ActuatorError("computed write_set does not contain the requested MaxMessageCount")
            if _find_max_message_count(update, "read_set") is not None:
                raise ActuatorError("computed read_set unexpectedly contains a changed MaxMessageCount")
            envelope_json = work / "config_update_in_envelope.json"
            envelope = {"payload": {"header": {"channel_header": {"channel_id": self.channel, "type": CONFIG_UPDATE_TYPE}}, "data": {"config_update": update}}}
            _write_new(envelope_json, envelope)
            envelope_pb = work / "config_update_in_envelope.pb"
            self._run("encode_envelope", [str(self.configtxlator_bin), "proto_encode", "--input", str(envelope_json), "--type", "common.Envelope", "--output", str(envelope_pb)], work, commands)
            result = {
                "tool": TOOL_VERSION,
                "prepared_utc": utc_now(),
                "channel": self.channel,
                "parameter": "Orderer.BatchSize.MaxMessageCount",
                "before_block_number": block_number(before_block),
                "before_block_timestamp_utc": block_timestamp_utc(before_block),
                "before_config": before.as_dict(),
                "requested_config": target.as_dict(),
                "policy": policy,
                "signer_msp_id": self.orderer_msp_id,
                "signer_msp_path": str(self.orderer_admin_msp),
                "read_set": update.get("read_set"),
                "write_set": update.get("write_set"),
                "artifacts_dir": str(work),
                "envelope_pb": str(envelope_pb),
                "commands": [c.__dict__ for c in commands],
            }
            _write_new(work / "prepare_record.json", result)
            return result

    def _validate_prepared_artifact(self, prepared_dir: Path, before_block: dict,
                                    before: FabricConfig, policy: dict,
                                    target: FabricConfig, *, approved_before_b: int | None = None) -> dict:
        """Reject stale or altered preparation artifacts before submission."""
        prepared_dir = Path(prepared_dir)
        required = ["prepare_record.json", "before.json", "original_config.json",
                    "config_update.json", "config_update_in_envelope.json",
                    "config_update_in_envelope.pb"]
        missing = [name for name in required if not (prepared_dir / name).is_file()]
        if missing:
            raise SafetyError(f"prepared artifact is incomplete; missing {missing}")
        prep = _load(prepared_dir / "prepare_record.json")
        if prep.get("channel") != self.channel:
            raise SafetyError("prepared artifact channel does not match the actuator channel")
        if prep.get("parameter") != "Orderer.BatchSize.MaxMessageCount":
            raise SafetyError("prepared artifact parameter is not the approved MaxMessageCount field")
        if prep.get("signer_msp_id") != self.orderer_msp_id or self.orderer_msp_id != "OrdererMSP":
            raise SafetyError("prepared artifact signer is not the required OrdererMSP identity")
        if approved_before_b is not None and before.max_message_count != approved_before_b:
            raise SafetyError(f"active MaxMessageCount {before.max_message_count} differs from approved starting value {approved_before_b}")
        if prep.get("before_config") != before.as_dict():
            raise SafetyError("prepared artifact is stale: before_config differs from live channel")
        if prep.get("requested_config") != target.as_dict():
            raise SafetyError("prepared artifact target differs from the approved target")
        if int(prep.get("before_block_number", -1)) != block_number(before_block):
            raise SafetyError("prepared artifact is stale: before block number differs")
        if _load(prepared_dir / "before.json") != before_block:
            raise SafetyError("prepared artifact is stale or altered: before block differs")
        if _load(prepared_dir / "original_config.json") != config_from_block_document(before_block):
            raise SafetyError("prepared artifact is stale or altered: original config differs")

        update = _load(prepared_dir / "config_update.json")
        envelope = _load(prepared_dir / "config_update_in_envelope.json")
        try:
            ch = envelope["payload"]["header"]["channel_header"]
            envelope_update = envelope["payload"]["data"]["config_update"]
        except (KeyError, TypeError) as exc:
            raise SafetyError(f"prepared envelope has an invalid structure: {exc}") from exc
        if ch.get("channel_id") != self.channel or int(ch.get("type")) != CONFIG_UPDATE_TYPE:
            raise SafetyError("prepared envelope channel ID or type is incorrect")
        if envelope_update != update:
            raise SafetyError("prepared envelope ConfigUpdate differs from decoded ConfigUpdate")
        if update.get("channel_id") != self.channel:
            raise SafetyError("prepared ConfigUpdate channel ID is incorrect")
        try:
            read_set = update["read_set"]
            write_set = update["write_set"]
            read_orderer = read_set["groups"]["Orderer"]
            write_orderer = write_set["groups"]["Orderer"]
            read_values = read_orderer["values"]
            write_values = write_orderer["values"]
        except (KeyError, TypeError) as exc:
            raise SafetyError(f"prepared ConfigUpdate lacks the expected read/write set: {exc}") from exc
        if read_set.get("groups", {}).keys() != {"Orderer"} or read_set.get("values") or read_set.get("policies"):
            raise SafetyError("prepared read set contains fields outside the Orderer group")
        if read_orderer.get("version") != str(policy["orderer_group_version"]) or read_values or read_orderer.get("policies") or read_orderer.get("groups"):
            raise SafetyError("prepared read set version or scope is not current")
        if write_set.get("groups", {}).keys() != {"Orderer"} or write_set.get("values") or write_set.get("policies"):
            raise SafetyError("prepared write set contains fields outside the Orderer group")
        if write_orderer.get("version") != str(policy["orderer_group_version"]) or write_orderer.get("policies") or write_orderer.get("groups"):
            raise SafetyError("prepared write set Orderer version or scope is invalid")
        if write_values.keys() != {"BatchSize"}:
            raise SafetyError("prepared write set changes a field other than BatchSize")
        batch = write_values["BatchSize"]
        if batch.get("version") != str(int(policy["batch_size_version"]) + 1):
            raise SafetyError("prepared BatchSize version is not current version plus one")
        current_batch = {"absolute_max_bytes": before.absolute_max_bytes,
                         "preferred_max_bytes": before.preferred_max_bytes,
                         "max_message_count": before.max_message_count}
        if batch.get("value", {}).get("max_message_count") != target.max_message_count:
            raise SafetyError("prepared write set target MaxMessageCount is incorrect")
        if batch.get("value", {}).get("absolute_max_bytes") != current_batch["absolute_max_bytes"] or batch.get("value", {}).get("preferred_max_bytes") != current_batch["preferred_max_bytes"]:
            raise SafetyError("prepared write set changes a byte limit")
        if batch.get("mod_policy") != policy["batch_size_mod_policy"]:
            raise SafetyError("prepared BatchSize modification policy differs from the active policy")
        return prep

    def execute_prepared(self, prepared_dir: Path, target: FabricConfig, *, approved_before_b: int | None = None) -> UpdateRecord:
        """Execute exactly one previously prepared envelope after stale checks."""
        decision_epoch = time.time()
        with self._exclusive_lock():
            self._check_cooldown(decision_epoch)
            work = self._new_attempt_dir()
            commands: list[CommandRecord] = []
            start_mono = time.monotonic()
            record: UpdateRecord | None = None
            try:
                before_block, before, policy = self._fetch_decode(work, "before", commands)
                prepared = self._validate_prepared_artifact(Path(prepared_dir), before_block, before, policy, target, approved_before_b=approved_before_b)
                preparation_utc = prepared.get("prepared_utc")
                record = UpdateRecord(
                    attempt_id=work.name, channel=self.channel, parameter="Orderer.BatchSize.MaxMessageCount",
                    before_config=before.as_dict(), requested_config=target.as_dict(), after_config=None,
                    decision_timestamp_utc=utc_now(), config_fetch_timestamp_utc=utc_now(),
                    before_block_number=block_number(before_block), policy=policy,
                    artifacts_dir=str(work), commands=[c.__dict__ for c in commands],
                    prepared_artifacts_dir=str(prepared_dir), preparation_completion_timestamp_utc=preparation_utc,
                    before_block_data_hash=block_data_hash(before_block), before_block_sha256=sha256(work / "before.pb"),
                )
                _write_new(work / "execution_preflight.json", {
                    "prepared_artifacts_dir": str(prepared_dir), "prepared_record": prepared,
                    "active_config": before.as_dict(), "policy": policy,
                    "verified_before_submission": True,
                })
                submit_start = utc_now()
                submit_start_mono = time.monotonic()
                record.submission_start_timestamp_utc = submit_start
                self._run("submit_prepared_update", [str(self.peer_bin), "channel", "update", "-f", str(Path(prepared_dir) / "config_update_in_envelope.pb"),
                                                      "-c", self.channel, "-o", self.orderer_address,
                                                      "--ordererTLSHostnameOverride", self.orderer_tls_hostname,
                                                      "--tls", "--cafile", str(self.orderer_ca)], work, commands)
                record.submission_completion_timestamp_utc = utc_now()
                record.submission_timestamp_utc = record.submission_completion_timestamp_utc
                record.submission_duration_ms = (time.monotonic() - submit_start_mono) * 1000
                deadline = time.monotonic() + self.commit_timeout_s
                while time.monotonic() < deadline:
                    try:
                        stem = f"after_{len(commands)}"
                        latest_block, latest_active, latest_policy = self._fetch_decode(work, stem, commands)
                        if block_number(latest_block) > record.before_block_number and latest_active.max_message_count == target.max_message_count:
                            record.verification_checks = _validate_committed_transition(before_block, latest_block, target.max_message_count)
                            record.after_config = latest_active.as_dict()
                            record.after_block_number = block_number(latest_block)
                            record.after_block_data_hash = block_data_hash(latest_block)
                            record.after_block_sha256 = sha256(work / f"{stem}.pb")
                            record.config_block_commit_timestamp_utc = block_timestamp_utc(latest_block) or utc_now()
                            record.active_verified_timestamp_utc = utc_now()
                            record.independent_verification_timestamp_utc = utc_now()
                            record.commit_latency_ms = _utc_delta_ms(record.submission_start_timestamp_utc, record.config_block_commit_timestamp_utc)
                            record.total_adaptation_latency_ms = (time.monotonic() - start_mono) * 1000
                            record.success = True
                            record.commands = [c.__dict__ for c in commands]
                            _write_new(work / "update_record.json", record.as_dict())
                            self._last_active = latest_active
                            return record
                    except SafetyError:
                        raise
                    except ActuatorError:
                        pass
                    time.sleep(self.poll_s)
                raise ActuatorError("prepared update was submitted but target was not observed in a later active config block before timeout")
            except Exception as exc:
                if record is None:
                    record = UpdateRecord(work.name, self.channel, "Orderer.BatchSize.MaxMessageCount", {}, target.as_dict(), None, utc_now(), success=False, error=f"{type(exc).__name__}: {exc}", artifacts_dir=str(work), commands=[c.__dict__ for c in commands], prepared_artifacts_dir=str(prepared_dir))
                else:
                    record.error = f"{type(exc).__name__}: {exc}"
                    record.success = False
                    record.commands = [c.__dict__ for c in commands]
                _write_new(work / "update_record.json", record.as_dict())
                if isinstance(exc, ActuatorError):
                    raise
                raise ActuatorError(str(exc)) from exc

    def apply(self, target: FabricConfig) -> UpdateRecord:
        """Submit one bounded update and raise unless it is commit-verified."""
        decision_epoch = time.time()
        with self._exclusive_lock():
            self._check_cooldown(decision_epoch)
            work = self._new_attempt_dir()
            commands: list[CommandRecord] = []
            record: UpdateRecord | None = None
            start_mono = time.monotonic()
            try:
                before_block, before, policy = self._fetch_decode(work, "before", commands)
                self._validate_target(before, target)
                original = config_from_block_document(before_block)
                changed = updated_config(original, target.max_message_count)
                _write_new(work / "original_config.json", original)
                _write_new(work / "updated_config.json", changed)
                original_pb, changed_pb, update_pb = work / "original_config.pb", work / "updated_config.pb", work / "config_update.pb"
                self._run("encode_original", [str(self.configtxlator_bin), "proto_encode", "--input", str(work / "original_config.json"), "--type", "common.Config", "--output", str(original_pb)], work, commands)
                self._run("encode_updated", [str(self.configtxlator_bin), "proto_encode", "--input", str(work / "updated_config.json"), "--type", "common.Config", "--output", str(changed_pb)], work, commands)
                self._run("compute_update", [str(self.configtxlator_bin), "compute_update", "--channel_id", self.channel, "--original", str(original_pb), "--updated", str(changed_pb), "--output", str(update_pb)], work, commands)
                update_json = work / "config_update.json"
                self._run("decode_update", [str(self.configtxlator_bin), "proto_decode", "--input", str(update_pb), "--type", "common.ConfigUpdate", "--output", str(update_json)], work, commands)
                update = _load(update_json)
                if _find_max_message_count(update, "write_set") != target.max_message_count:
                    raise ActuatorError("computed write_set does not contain the requested MaxMessageCount")
                if _find_max_message_count(update, "read_set") is not None:
                    raise ActuatorError("computed read_set unexpectedly contains a changed MaxMessageCount")
                envelope_json = work / "config_update_in_envelope.json"
                envelope = {"payload": {"header": {"channel_header": {"channel_id": self.channel, "type": CONFIG_UPDATE_TYPE}}, "data": {"config_update": update}}}
                _write_new(envelope_json, envelope)
                envelope_pb = work / "config_update_in_envelope.pb"
                self._run("encode_envelope", [str(self.configtxlator_bin), "proto_encode", "--input", str(envelope_json), "--type", "common.Envelope", "--output", str(envelope_pb)], work, commands)
                constructed_utc = utc_now()
                record = UpdateRecord(
                    attempt_id=work.name, channel=self.channel, parameter="Orderer.BatchSize.MaxMessageCount",
                    before_config=before.as_dict(), requested_config=target.as_dict(), after_config=None,
                    decision_timestamp_utc=utc_now(), config_fetch_timestamp_utc=utc_now(),
                    update_constructed_timestamp_utc=constructed_utc, before_block_number=block_number(before_block),
                    policy=policy, artifacts_dir=str(work), commands=[c.__dict__ for c in commands],
                )
                _write_new(work / "update_plan.json", {"read_set": update.get("read_set"), "write_set": update.get("write_set"), "policy": policy, "signer_msp_id": self.orderer_msp_id, "signer_msp_path": str(self.orderer_admin_msp)})
                self._run("submit_update", [str(self.peer_bin), "channel", "update", "-f", str(envelope_pb), "-c", self.channel,
                                              "-o", self.orderer_address, "--ordererTLSHostnameOverride", self.orderer_tls_hostname,
                                              "--tls", "--cafile", str(self.orderer_ca)], work, commands)
                record.submission_timestamp_utc = utc_now()
                deadline = time.monotonic() + self.commit_timeout_s
                latest_block, latest_active, _ = before_block, before, policy
                while time.monotonic() < deadline:
                    try:
                        latest_block, latest_active, _ = self._fetch_decode(work, f"after_{len(commands)}", commands)
                        if block_number(latest_block) > record.before_block_number and latest_active.max_message_count == target.max_message_count:
                            record.verification_checks = _validate_committed_transition(before_block, latest_block, target.max_message_count)
                            record.after_config = latest_active.as_dict()
                            record.after_block_number = block_number(latest_block)
                            record.config_block_commit_timestamp_utc = block_timestamp_utc(latest_block) or utc_now()
                            record.active_verified_timestamp_utc = utc_now()
                            record.total_adaptation_latency_ms = (time.monotonic() - start_mono) * 1000
                            record.success = True
                            record.commands = [c.__dict__ for c in commands]
                            _write_new(work / "update_record.json", record.as_dict())
                            self._last_active = latest_active
                            return record
                    except SafetyError:
                        raise
                    except ActuatorError:
                        pass
                    time.sleep(self.poll_s)
                raise ActuatorError("configuration update was submitted but target was not observed in a later active config block before timeout")
            except Exception as exc:
                if record is None:
                    record = UpdateRecord(work.name, self.channel, "Orderer.BatchSize.MaxMessageCount", {}, target.as_dict(), None, utc_now(), success=False, error=f"{type(exc).__name__}: {exc}", artifacts_dir=str(work), commands=[c.__dict__ for c in commands])
                else:
                    record.error = f"{type(exc).__name__}: {exc}"
                    record.success = False
                    record.commands = [c.__dict__ for c in commands]
                _write_new(work / "update_record.json", record.as_dict())
                if isinstance(exc, ActuatorError):
                    raise
                raise ActuatorError(str(exc)) from exc


def actuator_from_args(args: argparse.Namespace) -> FabricChannelConfigActuator:
    network = _load(Path(args.network))
    orderer = network["orderers"][0]
    return FabricChannelConfigActuator(
        channel=args.channel,
        peer_bin=Path(network["bin_dir"]) / "peer",
        configtxlator_bin=Path(network["bin_dir"]) / "configtxlator",
        fabric_cfg_path=Path(network["fabric_cfg_path"]),
        orderer_address=orderer["address"],
        orderer_tls_hostname=orderer["tls_hostname"],
        orderer_ca=Path(network["orderer_ca"]),
        orderer_admin_msp=Path(args.orderer_admin_msp),
        output_dir=Path(args.out_dir),
        min_b=args.min_b, max_b=args.max_b, cooldown_s=args.cooldown_s,
        poll_s=args.poll_s, commit_timeout_s=args.commit_timeout_s,
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--network", required=True, type=Path)
    p.add_argument("--channel", required=True)
    p.add_argument("--orderer-admin-msp", required=True, type=Path)
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--min-b", type=int, default=DEFAULT_MIN_B)
    p.add_argument("--max-b", type=int, default=DEFAULT_MAX_B)
    p.add_argument("--cooldown-s", type=float, default=30.0)
    p.add_argument("--poll-s", type=float, default=1.0)
    p.add_argument("--commit-timeout-s", type=float, default=60.0)
    sub = p.add_subparsers(dest="command", required=True)
    sub.add_parser("inspect")
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--target-b", type=int, required=True)
    execute_prepared = sub.add_parser("execute-prepared")
    execute_prepared.add_argument("--prepared-dir", type=Path, required=True)
    execute_prepared.add_argument("--approved-start-b", type=int, required=True)
    execute_prepared.add_argument("--approved-target", type=int, required=True)
    execute_prepared.add_argument("--execute", action="store_true")
    apply = sub.add_parser("apply")
    apply.add_argument("--target-b", type=int, required=True)
    apply.add_argument("--approved-target", type=int)
    apply.add_argument("--execute", action="store_true")
    rollback = sub.add_parser("rollback")
    rollback.add_argument("--record", type=Path, required=True)
    rollback.add_argument("--approved-target", type=int)
    rollback.add_argument("--execute", action="store_true")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    actuator = actuator_from_args(args)
    if args.command == "inspect":
        print(json.dumps(actuator.inspect_current(), indent=2, sort_keys=True))
        return 0
    if args.command == "prepare":
        current = actuator.inspect_current()["active_config"]
        base = FabricConfig(**{k: current[k] for k in ("max_message_count", "batch_timeout_ms", "preferred_max_bytes", "absolute_max_bytes", "orderers")})
        target = FabricConfig(max_message_count=args.target_b, batch_timeout_ms=base.batch_timeout_ms,
                              preferred_max_bytes=base.preferred_max_bytes,
                              absolute_max_bytes=base.absolute_max_bytes, orderers=base.orderers)
        print(json.dumps(actuator.prepare(target), indent=2, sort_keys=True))
        return 0
    if args.command == "execute-prepared":
        if not args.execute:
            print(json.dumps({"mode": "dry-run", "prepared_dir": str(args.prepared_dir),
                              "approved_start_b": args.approved_start_b,
                              "approved_target_b": args.approved_target,
                              "requires": "--execute"}, indent=2))
            return 0
        prep = _load(args.prepared_dir / "prepare_record.json")
        if int(prep.get("requested_config", {}).get("max_message_count", -1)) != args.approved_target:
            raise SystemExit("refusing live update: prepared target does not equal --approved-target")
        current = prep["before_config"]
        target = FabricConfig(max_message_count=args.approved_target, batch_timeout_ms=current["batch_timeout_ms"],
                              preferred_max_bytes=current["preferred_max_bytes"],
                              absolute_max_bytes=current["absolute_max_bytes"], orderers=current["orderers"])
        record = actuator.execute_prepared(args.prepared_dir, target, approved_before_b=args.approved_start_b)
        print(json.dumps(record.as_dict(), indent=2, sort_keys=True))
        return 0
    if args.command == "apply":
        if not args.execute:
            print(json.dumps({"mode": "dry-run", "target_max_message_count": args.target_b, "requires": "--execute plus --approved-target <same value>"}, indent=2))
            return 0
        if args.approved_target != args.target_b:
            raise SystemExit("refusing live update: --approved-target must equal --target-b")
        current = actuator.inspect_current()["active_config"]
        target = FabricConfig(**{k: current[k] for k in ("max_message_count", "batch_timeout_ms", "preferred_max_bytes", "absolute_max_bytes", "orderers")})
        target = FabricConfig(max_message_count=args.target_b, batch_timeout_ms=target.batch_timeout_ms,
                              preferred_max_bytes=target.preferred_max_bytes,
                              absolute_max_bytes=target.absolute_max_bytes, orderers=target.orderers)
        record = actuator.apply(target)
        print(json.dumps(record.as_dict(), indent=2, sort_keys=True))
        return 0
    rec = _load(args.record)
    if not rec.get("success"):
        raise SystemExit("rollback requires a successful update record")
    previous = rec.get("before_config") or {}
    target_b = int(previous.get("max_message_count", -1))
    if not args.execute:
        print(json.dumps({"mode": "dry-run", "rollback_target_max_message_count": target_b, "requires": "--execute plus --approved-target <same value>"}, indent=2))
        return 0
    if args.approved_target != target_b:
        raise SystemExit("refusing live rollback: --approved-target must equal the recorded previous value")
    current = actuator.inspect_current()["active_config"]
    target = validate_rollback_preconditions(rec, current, args.approved_target)
    print(json.dumps(actuator.apply(target).as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
