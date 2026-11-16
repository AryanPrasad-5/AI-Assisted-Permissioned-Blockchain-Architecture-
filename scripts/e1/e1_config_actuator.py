                                                                               

                                                                           
                                           

                                             
                                                            
                                                                               
                                
                                        
                                                         
                                                                             
                                     

                                                                              
                                                                            
                                                                            
   

from __future__ import annotations

import argparse
import copy
import datetime as dt
try:
    import fcntl
except ImportError:                                                             
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

from fabric_perf.controllers.base import FabricConfig, coerce_config
from fabric_perf.controllers.transition_fence import ChannelTransitionFence, FenceError
from e1_config_transition import (              
    TransitionValidationError,
    expected_batch_size_config,
    validate_batch_size_transition,
)
from e1_independent_config_verify import IndependentFabricConfigVerifier


TOOL_VERSION = "e1_config_actuator 0.1"
CONFIG_UPDATE_TYPE = 2
DEFAULT_MIN_B, DEFAULT_MAX_B = 10, 800


class ActuatorError(RuntimeError):
    pass


class SafetyError(ActuatorError):
    pass


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
                                                                           
    try:
        return validate_batch_size_transition(before_block, after_block, target_b)
    except TransitionValidationError as exc:
        raise SafetyError(f"committed configuration verification failed [{exc.category}]: {exc}") from exc


def validate_rollback_preconditions(
    record: dict, current_block: dict, approved_target: int | None, *, authorization: str | None = None,
) -> FabricConfig:
    """Require exact full-state evidence from a fresh independently fetched block."""
    if record.get("success") is not True:
        raise SafetyError("rollback requires a successful update record")
    if authorization != f"ROLLBACK:{record.get('channel')}:{record.get('attempt_id')}":
        raise SafetyError("rollback requires explicit transition-specific authorization")
    before, after = record.get("before_config"), record.get("after_config")
    before_doc, after_doc = record.get("before_config_document"), record.get("after_config_document")
    if not all(isinstance(value, dict) for value in (before, after, before_doc, after_doc)):
        raise SafetyError("rollback record lacks exact full pre-state and verified post-state documents")
    if not record.get("verification_checks", {}).get("independent_verification"):
        raise SafetyError("rollback record lacks successful independent post-state verification")
    if not isinstance(current_block, dict):
        raise SafetyError("rollback requires a fresh independently retrieved configuration block")
    try:
        channel = current_block["data"]["data"][0]["payload"]["header"]["channel_header"]["channel_id"]
        current_doc = config_from_block_document(current_block)
        recorded_sequence = int(after_doc["sequence"])
        current_sequence = int(current_doc["sequence"])
        recorded_after_block = int(record["after_block_number"])
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise SafetyError(f"rollback current or recorded configuration evidence is incomplete: {exc}") from exc
    if channel != record.get("channel"):
        raise SafetyError("rollback current configuration belongs to a different channel")
    if current_sequence != recorded_sequence:
        raise SafetyError("rollback refused: current configuration sequence differs from recorded post-state")
    if block_number(current_block) < recorded_after_block:
        raise SafetyError("rollback current config block predates the recorded verified post-state")
    if current_doc != after_doc:
        raise SafetyError("rollback refused: current full configuration differs from recorded verified post-state")
    synthetic_before = {"data": {"data": [{"payload": {"data": {"config": before_doc}}}]}}
    synthetic_after = {"data": {"data": [{"payload": {"data": {"config": after_doc}}}]}}
    _validate_committed_transition(synthetic_before, synthetic_after, int(after.get("max_message_count", -1)))
    if config_from_block(synthetic_before)[0].as_dict() != before or config_from_block(synthetic_after)[0].as_dict() != after:
        raise SafetyError("rollback bounded config projection does not match recorded full configuration documents")
    try:
        target_b = int(before["max_message_count"])
    except (KeyError, TypeError, ValueError) as exc:
        raise SafetyError("rollback record lacks the exact recorded pre-update MaxMessageCount") from exc
    if approved_target != target_b:
        raise SafetyError("refusing live rollback: --approved-target must equal the recorded previous value")
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
    commit_to_independent_verification_ms: float | None = None
    before_block_data_hash: str | None = None
    after_block_data_hash: str | None = None
    before_block_sha256: str | None = None
    after_block_sha256: str | None = None
    verification_checks: dict | None = None
    before_config_document: dict | None = None
    after_config_document: dict | None = None

    def as_dict(self) -> dict:
        return self.__dict__.copy()


class FabricChannelConfigActuator:
                                                                            

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
        fence_dir: Path | None = None,
        independent_verifier: Callable[[str, dict, FabricConfig, dict], dict] | None = None,
        live_enabled: bool = False,
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
        self.transition_fence = ChannelTransitionFence(
            Path(fence_dir) if fence_dir is not None else self.fabric_cfg_path.parent / ".e1-actuator-fence",
            channel, cooldown_s=cooldown_s,
        )
        self.independent_verifier = independent_verifier
        self.live_enabled = live_enabled
        if live_enabled and not isinstance(independent_verifier, IndependentFabricConfigVerifier):
            raise SafetyError("live execution requires the concrete IndependentFabricConfigVerifier")
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
        try:
            with self.transition_fence.channel_lock():
                yield
        except FenceError as exc:
            raise SafetyError(str(exc)) from exc

    def _require_live_verifier(self) -> None:
        if not self.live_enabled or self.independent_verifier is None:
            raise SafetyError("live submission disabled: production execution requires live_enabled and an independent verifier")

    def _assert_signing_msp_key_permissions(self) -> None:
        """Check metadata only; never open or log private-key bytes."""
        if os.name != "posix":
            raise SafetyError("signing is disabled because POSIX private-key permissions cannot be verified")
        keystore = self.orderer_admin_msp / "keystore"
        try:
            directory_stat = keystore.stat(follow_symlinks=False)
            if not keystore.is_dir() or keystore.is_symlink():
                raise SafetyError("OrdererMSP keystore must be a real directory")
            if directory_stat.st_mode & 0o022:
                raise SafetyError("OrdererMSP keystore directory is group/world writable")
            entries = list(keystore.iterdir())
        except OSError as exc:
            raise SafetyError(f"cannot inspect OrdererMSP keystore metadata: {exc}") from exc
        private_files = []
        for path in entries:
            try:
                info = path.lstat()
            except OSError as exc:
                raise SafetyError("cannot inspect a keystore entry") from exc
            if path.is_symlink():
                raise SafetyError("symlinks are not permitted in the OrdererMSP keystore")
            if not path.is_file():
                continue
            private_files.append((path, info))
            if info.st_mode & 0o077:
                raise SafetyError("a private-key file grants group or other permissions")
            if hasattr(os, "geteuid") and info.st_uid != os.geteuid():
                raise SafetyError("a private-key file is not owned by the actuator process identity")
        if not private_files:
            raise SafetyError("OrdererMSP keystore contains no regular signing-key file")

    def _submit_config_update(self, label: str, envelope_path: Path, config_block_path: Path,
                              target: FabricConfig, work: Path,
                              commands: list[CommandRecord]) -> None:
        """Validate offline with Fabric's pinned native code, then stay blocked.

        This deliberately does not call ``peer channel update``. The verifier
        is new and has not had independent security review, and the actuator
        still lacks an approved signing lifecycle. Passing local validation is
        therefore necessary but not sufficient to authorize a broadcast.
        """
        repo = Path(__file__).resolve().parents[2]
        verifier_script = repo / "scripts" / "e1" / "run_fabric_config_verifier.sh"

        def run_inspection(path: Path, label: str) -> dict:
            command_index = len(commands)
            self._run(label, ["bash", str(verifier_script), "--inspect-only", "--config-block",
                              str(config_block_path), "--envelope", str(path), "--channel", self.channel],
                      work, commands)
            try:
                return json.loads((work / f"{command_index:02d}_{label}.stdout").read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise SafetyError(f"Fabric envelope inspection evidence is incomplete: {exc}") from exc

        before_sign = run_inspection(envelope_path, "inspect_config_update_before_signing")
        if before_sign.get("valid") is not True or before_sign.get("fabric_source_commit") != "f871cf92a026aba7b12e6f06d71ded3e6e659d71":
            raise SafetyError("Fabric-native pre-sign inspection did not establish channel-bound ConfigUpdate bytes")
        candidate = envelope_path
        if before_sign.get("signature_count") == 0 and before_sign.get("outer_signature_present") is not True:
            self._assert_signing_msp_key_permissions()
            candidate = work / "offline_signed_config_update.pb"
            try:
                source_bytes = envelope_path.read_bytes()
                fd = os.open(candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                opened = os.fstat(fd)
                if opened.st_mode & 0o077:
                    os.close(fd)
                    raise SafetyError("offline signing workspace cannot enforce private artifact mode 0600")
                if hasattr(os, "geteuid") and opened.st_uid != os.geteuid():
                    os.close(fd)
                    raise SafetyError("offline signing artifact is not owned by the actuator process identity")
                with os.fdopen(fd, "wb") as stream:
                    stream.write(source_bytes)
                    stream.flush()
                    os.fsync(stream.fileno())
            except OSError as exc:
                raise SafetyError(f"cannot create restricted offline signing copy: {exc}") from exc
            self._run("fabric_native_offline_signconfigtx", [
                str(self.peer_bin), "channel", "signconfigtx", "-f", str(candidate),
            ], work, commands)
        elif before_sign.get("signature_count") != 1 or before_sign.get("outer_signature_present") is not True:
            raise SafetyError("partially signed or multi-signature input is rejected; refusing to add signatures")

        after_sign = run_inspection(candidate, "inspect_config_update_after_signing")
        if (after_sign.get("valid") is not True
                or after_sign.get("signature_count") != 1
                or after_sign.get("outer_signature_present") is not True
                or after_sign.get("config_update_sha256") != before_sign.get("config_update_sha256")):
            raise SafetyError("offline signing changed ConfigUpdate bytes or produced incomplete Fabric signatures")

        verify_index = len(commands)
        self._run("fabric_native_envelope_verification", [
            "bash", str(verifier_script), "--config-block", str(config_block_path),
            "--envelope", str(candidate), "--channel", self.channel,
            "--target-max-message-count", str(target.max_message_count),
        ], work, commands)
        try:
            evidence = json.loads((work / f"{verify_index:02d}_fabric_native_envelope_verification.stdout").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise SafetyError(f"native Fabric verifier returned incomplete evidence: {exc}") from exc
        if evidence.get("valid") is not True or evidence.get("fabric_source_commit") != "f871cf92a026aba7b12e6f06d71ded3e6e659d71":
            raise SafetyError("native Fabric verifier did not establish exact v2.5.16 envelope validity")
        if evidence.get("config_update_sha256") != before_sign.get("config_update_sha256"):
            raise SafetyError("verified ConfigUpdate bytes differ from the exact offline-signed payload")
        raise SafetyError(
            "offline Fabric-native signing and verification passed; live configuration submission remains disabled"
        )

    def _verify_and_reconcile(self, transition_id: str, before_block: dict, target: FabricConfig,
                              observed_block: dict) -> dict:
        self._require_live_verifier()
        evidence = self.independent_verifier(self.channel, before_block, target, observed_block)  # type: ignore[misc]
        if not isinstance(evidence, dict) or evidence.get("success") is not True:
            raise SafetyError("independent verification did not establish the requested committed transition")
        if evidence.get("independent") is not True or evidence.get("complete") is not True:
            raise SafetyError("independent verifier must attest independent=true and complete=true")
        if evidence.get("outcome") != "VERIFIED" or evidence.get("source") != "peer channel fetch config + configtxlator proto_decode":
            raise SafetyError("independent verification provenance or outcome is invalid")
        observed = evidence.get("observed_config")
        if not isinstance(observed, dict) or observed != target.as_dict():
            raise SafetyError("independent verifier's observed configuration does not equal the requested configuration")
        if not evidence.get("transition_checks") or not all(evidence["transition_checks"].values()):
            raise SafetyError("independent verifier lacks passing full transition checks")
        state = self.transition_fence.read()
        expected_doc = (state or {}).get("context", {}).get("expected_config_document")
        if not expected_doc or evidence.get("expected_config_document") != expected_doc:
            raise SafetyError("independent verifier's expected full config differs from the durable transition record")
        try:
            self.transition_fence.reconcile(
                transition_id, observed_config=observed, expected_config=target.as_dict(),
                evidence={**evidence, "outcome": "VERIFIED"},
            )
        except FenceError as exc:
            raise SafetyError(str(exc)) from exc
        return evidence

    def reconcile_pending(self, *, explicit_not_submitted_authorization: str | None = None) -> dict:
        """Read-only independent reconciliation; never submits or retries."""
        if self.independent_verifier is None:
            raise SafetyError("restart reconciliation requires the concrete independent verifier")
        with self._exclusive_lock():
            state = self.transition_fence.read()
            if state is None or state["status"] not in {"PREPARED", "SUBMITTING", "UNKNOWN"}:
                return {"resolved": False, "status": state.get("status") if state else "NO_TRANSITION",
                        "reason": "no unresolved transition"}
            if state["status"] == "PREPARED":
                # The durable ordering guarantees the external submitter is not
                # invoked until SUBMITTING has been fsynced.
                self.transition_fence.mark_not_submitted(state["transition_id"],
                                                          "restart found durable PREPARED before submission-attempt marker")
                return {"resolved": True, "status": "NOT_SUBMITTED", "source": "durable lifecycle ordering"}
            context = state.get("context", {})
            before_block = context.get("before_block")
            if not isinstance(before_block, dict):
                return {"resolved": False, "status": state["status"],
                        "reason": "journal lacks exact pre-state block; remains fenced"}
            target = coerce_config(state["intended_config"])
            try:
                evidence = self.independent_verifier(self.channel, before_block, target, None)
            except Exception as exc:
                observation = {"independent": False, "complete": False,
                               "failure_category": getattr(exc, "category", "verifier_error"),
                               "error": f"{type(exc).__name__}: {exc}", "observed_at_utc": utc_now()}
                self.transition_fence.record_unresolved_observation(state["transition_id"], observation)
                return {"resolved": False, "status": state["status"], "classification": "VERIFIER_FAILURE",
                        "reason": observation["error"], "evidence": observation}
            if evidence.get("outcome") == "VERIFIED" and evidence.get("success") is True:
                if (evidence.get("independent") is not True or evidence.get("complete") is not True
                        or evidence.get("source") != "peer channel fetch config + configtxlator proto_decode"
                        or evidence.get("observed_config") != target.as_dict()
                        or evidence.get("expected_config_document") != context.get("expected_config_document")
                        or not evidence.get("transition_checks")
                        or not all(evidence["transition_checks"].values())):
                    self.transition_fence.record_unresolved_observation(state["transition_id"], evidence)
                    return {"resolved": False, "status": state["status"],
                            "reason": "independent verification evidence is incomplete or differs from durable journal"}
                self.transition_fence.reconcile(state["transition_id"],
                    observed_config=evidence["observed_config"], expected_config=target.as_dict(), evidence=evidence)
                return {"resolved": True, "status": "VERIFIED", "evidence": evidence}
            if evidence.get("outcome") == "PRE_STATE":
                if (evidence.get("independent") is not True or evidence.get("complete") is not True
                        or evidence.get("observed_config") != context.get("pre_config")
                        or evidence.get("expected_config_document") != context.get("expected_config_document")):
                    self.transition_fence.record_unresolved_observation(state["transition_id"], evidence)
                    return {"resolved": False, "status": state["status"],
                            "reason": "pre-state reconciliation evidence does not match the exact durable pre-state"}
                if explicit_not_submitted_authorization is None:
                    self.transition_fence.record_unresolved_observation(state["transition_id"], evidence)
                    return {"resolved": False, "status": state["status"], "classification": "PRE_STATE",
                            "reason": "exact pre-state observed; explicit safe-resolution authorization required",
                            "evidence": evidence}
                safe_evidence = {**evidence, "outcome": "NOT_SUBMITTED", "safe_to_retry": True,
                                 "resolution_basis": "operator attested no pending update remains at the orderer"}
                self.transition_fence.resolve_not_submitted(state["transition_id"],
                    observed_config=evidence["observed_config"], evidence=safe_evidence,
                    explicit_authorization=explicit_not_submitted_authorization)
                return {"resolved": True, "status": "NOT_SUBMITTED", "evidence": safe_evidence}
            self.transition_fence.record_unresolved_observation(state["transition_id"], evidence)
            return {"resolved": False, "status": state["status"],
                    "classification": evidence.get("outcome", "VERIFIER_FAILURE"),
                    "reason": evidence.get("error", "independent evidence is inconclusive"),
                    "evidence": evidence}

    def _record_transition_failure(self, transition_id: str, exc: Exception) -> None:
        try:
            state = self.transition_fence.read()
            if not state or state.get("transition_id") != transition_id:
                return
            if state["status"] == "PREPARED":
                self.transition_fence.mark_not_submitted(transition_id, f"pre-submission failure: {type(exc).__name__}: {exc}")
            elif state["status"] in {"SUBMITTING", "UNKNOWN"}:
                self.transition_fence.mark_unknown(transition_id, f"submission outcome requires independent reconciliation: {type(exc).__name__}: {exc}")
        except FenceError:
            # Corrupt or inconsistent durable state remains fail-closed.
            return

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
                                                                                  
        decision_epoch = time.time()
        with self._exclusive_lock():
            self._require_live_verifier()
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
                    before_config_document=config_from_block_document(before_block),
                )
                _write_new(work / "execution_preflight.json", {
                    "prepared_artifacts_dir": str(prepared_dir), "prepared_record": prepared,
                    "active_config": before.as_dict(), "policy": policy,
                    "verified_before_submission": True,
                })
                self.transition_fence.begin(work.name, target.as_dict(), context={
                    "pre_config": before.as_dict(), "before_block": before_block,
                    "before_block_number": block_number(before_block),
                    "before_config_document": config_from_block_document(before_block),
                    "expected_config_document": expected_batch_size_config(before_block, target.max_message_count),
                    "attempt_artifacts_dir": str(work),
                })
                self._submit_config_update("submit_prepared_update",
                                           Path(prepared_dir) / "config_update_in_envelope.pb",
                                           work / "before.pb", target, work, commands)
                raise SafetyError("configuration broadcast is not implemented; refusing to mark the transition submitted")
                submit_start = utc_now()
                submit_start_mono = time.monotonic()
                record.submission_start_timestamp_utc = submit_start
                self.transition_fence.mark_submitting(work.name)
                self.transition_fence.mark_submission_acknowledged(work.name, {"command": "peer channel update", "exit_code": 0})
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
                            record.after_config_document = config_from_block_document(latest_block)
                            record.after_block_number = block_number(latest_block)
                            record.after_block_data_hash = block_data_hash(latest_block)
                            record.after_block_sha256 = sha256(work / f"{stem}.pb")
                            record.config_block_commit_timestamp_utc = block_timestamp_utc(latest_block) or utc_now()
                            independent = self._verify_and_reconcile(work.name, before_block, target, latest_block)
                            record.active_verified_timestamp_utc = utc_now()
                            record.independent_verification_timestamp_utc = independent.get("timestamp_utc") or record.active_verified_timestamp_utc
                            record.verification_checks = {**(record.verification_checks or {}), "independent_verification": True}
                            record.commit_latency_ms = (time.monotonic() - submit_start_mono) * 1000
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
                self._record_transition_failure(work.name, exc)
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
                                                                               
        decision_epoch = time.time()
        with self._exclusive_lock():
            self._require_live_verifier()
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
                    before_config_document=original,
                )
                _write_new(work / "update_plan.json", {"read_set": update.get("read_set"), "write_set": update.get("write_set"), "policy": policy, "signer_msp_id": self.orderer_msp_id, "signer_msp_path": str(self.orderer_admin_msp)})
                self.transition_fence.begin(work.name, target.as_dict(), context={
                    "pre_config": before.as_dict(), "before_block": before_block,
                    "before_block_number": block_number(before_block),
                    "before_config_document": config_from_block_document(before_block),
                    "expected_config_document": expected_batch_size_config(before_block, target.max_message_count),
                    "attempt_artifacts_dir": str(work),
                })
                self._submit_config_update("submit_update", envelope_pb, work / "before.pb", target, work, commands)
                raise SafetyError("configuration broadcast is not implemented; refusing to mark the transition submitted")
                submit_start = utc_now()
                submit_start_mono = time.monotonic()
                record.submission_start_timestamp_utc = submit_start
                self.transition_fence.mark_submitting(work.name)
                self.transition_fence.mark_submission_acknowledged(work.name, {"command": "peer channel update", "exit_code": 0})
                record.submission_completion_timestamp_utc = utc_now()
                record.submission_timestamp_utc = record.submission_completion_timestamp_utc
                record.submission_duration_ms = (time.monotonic() - submit_start_mono) * 1000
                deadline = time.monotonic() + self.commit_timeout_s
                latest_block, latest_active, _ = before_block, before, policy
                while time.monotonic() < deadline:
                    try:
                        latest_block, latest_active, _ = self._fetch_decode(work, f"after_{len(commands)}", commands)
                        if block_number(latest_block) > record.before_block_number and latest_active.max_message_count == target.max_message_count:
                            record.verification_checks = _validate_committed_transition(before_block, latest_block, target.max_message_count)
                            independent = self._verify_and_reconcile(work.name, before_block, target, latest_block)
                            record.after_config = latest_active.as_dict()
                            record.after_config_document = config_from_block_document(latest_block)
                            record.after_block_number = block_number(latest_block)
                            record.config_block_commit_timestamp_utc = block_timestamp_utc(latest_block) or utc_now()
                            record.active_verified_timestamp_utc = utc_now()
                            record.independent_verification_timestamp_utc = independent.get("timestamp_utc") or record.active_verified_timestamp_utc
                            record.verification_checks["independent_verification"] = True
                            record.commit_latency_ms = (time.monotonic() - submit_start_mono) * 1000
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
                self._record_transition_failure(work.name, exc)
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
    live_enabled = bool(getattr(args, "execute", False) and args.command in {"apply", "execute-prepared", "rollback"})
    verifier = None
    if live_enabled or args.command == "reconcile":
        peer = network["peers"][0]
        verifier = IndependentFabricConfigVerifier(
            peer=Path(network["bin_dir"]) / "peer",
            configtxlator=Path(network["bin_dir"]) / "configtxlator",
            fabric_cfg=Path(network["fabric_cfg_path"]),
            msp_id=peer["msp_id"], msp_path=Path(peer["admin_msp_dir"]),
            peer_address=peer["address"], peer_tls_root_cert=Path(peer["tls_root_cert"]),
            orderer=orderer["address"], orderer_tls_hostname=orderer["tls_hostname"],
            orderer_ca=Path(network["orderer_ca"]),
            output_dir=Path(args.out_dir) / "independent-verification",
        )
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
        fence_dir=Path(args.fence_dir) if args.fence_dir else Path(network["fabric_cfg_path"]).parent / ".e1-actuator-fence",
        independent_verifier=verifier,
        live_enabled=live_enabled,
        min_b=args.min_b, max_b=args.max_b, cooldown_s=args.cooldown_s,
        poll_s=args.poll_s, commit_timeout_s=args.commit_timeout_s,
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--network", required=True, type=Path)
    p.add_argument("--channel", required=True)
    p.add_argument("--orderer-admin-msp", required=True, type=Path)
    p.add_argument("--out-dir", required=True, type=Path)
    p.add_argument("--fence-dir", type=Path, help="persistent shared channel-fence root; defaults beside FABRIC_CFG_PATH")
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
    rollback.add_argument("--authorize-rollback", action="store_true",
                          help="explicitly authorize this recorded transition's rollback")
    rollback.add_argument("--execute", action="store_true")
    reconcile = sub.add_parser("reconcile", help="independently reconcile the durable unresolved transition; never submits")
    reconcile.add_argument("--resolve-not-submitted", action="store_true",
                           help="explicitly resolve exact pre-state after external confirmation no update remains pending")
    reconcile.add_argument("--confirm-safe-resolution", help="must equal RESOLVE-NOT-SUBMITTED:<transition-id>")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    actuator = actuator_from_args(args)
    if args.command == "inspect":
        print(json.dumps(actuator.inspect_current(), indent=2, sort_keys=True))
        return 0
    if args.command == "reconcile":
        authorization = args.confirm_safe_resolution if args.resolve_not_submitted else None
        result = actuator.reconcile_pending(explicit_not_submitted_authorization=authorization)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result.get("resolved") or result.get("status") == "NO_TRANSITION" else 2
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
    if not args.authorize_rollback:
        raise SystemExit("refusing rollback: --authorize-rollback is required")
    if not isinstance(actuator.independent_verifier, IndependentFabricConfigVerifier):
        raise SystemExit("refusing rollback: independent configuration verifier is unavailable")
    current_block, retrieval_evidence = actuator.independent_verifier.retrieve(actuator.channel)
    if retrieval_evidence.get("channel") not in (None, actuator.channel):
        raise SystemExit("refusing rollback: independent retrieval channel provenance mismatch")
    authorization = f"ROLLBACK:{rec.get('channel')}:{rec.get('attempt_id')}"
    target = validate_rollback_preconditions(rec, current_block, args.approved_target, authorization=authorization)
    print(json.dumps(actuator.apply(target).as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
