                                                                           

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from e1_config_transition import (TransitionValidationError, channel_id_from_block,
                                  expected_batch_size_config, validate_batch_size_transition)


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def run(label: str, argv: list[str], env: dict[str, str], out_dir: Path, records: list[dict]) -> None:
    started = now()
    t0 = dt.datetime.now(dt.timezone.utc)
    result = subprocess.run(argv, capture_output=True, env=env, timeout=30, check=False)
    ended = now()
    (out_dir / f"{label}.stdout").write_bytes(result.stdout)
    (out_dir / f"{label}.stderr").write_bytes(result.stderr)
    records.append({"label": label, "argv": argv, "start_utc": started, "end_utc": ended,
                    "elapsed_ms": (dt.datetime.now(dt.timezone.utc) - t0).total_seconds() * 1000,
                    "exit_code": result.returncode, "stdout": f"{label}.stdout", "stderr": f"{label}.stderr"})
    if result.returncode != 0:
        raise RuntimeError(f"{label} failed with exit code {result.returncode}: {result.stderr.decode(errors='replace')}")


def config_facts(channel: str, block: dict, block_path: Path) -> dict:
    config = block["data"]["data"][0]["payload"]["data"]["config"]
    group = config["channel_group"]
    orderer = group["groups"]["Orderer"]
    values = orderer["values"]
    application = group["groups"]["Application"]
    return {
        "channel": channel,
        "block_number": int(block["header"]["number"]),
        "block_data_hash": block["header"]["data_hash"],
        "block_sha256": sha256(block_path),
        "block_timestamp_utc": block["data"]["data"][0]["payload"]["header"]["channel_header"].get("timestamp"),
        "max_message_count": int(values["BatchSize"]["value"]["max_message_count"]),
        "batch_size_version": values["BatchSize"]["version"],
        "batch_timeout_version": values["BatchTimeout"]["version"],
        "orderer_group_version": orderer["version"],
        "channel_group_version": group["version"],
        "config_sequence": config.get("sequence"),
        "batch_size_mod_policy": values["BatchSize"]["mod_policy"],
        "batch_timeout": values["BatchTimeout"]["value"]["timeout"],
        "preferred_max_bytes": int(values["BatchSize"]["value"]["preferred_max_bytes"]),
        "absolute_max_bytes": int(values["BatchSize"]["value"]["absolute_max_bytes"]),
        "consensus_type": values["ConsensusType"]["value"]["type"],
        "consenters": len(values["ConsensusType"]["value"]["metadata"]["consenters"]),
        "application_orgs": sorted(application["groups"].keys()),
        "orderer_group_mod_policy": orderer["mod_policy"],
        "orderer_admin_policy": orderer["policies"]["Admins"]["policy"],
    }


def verification_checks(cfg: dict, baseline: dict) -> dict[str, bool]:
                                                                                  
    return {
        "target_B_is_40": cfg["max_message_count"] == 40,
        "target_batch_size_version_is_1": cfg["batch_size_version"] == "1",
        "target_orderer_group_version_is_0": cfg["orderer_group_version"] == "0",
        "target_batch_timeout_version_is_0": cfg["batch_timeout_version"] == "0",
        "timeout_unchanged": cfg["batch_timeout"] == "2s" and baseline["batch_timeout"] == "2s",
        "preferred_bytes_unchanged": cfg["preferred_max_bytes"] == baseline["preferred_max_bytes"],
        "absolute_bytes_unchanged": cfg["absolute_max_bytes"] == baseline["absolute_max_bytes"],
        "orderer_count_unchanged": cfg["consenters"] == baseline["consenters"] == 3,
        "consensus_unchanged": cfg["consensus_type"] == baseline["consensus_type"] == "etcdraft",
        "application_orgs_unchanged": cfg["application_orgs"] == baseline["application_orgs"],
        "policy_unchanged": cfg["orderer_admin_policy"] == baseline["orderer_admin_policy"],
        "e1_r002_still_45": baseline["max_message_count"] == 45,
    }


class IndependentVerificationError(RuntimeError):
    def __init__(self, category: str, message: str):
        self.category = category
        super().__init__(message)


def config_projection(block: dict) -> dict:
    """Extract the bounded actuator projection from one decoded config block."""
    try:
        config = block["data"]["data"][0]["payload"]["data"]["config"]
        orderer = config["channel_group"]["groups"]["Orderer"]
        values = orderer["values"]
        batch = values["BatchSize"]["value"]
        timeout = values["BatchTimeout"]["value"]["timeout"]
        if timeout.endswith("ms"):
            timeout_ms = int(float(timeout[:-2]))
        elif timeout.endswith("s"):
            timeout_ms = int(float(timeout[:-1]) * 1000)
        else:
            raise ValueError(f"unsupported BatchTimeout {timeout!r}")
        consenters = values["ConsensusType"]["value"]["metadata"]["consenters"]
        return {"max_message_count": int(batch["max_message_count"]),
                "batch_timeout_ms": timeout_ms, "batch_timeout": timeout,
                "preferred_max_bytes": int(batch["preferred_max_bytes"]),
                "absolute_max_bytes": int(batch["absolute_max_bytes"]),
                "orderers": len(consenters)}
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise IndependentVerificationError("malformed_decoded_configuration", str(exc)) from exc


class IndependentFabricConfigVerifier:
    """Fetch and decode the committed config using a separate peer identity.

    The submission result is never an input to the decision. A fresh
    ``peer channel fetch config`` uses the configured organizational peer
    Admin MSP, and configtxlator decodes the returned common.Block.
    """

    def __init__(self, *, peer: Path, configtxlator: Path, fabric_cfg: Path,
                 msp_id: str, msp_path: Path, peer_address: str, peer_tls_root_cert: Path,
                 orderer: str, orderer_tls_hostname: str, orderer_ca: Path,
                 output_dir: Path, timeout_s: float = 30.0,
                 runner=subprocess.run):
        self.peer = Path(peer)
        self.configtxlator = Path(configtxlator)
        self.fabric_cfg = Path(fabric_cfg)
        self.msp_id = msp_id
        self.msp_path = Path(msp_path)
        self.peer_address = peer_address
        self.peer_tls_root_cert = Path(peer_tls_root_cert)
        self.orderer = orderer
        self.orderer_tls_hostname = orderer_tls_hostname
        self.orderer_ca = Path(orderer_ca)
        self.output_dir = Path(output_dir)
        self.timeout_s = float(timeout_s)
        self.runner = runner
        if self.timeout_s <= 0:
            raise ValueError("verification timeout must be positive")
        if not self.msp_id or self.msp_id == "OrdererMSP":
            raise ValueError("independent verifier requires a separate organization MSP identity")

    def _run(self, label: str, argv: list[str], env: dict[str, str], root: Path,
             commands: list[dict]) -> None:
        started = now()
        tick = time.monotonic()
        try:
            result = self.runner(argv, capture_output=True, env=env, timeout=self.timeout_s, check=False)
        except subprocess.TimeoutExpired as exc:
            commands.append({"label": label, "argv": argv, "started_utc": started,
                             "elapsed_ms": (time.monotonic() - tick) * 1000,
                             "outcome": "timeout"})
            raise IndependentVerificationError("retrieval_timeout", f"{label} exceeded {self.timeout_s}s") from exc
        except (OSError, subprocess.SubprocessError) as exc:
            commands.append({"label": label, "argv": argv, "started_utc": started,
                             "elapsed_ms": (time.monotonic() - tick) * 1000,
                             "outcome": "unavailable", "error": repr(exc)})
            raise IndependentVerificationError("verifier_unavailable", f"{label}: {exc}") from exc
        stdout, stderr = result.stdout or b"", result.stderr or b""
        (root / f"{label}.stdout").write_bytes(stdout)
        (root / f"{label}.stderr").write_bytes(stderr)
        commands.append({"label": label, "argv": argv, "started_utc": started,
                         "completed_utc": now(), "elapsed_ms": (time.monotonic() - tick) * 1000,
                         "exit_code": result.returncode, "stdout": f"{label}.stdout",
                         "stderr": f"{label}.stderr"})
        if result.returncode != 0:
            raise IndependentVerificationError("retrieval_failed", f"{label} exited {result.returncode}")

    def retrieve(self, channel: str) -> tuple[dict, dict]:
        root = self.output_dir / f"verify-{channel}-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex[:8]}"
        root.mkdir(parents=True, exist_ok=False)
        started = now()
        commands: list[dict] = []
        env = os.environ.copy()
        env.update({"FABRIC_CFG_PATH": str(self.fabric_cfg), "CORE_PEER_TLS_ENABLED": "true",
                    "CORE_PEER_LOCALMSPID": self.msp_id, "CORE_PEER_MSPCONFIGPATH": str(self.msp_path),
                    "CORE_PEER_ADDRESS": self.peer_address,
                    "CORE_PEER_TLS_ROOTCERT_FILE": str(self.peer_tls_root_cert)})
        block_path, json_path = root / "committed_config.pb", root / "committed_config.json"
        try:
            self._run("peer_version", [str(self.peer), "version"], env, root, commands)
            self._run("configtxlator_version", [str(self.configtxlator), "version"], env, root, commands)
            versions = {}
            for label in ("peer_version", "configtxlator_version"):
                output = (root / f"{label}.stdout").read_text(encoding="utf-8", errors="replace")
                match = re.search(r"^\s*Version:\s*(\S+)", output, flags=re.MULTILINE)
                if not match:
                    raise IndependentVerificationError("invalid_tool_version", f"{label} output has no Fabric Version field")
                versions[label.removesuffix("_version")] = match.group(1)
            self._run("peer_fetch_config", [str(self.peer), "channel", "fetch", "config", str(block_path),
                      "-c", channel, "-o", self.orderer,
                      "--ordererTLSHostnameOverride", self.orderer_tls_hostname,
                      "--tls", "--cafile", str(self.orderer_ca)], env, root, commands)
            if not block_path.is_file() or block_path.stat().st_size == 0:
                raise IndependentVerificationError("incomplete_response", "peer fetch returned no configuration block")
            self._run("configtxlator_decode", [str(self.configtxlator), "proto_decode", "--input",
                      str(block_path), "--type", "common.Block", "--output", str(json_path)], env, root, commands)
            if not json_path.is_file():
                raise IndependentVerificationError("incomplete_response", "configtxlator produced no decoded block")
            try:
                block = json.loads(json_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise IndependentVerificationError("malformed_decoded_configuration", str(exc)) from exc
            evidence = {"evidence_dir": str(root), "source": "peer channel fetch config + configtxlator proto_decode",
                        "verifier_msp_id": self.msp_id, "verifier_msp_path": str(self.msp_path),
                        "peer_address": self.peer_address, "orderer": self.orderer,
                        "tool_versions": versions,
                        "retrieval_started_utc": started, "retrieval_completed_utc": now(),
                        "observation_timestamp_utc": now(), "commands": commands,
                        "block_sha256": sha256(block_path), "decoded_json_sha256": sha256(json_path)}
            (root / "retrieval_evidence.json").write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
            return block, evidence
        except IndependentVerificationError as exc:
            evidence = {"evidence_dir": str(root), "source": "peer channel fetch config + configtxlator proto_decode",
                        "verifier_msp_id": self.msp_id, "retrieval_started_utc": started,
                        "retrieval_completed_utc": now(), "commands": commands,
                        "failure_category": exc.category, "error": str(exc)}
            (root / "verification_failure.json").write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
            raise

    def verify_transition(self, channel: str, before_block: dict, target, _submission_observed_block: dict | None = None) -> dict:
        """Independently classify the current config as pre, expected post, or third-party state."""
        evidence = None
        try:
            before_channel = channel_id_from_block(before_block)
            if before_channel != channel:
                raise IndependentVerificationError("wrong_channel", f"recorded pre-state channel {before_channel!r} != {channel!r}")
            expected = expected_batch_size_config(before_block, target.max_message_count)
            observed_block, evidence = self.retrieve(channel)
            observed_channel = channel_id_from_block(observed_block)
            if observed_channel != channel:
                raise IndependentVerificationError("wrong_channel", f"retrieved config channel {observed_channel!r} != {channel!r}")
            try:
                observed_doc = observed_block["data"]["data"][0]["payload"]["data"]["config"]
            except (KeyError, IndexError, TypeError) as exc:
                raise IndependentVerificationError("malformed_decoded_configuration", str(exc)) from exc
            observed_projection = config_projection(observed_block)
            before_projection = config_projection(before_block)
            transition_checks = None
            if observed_doc == expected and observed_projection == target.as_dict():
                transition_checks = validate_batch_size_transition(before_block, observed_block,
                                                                    target.max_message_count)
                result = {"success": True, "independent": True, "complete": True,
                        "outcome": "VERIFIED", "observed_config": observed_projection,
                        "observed_config_document": observed_doc,
                        "expected_config_document": expected,
                        "transition_checks": transition_checks, **evidence}
                (Path(evidence["evidence_dir"]) / "transition_verification.json").write_text(
                    json.dumps(result, indent=2) + "\n", encoding="utf-8")
                return result
            if observed_doc == before_block["data"]["data"][0]["payload"]["data"]["config"]:
                result = {"success": False, "independent": True, "complete": True,
                        "outcome": "PRE_STATE", "safe_to_retry": False,
                        "observed_config": before_projection,
                        "observed_config_document": observed_doc,
                        "expected_config_document": expected,
                        "error": "channel remains at exact pre-state; explicit safe resolution is required",
                        **evidence}
                (Path(evidence["evidence_dir"]) / "transition_verification.json").write_text(
                    json.dumps(result, indent=2) + "\n", encoding="utf-8")
                return result
            result = {"success": False, "independent": True, "complete": True,
                    "outcome": "UNEXPECTED_STATE", "safe_to_retry": False,
                    "observed_config": observed_projection,
                    "observed_config_document": observed_doc,
                    "expected_config_document": expected,
                    "error": "retrieved config matches neither exact pre-state nor expected post-state",
                    **evidence}
            (Path(evidence["evidence_dir"]) / "transition_verification.json").write_text(
                json.dumps(result, indent=2) + "\n", encoding="utf-8")
            return result
        except TransitionValidationError as exc:
            failure = IndependentVerificationError(exc.category, str(exc))
            if evidence and evidence.get("evidence_dir"):
                (Path(evidence["evidence_dir"]) / "transition_verification.json").write_text(
                    json.dumps({**evidence, "success": False, "independent": True,
                                "complete": False, "failure_category": failure.category,
                                "error": str(failure)}, indent=2) + "\n", encoding="utf-8")
            raise failure from exc
        except IndependentVerificationError as exc:
            if evidence and evidence.get("evidence_dir"):
                (Path(evidence["evidence_dir"]) / "transition_verification.json").write_text(
                    json.dumps({**evidence, "success": False, "independent": True,
                                "complete": False, "failure_category": exc.category,
                                "error": str(exc)}, indent=2) + "\n", encoding="utf-8")
            raise

    def __call__(self, *args, **kwargs):
        return self.verify_transition(*args, **kwargs)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--peer", type=Path, required=True)
    p.add_argument("--configtxlator", type=Path, required=True)
    p.add_argument("--fabric-cfg", type=Path, required=True)
    p.add_argument("--orderer-ca", type=Path, required=True)
    p.add_argument("--org1-admin-msp", type=Path, required=True)
    p.add_argument("--orderer", default="localhost:7050")
    p.add_argument("--orderer-tls-hostname", default="orderer.example.com")
    p.add_argument("--before-block", type=Path, help="optional prepared pre-update common.Block JSON for transition validation")
    args = p.parse_args()
    root = args.out_dir / f"independent-verification-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
    root.mkdir(parents=True, exist_ok=False)
    env = os.environ.copy()
    env.update({"FABRIC_CFG_PATH": str(args.fabric_cfg), "CORE_PEER_TLS_ENABLED": "true",
                "CORE_PEER_LOCALMSPID": "Org1MSP", "CORE_PEER_MSPCONFIGPATH": str(args.org1_admin_msp),
                "CORE_PEER_ADDRESS": "localhost:7051",
                "CORE_PEER_TLS_ROOTCERT_FILE": str(args.org1_admin_msp.parent.parent / ".." / "tlsca" / "tlsca.org1.example.com-cert.pem")})
                                                                            
    env["CORE_PEER_TLS_ROOTCERT_FILE"] = str(args.org1_admin_msp.parents[2] / "tlsca" / "tlsca.org1.example.com-cert.pem")
    commands: list[dict] = []
    facts = {}
    for channel in ("e1-cfg001", "e1-r002"):
        block_path = root / f"{channel}.pb"
        json_path = root / f"{channel}.json"
        run(f"{channel}-fetch", [str(args.peer), "channel", "fetch", "config", str(block_path), "-c", channel,
                                  "-o", args.orderer, "--ordererTLSHostnameOverride", args.orderer_tls_hostname,
                                  "--tls", "--cafile", str(args.orderer_ca)], env, root, commands)
        run(f"{channel}-decode", [str(args.configtxlator), "proto_decode", "--input", str(block_path),
                                  "--type", "common.Block", "--output", str(json_path)], env, root, commands)
        facts[channel] = config_facts(channel, json.loads(json_path.read_text(encoding="utf-8")), block_path)
    cfg, baseline = facts["e1-cfg001"], facts["e1-r002"]
    checks = verification_checks(cfg, baseline)
    transition = {"provided": False, "valid": None}
    if args.before_block:
        transition["provided"] = True
        try:
            transition["checks"] = validate_batch_size_transition(json.loads(args.before_block.read_text(encoding="utf-8")), json.loads((root / "e1-cfg001.json").read_text(encoding="utf-8")), 40)
            transition["valid"] = True
        except (OSError, json.JSONDecodeError, TransitionValidationError) as exc:
            transition["valid"] = False
            transition["error"] = f"{type(exc).__name__}: {exc}"
    checks["approved_transition_valid"] = transition["valid"] is not False
    result = {"tool": "e1_independent_config_verify 0.2", "verification_timestamp_utc": now(),
              "evidence_dir": str(root), "commands": commands, "e1_cfg001": cfg, "e1_r002": baseline,
              "transition": transition,
              "checks": checks, "all_checks_pass": all(checks.values())}
    (root / "independent_verification.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0 if result["all_checks_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
