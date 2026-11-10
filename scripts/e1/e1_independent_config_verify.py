                                                                           

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from e1_config_transition import TransitionValidationError, validate_batch_size_transition              


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
