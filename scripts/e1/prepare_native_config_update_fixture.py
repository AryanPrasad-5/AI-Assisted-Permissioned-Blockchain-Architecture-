#!/usr/bin/env python3
"""Construct an offline MaxMessageCount-only CONFIG_UPDATE fixture.

No peer/orderer RPCs are made. Input block is read only; all outputs are new
files beneath a caller-selected empty output directory.
"""
from __future__ import annotations

import argparse
import copy
import json
import subprocess
from pathlib import Path


def run(binary: Path, *args: str) -> None:
    subprocess.run([str(binary), *args], check=True, capture_output=True)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--configtxlator", type=Path, required=True)
    p.add_argument("--config-block", type=Path, required=True)
    p.add_argument("--channel", required=True)
    p.add_argument("--target", type=int, required=True)
    p.add_argument("--target-batch-timeout", help="optional unrelated timeout mutation for negative fixtures, e.g. 3s")
    p.add_argument("--stale-orderer-read-version", type=int, help="optional invalid read-set version for negative fixtures")
    p.add_argument("--output-dir", type=Path, required=True)
    a = p.parse_args()
    out = a.output_dir
    out.mkdir(parents=True, exist_ok=False)
    block_json = out / "source_block.json"
    run(a.configtxlator, "proto_decode", "--input", str(a.config_block), "--type", "common.Block", "--output", str(block_json))
    block = json.loads(block_json.read_text(encoding="utf-8"))
    config = block["data"]["data"][0]["payload"]["data"]["config"]
    block_channel = block["data"]["data"][0]["payload"]["header"]["channel_header"]["channel_id"]
    if block_channel != a.channel:
        raise SystemExit(f"block channel {block_channel!r} does not match {a.channel!r}")
    original = copy.deepcopy(config)
    batch = config["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]["value"]
    if int(batch["max_message_count"]) == a.target:
        raise SystemExit("target already active")
    config["channel_group"]["groups"]["Orderer"]["values"]["BatchSize"]["value"]["max_message_count"] = a.target
    if a.target_batch_timeout:
        config["channel_group"]["groups"]["Orderer"]["values"]["BatchTimeout"]["value"]["timeout"] = a.target_batch_timeout
    original_json, updated_json = out / "original_config.json", out / "updated_config.json"
    original_json.write_text(json.dumps(original), encoding="utf-8")
    updated_json.write_text(json.dumps(config), encoding="utf-8")
    original_pb, updated_pb, update_pb = out / "original_config.pb", out / "updated_config.pb", out / "config_update.pb"
    run(a.configtxlator, "proto_encode", "--input", str(original_json), "--type", "common.Config", "--output", str(original_pb))
    run(a.configtxlator, "proto_encode", "--input", str(updated_json), "--type", "common.Config", "--output", str(updated_pb))
    run(a.configtxlator, "compute_update", "--channel_id", a.channel, "--original", str(original_pb), "--updated", str(updated_pb), "--output", str(update_pb))
    update_json = out / "config_update.json"
    run(a.configtxlator, "proto_decode", "--input", str(update_pb), "--type", "common.ConfigUpdate", "--output", str(update_json))
    update_doc = json.loads(update_json.read_text(encoding="utf-8"))
    if a.stale_orderer_read_version is not None:
        update_doc["read_set"]["groups"]["Orderer"]["version"] = str(a.stale_orderer_read_version)
    final_update_json = out / "config_update_for_envelope.json"
    final_update_json.write_text(json.dumps(update_doc), encoding="utf-8")
    final_update_pb = out / "config_update_for_envelope.pb"
    run(a.configtxlator, "proto_encode", "--input", str(final_update_json), "--type", "common.ConfigUpdate", "--output", str(final_update_pb))
    envelope_json = out / "envelope.json"
    envelope_json.write_text(json.dumps({"payload": {
        "header": {"channel_header": {"channel_id": a.channel, "type": 2}},
        "data": {"config_update": json.loads(final_update_json.read_text(encoding="utf-8")), "signatures": []}
    }}), encoding="utf-8")
    envelope_pb = out / "config_update_in_envelope.pb"
    run(a.configtxlator, "proto_encode", "--input", str(envelope_json), "--type", "common.Envelope", "--output", str(envelope_pb))
    (out / "fixture_metadata.json").write_text(json.dumps({
        "offline_only": True, "channel": a.channel,
        "before_max_message_count": int(batch["max_message_count"]),
        "target_max_message_count": a.target,
        "config_update_sha256": __import__("hashlib").sha256(final_update_pb.read_bytes()).hexdigest(),
        "optional_negative_mutations": {"target_batch_timeout": a.target_batch_timeout,
                                         "stale_orderer_read_version": a.stale_orderer_read_version},
        "envelope_sha256": __import__("hashlib").sha256(envelope_pb.read_bytes()).hexdigest(),
        "signature_status": "unsigned; must not be submitted"
    }, indent=2) + "\n", encoding="utf-8")
    print(envelope_pb)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
