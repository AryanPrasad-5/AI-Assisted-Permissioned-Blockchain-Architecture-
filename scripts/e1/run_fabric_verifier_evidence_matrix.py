#!/usr/bin/env python3
"""Run a read-only native verifier matrix and save machine-readable evidence."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", type=Path, required=True)
    p.add_argument("--config-block", type=Path, required=True)
    p.add_argument("--fixture-dir", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--source-commit", default="f871cf92a026aba7b12e6f06d71ded3e6e659d71")
    a = p.parse_args()
    a.output_dir.mkdir(parents=True, exist_ok=True)
    exe = a.repo_root / "scripts/e1/run_fabric_config_verifier.sh"
    cases = [
        ("valid_orderer_admin", "actual_orderer_admin_fixture_v5/config_update_in_envelope.pb", "e1-cfg001", 41, True, ""),
        ("org1_admin_unauthorized", "org1_admin_fixture/config_update_in_envelope.pb", "e1-cfg001", 42, False, "not satisfied"),
        ("unrelated_batch_timeout_change", "unrelated_timeout_fixture/config_update_in_envelope.pb", "e1-cfg001", 43, False, "outside Orderer.BatchSize.MaxMessageCount"),
        ("stale_read_set", "stale_read_set_fixture/config_update_in_envelope.pb", "e1-cfg001", 44, False, "ReadSet"),
        ("wrong_channel", "actual_orderer_admin_fixture_v5/config_update_in_envelope.pb", "e1-r002", 41, False, "channel"),
        ("wrong_requested_target", "actual_orderer_admin_fixture_v5/config_update_in_envelope.pb", "e1-cfg001", 42, False, "outside Orderer.BatchSize.MaxMessageCount"),
    ]
    matrix = []
    env = os.environ.copy()
    env["FABRIC_2_5_16_SOURCE"] = env.get("FABRIC_2_5_16_SOURCE", "")
    for name, relative, channel, target, expected_valid, expected_error in cases:
        envelope = a.fixture_dir / relative
        argv = ["bash", str(exe), "--config-block", str(a.config_block), "--envelope", str(envelope),
                "--channel", channel, "--target-max-message-count", str(target)]
        run = subprocess.run(argv, capture_output=True, text=True, env=env, check=False)
        stdout_path = a.output_dir / f"{name}.stdout.json"
        stderr_path = a.output_dir / f"{name}.stderr.txt"
        stdout_path.write_text(run.stdout, encoding="utf-8")
        stderr_path.write_text(run.stderr, encoding="utf-8")
        try:
            output = json.loads(run.stdout)
        except json.JSONDecodeError:
            output = {}
        valid = output.get("valid") is True
        exit_consistent = run.returncode == 0 if expected_valid else run.returncode != 0
        passed = (valid == expected_valid and exit_consistent
                  and (not expected_error or expected_error in output.get("error", "")))
        matrix.append({"case": name, "expected_valid": expected_valid, "observed_valid": valid,
                       "expected_error_fragment": expected_error or None, "observed_error": output.get("error"),
                       "exit_code": run.returncode, "test_passed": passed,
                       "config_block_sha256": digest(a.config_block), "envelope_sha256": digest(envelope),
                       "stdout_file": stdout_path.name, "stderr_file": stderr_path.name,
                       "fabric_source_commit": output.get("fabric_source_commit")})
    report = {"task": "Phase 2B.1R.6 offline native Fabric ConfigUpdateEnvelope verification",
              "live_submission_performed": False, "channel_update_or_rollback_performed": False,
              "docker_or_fabric_restart_performed": False, "fabric_source_commit": a.source_commit,
              "matrix": matrix, "passed": all(item["test_passed"] for item in matrix)}
    (a.output_dir / "validation_matrix.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"passed": report["passed"], "case_count": len(matrix), "output": str(a.output_dir / "validation_matrix.json")}, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
