"""Capture production Fabric telemetry into the safe shadow integration path.

This command is read-only with respect to Fabric. It samples the configured
Prometheus endpoints and optionally joins timestamped Caliper/ledger evidence.
It never invokes the configuration actuator. Missing or delayed Caliper data is
recorded and causes the integrated runner to stop when required fields are
enabled.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from fabric_perf.controllers import (  # noqa: E402
    FabricConfig,
    ContinuousCaliperLedgerAdapter,
    ProductionTelemetryProvider,
    PrometheusTelemetryAdapter,
    ledger_observation_from_blocks_csv,
    load_direct_cut_events,
)


def config_from_block(path: Path) -> FabricConfig:
    block = json.loads(path.read_text(encoding="utf-8"))
    orderer = block["data"]["data"][0]["payload"]["data"]["config"]["channel_group"]["groups"]["Orderer"]
    values = orderer["values"]
    timeout = values["BatchTimeout"]["value"]["timeout"]
    timeout_ms = int(float(timeout[:-1]) * 1000) if timeout.endswith("s") else int(timeout[:-2])
    consenters = values["ConsensusType"]["value"]["metadata"]["consenters"]
    batch = values["BatchSize"]["value"]
    return FabricConfig(max_message_count=int(batch["max_message_count"]), batch_timeout_ms=timeout_ms,
                        preferred_max_bytes=int(batch["preferred_max_bytes"]),
                        absolute_max_bytes=int(batch["absolute_max_bytes"]), orderers=len(consenters))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--channel", required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--config-json", type=Path)
    parser.add_argument("--ledger-blocks-csv", type=Path)
    parser.add_argument("--caliper-summary", type=Path)
    parser.add_argument("--recorder-csv", type=Path,
                        help="append-only Caliper recorder CSV for continuous mode")
    parser.add_argument("--submits-csv", type=Path,
                        help="append-only Caliper submit-event CSV for continuous mode")
    parser.add_argument("--ledger-tx-csv", type=Path,
                        help="append-only verified Fabric ledger_tx.csv for continuous mode")
    parser.add_argument("--telemetry-samples", type=int, default=1,
                        help="finite number of frames to capture")
    parser.add_argument("--telemetry-interval-s", type=float, default=0.0,
                        help="delay between bounded samples")
    parser.add_argument("--cut-events", type=Path,
                        help="optional validated CSV of direct MESSAGE_COUNT/TIMEOUT events")
    parser.add_argument("--prom-endpoint", action="append", default=None,
                        help="name=url endpoint; repeat for each orderer (defaults to the three E1 endpoints)")
    parser.add_argument("--allow-missing-client-metrics", action="store_true",
                        help="capture Prometheus-only frames; does not make client metrics available")
    args = parser.parse_args(argv)
    if args.out.exists():
        parser.error(f"refusing to overwrite {args.out}")
    endpoint_specs = args.prom_endpoint or [
        "orderer1=http://localhost:9443/metrics",
        "orderer2=http://localhost:9446/metrics",
        "orderer3=http://localhost:9447/metrics",
    ]
    endpoints = {}
    for item in endpoint_specs:
        name, sep, url = item.partition("=")
        if not sep or not name or not url:
            parser.error("--prom-endpoint must be name=url")
        endpoints[name] = url
    continuous_paths = (args.recorder_csv, args.submits_csv, args.ledger_tx_csv)
    if any(continuous_paths) and not all(continuous_paths):
        parser.error("--recorder-csv, --submits-csv and --ledger-tx-csv must be supplied together")
    if args.telemetry_samples < 1 or args.telemetry_interval_s < 0:
        parser.error("telemetry samples must be positive and interval must be non-negative")
    config = (config_from_block(args.config_json) if args.config_json else None)
    ledger_reader = None
    if args.ledger_blocks_csv:
        ledger_reader = lambda: ledger_observation_from_blocks_csv(args.ledger_blocks_csv)
    continuous_source = None
    if all(continuous_paths):
        continuous_source = ContinuousCaliperLedgerAdapter(
            recorder_csv=args.recorder_csv, submits_csv=args.submits_csv, ledger_csv=args.ledger_tx_csv,
            channel_id=args.channel, run_id=args.run_id,
        )
    provider = ProductionTelemetryProvider(
        PrometheusTelemetryAdapter(endpoints), args.channel,
        config_reader=(lambda: config) if config is not None else None,
        caliper_summary_path=args.caliper_summary,
        continuous_source=continuous_source,
        ledger_reader=ledger_reader,
        required_fields=() if args.allow_missing_client_metrics else (
            "observed_arrival_rate_tps", "observed_throughput_tps", "observed_latency_ms", "queue_depth"),
    )
    frames = []
    validations = []
    for index in range(args.telemetry_samples):
        if index and args.telemetry_interval_s:
            time.sleep(args.telemetry_interval_s)
        observed = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        frame = provider.collect(run_id=args.run_id, decision_index=index, observation_timestamp_utc=observed)
        try:
            frame.validate(required_fields=provider.required_fields, max_age_s=5.0)
            validations.append({"decision_index": index, "status": "PASS"})
        except Exception as exc:
            validations.append({"decision_index": index, "status": "REJECTED", "error": f"{type(exc).__name__}: {exc}"})
        frames.append(frame)
    cut_events = None
    cut_event_error = None
    if args.cut_events:
        try:
            cut_events = load_direct_cut_events(args.cut_events, channel_id=args.channel)
        except Exception as exc:
            cut_event_error = f"{type(exc).__name__}: {exc}"
    validation = {"status": "PASS" if all(row["status"] == "PASS" for row in validations) else "REJECTED",
                  "samples": validations}
    result = {
        "tool": "run_production_shadow_validation 1.0",
        "run_id": args.run_id,
        "channel": args.channel,
        "mode": "read_only_shadow_capture",
        "live_configuration_submission": False,
        "observed_timestamp_utc": observed,
        "validation": validation,
        "frame": frames[-1].as_dict(),
        "frames": [item.as_dict() for item in frames],
        "direct_cut_events": cut_events,
        "direct_cut_event_error": cut_event_error,
        "safety": {"actuator_invoked": False, "configuration_updates": 0, "rollback": False},
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"out": str(args.out), "validation": validation, "live_configuration_submission": False}, indent=2, sort_keys=True))
    return 0 if validation["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())

