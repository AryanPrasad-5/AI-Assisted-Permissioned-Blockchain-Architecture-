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
    FreshnessPolicy,
    ProductionTelemetryProvider,
    PrometheusTelemetryAdapter,
    ledger_observation_from_blocks_csv,
    load_direct_cut_events,
)
from fabric_perf.controllers.base import confirmed_config  # noqa: E402
from fabric_perf.controllers.config_evidence import ConfigEvidenceError, build_evidence, load_block_file  # noqa: E402
from fabric_perf.controllers import channel_selection as cs  # noqa: E402  (standard library only)


class VerifiedConfigReader:
    """Configuration reader that carries the provenance of its evidence into every frame.

    The block size is read from a decoded CONFIG block that is validated against the channel; it is
    never a constant. (The previous parser also failed on millisecond timeouts such as ``2000ms``,
    because ``"2000ms".endswith("s")``; the shared parser reads ms, s and m correctly.)
    """

    def __init__(self, path: Path, channel: str):
        block, digest = load_block_file(path)
        evidence = build_evidence(channel=channel, block=block, block_path=path, block_sha256=digest)
        self._config = confirmed_config(evidence.as_current_config())
        self._provenance = dict(evidence.provenance)

    def __call__(self) -> FabricConfig:
        return self._config

    def provenance(self) -> dict:
        return dict(self._provenance)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--channel", required=True, type=cs.experiment_channel_arg, metavar="CHANNEL",
                        help="the dedicated validation channel the frames describe; the protected reference channels are refused")
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
    parser.add_argument("--ledger-observations-csv", type=Path, default=None,
                        help="opt in (task A0d): the append-only observation journal written by ledger_evidence.LedgerCollector "
                             "(e1_ledger_follow.py). Committed throughput is then computed from ledger observation time "
                             "instead of this reader's own polling rhythm. Needs the continuous trio above.")
    parser.add_argument("--recorder-latency", choices=ContinuousCaliperLedgerAdapter.RECORDER_LATENCY_MODES, default="column",
                        help="where client latency comes from: 'column' (default) needs a latency_ms column in the recorder CSV; "
                             "'derive_from_timestamps' computes final_ms - create_ms of each successful row (the v1 recorder "
                             "has no latency_ms column; the v2 live stream is cross-checked against it)")
    parser.add_argument("--adapter-window-s", type=float, default=None,
                        help="opt in (task A0e): the continuous adapter's observation window in seconds. Omitted: the adapter's "
                             "own default window is used, exactly as before. Needs the continuous trio above; must be finite "
                             "and positive, and longer than the ledger collector's poll interval.")
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
    parser.add_argument("--max-observation-age-s", type=float, default=None,
                        help="validated freshness limit in seconds (default 5.0, accepted range (0, 30])")
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
    if args.ledger_observations_csv is not None and not all(continuous_paths):
        parser.error("--ledger-observations-csv needs --recorder-csv, --submits-csv and --ledger-tx-csv")
    if args.adapter_window_s is not None:
        if not all(continuous_paths):
            parser.error("--adapter-window-s needs --recorder-csv, --submits-csv and --ledger-tx-csv")
        if not (args.adapter_window_s > 0 and args.adapter_window_s < float("inf")):
            parser.error("--adapter-window-s must be a finite number greater than zero")
    if args.telemetry_samples < 1 or args.telemetry_interval_s < 0:
        parser.error("telemetry samples must be positive and interval must be non-negative")
    try:
        freshness_policy = FreshnessPolicy() if args.max_observation_age_s is None else FreshnessPolicy(
            max_age_s=args.max_observation_age_s)
        config_reader = VerifiedConfigReader(args.config_json, args.channel) if args.config_json else None
    except (ConfigEvidenceError, OSError, ValueError) as exc:
        parser.error(f"rejected: {exc}")
    ledger_reader = None
    if args.ledger_blocks_csv:
        ledger_reader = lambda: ledger_observation_from_blocks_csv(args.ledger_blocks_csv)
    continuous_source = None
    if all(continuous_paths):
        window_option = {} if args.adapter_window_s is None else {"window_s": args.adapter_window_s}
        continuous_source = ContinuousCaliperLedgerAdapter(
            recorder_csv=args.recorder_csv, submits_csv=args.submits_csv, ledger_csv=args.ledger_tx_csv,
            channel_id=args.channel, run_id=args.run_id, recorder_latency=args.recorder_latency,
            ledger_observations_csv=args.ledger_observations_csv, **window_option,
        )
    provider = ProductionTelemetryProvider(
        PrometheusTelemetryAdapter(endpoints), args.channel,
        config_reader=config_reader,
        caliper_summary_path=args.caliper_summary,
        continuous_source=continuous_source,
        ledger_reader=ledger_reader,
        required_fields=() if args.allow_missing_client_metrics else (
            "observed_arrival_rate_tps", "observed_throughput_tps", "observed_latency_ms", "queue_depth"),
        freshness_policy=freshness_policy,
    )
    frames = []
    validations = []
    for index in range(args.telemetry_samples):
        if index and args.telemetry_interval_s:
            time.sleep(args.telemetry_interval_s)
        observed = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        frame = provider.collect(run_id=args.run_id, decision_index=index, observation_timestamp_utc=observed)
        try:
            frame.validate(required_fields=provider.required_fields, policy=provider.freshness_policy)
            validations.append({"decision_index": index, "status": "PASS"})
        except Exception as exc:
            validations.append({"decision_index": index, "status": "REJECTED", "error": f"{type(exc).__name__}: {exc}",
                                "reason_code": getattr(exc, "reason_code", None),
                                "abstention_reason": getattr(exc, "abstention_reason", None)})
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
        "tool": "run_production_shadow_validation 1.1 (2026-11-17: validated config evidence, one freshness policy)",
        "run_id": args.run_id,
        "channel": args.channel,
        "freshness_policy": provider.freshness_policy.as_dict(),
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

