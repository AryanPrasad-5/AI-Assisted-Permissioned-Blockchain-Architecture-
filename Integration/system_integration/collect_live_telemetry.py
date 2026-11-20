"""One bounded read-only Fabric operations scrape; never invokes controllers or actuators."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from fabric_perf.controllers.telemetry import PrometheusTelemetryAdapter
from fabric_perf.controllers import channel_selection as cs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True, help="Fabric operations metrics URL")
    # Read-only observer: any explicit, well-formed existing channel may be scraped (task A0b); nothing is defaulted.
    parser.add_argument("--channel", required=True, type=cs.observed_channel_arg, metavar="CHANNEL")
    parser.add_argument("--timeout", type=float, default=2.0)
    args = parser.parse_args()
    if not 0 < args.timeout <= 30:
        parser.error("timeout must be in (0, 30] seconds")
    snapshot = PrometheusTelemetryAdapter({"orderer": args.endpoint}, timeout_s=args.timeout).collect(args.channel)
    print(json.dumps(snapshot.as_dict(), indent=2, allow_nan=False))
    return 1 if snapshot.health.get("endpoint_errors") else 0


if __name__ == "__main__":
    raise SystemExit(main())
