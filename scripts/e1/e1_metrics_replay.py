"""Metrics replay (E0-0 remediation F3a, 2026-11-09): re-evaluate stored raw scrapes offline under the
current scraper rules. Reads a stored metrics phase directory (raw/<phase>_<round>_<target>.prom and
metrics_manifest.json, as written by e1_metrics_scraper.py), serves each stored body to run_scrape
through a fake fetch, and writes a new phase directory. Nothing is contacted; the stored evidence is
only read. Optionally summarizes two replayed phases.

  replay     --in-dir STORED_PHASE_DIR --out-dir NEW_DIR [--rule 0.1|current]
  summarize  --before DIR --after DIR --out-dir NEW_DIR --channel CH [--fresh-channel] [--expect-traffic]

--rule 0.1 replays with the 0.1 expected-family table (every family required on every target), to show
the defect; --rule current uses the defaults of the scraper in this repository.
Standard library only.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_metrics_scraper as ms  # noqa: E402

RULE_0_1 = {"orderer": ["blockcutter_block_fill_duration", "broadcast_processed_count", "consensus_etcdraft_is_leader",
                        "consensus_etcdraft_leader_changes", "consensus_etcdraft_committed_block_number"],
            "peer": ["ledger_blockchain_height", "ledger_block_processing_time", "ledger_blockstorage_commit_time",
                     "ledger_statedb_commit_time"], "other": []}


def replay(in_dir: Path, out_dir: Path, rule: str, argv: list[str]) -> dict:
    man = json.loads((in_dir / "metrics_manifest.json").read_text(encoding="utf-8"))
    phase, targets = man["phase"], man["targets"]
    if phase == "during":
        raise ValueError("replay supports single-round phases (before, after) only")
    bodies = {t["url"]: (in_dir / "raw" / f"{phase}_000000_{t['name']}.prom").read_bytes() for t in targets}
    kw = {"expected": RULE_0_1, "expected_any": {}} if rule == "0.1" else {}
    t = [0]

    def clock() -> int:
        t[0] += 1_000_000
        return 1_000_000_000_000 + t[0]
    m = ms.run_scrape(targets, phase, out_dir, fetch=lambda u, s: (200, bodies[u]), wall_ns=clock,
                      mono_ns=lambda: t[0], sleep=lambda s: None, argv=argv, **kw)
    m_in = {"stored_manifest_status": man["status"], "stored_anomalies": man["anomalies"],
            "stored_tool": man.get("tool"), "rule": rule}
    (out_dir / "replay_note.json").write_text(json.dumps(m_in, indent=2) + "\n", encoding="utf-8")
    return m


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("replay")
    r.add_argument("--in-dir", required=True, type=Path)
    r.add_argument("--out-dir", required=True, type=Path)
    r.add_argument("--rule", choices=["0.1", "current"], default="current")
    z = sub.add_parser("summarize")
    z.add_argument("--before", required=True, type=Path)
    z.add_argument("--after", required=True, type=Path)
    z.add_argument("--out-dir", required=True, type=Path)
    z.add_argument("--channel", required=True)
    z.add_argument("--fresh-channel", action="store_true")
    z.add_argument("--expect-traffic", action="store_true")
    a = ap.parse_args(argv)
    cmdline = sys.argv if argv is None else argv
    if a.cmd == "replay":
        m = replay(a.in_dir, a.out_dir, a.rule, cmdline)
        print(f"REPLAY rule={a.rule} {m['phase']} {m['status']} anomalies={m['anomalies']} "
              f"any_holders={m['expected_any_holders']}")
        return 0
    s = ms.summarize(a.before, a.after, a.out_dir, channel=a.channel, fresh_channel=a.fresh_channel,
                     expect_traffic=a.expect_traffic, argv=cmdline)
    print(f"SUMMARY {s['status']} anomalies={s['anomalies']} traffic={json.dumps(s['traffic'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
