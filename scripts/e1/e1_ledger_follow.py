"""Follow a channel's committed ledger and append continuous evidence (task A0d, blocker B-ledger).

This is the live face of ``fabric_perf.controllers.ledger_evidence``: a ``LedgerSource`` that reads a PEER (never an
orderer) with the same ``peer channel getinfo`` / ``peer channel fetch`` commands ``e1_block_fetch.py`` uses, parses the
blocks with the same decoder (``e1_ledger_blocks``), and verifies the same properties (block number, data hash, hash
chain, channel, validation flags). The collector then appends ``ledger_tx.csv``, ``ledger_blocks.csv`` and the
observation journal ``ledger_observations.csv`` as it goes.

Nothing is executed without ``--execute``. ``--execute`` runs the Linux peer CLI with POSIX paths, so it needs the Ubuntu
WSL distro (as ``e1_block_fetch.py``) and, like every Fabric use in this project, separate owner authorization. This task
(A0d) wrote and unit-tested the code against recorded/synthetic blocks only; it was never run against a peer.

The channel is explicit and validated by the shared A0b rules (``--channel`` has no default; the protected reference
channels and earlier experiments' channels are refused).
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Callable

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parents[1] / "src"))

import e1_block_fetch as bf  # noqa: E402
from fabric_perf.controllers import channel_selection as cs  # noqa: E402  (standard library only)
from fabric_perf.controllers.ledger_evidence import (  # noqa: E402
    LedgerBlock,
    LedgerCollector,
    LedgerEvidenceError,
    LedgerReadError,
    LedgerTx,
)

TOOL_VERSION = "e1_ledger_follow 0.1 (2026-11-21)"
NetRunner = Callable[[list, dict], "tuple[int | None, bytes, bytes]"]


class PeerCliLedgerSource:
    """``LedgerSource`` over ``peer channel getinfo`` / ``fetch``. ``runner`` is injected (tests pass a synthetic peer)."""

    def __init__(self, net, channel: str, runner: NetRunner, blocks_dir: Path):
        self.net, self.channel, self.runner = net, channel, runner
        self.blocks_dir = Path(blocks_dir)
        self.blocks_dir.mkdir(parents=True, exist_ok=True)
        self.env = net.peer_env(net.orgs()[0])
        self._attempts: dict[int, int] = {}

    def _run(self, argv: list) -> tuple[int | None, bytes, bytes]:
        try:
            return self.runner(argv, self.env)
        except Exception as exc:                                   # a failed process is a failed read, never a crash
            return None, b"", repr(exc).encode()

    def height(self) -> int:
        code, out, err = self._run(bf.getinfo_argv(self.net, self.channel))
        info = bf.parse_getinfo((out + b"\n" + err).decode("utf-8", "replace")) if code == 0 else None
        if info is None:
            raise LedgerReadError(f"getinfo exit {code}; no 'Blockchain info' JSON with a height")
        return int(info["height"])

    def block(self, number: int) -> LedgerBlock:
        attempt = self._attempts[number] = self._attempts.get(number, 0) + 1
        # Unique per process and attempt: a restart (or a retry) never reuses or overwrites an earlier raw block file.
        path = self.blocks_dir / f"{self.channel}_{number:06d}_{time.time_ns()}_a{attempt}.block"
        code, _, err = self._run(bf.fetch_argv(self.net, self.channel, number, path.resolve().as_posix()))
        if code != 0 or not path.exists() or path.stat().st_size == 0:
            raise LedgerReadError(f"fetch exit {code}: {err.decode('utf-8', 'replace').strip()[-160:]}")
        try:
            facts = bf.block_facts(path.read_bytes())
        except ValueError as exc:
            raise LedgerReadError(f"block file does not decode: {exc}") from exc
        parsed = facts["parsed"]
        txs = tuple(LedgerTx(tx_id=t["tx_id"], index=t["index"], header_type=int(t["header_type"]),
                             validation_code=(int(t["validation_code"]) if t["validation_code"] != "" else None),
                             env_bytes=int(t["env_bytes"]), channel_id=t["channel_id"],
                             ch_ts_ms=(int(t["ch_ts_ms"]) if t["ch_ts_ms"] != "" else None)) for t in parsed["txs"])
        return LedgerBlock(number=facts["number"], txs=txs, header_hash_hex=facts["header_hash"].hex(),
                           previous_hash_hex=facts["previous_hash"].hex(),
                           data_hash_ok=facts["computed_data_hash"] == facts["data_hash"], flags_len=parsed["flags_len"])


def main(argv: list[str] | None = None, runner: NetRunner | None = None, platform_name: str = sys.platform,
         sleep: Callable[[float], None] = time.sleep) -> int:
    ap = argparse.ArgumentParser(description=(__doc__ or "E1 continuous ledger follower").splitlines()[0])
    ap.add_argument("--channel", required=True, type=cs.experiment_channel_arg, metavar="CHANNEL",
                    help="the dedicated validation channel; the protected reference channels are refused")
    ap.add_argument("--network", required=True, type=Path, help="network description (as e1_block_fetch.py)")
    ap.add_argument("--out-dir", required=True, type=Path, help="journal directory (created exclusively; or resumed with --resume)")
    ap.add_argument("--collector-run-id", required=True)
    ap.add_argument("--max-polls", required=True, type=int, help="number of observations to take (no unbounded default)")
    ap.add_argument("--poll-interval-s", required=True, type=float, help="pause between observations, seconds (no default)")
    ap.add_argument("--resume", action="store_true", help="continue an existing journal (its first new row carries no rate)")
    ap.add_argument("--execute", action="store_true", help="run the peer CLI (needs Fabric authorization and the WSL host)")
    args = ap.parse_args(argv)
    try:
        if args.max_polls < 1 or args.poll_interval_s <= 0:
            raise ValueError("--max-polls must be positive and --poll-interval-s must be positive")
        net_doc = json.loads(args.network.read_text(encoding="utf-8"))
        from e1_channel_run import Network, RequestError
        try:
            net = Network(net_doc)
        except RequestError as err:
            raise ValueError(str(err)) from err
        if not args.execute:
            print("PLAN " + " ".join(bf.getinfo_argv(net, args.channel)))
            print("PLAN " + " ".join(bf.fetch_argv(net, args.channel, "<n>", "<out-dir>/blocks/<file>"))
                  + "   for each block committed after the baseline height")
            print(f"dry run: {args.max_polls} polls, {args.poll_interval_s}s apart; nothing executed or written "
                  "(add --execute once Fabric use is authorized)")
            return 0
        if runner is None and not platform_name.startswith("linux"):
            raise ValueError(f"--execute runs the Linux peer CLI with POSIX paths: run it inside the Ubuntu WSL distro "
                             f"(platform here: {platform_name})")
        # Raw block files live BESIDE the journal directory: the journal directory must be empty when a journal starts.
        raw_dir = args.out_dir.with_name(args.out_dir.name + ".blocks_raw")
        source = PeerCliLedgerSource(net, args.channel, runner or bf._net_subprocess_runner, raw_dir)
        collector = LedgerCollector(source, channel_id=args.channel, out_dir=args.out_dir,
                                    collector_run_id=args.collector_run_id, resume=args.resume)
        rows = collector.follow(max_polls=args.max_polls, poll_interval_s=args.poll_interval_s, sleep=sleep)
    except (OSError, ValueError, KeyError, json.JSONDecodeError, LedgerEvidenceError, cs.ChannelSelectionError) as err:
        print(f"INVALID REQUEST, UNREADABLE INPUT OR UNTRUSTED JOURNAL: {err!r}")
        return 2
    by_status: dict[str, int] = {}
    for row in rows:
        by_status[row.status] = by_status.get(row.status, 0) + 1
    print(json.dumps({"tool": TOOL_VERSION, "polls": len(rows), "by_status": by_status, "halted": collector.halted,
                      "recovery": collector.recovery, "out_dir": str(args.out_dir)}, indent=2, sort_keys=True))
    return 3 if collector.halted else 0


if __name__ == "__main__":
    sys.exit(main())
