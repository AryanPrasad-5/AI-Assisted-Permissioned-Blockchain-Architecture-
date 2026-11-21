"""Synthetic doubles for the A0d tests (2026-11-21). Nothing here is Fabric output; no network, Docker or Caliper is touched.

``ScriptedSource`` is a ``LedgerSource`` whose height and blocks are set by the test. ``FakeClock`` is a controllable
timezone-aware clock. Block builders make ``LedgerBlock`` values with the fields the collector checks.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fabric_perf.controllers.ledger_evidence import LedgerBlock, LedgerReadError, LedgerTx

CHANNEL = "e1-offline.a0d"
T0 = datetime(2026, 11, 21, 10, 0, 0, tzinfo=timezone.utc)


class FakeClock:
    """A timezone-aware clock the test advances explicitly. Each call returns the current time (no auto-advance)."""

    def __init__(self, start: datetime = T0):
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> datetime:
        self.now = self.now + timedelta(seconds=seconds)
        return self.now


def tx(tx_id: str, index: int, code: int | None = 0, header_type: int = 3, channel: str = CHANNEL) -> LedgerTx:
    return LedgerTx(tx_id=tx_id, index=index, header_type=header_type, validation_code=code, env_bytes=100,
                    channel_id=channel, ch_ts_ms=1_790_000_000_000 + index)


def make_block(number: int, txs: list[LedgerTx], *, previous: str | None = None, flags_len: int | None = None,
               data_hash_ok: bool | None = True, header: str | None = None) -> LedgerBlock:
    return LedgerBlock(number=number, txs=tuple(txs), header_hash_hex=header if header is not None else f"{number:064x}",
                       previous_hash_hex=previous if previous is not None else (f"{number - 1:064x}" if number else ""),
                       data_hash_ok=data_hash_ok, flags_len=len(txs) if flags_len is None else flags_len)


def valid_block(number: int, count: int, *, prefix: str = "t") -> LedgerBlock:
    return make_block(number, [tx(f"{prefix}{number}-{i}", i) for i in range(count)])


class ScriptedSource:
    """height is ``self.current_height``; blocks come from ``self.blocks``. ``fail_height`` / ``fail_blocks`` raise a read error."""

    def __init__(self, height: int = 0):
        self.current_height = height
        self.blocks: dict[int, LedgerBlock] = {}
        self.fail_height = False
        self.fail_blocks: set[int] = set()
        self.fetches: list[int] = []
        self.height_value_override = None

    def height(self) -> int:
        if self.fail_height:
            raise LedgerReadError("peer unreachable")
        return self.current_height if self.height_value_override is None else self.height_value_override

    def block(self, number: int) -> LedgerBlock:
        self.fetches.append(number)
        if number in self.fail_blocks or number not in self.blocks:
            raise LedgerReadError(f"cannot read block {number}")
        return self.blocks[number]

    def commit(self, *blocks: LedgerBlock) -> None:
        """Make these (consecutive, in order) blocks part of the ledger and raise the height accordingly."""
        for block in blocks:
            self.blocks[block.number] = block
            self.current_height = max(self.current_height, block.number + 1)


# ====================================================================== an offline pipeline rig
import csv  # noqa: E402
from dataclasses import replace  # noqa: E402
from pathlib import Path  # noqa: E402

from fabric_perf.controllers import (  # noqa: E402
    ContinuousCaliperLedgerAdapter,
    FabricConfig,
    IntegratedControllerRunner,
    PredictiveController,
    ProductionTelemetryProvider,
    PrometheusTelemetryAdapter,
    ReactiveController,
    StaticController,
)
from fabric_perf.controllers.base import PerformancePrediction  # noqa: E402
from fabric_perf.controllers.ledger_evidence import LedgerCollector  # noqa: E402
from fabric_perf.controllers.stage3_frame_quality import Stage3FrameQualityContract  # noqa: E402

CONFIG = FabricConfig(max_message_count=45)
THREE = {"static", "reactive", "predictive"}
RECORDER_HEADER = ["tx_id", "seq_finish", "create_ms", "final_ms", "latency_ms", "latency_state", "status", "verified",
                   "err", "obs_wall_ms", "obs_hr_ns", "worker", "round", "label"]
SUBMIT_HEADER = ["seq_submit", "t_submit_evt_ms", "count"]
LEDGER_TX_HEADER = "tx_id,block,index,header_type,validation_code,env_bytes,channel_id,ch_ts_ms,obs_seq,observed_utc\n"


class FakeModel:
    def predict(self, arrival, config):
        return PerformancePrediction(10.0, 100.0)


class FakeForecaster:
    def predict(self, history, horizon):
        return 77.0


def iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def epoch_ms(moment: datetime) -> int:
    return int(round(moment.timestamp() * 1000))


class Rig:
    """Synthetic ledger source + ledger collector + client CSVs + adapter + provider, all on one FakeClock.

    ``client(n)`` appends n successful client completions (latency 100 ms) and n submit events inside the last second of
    fake time. ``ledger(count)`` commits one block of ``count`` valid transactions. ``step(s)`` advances the clock and
    polls the collector once. ``frame(i)`` asks the provider for a frame at the current fake time.
    """

    def __init__(self, tmp_path: Path, *, window_s: float = 5.0, stale_after_s: float = 5.0, start_height: int = 0,
                 provider_required_fields=(), observations: bool = True, prometheus_ok: bool = True):
        self.tmp = Path(tmp_path)
        self.clock = FakeClock()
        self.source = ScriptedSource(start_height)
        self.collector = LedgerCollector(self.source, channel_id=CHANNEL, out_dir=self.tmp / "ledger", collector_run_id="rig",
                                         clock=self.clock)
        self.recorder, self.submits = self.tmp / "recorder.csv", self.tmp / "submits.csv"
        self._write(self.recorder, RECORDER_HEADER)
        self._write(self.submits, SUBMIT_HEADER)
        self.seq = 0
        self.adapter = ContinuousCaliperLedgerAdapter(
            recorder_csv=self.recorder, submits_csv=self.submits, ledger_csv=self.collector.paths["tx"], channel_id=CHANNEL,
            run_id="r", window_s=window_s, stale_after_s=stale_after_s, recorder_latency="derive_from_timestamps",
            ledger_observations_csv=self.collector.paths["observations"] if observations else None)
        def fetch(url, timeout):
            if not prometheus_ok:
                raise ConnectionRefusedError("orderer metrics endpoint is down")
            return f'consensus_etcdraft_is_leader{{channel="{CHANNEL}"}} 1\n'
        prometheus = PrometheusTelemetryAdapter({"orderer1": "http://orderer1/metrics"}, fetch=fetch)
        self.provider = ProductionTelemetryProvider(
            prometheus, CHANNEL, config_reader=lambda: CONFIG, continuous_source=self.adapter,
            arrival_history_reader=self._history, required_fields=provider_required_fields,
            required_prometheus_metrics=("raft_leader_count",), clock=None)

    @staticmethod
    def _write(path: Path, header) -> None:
        with path.open("w", newline="", encoding="utf-8") as fh:
            csv.writer(fh, lineterminator="\n").writerow(header)

    def _history(self):
        return (iso(self.clock.now - timedelta(seconds=2)), iso(self.clock.now - timedelta(seconds=1))), (10.0, 12.0)

    def client(self, n: int = 5, *, latency_ms: int = 100, status: str = "success") -> None:
        now = epoch_ms(self.clock.now)
        with self.recorder.open("a", newline="", encoding="utf-8") as fh, self.submits.open("a", newline="", encoding="utf-8") as sub:
            w, s = csv.writer(fh, lineterminator="\n"), csv.writer(sub, lineterminator="\n")
            for i in range(n):
                self.seq += 1
                final = now - 100 * (n - 1 - i)
                w.writerow([f"c{self.seq}", self.seq, final - latency_ms, final, latency_ms, "ok", status, "", "", now, 0, 0, 1, "m"])
                s.writerow([self.seq, final - latency_ms - 50, 1])

    def ledger(self, count: int, *, number: int | None = None) -> None:
        number = self.source.current_height if number is None else number
        self.source.commit(valid_block(number, count, prefix=f"b{number}-"))

    def step(self, seconds: float = 1.0):
        self.clock.advance(seconds)
        return self.collector.poll()

    def frame(self, index: int):
        return self.provider.collect(run_id="r", decision_index=index, observation_timestamp_utc=iso(self.clock.now))


def build_runner(limit: int, *, max_initialization_frames: int | None = None):
    controllers = {
        "static": StaticController(CONFIG),
        "reactive": ReactiveController(CONFIG, congested_config=replace(CONFIG, max_message_count=40), latency_high_ms=1e9,
                                       latency_low_ms=1.0, consecutive_samples=2, cooldown_samples=0),
        "predictive": PredictiveController(FakeModel(), CONFIG, arrival_forecaster=FakeForecaster(),
                                           candidate_configs=[CONFIG, replace(CONFIG, max_message_count=40)]),
    }
    contract = Stage3FrameQualityContract(max_consecutive_abstentions=limit, max_initialization_frames=max_initialization_frames)
    return IntegratedControllerRunner(controllers, CONFIG, run_id="r", invalid_frame_policy="abstain", frame_quality=contract)
