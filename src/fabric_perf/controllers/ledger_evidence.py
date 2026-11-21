"""Continuous committed-ledger evidence (task A0d, blocker B-ledger).

Why this exists. ``e1_block_fetch.py`` writes ``ledger_tx.csv`` once, after a run. Nothing appended committed-ledger
evidence while a run was in progress, and ``ContinuousCaliperLedgerAdapter`` timestamped a ledger row with the moment
the *adapter* happened to read it. A committed-throughput value built that way depends on the adapter's own polling
rhythm, and it can never look stale. This module is the missing producer, plus the rule that turns its records into a
throughput value.

What is measured (the definition, stated once)
    A transaction is *committed* application evidence when it is an ENDORSER_TRANSACTION envelope (header type 3) in a
    block that the peer ledger holds, and its validation code is 0 (VALID). That is the project's existing definition
    (``e1_benchmark_analyze.py``: header type 3 and validation code 0; ``telemetry.py``: "VALID transactions/s").
    Envelopes with any other validation code are counted as INVALID, never as committed. Envelopes of other header
    types (configuration, for example) are counted separately as ``other`` and are not application throughput.
    Not counted, ever: submitted transactions, client acknowledgements, endorsements, orderer submissions, predictions.

How a rate is obtained
    The collector records OBSERVATIONS. An observation is the moment the channel height was read (``observed_utc``)
    plus the height. Two consecutive complete observations bound an interval; the blocks whose numbers lie in
    ``[previous height, this height)`` were committed inside it. The rate of an interval is
    ``VALID endorser transactions in those blocks / (observed_utc - previous observed_utc)``. The interval is a fully
    observed, positive span; the rate has block granularity (a block is attributed to the interval in which it was
    first seen, not to its own commit instant, which the peer ledger does not give; the channel-header timestamp is
    kept as ``ch_ts_ms`` and is not a commit time).

What is never done
    * The first observation of a journal is a BASELINE. It has no interval, so no rate. It is not zero.
    * A failed poll, an incomplete block fetch or a non-positive interval produces a row with no counts. The next
      complete observation then spans the gap, so nothing is lost and nothing is invented.
    * A genuine zero is a complete observed interval in which no VALID transaction was committed.
    * After a restart, the first complete observation is a RESUME row: its blocks are recorded and counted, but it
      carries no rate (``rate_valid`` is 0), because the interval includes the time the collector was down.
    * An integrity violation (height going backwards, a block that decodes to another number, a foreign channel, a
      broken hash chain, a validation code that is missing, a repeated committed transaction id) writes a HALT row and
      stops the collector for good. A halted journal is refused on restart.

Files (all append-only, one directory per collector run, created exclusively)
    ``ledger_tx.csv``            one row per ENDORSER transaction, in the ``e1_block_fetch`` schema plus
                                 ``obs_seq`` and ``observed_utc``; read by ``ContinuousCaliperLedgerAdapter``.
    ``ledger_blocks.csv``        one row per block (empty and configuration blocks included) with its counts and hashes.
    ``ledger_observations.csv``  one row per poll; the journal. It is written LAST in each poll, so it is the commit
                                 marker: rows of ``ledger_tx.csv``/``ledger_blocks.csv`` without a later observation are
                                 recognised on restart and reused, never counted twice.
    ``ledger_collector_meta.json``  schema, channel, definition (written once).

Offline by construction. The collector reads a ``LedgerSource`` (``height()`` and ``block(n)``). Tests use synthetic
sources. The class that reads a real peer lives in ``scripts/e1/e1_ledger_follow.py`` and is not run by any test that
needs a network. Nothing here imports a network client, Docker, Caliper or a model.
"""

from __future__ import annotations

import csv
import io
import json
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol, Sequence

from .channel_selection import select_experiment_channel

SCHEMA_VERSION = "fabric_ledger_evidence/1"
ENDORSER_HEADER_TYPE = 3
VALID_CODE = 0
DEFINITION = ("committed throughput = ENDORSER_TRANSACTION envelopes (header type 3) with validation code 0 (VALID) in "
              "blocks first observed inside an interval bounded by two consecutive complete channel-height "
              "observations, divided by the interval's elapsed observation time; invalid-status envelopes are counted "
              "separately and are never committed throughput")

TX_COLUMNS = ["tx_id", "block", "index", "header_type", "validation_code", "env_bytes", "channel_id", "ch_ts_ms",
              "obs_seq", "observed_utc"]
BLOCK_COLUMNS = ["block", "tx_total", "endorser_tx", "valid_tx", "invalid_tx", "other_tx", "channel_id",
                 "header_hash_hex", "previous_hash_hex", "data_hash_ok", "obs_seq", "observed_utc"]
OBS_COLUMNS = ["obs_seq", "kind", "status", "rate_valid", "channel_id", "collector_run_id", "observed_utc",
               "poll_start_utc", "height", "prev_seq", "prev_height", "prev_observed_utc", "interval_s",
               "blocks_in_interval", "first_block", "last_block", "tx_total", "endorser_tx", "valid_tx",
               "invalid_tx", "other_tx", "detail"]
FILES = {"tx": "ledger_tx.csv", "blocks": "ledger_blocks.csv", "observations": "ledger_observations.csv",
         "meta": "ledger_collector_meta.json"}

# Observation kinds and statuses.
KIND_BASELINE, KIND_INTERVAL, KIND_RESUME, KIND_FAILURE = "baseline", "interval", "resume", "failure"
KINDS = (KIND_BASELINE, KIND_INTERVAL, KIND_RESUME, KIND_FAILURE)
STATUS_OK = "ok"
FAILURE_STATUSES = ("read_failed", "fetch_incomplete", "nonpositive_interval")        # transient: the next poll may succeed
HALT_STATUSES = ("inconsistent", "height_rollback")                                    # integrity: the journal stops
STATUSES = (STATUS_OK,) + FAILURE_STATUSES + HALT_STATUSES

# States a consumer can see (``windowed_committed_rate``). The first group is machine-readable and stable.
STATE_NO_OBSERVATIONS = "no_observations"
STATE_INITIALIZING = "initializing"              # only the baseline exists: the first interval is still pending
STATE_RESUMED = "resumed_no_interval"            # a restart's first observation: no rate yet
STATE_MEASURING = "measuring"
STATE_FAILED = "latest_observation_failed"
STATE_HALTED = "integrity_halted"
STATE_NO_INTERVAL_IN_WINDOW = "no_interval_in_window"
STATE_INCONSISTENT = "journal_inconsistent"


class LedgerEvidenceError(RuntimeError):
    """The evidence cannot be trusted or the collector cannot proceed. Never swallowed."""


class LedgerReadError(RuntimeError):
    """A source could not deliver the height or a block right now (transient; the poll records a failure)."""


# --------------------------------------------------------------------------------------- time helpers
def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def parse_time(value: Any, *, field_name: str = "timestamp") -> datetime:
    """A timezone-aware ISO-8601 time. Anything else is refused, never guessed."""
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except (TypeError, ValueError, AttributeError) as exc:
            raise LedgerEvidenceError(f"invalid {field_name} {value!r}: a timezone-aware ISO timestamp is required") from exc
    if parsed.tzinfo is None:
        raise LedgerEvidenceError(f"invalid {field_name} {value!r}: a timezone is required")
    return parsed.astimezone(timezone.utc)


def _now() -> datetime:
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------------------- source interface
@dataclass(frozen=True)
class LedgerTx:
    tx_id: str
    index: int
    header_type: int
    validation_code: int | None
    env_bytes: int = 0
    channel_id: str = ""
    ch_ts_ms: int | None = None


@dataclass(frozen=True)
class LedgerBlock:
    number: int
    txs: tuple[LedgerTx, ...]
    header_hash_hex: str | None = None
    previous_hash_hex: str | None = None
    data_hash_ok: bool | None = None
    flags_len: int | None = None


class LedgerSource(Protocol):
    def height(self) -> int: ...
    def block(self, number: int) -> LedgerBlock: ...


Clock = Callable[[], datetime]


# --------------------------------------------------------------------------------------- journal rows
@dataclass(frozen=True)
class ObservationRow:
    seq: int
    kind: str
    status: str
    rate_valid: bool
    channel_id: str
    collector_run_id: str
    observed: datetime
    poll_start: datetime | None
    height: int | None
    prev_seq: int | None
    prev_height: int | None
    prev_observed: datetime | None
    interval_s: float | None
    blocks: int | None
    first_block: int | None
    last_block: int | None
    tx_total: int | None
    endorser: int | None
    valid: int | None
    invalid: int | None
    other: int | None
    detail: str

    @property
    def complete(self) -> bool:
        return self.status == STATUS_OK


def _opt_int(row: Mapping[str, Any], name: str, *, minimum: int = 0) -> int | None:
    text = row.get(name)
    if text is None or str(text).strip() == "":
        return None
    try:
        number = int(str(text).strip())
    except ValueError as exc:
        raise LedgerEvidenceError(f"observation column {name} is not an integer: {text!r}") from exc
    if number < minimum:
        raise LedgerEvidenceError(f"observation column {name} is below {minimum}: {number}")
    return number


def parse_observation_row(row: Mapping[str, Any]) -> ObservationRow:
    """Strictly parse one journal row. Any violation raises ``LedgerEvidenceError``; nothing is repaired."""
    missing = [name for name in OBS_COLUMNS if name not in row]
    if missing:
        raise LedgerEvidenceError(f"observation row lacks columns {missing}")
    seq = _opt_int(row, "obs_seq", minimum=1)
    if seq is None:
        raise LedgerEvidenceError("observation row has no obs_seq")
    kind, status = str(row["kind"]), str(row["status"])
    if kind not in KINDS or status not in STATUSES:
        raise LedgerEvidenceError(f"observation {seq}: unknown kind {kind!r} or status {status!r}")
    if (kind == KIND_FAILURE) != (status != STATUS_OK):
        raise LedgerEvidenceError(f"observation {seq}: kind {kind!r} does not match status {status!r}")
    rate_valid_text = str(row["rate_valid"]).strip()
    if rate_valid_text not in {"0", "1"}:
        raise LedgerEvidenceError(f"observation {seq}: rate_valid must be 0 or 1")
    observed = parse_time(row["observed_utc"], field_name=f"observation {seq} observed_utc")
    poll_start = (parse_time(row["poll_start_utc"], field_name=f"observation {seq} poll_start_utc")
                  if str(row["poll_start_utc"]).strip() else None)
    prev_observed = (parse_time(row["prev_observed_utc"], field_name=f"observation {seq} prev_observed_utc")
                     if str(row["prev_observed_utc"]).strip() else None)
    interval_text = str(row["interval_s"]).strip()
    try:
        interval_s = float(interval_text) if interval_text else None
    except ValueError as exc:
        raise LedgerEvidenceError(f"observation {seq}: interval_s is not numeric") from exc
    if interval_s is not None and (not math.isfinite(interval_s) or interval_s <= 0):
        raise LedgerEvidenceError(f"observation {seq}: interval_s must be a positive finite number, got {interval_s}")
    parsed = ObservationRow(
        seq=seq, kind=kind, status=status, rate_valid=rate_valid_text == "1", channel_id=str(row["channel_id"]),
        collector_run_id=str(row["collector_run_id"]), observed=observed, poll_start=poll_start,
        height=_opt_int(row, "height"), prev_seq=_opt_int(row, "prev_seq", minimum=1),
        prev_height=_opt_int(row, "prev_height"), prev_observed=prev_observed, interval_s=interval_s,
        blocks=_opt_int(row, "blocks_in_interval"), first_block=_opt_int(row, "first_block"),
        last_block=_opt_int(row, "last_block"), tx_total=_opt_int(row, "tx_total"), endorser=_opt_int(row, "endorser_tx"),
        valid=_opt_int(row, "valid_tx"), invalid=_opt_int(row, "invalid_tx"), other=_opt_int(row, "other_tx"),
        detail=str(row["detail"]))
    if parsed.rate_valid != (kind == KIND_INTERVAL and status == STATUS_OK):
        raise LedgerEvidenceError(f"observation {seq}: rate_valid disagrees with kind {kind!r} / status {status!r}")
    if kind == KIND_BASELINE and parsed.height is None:
        raise LedgerEvidenceError(f"observation {seq}: a baseline needs a height")
    if kind in (KIND_INTERVAL, KIND_RESUME):
        needed = (parsed.height, parsed.prev_seq, parsed.prev_height, parsed.prev_observed, parsed.interval_s,
                  parsed.blocks, parsed.tx_total, parsed.endorser, parsed.valid, parsed.invalid, parsed.other)
        if any(value is None for value in needed):
            raise LedgerEvidenceError(f"observation {seq}: an interval row lacks height, previous observation or counts")
        if parsed.prev_height > parsed.height or parsed.blocks != parsed.height - parsed.prev_height:
            raise LedgerEvidenceError(f"observation {seq}: blocks_in_interval disagrees with the heights")
        if parsed.tx_total != parsed.endorser + parsed.other or parsed.endorser != parsed.valid + parsed.invalid:
            raise LedgerEvidenceError(f"observation {seq}: transaction counts do not add up")
        if parsed.prev_observed >= parsed.observed:
            raise LedgerEvidenceError(f"observation {seq}: the interval is not positive")
        if abs((parsed.observed - parsed.prev_observed).total_seconds() - parsed.interval_s) > 0.002:
            raise LedgerEvidenceError(f"observation {seq}: interval_s disagrees with the timestamps")
    if kind == KIND_FAILURE and any(value is not None for value in (parsed.valid, parsed.invalid, parsed.endorser)):
        raise LedgerEvidenceError(f"observation {seq}: a failed poll must carry no counts")
    return parsed


# --------------------------------------------------------------------------------------- windowed rate
@dataclass(frozen=True)
class LedgerRate:
    """The committed-throughput reading for one window, or the reason there is none. ``value`` is None unless MEASURING."""

    state: str
    value: float | None
    reason: str | None
    window_s: float
    interval_start: datetime | None = None
    interval_end: datetime | None = None
    interval_s: float | None = None
    valid: int | None = None
    invalid: int | None = None
    observations_used: int = 0
    window_covered: bool | None = None
    latest_seq: int | None = None
    latest_status: str | None = None
    latest_observed: datetime | None = None
    baseline_height: int | None = None

    def as_health(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION, "definition": DEFINITION, "state": self.state, "value_tps": self.value,
            "reason": self.reason, "window_s": self.window_s,
            "interval_start_utc": None if self.interval_start is None else _iso(self.interval_start),
            "interval_end_utc": None if self.interval_end is None else _iso(self.interval_end),
            "interval_s": self.interval_s, "valid_in_interval": self.valid, "invalid_in_interval": self.invalid,
            "observations_used": self.observations_used, "window_covered": self.window_covered,
            "latest_observation_seq": self.latest_seq, "latest_observation_status": self.latest_status,
            "latest_observation_utc": None if self.latest_observed is None else _iso(self.latest_observed),
            "baseline_height": self.baseline_height,
            "time_basis": "ledger observation time (when the collector read the channel height); not commit time, "
                          "not the time any consumer read the file",
        }


def windowed_committed_rate(records: Sequence[ObservationRow], *, now: datetime, window_s: float) -> LedgerRate:
    """Committed (VALID endorser) transactions per second over the observed intervals inside ``now - window_s``.

    Whole observed intervals only: the window starts at the beginning of the first interval that ends inside it, so a
    block is never split. The newest observation of any status decides health: if it is a failure the value is
    missing (not the previous value served as current, and never zero).
    """
    if isinstance(window_s, bool) or not isinstance(window_s, (int, float)) or not math.isfinite(window_s) or window_s <= 0:
        raise ValueError("window_s must be a positive number")
    window_start = now.timestamp() - window_s
    if not records:
        return LedgerRate(STATE_NO_OBSERVATIONS, None, "no_observations: the ledger observation journal has no rows yet",
                          window_s)
    ordered = sorted(records, key=lambda row: row.seq)
    latest = ordered[-1]
    base = dict(window_s=window_s, latest_seq=latest.seq, latest_status=latest.status, latest_observed=latest.observed)
    halted = next((row for row in ordered if row.status in HALT_STATUSES), None)
    if halted is not None:
        return LedgerRate(STATE_HALTED, None, f"integrity_halted: observation {halted.seq} {halted.status}: {halted.detail}", **base)
    complete = [row for row in ordered if row.complete]
    # Continuity of the journal itself: an interval row must start where the previous complete observation ended.
    for index, row in enumerate(complete):
        if row.kind in (KIND_INTERVAL, KIND_RESUME):
            previous = complete[index - 1] if index else None
            if (previous is None or row.prev_seq != previous.seq or row.prev_height != previous.height
                    or row.prev_observed != previous.observed):
                return LedgerRate(STATE_INCONSISTENT, None,
                                  f"journal_inconsistent: observation {row.seq} does not continue the previous complete observation",
                                  **base)
        if index and row.observed <= complete[index - 1].observed:
            return LedgerRate(STATE_INCONSISTENT, None,
                              f"journal_inconsistent: observation {row.seq} is not later than the previous one", **base)
    if latest.status in FAILURE_STATUSES:
        return LedgerRate(STATE_FAILED, None, f"latest_observation_failed: observation {latest.seq} {latest.status}: {latest.detail}",
                          baseline_height=_baseline(complete), **base)
    newest = complete[-1]
    used: list[ObservationRow] = []
    for row in reversed(complete):
        if row.kind != KIND_INTERVAL or row.observed.timestamp() <= window_start:
            break
        used.append(row)
    used.reverse()
    if not used:
        if newest.kind == KIND_BASELINE:
            return LedgerRate(STATE_INITIALIZING, None,
                              "initializing: only the baseline ledger observation exists; the first interval needs a second observation",
                              baseline_height=newest.height, **base)
        if newest.kind == KIND_RESUME:
            return LedgerRate(STATE_RESUMED, None,
                              "resumed_no_interval: the first observation after a collector restart carries no rate; the next interval will",
                              baseline_height=_baseline(complete), **base)
        return LedgerRate(STATE_NO_INTERVAL_IN_WINDOW, None,
                          f"no_interval_in_window: the newest complete ledger interval ended {now.timestamp() - newest.observed.timestamp():.3f}s "
                          f"before this observation, outside the {window_s}s window",
                          baseline_height=_baseline(complete), **base)
    start, end = used[0].prev_observed, used[-1].observed
    span = (end - start).total_seconds()
    if span <= 0:
        return LedgerRate(STATE_INCONSISTENT, None, "journal_inconsistent: the measurement interval is not positive", **base)
    valid = sum(row.valid for row in used)
    invalid = sum(row.invalid for row in used)
    return LedgerRate(STATE_MEASURING, valid / span, None, window_s, interval_start=start, interval_end=end, interval_s=span,
                      valid=valid, invalid=invalid, observations_used=len(used), window_covered=start.timestamp() <= window_start,
                      latest_seq=latest.seq, latest_status=latest.status, latest_observed=latest.observed,
                      baseline_height=_baseline(complete))


def _baseline(complete: Sequence[ObservationRow]) -> int | None:
    return next((row.height for row in complete if row.kind == KIND_BASELINE), None)


# --------------------------------------------------------------------------------------- csv + file helpers
def _csv_line(columns: Sequence[str], row: Mapping[str, Any]) -> str:
    buffer = io.StringIO()
    csv.DictWriter(buffer, fieldnames=list(columns), lineterminator="\n", extrasaction="raise").writerow(
        {name: ("" if row.get(name) is None else row[name]) for name in columns})
    return buffer.getvalue()


def _append_durably(path: Path, text: str) -> None:
    """Append whole lines and force them to disk, so a crash leaves either the row or none of it (plus, at worst, a torn tail)."""
    data = text.encode("utf-8")
    with path.open("ab") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _create_exclusive(path: Path, text: str) -> None:
    with path.open("xb") as handle:
        handle.write(text.encode("utf-8"))
        handle.flush()
        os.fsync(handle.fileno())


def _trim_torn_tail(path: Path) -> int:
    """Remove bytes after the last newline (an interrupted write). They are preserved beside the file, never dropped silently."""
    raw = path.read_bytes()
    cut = raw.rfind(b"\n") + 1
    if cut == len(raw):
        return 0
    sidecar = path.with_name(f"{path.name}.torn_tail")
    index = 0
    while sidecar.exists():
        index += 1
        sidecar = path.with_name(f"{path.name}.torn_tail.{index}")
    _create_exclusive(sidecar, "")
    with sidecar.open("ab") as handle:
        handle.write(raw[cut:])
    with path.open("r+b") as handle:
        handle.truncate(cut)
        handle.flush()
        os.fsync(handle.fileno())
    return len(raw) - cut


def _read_csv(path: Path, columns: Sequence[str]) -> list[dict[str, str]]:
    text = path.read_text(encoding="utf-8")
    if not text:
        raise LedgerEvidenceError(f"{path.name} is empty: its header is missing")
    reader = csv.DictReader(io.StringIO(text))
    if list(reader.fieldnames or ()) != list(columns):
        raise LedgerEvidenceError(f"{path.name} has an unexpected header {reader.fieldnames!r}")
    rows = []
    for line_number, row in enumerate(reader, 2):
        if None in row or any(value is None for value in row.values()):
            raise LedgerEvidenceError(f"{path.name} line {line_number} is malformed (wrong number of fields)")
        rows.append(row)
    return rows


# --------------------------------------------------------------------------------------- the collector
class LedgerCollector:
    """Poll a ledger source, append committed-ledger evidence, and keep a journal that can be audited and resumed.

    ``poll()`` never talks to anything but its injected ``source`` and ``clock``. ``follow()`` is bounded: the caller
    states the number of polls and the pause, so no collector runs unbounded by default.
    """

    def __init__(self, source: LedgerSource, *, channel_id: str, out_dir: Path, collector_run_id: str,
                 clock: Clock | None = None, resume: bool = False):
        self.channel_id = select_experiment_channel(channel_id, purpose="continuous ledger collection", option="--channel")
        if not isinstance(collector_run_id, str) or not collector_run_id.strip():
            raise ValueError("collector_run_id must be a non-empty string")
        self.source = source
        self.collector_run_id = collector_run_id
        self.out_dir = Path(out_dir)
        self._clock: Clock = clock or _now
        self._seq = 0
        self._last_complete: ObservationRow | None = None
        self._halted: ObservationRow | None = None
        self._resume_pending = False
        self._blocks: dict[int, dict[str, Any]] = {}
        self._tx_keys: dict[tuple[int, int], str] = {}
        self._endorser_ids: dict[str, int | None] = {}
        self.recovery: dict[str, Any] = {"resumed": False}
        self.paths = {key: self.out_dir / name for key, name in FILES.items()}
        if resume:
            self._recover()
        else:
            self._create()

    # ---- creation and recovery
    def _create(self) -> None:
        if self.out_dir.exists() and any(self.out_dir.iterdir()):
            raise LedgerEvidenceError(f"refusing to start a new ledger journal in the non-empty directory {self.out_dir}; "
                                      "use resume=True to continue one")
        self.out_dir.mkdir(parents=True, exist_ok=True)
        created = self._now()
        _create_exclusive(self.paths["tx"], _csv_line(TX_COLUMNS, {name: name for name in TX_COLUMNS}))
        _create_exclusive(self.paths["blocks"], _csv_line(BLOCK_COLUMNS, {name: name for name in BLOCK_COLUMNS}))
        _create_exclusive(self.paths["observations"], _csv_line(OBS_COLUMNS, {name: name for name in OBS_COLUMNS}))
        _create_exclusive(self.paths["meta"], json.dumps({
            "schema_version": SCHEMA_VERSION, "channel_id": self.channel_id, "collector_run_id": self.collector_run_id,
            "created_utc": _iso(created), "definition": DEFINITION, "tx_columns": TX_COLUMNS,
            "block_columns": BLOCK_COLUMNS, "observation_columns": OBS_COLUMNS,
            "counting_starts_at": "the height read by the baseline observation; earlier blocks are not counted",
        }, indent=2, sort_keys=True) + "\n")

    def _recover(self) -> None:
        for key in ("tx", "blocks", "observations", "meta"):
            if not self.paths[key].is_file():
                raise LedgerEvidenceError(f"cannot resume: {self.paths[key]} is missing")
        meta = json.loads(self.paths["meta"].read_text(encoding="utf-8"))
        if meta.get("channel_id") != self.channel_id:
            raise LedgerEvidenceError(f"cannot resume: the journal is for channel {meta.get('channel_id')!r}, not {self.channel_id!r}")
        if meta.get("schema_version") != SCHEMA_VERSION:
            raise LedgerEvidenceError(f"cannot resume: unknown journal schema {meta.get('schema_version')!r}")
        torn = {key: _trim_torn_tail(self.paths[key]) for key in ("tx", "blocks", "observations")}
        observations = [parse_observation_row(row) for row in _read_csv(self.paths["observations"], OBS_COLUMNS)]
        blocks = _read_csv(self.paths["blocks"], BLOCK_COLUMNS)
        txs = _read_csv(self.paths["tx"], TX_COLUMNS)
        for index, row in enumerate(observations):
            if row.seq != index + 1:
                raise LedgerEvidenceError(f"cannot resume: observation sequence breaks at row {index + 1} (found {row.seq})")
            if row.channel_id != self.channel_id:
                raise LedgerEvidenceError(f"cannot resume: observation {row.seq} is for channel {row.channel_id!r}")
        for row in observations:
            if row.status in HALT_STATUSES:
                raise LedgerEvidenceError(f"cannot resume: the journal was halted by observation {row.seq} "
                                          f"({row.status}: {row.detail}); start a new journal")
        complete = [row for row in observations if row.complete]
        previous = None
        for row in complete:
            if previous is not None:
                if row.observed <= previous.observed:
                    raise LedgerEvidenceError(f"cannot resume: observation {row.seq} is not later than observation {previous.seq}")
                if row.height < previous.height:
                    raise LedgerEvidenceError(f"cannot resume: the height decreases at observation {row.seq}")
                if row.kind in (KIND_INTERVAL, KIND_RESUME) and (row.prev_seq != previous.seq or row.prev_height != previous.height):
                    raise LedgerEvidenceError(f"cannot resume: observation {row.seq} does not continue observation {previous.seq}")
            elif row.kind != KIND_BASELINE:
                raise LedgerEvidenceError("cannot resume: the first complete observation is not a baseline")
            previous = row
        # Block records must be consecutive from the baseline height.
        by_number: dict[int, dict[str, Any]] = {}
        expected = complete[0].height if complete else None
        for row in blocks:
            number = int(row["block"])
            if expected is None or number != expected:
                raise LedgerEvidenceError(f"cannot resume: block records are not consecutive (found block {number}, expected {expected})")
            by_number[number] = {"tx_total": int(row["tx_total"]), "endorser": int(row["endorser_tx"]),
                                 "valid": int(row["valid_tx"]), "invalid": int(row["invalid_tx"]),
                                 "other": int(row["other_tx"]), "header_hash_hex": row["header_hash_hex"] or None}
            expected = number + 1
        if complete:
            last = complete[-1]
            if by_number and max(by_number) + 1 < last.height:
                raise LedgerEvidenceError("cannot resume: the last complete observation claims blocks the block file does not hold")
            for row in complete:
                if row.kind in (KIND_INTERVAL, KIND_RESUME):
                    numbers = range(row.prev_height, row.height)
                    if any(n not in by_number for n in numbers):
                        raise LedgerEvidenceError(f"cannot resume: observation {row.seq} counts blocks missing from the block file")
                    counted = [by_number[n] for n in numbers]
                    if (sum(b["valid"] for b in counted), sum(b["invalid"] for b in counted),
                            sum(b["other"] for b in counted)) != (row.valid, row.invalid, row.other):
                        raise LedgerEvidenceError(f"cannot resume: observation {row.seq} disagrees with its block records")
        tx_keys: dict[tuple[int, int], str] = {}
        endorser_ids: dict[str, int | None] = {}
        for row in txs:
            key = (int(row["block"]), int(row["index"]))
            if key in tx_keys:
                raise LedgerEvidenceError(f"cannot resume: transaction position {key} is recorded twice")
            tx_keys[key] = row["tx_id"]
            code = int(row["validation_code"]) if row["validation_code"] != "" else None
            if row["tx_id"] in endorser_ids and code == VALID_CODE and endorser_ids[row["tx_id"]] == VALID_CODE:
                raise LedgerEvidenceError(f"cannot resume: committed transaction id {row['tx_id']} is recorded twice")
            endorser_ids.setdefault(row["tx_id"], code)
        baseline_height = complete[0].height if complete else None
        for (block, _), _id in tx_keys.items():
            # Blocks beyond the last recorded one are the remains of an interrupted poll (the next poll re-checks them against
            # the ledger and reuses them); anything below the baseline can only be foreign content.
            if baseline_height is None or block < baseline_height:
                raise LedgerEvidenceError(f"cannot resume: transaction rows exist for block {block}, before the journal's baseline")
        self._seq = len(observations)
        self._last_complete = complete[-1] if complete else None
        self._blocks, self._tx_keys, self._endorser_ids = by_number, tx_keys, endorser_ids
        self._resume_pending = self._last_complete is not None
        orphan_blocks = sorted(n for n in by_number if self._last_complete is not None and n >= self._last_complete.height)
        orphan_tx = sorted({b for (b, _) in tx_keys if b not in by_number})
        self.recovery = {"resumed": True, "observations": len(observations), "torn_tail_bytes_removed": torn,
                         "orphan_blocks_reused": orphan_blocks, "orphan_tx_blocks": orphan_tx,
                         "next_observation_is": KIND_RESUME if self._resume_pending else "baseline"}

    # ---- polling
    def _now(self) -> datetime:
        value = self._clock()
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise LedgerEvidenceError(f"the collector clock returned {value!r}; a timezone-aware datetime is required")
        value = value.astimezone(timezone.utc)
        # Journal timestamps carry milliseconds, so readings are truncated here: an interval computed from the
        # in-memory value is then exactly the interval a reader recomputes from the journal.
        return value.replace(microsecond=value.microsecond // 1000 * 1000)

    @property
    def halted(self) -> bool:
        return self._halted is not None

    def poll(self) -> ObservationRow:
        """One observation. Returns the journal row it wrote. Raises only for an unusable clock or a halted collector."""
        if self._halted is not None:
            raise LedgerEvidenceError(f"the collector is halted by observation {self._halted.seq} "
                                      f"({self._halted.status}: {self._halted.detail})")
        seq = self._seq + 1
        start = self._now()
        try:
            height = self.source.height()
        except LedgerReadError as exc:
            return self._write_failure(seq, start, self._now(), "read_failed", f"height unreadable: {exc}", None)
        observed = self._now()
        if isinstance(height, bool) or not isinstance(height, int) or height < 0:
            return self._write_failure(seq, start, observed, "read_failed", f"the source returned an invalid height {height!r}", None)
        last = self._last_complete
        if last is None:
            return self._write_baseline(seq, start, observed, height)
        if height < last.height:
            return self._write_failure(seq, start, observed, "height_rollback",
                                       f"height {height} is below the last complete observation's {last.height}", height)
        if observed <= last.observed:
            return self._write_failure(seq, start, observed, "nonpositive_interval",
                                       f"observation time {_iso(observed)} is not after the previous {_iso(last.observed)}", height)
        fetched: dict[int, LedgerBlock] = {}
        pending_valid_ids: set[str] = set()
        tip = self._tip_for(last.height - 1)
        for number in range(last.height, height):
            if number in self._blocks:
                tip = self._blocks[number]["header_hash_hex"]
                continue
            try:
                block = self.source.block(number)
            except LedgerReadError as exc:
                return self._write_failure(seq, start, observed, "fetch_incomplete", f"block {number} unreadable: {exc}", height)
            problem = self._check_block(block, number, tip, pending_valid_ids)
            if problem is not None:
                return self._write_failure(seq, start, observed, "inconsistent", f"block {number}: {problem}", height)
            fetched[number] = block
            tip = block.header_hash_hex or tip
        return self._write_interval(seq, start, observed, height, last, fetched)

    def _tip_for(self, number: int) -> str | None:
        entry = self._blocks.get(number)
        return None if entry is None else entry["header_hash_hex"]

    def _check_block(self, block: LedgerBlock, number: int, tip: str | None, pending_valid_ids: set[str]) -> str | None:
        if block.number != number:
            return f"decodes to block {block.number} (number_mismatch)"
        if block.data_hash_ok is False:
            return "the data hash does not match the envelopes (data_hash_mismatch)"
        if block.flags_len is not None and block.flags_len != len(block.txs):
            return f"validation-flag length {block.flags_len} differs from the {len(block.txs)} envelopes (flags_length_mismatch)"
        if tip is not None and block.previous_hash_hex is not None and block.previous_hash_hex != tip:
            return "the previous-block hash does not match the previous block (chain_break)"
        seen: set[int] = set()
        for tx in block.txs:
            if tx.channel_id and tx.channel_id != self.channel_id:
                return f"envelope of channel {tx.channel_id!r} on channel {self.channel_id!r} (channel_mismatch)"
            if tx.index in seen:
                return f"envelope index {tx.index} repeats (duplicate_position)"
            seen.add(tx.index)
            if tx.header_type == ENDORSER_HEADER_TYPE:
                if tx.validation_code is None:
                    return f"endorser envelope {tx.index} has no validation code (validation_code_missing)"
                if not tx.tx_id:
                    return f"endorser envelope {tx.index} has no transaction id (tx_id_missing)"
                if tx.validation_code == VALID_CODE:
                    already_here = self._tx_keys.get((number, tx.index)) == tx.tx_id          # an orphan row of this very position
                    if (self._endorser_ids.get(tx.tx_id) == VALID_CODE and not already_here) or tx.tx_id in pending_valid_ids:
                        return f"committed transaction id {tx.tx_id} appears twice (duplicate_committed_tx)"
                    pending_valid_ids.add(tx.tx_id)
        return None

    @staticmethod
    def _counts(block: LedgerBlock) -> dict[str, int]:
        endorser = [tx for tx in block.txs if tx.header_type == ENDORSER_HEADER_TYPE]
        valid = sum(1 for tx in endorser if tx.validation_code == VALID_CODE)
        return {"tx_total": len(block.txs), "endorser": len(endorser), "valid": valid,
                "invalid": len(endorser) - valid, "other": len(block.txs) - len(endorser)}

    # ---- row writers (the observation row is always written last)
    def _commit_observation(self, row: Mapping[str, Any]) -> ObservationRow:
        # Validate first, append second: a row the strict reader would refuse is never written.
        parsed = parse_observation_row({name: ("" if row.get(name) is None else str(row[name])) for name in OBS_COLUMNS})
        _append_durably(self.paths["observations"], _csv_line(OBS_COLUMNS, row))
        self._seq = parsed.seq
        if parsed.complete:
            self._last_complete = parsed
        if parsed.status in HALT_STATUSES:
            self._halted = parsed
        return parsed

    def _base_row(self, seq: int, start: datetime, observed: datetime, kind: str, status: str) -> dict[str, Any]:
        return {"obs_seq": seq, "kind": kind, "status": status, "rate_valid": 0, "channel_id": self.channel_id,
                "collector_run_id": self.collector_run_id, "observed_utc": _iso(observed), "poll_start_utc": _iso(start)}

    def _write_baseline(self, seq: int, start: datetime, observed: datetime, height: int) -> ObservationRow:
        row = self._base_row(seq, start, observed, KIND_BASELINE, STATUS_OK)
        row.update(height=height, detail="first observation of this journal: counting starts at this height; no interval, no rate")
        return self._commit_observation(row)

    def _write_failure(self, seq: int, start: datetime, observed: datetime, status: str, detail: str,
                       height: int | None) -> ObservationRow:
        row = self._base_row(seq, start, observed, KIND_FAILURE, status)
        row.update(height=height, detail=detail.replace("\n", " ")[:300])
        return self._commit_observation(row)

    def _write_interval(self, seq: int, start: datetime, observed: datetime, height: int, last: ObservationRow,
                        fetched: dict[int, LedgerBlock]) -> ObservationRow:
        stamp = _iso(observed)
        tx_text, block_text = "", ""
        for number in sorted(fetched):
            block = fetched[number]
            counts = self._counts(block)
            for tx in block.txs:
                if tx.header_type != ENDORSER_HEADER_TYPE:
                    continue
                key = (number, tx.index)
                if key in self._tx_keys:
                    if self._tx_keys[key] != tx.tx_id:
                        return self._write_failure(seq, start, observed, "inconsistent",
                                                   f"block {number} position {tx.index} was recorded as {self._tx_keys[key]!r}, now {tx.tx_id!r}",
                                                   height)
                    continue                                                     # an orphan from an interrupted poll: already on disk
                tx_text += _csv_line(TX_COLUMNS, {"tx_id": tx.tx_id, "block": number, "index": tx.index, "header_type": tx.header_type,
                                                  "validation_code": tx.validation_code, "env_bytes": tx.env_bytes,
                                                  "channel_id": tx.channel_id, "ch_ts_ms": tx.ch_ts_ms, "obs_seq": seq,
                                                  "observed_utc": stamp})
            channels = sorted({tx.channel_id for tx in block.txs if tx.channel_id})
            block_text += _csv_line(BLOCK_COLUMNS, {"block": number, **{"tx_total": counts["tx_total"], "endorser_tx": counts["endorser"],
                                                    "valid_tx": counts["valid"], "invalid_tx": counts["invalid"], "other_tx": counts["other"]},
                                                    "channel_id": ";".join(channels), "header_hash_hex": block.header_hash_hex,
                                                    "previous_hash_hex": block.previous_hash_hex,
                                                    "data_hash_ok": "" if block.data_hash_ok is None else int(block.data_hash_ok),
                                                    "obs_seq": seq, "observed_utc": stamp})
        if tx_text:
            _append_durably(self.paths["tx"], tx_text)
        if block_text:
            _append_durably(self.paths["blocks"], block_text)
        for number in sorted(fetched):
            block, counts = fetched[number], self._counts(fetched[number])
            self._blocks[number] = {**{"tx_total": counts["tx_total"], "endorser": counts["endorser"], "valid": counts["valid"],
                                       "invalid": counts["invalid"], "other": counts["other"]}, "header_hash_hex": block.header_hash_hex}
            for tx in block.txs:
                if tx.header_type == ENDORSER_HEADER_TYPE:
                    self._tx_keys[(number, tx.index)] = tx.tx_id
                    self._endorser_ids.setdefault(tx.tx_id, tx.validation_code)
                    if tx.validation_code == VALID_CODE:
                        self._endorser_ids[tx.tx_id] = VALID_CODE
        numbers = range(last.height, height)
        counted = [self._blocks[n] for n in numbers]
        totals = {key: sum(b[key] for b in counted) for key in ("tx_total", "endorser", "valid", "invalid", "other")}
        kind = KIND_RESUME if self._resume_pending else KIND_INTERVAL
        row = self._base_row(seq, start, observed, kind, STATUS_OK)
        row.update(rate_valid=1 if kind == KIND_INTERVAL else 0, height=height, prev_seq=last.seq, prev_height=last.height,
                   prev_observed_utc=_iso(last.observed), interval_s=f"{(observed - last.observed).total_seconds():.6f}",
                   blocks_in_interval=len(numbers), first_block=numbers[0] if numbers else None,
                   last_block=numbers[-1] if numbers else None, tx_total=totals["tx_total"], endorser_tx=totals["endorser"],
                   valid_tx=totals["valid"], invalid_tx=totals["invalid"], other_tx=totals["other"],
                   detail="" if kind == KIND_INTERVAL else "first observation after a collector restart: the interval includes the downtime, so it carries no rate")
        parsed = self._commit_observation(row)
        self._resume_pending = False
        return parsed

    # ---- bounded loop
    def follow(self, *, max_polls: int, poll_interval_s: float, sleep: Callable[[float], None] = time.sleep) -> list[ObservationRow]:
        """Poll ``max_polls`` times, ``poll_interval_s`` apart. Both are required: a collector is never unbounded by default."""
        if isinstance(max_polls, bool) or not isinstance(max_polls, int) or max_polls < 1:
            raise ValueError("max_polls must be a positive integer")
        if isinstance(poll_interval_s, bool) or not isinstance(poll_interval_s, (int, float)) or not math.isfinite(poll_interval_s) or poll_interval_s <= 0:
            raise ValueError("poll_interval_s must be a positive number")
        rows: list[ObservationRow] = []
        for index in range(max_polls):
            if index:
                sleep(poll_interval_s)
            rows.append(self.poll())
            if self.halted:
                break
        return rows


def read_observation_journal(path: Path) -> list[ObservationRow]:
    """Strictly read a journal for audit. A torn last line is ignored here (it is never a row) and reported by recovery."""
    path = Path(path)
    raw = path.read_bytes()
    cut = raw.rfind(b"\n") + 1
    text = raw[:cut].decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    if list(reader.fieldnames or ()) != OBS_COLUMNS:
        raise LedgerEvidenceError(f"{path.name} has an unexpected header {reader.fieldnames!r}")
    return [parse_observation_row(row) for row in reader]
