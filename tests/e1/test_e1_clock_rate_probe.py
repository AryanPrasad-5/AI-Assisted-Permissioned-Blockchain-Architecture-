"""Clock-rate probe analysis (E0-0 remediation F1, 2026-11-09): synthetic clock samples, timesyncd NTP
messages and host reference reads. Not real clock data."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))

import e1_clock_rate_probe as cp  # noqa: E402

S = 1_000_000_000
R0 = 1_794_240_000 * S       # realtime origin


def synth(duration_s=100, period_s=0.05, tick=9788, steps=((30.0, 686_000_000), (63.0, 690_000_000)),
          ntp_for_steps=True, ntp_offset_error_ns=0, refs_every_s=10):
    """MONOTONIC_RAW is true time; MONOTONIC = RAW x tick/10000; REALTIME = MONOTONIC + steps."""
    rate = tick / 10_000
    samples, step_acc, pending = [], 0, list(steps)
    n = int(duration_s / period_s)
    for i in range(n + 1):
        raw = int(i * period_s * S)
        mono = int(raw * rate)
        while pending and raw >= pending[0][0] * S:
            step_acc += pending.pop(0)[1]
        samples.append({"i": i, "real_ns": R0 + mono + step_acc, "mono_ns": mono, "raw_ns": raw, "boot_ns": mono,
                        "adj_rc": 0, "tick_us": tick, "freq_scaled": 0, "status": 0x2000, "offset": 0, "constant": 1})
    ntp = []
    if ntp_for_steps:
        acc = 0
        for k, (t, size) in enumerate(steps):
            dest_us = (R0 + int(t * S * rate) + acc) // 1000 - 20_000                # 20 ms before the step
            off_us = (size + ntp_offset_error_ns) // 1000
            # offset = ((recv - orig) + (trans - dest)) / 2 with a symmetric 100 ms path
            orig = dest_us - 100_000
            ntp.append({"packet_count": k + 1, "origin_us": orig, "receive_us": orig + 50_000 + off_us,
                        "transmit_us": orig + 50_000 + off_us, "dest_us": dest_us})
            acc += size
    refs = []
    for k, t in enumerate(range(0, duration_s, refs_every_s)):
        s = samples[int(t / period_s)]
        row = {"k": k, "ref_qpc_ns": s["raw_ns"] + 7 * S, "ref_utc_ns": R0 + s["raw_ns"]}
        for c in cp.CLOCKS:
            row[f"{c}_before_ns"] = s[f"{c}_ns"] - 20_000_000
            row[f"{c}_after_ns"] = s[f"{c}_ns"] + 20_000_000
        refs.append(row)
    return samples, ntp, refs


def test_busctl_ntpmessage_parsing_real_format():
    text = ("(uuuuittayttttbtt) 0 4 4 2 -25 6515 411 4 29 88 99 4 1794241053525003 1794241054518515 "
            "1794241054518986 1794241053858609 false 128 31770")
    m = cp.parse_busctl_ntp(text)
    assert m["origin_us"] == 1794241053525003 and m["dest_us"] == 1794241053858609 and m["packet_count"] == 128
    assert cp.ntp_offset_us(m) == pytest.approx(826_944.0, abs=1)
    with pytest.raises(ValueError):
        cp.parse_busctl_ntp("Failed to get property")


def test_slow_tick_and_timesyncd_steps_are_measured_and_attributed():
    samples, ntp, refs = synth()
    r = cp.analyze(samples, ntp, refs)
    assert r["n_steps"] == 2 and r["n_attributed_to_timesyncd"] == 2 and r["n_unattributed"] == 0
    assert r["step_total_ms"] == pytest.approx(1376.0)
    assert r["tick_values"] == [9788]
    k = r["kernel_adjustment"]
    assert k["mono_vs_raw_ppm"] == pytest.approx(-21_200, abs=5)
    assert k["mono_over_raw_predicted_from_adjtimex"] == pytest.approx(0.9788, abs=1e-6)
    assert r["rates"]["mono_vs_host_qpc_ppm"] == pytest.approx(-21_200, abs=5)
    assert r["rates"]["raw_vs_host_qpc_ppm"] == pytest.approx(0, abs=5)
    assert r["rates"]["host_utc_vs_host_qpc_ppm"] == pytest.approx(0, abs=1)


def test_nominal_tick_without_steps_is_clean():
    samples, ntp, refs = synth(tick=10_000, steps=())
    r = cp.analyze(samples, ntp, refs)
    assert r["n_steps"] == 0 and abs(r["kernel_adjustment"]["mono_vs_raw_ppm"]) < 1
    assert abs(r["rates"]["mono_vs_host_qpc_ppm"]) < 1


def test_step_without_a_matching_exchange_is_unattributed():
    samples, _, refs = synth(ntp_for_steps=False)
    assert cp.analyze(samples, [], refs)["n_unattributed"] == 2
    samples, ntp, refs = synth(ntp_offset_error_ns=5_000_000)      # NTP offset 5 ms away from the step size
    assert cp.analyze(samples, ntp, refs, match_ms=1.0)["n_unattributed"] == 2


def test_window_rates_bound_short_interval_errors():
    w = cp.analyze(*synth(steps=()))["window_rates_3s"]
    assert w["windows"] == 33 and w["max_abs_ppm"] == pytest.approx(21_200, abs=5) and w["share_over_333ppm"] == 1.0
    w0 = cp.analyze(*synth(tick=10_000, steps=()))["window_rates_3s"]
    assert w0["max_abs_ppm"] < 1 and w0["share_over_333ppm"] == 0.0
    samples, ntp, refs = synth(tick=10_000, steps=())
    for s in samples[600:660]:                      # a 3 s slew at +8.33 % inside an otherwise nominal clock
        s["mono_ns"] += int((int(s["raw_ns"]) - int(samples[600]["raw_ns"])) * 0.0833)
    for s in samples[660:]:
        s["mono_ns"] += int(3 * S * 0.0833)
    w1 = cp.analyze(samples, ntp, refs)["window_rates_3s"]
    assert w1["max_abs_ppm"] > 50_000 and 0 < w1["share_over_333ppm"] < 0.1


def test_tick_change_is_recorded_in_the_history():
    samples, ntp, refs = synth(steps=())
    for s in samples[1000:]:
        s["tick_us"] = 10_000
    r = cp.analyze(samples, ntp, refs)
    assert r["tick_values"] == [9788, 10_000] and len(r["adjtimex_history"]) == 2
