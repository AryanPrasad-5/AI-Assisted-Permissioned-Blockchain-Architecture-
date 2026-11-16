"""Exploratory data analysis of the two Flashbots days (descriptive only; no model is fitted here).

Inputs : Flashbots/data/derived/tx_extract_<date>.npz, Flashbots/time_series/target_series_<date>_1s.csv.gz
Outputs: Flashbots/eda/*.csv|json (numbers behind every statement) and Flashbots/visualizations/eda_*.png

    .venv/Scripts/python.exe -B Flashbots/eda/run_eda.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import signal, stats

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fbcommon as fc  # noqa: E402

fc.plot_style()
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

OUT = fc.FB / "eda"
WINDOWS = (1, 5, 10, 12, 30, 60, 300, 900, 3600)
TX_TYPE_NAMES = {0: "0 legacy", 1: "1 access-list", 2: "2 EIP-1559", 3: "3 blob", 4: "4 set-code (7702)"}


def acf(x: np.ndarray, max_lag: int) -> np.ndarray:
    x = np.asarray(x, float) - np.mean(x)
    denom = float(x @ x)
    return np.array([float(x[:-k] @ x[k:]) / denom for k in range(1, max_lag + 1)])


def hour_axis(ax) -> None:
    ax.set_xlim(0, 24)
    ax.set_xticks(range(0, 25, 3))
    ax.set_xlabel("Time of day (UTC hour)")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    series = {d: fc.load_target_series(d) for d in fc.DATES}
    tx = {d: fc.load_extract(d) for d in fc.DATES}
    summary: dict = {"generated_utc": fc.utc_now(), "script": "Flashbots/eda/run_eda.py", "days": {}}

    # ------------------------------------------------------------ arrival process statistics
    minute_rows, hourly_rows, disp_rows, acf_rows, spike_rows = [], [], [], [], []
    for date, frame in series.items():
        c = frame["arrival_count"].to_numpy(float)
        info: dict = {"transactions": int(c.sum()), "mean_tx_per_s": c.mean(), "sd_1s": c.std(ddof=1),
                      "median_1s": float(np.median(c)), "p95_1s": float(np.quantile(c, 0.95)), "p99_1s": float(np.quantile(c, 0.99)),
                      "max_1s": int(c.max()), "zero_seconds": int((c == 0).sum())}
        for w in WINDOWS:
            agg = c.reshape(-1, w).sum(axis=1) if fc.DAY_S % w == 0 else c[: fc.DAY_S // w * w].reshape(-1, w).sum(axis=1)
            disp_rows.append({"date_utc": date, "window_s": w, "bins": len(agg), "mean_count": agg.mean(),
                              "variance": agg.var(ddof=1), "dispersion_index": agg.var(ddof=1) / agg.mean()})
        minute = c.reshape(-1, 60).mean(axis=1)
        for i, r in enumerate(minute):
            minute_rows.append({"date_utc": date, "minute_of_day": i, "mean_tx_per_s": r,
                                "gas_limit_Mgas_per_s": frame["gas_limit_sum"].to_numpy(float)[i * 60:(i + 1) * 60].mean() / 1e6})
        hourly = c.reshape(24, 3600).mean(axis=1)
        for h, r in enumerate(hourly):
            hourly_rows.append({"date_utc": date, "utc_hour": h, "mean_tx_per_s": r,
                                "zero_seconds": int((c.reshape(24, 3600)[h] == 0).sum())})
        info["hourly_mean_min"] = {"utc_hour": int(hourly.argmin()), "tx_per_s": float(hourly.min())}
        info["hourly_mean_max"] = {"utc_hour": int(hourly.argmax()), "tx_per_s": float(hourly.max())}
        info["hourly_peak_to_trough"] = float(hourly.max() / hourly.min())
        info["minute_mean_range"] = [float(minute.min()), float(minute.max())]
        # autocorrelation: 1-s counts up to 120 s, 60-s means up to 120 min
        a1, a60 = acf(c, 120), acf(minute, 120)
        for k, v in enumerate(a1, 1):
            acf_rows.append({"date_utc": date, "series": "1s_counts", "lag": k, "lag_seconds": k, "acf": v})
        for k, v in enumerate(a60, 1):
            acf_rows.append({"date_utc": date, "series": "60s_mean_rate", "lag": k, "lag_seconds": 60 * k, "acf": v})
        # 12-s slot periodicity (Ethereum slot time): periodogram peak between 5 s and 60 s periods
        freqs, power = signal.periodogram(c - c.mean(), fs=1.0)
        band = (freqs >= 1 / 60) & (freqs <= 1 / 5)
        peak = freqs[band][power[band].argmax()]
        near12 = (freqs > 1 / 12.5) & (freqs < 1 / 11.5)
        info["periodicity_12s"] = {"acf_lag_12": float(a1[11]), "acf_lag_11": float(a1[10]), "acf_lag_13": float(a1[12]),
                                   "acf_lag_24": float(a1[23]), "periodogram_peak_period_s_5_to_60": float(1 / peak),
                                   "power_ratio_12s_band_vs_median_5_60": float(power[near12].max() / np.median(power[band]))}
        info["acf_1s_lag1"], info["acf_60s_lag1"], info["acf_60s_lag60"] = float(a1[0]), float(a60[0]), float(a60[59])
        top = np.argsort(c)[::-1][:20]
        for rank, s in enumerate(top, 1):
            spike_rows.append({"date_utc": date, "rank": rank, "utc_time": str(frame["timestamp_utc"].iloc[s]), "tx_in_second": int(c[s]),
                               "trailing_60s_mean": float(c[max(0, s - 60):s].mean()) if s else None})
        low = minute.argsort()[:5]
        info["lowest_activity_minutes"] = [{"utc_minute": f"{m // 60:02d}:{m % 60:02d}", "tx_per_s": float(minute[m])} for m in sorted(low)]
        # transaction attributes
        t = tx[date]
        types, counts = np.unique(t["tx_type"], return_counts=True)
        info["tx_type_share"] = {TX_TYPE_NAMES.get(int(k), str(k)): float(v / counts.sum()) for k, v in zip(types, counts)}
        for name, values in (("gas_limit", t["gas"]), ("gas_fee_cap_gwei", t["gas_fee_cap_gwei"]),
                             ("gas_tip_cap_gwei", t["gas_tip_cap_gwei"]), ("calldata_bytes", t["data_size"])):
            info[name] = {f"q{q:g}": float(np.quantile(values, q)) for q in (0.01, 0.25, 0.5, 0.75, 0.99)}
        info["contract_creation_share"] = float(t["to_is_null"].mean())
        info["plain_transfer_share_no_selector"] = float((~t["has_selector"]).mean())
        inc = t["included_block_timestamp_ms"] > 0
        info["inclusion_delay_s_median_included"] = float(np.median(t["inclusion_delay_ms"][inc]) / 1000)
        summary["days"][date] = info

    # ------------------------------------------------------------ distribution shift between the two dates
    d1, d2 = fc.DATES
    m1 = np.array([r["mean_tx_per_s"] for r in minute_rows if r["date_utc"] == d1])
    m2 = np.array([r["mean_tx_per_s"] for r in minute_rows if r["date_utc"] == d2])
    shift_rows = []
    rng = np.random.default_rng(20261116)
    for name, a, b in (
        ("per-minute mean arrival rate (tx/s)", m1, m2),
        ("1-s arrival count", series[d1]["arrival_count"].to_numpy(), series[d2]["arrival_count"].to_numpy()),
        ("gas fee cap per tx (gwei)", rng.choice(tx[d1]["gas_fee_cap_gwei"], 200_000, replace=False), rng.choice(tx[d2]["gas_fee_cap_gwei"], 200_000, replace=False)),
        ("gas limit per tx", rng.choice(tx[d1]["gas"], 200_000, replace=False), rng.choice(tx[d2]["gas"], 200_000, replace=False)),
        ("calldata bytes per tx", rng.choice(tx[d1]["data_size"], 200_000, replace=False), rng.choice(tx[d2]["data_size"], 200_000, replace=False)),
    ):
        ks = stats.ks_2samp(a, b)
        shift_rows.append({"quantity": name, "median_" + d1: float(np.median(a)), "median_" + d2: float(np.median(b)),
                           "mean_" + d1: float(np.mean(a)), "mean_" + d2: float(np.mean(b)),
                           "ks_statistic": float(ks.statistic), "ks_p_value": float(ks.pvalue), "n_each": f"{len(a)}/{len(b)}",
                           "note": "per-tx quantities: random subsample of 200,000 per day (seed 20261116); "
                                   "KS p-values assume independence, which autocorrelated time series violate - read the statistic, not p"})
    # rate vs price relation (per minute)
    rel = {}
    for date in fc.DATES:
        frame = series[date]
        minute_rate = frame["arrival_count"].to_numpy(float).reshape(-1, 60).mean(axis=1)
        med_fee = frame.assign(minute=frame["second_of_day"] // 60).groupby("minute")["median_fee_cap_gwei"].median().to_numpy()
        gas_rate = frame["gas_limit_sum"].to_numpy(float).reshape(-1, 60).mean(axis=1)
        rho_fee = stats.spearmanr(minute_rate, med_fee, nan_policy="omit")
        rho_gas = stats.spearmanr(minute_rate, gas_rate)
        rel[date] = {"spearman_rate_vs_median_fee_cap": float(rho_fee.statistic), "spearman_rate_vs_gas_limit_rate": float(rho_gas.statistic),
                     "pearson_rate_vs_gas_limit_rate": float(np.corrcoef(minute_rate, gas_rate)[0, 1])}
    summary["per_minute_relationships"] = rel
    summary["note"] = ("Descriptive statistics of two isolated UTC days. Two days cannot establish a daily or weekly seasonal "
                       "pattern, a trend, or day-of-week effects.")

    pd.DataFrame(minute_rows).to_csv(OUT / "per_minute_activity.csv", index=False)
    pd.DataFrame(hourly_rows).to_csv(OUT / "hourly_profile.csv", index=False)
    pd.DataFrame(disp_rows).to_csv(OUT / "dispersion_by_window.csv", index=False)
    pd.DataFrame(acf_rows).to_csv(OUT / "autocorrelation.csv", index=False)
    pd.DataFrame(spike_rows).to_csv(OUT / "top_spikes_1s.csv", index=False)
    pd.DataFrame(shift_rows).to_csv(OUT / "distribution_shift.csv", index=False)
    fc.write_json(OUT / "eda_summary.json", summary)
    figures(series, tx, pd.DataFrame(minute_rows), pd.DataFrame(hourly_rows), pd.DataFrame(disp_rows), pd.DataFrame(acf_rows))
    print("EDA written to", OUT)


def figures(series, tx, minute, hourly, disp, acf_frame) -> None:
    # 1. activity over the day
    fig, ax = plt.subplots(figsize=(10, 4))
    for date in fc.DATES:
        m = minute[minute.date_utc == date]
        ax.plot(m.minute_of_day / 60, m.mean_tx_per_s, color=fc.DAY_COLOR[date], lw=1.1, label=date)
    hour_axis(ax)
    ax.set_ylabel("Arrival rate (tx/s, 1-min mean)")
    ax.set_title("Public-mempool transaction arrivals per minute, two UTC days")
    ax.legend(loc="upper left")
    fc.save_figure(fig, "eda_01_arrivals_per_minute.png")

    # 2. hourly profile
    fig, ax = plt.subplots(figsize=(8, 3.8))
    for date in fc.DATES:
        h = hourly[hourly.date_utc == date]
        ax.plot(h.utc_hour + 0.5, h.mean_tx_per_s, marker="o", ms=4, color=fc.DAY_COLOR[date], label=date)
    hour_axis(ax)
    ax.set_ylabel("Mean arrival rate (tx/s)")
    ax.set_title("Intraday profile: hourly mean arrival rate")
    ax.legend(loc="upper left")
    fc.save_figure(fig, "eda_02_hourly_profile.png")

    # 3. distribution of 1-s counts with Poisson reference
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8), sharey=True)
    for ax, date in zip(axes, fc.DATES):
        c = series[date]["arrival_count"].to_numpy()
        k = np.arange(0, 81)
        observed = np.bincount(np.minimum(c, 80), minlength=81) / len(c)
        ax.bar(k, observed, width=0.9, color=fc.DAY_COLOR[date], label="observed")
        ax.plot(k, stats.poisson.pmf(k, c.mean()), color=fc.TEXT, lw=1.2, label=f"Poisson(mean={c.mean():.1f})")
        ax.set_yscale("log")
        ax.set_ylim(1e-6, 0.2)
        ax.set_xlabel("Transactions in a 1-s bin (80 = 80 or more)")
        ax.set_title(date)
        ax.legend()
    axes[0].set_ylabel("Share of seconds (log scale)")
    fig.suptitle("1-s arrival counts are heavier-tailed than a Poisson process with the same mean", fontweight="bold")
    fig.tight_layout()
    fc.save_figure(fig, "eda_03_count_distribution.png")

    # 4. dispersion index
    fig, ax = plt.subplots(figsize=(7, 3.8))
    for date in fc.DATES:
        d = disp[disp.date_utc == date]
        ax.plot(d.window_s, d.dispersion_index, marker="o", ms=5, color=fc.DAY_COLOR[date], label=date)
    ax.axhline(1, color=fc.TEXT2, lw=1, ls="--")
    ax.text(1.1, 1.15, "Poisson = 1", color=fc.TEXT2, fontsize=9)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Aggregation window (s, log scale)")
    ax.set_ylabel("Variance / mean of counts (log)")
    ax.set_title("Over-dispersion grows with the window: bursty, correlated arrivals")
    ax.legend()
    fc.save_figure(fig, "eda_04_dispersion_index.png")

    # 5. inter-arrival ECDF
    fig, ax = plt.subplots(figsize=(7, 3.8))
    for date in fc.DATES:
        gaps = np.diff(tx[date]["timestamp_ms"]).astype(float)
        xs = np.sort(gaps)
        ys = np.arange(1, len(xs) + 1) / len(xs)
        idx = np.unique(np.linspace(0, len(xs) - 1, 4000).astype(int))
        ax.plot(np.maximum(xs[idx], 0.5), ys[idx], color=fc.DAY_COLOR[date], label=f"{date} (median {np.median(gaps):.0f} ms)")
    ax.set_xscale("log")
    ax.set_xlabel("Gap to previous arrival (ms, log; zero gaps drawn at 0.5 ms)")
    ax.set_ylabel("Cumulative share of gaps")
    ax.set_title("Inter-arrival times")
    ax.legend(loc="lower right")
    fc.save_figure(fig, "eda_05_interarrival_ecdf.png")

    # 6. autocorrelation
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8))
    for date in fc.DATES:
        a = acf_frame[(acf_frame.date_utc == date) & (acf_frame.series == "1s_counts")]
        axes[0].plot(a.lag, a.acf, color=fc.DAY_COLOR[date], label=date)
        b = acf_frame[(acf_frame.date_utc == date) & (acf_frame.series == "60s_mean_rate")]
        axes[1].plot(b.lag, b.acf, color=fc.DAY_COLOR[date], label=date)
    axes[0].set_xlabel("Lag (s)")
    axes[0].set_title("ACF of 1-s counts")
    axes[1].set_xlabel("Lag (minutes)")
    axes[1].set_title("ACF of 1-min mean rate")
    for ax in axes:
        ax.axhline(0, color=fc.TEXT2, lw=0.8)
        ax.set_ylabel("Autocorrelation")
        ax.legend()
    fig.suptitle("Persistence: the level carries information for minutes to hours", fontweight="bold")
    fig.tight_layout()
    fc.save_figure(fig, "eda_06_autocorrelation.png")

    # 7. gas price distributions
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8))
    bins = np.logspace(-3, 4, 90)
    for date in fc.DATES:
        axes[0].hist(np.clip(tx[date]["gas_fee_cap_gwei"], 1e-3, 1e4), bins=bins, histtype="step", lw=1.5, color=fc.DAY_COLOR[date], label=date, density=True)
        axes[1].hist(np.clip(tx[date]["gas_tip_cap_gwei"], 1e-3, 1e4), bins=bins, histtype="step", lw=1.5, color=fc.DAY_COLOR[date], label=date, density=True)
    for ax, title in zip(axes, ("Max fee per gas (fee cap)", "Priority fee cap (tip)")):
        ax.set_xscale("log")
        ax.set_xlabel("gwei (log scale; clipped to [0.001, 10,000])")
        ax.set_ylabel("Density")
        ax.set_title(title)
        ax.legend()
    fig.suptitle("Gas price bids per transaction", fontweight="bold")
    fig.tight_layout()
    fc.save_figure(fig, "eda_07_gas_price_distribution.png")

    # 8. gas limit and calldata
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8))
    for date in fc.DATES:
        axes[0].hist(tx[date]["gas"], bins=np.logspace(4, 8, 90), histtype="step", lw=1.5, color=fc.DAY_COLOR[date], label=date, density=True)
        axes[1].hist(tx[date]["data_size"] + 1, bins=np.logspace(0, 6, 90), histtype="step", lw=1.5, color=fc.DAY_COLOR[date], label=date, density=True)
    axes[0].set_xscale("log")
    axes[0].set_xlabel("Declared gas limit (gas units, log)")
    axes[0].set_title("Gas limit per transaction (21,000 = plain transfer)")
    axes[1].set_xscale("log")
    axes[1].set_xlabel("Calldata size + 1 (bytes, log)")
    axes[1].set_title("Calldata size per transaction")
    for ax in axes:
        ax.set_ylabel("Density")
        ax.legend()
    fc.save_figure(fig, "eda_08_gas_limit_calldata.png")

    # 9. transaction types
    fig, ax = plt.subplots(figsize=(8, 3.6))
    all_types = sorted(set(np.unique(tx[fc.DATES[0]]["tx_type"])) | set(np.unique(tx[fc.DATES[1]]["tx_type"])))
    width = 0.38
    for i, date in enumerate(fc.DATES):
        t = tx[date]["tx_type"]
        shares = [float((t == k).mean() * 100) for k in all_types]
        xs = np.arange(len(all_types)) + (i - 0.5) * (width + 0.02)
        ax.bar(xs, shares, width=width, color=fc.DAY_COLOR[date], label=date)
        for x, s in zip(xs, shares):
            ax.text(x, s + 1, f"{s:.1f}", ha="center", fontsize=8, color=fc.TEXT2)
    ax.set_xticks(range(len(all_types)), [TX_TYPE_NAMES.get(int(k), str(k)) for k in all_types])
    ax.set_ylabel("Share of transactions (%)")
    ax.set_title("Transaction type mix")
    ax.legend()
    fc.save_figure(fig, "eda_09_tx_type_mix.png")

    # 10. rate vs gas demand (secondary target) per minute
    fig, axes = plt.subplots(1, 2, figsize=(11, 3.8))
    for date in fc.DATES:
        m = minute[minute.date_utc == date]
        axes[0].scatter(m.mean_tx_per_s, m.gas_limit_Mgas_per_s, s=6, alpha=0.45, color=fc.DAY_COLOR[date], label=date, linewidths=0)
        axes[1].plot(m.minute_of_day / 60, m.gas_limit_Mgas_per_s, color=fc.DAY_COLOR[date], lw=1, label=date)
    axes[0].set_xlabel("Arrival rate (tx/s, 1-min mean)")
    axes[0].set_ylabel("Declared gas demand (Mgas/s)")
    axes[0].set_title("Gas demand vs arrival rate (each dot = 1 min)")
    hour_axis(axes[1])
    axes[1].set_ylabel("Declared gas demand (Mgas/s)")
    axes[1].set_title("Declared gas demand per minute")
    for ax in axes:
        ax.legend()
    fc.save_figure(fig, "eda_10_gas_demand.png")

    # 11. zero-arrival seconds per hour (gaps / low activity)
    fig, ax = plt.subplots(figsize=(8, 3.6))
    for i, date in enumerate(fc.DATES):
        h = hourly[hourly.date_utc == date]
        ax.bar(h.utc_hour + (i - 0.5) * 0.4, h.zero_seconds, width=0.38, color=fc.DAY_COLOR[date], label=date)
    hour_axis(ax)
    ax.set_ylabel("Seconds with zero arrivals")
    ax.set_title("Empty 1-s bins per UTC hour (of 3,600)")
    ax.legend()
    fc.save_figure(fig, "eda_11_zero_arrival_seconds.png")

    # 12. inclusion delay (post-inclusion field: descriptive only, never a forecasting input)
    fig, ax = plt.subplots(figsize=(7, 3.8))
    for date in fc.DATES:
        t = tx[date]
        d = t["inclusion_delay_ms"][t["included_block_timestamp_ms"] > 0] / 1000
        xs = np.sort(d)
        idx = np.unique(np.linspace(0, len(xs) - 1, 4000).astype(int))
        ax.plot(xs[idx], (idx + 1) / len(xs), color=fc.DAY_COLOR[date], label=date)
    ax.set_xscale("symlog", linthresh=1)
    ax.set_xlabel("Inclusion delay (s, symmetric log; negative = block time before first sighting)")
    ax.set_ylabel("Cumulative share of included txs")
    ax.set_title("Time from first sighting to block inclusion (descriptive only)")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax.legend(loc="lower right")
    fc.save_figure(fig, "eda_12_inclusion_delay.png")


if __name__ == "__main__":
    main()
