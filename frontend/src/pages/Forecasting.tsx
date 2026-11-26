import { useMemo, useState } from "react";
import { Await, Await2 } from "../components/Await";
import { Callout, Figure, Tech } from "../components/Evidence";
import { PageHead, Pager } from "../components/Layout";
import { Table, XYChart } from "../components/charts";
import type { Flashbots, Forecasts, M2 } from "../lib/data";
import { fmt, hms } from "../lib/format";

export const FC: Record<string, { label: string; how: string }> = {
  persistence_interval: { label: "Persistence", how: "The last observed interval's rate, carried forward. The naive benchmark." },
  seasonal_naive_12: { label: "Seasonal naive, 12 s", how: "The value one Ethereum slot (12 s) earlier." },
  seasonal_naive_60: { label: "Seasonal naive, 60 s", how: "The value 60 s earlier." },
  seasonal_naive_300: { label: "Seasonal naive, 300 s", how: "The value 300 s earlier." },
  rolling_mean_60: { label: "Rolling mean, 60 s", how: "Mean of the last 60 one-second counts." },
  rolling_mean_300: { label: "Rolling mean, 300 s", how: "Mean of the last 300 one-second counts." },
  rolling_mean_900: { label: "Rolling mean, 900 s", how: "Mean of the last 900 one-second counts." },
  expanding_mean: { label: "Expanding mean", how: "Mean of every count since the start of the day." },
  ewma: { label: "EWMA", how: "Exponentially weighted moving average of one-second counts, α fitted on the first day." },
  ridge_ar: { label: "Ridge AR", how: "Ridge regression on trailing means; the only learned model." },
  tod_profile_0921: { label: "Day 1, same time of day", how: "Exploratory, not selectable: the other day's value at the same clock time." },
};
const PICK_COLORS = ["var(--c-static)", "var(--c-reactive)", "var(--c-predictive)"];
const WINDOW: Record<string, number> = { "10": 1800, "60": 3 * 3600, "300": 24 * 3600 };

function Studio({ fc, fb }: { fc: Forecasts; fb: Flashbots }) {
  const [h, setH] = useState<"10" | "60" | "300">("60");
  const data = fc[h];
  const available = Object.keys(data.models);
  const [picked, setPicked] = useState<string[]>(["ewma", "persistence_interval"]);
  const shown = picked.filter((m) => available.includes(m));
  const span = WINDOW[h];
  const dayStart = data.origin_s[0], dayEnd = data.origin_s[data.origin_s.length - 1];
  const [start, setStart] = useState(20400 - 3600);
  const s0 = Math.min(Math.max(start, dayStart), Math.max(dayStart, dayEnd - span));
  const idx = useMemo(() => data.origin_s.map((o, i) => [o, i] as const).filter(([o]) => o >= s0 && o < s0 + span).map(([, i]) => i), [data, s0, span]);
  const toggle = (m: string) => setPicked((p) => (p.includes(m) ? p.filter((x) => x !== m) : [...p, m].slice(-3)));
  const windowMae = (m: string) => {
    const e = idx.map((i) => { const y = data.models[m][i]; return y === null ? null : Math.abs(y - data.actual[i]); }).filter((v): v is number => v !== null);
    return e.reduce((a, b) => a + b, 0) / e.length;
  };
  const stored = fb.comparison.filter((r) => r.target === "arrival_rate" && String(r.horizon_s) === h);
  const series = [
    { id: "actual", label: "Observed", color: "var(--d-actual)", width: 1.5, marker: false, points: idx.map((i) => ({ x: data.origin_s[i] + Number(h), y: data.actual[i] })) },
    ...shown.map((m) => ({ id: m, label: FC[m].label, color: PICK_COLORS[picked.indexOf(m) % 3], width: 1.75, marker: false,
      points: idx.map((i) => ({ x: data.origin_s[i] + Number(h), y: data.models[m][i] })) })),
  ];
  const sel = stored.find((r) => r.selected === true);
  return (
    <>
      <div className="toolbar">
        <div><span className="lbl">Forecast horizon</span><div className="segmented" role="radiogroup" aria-label="Forecast horizon">
          {(["10", "60", "300"] as const).map((x) => <button key={x} role="radio" aria-checked={h === x} onClick={() => setH(x)}>{x} s</button>)}
        </div></div>
        {h !== "300" && (
          <div className="field" style={{ minWidth: 260, flex: "1 1 260px" }}>
            <div className="field-head"><label htmlFor="win">Window start (test day, UTC)</label><output htmlFor="win" className="num">{hms(s0)}–{hms(s0 + span)}</output></div>
            <input id="win" type="range" min={dayStart} max={dayEnd - span} step={300} value={s0} onChange={(e) => setStart(Number(e.target.value))} />
          </div>
        )}
      </div>
      <div style={{ marginBottom: 12 }}>
        <span className="lbl" style={{ fontSize: "var(--t-xs)", color: "var(--ink-3)" }}>Methods (up to three)</span>
        <div className="pill-row" style={{ marginTop: 4 }}>
          {available.map((m) => (
            <button key={m} className="btn btn-quiet btn-sm" aria-pressed={shown.includes(m)} onClick={() => toggle(m)}
              style={shown.includes(m) ? { borderColor: PICK_COLORS[picked.indexOf(m) % 3], boxShadow: `inset 0 -3px 0 ${PICK_COLORS[picked.indexOf(m) % 3]}` } : undefined} title={FC[m]?.how}>
              {FC[m]?.label ?? m}
            </button>
          ))}
        </div>
      </div>
      <Figure title={`Observed and forecast arrival rate, ${h}-second horizon`} kind="model"
        caption={<>Each point is a forecast issued from data before its origin, plotted at the end of the interval it predicts. The test day was held out from fitting and selection. {h === "300" ? "Whole day shown." : "Drag the slider to move the window."}</>}
        table={<Table head={["Method", `Window MAE (tx/s, ${idx.length} origins)`]} align={["l"]} rows={shown.map((m) => [FC[m].label, fmt(windowMae(m), 3)])} />}>
        <XYChart series={series} ariaLabel={`Observed versus forecast arrival rate at ${h} s horizon`} xLabel="Time on the test day (UTC)" yLabel="Mean arrival rate over the horizon (tx/s)"
          height={320} zeroBased fx={(v) => hms(v)} tipX={(v) => hms(v) + " UTC"} fy={(v) => fmt(v, 1)}
          bands={s0 <= 20400 && s0 + span >= 21000 ? [{ x0: 20400, x1: 21000, label: "M2 window" }] : []} />
        <div className="pill-row" style={{ marginTop: 12 }}>
          {shown.map((m) => <span key={m} className="small">{FC[m].label}: <b className="num">{fmt(windowMae(m), 2)}</b> tx/s MAE in this window</span>)}
        </div>
      </Figure>

      <h3 style={{ marginTop: 32, marginBottom: 12 }}>Full test day, {h}-second horizon</h3>
      <Table caption={`Stored scores on the test day. Selected: chosen on validation data before the test day was scored.`}
        head={["Method", "MAE (tx/s)", "RMSE", "Skill vs persistence", "Rank on test", ""]} align={["l", "r", "r", "r", "r", "l"]}
        rows={stored.slice().sort((a, b) => (a.mae as number) - (b.mae as number)).map((r) => [FC[r.model as string]?.label ?? r.model, fmt(r.mae as number, 3), fmt(r.rmse as number, 2),
          `${(r.skill_mae_vs_persistence as number) >= 0 ? "+" : "−"}${fmt(Math.abs((r.skill_mae_vs_persistence as number) * 100), 1)}%`, r.rank_F_mae as number,
          r.selected ? "Selected" : r.exploratory_not_selectable ? "Exploratory" : ""])} />
      {sel && h === "60" && <p className="small muted" style={{ marginTop: 8 }}>Ridge AR scored 0.01 tx/s better than the selected EWMA on the test day. The difference is not significant (p = 0.31), and selection was fixed before the test, so EWMA stays the choice.</p>}
    </>
  );
}

function LiveForecast({ m2 }: { m2: M2 }) {
  const rows = m2.forecast.filter((r) => r.run_id === m2.forecast[0].run_id);
  const ids = [...new Set(m2.forecast.map((r) => r.run_id))];
  const identical = ids.every((id) => m2.forecast.filter((r) => r.run_id === id).every((r, i) => r.forecast_tps === rows[i].forecast_tps));
  const mae = (k: "err_ewma" | "err_naive") => rows.reduce((s, r) => s + Math.abs(r[k]), 0) / rows.length;
  return (
    <Figure title="The live forecast inside M2" kind="measured"
      caption={<>At each of the ten decisions the Predictive controller forecast the next minute's rate. The largest miss is decision 5, where the offered rate jumped to 54 tx/s against a forecast of 26.8 tx/s: the EWMA's long memory damps jumps. The arrival schedule is identical in every run, so the three Predictive runs ({ids.join(", ")}) produced {identical ? "the same" : "different"} ten forecasts: one forecast sequence, not three samples.</>}
      table={<Table head={["Decision", "Realised (tx/s)", "EWMA forecast", "Persistence"]} align={["l"]} rows={rows.map((r) => [r.decision + 1, fmt(r.realized_tps, 2), fmt(r.forecast_tps, 2), fmt(r.naive_persistence_tps, 2)])} />}>
      <XYChart ariaLabel="Live M2 forecasts against realised rate at each decision" xLabel="Decision" yLabel="Arrival rate of the next minute (tx/s)" height={280}
        xTicks={[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]} tipX={(v) => `Decision ${v}`} fy={(v) => fmt(v, 1)}
        series={[
          { id: "real", label: "Realised", color: "var(--d-actual)", points: rows.map((r) => ({ x: r.decision + 1, y: r.realized_tps })) },
          { id: "ewma", label: "EWMA forecast", color: "var(--c-predictive)", points: rows.map((r) => ({ x: r.decision + 1, y: r.forecast_tps })) },
          { id: "naive", label: "Persistence", color: "var(--c-reference)", dash: "5 4", points: rows.map((r) => ({ x: r.decision + 1, y: r.naive_persistence_tps })) },
        ]} />
      <p className="small" style={{ marginTop: 8 }}>MAE over the ten decisions: EWMA <b className="num">{fmt(mae("err_ewma"), 2)}</b> tx/s, persistence <b className="num">{fmt(mae("err_naive"), 2)}</b> tx/s.</p>
    </Figure>
  );
}

export default function Forecasting() {
  return (
    <div className="page">
      <PageHead crumb="Research" title="Workload forecasting"
        summary="How well can the next minute's transaction arrival rate be predicted from the last few minutes? Better than carrying the last value forward, by about a fifth. On this testbed that improvement never changed a control decision." />
      <Await2<Forecasts, Flashbots> files={["data/forecasts.json", "data/flashbots.json"]} height={420}>
        {(fc, fb) => <Studio fc={fc} fb={fb} />}
      </Await2>

      <section className="page-section">
        <h2>From offline benchmark to live controller</h2>
        <p className="read">The 60-second EWMA was frozen after the benchmark and ran unchanged inside the Predictive controller during M2, on the scaled replay of a test-day window.</p>
        <Await<M2> file="data/m2.json">{(m2) => <LiveForecast m2={m2} />}</Await>
      </section>

      <section className="page-section">
        <h2>How the Ethereum workload is used</h2>
        <div className="cols-2">
          <div className="read" style={{ fontSize: "var(--t-md)" }}>
            <p>Ethereum's public mempool gives a long, real record of when transactions arrive: bursty, persistent and periodic. That timing is what a Fabric controller would need to anticipate.</p>
            <p>On the testbed, one 10-minute window's per-minute rates were scaled by 2.114, so the peak stays below the highest rate the co-located client could drive. Caliper then submits synthetic Fabric transactions at those rates.</p>
          </div>
          <Callout title="What is not transferred">
            Ethereum and Fabric are not interchangeable. No Ethereum transaction, fee, gas value or contract reaches Fabric. The replay also
            spaces arrivals evenly inside each minute, which removes bursts and the 12-second slot cycle.
          </Callout>
        </div>
      </section>

      <section className="page-section">
        <h2>Limitations</h2>
        <ul className="read">
          <li>Two days of data. The test day had been opened once before by an earlier benchmark, so the final score is a second look.</li>
          <li>Point forecasts only: no uncertainty bands or calibration.</li>
          <li>The gain is statistically clear (Holm-adjusted Diebold–Mariano, all four horizons) but practically small: about 0.26 tx/s at 60 s on a mean of 10.6 tx/s.</li>
          <li>Declared gas demand was also tested and could not be forecast better than naive (Holm p = 1).</li>
        </ul>
        <Tech summary="Protocol FB-FC-v1">
          <p>Target: mean arrival rate over horizon H from one-second counts before the origin. Fit on the first half of day 1, select on its second half, score once on day 2 after the selection file was frozen and hashed (SHA-256 7823173D…898AA6). Skill = 1 − MAE / MAE(persistence).</p>
        </Tech>
      </section>
      <Pager />
    </div>
  );
}
