import { useMemo, useState } from "react";
import { Await, Await2 } from "../components/Await";
import { Callout, Ev, Figure, Tech } from "../components/Evidence";
import { PageHead, Pager } from "../components/Layout";
import { GridHeatmap } from "../components/Heatmap";
import { Table, XYChart } from "../components/charts";
import type { Flashbots, HfbtpGrid, HfbtpSummary, Models } from "../lib/data";
import { fmt, int } from "../lib/format";

const ROLE: Record<string, [string, string, string]> = {
  "Set Transaction Arrival Rate": ["Feature", "tx/s", "Offered load, chosen before the run."],
  "Block Size": ["Feature", "transactions", "MaxMessageCount, the block-size setting."],
  "Orderers": ["Feature", "count", "Number of ordering nodes."],
  "Throughput": ["Target", "tx/s", "Committed throughput as reported by Caliper."],
  "Avg Latency": ["Target", "s", "Mean transaction latency as reported by Caliper."],
  "Actual Transaction Arrival Rate": ["Excluded", "tx/s", "Measured during the run (r = 0.985 with throughput). Using it would leak the answer."],
  "Min Latency": ["Excluded", "s", "Outcome of the run, not known at decision time."],
  "Max Latency": ["Excluded", "s", "Outcome of the run. Half the rows sit on a 2.0–2.3 s shelf near the batch timeout."],
};

function Hist({ counts, start, width, unit, color = "var(--b-5)" }: { counts: number[]; start: number; width: number; unit: string; color?: string }) {
  const max = Math.max(...counts);
  return (
    <svg viewBox={`0 0 ${counts.length * 6} 40`} preserveAspectRatio="none" style={{ width: "100%", height: 40 }} role="img"
      aria-label={`Histogram from ${start} in steps of ${width} ${unit}`}>
      {counts.map((c, i) => <rect key={i} x={i * 6 + 0.5} width={5} y={40 - (c / max) * 38} height={(c / max) * 38} rx={1} fill={color} />)}
    </svg>
  );
}

const SPLITS = [
  { id: "T1", label: "T1 interior block sizes", axis: "block", test: "Block size 210–305", val: "Block size 610–705", testKey: "T1" },
  { id: "T1b", label: "T1b right-edge block sizes", axis: "block", test: "Block size 710–800", val: "Block size 610–705", testKey: "T1b" },
  { id: "T2D0", label: "T2 unseen load, draw D0", axis: "rate", test: [30, 140], val: [150, 160], testKey: "T2D0" },
  { id: "T2D1", label: "T2 draw D1", axis: "rate", test: [50, 115], val: [25, 135], testKey: "T2D1" },
  { id: "T2D2", label: "T2 draw D2", axis: "rate", test: [85, 130], val: [20, 145], testKey: "T2D2" },
  { id: "T2D3", label: "T2 draw D3", axis: "rate", test: [55, 125], val: [40, 105], testKey: "T2D3" },
  { id: "T3", label: "T3 orderer transfer", axis: "orderers", test: "Orderers 5 and 9", val: "Train on 3 and 7", testKey: "T3" },
  { id: "HEAVY", label: "HEAVY overload extrapolation", axis: "rate", test: "≥ 165", val: [150, 155], testKey: "HEAVY" },
] as const;

function SplitView({ levels, models }: { levels: number[]; models: Models }) {
  const [id, setId] = useState<string>("T2D0");
  const s = SPLITS.find((x) => x.id === id)!;
  const nTest = models.phase1c.test.find((r) => r.manifest === s.testKey)?.n_test;
  const roleOf = (v: number) => {
    if (s.id === "HEAVY") return v >= 165 ? "test" : v === 150 || v === 155 ? "val" : v <= 145 ? "train" : "none";
    if (Array.isArray(s.test)) return (s.test as readonly number[]).includes(v) ? "test" : (s.val as readonly number[]).includes(v) ? "val" : s.id === "T2D0" ? (v <= 145 ? "train" : "none") : "other";
    return "na";
  };
  const color: Record<string, string> = { test: "var(--ink)", val: "var(--signal)", train: "var(--b-2)", none: "var(--paper-sunk)", other: "var(--rule)", na: "var(--rule)" };
  return (
    <div>
      <div className="toolbar">
        <div><label className="lbl" htmlFor="split">Holdout</label>
          <select id="split" value={id} onChange={(e) => setId(e.target.value)} style={{ minWidth: 260 }}>
            {SPLITS.map((x) => <option key={x.id} value={x.id}>{x.label}</option>)}
          </select></div>
        <div className="small muted">Test rows scored: <b className="num" style={{ color: "var(--ink)" }}>{nTest ? int(nTest) : "not stored"}</b></div>
      </div>
      {s.axis === "rate" ? (
        <>
          <div style={{ display: "grid", gridTemplateColumns: `repeat(${levels.length}, 1fr)`, gap: 2 }} role="img"
            aria-label={`Arrival-rate levels for ${s.label}: test ${String(s.test)}, validation ${String(s.val)}`}>
            {levels.map((v) => <div key={v} title={`${v} tx/s: ${roleOf(v)}`} style={{ height: 28, borderRadius: 2, background: color[roleOf(v)] }} />)}
          </div>
          <div className="xs muted" style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}><span>10 tx/s</span><span>200 tx/s</span></div>
          <div className="legend" style={{ marginTop: 8 }}>
            <span><i className="dot" style={{ background: color.test }} />Test</span><span><i className="dot" style={{ background: color.val }} />Validation</span>
            {s.id === "T2D0" || s.id === "HEAVY" ? <span><i className="dot" style={{ background: color.train }} />Training</span> : <span><i className="dot" style={{ background: color.other }} />Training or excluded (see protocol)</span>}
            {(s.id === "T2D0") && <span><i className="dot" style={{ background: color.none, border: "1px solid var(--rule-strong)" }} />Not used (155, ≥ 165)</span>}
          </div>
        </>
      ) : (
        <Table head={["Partition", "Definition"]} align={["l", "l"]} rows={[["Test", String(s.test)], [s.axis === "orderers" ? "Training" : "Validation", String(s.val)]]} />
      )}
      <p className="small muted" style={{ marginTop: 12 }}>Groups, not rows, are held out: a model never sees any cell of a test level or block range. One locked test pass per model family.</p>
    </div>
  );
}

function Surface({ grid, summary }: { grid: HfbtpGrid; summary: HfbtpSummary }) {
  const [target, setTarget] = useState<"throughput" | "avg_latency">("avg_latency");
  const [ord, setOrd] = useState(3);
  const blocks = useMemo(() => { const [a, b, step] = summary.blocks; const out: number[] = []; for (let v = a; v <= b; v += step) out.push(v); return out; }, [summary]);
  const cells = useMemo(() => {
    const m = new Map<string, number>();
    for (const r of grid.rows) if (r[2] === ord) m.set(`${r[0]}|${r[1]}`, target === "throughput" ? r[3] : r[4]);
    return m;
  }, [grid, ord, target]);
  return (
    <Figure title="Observed response surface" kind="historical" source={grid.source}
      caption={<>Each cell is one Caliper run in HFBTP; there are no replicates. Latency is lowest in a band of mid-size blocks and rises at small blocks under heavy load; throughput tracks offered load until it saturates.</>}
      note={<>Fabric 2.3, three organisations with three peers each, other hardware. Not our testbed.</>}>
      <div className="toolbar">
        <div><span className="lbl">Value</span><div className="segmented" role="radiogroup" aria-label="Value">
          {(["avg_latency", "throughput"] as const).map((t) => <button key={t} role="radio" aria-checked={target === t} onClick={() => setTarget(t)}>{t === "throughput" ? "Throughput" : "Average latency"}</button>)}
        </div></div>
        <div><span className="lbl">Orderers</span><div className="segmented" role="radiogroup" aria-label="Orderers">
          {summary.orderers.map((o) => <button key={o} role="radio" aria-checked={ord === o} onClick={() => setOrd(o)}>{o}</button>)}
        </div></div>
      </div>
      <GridHeatmap cells={cells} levels={summary.levels} blocks={blocks} value={target === "throughput" ? "Throughput" : "Latency"}
        unit={target === "throughput" ? "tx/s" : "s"} fmt={(v) => fmt(v, target === "throughput" ? 1 : 2)}
        ariaLabel={`HFBTP observed ${target} for ${ord} orderers over arrival rate and block size`} />
    </Figure>
  );
}

function Hfbtp() {
  return (
    <Await2<HfbtpSummary, Models> files={["data/hfbtp_summary.json", "data/models.json"]}>
      {(s, models) => (
        <>
          <dl className="kv">
            <div><dt>Rows</dt><dd className="num">{int(s.rows)}</dd></div>
            <div><dt>Arrival-rate levels</dt><dd className="num">{s.levels.length} <small>10–200 tx/s</small></dd></div>
            <div><dt>Block sizes</dt><dd className="num">{s.blocks[3]} <small>10–800</small></dd></div>
            <div><dt>Orderer counts</dt><dd className="num">{s.orderers.length} <small>3, 5, 7, 9</small></dd></div>
            <div><dt>Absent cells</dt><dd className="num">{s.absent_count} <small>never imputed</small></dd></div>
            <div><dt>Missing values</dt><dd className="num">0</dd></div>
          </dl>
          <p className="small muted" style={{ marginTop: 8 }}><Ev kind="historical" /> SHA-256 <span className="hash">{s.sha256.slice(0, 16)}…</span> verified when this site's data was built. One observation per configuration, no timestamps, no replicates.</p>

          <section className="page-section">
            <h2>Columns and what each may be used for</h2>
            <Table head={["Column", "Use", "Unit", "Range", "Distinct", "Distribution", "Why"]} align={["l", "l", "l", "r", "r", "l", "l"]}
              rows={s.columns.map((c) => {
                const [use, unit, why] = ROLE[c.name] ?? ["", "", ""];
                return [c.name, use === "Excluded" ? <span style={{ color: "var(--bad)", fontWeight: 600 }}>Excluded</span> : use, unit,
                  `${fmt(c.min, Number.isInteger(c.min) && Number.isInteger(c.max) ? 0 : 2)}–${fmt(c.max, Number.isInteger(c.min) && Number.isInteger(c.max) ? 0 : 2)}`, int(c.distinct),
                  <div style={{ width: 120 }}><Hist counts={c.hist.counts} start={c.hist.start} width={c.hist.width} unit={unit} color={use === "Target" ? "var(--b-10)" : use === "Excluded" ? "var(--c-reference)" : "var(--b-5)"} /></div>,
                  <span className="muted">{why}</span>];
              })} />
          </section>

          <section className="page-section">
            <h2>Correlation</h2>
            <p className="read">Pearson correlation over all rows. Throughput follows the offered rate closely (r = 0.98). Average latency falls as the rate rises (r = −0.67), because at low load a transaction waits longer for its block to fill. Block size (r = 0.29 with latency) and orderers (0.03) look weak on average because their effect depends on load. The measured arrival rate is almost a copy of throughput, which is why it is excluded.</p>
            <div className="table-wrap" tabIndex={0} role="region" aria-label="Correlation matrix">
              <table className="data"><thead><tr><th scope="col">Column</th>{s.header.map((h) => <th key={h} scope="col" title={h}>{abbr(h)}</th>)}</tr></thead>
                <tbody>{s.header.map((h, i) => <tr key={h}><td>{h}</td>{s.correlation[i].map((v, j) => <td key={j} style={{ background: corrColor(v) }}>{fmt(v, 2)}</td>)}</tr>)}</tbody></table>
            </div>
            <div className="scale-bar"><span>−1</span><span className="ramp" style={{ background: "linear-gradient(90deg,color-mix(in srgb,#e5534b 45%,transparent),transparent,color-mix(in srgb,#338ced 45%,transparent))", border: "1px solid var(--rule)" }} /><span>+1</span></div>
          </section>

          <section className="page-section">
            <Await<HfbtpGrid> file="data/hfbtp_grid.json" height={320}>{(grid) => <Surface grid={grid} summary={s} />}</Await>
          </section>

          <section className="page-section">
            <h2>Missing cells</h2>
            <p className="read">The full grid would have 24,804 cells. {s.absent_count} are absent, exactly three at every arrival-rate level, always in the same slots. The cause is unknown. They are reported, never filled in, and every split accounts for them.</p>
            <Table head={["Block size", "Orderers", "Absent at"]} align={["l", "l", "l"]} rows={s.absent_slots.map((a) => [a.block, a.orderers, `all ${s.levels.length} arrival-rate levels`])} />
          </section>

          <section className="page-section">
            <h2>Training and test splits</h2>
            <p className="read">Random row splits on a one-observation grid measure interpolation between neighbouring cells. These group holdouts ask harder questions: unseen block sizes, unseen load levels, unseen orderer counts, and load beyond the training range.</p>
            <SplitView levels={s.levels} models={models} />
          </section>
        </>
      )}
    </Await2>
  );
}

function FlashbotsView() {
  return (
    <Await<Flashbots> file="data/flashbots.json">
      {(f) => {
        const days = Object.keys(f.days);
        const series = days.map((d, i) => ({
          id: d, label: i === 0 ? "Day 1 (fit and select)" : "Day 2 (held-out test)", color: i === 0 ? "var(--c-reference)" : "var(--d-actual)", width: 1.25, marker: false,
          points: f.days[d].per_minute_tps.map((v, m) => ({ x: m / 60, y: v })),
        }));
        const w = f.m2_window;
        return (
          <>
            <dl className="kv">
              {days.map((d) => <div key={d}><dt>Day {days.indexOf(d) + 1}</dt><dd className="num">{int(f.days[d].transactions)} <small>tx</small></dd></div>)}
              {days.map((d) => <div key={d + "m"}><dt>Mean rate, day {days.indexOf(d) + 1}</dt><dd className="num">{fmt(f.eda[d].mean_tx_per_s as number)} <small>tx/s</small></dd></div>)}
              {days.map((d) => <div key={d + "x"}><dt>Busiest second, day {days.indexOf(d) + 1}</dt><dd className="num">{int(f.days[d].max_1s)} <small>tx</small></dd></div>)}
            </dl>
            <Figure title="Arrival rate through the day" kind="historical" source={days.map((d) => f.days[d].source)}
              caption={<>Per-minute mean of one-second arrival counts. The shaded band is the 10-minute window of day 2 replayed in M2 (start {Math.floor(w.start_s / 3600)}:{String((w.start_s % 3600) / 60).padStart(2, "0")} UTC), multiplied by k = {w.k} on the testbed. The two days are never joined.</>}
              note={<>Arrivals are over-dispersed and persistent, with a 12-second cycle from Ethereum's slot time.</>}
              table={<Table head={["Day", "Transactions", "Mean (tx/s)", "Median (1 s)", "p99 (1 s)", "Max (1 s)", "Zero seconds"]} align={["l"]}
                rows={days.map((d, i) => [`Day ${i + 1}`, int(f.days[d].transactions), fmt(f.eda[d].mean_tx_per_s as number, 2), String(f.eda[d].median_1s), String(f.eda[d].p99_1s), String(f.eda[d].max_1s), String(f.eda[d].zero_seconds)])} />}>
              <XYChart series={series} ariaLabel="Per-minute arrival rate for the two Flashbots days" xLabel="Hour of day (UTC)" yLabel="Arrival rate (tx/s, per-minute mean)"
                xDomain={[0, 24]} xTicks={[0, 3, 6, 9, 12, 15, 18, 21, 24]} height={300} bands={[{ x0: w.start_s / 3600, x1: w.start_s / 3600 + w.minutes / 60, label: "M2 window" }]}
                fx={(v) => `${v}h`} tipX={(v) => `${String(Math.floor(v)).padStart(2, "0")}:${String(Math.round((v % 1) * 60)).padStart(2, "0")} UTC`} fy={(v) => fmt(v, 1)} />
            </Figure>
            <Callout title="How this trace is used on Fabric">
              Only the reception times are kept. They set when Caliper submits synthetic CreateAsset transactions to Fabric. Ethereum and Fabric are
              different systems: the trace provides a realistic, varying arrival rate, not Ethereum transactions, fees or contracts.
            </Callout>
          </>
        );
      }}
    </Await>
  );
}

export default function Datasets() {
  const [tab, setTab] = useState<"hfbtp" | "flash">("hfbtp");
  return (
    <div className="page">
      <PageHead crumb="Research" title="Datasets"
        summary="Two public datasets with different jobs. HFBTP records how one Fabric deployment responds to load and configuration; it was used to train and test offline models. Flashbots records when Ethereum transactions arrived; it was used to forecast load and to pace the live testbed." />
      <div className="segmented" role="tablist" aria-label="Dataset" style={{ marginBottom: 24 }}>
        <button role="tab" aria-selected={tab === "hfbtp"} onClick={() => setTab("hfbtp")}>HFBTP performance grid</button>
        <button role="tab" aria-selected={tab === "flash"} onClick={() => setTab("flash")}>Flashbots arrivals</button>
      </div>
      {tab === "hfbtp" ? <Hfbtp /> : <FlashbotsView />}
      <section className="page-section">
        <h2>Limitations</h2>
        <ul className="read">
          <li>HFBTP has one observation per cell, so it cannot measure run-to-run noise, calibrate a prediction interval, or show anything over time.</li>
          <li>HFBTP comes from Fabric 2.3 on other hardware. Where B = 10 saturates there (130–160 tx/s) differs from our testbed, where it did not saturate at 160 tx/s.</li>
          <li>Flashbots covers two days of the public mempool only. The test day had been opened once by an earlier benchmark before the final test, so it is a second look.</li>
          <li>The secondary MMBPD dataset (same author group, no orderer column) was audited but not used for any model result.</li>
        </ul>
        <Tech summary="Integrity checks run when this site's data was built">
          <p>HFBTP SHA-256 matched the frozen value; 24,687 × 8; 117 absent cells at exactly 3 per level; no duplicate cells. Flashbots day totals matched the stored EDA (1,055,934 and 913,856). Script: <code>frontend/scripts/build_data.py</code>.</p>
        </Tech>
      </section>
      <Pager />
    </div>
  );
}

function abbr(h: string) {
  return ({ "Set Transaction Arrival Rate": "Set", "Actual Transaction Arrival Rate": "Actual", "Block Size": "Block", "Orderers": "Ord.", "Throughput": "Thr.", "Avg Latency": "Avg lat.", "Min Latency": "Min lat.", "Max Latency": "Max lat." } as Record<string, string>)[h] ?? h;
}
function corrColor(v: number) {
  const a = Math.min(1, Math.abs(v));
  return v >= 0 ? `color-mix(in srgb, #338ced ${Math.round(a * 45)}%, transparent)` : `color-mix(in srgb, #e5534b ${Math.round(a * 45)}%, transparent)`;
}
