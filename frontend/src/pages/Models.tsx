import { useState } from "react";
import { Await } from "../components/Await";
import { Callout, Figure, Tech } from "../components/Evidence";
import { PageHead, Pager } from "../components/Layout";
import { DotStrip, Table, XYChart } from "../components/charts";
import type { Models as M } from "../lib/data";
import { fileName, fmt } from "../lib/format";

export const FAMILY: Record<string, { label: string; kind: "baseline" | "learned"; plain: string }> = {
  lookup_workload: { label: "Workload lookup", kind: "baseline", plain: "Median of the target over training rows at the same arrival-rate level." },
  lookup_no_blocksize: { label: "Lookup without block size", kind: "baseline", plain: "Ablation: uses arrival rate and orderers but no block-size information." },
  lookup_config: { label: "Configuration lookup", kind: "baseline", plain: "Median of the target over training rows at the same (rate, block size, orderers) cell." },
  linear: { label: "Multiple linear regression", kind: "learned", plain: "Ordinary least squares on the three raw features; no scaling, interactions or transforms." },
  dt: { label: "Shallow decision tree", kind: "learned", plain: "Regression tree, depth 3–6 and minimum leaf size searched on validation." },
  rf: { label: "Random forest", kind: "learned", plain: "300 regression trees; depth and leaf size searched on validation." },
  hgb: { label: "Histogram gradient boosting", kind: "learned", plain: "Boosted trees; iterations, learning rate and leaves searched on validation. The exported family." },
};
const FAM_ORDER = ["lookup_workload", "lookup_no_blocksize", "lookup_config", "linear", "dt", "rf", "hgb"];
const SPLIT_ORDER = ["T1", "T1b", "T2D0", "T2D1", "T2D2", "T2D3", "T3", "HEAVY"];
const SPLIT_TEXT: Record<string, string> = {
  T1: "Unseen interior block sizes", T1b: "Unseen right-edge block sizes", T2D0: "Unseen load, draw D0", T2D1: "Unseen load, D1",
  T2D2: "Unseen load, D2", T2D3: "Unseen load, D3", T3: "Unseen orderer counts", HEAVY: "Overload beyond training",
};

function Matrix({ m, target }: { m: M; target: string }) {
  const lat = target === "Avg Latency";
  const sel = new Map(m.phase1c.selected.filter((s) => s.target === target).map((s) => [s.manifest, s.family]));
  return (
    <div className="table-wrap" tabIndex={0} role="region" aria-label={`Test MAE by family and holdout, ${target}`}>
      <table className="data">
        <caption>Test MAE ({lat ? "s" : "tx/s"}). Outlined cell: the family selected on validation error, before the test was opened. "n/a": the family cannot produce an estimate on that split by design.</caption>
        <thead><tr><th scope="col">Holdout</th>{FAM_ORDER.map((f) => <th scope="col" key={f} title={FAMILY[f].plain}>{FAMILY[f].label}</th>)}</tr></thead>
        <tbody>
          {SPLIT_ORDER.map((sp) => (
            <tr key={sp} className={sp === "HEAVY" ? "sep" : undefined}>
              <td><b>{sp}</b><div className="xs muted">{SPLIT_TEXT[sp]}</div></td>
              {FAM_ORDER.map((f) => {
                const r = m.phase1c.test.find((x) => x.manifest === sp && x.target === target && x.family === f);
                const chosen = sel.get(sp) === f;
                const v = r?.test_mae;
                return (
                  <td key={f} style={chosen ? { outline: "2px solid var(--ink)", outlineOffset: -3, fontWeight: 600 } : undefined}>
                    {v === null || v === undefined || (r?.n_unsupported && r.n_unsupported === r.n_test) ? <span className="muted">n/a</span> : fmt(v, lat ? 4 : 2)}
                    {chosen && <span className="visually-hidden"> (selected)</span>}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function Models() {
  const [target, setTarget] = useState("Throughput");
  return (
    <div className="page">
      <PageHead crumb="Research" title="Model evaluation"
        summary="Can a model learned from the public HFBTP grid predict Fabric's throughput and latency for a configuration it has not seen? Inside the measured range, mostly yes. Beyond it, no family worked, and none of these models was used by a live controller." />
      <Await<M> file="data/models.json">
        {(m) => {
          const lat = target === "Avg Latency";
          const sel = m.phase1c.selected.filter((s) => s.target === target);
          const selTest = (sp: string) => { const f = sel.find((s) => s.manifest === sp)!.family; return m.phase1c.test.find((r) => r.manifest === sp && r.target === target && r.family === f)!; };
          const heavy = (fam: string) => m.phase1c.per_level.filter((r) => r.manifest === "HEAVY" && r.target === target && r.family === fam)
            .map((r) => ({ x: r.set_level as number, y: r.mae as number }));
          return (
            <>
              <div className="toolbar">
                <div><span className="lbl">Target</span><div className="segmented" role="radiogroup" aria-label="Target">
                  {["Throughput", "Avg Latency"].map((t) => <button key={t} role="radio" aria-checked={target === t} onClick={() => setTarget(t)}>{t === "Throughput" ? "Throughput (tx/s)" : "Average latency (s)"}</button>)}
                </div></div>
              </div>

              <section>
                <h2 style={{ marginBottom: 12 }}>Every family on every holdout</h2>
                <p className="read" style={{ marginBottom: 16 }}>
                  Seven pre-registered families, each tuned on its validation partition and scored once on test. Simple lookups are
                  hard to beat inside the block-size grid; the learned models win on unseen load levels; nothing survives overload.
                </p>
                <Matrix m={m} target={target} />
                <p className="small muted" style={{ marginTop: 8 }}>Source <span className="hash">{fileName(m.phase1c.source)}</span></p>
              </section>

              <Figure title="Unseen load: report the range, not one draw" kind="model" source={m.phase1c.source}
                caption={<>Test MAE of the validation-selected model on each of four random draws of held-out load levels. Draw D3 holds out 125 tx/s, near the saturation knee, and is about three times harder than D0.</>}
                note={<>The paper reports T2 as the range across draws ({fmt(Math.min(...["T2D0", "T2D1", "T2D2", "T2D3"].map((d) => selTest(d).test_mae!)), lat ? 3 : 1)} to {fmt(Math.max(...["T2D0", "T2D1", "T2D2", "T2D3"].map((d) => selTest(d).test_mae!)), lat ? 3 : 1)} {lat ? "s" : "tx/s"}), never pooled.</>}
                table={<Table head={["Draw", "Selected family", "Test MAE", "Test R²"]} align={["l", "l"]} rows={["T2D0", "T2D1", "T2D2", "T2D3"].map((d) => { const r = selTest(d); return [d, FAMILY[r.family].label, fmt(r.test_mae, lat ? 4 : 2), fmt(r.test_r2, 3)]; })} />}>
                <DotStrip unit={lat ? "s" : "tx/s"} ariaLabel="Selected-model test MAE across the four T2 draws" domain={lat ? [0, 0.04] : [0, 12]} fmt={(v) => fmt(v, lat ? 3 : 1)}
                  rows={["T2D0", "T2D1", "T2D2", "T2D3"].map((d) => { const r = selTest(d); return { id: d, label: `${d.slice(2)} · ${FAMILY[r.family].label.split(" ")[0]}`.replace(" · ", ", "), color: "var(--ev-model)", values: [{ v: r.test_mae!, label: d }] }; })} />
              </Figure>

              <Figure title="Overload: every family fails" kind="model" source={m.phase1c.source}
                caption={<>Test MAE per load level when models trained on ≤ 145 tx/s are asked about 165–200 tx/s. Tree models predict the same response at every heavy level as at 145 tx/s, so their error is roughly the gap between that response and the saturated measurements.</>}
                note={<>Best-on-validation (gradient boosting): {fmt(selTest("HEAVY").test_mae, lat ? 3 : 1)} {lat ? "s" : "tx/s"} MAE, R² {fmt(selTest("HEAVY").test_r2, 2)}. Linear regression is least bad but still unusable.</>}
                table={<Table head={["Load (tx/s)", ...FAM_ORDER.filter((f) => f !== "lookup_no_blocksize").map((f) => FAMILY[f].label)]} align={["l"]}
                  rows={heavy("hgb").map((p) => [p.x, ...FAM_ORDER.filter((f) => f !== "lookup_no_blocksize").map((f) => fmt(heavy(f).find((q) => q.x === p.x)?.y, lat ? 3 : 1))])} />}>
                <XYChart ariaLabel="Per-level overload test MAE by model family" xLabel="Held-out load level (tx/s)" yLabel={`Test MAE (${lat ? "s" : "tx/s"})`} height={280} mode="x"
                  fy={(v) => fmt(v, lat ? 3 : 0)} tipX={(v) => `${v} tx/s`} xTicks={[165, 170, 175, 180, 185, 190, 195, 200]}
                  series={[
                    ...["lookup_workload", "lookup_config", "dt", "rf"].map((f) => ({ id: f, label: FAMILY[f].label, color: "var(--c-reference)", width: 1.25, marker: false, opacity: 0.7, points: heavy(f) })),
                    { id: "linear", label: FAMILY.linear.label, color: "var(--b-5)", points: heavy("linear"), endLabel: true },
                    { id: "hgb", label: "Gradient boosting (selected)", color: "var(--ink)", points: heavy("hgb"), endLabel: true },
                  ]} />
              </Figure>

              <section className="page-section">
                <h2>Would the model have chosen better configurations?</h2>
                <p className="read">
                  A retrospective check on the same grid: at each test level, pick the configuration the frozen model ranks best, and compare its
                  observed outcome with a static choice tuned on training levels (block 10, 3 orderers for throughput). Descriptive only:
                  one observation per cell cannot establish noise or optimality.
                </p>
                <Table caption={`E6 decision analysis, throughput objective. Source ${fileName(m.e6.report ?? "E6_decisions_by_level.csv")}.`}
                  head={["Level (tx/s)", "Stratum", "Model choice", "Static choice", "Observed difference (tx/s)", "Rank correlation"]} align={["l", "l", "l", "l"]}
                  rows={m.e6.decisions.filter((r) => r.objective === "Throughput").map((r) => [r.level, String(r.stratum), `B ${r.model_block}, ${r.model_orderers} ord.`, `B ${r.static_block}, ${r.static_orderers} ord.`,
                    fmt(r.paired_diff_delta as number, 1), fmt(m.e6.spearman.find((s) => s.objective === "Throughput" && s.level === r.level)?.spearman_rho as number, 2)])} />
                <Callout title="Result">
                  At every heavy level the model picked the same configuration as the static baseline, so the difference is zero. For latency the
                  sign of the difference flips between levels. There is no demonstrated decision advantage from the model.
                </Callout>
              </section>

              <section className="page-section">
                <h2>Limitations</h2>
                <ul className="read">
                  <li>These are offline accuracies on a grid with one observation per cell, from another deployment. They are not live accuracies.</li>
                  <li>R² is inflated by the wide spread of the targets; MAE is the primary metric.</li>
                  <li>T3 "orderer transfer" brackets the trained counts (train 3 and 7, test 5 and 9). It is interpolation between neighbours, not an unseen topology.</li>
                  <li>No calibration, prediction interval or run-to-run claim is possible from HFBTP.</li>
                </ul>
                <Tech summary="A separate, smaller baseline ladder in the repository">
                  <p>{m.repo_baselines.note}</p>
                  <Table head={Object.keys(m.repo_baselines.rows[0]).slice(0, 7)} align={["l", "l"]} rows={m.repo_baselines.rows.map((r) => Object.values(r).slice(0, 7).map((v) => typeof v === "number" ? fmt(v, 3) : String(v)))} />
                  <p>Source <span className="hash">{fileName(m.repo_baselines.source)}</span></p>
                </Tech>
              </section>
            </>
          );
        }}
      </Await>
      <Pager />
    </div>
  );
}
