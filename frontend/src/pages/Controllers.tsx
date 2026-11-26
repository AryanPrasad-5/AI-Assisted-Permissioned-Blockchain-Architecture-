import { useState } from "react";
import { Await2 } from "../components/Await";
import { Callout, Ev, Figure, Status, Tech } from "../components/Evidence";
import { PageHead, Pager } from "../components/Layout";
import { DotStrip, Table, XYChart } from "../components/charts";
import type { Arm, LiveSeries, M2 } from "../lib/data";
import { ARMS, ARM_ORDER, BCOLOR, fileName, fmt, signed } from "../lib/format";

function Flow({ steps, color }: { steps: string[]; color: string }) {
  return (
    <ol className="flow" style={{ ["--flow-c" as string]: color }}>
      {steps.map((s) => <li key={s}>{s}</li>)}
    </ol>
  );
}

function HowTheyWork() {
  return (
    <div className="cols-3" style={{ marginTop: 16 }}>
      <div>
        <h3 style={{ display: "flex", gap: 8, alignItems: "center" }}><i className="swatch" style={{ background: ARMS.static_b2.color }} />Static</h3>
        <p className="small muted" style={{ margin: "6px 0 12px" }}>Holds one block size for the whole run. In M2 that is B = 2, the best setting found in the X1 scan.</p>
        <Flow color={ARMS.static_b2.color} steps={["Start at B = 2", "Hold B every minute", "Never actuates"]} />
        <p className="xs muted" style={{ marginTop: 8 }}>Inputs: none.</p>
      </div>
      <div>
        <h3 style={{ display: "flex", gap: 8, alignItems: "center" }}><i className="swatch" style={{ background: ARMS.reactive.color }} />Reactive</h3>
        <p className="small muted" style={{ margin: "6px 0 12px" }}>Watches the last minute's mean latency and steps B up after two slow minutes, down after two fast ones.</p>
        <Flow color={ARMS.reactive.color} steps={["Read last round's mean latency", "Two rounds ≥ 243.09 ms: step 2 → 5", "Two rounds ≤ 194.47 ms: step back", "Otherwise hold"]} />
        <p className="xs muted" style={{ marginTop: 8 }}>Inputs: measured latency only. Threshold = twice the worst healthy B = 2 mean in X1 (121.5 ms at 5 tx/s).</p>
      </div>
      <div>
        <h3 style={{ display: "flex", gap: 8, alignItems: "center" }}><i className="swatch" style={{ background: ARMS.predictive_live.color }} />Predictive</h3>
        <p className="small muted" style={{ margin: "6px 0 12px" }}>Forecasts the next minute's load, predicts each block size's outcome, and picks the best utility.</p>
        <Flow color={ARMS.predictive_live.color} steps={["Forecast rate λ̂ with frozen EWMA", "Predict throughput X̂(B) and latency L̂(B) from the X1 table", "Score U(B) = X̂ − 0.01·L̂ − max(0, λ̂ − X̂)", "Switch only if U improves and 3 decisions since the last change"]} />
        <p className="xs muted" style={{ marginTop: 8 }}>Inputs: arrival counts and the measured surface. Outside the table it abstains and holds.</p>
      </div>
    </div>
  );
}

function DecisionExplorer({ m2 }: { m2: M2 }) {
  const reactiveRuns = m2.runs.filter((r) => r.arm_label === "reactive").map((r) => r.run_id);
  const predRuns = m2.runs.filter((r) => r.arm_label === "predictive_live").map((r) => r.run_id);
  const [pr, setPr] = useState(predRuns[0]);
  const dec = (id: string) => m2.decisions.filter((d) => d.run_id === id).sort((a, b) => a.index - b.index);
  const reactSeries = reactiveRuns.map((id, i) => ({
    id, label: id, color: ARMS.reactive.color, opacity: [1, 0.75, 0.5][i], marker: true, width: 1.5,
    points: dec(id).map((d) => ({ x: d.index + 1, y: d.reactive?.observed_latency_ms ?? null })),
  }));
  const high = m2.decisions.find((d) => d.reactive)?.reactive;
  const pd = dec(pr);
  const utilSeries = [2, 5, 10].map((b) => ({
    id: `u${b}`, label: `U(B = ${b})`, color: BCOLOR[b], endLabel: true,
    points: pd.map((d) => ({ x: d.index + 1, y: d.predictive?.candidates.find((c) => c.B === b)?.utility ?? null })),
  }));
  const maxReact = Math.max(...m2.decisions.filter((d) => d.reactive).map((d) => d.reactive!.observed_latency_ms));
  return (
    <div className="cols-2">
      <Figure title="What Reactive saw" kind="measured" source={m2.sources.decisions}
        caption={<>Mean latency of the round each Reactive decision read, for its three runs. The trigger is {fmt(high?.high_ms, 2)} ms; the highest value seen was {fmt(maxReact, 1)} ms (run cc12). Reactive never had a reason to move.</>}
        table={<Table head={["Decision", ...reactiveRuns]} align={["l"]} rows={Array.from({ length: 10 }, (_, i) => [i + 1, ...reactiveRuns.map((id) => fmt(dec(id)[i]?.reactive?.observed_latency_ms, 1))])} />}>
        <XYChart series={reactSeries} ariaLabel="Observed latency at each Reactive decision against its thresholds" xLabel="Decision" yLabel="Observed mean latency of last round (ms)"
          height={260} yDomain={[0, 280]} xTicks={[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]} tipX={(v) => `Decision ${v}`} fy={(v) => fmt(v, 0)}
          refY={[{ y: high?.high_ms ?? 243.09, label: "step-up trigger" }, { y: high?.low_ms ?? 194.47, label: "step-down" }]} />
      </Figure>
      <Figure title="What Predictive weighed" kind="measured" source={m2.sources.decisions}
        caption={<>Utility the controller computed for each candidate at each decision of run {pr}, from its logged forecast and the X1 surface. B = 2 scored highest every time, so the controller held.</>}
        note={<div className="segmented" role="radiogroup" aria-label="Predictive run">{predRuns.map((id) => <button key={id} role="radio" aria-checked={pr === id} onClick={() => setPr(id)}>{id}</button>)}</div>}
        table={<Table head={["Decision", "Forecast (tx/s)", "U(2)", "U(5)", "U(10)", "Chosen"]} align={["l"]}
          rows={pd.map((d) => [d.index + 1, fmt(d.predictive?.forecast_tps, 2), ...[2, 5, 10].map((b) => fmt(d.predictive?.candidates.find((c) => c.B === b)?.utility, 3)), `B = ${d.target_B}`])} />}>
        <XYChart series={utilSeries} ariaLabel="Candidate utilities at each Predictive decision" xLabel="Decision" yLabel="Utility U(B), higher is better" height={260}
          xTicks={[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]} tipX={(v) => `Decision ${v}`} fy={(v) => fmt(v, 1)} zeroBased={false} />
      </Figure>
    </div>
  );
}

function SegmentTrace({ m2 }: { m2: M2 }) {
  const first = m2.runs[0].run_id;
  const rate = m2.segments.filter((s) => s.run_id === first).map((s) => ({ x: s.segment + 1, y: s.rate_tps }));
  const armMean = (arm: Arm) => Array.from({ length: 10 }, (_, i) => {
    const seg = m2.segments.filter((s) => s.arm_label === arm && s.segment === i);
    return { x: i + 1, y: seg.reduce((a, s) => a + s.mean_ms, 0) / seg.length };
  });
  return (
    <Figure title="Offered load and latency, minute by minute" kind="measured" source={m2.sources.segments}
      caption={<>Top: the offered rate, identical in every run. Bottom: arm-mean latency per 60-second segment. The three compared arms track the load together; the bump in segments 3–4 (indices 2–3 in the records) is one Reactive run, cc12, which made no configuration change. The reference arm is grey.</>}
      table={<Table head={["Segment", "Offered (tx/s)", ...ARM_ORDER.map((a) => ARMS[a].short + " (ms)")]} align={["l"]}
        rows={rate.map((r, i) => [i + 1, fmt(r.y, 1), ...ARM_ORDER.map((a) => fmt(armMean(a)[i].y, 1))])} />}>
      <XYChart series={[{ id: "rate", label: "Offered rate", color: "var(--d-actual)", points: rate }]} ariaLabel="Offered rate per segment" xLabel="" yLabel="Offered rate (tx/s)" height={140}
        xTicks={[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]} fy={(v) => fmt(v, 0)} tipX={(v) => `Segment ${v}`} />
      <div style={{ height: 12 }} />
      <XYChart ariaLabel="Arm-mean latency per segment" xLabel="Segment (60 s each)" yLabel="Mean latency (ms, log scale)" height={260} yLog yDomain={[25, 400]} yTicks={[30, 50, 100, 200, 400]}
        xTicks={[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]} fy={(v) => fmt(v, 0)} tipX={(v) => `Segment ${v}`}
        series={ARM_ORDER.map((a) => ({ id: a, label: ARMS[a].label, color: ARMS[a].color, points: armMean(a), dash: a === "static_b10" ? "5 4" : undefined, width: a === "static_b10" ? 1.5 : 2 }))} />
    </Figure>
  );
}

export default function Controllers() {
  const [metric, setMetric] = useState<"mean_ms" | "p95_ms" | "throughput_tps">("p95_ms");
  return (
    <div className="page">
      <PageHead crumb="Research" title="Controller comparison"
        summary="Static, Reactive and Predictive control ran on the same trace, network and actuator in 12 block-randomised runs. All three chose B = 2 at every decision and never changed the configuration. Their results are equivalent within the margins fixed beforehand." />
      <Await2<M2, LiveSeries> files={["data/m2.json", "data/live_series.json"]} height={420}>
        {(m2, live) => {
          const a = m2.arms_recomputed;
          const unit = metric === "throughput_tps" ? "tx/s" : "ms";
          const label = { mean_ms: "Mean latency", p95_ms: "p95 latency", throughput_tps: "Committed throughput" }[metric];
          const pairsFor = (cmp: string) => m2.pairwise.filter((p) => p.comparison === cmp);
          return (
            <>
              <Callout title="Verdict: no meaningful difference">
                {m2.verdict.text}
              </Callout>

              <section className="page-section">
                <h2>How each controller decides</h2>
                <p className="read">All three decide once a minute from data up to the end of the previous minute, and share one actuator, one menu (B ∈ {"{2, 5, 10}"}) and one set of constraints. The three compared controllers all start at B = 2.</p>
                <HowTheyWork />
              </section>

              <section className="page-section">
                <h2>Inside the decisions</h2>
                <p className="read">Every decision is logged with what the controller observed and, for Predictive, the utility of each candidate. These two views explain the tie.</p>
                <DecisionExplorer m2={m2} />
              </section>

              <section className="page-section">
                <SegmentTrace m2={m2} />
              </section>

              <section className="page-section">
                <h2>Run-level results</h2>
                <div className="toolbar">
                  <div><span className="lbl">Metric</span><div className="segmented" role="radiogroup" aria-label="Metric">
                    {(["p95_ms", "mean_ms", "throughput_tps"] as const).map((k) => <button key={k} role="radio" aria-checked={metric === k} onClick={() => setMetric(k)}>{{ mean_ms: "Mean latency", p95_ms: "p95 latency", throughput_tps: "Throughput" }[k]}</button>)}
                  </div></div>
                </div>
                <Figure title={`${label} per run`} kind="measured" source={m2.sources.runs}
                  caption={<>Dots are runs (3 per arm), bars are arm means. Arms are listed in a fixed order, not by result. {metric === "throughput_tps" ? "Throughput is arrival-limited: every arm was offered the same 26.49 tx/s." : "The Static B = 10 reference is shown for scale and is never ranked."}</>}
                  note={<>Margin (SESOI): {metric === "throughput_tps" ? "2% of the better arm (≈ 0.52 tx/s)" : "max(10% of the better arm, 25 ms) = 25 ms"}. A difference is meaningful only if it exceeds the margin and every run of one arm is worse than every run of the other.</>}
                  table={<Table head={["Run", "Arm", "Mean (ms)", "p50", "p95", "p99", "Throughput (tx/s)", "Changes"]} align={["l", "l"]}
                    rows={m2.runs.map((r) => [r.run_id, ARMS[r.arm_label].label, fmt(r.mean_ms, 2), r.p50_ms, r.p95_ms, r.p99_ms, fmt(r.throughput_tps, 3), r.config_changes])} />}>
                  <DotStrip unit={unit} ariaLabel={`${label} per run by arm`} fmt={(v) => fmt(v, metric === "throughput_tps" ? 2 : 1)}
                    domain={metric === "throughput_tps" ? [25.6, 26.0] : metric === "p95_ms" ? [0, 480] : [0, 220]}
                    rows={ARM_ORDER.map((k) => ({ id: k, label: ARMS[k].short, color: ARMS[k].color, muted: !ARMS[k].compared, mean: (a[k] as unknown as Record<string, number>)[metric],
                      values: m2.runs.filter((r) => r.arm_label === k).map((r) => ({ v: r[metric], label: r.run_id })) }))} />
                </Figure>
                <Table caption="Arm summary, mean ± sd over 3 runs (recomputed from run_records.csv; equals the paper's table)."
                  head={["Metric", ...ARM_ORDER.map((k) => ARMS[k].label)]} align={["l"]}
                  rows={[
                    ["Runs (valid)", ...ARM_ORDER.map((k) => `${a[k].runs} (${m2.runs.filter((r) => r.arm_label === k && r.verdict === "VALID").length})`)],
                    ["Throughput (tx/s)", ...ARM_ORDER.map((k) => fmt(a[k].throughput_tps, 2))],
                    ["Mean latency (ms)", ...ARM_ORDER.map((k) => `${fmt(a[k].mean_ms)} ± ${fmt(a[k].mean_sd)}`)],
                    ["p95 latency (ms)", ...ARM_ORDER.map((k) => `${fmt(a[k].p95_ms)} ± ${fmt(a[k].p95_sd)}`)],
                    ["p99 latency (ms)", ...ARM_ORDER.map((k) => fmt(a[k].p99_ms))],
                    ["Failed transactions", ...ARM_ORDER.map((k) => a[k].failed)],
                    ["Configuration changes", ...ARM_ORDER.map((k) => a[k].config_changes)],
                    ["Decisions selecting", ...ARM_ORDER.map((k) => `B = ${k === "static_b10" ? 10 : 2}: ${m2.decisions.filter((d) => d.arm === k).length}`)],
                  ]} />
              </section>

              <section className="page-section">
                <h2>Pre-registered pairwise verdicts</h2>
                <p className="read">Differences of arm means, with the smallest and largest difference over all 3 × 3 cross-arm run pairs. With three runs per arm the smallest attainable two-sided permutation p is 0.10, so p-values are shown for completeness, not as evidence.</p>
                <Table head={["Comparison", "Metric", "Difference", "Margin", "Run-pair range", "Permutation p", "Verdict"]} align={["l", "l"]}
                  rows={["reactive_vs_static_b2", "predictive_live_vs_static_b2", "predictive_live_vs_reactive", "static_b10_vs_static_b2"].flatMap((c) =>
                    pairsFor(c).filter((p) => p.metric !== "failure_rate").map((p) => {
                      const [x, y] = c.split("_vs_");
                      const lab = `${ARMS[x as Arm]?.short ?? x} − ${ARMS[y as Arm]?.short ?? y}`;
                      const d = p.metric === "throughput_tps" ? 4 : 1;
                      return [lab, { mean_ms: "Mean (ms)", p95_ms: "p95 (ms)", throughput_tps: "Throughput (tx/s)" }[p.metric] ?? p.metric, signed(p.difference_of_means, d), fmt(p.sesoi, 2),
                        `${signed(p.min_pair_diff, d)} to ${signed(p.max_pair_diff, d)}`, fmt(p.exact_permutation_p_two_sided, 1),
                        p.verdict.startsWith("MEANINGFUL") ? <Status kind="neutral"><b style={{ color: "var(--ink)" }}>Meaningful (B = 10 worse)</b></Status> : <Status kind="neutral">Equivalent within margin</Status>];
                    }))} />
                <p className="small muted" style={{ marginTop: 8 }}>Failure rate: 0 failed transactions in every run, so every failure-rate comparison is 0 against a 0.5-point margin. Source <span className="hash">{fileName(m2.sources.pairwise)}</span></p>
              </section>

              <section className="page-section">
                <h2>Predictions written before the runs</h2>
                <Table head={["", "Prediction", "Measured", "Status"]} align={["l", "l", "l", "l"]}
                  rows={m2.hypotheses.map((h) => [h.id, h.prediction, <span className="muted">{h.measured}</span>, <Status kind="ok">Not falsified</Status>])} />
                <p className="small muted" style={{ marginTop: 8 }}>The X1 surface predicted run means of 41.5 ms (B = 2) and 195.6 ms (B = 10); measured values were 42.9 and 201.6 ms. This is a consistency check on the same machine, not an independent validation.</p>
              </section>

              <section className="page-section">
                <h2>Configuration evidence</h2>
                <div className="cols-2">
                  <div className="read" style={{ fontSize: "var(--t-md)" }}>
                    <p>No compared run submitted a configuration update: 0 changes and 0 configuration blocks on the ledger in each of the nine runs. That the actuator works was proven separately, before M2.</p>
                    <p>In ca01, a scripted sequence 2 → 5 → 10 → 5 → 2 at 10 tx/s committed four updates, each confirmed by Fabric's verifier at three stages and an independent decoder, with all {live.ca01.transactions.toLocaleString("en-GB")} transactions succeeding.</p>
                  </div>
                  <Table caption={`ca01 actuation check. Source ${fileName(live.ca01.source)}.`}
                    head={["Change", "Config block", "Actuator time (s)"]} align={["l"]}
                    rows={live.ca01.transitions.map((t) => [`B ${t.from} → ${t.to}`, t.config_block, fmt(t.actuator_s, 2)])} />
                </div>
              </section>

              <section className="page-section">
                <h2>The earlier series, and a reactive misfire</h2>
                <p className="read">Before M2, cl01–cl12 compared the controllers on a {"{10, 40}"} menu at about 10 tx/s. Static B = 10, Reactive R1 and a stored-decision Predictive arm were identical within 0.1%. Reactive R0, a deliberately uncalibrated arm, switched to B = 40 in both of its runs and made latency worse.</p>
                <Table caption={`cl01–cl12 arm means (cl04 was an infrastructure abort). Source ${fileName(live.closed_loop.sources.arms)}.`}
                  head={["Arm", "Runs", "Throughput (tx/s)", "Mean (ms)", "p95 (ms)", "Changes per run"]} align={["l"]}
                  rows={live.closed_loop.arms.map((r) => [String(r.arm), r.runs as number, fmt(r.throughput_tps as number, 3), fmt(r.mean_latency_ms as number, 1), fmt(r.p95_latency_ms as number, 1), fmt(r.config_changes_per_run as number, 0)])} />
                <Callout tone="warn" title="Why R0 misfired">
                  At B = 10 and 8.5–14 tx/s, waiting for a block to fill alone gives 350–555 ms mean latency. That crossed R0's 500 ms trigger with no
                  congestion present, so R0 moved to B = 40, where blocks cannot fill before the 2-second timeout. A latency rule only works if latency
                  separates congestion from batching wait.
                </Callout>
                <p className="small muted" style={{ marginTop: 8 }}><Ev kind="measured" /> The stored-decision Predictive arm replayed decisions computed offline and ran no forecaster during those runs. Only M2 ran live inference.</p>
              </section>

              <section className="page-section">
                <h2>What this does and does not show</h2>
                <ul className="read">
                  <li>It shows that, here, adaptation cost nothing: no change, no failure, no throughput or latency penalty beyond noise.</li>
                  <li>It does not show that the controllers are equivalent under every workload. M2 tested a regime with no decision headroom, and all three started at the best setting.</li>
                  <li>Three runs per arm, one evening, one laptop. Day-to-day variation was not sampled, and no significance claim is made.</li>
                  <li>The cc12 transient (segments 3–4, up to 89 ms) has no established cause; the run is kept as pre-registered.</li>
                </ul>
                <Tech summary="Decision rule, exactly as pre-registered">
                  <p>Margin (SESOI): max(0.1 × better arm mean, 25 ms) for latency; 2% of the better arm for throughput; 0.5 percentage points for failure rate. Meaningful = exceeds the margin and complete separation of runs. Equivalent within the margin = every cross-arm run pair differs by less than the margin. Otherwise: not distinguishable. Run order was block-randomised.</p>
                </Tech>
              </section>
            </>
          );
        }}
      </Await2>
      <Pager />
    </div>
  );
}

