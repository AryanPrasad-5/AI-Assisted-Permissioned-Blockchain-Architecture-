import { Link } from "react-router-dom";
import { Await } from "../components/Await";
import { Callout, Ev, Figure } from "../components/Evidence";
import { PageHead, Pager } from "../components/Layout";
import { DotStrip, Table } from "../components/charts";
import type { M2 } from "../lib/data";
import { ARMS, ARM_ORDER, fmt, int } from "../lib/format";

export default function Overview() {
  return (
    <div className="page">
      <PageHead crumb="Research" title="Research overview"
        summary="We built a forecasting and prediction pipeline and a verified live control loop for Fabric's block size, then ran a pre-registered comparison of static, reactive and predictive control. They tied, because one block size was best at every load we could reach." />
      <Await<M2> file="data/m2.json">
        {(m2) => {
          const a = m2.arms_recomputed;
          const compared = m2.decisions.filter((d) => ARMS[d.arm].compared);
          return (
            <>
              <dl className="kv" aria-label="M2 at a glance">
                <div><dt>Valid runs</dt><dd className="num">{m2.runs.filter((r) => r.verdict === "VALID").length} <small>of {m2.runs.length}</small></dd></div>
                <div><dt>Measured transactions</dt><dd className="num">{int(m2.runs.reduce((s, r) => s + r.success, 0))}</dd></div>
                <div><dt>Failed transactions</dt><dd className="num">{m2.runs.reduce((s, r) => s + r.failed, 0)}</dd></div>
                <div><dt>Controller decisions</dt><dd className="num">{compared.length} <small>all B = 2</small></dd></div>
                <div><dt>Configuration changes</dt><dd className="num">{m2.runs.filter((r) => ARMS[r.arm_label].compared).reduce((s, r) => s + r.config_changes, 0)}</dd></div>
              </dl>
              <p className="small muted" style={{ marginTop: 8 }}><Ev kind="measured" /> M2 comparison.</p>

              <section className="page-section">
                <h2>What the comparison found</h2>
                <p className="read">Under the rule fixed before the runs, the three controllers are equivalent within the smallest effect of interest (25 ms for latency, 2% for throughput). Points are runs; the bar is the arm mean.</p>
                <Figure title="Mean latency per run" kind="measured" source={m2.sources.runs}
                  caption={<>Static, Reactive and Predictive overlap. Reactive's spread comes from one run, cc12, whose transient had no controller cause. The <b>reference arm</b> is shown greyed and is not ranked.</>}
                  note={<>Run is the unit of analysis. With 3 runs per arm the smallest attainable two-sided permutation p is 0.10, so no significance is claimed.</>}
                  table={<Table head={["Arm", "Runs", "Mean (ms)", "p95 (ms)", "Throughput (tx/s)"]} align={["l"]}
                    rows={ARM_ORDER.map((k) => [ARMS[k].label, a[k].runs, `${fmt(a[k].mean_ms)} ± ${fmt(a[k].mean_sd)}`, `${fmt(a[k].p95_ms)} ± ${fmt(a[k].p95_sd)}`, fmt(a[k].throughput_tps, 2)])} />}>
                  <DotStrip unit="ms" ariaLabel="Run-level mean latency by arm" domain={[0, 220]}
                    rows={ARM_ORDER.map((k) => ({ id: k, label: ARMS[k].short, color: ARMS[k].color, muted: !ARMS[k].compared, mean: a[k].mean_ms,
                      values: m2.runs.filter((r) => r.arm_label === k).map((r) => ({ v: r.mean_ms, label: r.run_id })) }))} />
                </Figure>
                <Callout title="How to read this">
                  The lowest p95 point estimate was Predictive's, 81.3 ms against 82.3 ms for Static. That 1.0 ms gap is smaller than the
                  run-to-run spread (2–8 ms) and 25 times below the margin. It is not evidence of an advantage. All three controllers ran
                  the same configuration for the whole experiment.
                </Callout>
              </section>
            </>
          );
        }}
      </Await>

      <section className="page-section">
        <h2>Where to go next</h2>
        <div className="cols-3" style={{ marginTop: 16 }}>
          <div><h3>Mechanism</h3><p className="muted small" style={{ margin: "6px 0 10px" }}>How a trace becomes a decision, and what ran live.</p><Link to="/research/architecture">System architecture</Link></div>
          <div><h3>Evidence</h3><p className="muted small" style={{ margin: "6px 0 10px" }}>Run-level data, decisions and statistics for every arm.</p><Link to="/research/controllers">Controller comparison</Link></div>
          <div><h3>Offline models</h3><p className="muted small" style={{ margin: "6px 0 10px" }}>How well models learned from HFBTP predict unseen configurations, and where they fail.</p><Link to="/research/models">Model evaluation</Link></div>
        </div>
      </section>
      <Pager />
    </div>
  );
}
