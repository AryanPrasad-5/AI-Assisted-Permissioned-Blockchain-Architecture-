import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { FileText } from "lucide-react";
import { Await2 } from "../components/Await";
import { DecisionLedger } from "../components/DecisionLedger";
import { Ev } from "../components/Evidence";
import { SiteFooter, SiteHeader, SITE } from "../components/Layout";
import { XYChart } from "../components/charts";
import type { LiveSeries, M2 } from "../lib/data";
import { BCOLOR, fmt } from "../lib/format";

const STAGES = [
  { name: "Workload trace", mode: "Historical replay", body: "Arrival times from two days of public Ethereum mempool data (Flashbots, 1,969,790 transactions). Only the timing is reused, to pace synthetic Fabric writes. Ethereum transactions never reach Fabric.", to: "/research/forecasting" },
  { name: "Forecast", mode: "Offline, then frozen", body: "Ten forecasters were benchmarked on a held-out day. The frozen 60-second EWMA forecaster beat persistence by 17.7% in MAE and runs live inside the Predictive controller.", to: "/research/forecasting" },
  { name: "Performance model", mode: "Offline", body: "Seven model families predicted Fabric throughput and latency on the public HFBTP grid. They interpolate well and fail beyond the trained load. The live controller instead uses a table measured on our own testbed.", to: "/research/models" },
  { name: "Controllers", mode: "Live, closed loop", body: "Static, Reactive and Predictive controllers choose the block size B once a minute. Any change goes through one actuator that verifies it on the ledger.", to: "/research/controllers" },
  { name: "Fabric testbed", mode: "Live, measured", body: "Fabric v2.5.16 with three Raft orderers and two peers, driven by Caliper on one laptop. Every transaction is reconciled against the ledger after each run.", to: "/research/controllers" },
];

function Pipeline() {
  const [i, setI] = useState(3);
  const s = STAGES[i];
  return (
    <div>
      <div className="pipeline" role="tablist" aria-label="Research pipeline">
        {STAGES.map((st, k) => (
          <button key={st.name} role="tab" aria-selected={k === i} aria-controls="pipeline-detail" id={`stage-${k}`} onClick={() => setI(k)}
            onKeyDown={(e) => { if (e.key === "ArrowRight") setI((k + 1) % STAGES.length); if (e.key === "ArrowLeft") setI((k + STAGES.length - 1) % STAGES.length); }}>
            <span className="step">Stage {k + 1}</span>
            <span className="name">{st.name}</span>
            <span className="mode">{st.mode}</span>
          </button>
        ))}
      </div>
      <div className="pipeline-detail" id="pipeline-detail" role="tabpanel" aria-labelledby={`stage-${i}`}>
        <p className="read" style={{ fontSize: "var(--t-md)" }}>{s.body}</p>
        <div><Link to={s.to} className="btn btn-quiet btn-sm">Open {s.name.toLowerCase()}</Link></div>
      </div>
    </div>
  );
}

function X1Mini({ live }: { live: LiveSeries }) {
  const series = [2, 5, 10].map((b) => ({
    id: `b${b}`, label: `B = ${b}`, color: BCOLOR[b], endLabel: true,
    points: live.x1.rungs.filter((r) => r.B === b).map((r) => ({ x: r.rate_tps, y: r.p95_ms })),
  }));
  return (
    <XYChart series={series} ariaLabel="X1: p95 latency against offered rate for block sizes 2, 5 and 10. B = 2 is lowest at every rate."
      xLabel="Offered rate (tx/s)" yLabel="p95 latency (ms, log scale)" yLog yDomain={[30, 2400]} yTicks={[50, 100, 200, 500, 1000, 2000]}
      xTicks={[5, 10, 20, 30, 40, 50, 60]} height={260} fy={(v) => fmt(v, 0)} tipX={(v) => `${v} tx/s`} />
  );
}

export default function Landing() {
  const [animate, setAnimate] = useState(false);
  useEffect(() => { document.title = `${SITE.name}: block-size control on Hyperledger Fabric`; setAnimate(true); }, []);
  return (
    <>
      <a className="skip" href="#main">Skip to content</a>
      <SiteHeader />
      <main id="main">
        <Await2<M2, LiveSeries> files={["data/m2.json", "data/live_series.json"]} height={420}>
          {(m2, live) => {
            const a = m2.arms_recomputed;
            return (
              <>
                <div className="wrap">
                  <section className="hero" aria-labelledby="hero-title">
                    <div>
                      <h1 id="hero-title">Three controllers made 90 decisions. Every one chose B&nbsp;=&nbsp;2.</h1>
                      <p className="lede">
                        We compared static, reactive and predictive block-size control live on a Hyperledger Fabric testbed,
                        under a protocol fixed before the first run. The three controllers tied. The constant-rate scans show why:
                        the smallest block was fastest at every load we could generate, so there was no decision to make.
                      </p>
                      <div className="hero-actions">
                        <Link className="btn btn-primary" to="/research">Explore the research</Link>
                        <Link className="btn btn-quiet" to="/research/paper"><FileText size={16} aria-hidden="true" /> Read the paper</Link>
                      </div>
                    </div>
                    <figure style={{ margin: 0 }}>
                      <DecisionLedger m2={m2} animate={animate} />
                      <figcaption className="figure-caption" style={{ display: "flex", gap: 10, alignItems: "flex-start", flexWrap: "wrap" }}>
                        <Ev kind="measured" />
                        <span>Block size chosen at each one-minute decision in the 12 M2 runs. The bottom rows are a Static B&nbsp;=&nbsp;10 reference arm, run as a positive control and never ranked.</span>
                      </figcaption>
                    </figure>
                  </section>
                </div>

                <div className="wrap">
                  <section className="section" aria-labelledby="q-title">
                    <div className="section-head">
                      <h2 id="q-title">The question</h2>
                      <div className="read">
                        <p>
                          A Fabric ordering service cuts a block when <em>B</em> transactions are waiting or a 2-second timeout expires.
                          Small blocks shorten the wait at low load. Large blocks may cope better at high load. Because the best <em>B</em> seems to
                          depend on load, adaptive control is an obvious idea: react to measured latency, or forecast the load and choose ahead of it.
                        </p>
                        <p>
                          Published evaluations often compare an adaptive controller with a default setting, not with the best fixed setting for the
                          same workload, and treat forecast accuracy as if it were control benefit. We asked the plain version of the question: under
                          equivalent workloads, how do static, reactive and predictive control compare in throughput, latency and reliability?
                        </p>
                      </div>
                    </div>
                  </section>

                  <section className="section" aria-labelledby="a-title">
                    <div className="section-head">
                      <h2 id="a-title">What we built</h2>
                      <p className="read">A complete pipeline from a historical workload trace to a live control loop. Select a stage to see what it does and whether it ran offline or live.</p>
                    </div>
                    <Pipeline />
                  </section>

                  <section className="section" aria-labelledby="f-title">
                    <div className="section-head">
                      <h2 id="f-title">What we found</h2>
                      <p className="read">Each finding links to the records behind it. Margins and decision rules were fixed before any run.</p>
                    </div>
                    <div className="findings">
                      <article className="finding">
                        <div className="figure-n num">{fmt(a.static_b2.mean_ms)}, {fmt(a.reactive.mean_ms)}, {fmt(a.predictive_live.mean_ms)} <small>ms</small></div>
                        <div>
                          <h3>No meaningful difference between the controllers</h3>
                          <p className="muted">Mean latency for Static, Reactive and Predictive. Every pair of runs differed by less than the 25 ms margin set in advance, on mean and p95 latency, throughput and failures. With three runs per arm, no significance claim is possible.</p>
                        </div>
                        <div className="meta"><Ev kind="measured" /><Link to="/research/controllers">Controller comparison</Link></div>
                      </article>
                      <article className="finding">
                        <div><X1Mini live={live} /></div>
                        <div>
                          <h3>There was no decision to make</h3>
                          <p className="muted">In constant-rate scans from 5 to 60 tx/s, B = 2 had the lowest p95 latency at every rate, 4.2 to 8.2 times below B = 10. When one setting wins everywhere, a forecast cannot change the choice.</p>
                        </div>
                        <div className="meta"><Ev kind="measured" /><Link to="/research/paper">Scan design and results</Link></div>
                      </article>
                      <article className="finding">
                        <div className="figure-n num">{fmt(a.static_b10.mean_ms / a.static_b2.mean_ms)}&times; <small>slower</small></div>
                        <div>
                          <h3>The baseline mattered more than the controller</h3>
                          <p className="muted">On the same trace, a Static B = 10 arm had {fmt(a.static_b10.mean_ms)} ms mean latency against {fmt(a.static_b2.mean_ms)} ms for B = 2. A controller judged against B = 10 would have been credited with a gain that a well-chosen fixed setting already gives.</p>
                        </div>
                        <div className="meta"><Ev kind="measured" /><Link to="/research/controllers">Reference arm</Link></div>
                      </article>
                      <article className="finding">
                        <div className="figure-n num">9.19 <small>vs 10.49 tx/s</small></div>
                        <div>
                          <h3>A better forecast changed nothing</h3>
                          <p className="muted">The live EWMA forecast beat persistence (mean absolute error per decision). It still never changed a decision, because B = 2 ranked first at every forecast. Forecast skill and decision value are different quantities.</p>
                        </div>
                        <div className="meta"><Ev kind="measured" /><Link to="/research/forecasting">Forecasting</Link></div>
                      </article>
                      <article className="finding">
                        <div className="figure-n num">34.8 <small>tx/s MAE</small></div>
                        <div>
                          <h3>Offline models interpolate, and fail beyond their data</h3>
                          <p className="muted">On the public HFBTP grid, held-out block sizes and orderer counts were predicted within 1.2 to 1.5 tx/s, and held-out load levels within 2.9 to 10.5 tx/s depending on the draw. Asked about heavier load than they were trained on, every family failed (R² −8.5). No HFBTP model drove a live decision.</p>
                        </div>
                        <div className="meta"><Ev kind="model" /><Link to="/research/models">Model evaluation</Link></div>
                      </article>
                    </div>
                  </section>

                  <section className="section" aria-labelledby="c-title">
                    <div className="section-head">
                      <h2 id="c-title">Contributions</h2>
                      <ol className="list-plain contrib read" style={{ fontSize: "var(--t-md)" }}>
                        <li><span><strong>A verified live control loop.</strong> Three controllers share one actuator that applies a channel-configuration update and confirms it with Fabric's own verifier, the ledger's configuration blocks and a reconciliation of every transaction.</span></li>
                        <li><span><strong>A pre-registered, block-randomised comparison against a tuned static baseline,</strong> reported in full, including the null result, run-level spread and one unexplained transient.</span></li>
                        <li><span><strong>A decision-headroom analysis</strong> that explains, with constant-rate scans and a batching relation, why forecast skill could not become control benefit here.</span></li>
                        <li><span><strong>A plain account of the offline components:</strong> what forecasting skill and HFBTP model accuracy support, and the fact that transfer from HFBTP to our testbed was not established.</span></li>
                      </ol>
                    </div>
                  </section>

                  <section className="section" aria-labelledby="m-title">
                    <div className="section-head">
                      <h2 id="m-title">How it was tested</h2>
                      <p className="read">Each live series was designed and frozen, with SHA-256 hashes of the protocol and code, before it ran. A run could stop the series only for an integrity failure, never for a performance result, and nothing was retried.</p>
                    </div>
                    <ol className="timeline">
                      <li><span className="what">cl01–cl12</span><span className="small muted">Static, Reactive and stored-decision Predictive at B ∈ {"{10, 40}"}. 11 valid runs.</span></li>
                      <li><span className="what">X0 scan</span><span className="small muted">Does B = 40 ever beat B = 10? No, up to 60 tx/s.</span></li>
                      <li><span className="what">X1 scan</span><span className="small muted">B ∈ {"{2, 5, 10}"} at 5–60 tx/s. B = 2 lowest everywhere.</span></li>
                      <li><span className="what">ca01</span><span className="small muted">Four live block-size changes, each verified on the ledger.</span></li>
                      <li><span className="what">M2</span><span className="small muted">12 block-randomised runs, 190,704 transactions, 0 failed.</span></li>
                    </ol>
                  </section>

                  <section className="section" aria-labelledby="t-title">
                    <div className="section-head">
                      <h2 id="t-title">Technology</h2>
                      <p className="read">The research system, and the separate site you are reading.</p>
                    </div>
                    <dl className="stack-list">
                      <div><dt>Blockchain</dt><dd>Hyperledger Fabric v2.5.16, 3 Raft orderers, 2 peers, BatchTimeout 2 s</dd></div>
                      <div><dt>Load generation</dt><dd>Hyperledger Caliper 0.6.0, co-located on one laptop under WSL2 and Docker</dd></div>
                      <div><dt>Workload data</dt><dd>Flashbots Mempool Dumpster, two days</dd></div>
                      <div><dt>Performance data</dt><dd>HFBTP: 24,687 Caliper measurements on Fabric 2.3 (LearningChain, IEEE TNSM 2024)</dd></div>
                      <div><dt>Analysis</dt><dd>Python 3.14, scikit-learn 1.9.1, pre-registered protocols with SHA-256 freezes</dd></div>
                      <div><dt>This site</dt><dd>React, TypeScript, Vite, d3-scale. Static files only; no backend.</dd></div>
                    </dl>
                  </section>

                  <section className="cta-band">
                    <div>
                      <h2 style={{ maxWidth: "22ch" }}>Every number here traces to a stored record.</h2>
                      <p className="muted" style={{ marginTop: 8 }}>The workspace has the full controller comparison, the model evaluation and the paper.</p>
                    </div>
                    <div className="hero-actions">
                      <Link className="btn btn-primary" to="/research">Explore the research</Link>
                      <a className="btn btn-quiet" href={SITE.github} rel="noreferrer">Source repository</a>
                    </div>
                  </section>
                </div>
              </>
            );
          }}
        </Await2>
      </main>
      <SiteFooter />
    </>
  );
}
