import { Link } from "react-router-dom";
import { Await } from "../components/Await";
import { PageHead, SITE } from "../components/Layout";
import { fileName } from "../lib/format";

interface Refs { references: { key: string; authors: string; title: string; venue: string; year: string; doi: string; url: string }[]; source: string; manuscript_version: string }

const SECTIONS = [
  ["abstract", "Abstract"], ["problem", "Research problem"], ["objectives", "Objectives"], ["related", "Related work"],
  ["method", "Methodology"], ["design", "Experimental design"], ["results", "Results and discussion"], ["contributions", "Contributions"],
  ["limits", "Limitations"], ["threats", "Threats to validity"], ["conclusion", "Conclusion"], ["future", "Future work"], ["refs", "References"],
] as const;

export default function Paper() {
  return (
    <div className="page">
      <PageHead crumb="Research" title="Static, reactive or predictive?"
        summary={<>A pre-registered live comparison of block-size control on a Hyperledger Fabric testbed. {SITE.authors}.</>}>
        <div className="pill-row">
          <a className="btn btn-quiet btn-sm" href={SITE.github} rel="noreferrer">Source repository</a>
        </div>
      </PageHead>
      <div className="doc">
        <nav className="doc-toc" aria-label="Paper sections"><ol>{SECTIONS.map(([id, t]) => <li key={id}><a href={`#${id}`}>{t}</a></li>)}</ol></nav>
        <article className="doc-body">
          <h2 id="abstract">Abstract</h2>
          <div className="read abstract">
            <p>Adaptive configuration of permissioned blockchains is usually motivated by the expectation that a controller that forecasts load can choose a better block size than a fixed setting. We test that expectation on a live Hyperledger Fabric v2.5 testbed (three Raft orderers, two peers, Caliper) by comparing Static, Reactive and Predictive control of the ordering-service batch size B under equivalent workloads. The Predictive controller runs a genuine online pipeline: an exponentially weighted forecast of the arrival rate of a scaled historical Ethereum (Flashbots) mempool trace, a measured local performance surface, and a utility-maximising decision applied through a verified configuration actuator.</p>
            <p>A frozen, pre-registered protocol (menu B ∈ {"{2, 5, 10}"}, 15–54 tx/s, three block-randomised replicates per arm) produced 12 valid runs with 190,704 measured transactions and zero failures. All three controllers selected B = 2 at every one of their 90 decisions and applied no configuration change. Throughput was 25.93 tx/s in every arm; mean latency was 42.9, 48.4 and 44.1 ms and p95 latency 82.3, 85.0 and 81.3 ms for Static, Reactive and Predictive, all within the pre-registered margin of 25 ms. The live forecast was more accurate than persistence (MAE 9.19 vs 10.49 tx/s) but changed no decision. Earlier constant-rate scans explain why: B = 2 had the lowest p95 latency at every rate from 5 to 60 tx/s, so no decision headroom existed. A static B = 10 reference was 4.7 times slower in mean latency. Under the evaluated conditions the controllers were practically equivalent; the choice of the static baseline mattered far more than the control strategy.</p>
          </div>

          <h2 id="problem">Research problem</h2>
          <div className="read">
            <p>Fabric's ordering service cuts a block when B transactions are pending or a batch timeout expires, so block size is one of the most direct levers on latency and throughput. Because the best setting depends on load, adapting B at run time is a natural idea. Two issues limit what published evaluations can conclude: adaptive strategies are often compared with a default fixed configuration rather than the best static setting for the workload, and offline prediction accuracy is often treated as a proxy for control benefit.</p>
          </div>

          <h2 id="objectives">Objectives</h2>
          <div className="read">
            <ol>
              <li>Compare Static, Reactive and Predictive block-size control in throughput, latency and transaction reliability under equivalent workloads, against a tuned static baseline.</li>
              <li>Build the full pipeline that comparison needs: a historical workload trace, a forecasting benchmark, offline performance models, three controllers and a verified live actuator.</li>
              <li>Fix hypotheses, margins and stopping rules before each live run, and report every outcome, including null and negative results.</li>
            </ol>
          </div>

          <h2 id="related">Related work</h2>
          <div className="read">
            <p>Empirical studies have characterised how block size, endorsement policy, channels and resources affect Fabric performance, and analytical models of its latency exist. Block size and batch timeout have been tuned offline with Caliper, learned models and genetic or Bayesian optimisation. Run-time adaptation has been proposed for permissionless and private chains, and an adaptive framework for Fabric reports gains over the default fixed configuration. LearningChain forecasts arrivals, learns a performance model and released the HFBTP dataset used here. The reactive-versus-predictive question has a long history in cloud auto-scaling.</p>
            <p>We found no study that compares the three strategies live on the same trace and actuator, uses a tuned static baseline, measures decision headroom before attributing benefit to prediction, and fixes its hypotheses in advance. This describes the literature we retrieved, not an exhaustive survey.</p>
          </div>

          <h2 id="method">Methodology</h2>
          <div className="read">
            <p><strong>Forecasting.</strong> The target is the mean arrival rate over horizon H. Ten forecasters were fitted on the first day, selected on its afternoon and scored once on the second day; comparisons use the Diebold–Mariano test with the Harvey–Leybourne–Newbold correction and Holm adjustment. The live controller uses a causal EWMA, α = 0.005 per second, with at least 600 s of history. <Link to="/research/forecasting">Details</Link>.</p>
            <p><strong>Performance prediction.</strong> Seven families were evaluated on group holdouts over block size, load level and orderer count on HFBTP. The live controller does not use them: its predictor is a table of measured cells from our own scan X1, interpolated in log-rate without extrapolation. <Link to="/research/models">Details</Link>.</p>
            <p><strong>Control.</strong> Static holds B. Reactive steps B after two consecutive rounds beyond a latency threshold. Predictive maximises U(B) = X̂(B) − κ·L̂(B) − β·max(0, λ̂ − X̂(B)) with κ = 0.01 per ms and β = 1, switching only on strict improvement and after a three-decision cooldown. <Link to="/research/controllers">Details</Link>.</p>
          </div>

          <h2 id="design">Experimental design</h2>
          <div className="read">
            <p>One laptop runs Fabric v2.5.16 in Docker under WSL2, with Caliper co-located. Each run uses a fresh channel and a fixed 2-second batch timeout. Five live series ran in order: cl01–cl12, X0, X1, ca01 and M2. M2 compares Static B = 2, Reactive and Predictive, plus a Static B = 10 reference arm, in 12 block-randomised runs on one replayed trace. The margin is max(10% of the better arm, 25 ms) for latency and 2% for throughput; a difference is meaningful only if it exceeds the margin and the runs separate completely. Continuation was integrity-only.</p>
          </div>

          <h2 id="results">Results and discussion</h2>
          <div className="read">
            <p>All 12 M2 runs were valid. The three controllers took 90 decisions between them, all B = 2, and no configuration update was submitted. Every cross-arm pair of runs differed by less than the margin on every metric. The only meaningful difference was between block sizes: Static B = 10 was 4.7 times slower in mean latency and 5.5 times slower in p95.</p>
            <p>The constant-rate scans explain the tie. X0 found no rate up to 60 tx/s at which B = 40 beat B = 10, and X1 found B = 2 best at every rate from 5 to 60 tx/s. When one menu member dominates, the value of a forecast is zero: the Predictive controller's forecast beat persistence live and offline, but no decision depended on it. Choosing B = 2 once, from a measurement, captured the entire available benefit.</p>
            <p>Adaptation could help only where the trade-off changes sign within the operating range. HFBTP shows such a crossover on another deployment, with B = 10 saturating near 160 tx/s; our testbed showed none within the rates its co-located client could drive.</p>
          </div>

          <h2 id="contributions">Contributions</h2>
          <div className="read">
            <ol>
              <li>An implemented, verified live control loop for Fabric block size, with one actuator shared by three controllers.</li>
              <li>A pre-registered, block-randomised comparison against a tuned static baseline, reported in full.</li>
              <li>A decision-headroom analysis showing why forecast skill could not become control benefit here.</li>
              <li>An account of what the offline components do and do not support, including that HFBTP-to-testbed transfer was not established.</li>
            </ol>
          </div>

          <h2 id="limits">Limitations</h2>
          <div className="read">
            <ul>
              <li>One laptop, one topology, one batch timeout, one chaincode, limited menus and rates.</li>
              <li>B = 2 is the smallest value tested, so it is the best of the menu, not an optimum.</li>
              <li>Three runs per arm on one evening; no day-to-day variance.</li>
              <li>The replay keeps minute-to-minute shape but removes bursts and transaction content.</li>
            </ul>
          </div>

          <h2 id="threats">Threats to validity</h2>
          <div className="read">
            <p><strong>Construct.</strong> Transactions are synthetic 128-byte writes with unique keys; M2 is a rate-trace replay, not natural load. The M2 performance model is a measured table, not a learned model. Throughput is arrival-limited and is not a capacity measure.</p>
            <p><strong>Internal.</strong> Two continuation amendments in cl01–cl12 were introduced after observing runs; they governed only whether the series continued. X1 rests on two replicates per configuration. The cc12 transient is unexplained. The client was co-located, which bounded the usable rate range.</p>
            <p><strong>External.</strong> HFBTP comes from a different deployment, and transfer of its models or its crossover location was not established.</p>
            <p><strong>Statistical.</strong> Three runs per arm give a minimum permutation p of 0.10; equivalence is judged against the margin by run-level comparisons, not by a powered test.</p>
          </div>

          <h2 id="conclusion">Conclusion</h2>
          <div className="read">
            <p>Under the evaluated conditions, all three controllers selected B = 2 at every decision and showed no practically meaningful difference in throughput, latency or reliability. The smallest block size dominated at every client-valid rate, leaving no decision headroom, while a poorly chosen static setting cost a factor of four to five in latency.</p>
          </div>

          <h2 id="future">Future work</h2>
          <div className="read">
            <ol>
              <li>A load generator on separate hardware, to reach and locate the saturation point of small blocks.</li>
              <li>Menus that include B = 1 and the batch timeout, and objectives that price small-block overhead.</li>
              <li>Cold-start and burst-preserving workloads in which a controller must move.</li>
              <li>More replicates and days, to estimate run-to-run and day-to-day variance.</li>
              <li>A live test of whether HFBTP-style crossovers appear on other deployments before any learned model is transferred.</li>
            </ol>
          </div>

          <h2 id="refs">References</h2>
          <Await<Refs> file="data/references.json">
            {(r) => (
              <>
                <ol className="refs">
                  {r.references.map((x) => (
                    <li key={x.key}>
                      {x.authors}, “{x.title},” <em>{x.venue}</em>{x.year ? `, ${x.year}` : ""}.
                      {x.doi && <> <a href={`https://doi.org/${x.doi}`} rel="noreferrer">doi:{x.doi}</a></>}
                      {!x.doi && x.url && <> <a href={x.url} rel="noreferrer">{x.url}</a></>}
                    </li>
                  ))}
                </ol>
                <p className="xs muted" style={{ marginTop: 12 }}>From the manuscript's bibliography ({fileName(r.source)}). Its validation report built each of the 36 scholarly entries from a retrieved Crossref or OpenAlex record and confirmed the 4 web sources.</p>
              </>
            )}
          </Await>

          <h2>Manuscript</h2>
          <div className="state">
            <strong>The paper will be published soon, with more experiments.</strong>
          </div>
        </article>
      </div>
    </div>
  );
}
