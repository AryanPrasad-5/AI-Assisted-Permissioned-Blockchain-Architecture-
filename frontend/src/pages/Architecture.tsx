import { useState } from "react";
import { Link } from "react-router-dom";
import { Callout, Ev, Status, type EvidenceKind, type StatusKind } from "../components/Evidence";
import { PageHead, Pager } from "../components/Layout";
import { Table } from "../components/charts";

type Mode = "offline" | "replay" | "live" | "evaluation";
const MODES: Record<Mode, { label: string; text: string }> = {
  offline: { label: "Offline analysis", text: "Ran on stored data before any live run. Produces fixed parameters; nothing is learned online." },
  replay: { label: "Historical replay", text: "Re-uses the timing of a recorded workload to pace new synthetic transactions." },
  live: { label: "Live closed loop", text: "Ran on the Fabric testbed during measured runs, with real configuration changes possible." },
  evaluation: { label: "Post-run evaluation", text: "Recomputes every result from raw run outputs after the run, independently of the runner." },
};

interface Node {
  id: string; x: number; y: number; title: string; sub: string; mode: Mode; dashed?: boolean; ev: EvidenceKind;
  status: [StatusKind, string]; what: string; why: string; how: string; inputs: string; outputs: string; role: string; to?: string;
}
const N: Node[] = [
  { id: "hfbtp", x: 20, y: 40, title: "HFBTP dataset", sub: "24,687 cells, Fabric 2.3", mode: "offline", ev: "historical", status: ["ok", "Verified checksum"],
    what: "A public grid of Caliper measurements: offered rate, block size and orderer count against throughput and latency.", why: "The only large public Fabric performance dataset; it lets us test whether a learned model can predict Fabric's response.",
    how: "Checksummed, audited for missing cells (117 absent, never imputed) and leakage; only decision-time features are used.", inputs: "Kaggle archive (Apache-2.0)", outputs: "Training and holdout partitions", role: "Offline model development only. A different deployment from our testbed.", to: "/research/data" },
  { id: "flash", x: 20, y: 135, title: "Flashbots archives", sub: "2 days, 1.97 M tx", mode: "offline", ev: "historical", status: ["ok", "Verified checksum"],
    what: "Daily public-mempool Ethereum transactions with millisecond reception times.", why: "A real, bursty arrival process to drive the testbed and to test forecasting.",
    how: "Each day binned into 86,400 one-second counts; days never joined; no row removed or repaired.", inputs: "Mempool Dumpster archives, two days", outputs: "1-second arrival series", role: "Only arrival timing is used. Ethereum transactions never reach Fabric.", to: "/research/data" },
  { id: "fcbench", x: 20, y: 230, title: "Forecasting benchmark", sub: "FB-FC-v1, 10 models", mode: "offline", ev: "model", status: ["ok", "Executed once, frozen"],
    what: "Persistence, seasonal naive, rolling and expanding means, EWMA and ridge AR at 1, 10, 60 and 300 s.", why: "To choose a forecaster on evidence before it is used live.",
    how: "Fit on day 1 morning, select on day 1 afternoon, score once on day 2 after the selection was hashed.", inputs: "1-second arrival series", outputs: "Frozen EWMA, α = 0.005 per second, 60 s horizon", role: "Supplies the Predictive controller's forecaster.", to: "/research/forecasting" },
  { id: "models", x: 20, y: 325, title: "HFBTP models", sub: "7 families, offline only", mode: "offline", dashed: true, ev: "model", status: ["warn", "Not used live"],
    what: "Lookups, linear regression, a shallow tree, random forest and gradient boosting predicting throughput and latency.", why: "To test whether a learned performance model could serve a predictive controller.",
    how: "Group holdouts on block size, load level and orderers; selection by validation error; one locked test pass per family.", inputs: "HFBTP partitions", outputs: "Test errors; two exported HistGB models", role: "Evaluated offline. No HFBTP model drives a live decision, because transfer to our testbed was not established.", to: "/research/models" },
  { id: "x1", x: 20, y: 420, title: "Measured surface (X1)", sub: "B ∈ {2,5,10}, 5–60 tx/s", mode: "live", ev: "measured", status: ["ok", "Measured, 6 runs"],
    what: "A table of latency for each block size at ten constant rates, measured on our own testbed.", why: "The Predictive controller needs a latency model of this testbed, not of another deployment.",
    how: "Interpolated linearly in log-rate; outside the table the controller abstains and holds.", inputs: "X1 static runs cb01–cb06", outputs: "Predicted latency and throughput per candidate B", role: "The Predictive controller's performance model in M2.", to: "/research/paper" },
  { id: "trace", x: 360, y: 40, title: "Replay schedule", sub: "Day 2 window × 2.114", mode: "replay", ev: "historical", status: ["ok", "Identical in all runs"],
    what: "Ten one-minute segments at 15–54 tx/s, from the per-minute rates of one historical window scaled by k = 2.114.", why: "Gives every arm the same realistic, varying load.",
    how: "Window chosen from history only; rates evenly spaced within each minute; a 60-s unmeasured pre-roll.", inputs: "Day 2 arrival series", outputs: "15,892 measured transactions per run", role: "Keeps minute-to-minute shape; removes within-minute bursts and the 12-s slot cycle.", to: "/research/forecasting" },
  { id: "caliper", x: 560, y: 40, title: "Caliper", sub: "Load generator 0.6.0", mode: "live", ev: "measured", status: ["ok", "Dispatch lag checked"],
    what: "Submits CreateAsset transactions (one 128-byte write, unique keys) on the schedule.", why: "Standard Fabric benchmark client; records per-transaction latency.",
    how: "Co-located with the network. A round whose dispatch lag p99 exceeds 100 ms is client-limited and excluded.", inputs: "Replay schedule", outputs: "Transactions; per-transaction timings", role: "Drives load; its capacity bounded the usable range at about 60 tx/s.", to: "/research/controllers" },
  { id: "fabric", x: 760, y: 40, title: "Fabric testbed", sub: "v2.5.16, 3 Raft orderers", mode: "live", ev: "measured", status: ["ok", "Live, recorded"],
    what: "Three Raft orderers and two peers in Docker under WSL2 on one laptop. BatchTimeout 2 s; a fresh channel per run.", why: "The plant being controlled.",
    how: "Orderers cut a block at B pending transactions or when the timeout expires.", inputs: "Transactions; channel configuration", outputs: "Blocks and configuration blocks on the ledger", role: "Everything measured on this site comes from here.", to: "/research/controllers" },
  { id: "recorder", x: 760, y: 200, title: "Recorder", sub: "Per-round telemetry", mode: "live", ev: "measured", status: ["ok", "Verified"],
    what: "After every 60-s round, summarises latency, round rate and arrival counts.", why: "The controllers' only view of the system.",
    how: "Reads Caliper's per-transaction records for the finished round.", inputs: "Per-transaction timings", outputs: "Round observations", role: "Input to every decision.", to: "/research/controllers" },
  { id: "ctrl", x: 560, y: 200, title: "Controllers", sub: "Static, Reactive, Predictive", mode: "live", ev: "measured", status: ["ok", "90 live decisions"],
    what: "Static holds B. Reactive steps B after two rounds over a latency threshold. Predictive forecasts load and maximises a utility over the candidates.", why: "The three strategies being compared.",
    how: "One decision per minute, using data up to the end of the previous round.", inputs: "Round observations; EWMA forecaster; measured surface", outputs: "Target B, with candidate utilities logged", role: "In M2 every decision was B = 2, so no change was sent. The same code can run in shadow mode (decide, never actuate), which was used only during development.", to: "/research/controllers" },
  { id: "act", x: 360, y: 200, title: "Actuator", sub: "Verified config update", mode: "live", ev: "measured", status: ["ok", "4 of 4 verified (ca01)"],
    what: "Sets MaxMessageCount through a signed channel-configuration update.", why: "A change only counts if it is proven to have taken effect.",
    how: "configtxlator diff, one peer channel update, then Fabric's verifier at three stages, an independent decoder and the ledger's configuration block. 20-s cooldown, at most 10 broadcasts per run.", inputs: "Target B", outputs: "Configuration block on the ledger", role: "Proven live in ca01 (2→5→10→5→2). Not reached by any compared controller in M2.", to: "/research/controllers" },
  { id: "eval", x: 760, y: 360, title: "Evaluation", sub: "Reconcile, re-analyse, decide", mode: "evaluation", ev: "measured", status: ["ok", "12 of 12 passed"],
    what: "Reconciles every transaction against the ledger, recomputes every round with an independent analyser, then applies the pre-registered decision rule.", why: "So that results do not depend on the runner's own bookkeeping.",
    how: "Run is the unit; a difference is meaningful only if it exceeds the margin and the runs separate completely.", inputs: "Ledger, Caliper records, decision logs", outputs: "Run records, pairwise verdicts, hypothesis checks", role: "Produces every number on the controller and experiment pages.", to: "/research/controllers" },
];
const E: [string, string, string?][] = [
  ["hfbtp", "models"], ["flash", "fcbench"], ["flash", "trace"], ["fcbench", "ctrl", "frozen EWMA"], ["x1", "ctrl", "surface"],
  ["trace", "caliper"], ["caliper", "fabric"], ["fabric", "recorder"], ["recorder", "ctrl"], ["ctrl", "act"], ["act", "fabric"], ["fabric", "eval"], ["recorder", "eval"],
];
const W = 172, H = 62;
const center = (n: Node) => ({ cx: n.x + W / 2, cy: n.y + H / 2 });
/** Hand-routed edges where the default curve would cross an unrelated box. */
function customPath(a: Node, b: Node): [string, number, number] | null {
  if (b.id === "ctrl" && (a.id === "fcbench" || a.id === "x1")) {
    const x1 = a.x + W, y1 = a.y + H / 2, tx = b.x + (a.id === "fcbench" ? 50 : 120), ty = b.y + H;
    return [`M${x1},${y1} C${x1 + 180},${y1} ${tx},${ty + 110} ${tx},${ty}`, (x1 + tx) / 2 + 10, Math.max(y1, ty + 60)];
  }
  if (a.id === "act" && b.id === "fabric") {
    const x1 = a.x + W / 2, y1 = a.y, x2 = b.x + 30, y2 = b.y + H;
    return [`M${x1},${y1} C${x1},${y1 - 70} ${x2},${y2 + 60} ${x2},${y2}`, (x1 + x2) / 2, y1 - 40];
  }
  if (a.id === "fabric" && b.id === "eval") {
    const x1 = a.x + W, y1 = a.y + H / 2, x2 = b.x + W, y2 = b.y + H / 2;
    return [`M${x1},${y1} C${x1 + 60},${y1} ${x2 + 60},${y2} ${x2},${y2}`, x1 + 18, (y1 + y2) / 2];
  }
  return null;
}
function edgePath(a: Node, b: Node) {
  const c = customPath(a, b);
  if (c) return c[0];
  const A = center(a), B = center(b);
  const horizontal = Math.abs(A.cx - B.cx) > Math.abs(A.cy - B.cy);
  if (horizontal) {
    const x1 = A.cx < B.cx ? a.x + W : a.x, x2 = A.cx < B.cx ? b.x : b.x + W;
    const mx = (x1 + x2) / 2;
    return `M${x1},${A.cy} C${mx},${A.cy} ${mx},${B.cy} ${x2},${B.cy}`;
  }
  const y1 = A.cy < B.cy ? a.y + H : a.y, y2 = A.cy < B.cy ? b.y : b.y + H;
  const my = (y1 + y2) / 2;
  return `M${A.cx},${y1} C${A.cx},${my} ${B.cx},${my} ${B.cx},${y2}`;
}

export default function Architecture() {
  const [sel, setSel] = useState("ctrl");
  const node = N.find((n) => n.id === sel)!;
  const byId = Object.fromEntries(N.map((n) => [n.id, n]));
  const related = E.filter(([a, b]) => a === sel || b === sel).map(([a, b]) => (a === sel ? byId[b] : byId[a]));
  return (
    <div className="page">
      <PageHead crumb="Research" title="System architecture"
        summary="Offline analysis produced three fixed inputs: a replay trace, a frozen forecaster and a measured latency table. The live testbed ran the control loop. Select any component to see what it does, how it ran and how it connects." />
      <div className="arch">
        <div>
          <div className="pill-row" style={{ marginBottom: 12 }}>
            {(Object.keys(MODES) as Mode[]).map((m) => <span key={m} className="small" style={{ display: "inline-flex", gap: 6, alignItems: "center", marginRight: 12 }}><ModeSwatch mode={m} />{MODES[m].label}</span>)}
          </div>
          <svg className="arch-svg hide-narrow" viewBox="0 0 1010 500" role="group" aria-label="Architecture diagram. Use Tab to move between components and Enter to select.">
            <rect className="lane" x="8" y="8" width="196" height="484" rx="4" />
            <text className="lane-label" x="20" y="28">Fixed before M2</text>
            <text className="lane-label" x="360" y="28">Replay and live testbed</text>
            {E.map(([a, b, label]) => (
              <g key={a + b}>
                <path className={`edge ${a === sel || b === sel ? "hot" : ""}`} d={edgePath(byId[a], byId[b])} markerEnd="url(#arrow)" />
                {label && (() => { const c = customPath(byId[a], byId[b]); const A = center(byId[a]), B = center(byId[b]); const lx = c ? c[1] : (A.cx + B.cx) / 2 + 30, ly = c ? c[2] : (A.cy + B.cy) / 2 - 6; return <text className="ref-text" x={lx} y={ly} style={{ fill: "var(--ink-3)", font: "400 11px var(--font-ui)" }}>{label}</text>; })()}
              </g>
            ))}
            <defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--ink-3)" /></marker></defs>
            {N.map((n) => (
              <g key={n.id} className={`node ${n.dashed ? "dashed" : ""}`} transform={`translate(${n.x},${n.y})`} tabIndex={0} role="button" aria-pressed={n.id === sel}
                aria-label={`${n.title}: ${n.sub}. ${MODES[n.mode].label}.`} onClick={() => setSel(n.id)}
                onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setSel(n.id); } }}>
                <rect width={W} height={H} rx="4" />
                <rect width="4" height={H} rx="2" fill={modeColor(n.mode)} />
                <text x="14" y="26">{n.title}</text>
                <text x="14" y="45" className="sub">{n.sub}</text>
              </g>
            ))}
          </svg>
          <div className="show-narrow" aria-label="Architecture components">
            {(Object.keys(MODES) as Mode[]).map((m) => (
              <div key={m} style={{ marginBottom: 12 }}>
                <div className="small muted" style={{ marginBottom: 6 }}>{MODES[m].label}</div>
                <div className="pill-row">
                  {N.filter((n) => n.mode === m).map((n) => (
                    <button key={n.id} className="btn btn-quiet btn-sm" aria-pressed={n.id === sel} onClick={() => setSel(n.id)}
                      style={n.id === sel ? { background: "var(--ink)", color: "var(--paper)" } : undefined}>{n.title}</button>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
        <aside className="arch-panel" aria-live="polite">
          <div className="badge-row"><Ev kind={node.ev} /><Status kind={node.status[0]}>{node.status[1]}</Status></div>
          <div><h2 style={{ fontSize: "var(--t-2xl)" }}>{node.title}</h2><p className="muted small">{MODES[node.mode].label}: {MODES[node.mode].text}</p></div>
          <dl>
            <div><dt>What it is</dt><dd>{node.what}</dd></div>
            <div><dt>Why it exists</dt><dd>{node.why}</dd></div>
            <div><dt>How it works</dt><dd>{node.how}</dd></div>
            <div><dt>Inputs</dt><dd>{node.inputs}</dd></div>
            <div><dt>Outputs</dt><dd>{node.outputs}</dd></div>
            <div><dt>Role in the research</dt><dd>{node.role}</dd></div>
            <div><dt>Connected to</dt><dd>{related.map((r, i) => <span key={r.id}>{i > 0 && ", "}<button className="linklike" onClick={() => setSel(r.id)}>{r.title}</button></span>)}</dd></div>
          </dl>
          {node.to && <Link to={node.to} className="btn btn-quiet btn-sm" style={{ justifySelf: "start" }}>See the evidence</Link>}
        </aside>
      </div>

      <section className="page-section">
        <h2>Four ways a component can run</h2>
        <p className="read">These are easy to confuse, and the claims each supports are different.</p>
        <Table head={["Mode", "What it means here", "Where it was used", "What it can support"]} align={["l", "l", "l", "l"]}
          rows={[
            ["Offline analysis", "Computed on stored data, once, before live use", "Forecasting benchmark, HFBTP models", "Accuracy on held-out data. Not control benefit."],
            ["Historical replay", "Recorded timing paces new synthetic transactions", "Every live run (Flashbots window); cl01–cl12 stored-decision Predictive arm", "Equal workloads across arms. Not natural Ethereum or enterprise load."],
            ["Shadow evaluation", "Controllers decide beside a run but never actuate", "Development checks only", "That the code produces decisions. Nothing reported relies on it."],
            ["Live closed loop", "Decisions can change the running network's configuration", "M2 (all three arms), cl01–cl12 Reactive arms, ca01 scripted changes", "Measured throughput, latency and failures under each controller."],
          ]} />
        <Callout tone="model" title="One thing the diagram does not show">
          The dashed HFBTP models box has no edge into the live loop on purpose. Those models were evaluated offline only. They were never
          used by a live controller.
        </Callout>
      </section>
      <Pager />
    </div>
  );
}

function modeColor(m: Mode) { return { offline: "var(--ev-historical)", replay: "var(--ev-illustrative)", live: "var(--ink)", evaluation: "var(--signal)" }[m]; }
function ModeSwatch({ mode }: { mode: Mode }) { return <i aria-hidden="true" style={{ width: 4, height: 14, borderRadius: 2, background: modeColor(mode), display: "inline-block" }} />; }
