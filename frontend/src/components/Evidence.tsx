import type { ReactNode } from "react";
import { useState } from "react";
import { fileName } from "../lib/format";
import { Activity, Archive, CircleSlash, PencilRuler, Sigma, AlertTriangle, CheckCircle2, CircleDashed, XCircle, Loader2, Table2 } from "lucide-react";

export type EvidenceKind = "measured" | "model" | "historical" | "illustrative" | "unavailable";

const EV: Record<EvidenceKind, { label: string; icon: typeof Activity; title: string }> = {
  measured: { label: "Measured", icon: Activity, title: "Recorded on the live Fabric testbed and verified against the ledger." },
  model: { label: "Model output", icon: Sigma, title: "Produced by a model or fitted relation. Not a measurement." },
  historical: { label: "Public dataset", icon: Archive, title: "Historical public data (HFBTP or Flashbots). Not produced on our testbed." },
  illustrative: { label: "Illustrative", icon: PencilRuler, title: "A diagram or explanation. Not data." },
  unavailable: { label: "Not available", icon: CircleSlash, title: "Not implemented, not run, or not stored." },
};

export function Ev({ kind, children }: { kind: EvidenceKind; children?: ReactNode }) {
  const { label, icon: Icon, title } = EV[kind];
  return (
    <span className={`ev ev-${kind}`} title={title}>
      <Icon aria-hidden="true" />
      {children ?? label}
    </span>
  );
}

export function EvidenceKey() {
  return (
    <div className="legend-key" aria-label="Evidence classes">
      {(Object.keys(EV) as EvidenceKind[]).map((k) => <Ev key={k} kind={k} />)}
    </div>
  );
}

export type StatusKind = "ok" | "warn" | "bad" | "neutral";
const ST: Record<StatusKind, typeof CheckCircle2> = { ok: CheckCircle2, warn: AlertTriangle, bad: XCircle, neutral: CircleDashed };
export function Status({ kind, children }: { kind: StatusKind; children: ReactNode }) {
  const Icon = ST[kind];
  return <span className={`status status-${kind}`}><Icon aria-hidden="true" />{children}</span>;
}

/** Figure = title + evidence class + chart + caption, with the provenance margin beside it. */
export function Figure({ title, kind, caption, source, note, table, children, id }: {
  title: string; kind: EvidenceKind; caption?: ReactNode; source?: string | string[]; note?: ReactNode;
  table?: ReactNode; children: ReactNode; id?: string;
}) {
  const [showTable, setShowTable] = useState(false);
  const sources = [...new Set((source ? (Array.isArray(source) ? source : [source]) : []).map(fileName))];
  const margin = Boolean(note) || sources.length > 0;
  return (
    <figure className={margin ? "figure" : "figure figure-full"} id={id}>
      <div className="figure-main">
        <div className="figure-title"><h3>{title}</h3><Ev kind={kind} /></div>
        {showTable && table ? table : children}
        {caption && <figcaption className="figure-caption">{caption}</figcaption>}
        {table && (
          <div className="figure-tools">
            <button className="btn btn-quiet btn-sm" aria-pressed={showTable} onClick={() => setShowTable((s) => !s)}>
              <Table2 size={14} aria-hidden="true" /> {showTable ? "Show chart" : "Show data table"}
            </button>
          </div>
        )}
      </div>
      {margin && <aside className="margin-note" aria-label="Provenance">
        {note && <div>{note}</div>}
        {sources.length > 0 && (
          <div>
            <div>Source</div>
            {sources.map((s) => <div className="src" key={s}>{s}</div>)}
          </div>
        )}
      </aside>}
    </figure>
  );
}

export function Callout({ tone = "plain", title, children }: { tone?: "plain" | "warn" | "bad" | "model"; title?: string; children: ReactNode }) {
  return (
    <div className={`callout ${tone !== "plain" ? `callout-${tone}` : ""}`} role={tone === "bad" || tone === "warn" ? "note" : undefined}>
      {title && <strong>{title}</strong>}
      <div>{children}</div>
    </div>
  );
}

export function Tech({ summary = "Technical detail", children }: { summary?: string; children: ReactNode }) {
  return (
    <details className="tech">
      <summary>{summary}</summary>
      <div>{children}</div>
    </details>
  );
}

/* ------------------------------------------------------------ states */
export function Loading({ label = "Loading evidence", height = 240 }: { label?: string; height?: number }) {
  return (
    <div role="status" aria-live="polite">
      <div className="skeleton" style={{ height }} />
      <span className="visually-hidden">{label}</span>
      <div className="small muted" style={{ marginTop: 8, display: "flex", gap: 6, alignItems: "center" }}>
        <Loader2 size={14} aria-hidden="true" style={{ animation: "spin 1s linear infinite" }} /> {label}
      </div>
    </div>
  );
}

export function ErrorState({ message, retry }: { message: string; retry?: () => void }) {
  return (
    <div className="state" role="alert">
      <strong>This evidence file did not load.</strong>
      <span>{message} The page still works; reload to try again.</span>
      {retry && <button className="btn btn-quiet btn-sm" onClick={retry}>Reload page</button>}
    </div>
  );
}

export function Unavailable({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="state">
      <Ev kind="unavailable" />
      <strong>{title}</strong>
      <span>{children}</span>
    </div>
  );
}
