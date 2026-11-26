import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { scaleLinear, scaleLog } from "d3-scale";
import { line as d3line } from "d3-shape";

/* ------------------------------------------------------------ sizing */
export function useWidth<T extends HTMLElement>(): [React.RefObject<T | null>, number] {
  const ref = useRef<T>(null);
  const [w, setW] = useState(640);
  useEffect(() => {
    if (!ref.current) return;
    const ro = new ResizeObserver(([e]) => setW(Math.max(260, Math.floor(e.contentRect.width))));
    ro.observe(ref.current);
    return () => ro.disconnect();
  }, []);
  return [ref, w];
}

/* ------------------------------------------------------------ XY chart */
export interface Pt { x: number; y: number | null; hollow?: boolean; note?: string }
export interface Series {
  id: string; label: string; color: string; points: Pt[];
  line?: boolean; marker?: boolean; dash?: string; width?: number; endLabel?: boolean; opacity?: number; r?: number;
}
export interface XYProps {
  series: Series[]; ariaLabel: string; xLabel: string; yLabel: string;
  xLog?: boolean; yLog?: boolean; xDomain?: [number, number]; yDomain?: [number, number];
  xTicks?: number[]; yTicks?: number[]; height?: number;
  fx?: (v: number) => string; fy?: (v: number) => string; tipX?: (v: number) => string;
  bands?: { x0: number; x1: number; label?: string }[]; refX?: { x: number; label?: string }[]; refY?: { y: number; label?: string }[];
  legend?: boolean; mode?: "x" | "point"; zeroBased?: boolean;
}

const M = { t: 12, r: 96, b: 44, l: 56 };

export function XYChart(p: XYProps) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const height = p.height ?? 320;
  const narrow = width < 520;
  const endLen = Math.max(0, ...p.series.filter((s) => s.endLabel).map((s) => s.label.length));
  const m = { ...M, r: endLen && !narrow ? Math.min(200, endLen * 6.8 + 16) : 16, l: narrow ? 48 : M.l };
  const iw = width - m.l - m.r, ih = height - m.t - m.b;
  const all = p.series.flatMap((s) => s.points.filter((q) => q.y !== null)) as { x: number; y: number }[];
  const xd = p.xDomain ?? [Math.min(...all.map((q) => q.x)), Math.max(...all.map((q) => q.x))];
  const yMin = Math.min(...all.map((q) => q.y)), yMax = Math.max(...all.map((q) => q.y));
  const yd = p.yDomain ?? (p.yLog ? [yMin * 0.85, yMax * 1.15] : [p.zeroBased === false ? yMin - (yMax - yMin) * 0.08 : Math.min(0, yMin), yMax * 1.06]);
  const x = (p.xLog ? scaleLog() : scaleLinear()).domain(xd).range([0, iw]);
  const y = (p.yLog ? scaleLog() : scaleLinear()).domain(yd).range([ih, 0]);
  if (!p.xLog && !p.xDomain) (x as ReturnType<typeof scaleLinear>).nice();
  if (!p.yLog && !p.yDomain) (y as ReturnType<typeof scaleLinear>).nice();
  const xt = p.xTicks ?? (x.ticks(narrow ? 4 : 7) as number[]);
  const yt = p.yTicks ?? (y.ticks(5) as number[]);
  const fx = p.fx ?? ((v: number) => String(v));
  const fy = p.fy ?? ((v: number) => String(v));
  const [hover, setHover] = useState<{ px: number; py: number; xv: number; sid?: string; idx?: number } | null>(null);

  const xs = useMemo(() => [...new Set(all.map((q) => q.x))].sort((a, b) => a - b), [p.series]); // eslint-disable-line

  function onMove(e: React.PointerEvent<SVGRectElement>) {
    const rect = (e.currentTarget as SVGRectElement).getBoundingClientRect();
    const px = e.clientX - rect.left, py = e.clientY - rect.top;
    if (p.mode === "point") {
      let best = { d: Infinity, sid: "", idx: -1, x: 0, y: 0 };
      p.series.forEach((s) => s.points.forEach((q, i) => {
        if (q.y === null) return;
        const d = Math.hypot(x(q.x) - px, y(q.y) - py);
        if (d < best.d) best = { d, sid: s.id, idx: i, x: q.x, y: q.y };
      }));
      setHover(best.d < 28 ? { px: x(best.x), py: y(best.y), xv: best.x, sid: best.sid, idx: best.idx } : null);
    } else {
      const xv = xs.reduce((a, b) => (Math.abs(x(b) - px) < Math.abs(x(a) - px) ? b : a), xs[0]);
      setHover({ px: x(xv), py, xv });
    }
  }

  const lineGen = d3line<Pt>().defined((q) => q.y !== null).x((q) => x(q.x)).y((q) => y(q.y as number));
  const tipRows = hover
    ? p.mode === "point"
      ? p.series.filter((s) => s.id === hover.sid).map((s) => ({ s, q: s.points[hover.idx!] }))
      : p.series.map((s) => ({ s, q: s.points.find((q) => q.x === hover.xv) })).filter((r) => r.q && r.q.y !== null)
    : [];

  return (
    <div className="chart" ref={ref}>
      {p.legend !== false && p.series.length > 1 && (
        <div className="legend" aria-hidden="true">
          {p.series.map((s) => (
            <span key={s.id}>
              <i className={s.line === false ? "dot" : ""} style={{ background: s.color, opacity: s.opacity }} />
              {s.label}
            </span>
          ))}
        </div>
      )}
      <div className="y-label" aria-hidden="true">{p.yLabel}</div>
      <svg width={width} height={height} role="img" aria-label={p.ariaLabel}>
        <g transform={`translate(${m.l},${m.t})`}>
          {(p.bands ?? []).map((b, i) => (
            <g key={i}>
              <rect className="band" x={x(b.x0)} width={Math.max(0, x(b.x1) - x(b.x0))} y={0} height={ih} />
              {b.label && <text className="ref-text" x={x(b.x0) + 6} y={14}>{b.label}</text>}
            </g>
          ))}
          <g className="grid">{yt.map((t) => <line key={t} x1={0} x2={iw} y1={y(t)} y2={y(t)} />)}</g>
          <g className="axis">
            <line x1={0} x2={iw} y1={ih} y2={ih} />
            {xt.map((t) => <g key={t} className="tick" transform={`translate(${x(t)},${ih})`}><line y2={4} /><text y={18} textAnchor="middle">{fx(t)}</text></g>)}
            {yt.map((t) => <g key={t} className="tick" transform={`translate(0,${y(t)})`}><text x={-8} dy="0.32em" textAnchor="end">{fy(t)}</text></g>)}
            <text className="axis-label" x={iw} y={ih + 38} textAnchor="end">{p.xLabel}</text>
          </g>
          {(p.refX ?? []).map((r, i) => (
            <g key={i}><line className="ref-line" x1={x(r.x)} x2={x(r.x)} y1={0} y2={ih} />{r.label && <text className="ref-text" x={x(r.x) + 5} y={ih - 6}>{r.label}</text>}</g>
          ))}
          {(p.refY ?? []).map((r, i) => (
            <g key={i}><line className="ref-line" x1={0} x2={iw} y1={y(r.y)} y2={y(r.y)} />{r.label && <text className="ref-text" x={iw - 4} y={y(r.y) - 5} textAnchor="end">{r.label}</text>}</g>
          ))}
          {p.series.map((s) => (
            <g key={s.id} opacity={s.opacity ?? 1}>
              {s.line !== false && <path d={lineGen(s.points) ?? ""} fill="none" stroke={s.color} strokeWidth={s.width ?? 2} strokeDasharray={s.dash} strokeLinejoin="round" strokeLinecap="round" />}
              {s.marker !== false && s.points.map((q, i) => q.y === null ? null : (
                <circle key={i} cx={x(q.x)} cy={y(q.y)} r={s.r ?? 4.5} fill={q.hollow ? "var(--paper)" : s.color} stroke={q.hollow ? s.color : "var(--paper)"} strokeWidth={q.hollow ? 2 : 1.5} />
              ))}
              {s.endLabel && !narrow && (() => {
                const last = [...s.points].reverse().find((q) => q.y !== null);
                return last ? <text className="end-label" x={x(last.x) + 8} y={y(last.y as number)} dy="0.32em">{s.label}</text> : null;
              })()}
            </g>
          ))}
          {hover && p.mode !== "point" && <line className="crosshair" x1={hover.px} x2={hover.px} y1={0} y2={ih} />}
          {hover && p.mode === "point" && <circle cx={hover.px} cy={hover.py} r={8} fill="none" stroke="var(--ink)" strokeWidth={1.5} />}
          <rect x={0} y={0} width={iw} height={ih} fill="transparent" onPointerMove={onMove} onPointerLeave={() => setHover(null)} />
        </g>
      </svg>
      {hover && tipRows.length > 0 && (
        <div className="tooltip" style={{ left: Math.min(width - 200, m.l + hover.px + 14), top: Math.max(0, (p.mode === "point" ? hover.py : 20) + m.t) }}>
          <div className="tt-head">{(p.tipX ?? fx)(hover.xv)}</div>
          {tipRows.map(({ s, q }) => (
            <div className="tt-row" key={s.id}>
              <span><i className="swatch" style={{ background: s.color, margin: 0 }} />{s.label}</span>
              <span>{fy(q!.y as number)}{q!.hollow ? " (excluded)" : ""}</span>
            </div>
          ))}
          {tipRows.map(({ s, q }) => q?.note ? <div key={s.id + "n"} className="xs muted" style={{ marginTop: 4 }}>{q.note}</div> : null)}
        </div>
      )}
    </div>
  );
}

/* ------------------------------------------------------------ run-level dot strip */
export interface StripRow { id: string; label: string; color: string; values: { v: number; label: string }[]; mean?: number; muted?: boolean }
export function DotStrip({ rows, unit, ariaLabel, domain, fmt = (v: number) => v.toFixed(1), marker }: {
  rows: StripRow[]; unit: string; ariaLabel: string; domain?: [number, number]; fmt?: (v: number) => string;
  marker?: { v: number; label: string };
}) {
  const [ref, width] = useWidth<HTMLDivElement>();
  const narrow = width < 520;
  const lw = narrow ? 92 : 150, rowH = 40, top = 8;
  const iw = width - lw - 24;
  const vals = rows.flatMap((r) => r.values.map((q) => q.v));
  const d = domain ?? [Math.min(...vals), Math.max(...vals)];
  const x = scaleLinear().domain(d).nice().range([0, iw]);
  const ticks = x.ticks(narrow ? 4 : 6);
  const h = top + rows.length * rowH + 32;
  const [tip, setTip] = useState<{ x: number; y: number; text: string } | null>(null);
  return (
    <div className="chart" ref={ref}>
      <svg width={width} height={h} role="img" aria-label={ariaLabel}>
        <g transform={`translate(${lw},${top})`}>
          <g className="grid">{ticks.map((t) => <line key={t} x1={x(t)} x2={x(t)} y1={0} y2={rows.length * rowH} />)}</g>
          {marker && <g><line className="ref-line" x1={x(marker.v)} x2={x(marker.v)} y1={-4} y2={rows.length * rowH} /><text className="ref-text" x={x(marker.v) + 4} y={6}>{marker.label}</text></g>}
          {rows.map((r, i) => {
            const cy = i * rowH + rowH / 2;
            return (
              <g key={r.id}>
                <text x={-12} y={cy} dy="0.32em" textAnchor="end" className="end-label" style={{ fill: r.muted ? "var(--ink-3)" : undefined }}>{r.label}</text>
                {r.mean !== undefined && <line x1={x(r.mean)} x2={x(r.mean)} y1={cy - 12} y2={cy + 12} stroke="var(--ink)" strokeWidth={2} />}
                {r.values.map((q, j) => (
                  <circle key={j} cx={x(q.v)} cy={cy} r={6} fill={r.color} stroke="var(--paper)" strokeWidth={2} fillOpacity={r.muted ? 0.75 : 1}
                    onPointerEnter={() => setTip({ x: lw + x(q.v), y: top + cy, text: `${q.label}: ${fmt(q.v)} ${unit}` })}
                    onPointerLeave={() => setTip(null)} />
                ))}
              </g>
            );
          })}
          <g className="axis">
            <line x1={0} x2={iw} y1={rows.length * rowH} y2={rows.length * rowH} />
            {ticks.map((t) => <g key={t} className="tick" transform={`translate(${x(t)},${rows.length * rowH})`}><text y={18} textAnchor="middle">{fmt(t)}</text></g>)}
            <text className="axis-label" x={iw} y={rows.length * rowH + 30} textAnchor="end">{unit}</text>
          </g>
        </g>
      </svg>
      {tip && <div className="tooltip" style={{ left: Math.min(width - 180, tip.x + 12), top: tip.y - 14 }}>{tip.text}</div>}
    </div>
  );
}

export function Legend({ items }: { items: { label: string; color: string; kind?: "line" | "dot" | "hollow" }[] }) {
  return (
    <div className="legend">
      {items.map((i) => (
        <span key={i.label}><i className={i.kind === "dot" ? "dot" : i.kind === "hollow" ? "hollow" : ""} style={{ background: i.kind === "hollow" ? undefined : i.color, color: i.color }} />{i.label}</span>
      ))}
    </div>
  );
}

export function Table({ head, rows, caption, align }: { head: ReactNode[]; rows: ReactNode[][]; caption?: string; align?: ("l" | "r")[] }) {
  return (
    <div className="table-wrap" tabIndex={0} role="region" aria-label={caption ?? "Data table"}>
      <table className="data">
        {caption && <caption>{caption}</caption>}
        <thead><tr>{head.map((h, i) => <th key={i} scope="col" className={align?.[i] === "l" ? "l" : undefined}>{h}</th>)}</tr></thead>
        <tbody>{rows.map((r, i) => <tr key={i}>{r.map((c, j) => <td key={j} className={align?.[j] === "l" ? "l" : undefined}>{c}</td>)}</tr>)}</tbody>
      </table>
    </div>
  );
}
