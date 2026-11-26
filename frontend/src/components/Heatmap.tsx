import { useEffect, useMemo, useRef, useState } from "react";
import { scaleQuantize } from "d3-scale";

/** Observed (or predicted) value over the set-rate x block-size grid for one orderer count. */
export function GridHeatmap({ cells, levels, blocks, value, unit, fmt, ariaLabel, marker }: {
  cells: Map<string, number>; levels: number[]; blocks: number[]; value: string; unit: string;
  fmt: (v: number) => string; ariaLabel: string; marker?: { set: number; block: number };
}) {
  const ref = useRef<HTMLCanvasElement>(null);
  const [tip, setTip] = useState<{ x: number; y: number; text: string } | null>(null);
  const vals = useMemo(() => [...cells.values()], [cells]);
  const lo = Math.min(...vals), hi = Math.max(...vals);
  const steps = ["--seq-0", "--seq-1", "--seq-2", "--seq-3", "--seq-4", "--seq-5"];
  useEffect(() => {
    const c = ref.current; if (!c) return;
    const css = getComputedStyle(document.documentElement);
    const ramp = steps.map((s) => css.getPropertyValue(s).trim());
    const q = scaleQuantize<string>().domain([lo, hi]).range(ramp);
    const ctx = c.getContext("2d")!;
    const cw = 4, ch = 8;
    c.width = blocks.length * cw; c.height = levels.length * ch;
    levels.forEach((s, yi) => blocks.forEach((b, xi) => {
      const v = cells.get(`${s}|${b}`);
      const y = (levels.length - 1 - yi) * ch;
      if (v === undefined) {
        ctx.fillStyle = css.getPropertyValue("--paper").trim(); ctx.fillRect(xi * cw, y, cw, ch);
        ctx.fillStyle = css.getPropertyValue("--bad").trim(); ctx.fillRect(xi * cw + 1, y + 3, cw - 2, 2);
      } else { ctx.fillStyle = q(v); ctx.fillRect(xi * cw, y, cw, ch); }
    }));
    if (marker) {
      const xi = blocks.indexOf(marker.block), yi = levels.indexOf(marker.set);
      if (xi >= 0 && yi >= 0) { ctx.strokeStyle = css.getPropertyValue("--ink").trim(); ctx.lineWidth = 2; ctx.strokeRect(xi * cw - 3, (levels.length - 1 - yi) * ch - 3, cw + 6, ch + 6); }
    }
  }, [cells, levels, blocks, lo, hi, marker]); // eslint-disable-line
  function move(e: React.PointerEvent<HTMLCanvasElement>) {
    const r = e.currentTarget.getBoundingClientRect();
    const xi = Math.floor(((e.clientX - r.left) / r.width) * blocks.length);
    const yi = levels.length - 1 - Math.floor(((e.clientY - r.top) / r.height) * levels.length);
    const s = levels[yi], b = blocks[xi];
    if (s === undefined || b === undefined) return setTip(null);
    const v = cells.get(`${s}|${b}`);
    setTip({ x: e.clientX - r.left, y: e.clientY - r.top, text: `${s} tx/s, block ${b}: ${v === undefined ? "absent from dataset" : `${fmt(v)} ${unit}`}` });
  }
  const css = typeof window !== "undefined" ? getComputedStyle(document.documentElement) : null;
  const rampCss = css ? `linear-gradient(90deg, ${steps.map((s) => css.getPropertyValue(s).trim()).join(",")})` : undefined;
  return (
    <div className="heat-wrap">
      <div style={{ display: "grid", gridTemplateColumns: "auto 1fr", gap: 8 }}>
        <div className="xs muted" style={{ writingMode: "vertical-rl", transform: "rotate(180deg)", textAlign: "center" }}>Set arrival rate, 10 → 200 tx/s</div>
        <div style={{ position: "relative" }}>
          <canvas ref={ref} role="img" aria-label={ariaLabel} onPointerMove={move} onPointerLeave={() => setTip(null)} style={{ aspectRatio: `${blocks.length * 4} / ${levels.length * 8}` }} />
          <div className="xs muted" style={{ display: "flex", justifyContent: "space-between", marginTop: 4 }}><span>Block size {blocks[0]}</span><span>{blocks[blocks.length - 1]}</span></div>
          {tip && <div className="tooltip" style={{ left: Math.min(tip.x + 12, 9999), top: tip.y + 12 }}>{tip.text}</div>}
        </div>
      </div>
      <div className="scale-bar"><span>{value}: {fmt(lo)}</span><span className="ramp" style={{ background: rampCss }} /><span>{fmt(hi)} {unit}</span>
        <span style={{ marginLeft: 12, display: "inline-flex", alignItems: "center", gap: 6 }}><i style={{ width: 12, height: 3, background: "var(--bad)", display: "inline-block" }} />absent cell</span></div>
    </div>
  );
}
