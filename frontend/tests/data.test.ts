/** Every headline number the site shows, checked against the manuscript tables (v1.0, 26 Nov 2026)
 *  and the stored result files. A rebuild that changes any of these fails the test. */
import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
const j = (f: string) => JSON.parse(readFileSync(`public/data/${f}`, "utf-8"));
const r1 = (v: number) => Math.round(v * 10) / 10;

describe("M2 (manuscript Table 'M2 run-level results')", () => {
  const m2 = j("m2.json");
  it("verdict and totals", () => {
    expect(m2.verdict.status).toBe("NO_MEANINGFUL_DIFFERENCE");
    expect(m2.runs).toHaveLength(12);
    expect(m2.runs.every((r: { verdict: string }) => r.verdict === "VALID")).toBe(true);
    expect(m2.runs.reduce((s: number, r: { success: number }) => s + r.success, 0)).toBe(190704);
    expect(m2.runs.reduce((s: number, r: { failed: number }) => s + r.failed, 0)).toBe(0);
  });
  it("arm means and sds", () => {
    const a = m2.arms_recomputed;
    expect([r1(a.static_b2.mean_ms), r1(a.reactive.mean_ms), r1(a.predictive_live.mean_ms), r1(a.static_b10.mean_ms)]).toEqual([42.9, 48.4, 44.1, 201.6]);
    expect([r1(a.static_b2.p95_ms), r1(a.reactive.p95_ms), r1(a.predictive_live.p95_ms), r1(a.static_b10.p95_ms)]).toEqual([82.3, 85.0, 81.3, 451.0]);
    expect(r1(a.static_b10.mean_ms / a.static_b2.mean_ms)).toBe(4.7);
    expect(r1(a.static_b10.p95_ms / a.static_b2.p95_ms)).toBe(5.5);
  });
  it("90 compared decisions, all hold at B = 2", () => {
    const compared = m2.decisions.filter((d: { arm: string }) => d.arm !== "static_b10");
    expect(compared).toHaveLength(90);
    expect(compared.every((d: { target_B: number; changed: boolean }) => d.target_B === 2 && !d.changed)).toBe(true);
  });
  it("live forecast MAE 9.19 vs 10.49 tx/s, identical across Predictive runs", () => {
    const ids = [...new Set(m2.forecast.map((r: { run_id: string }) => r.run_id))] as string[];
    for (const id of ids) {
      const rows = m2.forecast.filter((r: { run_id: string }) => r.run_id === id);
      const mae = (k: string) => rows.reduce((s: number, r: Record<string, number>) => s + Math.abs(r[k]), 0) / rows.length;
      expect(Math.round(mae("err_ewma") * 100) / 100).toBe(9.19);
      expect(Math.round(mae("err_naive") * 100) / 100).toBe(10.49);
    }
  });
  it("every pairwise verdict among compared arms is equivalence", () => {
    const compared = m2.pairwise.filter((p: { comparison: string }) => !p.comparison.startsWith("static_b10"));
    expect(compared.every((p: { verdict: string }) => p.verdict === "EQUIVALENT_WITHIN_SESOI")).toBe(true);
  });
});

describe("live scans", () => {
  const live = j("live_series.json");
  it("X1: B = 2 lowest p95 at every rung; manuscript cells", () => {
    const cell = (b: number, r: number) => live.x1.rungs.find((x: { B: number; rate_tps: number }) => x.B === b && x.rate_tps === r).p95_ms;
    expect([cell(2, 5), cell(5, 5), cell(10, 5), cell(2, 60), cell(10, 60)]).toEqual([223.5, 824.5, 1825.0, 42.0, 175.5]);
    for (const r of [5, 10, 15, 20, 25, 30, 35, 40, 50, 60]) expect(cell(2, r)).toBeLessThan(Math.min(cell(5, r), cell(10, r)));
  });
  it("X0 cells and ca01", () => {
    const cell = (b: number, r: number) => live.x0.rungs.find((x: { B: number; rate_tps: number }) => x.B === b && x.rate_tps === r).p95_ms;
    expect([cell(10, 10), cell(40, 10), cell(10, 60), cell(40, 60)]).toEqual([930, 1945, 180.5, 667]);
    expect(live.ca01.transitions.map((t: { config_block: number }) => t.config_block)).toEqual([304, 425, 486, 607]);
  });
});

describe("offline results", () => {
  it("Phase 1C selected test MAE (report section 4)", () => {
    const m = j("models.json");
    const sel = (sp: string, t: string) => { const f = m.phase1c.selected.find((s: { manifest: string; target: string }) => s.manifest === sp && s.target === t).family;
      return m.phase1c.test.find((r: { manifest: string; target: string; family: string }) => r.manifest === sp && r.target === t && r.family === f).test_mae; };
    expect([sel("T1", "Throughput"), sel("T1b", "Throughput"), sel("T2D0", "Throughput"), sel("T3", "Throughput"), sel("HEAVY", "Throughput")].map((v) => Math.round(v * 100) / 100)).toEqual([1.27, 1.22, 3.27, 1.53, 34.79]);
    const t2 = ["T2D0", "T2D1", "T2D2", "T2D3"].map((d) => sel(d, "Throughput"));
    expect([r1(Math.min(...t2)), r1(Math.max(...t2))]).toEqual([2.9, 10.5]);
  });
  it("Flashbots totals and FB-FC-v1 selected MAE", () => {
    const f = j("flashbots.json");
    expect(f.days["2026-09-21"].transactions + f.days["2026-10-12"].transactions).toBe(1969790);
    const sel = f.comparison.filter((r: { target: string; selected: boolean }) => r.target === "arrival_rate" && r.selected);
    expect(sel.map((r: { horizon_s: number; mae: number }) => [r.horizon_s, Math.round(r.mae * 1000) / 1000])).toEqual([[1, 4.064], [10, 2.195], [60, 1.22], [300, 0.94]]);
  });
  it("HFBTP integrity", () => {
    const h = j("hfbtp_summary.json");
    expect([h.rows, h.absent_count, h.sha256]).toEqual([24687, 117, "CE295D6451787FEFCFE2101579238CDA4BEDE8539BC302E6A482AA4E5782BD5E"]);
  });
  it("provenance: build checks passed", () => expect(j("provenance.json").checks_passed).toBe(true));
});
