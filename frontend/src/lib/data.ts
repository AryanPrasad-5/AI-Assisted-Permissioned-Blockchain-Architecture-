import { useEffect, useState } from "react";

/** Static evidence files produced by scripts/build_data.py. */
export type DataFile =
  | "data/m2.json"
  | "data/live_series.json"
  | "data/hfbtp_summary.json"
  | "data/hfbtp_grid.json"
  | "data/models.json"
  | "data/flashbots.json"
  | "data/forecasts.json"
  | "data/references.json";

const cache = new Map<string, Promise<unknown>>();

export function fetchData<T>(file: DataFile): Promise<T> {
  if (!cache.has(file)) {
    const url = `${import.meta.env.BASE_URL}${file}`;
    const p = fetch(url).then((r) => {
      if (!r.ok) throw new Error(`${file} could not be loaded (HTTP ${r.status}).`);
      return r.json();
    });
    p.catch(() => cache.delete(file));
    cache.set(file, p);
  }
  return cache.get(file) as Promise<T>;
}

export type Loadable<T> = { status: "loading" } | { status: "error"; message: string } | { status: "ready"; data: T };

export function useData<T>(file: DataFile): Loadable<T> {
  const [state, setState] = useState<Loadable<T>>({ status: "loading" });
  useEffect(() => {
    let live = true;
    fetchData<T>(file)
      .then((data) => live && setState({ status: "ready", data }))
      .catch((e: Error) => live && setState({ status: "error", message: e.message }));
    return () => { live = false; };
  }, [file]);
  return state;
}

/* ------------------------------------------------------------ shapes */
export type Row = Record<string, number | string | boolean | null>;

export interface M2Run {
  run_id: string; arm_label: Arm; replicate: number; verdict: string; scheduled: number; success: number;
  failed: number; throughput_tps: number; offered_tps: number; delivered_ratio: number; mean_ms: number;
  p50_ms: number; p95_ms: number; p99_ms: number; max_ms: number; n_decisions: number; decisions_changed: number;
  decisions_abstained: number; config_changes: number; n_client_limited: number; independent_pass: boolean;
  start_utc: string; finish_utc: string; run_result_sha256: string; analysis_sha256: string;
  evidence_list_sha256: string; decision_sha256: string; independent_verification_sha256: string;
}
export type Arm = "static_b2" | "reactive" | "predictive_live" | "static_b10";
export interface M2Decision {
  run_id: string; arm: Arm; index: number; telemetry_round: number; current_B: number; target_B: number;
  changed: boolean; abstained: boolean; applied: boolean; reason: string | null;
  observed_mean_ms?: number; observed_p95_ms?: number;
  reactive?: { observed_latency_ms: number; high_ms: number; low_ms: number; congestion_streak: number };
  predictive?: { forecast_tps: number; hold_cause: string; decision_kind: string; kappa_per_ms: number; beta: number;
    candidates: { B: number; latency_ms: number; throughput_tps: number; utility: number }[] };
}
export interface M2Segment { run_id: string; arm_label: Arm; segment: number; rate_tps: number; B: number; mean_ms: number; p95_ms: number; success: number; failed: number; class: string }
export interface M2Pair { comparison: string; metric: string; verdict: string; difference_of_means: number; sesoi: number; min_pair_diff: number; max_pair_diff: number; n_a: number; n_b: number; exact_permutation_p_two_sided: number }
export interface M2 {
  verdict: { status: string; text: string; ranking_by_p95: string[]; best_by_p95_point_estimate: string; meaningful: unknown[] };
  runs: M2Run[]; pairwise: M2Pair[]; segments: M2Segment[]; decisions: M2Decision[];
  forecast: { run_id: string; decision: number; forecast_tps: number; realized_tps: number; naive_persistence_tps: number; err_ewma: number; err_naive: number }[];
  hypotheses: { id: string; prediction: string; measured: string; status: string }[];
  arms_recomputed: Record<Arm, { runs: number; mean_ms: number; mean_sd: number; p95_ms: number; p95_sd: number; p99_ms: number; throughput_tps: number; delivered_ratio: number; success: number; failed: number; config_changes: number }>;
  offline_prediction: { kind: string; per_B: Record<string, { run_mean_ms: number }> };
  archive_checksums: { file: string; sha256: string; verified: boolean }[];
  sources: Record<string, string>;
}
export interface Rung { B: number; rate_tps: number; p95_ms: number; mean_ms: number; n_reps: number; p95_reps: string; client_valid?: boolean; cell_valid?: boolean; classes: string; delivered_ratio?: number }
export interface LiveSeries {
  x1: { rungs: Rung[]; source: string };
  x0: { rungs: Rung[]; source: string; checksums: { file: string; verified: boolean }[] };
  closed_loop: { arms: Row[]; registry: Row[]; segments: Row[]; sources: Record<string, string> };
  hfbtp_vs_x0: { rows: Row[]; source: string };
  batching_check: { rows: Row[]; source: string; constants_ms: Record<string, number>; batch_timeout_s: number };
  ca01: { verdict: string; executed_utc: string; rate_tps: number; transactions: number; failed: number;
    transitions: { from: number; to: number; config_block: number; actuator_s: number }[];
    positive_control: { B2_ms: number; B5_ms: string; B10_ms: number; x1_cells_ms: Record<string, number> }; source: string };
}
export interface HfbtpSummary {
  rows: number; header: string[]; correlation: number[][]; levels: number[]; blocks: number[]; orderers: number[];
  absent_count: number; absent_slots: { block: number; orderers: number }[]; sha256: string; source: string;
  columns: { name: string; min: number; max: number; mean: number; sd: number; distinct: number; missing: number; hist: { start: number; width: number; counts: number[] } }[];
}
export interface HfbtpGrid { columns: string[]; rows: [number, number, number, number, number][]; source: string; sha256: string; note: string }
export interface Phase1CRow { manifest: string; target: string; family: string; config: string; val_mae: number | null; test_mae: number | null; test_rmse: number | null; test_r2: number | null; n_test: number; n_unsupported: number; unsupported_note: string | null }
export interface Models {
  phase1c: { test: Phase1CRow[]; selected: { manifest: string; target: string; family: string; config: string; val_mae: number }[]; per_level: Row[]; source: string; report: string };
  e6: { decisions: Row[]; static_variants: Row[]; spearman: Row[]; menu: Row[]; report: string | null };
  repo_baselines: { rows: Row[]; source: string; note: string };
}
export interface Flashbots {
  comparison: Row[]; dm: Row[];
  days: Record<string, { transactions: number; per_minute_tps: number[]; max_1s: number; source: string }>;
  eda: Record<string, Record<string, unknown>>;
  m2_window: { day: string; start_s: number; minutes: number; k: number };
  sources: Record<string, string>;
}
export type Forecasts = Record<"10" | "60" | "300", { origin_s: number[]; actual: number[]; models: Record<string, (number | null)[]>; source: string }>;
