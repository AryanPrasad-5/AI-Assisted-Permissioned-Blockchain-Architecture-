import type { Arm } from "./data";

export const fmt = (v: number | null | undefined, digits = 1) =>
  v === null || v === undefined || Number.isNaN(v) ? "–" : v.toLocaleString("en-GB", { minimumFractionDigits: digits, maximumFractionDigits: digits });
export const int = (v: number) => v.toLocaleString("en-GB");
export const signed = (v: number, digits = 1) => (v > 0 ? "+" : v < 0 ? "−" : "") + fmt(Math.abs(v), digits);
export const short = (h: string, n = 10) => (h ? h.slice(0, n) + "…" : "");
/** File name shown beside a figure: no folders and no date stamps. */
export const fileName = (p: string) => (p.split("/").pop() ?? p).replace(/[_-]?\d{4}-\d{2}-\d{2}/g, "").replace(/[_-]?\d{8}T\d+Z?/g, "");

export const ARMS: Record<Arm, { label: string; short: string; color: string; compared: boolean }> = {
  static_b2: { label: "Static (B = 2)", short: "Static", color: "var(--c-static)", compared: true },
  reactive: { label: "Reactive", short: "Reactive", color: "var(--c-reactive)", compared: true },
  predictive_live: { label: "Predictive", short: "Predictive", color: "var(--c-predictive)", compared: true },
  static_b10: { label: "Static B = 10 (reference)", short: "Static B=10", color: "var(--c-reference)", compared: false },
};
/** Display order is fixed and alphabetical-by-role (baseline first), never by result. */
export const ARM_ORDER: Arm[] = ["static_b2", "reactive", "predictive_live", "static_b10"];

export const BCOLOR: Record<number, string> = { 2: "var(--b-2)", 5: "var(--b-5)", 10: "var(--b-10)", 40: "var(--b-40)" };

export const hms = (s: number) => {
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
};
