import type { M2 } from "../lib/data";
import { ARMS, ARM_ORDER, BCOLOR } from "../lib/format";

/**
 * Block size selected at each of the 10 per-minute decisions of every M2 run,
 * read from each run's run_result.json (control.decisions[].target_B).
 */
export function DecisionLedger({ m2, animate = false }: { m2: M2; animate?: boolean }) {
  const byRun = new Map<string, number[]>();
  for (const d of m2.decisions) {
    const row = byRun.get(d.run_id) ?? [];
    row[d.index] = d.target_B;
    byRun.set(d.run_id, row);
  }
  const compared = m2.decisions.filter((d) => ARMS[d.arm].compared);
  const allTwo = compared.every((d) => d.target_B === 2 && !d.changed);
  const label = `Decision ledger for 12 M2 runs. ${compared.length} decisions by the three compared controllers, ${allTwo ? "all of them B = 2 with no configuration change" : "not all B = 2"}. The Static B = 10 reference arm held B = 10 for its 30 decisions.`;
  let cell = 0;
  return (
    <div className={`ledger ${animate ? "animate" : ""}`} role="img" aria-label={label}>
      <div className="ledger-head" aria-hidden="true"><span>Run</span>{Array.from({ length: 10 }, (_, i) => <span key={i}>{i + 1}</span>)}</div>
      {ARM_ORDER.map((arm) => {
        const runs = m2.runs.filter((r) => r.arm_label === arm).sort((a, b) => a.replicate - b.replicate);
        return (
          <div key={arm} style={{ display: "contents" }}>
            {arm === "static_b10" && <div className="ledger-gap" aria-hidden="true" />}
            {runs.map((r) => (
              <div className="ledger-row" key={r.run_id} aria-hidden="true">
                <span className="who"><i style={{ background: ARMS[arm].color }} />{ARMS[arm].short} <span className="muted">{r.run_id}</span></span>
                {(byRun.get(r.run_id) ?? []).map((b, i) => (
                  <span key={i} className={`ledger-cell ${b >= 10 ? "b10" : ""}`} style={{ background: BCOLOR[b], animationDelay: `${(cell++) * 9}ms` }}>{b}</span>
                ))}
              </div>
            ))}
          </div>
        );
      })}
    </div>
  );
}
