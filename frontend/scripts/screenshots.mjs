// Render every page at three widths, capture screenshots, and report console errors,
// horizontal overflow and axe-core accessibility violations.
// Usage: node scripts/screenshots.mjs <base-url> <out-dir> [--pages=/,/research] [--widths=1440,390] [--dark]
import { chromium } from "playwright-core";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";

const [base = "http://localhost:4173", out = "shots"] = process.argv.slice(2).filter((a) => !a.startsWith("--"));
const flag = (n) => process.argv.find((a) => a.startsWith(`--${n}=`))?.split("=")[1];
const PAGES = (flag("pages") ?? "/,/research,/research/architecture,/research/data,/research/models,/research/forecasting,/research/controllers,/research/paper").split(",");
const WIDTHS = (flag("widths") ?? "1440,834,390").split(",").map(Number);
const dark = process.argv.includes("--dark");
const full = !process.argv.includes("--viewport");
mkdirSync(out, { recursive: true });
const axeSrc = readFileSync(path.resolve("node_modules/axe-core/axe.min.js"), "utf-8");

const browser = await chromium.launch({ executablePath: "C:/Program Files/Google/Chrome/Application/chrome.exe" });
const report = [];
for (const w of WIDTHS) {
  const ctx = await browser.newContext({ viewport: { width: w, height: w < 600 ? 844 : 900 }, deviceScaleFactor: 1, colorScheme: dark ? "dark" : "light", reducedMotion: "reduce" });
  for (const p of PAGES) {
    const page = await ctx.newPage();
    const errors = [];
    page.on("console", (m) => m.type() === "error" && errors.push(m.text()));
    page.on("pageerror", (e) => errors.push(String(e)));
    await page.goto(base + p, { waitUntil: "networkidle" });
    await page.evaluate(() => document.fonts.ready);
    await page.waitForTimeout(400);
    const overflow = await page.evaluate(() => {
      const doc = document.documentElement.scrollWidth - window.innerWidth;
      const offenders = [...document.querySelectorAll("body *")].filter((el) => {
        const r = el.getBoundingClientRect();
        return r.right > window.innerWidth + 1 && !el.closest(".table-wrap") && getComputedStyle(el).position !== "fixed";
      }).slice(0, 5).map((el) => `${el.tagName.toLowerCase()}.${[...el.classList].join(".")}`);
      return { doc, offenders };
    });
    await page.addScriptTag({ content: axeSrc });
    const axe = await page.evaluate(async () => {
      const r = await window.axe.run(document, { runOnly: ["wcag2a", "wcag2aa"] });
      return r.violations.map((v) => ({ id: v.id, impact: v.impact, n: v.nodes.length, sample: v.nodes[0]?.target?.join(" ") }));
    });
    const name = `${(p === "/" ? "landing" : p.replace(/^\/research\/?/, "") || "overview")}_${w}${dark ? "_dark" : ""}.png`;
    await page.screenshot({ path: path.join(out, name), fullPage: full });
    report.push({ page: p, width: w, errors, overflow, axe });
    console.log(`${p.padEnd(24)} ${String(w).padStart(5)}  overflow ${overflow.doc}px ${overflow.offenders.join(" ")}  errors ${errors.length}  axe ${axe.map((a) => `${a.id}(${a.n})`).join(" ") || "none"}`);
    await page.close();
  }
  await ctx.close();
}
await browser.close();
writeFileSync(path.join(out, `report${dark ? "_dark" : ""}.json`), JSON.stringify(report, null, 2));
