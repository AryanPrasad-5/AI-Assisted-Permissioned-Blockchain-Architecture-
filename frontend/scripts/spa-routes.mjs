// Copy index.html to every page address, so direct links and reloads work on static
// hosting without a rewrite rule.
import { copyFileSync, mkdirSync } from "node:fs";

const routes = ["research", "research/architecture", "research/data", "research/models",
  "research/forecasting", "research/controllers", "research/paper"];
for (const route of routes) {
  mkdirSync(`dist/${route}`, { recursive: true });
  copyFileSync("dist/index.html", `dist/${route}/index.html`);
}
console.log(`page addresses prepared: ${routes.length}`);
