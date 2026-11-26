import { useEffect, useState, type ReactNode } from "react";
import { Link, NavLink, Outlet, useLocation } from "react-router-dom";
import { Menu, Moon, Sun, X } from "lucide-react";
import { EvidenceKey } from "./Evidence";

export const SITE = {
  name: "Cutpoint",
  title: "Static, reactive or predictive? Block-size control on Hyperledger Fabric",
  authors: "Aryan Prasad and Anuj Patel",
  github: "https://github.com/AryanPrasad-5/AI-Assisted-Permissioned-Blockchain-Architecture-",
};

/** The mark: transactions queue up, the orderer cuts a block. */
export function Mark() {
  return (
    <svg viewBox="0 0 28 28" aria-hidden="true">
      <rect x="2" y="10" width="5" height="8" rx="1.2" fill="currentColor" opacity="0.35" />
      <rect x="8.5" y="10" width="5" height="8" rx="1.2" fill="currentColor" opacity="0.6" />
      <rect x="15" y="10" width="5" height="8" rx="1.2" fill="currentColor" />
      <rect x="22.5" y="4" width="2.5" height="20" rx="1.25" fill="var(--signal)" />
    </svg>
  );
}

export function Brand() {
  return <Link to="/" className="brand"><Mark />{SITE.name}</Link>;
}

function useTheme(): [string, () => void] {
  const [theme, setTheme] = useState<string>(() => {
    try { return localStorage.getItem("theme") ?? ""; } catch { return ""; }
  });
  useEffect(() => {
    if (theme) document.documentElement.dataset.theme = theme; else delete document.documentElement.dataset.theme;
  }, [theme]);
  const resolved = theme || (window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light");
  const toggle = () => {
    const next = resolved === "dark" ? "light" : "dark";
    setTheme(next);
    try { localStorage.setItem("theme", next); } catch { /* storage unavailable */ }
  };
  return [resolved, toggle];
}

export function ThemeToggle() {
  const [theme, toggle] = useTheme();
  const dark = theme === "dark";
  return (
    <button className="icon-btn" onClick={toggle} aria-label={dark ? "Switch to light theme" : "Switch to dark theme"} title={dark ? "Light theme" : "Dark theme"}>
      {dark ? <Sun aria-hidden="true" /> : <Moon aria-hidden="true" />}
    </button>
  );
}

export function SiteHeader() {
  return (
    <header className="site-header">
      <div className="inner">
        <Brand />
        <nav className="site-nav" aria-label="Main">
          <Link to="/research" className="hide-sm">Research</Link>
          <Link to="/research/controllers" className="hide-sm">Controllers</Link>
          <Link to="/research/paper">Paper</Link>
          <ThemeToggle />
        </nav>
      </div>
    </header>
  );
}

export function SiteFooter() {
  return (
    <footer className="site-footer">
      <div className="wrap">
        <div>
          <Brand />
          <p style={{ marginTop: 12, maxWidth: "46ch" }}>
            {SITE.title}. {SITE.authors}. Everything on this site is recorded evidence or offline model output.
            No Fabric network runs behind it.
          </p>
        </div>
        <div>
          <h4>Explore</h4>
          <ul>
            <li><Link to="/research">Research overview</Link></li>
            <li><Link to="/research/controllers">Controller comparison</Link></li>
            <li><Link to="/research/models">Model evaluation</Link></li>
          </ul>
        </div>
        <div>
          <h4>Sources</h4>
          <ul>
            <li><Link to="/research/paper">Paper and references</Link></li>
            <li><a href={SITE.github} rel="noreferrer">Source repository</a></li>
            <li><a href="https://www.kaggle.com/datasets/loveffc/hfbtp-a-blockchain-performance-dataset" rel="noreferrer">HFBTP dataset (Apache-2.0)</a></li>
            <li><a href="https://mempool-dumpster.flashbots.net/" rel="noreferrer">Flashbots Mempool Dumpster</a></li>
          </ul>
        </div>
      </div>
    </footer>
  );
}

export const PAGES = [
  { path: "/research", label: "Overview", end: true },
  { path: "/research/architecture", label: "System architecture" },
  { path: "/research/data", label: "Datasets" },
  { path: "/research/models", label: "Model evaluation" },
  { path: "/research/forecasting", label: "Workload forecasting" },
  { path: "/research/controllers", label: "Controller comparison" },
  { path: "/research/paper", label: "Paper" },
];

export function Workspace() {
  const [open, setOpen] = useState(false);
  const loc = useLocation();
  useEffect(() => { setOpen(false); window.scrollTo(0, 0); }, [loc.pathname]);
  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && setOpen(false);
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open]);
  const here = PAGES.find((p) => p.path === loc.pathname)?.label ?? "Research";
  return (
    <div className="ws">
      <a className="skip" href="#main">Skip to content</a>
      <div className="ws-top">
        <button className="icon-btn" aria-label="Open navigation" aria-expanded={open} aria-controls="ws-nav" onClick={() => setOpen(true)}><Menu aria-hidden="true" /></button>
        <span className="here">{here}</span>
        <span style={{ marginLeft: "auto" }}><ThemeToggle /></span>
      </div>
      {open && <div className="scrim" onClick={() => setOpen(false)} aria-hidden="true" />}
      <aside className="ws-nav" id="ws-nav" data-open={open} aria-label="Research sections">
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
          <Brand />
          {open ? <button className="icon-btn" aria-label="Close navigation" onClick={() => setOpen(false)}><X aria-hidden="true" /></button> : <ThemeToggle />}
        </div>
        <nav>
          <ol>
            {PAGES.map((p, i) => (
              <li key={p.path}>
                <NavLink to={p.path} end={p.end}><span className="k">{i + 1}</span>{p.label}</NavLink>
              </li>
            ))}
          </ol>
        </nav>
        <div className="nav-foot">
          <div>Evidence classes used on every figure</div>
          <EvidenceKey />
        </div>
      </aside>
      <main className="ws-main" id="main" tabIndex={-1}>
        <Outlet />
      </main>
    </div>
  );
}

export function PageHead({ crumb, title, summary, children }: { crumb?: string; title: string; summary: ReactNode; children?: ReactNode }) {
  useEffect(() => { document.title = `${title} – ${SITE.name}`; }, [title]);
  return (
    <header className="page-head">
      {crumb && <div className="crumb">{crumb}</div>}
      <h1>{title}</h1>
      <p className="summary">{summary}</p>
      {children}
    </header>
  );
}

export function Pager() {
  const loc = useLocation();
  const i = PAGES.findIndex((p) => p.path === loc.pathname);
  const prev = PAGES[i - 1], next = PAGES[i + 1];
  return (
    <nav className="pager" aria-label="Previous and next section">
      {prev ? <Link to={prev.path}><span>Previous</span>{prev.label}</Link> : <span />}
      {next && <Link to={next.path}><span>Next</span>{next.label}</Link>}
    </nav>
  );
}
