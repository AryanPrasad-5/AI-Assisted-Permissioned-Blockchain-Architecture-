import { StrictMode, Suspense, lazy } from "react";
import { createRoot } from "react-dom/client";
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import "@fontsource/ibm-plex-sans/400.css";
import "@fontsource/ibm-plex-sans/500.css";
import "@fontsource/ibm-plex-sans/600.css";
import "@fontsource/ibm-plex-sans-condensed/500.css";
import "@fontsource/ibm-plex-sans-condensed/600.css";
import "@fontsource/ibm-plex-serif/400.css";
import "@fontsource/ibm-plex-serif/400-italic.css";
import "@fontsource/ibm-plex-mono/400.css";
import "./styles/tokens.css";
import "./styles/app.css";
import Landing from "./pages/Landing";
import { Workspace } from "./components/Layout";
import { Loading } from "./components/Evidence";
import NotFound from "./pages/NotFound";

const page = (load: () => Promise<{ default: React.ComponentType }>) => {
  const C = lazy(load);
  return <Suspense fallback={<div className="page"><Loading label="Loading page" /></div>}><C /></Suspense>;
};

const router = createBrowserRouter([
  { path: "/", element: <Landing /> },
  {
    path: "/research", element: <Workspace />, children: [
      { index: true, element: page(() => import("./pages/Overview")) },
      { path: "architecture", element: page(() => import("./pages/Architecture")) },
      { path: "data", element: page(() => import("./pages/Datasets")) },
      { path: "models", element: page(() => import("./pages/Models")) },
      { path: "forecasting", element: page(() => import("./pages/Forecasting")) },
      { path: "controllers", element: page(() => import("./pages/Controllers")) },
      { path: "paper", element: page(() => import("./pages/Paper")) },
      { path: "*", element: <NotFound /> },
    ],
  },
  { path: "*", element: <NotFound /> },
]);

createRoot(document.getElementById("root")!).render(<StrictMode><RouterProvider router={router} /></StrictMode>);
