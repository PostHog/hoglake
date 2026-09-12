import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, Outlet, useParams } from "react-router-dom";
import { checkHealth, getInstanceInfo } from "../api/client";
import { formatBytes, formatCompactCount, formatCount } from "../lib/format";
import { applyTheme, initialTheme, persistTheme, type Theme } from "../lib/theme";

function ThemeToggle() {
  const [theme, setTheme] = useState<Theme>(initialTheme);
  const next: Theme = theme === "dark" ? "light" : "dark";
  const flip = () => {
    applyTheme(next);
    persistTheme(next);
    setTheme(next);
  };
  return (
    <button
      type="button"
      className="theme-toggle"
      onClick={flip}
      title={`Switch to ${next} mode`}
      aria-label={`Switch to ${next} mode`}
    >
      {theme === "dark" ? "☀" : "☾"}
    </button>
  );
}

function InstanceName() {
  const { data } = useQuery({
    queryKey: ["instance-info"],
    queryFn: getInstanceInfo,
    staleTime: Infinity,
    retry: false,
  });
  if (!data?.name) return null;
  return <span className="instance-name">{data.name}</span>;
}

function InstanceTotals() {
  // Same endpoint as InstanceName under its own key: the name is
  // fetch-once (staleTime Infinity), the totals refresh. The server
  // caches them (~60s), so the refetch matches that cadence.
  const { data } = useQuery({
    queryKey: ["instance-totals"],
    queryFn: getInstanceInfo,
    refetchInterval: 60_000,
    refetchIntervalInBackground: false,
    retry: false,
  });
  if (data?.total_size_bytes === undefined) return null;
  return (
    <span
      className="instance-totals"
      title={
        `${data.total_size_bytes} bytes, ${formatCount(data.total_rows)} ` +
        "rows registered across all live data files (rows are gross of " +
        "deletion-vector masking; refreshed by the server's metrics sampler)"
      }
    >
      {formatBytes(data.total_size_bytes)} ·{" "}
      {formatCompactCount(data.total_rows)} rows
    </span>
  );
}

function HealthIndicator() {
  const { data, isPending } = useQuery({
    queryKey: ["healthz"],
    queryFn: checkHealth,
    refetchInterval: 10_000,
    refetchIntervalInBackground: false,
  });
  const state = isPending ? "unknown" : data ? "ok" : "down";
  const label = isPending ? "checking" : data ? "healthy" : "unreachable";
  const tooltip = isPending
    ? "Polling GET /healthz (every 10s)…"
    : data
      ? "GET /healthz → 200: the server is up and its catalog Postgres " +
        "answered a readiness query (SELECT 1). Polled every 10s."
      : "GET /healthz failed: the server is down, unreachable, or up but " +
        "unable to reach its catalog Postgres (readiness returns 503 on a " +
        "dead pool). Polled every 10s.";
  return (
    <span className={`health health-${state}`} title={tooltip}>
      <span className="health-dot" aria-hidden="true" />
      {label}
    </span>
  );
}

function Breadcrumbs() {
  const { catalog, namespace, table } = useParams();
  return (
    <nav className="breadcrumbs" aria-label="Breadcrumb">
      <Link to="/">catalogs</Link>
      {catalog && (
        <>
          <span className="crumb-sep">/</span>
          <Link to={`/catalogs/${encodeURIComponent(catalog)}`}>{catalog}</Link>
        </>
      )}
      {catalog && namespace && (
        <>
          <span className="crumb-sep">/</span>
          <Link
            to={`/catalogs/${encodeURIComponent(catalog)}/namespaces/${encodeURIComponent(namespace)}`}
          >
            {namespace}
          </Link>
        </>
      )}
      {catalog && namespace && table && (
        <>
          <span className="crumb-sep">/</span>
          <span className="crumb-current">{table}</span>
        </>
      )}
    </nav>
  );
}

export function Layout() {
  return (
    <div className="app">
      <header className="topbar">
        <Link to="/" className="brand">
          hoglake
        </Link>
        <InstanceName />
        <InstanceTotals />
        <Breadcrumbs />
        <div className="topbar-right">
          <Link to="/maintenance">maintenance</Link>
          <Link to="/metrics">metrics</Link>
          <a href="/openapi.yaml" target="_blank" rel="noreferrer">
            openapi.yaml
          </a>
          <ThemeToggle />
          <HealthIndicator />
        </div>
      </header>
      <main className="content">
        <Outlet />
      </main>
    </div>
  );
}
