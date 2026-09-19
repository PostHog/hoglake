// Shared pieces of the maintenance pages (central /maintenance and
// per-catalog /catalogs/:catalog/maintenance): run badges, the per-task
// outcome summaries, run-table formatting, and the paged runs table that
// serves both scopes (per-catalog and instance-wide).

import { Link } from "react-router-dom";
import { useInfiniteQuery } from "@tanstack/react-query";
import {
  ApiError,
  listInstanceMaintenanceRuns,
  listMaintenanceRuns,
} from "../api/client";
import type { Int64, MaintenanceRun } from "../api/types";
import { ErrorBox } from "./ErrorBox";
import { SkeletonRows } from "./Skeleton";
import { formatBytes, formatCount, formatTime } from "../lib/format";
import { useStoredPref } from "../lib/prefs";

/** Loop cadence for display: "500ms", "5s", "1m". Small values only. */
export function formatInterval(ms: Int64): string {
  const v = Number(ms);
  if (v < 1000) return `${v}ms`;
  const s = v / 1000;
  if (s % 60 === 0) return `${s / 60}m`;
  return `${s}s`;
}

/** Run duration from the ledger timestamps: "120ms", "1.2s". */
export function formatRunDuration(run: MaintenanceRun): string {
  const ms =
    new Date(run.finished_at).getTime() - new Date(run.started_at).getTime();
  if (Number.isNaN(ms) || ms < 0) return "—";
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

export function RunStateBadge({ status }: { status: "ok" | "failed" }) {
  // Reuses the stats-state badge colors: ok green, failed red.
  return (
    <span className={`badge stats-${status === "ok" ? "provided" : "failed"}`}>
      {status}
    </span>
  );
}

export function isLoopDisabled(interval: Int64 | undefined): boolean {
  return interval !== undefined && BigInt(interval) <= 0n;
}

function positive(value: Int64 | undefined): boolean {
  return value !== undefined && BigInt(value) > 0n;
}

/**
 * True when a run both SUCCEEDED and changed nothing — the predicate behind
 * the runs table's "hide runs that did nothing" toggle. Every pod records a
 * row for every sweep of every catalog, so on a busy instance the ledger is
 * mostly these; the point of the toggle is to let the few informative rows
 * surface.
 *
 * Derived per task from the result payload's own counters (the same fields
 * RunSummary renders, read structurally — never from the rendered text), and
 * three rules keep it honest:
 *
 *  - A run whose status is not "ok" is never quiet. Failures are never noise.
 *  - A warning or skip counter makes a run loud even when its headline counts
 *    are all zero: compaction's failed_groups / skipped_conflicts /
 *    dv_superseded / unconvertible_schema / invalid_data, cleanup's
 *    still_referenced, expiry's floored_by_consumer, a verify report that
 *    failed or counted violations. Those are exactly the runs where
 *    "0 groups" is the interesting part.
 *  - A null result is NOT quiet. Hiding needs positive evidence that nothing
 *    happened, and a missing payload is the absence of evidence — it is what
 *    a run recorded without one, or by a build that predates a counter, looks
 *    like. Ambiguity stays on screen.
 *
 * Expiry's new_earliest_snapshot_id is deliberately ignored: it reports where
 * the floor STANDS, which every sweep does whether or not the floor moved.
 */
export function isQuietRun(run: MaintenanceRun): boolean {
  if (run.status !== "ok") return false;
  switch (run.task) {
    case "hydrator": {
      const r = run.result;
      if (!r) return false;
      // Manual rows are rehydrate calls; loop rows are sweeps.
      if ("requeued" in r) return !positive(r.requeued);
      if ("claimed" in r) {
        return ![r.claimed, r.hydrated, r.failed, r.transient].some(positive);
      }
      return false;
    }
    case "expiry": {
      const r = run.result;
      if (!r) return false;
      return (
        ![r.snapshots_expired, r.data_files_queued, r.delete_files_queued].some(
          positive,
        ) && !r.floored_by_consumer
      );
    }
    case "cleanup": {
      const r = run.result;
      if (!r) return false;
      return ![r.removed, r.missing, r.still_referenced].some(positive);
    }
    case "compaction": {
      const r = run.result;
      if (!r) return false;
      return ![
        r.groups_compacted,
        r.files_in,
        r.files_out,
        r.bytes_in,
        r.bytes_out,
        r.skipped_conflicts,
        r.dv_superseded,
        r.unconvertible_schema,
        r.invalid_data,
        r.heap_budget_exceeded,
        r.failed_groups,
      ].some(positive);
    }
    case "verify": {
      const r = run.result;
      if (!r) return false;
      return r.status === "pass" && !r.checks.some((c) => positive(c.violations));
    }
  }
}

/** Invocation success does not imply that the work/checks succeeded. */
export function RunOutcomeBadge({ run }: { run: MaintenanceRun }) {
  const issues =
    (run.task === "compaction" && positive(run.result?.failed_groups)) ||
    (run.task === "cleanup" && positive(run.result?.still_referenced)) ||
    (run.task === "verify" && run.result?.status === "fail") ||
    (run.task === "hydrator" && run.result && "failed" in run.result && positive(run.result.failed));
  if (run.status === "ok" && issues) {
    return <span className="badge stats-failed" title="Invocation completed, but its result contains failures or violations">issues</span>;
  }
  return <RunStateBadge status={run.status} />;
}

/**
 * The one-line outcome of a run: the payload's headline numbers, or the
 * error for a failed run. Hydrator loop rows carry sweep counts; its manual
 * rows are rehydrate calls.
 */
export function RunSummary({ run }: { run: MaintenanceRun }) {
  if (run.status === "failed") {
    return (
      <span className="run-error" title={run.error}>
        {run.error}
      </span>
    );
  }
  switch (run.task) {
    case "hydrator": {
      const r = run.result;
      if (!r) return <span className="empty">—</span>;
      if (run.trigger === "manual" && "requeued" in r) {
        return <>requeued {formatCount(r.requeued)} failed files</>;
      }
      if ("claimed" in r) {
        return (
          <>
            claimed {formatCount(r.claimed)}, hydrated {formatCount(r.hydrated)}
            {r.failed !== "0" && <>, failed {formatCount(r.failed)}</>}
            {r.transient !== "0" && <>, transient {formatCount(r.transient)}</>}
          </>
        );
      }
      return <span className="empty">—</span>;
    }
    case "expiry": {
      const r = run.result;
      if (!r) return <span className="empty">—</span>;
      return (
        <>
          expired {formatCount(r.snapshots_expired)} snapshots, queued{" "}
          {formatCount(r.data_files_queued)} files, floor{" "}
          <span className="mono">{r.new_earliest_snapshot_id}</span>
          {r.floored_by_consumer && (
            <span className="badge badge-warn">
              floored by {r.floored_by_consumer}
            </span>
          )}
        </>
      );
    }
    case "cleanup": {
      const r = run.result;
      if (!r) return <span className="empty">—</span>;
      return (
        <>
          removed {formatCount(r.removed)}, absent {formatCount(r.missing)}
          {r.still_referenced !== "0" && (
            <span className="badge stats-failed">
              still-referenced {formatCount(r.still_referenced)}
            </span>
          )}
        </>
      );
    }
    case "compaction": {
      const r = run.result;
      if (!r) return <span className="empty">—</span>;
      return (
        <>
          {formatCount(r.groups_compacted)} groups, {formatCount(r.files_in)}→
          {formatCount(r.files_out)} files, {formatBytes(r.bytes_in)}→
          {formatBytes(r.bytes_out)}
          {positive(r.failed_groups) && (
            <span className="badge stats-failed">
              {formatCount(r.failed_groups)} failed (logged; retried next run)
            </span>
          )}
          {(positive(r.skipped_conflicts) ||
            positive(r.dv_superseded) ||
            positive(r.unconvertible_schema) ||
            positive(r.invalid_data) ||
            positive(r.heap_budget_exceeded)) && (
            <span className="badge badge-warn">
              skipped {formatCount(r.skipped_conflicts)}, dv-superseded{" "}
              {formatCount(r.dv_superseded)}, unconvertible{" "}
              {formatCount(r.unconvertible_schema)}, invalid-data{" "}
              {formatCount(r.invalid_data ?? "0")}, heap-budget{" "}
              {formatCount(r.heap_budget_exceeded ?? "0")}
            </span>
          )}
        </>
      );
    }
    case "verify": {
      const r = run.result;
      if (!r) return <span className="empty">—</span>;
      const failing = r.checks.filter((c) => c.status !== "pass");
      return (
        <>
          <RunStateBadge status={r.status === "pass" ? "ok" : "failed"} />
          {failing.length > 0 && (
            <span className="mono">
              {failing.map((c) => `${c.check}(${c.violations})`).join(" ")}
            </span>
          )}
        </>
      );
    }
  }
}

const RUNS_PAGE_SIZE = 20;

/**
 * The runs table's task filter: the spec's MaintenanceTask vocabulary, plus
 * the "all" default the table opens on.
 */
export const RUN_TASK_FILTERS = [
  "all",
  "hydrator",
  "expiry",
  "cleanup",
  "compaction",
  "verify",
] as const;
export type RunTaskFilter = (typeof RUN_TASK_FILTERS)[number];

// Both filters persist like the theme does, and under the same key prefix.
// They are a property of the operator's session, not of a catalog: the same
// person reading the central feed and a per-catalog page wants the same view
// in both, so the keys are deliberately not scoped by catalog.
const TASK_FILTER_KEY = "hoglake-runs-task";
const HIDE_QUIET_KEY = "hoglake-runs-hide-quiet";
const SWITCH = ["on", "off"] as const;

const HIDE_QUIET_TITLE =
  "Hide runs that succeeded and changed nothing: every pod records a row " +
  "for every sweep of every catalog, so most rows are no-ops. A failed run " +
  "is never hidden, and neither is one carrying a warning — compaction " +
  "failures or skips, still-referenced cleanup entries, an expiry floored " +
  "by a consumer — even when its counts are all zero.";

/**
 * The paged run ledger, newest first ("Load more" pages down via the
 * `before` cursor). With `catalog` it reads the per-catalog endpoint;
 * without it the instance-wide feed, adding a catalog column that links
 * to the catalog's own maintenance page.
 */
export function RunsTable({ catalog }: { catalog?: string }) {
  // Filter state lives outside the query key on purpose: filtering is a
  // view over the pages already fetched, so changing it refetches nothing
  // and a poll refresh cannot reset it.
  const [taskFilter, setTaskFilter] = useStoredPref(
    TASK_FILTER_KEY,
    RUN_TASK_FILTERS,
    "all",
  );
  const [hideQuiet, setHideQuiet] = useStoredPref(HIDE_QUIET_KEY, SWITCH, "off");
  const query = useInfiniteQuery({
    queryKey: ["maintenance-runs", catalog ?? "<instance>"],
    queryFn: ({ pageParam }) =>
      catalog
        ? listMaintenanceRuns(catalog, {
            before: pageParam,
            limit: RUNS_PAGE_SIZE,
          })
        : listInstanceMaintenanceRuns({
            before: pageParam,
            limit: RUNS_PAGE_SIZE,
          }),
    initialPageParam: undefined as Int64 | undefined,
    refetchInterval: 5_000,
    refetchIntervalInBackground: false,
    getNextPageParam: (lastPage) =>
      lastPage.has_more && lastPage.runs.length > 0
        ? lastPage.runs[lastPage.runs.length - 1].run_id
        : undefined,
  });

  // Both filters are client-side over the pages already loaded — the
  // /maintenance/runs endpoint has no task or outcome parameter. "Load
  // more" therefore still pages 20 RAW runs at a time, of which a narrow
  // filter may keep few; that is the honest behaviour (the pager reflects
  // the ledger, not the view) and the hidden count below says so.
  const fetched = (query.data?.pages ?? []).flatMap((p) => p.runs);
  const byTask =
    taskFilter === "all"
      ? fetched
      : fetched.filter((run) => run.task === taskFilter);
  const runs = hideQuiet === "on" ? byTask.filter((r) => !isQuietRun(r)) : byTask;
  const quietHidden = byTask.length - runs.length;
  const cols = catalog ? 7 : 8;

  return (
    <section className="panel runs-panel">
      <h3>Recent runs</h3>
      <div className="form-row runs-filters">
        <label className="check" title={HIDE_QUIET_TITLE}>
          <input
            type="checkbox"
            checked={hideQuiet === "on"}
            onChange={(e) => setHideQuiet(e.target.checked ? "on" : "off")}
          />
          Hide runs that did nothing
        </label>
        <label>
          task
          <select
            value={taskFilter}
            onChange={(e) => setTaskFilter(e.target.value as RunTaskFilter)}
            aria-label="task filter"
          >
            {RUN_TASK_FILTERS.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </label>
        {hideQuiet === "on" && (
          // Say what was removed rather than silently shrinking the list:
          // a short table under a filter must not read as "the loops stopped".
          <span className="subtle runs-hidden-count">
            {quietHidden} quiet run{quietHidden === 1 ? "" : "s"} hidden
          </span>
        )}
      </div>
      {query.isError && <ErrorBox error={query.error} />}
      {query.isError && !query.data && (
        <button type="button" onClick={() => void query.refetch()}>Retry runs</button>
      )}
      {(!query.isError || query.data) && (
        <>
          <table className="data-table">
            <thead>
              <tr>
                <th className="num">run</th>
                {!catalog && <th>catalog</th>}
                <th>task</th>
                <th>trigger</th>
                <th>started</th>
                <th>duration</th>
                <th>status</th>
                <th>outcome</th>
              </tr>
            </thead>
            {query.isPending ? (
              <SkeletonRows rows={5} cols={cols} />
            ) : (
              <tbody>
                {runs.length === 0 && (
                  <tr>
                    <td colSpan={cols} className="empty">
                      {fetched.length === 0
                        ? "No runs recorded yet."
                        : "No runs match these filters."}
                    </td>
                  </tr>
                )}
                {runs.map((run) => (
                  <tr key={run.run_id}>
                    <td className="num mono">{run.run_id}</td>
                    {!catalog && (
                      <td>
                        <Link
                          to={`/catalogs/${encodeURIComponent(run.catalog)}/maintenance`}
                        >
                          {run.catalog}
                        </Link>
                      </td>
                    )}
                    <td className="mono">{run.task}</td>
                    <td>{run.trigger}</td>
                    <td className="mono" title={run.started_at}>
                      {formatTime(run.started_at)}
                    </td>
                    <td className="mono">{formatRunDuration(run)}</td>
                    <td>
                      <RunOutcomeBadge run={run} />
                    </td>
                    <td>
                      <RunSummary run={run} />
                    </td>
                  </tr>
                ))}
              </tbody>
            )}
          </table>
          {query.hasNextPage && (
            <button
              type="button"
              className="load-more"
              onClick={() => void query.fetchNextPage()}
              disabled={query.isFetchingNextPage}
            >
              {query.isFetchingNextPage ? "Loading…" : query.isFetchNextPageError ? "Retry loading more" : "Load more"}
            </button>
          )}
        </>
      )}
    </section>
  );
}

/**
 * Version skew detector: a hoglake server that predates the run ledger
 * has no /maintenance/* routes at all, so it answers its bare
 * unmatched-route 404 (no typed {"error": "not_found"} body — that shape
 * means the CATALOG is missing, a different story the ErrorBox tells
 * accurately).
 */
export function isLedgerUnavailable(error: unknown): boolean {
  return (
    error instanceof ApiError &&
    error.status === 404 &&
    error.detail === undefined
  );
}

/** The skew notice body, shared so both pages phrase it identically. */
export function LedgerUnavailableNotice() {
  return (
    <p className="notice-banner" role="status">
      This server build predates the maintenance ledger (/maintenance/status
      and /maintenance/runs). Deploy a server that includes them to see task
      status and run history here.
    </p>
  );
}
