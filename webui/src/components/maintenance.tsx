// Shared pieces of the maintenance pages (central /maintenance and
// per-catalog /catalogs/:catalog/maintenance): run badges, the per-task
// outcome summaries, run-table formatting, and the paged runs table that
// serves both scopes (per-catalog and instance-wide).

import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { useInfiniteQuery } from "@tanstack/react-query";
import {
  ApiError,
  listInstanceMaintenanceRuns,
  listMaintenanceRuns,
} from "../api/client";
import type { Int64, LoopObservation, MaintenanceRun } from "../api/types";
import { ErrorBox } from "./ErrorBox";
import { SkeletonRows } from "./Skeleton";
import { formatBytes, formatCount, formatTime } from "../lib/format";
import { useStoredPref } from "../lib/prefs";

/**
 * Coarse duration: "45s", "12min", "3h", "2d". Rounded, never exact.
 *
 * Minutes are "min", not "m". These durations sit in column headers that
 * CSS upper-cases, and beside tables of counts a bare "M" reads as the
 * SI mega prefix — "EVERY ~17M" looks like 17 million of something. The
 * database page's own duration helper already says "min"; the other
 * units have no such collision.
 */
export function formatSeconds(seconds: number): string {
  if (seconds < 120) return `${Math.round(seconds)}s`;
  const m = seconds / 60;
  if (m < 120) return `${Math.round(m)}min`;
  const h = m / 60;
  if (h < 48) return `${Math.round(h)}h`;
  return `${Math.round(h / 24)}d`;
}

/**
 * An OBSERVED interval is never a round number — a loop sleeps its
 * interval after the body, so a 60s loop with an 8s sweep is measured
 * at 68s — so it is rounded for display rather than printed exactly
 * (a configured value would print exactly).
 */
function formatObservedInterval(ms: Int64): string {
  const v = Number(ms);
  return v < 1000 ? `${Math.round(v)}ms` : formatSeconds(v / 1000);
}

function ageSeconds(iso: string): number {
  return Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
}

/**
 * The header's answer to "is this task running?", taken from the run
 * ledger and therefore true of the whole fleet.
 *
 * NEVER derive this from loop_interval_ms. That is the config of
 * whichever process answered the request, and a deployment may run the
 * loop somewhere else — gigahog runs compaction only on its maintenance
 * pod, so the API pod's config reads "disabled" for a task that is
 * succeeding every minute (#114).
 *
 * Returns null when the server did not report an observation at all (an
 * older build): there is then nothing honest to say, and "no loop runs"
 * would be a claim the response never made.
 */
export function loopCadence(loop: LoopObservation | null | undefined): string | null {
  if (loop === undefined) return null;
  if (loop === null) return "manual only";
  if (loop.observed_interval_ms !== undefined) {
    return `every ~${formatObservedInterval(loop.observed_interval_ms)}`;
  }
  if (loop.last_run_at !== undefined) {
    return `last loop run ${formatSeconds(ageSeconds(loop.last_run_at))} ago`;
  }
  // Silence means different things per task, and the response says
  // which. The hydrator records the catalogs it claimed files for, not
  // its sweeps, so an idle catalog has no rows while the loop is
  // perfectly healthy — calling that "no loop runs" would repeat the
  // bug in a new place.
  return loop.records_every_sweep === false ? "nothing to do here" : "no loop runs";
}

/**
 * True when the ledger cannot show a loop currently running this task.
 * An absent observation (older server) is not evidence of anything and
 * must not raise a warning.
 */
export function noRunningLoop(loop: LoopObservation | null | undefined): boolean {
  return loop !== undefined && loop !== null && loop.observed_interval_ms === undefined;
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
 * How many rows the quiet filter tries to put on screen before it stops
 * following the `before` cursor, and how many extra requests it may spend
 * doing so. The product is the worst case a single filter activation can
 * cost the server: 10 x 20 = 200 rows walked, then it gives up and says so.
 *
 * The bound is not a nicety. A catalog whose loops are all idle records
 * nothing BUT quiet runs, so "keep paging until enough rows are visible"
 * has no natural end — it would walk the whole retention window, one
 * request per 20 rows, every time someone opened the page.
 */
const RUNS_AUTOPAGE_TARGET = RUNS_PAGE_SIZE;
const RUNS_AUTOPAGE_MAX_REQUESTS = 10;

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
  "by a consumer — even when its counts are all zero. The table pages back " +
  "through the ledger to fill a screen, up to a fixed number of requests, " +
  "then says how far back it looked.";

/**
 * The paged run ledger, newest first ("Load more" pages down via the
 * `before` cursor). With `catalog` it reads the per-catalog endpoint;
 * without it the instance-wide feed, adding a catalog column that links
 * to the catalog's own maintenance page.
 *
 * The two filters sit on opposite sides of the wire, and deliberately so.
 *
 * The TASK filter is the endpoint's own `?task=` parameter, so it is part
 * of the query key: the server returns 20 rows OF THAT TASK per page, and
 * the `before` cursor pages within them. Filtering an already-fetched page
 * by task instead (what this table did first) makes the page size mean
 * "20 runs of any task", so a task that runs rarely can be absent from
 * every window the table ever holds.
 *
 * "Hide quiet" stays CLIENT-side, and the table follows the cursor itself
 * until enough rows survive it. isQuietRun is a subtle predicate — per-task
 * warning counters, the never-hide rules for failures and null results,
 * fields the row renderer does not even show — and a second implementation
 * of it in Kotlin would be free to drift from this one with nothing to red
 * when it did. One predicate, in the language that also renders the rows.
 */
export function RunsTable({ catalog }: { catalog?: string }) {
  const [taskFilter, setTaskFilter] = useStoredPref(
    TASK_FILTER_KEY,
    RUN_TASK_FILTERS,
    "all",
  );
  const [hideQuiet, setHideQuiet] = useStoredPref(HIDE_QUIET_KEY, SWITCH, "off");
  const task = taskFilter === "all" ? undefined : taskFilter;
  const query = useInfiniteQuery({
    queryKey: ["maintenance-runs", catalog ?? "<instance>", taskFilter],
    queryFn: ({ pageParam }) =>
      catalog
        ? listMaintenanceRuns(catalog, {
            task,
            before: pageParam,
            limit: RUNS_PAGE_SIZE,
          })
        : listInstanceMaintenanceRuns({
            task,
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

  const fetched = (query.data?.pages ?? []).flatMap((p) => p.runs);
  const runs = hideQuiet === "on" ? fetched.filter((r) => !isQuietRun(r)) : fetched;
  const quietHidden = fetched.length - runs.length;
  const cols = catalog ? 7 : 8;

  // The auto-pager's request budget. It is spent following the cursor while
  // the quiet filter has nothing to show, and it re-arms only when the
  // search SETTLES — enough rows visible, or the ledger exhausted. A poll
  // refresh therefore cannot re-arm it: on a catalog that is quiet all the
  // way down, the search runs once and then stays stopped.
  const searchKey = `${catalog ?? ""} ${taskFilter} ${hideQuiet}`;
  const [armedFor, setArmedFor] = useState(searchKey);
  const [spent, setSpent] = useState(0);
  if (armedFor !== searchKey) {
    // Changing a filter is the user asking again; re-arm before this render
    // commits (React's documented adjust-state-on-change pattern).
    setArmedFor(searchKey);
    setSpent(0);
  }
  const searching = hideQuiet === "on" && runs.length < RUNS_AUTOPAGE_TARGET;
  const { fetchNextPage, hasNextPage, isFetching } = query;
  useEffect(() => {
    if (!searching || isFetching) return;
    if (!hasNextPage) {
      if (spent !== 0) setSpent(0);
      return;
    }
    if (spent >= RUNS_AUTOPAGE_MAX_REQUESTS) return;
    setSpent((n) => n + 1);
    void fetchNextPage();
  }, [searching, isFetching, hasNextPage, spent, fetchNextPage]);
  useEffect(() => {
    // Settled with a full screen: give the budget back for the next time
    // arrivals push the interesting rows off the bottom of the window.
    if (hideQuiet === "on" && !searching && spent !== 0) setSpent(0);
  }, [hideQuiet, searching, spent]);
  const gaveUp =
    searching && hasNextPage && !isFetching && spent >= RUNS_AUTOPAGE_MAX_REQUESTS;

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
          // Say what was removed rather than silently shrinking the list: a
          // short table under a filter must not read as "the loops stopped".
          // And say what the number COVERS whenever older runs remain
          // unread — a bare "20 quiet runs hidden" over an empty table
          // claims the filter found nothing interesting in the ledger, when
          // all it found was nothing interesting in the last 20 rows.
          <span className="subtle runs-hidden-count">
            {quietHidden} quiet run{quietHidden === 1 ? "" : "s"} hidden
            {hasNextPage && ` of the newest ${fetched.length} runs searched`}
            {gaveUp && " — Load more to look further back"}
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
              onClick={() => {
                // An explicit click re-arms the auto-pager: the operator is
                // asking again, so the search may spend its budget again.
                setSpent(0);
                void query.fetchNextPage();
              }}
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
