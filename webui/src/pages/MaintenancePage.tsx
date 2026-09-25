import { Link, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { getMaintenanceStatus, runVerify } from "../api/client";
import type {
  Int64,
  MaintenanceRun,
  MaintenanceTaskStatus,
  VerifyCheck,
  VerifyReport,
} from "../api/types";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonBlock } from "../components/Skeleton";
import {
  LedgerUnavailableNotice,
  RunOutcomeBadge,
  RunStateBadge,
  RunSummary,
  RunsTable,
  formatRunDuration,
  formatSeconds,
  isLedgerUnavailable,
  loopCadence,
} from "../components/maintenance";
import { formatBytes, formatCount, formatTime } from "../lib/format";

/** Retention seconds are int64 strings; only the magnitude is displayed. */
function formatRetention(seconds: Int64): string {
  return formatSeconds(Number(seconds));
}

function LastRun({ run }: { run: MaintenanceRun | null }) {
  if (!run) return <p className="empty">No recorded run yet.</p>;
  return (
    <p className="last-run">
      <RunOutcomeBadge run={run} />
      <span className="badge">{run.trigger}</span>
      <span className="mono" title={run.started_at}>
        {formatTime(run.started_at)}
      </span>
      <span className="subtle">in {formatRunDuration(run)}</span>
      <RunSummary run={run} />
    </p>
  );
}

/** Per-task backlog lines (live counts from the catalog, not the ledger). */
function Backlog({
  status,
  catalog,
}: {
  status: MaintenanceTaskStatus;
  catalog: string;
}) {
  switch (status.task) {
    case "hydrator":
      return (
        <dl className="stats-header">
          <div>
            <dt>pending files</dt>
            <dd className="mono">{formatCount(status.backlog.pending_files)}</dd>
          </div>
          <div>
            <dt>failed files</dt>
            <dd
              className={
                status.backlog.failed_files !== undefined && status.backlog.failed_files !== "0" ? "mono backlog-bad" : "mono"
              }
              title="Hydration failed loudly; requeue with POST /maintenance/rehydrate after fixing the cause"
            >
              {formatCount(status.backlog.failed_files)}
            </dd>
          </div>
        </dl>
      );
    case "expiry":
      return (
        <dl className="stats-header">
          <div>
            <dt>retention</dt>
            <dd className="mono">
              {status.backlog.snapshot_retention_seconds !== undefined
                ? formatRetention(status.backlog.snapshot_retention_seconds)
                : "disabled"}
            </dd>
          </div>
          <div>
            <dt>consumer floor</dt>
            <dd className="mono">
              {status.backlog.consumer_floor ? "on" : "off"}
            </dd>
          </div>
          <div>
            <dt>earliest snapshot</dt>
            <dd className="mono">{status.backlog.earliest_snapshot_id}</dd>
          </div>
          <div>
            <dt>head snapshot</dt>
            <dd className="mono">{status.backlog.head_snapshot_id}</dd>
          </div>
        </dl>
      );
    case "cleanup":
      return (
        <dl className="stats-header">
          <div>
            <dt>queued removals</dt>
            <dd className="mono">{formatCount(status.backlog.queued_removals)}</dd>
          </div>
          <div>
            <dt>oldest queued</dt>
            <dd className="mono">
              {status.backlog.oldest_queued_age_seconds !== undefined
                ? formatSeconds(status.backlog.oldest_queued_age_seconds)
                : "—"}
            </dd>
          </div>
        </dl>
      );
    case "compaction":
      return (
        <dl className="stats-header">
          <div>
            <dt>
              small files{" "}
              <span className="subtle">
                (&lt; {formatBytes(status.backlog.target_bytes)})
              </span>
            </dt>
            <dd className="mono">{formatCount(status.backlog.small_files)}</dd>
          </div>
          <div>
            <dt>detail</dt>
            <dd>
              <Link to={`/catalogs/${encodeURIComponent(catalog)}/partitions`}>
                compaction debt
              </Link>
            </dd>
          </div>
        </dl>
      );
    case "verify":
      return <VerifyPanel catalog={catalog} />;
    case "retirement":
      // Deliberately no numbers: this task has no backlog the dashboard
      // path may compute (see RetirementBacklog). What it has instead is
      // the rule an operator needs to know before reading the run rows
      // below, because "0 rows retired, forever" is a CORRECT answer on
      // a catalog with no retention and looks identical to a broken loop.
      return (
        <p className="empty">
          Deletes the file rows a dropped table left behind, in paced batches,
          queueing each object for the cleanup drain. A table only becomes
          eligible once its drop snapshot has sunk to the catalog&rsquo;s
          expiry floor — above the floor its rows are still readable by time
          travel — so a catalog with no snapshot retention never retires
          anything. The runs below are what this task did.
        </p>
      );
  }
}

/** One check of a report: outcome, count, samples, and the invariant. */
function VerifyCheckRow({ check }: { check: VerifyCheck }) {
  const failed = check.status !== "pass";
  return (
    <li className="verify-check" data-check={check.check} data-status={check.status}>
      <p className="verify-check-head">
        <RunStateBadge status={failed ? "failed" : "ok"} />
        <span className="mono">{check.check}</span>
        <span className={failed ? "mono backlog-bad" : "mono subtle"}>
          {formatCount(check.violations)} violations
        </span>
      </p>
      {/* Absent on a report from a build that predates the field; an
          empty paragraph would read as "this check has no invariant". */}
      {check.description && (
        <p className="verify-check-why subtle">{check.description}</p>
      )}
      {check.samples.length > 0 && (
        <ul className="verify-samples">
          {check.samples.map((sample, i) => (
            <li key={i} className="mono">
              {sample}
            </li>
          ))}
        </ul>
      )}
    </li>
  );
}

/**
 * The rendered report: overall status, then every check — failures
 * first, because a passing check is the uninteresting case and eleven of
 * them would bury the one that matters.
 */
function VerifyReportView({ report }: { report: VerifyReport }) {
  const ordered = [
    ...report.checks.filter((c) => c.status !== "pass"),
    ...report.checks.filter((c) => c.status === "pass"),
  ];
  return (
    <div className="verify-report">
      <p className="verify-status">
        <RunStateBadge status={report.status === "pass" ? "ok" : "failed"} />
        <span className="subtle">
          {report.status === "pass"
            ? "every check passed"
            : `${report.checks.filter((c) => c.status !== "pass").length} of ${report.checks.length} checks failed`}
        </span>
      </p>
      <ul className="verify-checks">
        {ordered.map((c) => (
          <VerifyCheckRow key={c.check} check={c} />
        ))}
      </ul>
    </div>
  );
}

/**
 * Verify has no backlog to show — it scans on demand — so its panel is
 * the trigger and the last report this page ran. The run is also
 * recorded in the ledger, so the runs table below picks it up; the
 * invalidation is what makes that appear without a manual reload.
 */
function VerifyPanel({ catalog }: { catalog: string }) {
  const queryClient = useQueryClient();
  const mutation = useMutation({
    mutationFn: () => runVerify(catalog),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["maintenance-runs"] });
      void queryClient.invalidateQueries({
        queryKey: ["maintenance-status", catalog],
      });
    },
  });
  return (
    <div className="verify-panel">
      <p className="subtle">
        Nothing queued — verify scans metadata only, on demand or on its
        loop.
      </p>
      <button
        type="button"
        onClick={() => mutation.mutate()}
        disabled={mutation.isPending}
      >
        {mutation.isPending ? "Verifying…" : "Verify"}
      </button>
      {mutation.isError && <ErrorBox error={mutation.error} />}
      {mutation.data && <VerifyReportView report={mutation.data} />}
    </div>
  );
}

function TaskPanel({
  status,
  catalog,
}: {
  status: MaintenanceTaskStatus;
  catalog: string;
}) {
  return (
    <section className="panel task-panel" data-task={status.task}>
      <h3>
        {status.task} <span className="subtle">{loopCadence(status.loop)}</span>
      </h3>
      <Backlog status={status} catalog={catalog} />
      <LastRun run={status.last_run} />
    </section>
  );
}

export function MaintenancePage() {
  const { catalog } = useParams();
  const status = useQuery({
    queryKey: ["maintenance-status", catalog],
    queryFn: () => getMaintenanceStatus(catalog!),
    enabled: Boolean(catalog),
    refetchInterval: 5_000,
    refetchIntervalInBackground: false,
  });
  if (!catalog) return null;

  // On a ledger-less (older) server: one notice, and don't fire the runs
  // query into the same void.
  const ledgerUnavailable = status.isError && isLedgerUnavailable(status.error);

  return (
    <section>
      <div className="page-head">
        <h2>
          Maintenance <span className="subtle">{catalog}</span>
        </h2>
      </div>
      {status.isError &&
        (ledgerUnavailable ? (
          <LedgerUnavailableNotice />
        ) : (
          <ErrorBox error={status.error} />
        ))}
      {status.isPending && <SkeletonBlock />}
      {status.data && (
        <p className="subtle">
          {status.data.sampled_at
            ? `Backlog sampled ${formatTime(status.data.sampled_at)} (snapshot ${status.data.sampled_snapshot_id ?? "—"}).`
            : "Backlog summary warming up — counts are unknown until the first sample completes."}
        </p>
      )}
      {status.data && (
        <div className="task-grid">
          {status.data.tasks.map((t) => (
            <TaskPanel key={t.task} status={t} catalog={catalog} />
          ))}
        </div>
      )}
      {status.data && <RunsTable catalog={catalog} />}
    </section>
  );
}
