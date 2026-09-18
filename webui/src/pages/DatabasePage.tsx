import { useQuery } from "@tanstack/react-query";
import {
  getDatabaseHealth,
  type DatabaseFinding,
  type DatabaseIndex,
  type DatabaseTable,
} from "../api/client";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonBlock } from "../components/Skeleton";
import { formatBytes, formatCount, formatTime } from "../lib/format";

// Health of the Postgres the catalog lives in. The findings lead, because
// they are the part that needs no Postgres expertise to act on; the raw
// statistics follow for the operator who wants to check the working.

function percent(fraction: number | undefined): string {
  return fraction === undefined ? "—" : `${Math.round(fraction * 100)}%`;
}

function duration(seconds: number | undefined): string {
  if (seconds === undefined) return "—";
  if (seconds >= 3600) return `${(seconds / 3600).toFixed(1)} h`;
  if (seconds >= 60) return `${(seconds / 60).toFixed(1)} min`;
  return `${seconds.toFixed(0)} s`;
}

function Stat({
  label,
  value,
  hint,
  tone,
}: {
  label: string;
  value: string;
  hint?: string;
  tone?: "warn" | "critical";
}) {
  return (
    <div className={`db-stat${tone ? ` db-stat-${tone}` : ""}`} title={hint}>
      <span className="db-stat-label">{label}</span>
      <span className="db-stat-value">{value}</span>
    </div>
  );
}

function Findings({ findings }: { findings: DatabaseFinding[] }) {
  if (findings.length === 0) {
    return (
      <p className="empty">
        No findings. Thresholds are deliberately quiet — this reads clean
        until something is worth acting on.
      </p>
    );
  }
  return (
    <ul className="db-findings">
      {findings.map((f) => (
        <li key={f.code} className={`db-finding db-finding-${f.severity}`}>
          <div className="db-finding-head">
            <span className={`badge severity-${f.severity}`}>{f.severity}</span>
            <span className="db-finding-title">{f.title}</span>
          </div>
          <p className="db-finding-detail">{f.detail}</p>
          {/* The reason this page exists rather than a link to a generic
              Postgres dashboard: what the number means for this schema. */}
          <p className="db-finding-impact">{f.hoglake_impact}</p>
        </li>
      ))}
    </ul>
  );
}

function Tables({ tables }: { tables: DatabaseTable[] }) {
  if (tables.length === 0) return <p className="empty">No tables.</p>;
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th>table</th>
          <th className="num">live</th>
          <th className="num">dead</th>
          <th className="num">dead %</th>
          <th className="num">heap</th>
          <th className="num">indexes</th>
          <th className="num">seq / idx scans</th>
          <th>last autovacuum</th>
        </tr>
      </thead>
      <tbody>
        {tables.map((t) => (
          <tr key={t.name}>
            <td className="mono">{t.name}</td>
            <td className="num">{formatCount(t.live_tuples)}</td>
            <td className="num">{formatCount(t.dead_tuples)}</td>
            <td
              className={`num${
                t.dead_ratio !== undefined && t.dead_ratio >= 0.2 ? " db-cell-warn" : ""
              }`}
            >
              {percent(t.dead_ratio)}
            </td>
            <td className="num">{formatBytes(t.table_bytes)}</td>
            <td className="num">{formatBytes(t.index_bytes)}</td>
            <td className="num">
              {formatCount(t.seq_scans)} / {formatCount(t.index_scans)}
            </td>
            <td>{t.last_autovacuum ? formatTime(t.last_autovacuum) : "never"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function Indexes({ indexes }: { indexes: DatabaseIndex[] }) {
  if (indexes.length === 0) return <p className="empty">No indexes.</p>;
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th>index</th>
          <th>table</th>
          <th className="num">size</th>
          <th className="num">scans</th>
          <th>role</th>
        </tr>
      </thead>
      <tbody>
        {indexes.map((i) => (
          <tr key={`${i.table}.${i.name}`}>
            <td className="mono">{i.name}</td>
            <td className="mono">{i.table}</td>
            <td className="num">{formatBytes(i.size_bytes)}</td>
            <td className={`num${String(i.scans) === "0" ? " db-cell-warn" : ""}`}>
              {formatCount(i.scans)}
            </td>
            <td>
              {/* An unscanned unique index is still enforcing a
                  constraint — saying so stops it reading as dead weight. */}
              {i.constraint_backing ? (
                <span className="badge">constraint</span>
              ) : (
                ""
              )}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export function DatabasePage() {
  const { data, error, isPending, refetch, isFetching } = useQuery({
    queryKey: ["database-health"],
    queryFn: getDatabaseHealth,
    // One snapshot on demand, like the metrics page: no polling, because
    // every call is a round of statistics queries on the instance this
    // page exists to avoid loading.
    refetchOnWindowFocus: false,
  });

  if (error) return <ErrorBox error={error} />;
  if (isPending) return <SkeletonBlock />;

  const { server, activity, tables, indexes, findings } = data;
  const xidUsed = Number(server.xid_age) / Number(server.xid_freeze_max_age);
  const connectionUse = server.connections_used / server.connections_max;

  return (
    <section>
      <div className="page-head">
        <h2>Database</h2>
        <button type="button" onClick={() => refetch()} disabled={isFetching}>
          {isFetching ? "Refreshing…" : "Refresh"}
        </button>
      </div>
      <p className="subtle">
        Postgres {server.version} · {server.database} ·{" "}
        {formatBytes(server.size_bytes)}
        {server.started_at ? ` · up since ${formatTime(server.started_at)}` : ""}
        {/* Cumulative counters are only as meaningful as the period they
            cover, so the uptime sits beside them deliberately. */}
      </p>

      <section>
        <h3>Findings</h3>
        <Findings findings={findings} />
      </section>

      <section>
        <h3>Instance</h3>
        <div className="db-stats">
          <Stat
            label="connections"
            value={`${server.connections_used} / ${server.connections_max}`}
            tone={connectionUse >= 0.8 ? "warn" : undefined}
            hint="In use against max_connections."
          />
          <Stat
            label="cache hit"
            value={percent(server.cache_hit_ratio)}
            tone={
              server.cache_hit_ratio !== undefined && server.cache_hit_ratio < 0.95
                ? "warn"
                : undefined
            }
            hint="Block reads served from the buffer cache, since the last statistics reset."
          />
          <Stat
            label="xid age"
            value={percent(xidUsed)}
            tone={xidUsed >= 0.8 ? "critical" : xidUsed >= 0.5 ? "warn" : undefined}
            hint={`age(datfrozenxid) ${formatCount(server.xid_age)} of ${formatCount(
              server.xid_freeze_max_age,
            )} before a forced freeze.`}
          />
          <Stat
            label="autovacuum"
            value={server.autovacuum_enabled ? "on" : "off"}
            tone={server.autovacuum_enabled ? undefined : "critical"}
          />
          <Stat label="committed" value={formatCount(server.committed)} />
          <Stat label="rolled back" value={formatCount(server.rolled_back)} />
          <Stat
            label="deadlocks"
            value={formatCount(server.deadlocks)}
            tone={String(server.deadlocks) === "0" ? undefined : "warn"}
          />
        </div>
      </section>

      <section>
        <h3>Activity</h3>
        <div className="db-stats">
          <Stat label="active" value={String(activity.active)} />
          <Stat label="idle" value={String(activity.idle)} />
          <Stat
            label="idle in transaction"
            value={String(activity.idle_in_transaction)}
            tone={activity.idle_in_transaction > 0 ? "warn" : undefined}
            hint="An open transaction pins the vacuum horizon for the whole database."
          />
          <Stat
            label="blocked on locks"
            value={String(activity.waiting)}
            tone={activity.waiting > 0 ? "warn" : undefined}
          />
          <Stat
            label="longest transaction"
            value={duration(activity.longest_transaction_seconds)}
          />
          <Stat
            label="longest idle in txn"
            value={duration(activity.longest_idle_in_transaction_seconds)}
          />
          <Stat
            label="longest lock wait"
            value={duration(activity.longest_wait_seconds)}
          />
        </div>
      </section>

      <section>
        <h3>Tables</h3>
        <Tables tables={tables} />
      </section>

      <section>
        <h3>Indexes</h3>
        <Indexes indexes={indexes} />
      </section>
    </section>
  );
}
