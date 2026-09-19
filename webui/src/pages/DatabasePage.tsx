import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  getDatabaseHealth,
  type CommitLockHolder,
  type DatabaseFinding,
  type DatabaseIndex,
  type DatabaseTable,
  type ReplicationSlot,
} from "../api/client";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonBlock } from "../components/Skeleton";
import { formatBytes, formatCount, formatTime } from "../lib/format";
import { applySort, cmpText, int64Column, nextSort, textColumn } from "../lib/sort";
import type { ColumnSort, SortState } from "../lib/sort";
import { SortableTh } from "../components/SortableTh";

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

/**
 * The commit locks, which are the page's most schema-specific view: this
 * IS the serialization point every writer to a catalog queues on, so
 * showing it directly beats inferring trouble from generic transaction
 * thresholds that only fire once starvation is severe.
 */
function CommitLocks({ locks }: { locks: CommitLockHolder[] }) {
  if (locks.length === 0) {
    return (
      <p className="empty">
        No commit locks held. Writers queue here only while a catalog is
        being committed to, so an empty list is the common case.
      </p>
    );
  }
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th>catalog</th>
          <th className="num">pid</th>
          <th>state</th>
          <th className="num">held</th>
          <th className="num">waiters</th>
        </tr>
      </thead>
      <tbody>
        {locks.map((l) => (
          <tr key={`${l.catalog_id}.${l.pid}`}>
            <td className="mono">{l.catalog ?? `catalog ${l.catalog_id}`}</td>
            <td className="num">{l.pid}</td>
            <td>
              {l.granted ? (
                "holding"
              ) : (
                <span className="badge badge-warn">waiting</span>
              )}
            </td>
            <td className="num">{duration(l.held_seconds)}</td>
            <td className={`num${l.waiters > 0 ? " db-cell-warn" : ""}`}>
              {l.waiters}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function Slots({ slots }: { slots: ReplicationSlot[] }) {
  if (slots.length === 0) return <p className="empty">No replication slots.</p>;
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th>slot</th>
          <th>type</th>
          <th>state</th>
          <th className="num">WAL retained</th>
        </tr>
      </thead>
      <tbody>
        {slots.map((s) => (
          <tr key={s.name}>
            <td className="mono">{s.name}</td>
            <td>{s.slot_type}</td>
            <td>
              {s.active ? (
                "active"
              ) : (
                <span className="badge badge-warn">inactive</span>
              )}
            </td>
            <td className={`num${s.active ? "" : " db-cell-warn"}`}>
              {s.retained_wal_bytes === undefined
                ? "—"
                : formatBytes(s.retained_wal_bytes)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

type TableSortKey =
  | "name"
  | "live"
  | "dead"
  | "ratio"
  | "heap"
  | "indexes"
  | "scans"
  | "vacuum";

const TABLE_COMPARATORS: Record<TableSortKey, ColumnSort<DatabaseTable>> = {
  name: textColumn((t) => t.name),
  live: int64Column((t) => t.live_tuples),
  dead: int64Column((t) => t.dead_tuples),
  // A table postgres has no statistics for yet has no ratio at all;
  // absent sorts last rather than reading as 0% bloat.
  ratio: {
    compare: (a, b) => (a.dead_ratio ?? 0) - (b.dead_ratio ?? 0),
    absent: (t) => t.dead_ratio === undefined,
  },
  heap: int64Column((t) => t.table_bytes),
  indexes: int64Column((t) => t.index_bytes),
  // Sequential scans are the number worth ranking here: the cell shows
  // both, and a table being seq-scanned is what an operator looks for.
  scans: int64Column((t) => t.seq_scans),
  // A table never autovacuumed is the LEAST recently vacuumed, so it
  // sorts as the oldest time (the empty string collates first) and the
  // ascending click surfaces it. Not marked absent: "never" is an
  // answer about this table, not a gap in what we know about it.
  vacuum: {
    compare: (a, b) =>
      cmpText(a.last_autovacuum ?? "", b.last_autovacuum ?? ""),
  },
};

function Tables({ tables }: { tables: DatabaseTable[] }) {
  const [sort, setSort] = useState<SortState<TableSortKey> | null>(null);
  const onSort = (key: TableSortKey) => setSort((prev) => nextSort(prev, key));
  const rows = applySort(tables, sort, TABLE_COMPARATORS);
  if (tables.length === 0) return <p className="empty">No tables.</p>;
  return (
    <table className="data-table">
      <thead>
        <tr>
          <SortableTh label="table" sortKey="name" sort={sort} onSort={onSort} />
          <SortableTh label="live" sortKey="live" sort={sort} onSort={onSort} numeric />
          <SortableTh label="dead" sortKey="dead" sort={sort} onSort={onSort} numeric />
          <SortableTh
            label="dead %"
            sortKey="ratio"
            sort={sort}
            onSort={onSort}
            numeric
          />
          <SortableTh label="heap" sortKey="heap" sort={sort} onSort={onSort} numeric />
          <SortableTh
            label="indexes"
            sortKey="indexes"
            sort={sort}
            onSort={onSort}
            numeric
          />
          <SortableTh
            label="seq / idx scans"
            sortKey="scans"
            sort={sort}
            onSort={onSort}
            numeric
            tooltip="Sorts on sequential scans — the number worth ranking."
          />
          <SortableTh
            label="last autovacuum"
            sortKey="vacuum"
            sort={sort}
            onSort={onSort}
          />
        </tr>
      </thead>
      <tbody>
        {rows.map((t) => (
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

type IndexSortKey = "name" | "table" | "size" | "scans" | "role";

const INDEX_COMPARATORS: Record<IndexSortKey, ColumnSort<DatabaseIndex>> = {
  name: textColumn((i) => i.name),
  table: textColumn((i) => i.table),
  size: int64Column((i) => i.size_bytes),
  scans: int64Column((i) => i.scans),
  role: {
    compare: (a, b) =>
      Number(Boolean(a.constraint_backing)) - Number(Boolean(b.constraint_backing)),
  },
};

function Indexes({ indexes }: { indexes: DatabaseIndex[] }) {
  const [sort, setSort] = useState<SortState<IndexSortKey> | null>(null);
  const onSort = (key: IndexSortKey) => setSort((prev) => nextSort(prev, key));
  const rows = applySort(indexes, sort, INDEX_COMPARATORS);
  if (indexes.length === 0) return <p className="empty">No indexes.</p>;
  return (
    <table className="data-table">
      <thead>
        <tr>
          <SortableTh label="index" sortKey="name" sort={sort} onSort={onSort} />
          <SortableTh label="table" sortKey="table" sort={sort} onSort={onSort} />
          <SortableTh label="size" sortKey="size" sort={sort} onSort={onSort} numeric />
          <SortableTh
            label="scans"
            sortKey="scans"
            sort={sort}
            onSort={onSort}
            numeric
          />
          <SortableTh label="role" sortKey="role" sort={sort} onSort={onSort} />
        </tr>
      </thead>
      <tbody>
        {rows.map((i) => (
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

  const {
    server,
    activity,
    tables,
    indexes,
    findings,
    commit_locks: commitLocks,
    replication_slots: slots,
    blind_spots: blindSpots,
  } = data;
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
            label="temp files"
            value={`${formatCount(server.temp_files)} · ${formatBytes(server.temp_bytes)}`}
            hint="Queries that exceeded work_mem and spilled to disk."
          />
          <Stat
            label="checkpoints (req/timed)"
            value={
              server.checkpoints_requested === undefined
                ? "—"
                : `${formatCount(server.checkpoints_requested)} / ${formatCount(
                    server.checkpoints_timed ?? 0,
                  )}`
            }
            hint="Requested checkpoints mean WAL hit max_wal_size before the timer. Unavailable on some Postgres versions."
          />
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
        <h3>Commit locks</h3>
        <CommitLocks locks={commitLocks} />
      </section>

      <section>
        <h3>Replication slots</h3>
        <Slots slots={slots} />
      </section>

      <section>
        <h3>Tables</h3>
        <Tables tables={tables} />
      </section>

      <section>
        <h3>Indexes</h3>
        <Indexes indexes={indexes} />
      </section>

      {/* Last, and deliberately present even when everything above is
          green: the most common way to lose a Postgres is not visible
          from any statistics view. */}
      <section>
        <h3>Not measured here</h3>
        <ul className="db-blind-spots">
          {blindSpots.map((spot) => (
            <li key={spot}>{spot}</li>
          ))}
        </ul>
      </section>
    </section>
  );
}
