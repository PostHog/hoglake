import { Fragment, useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { getFileStats, getTable, listFiles, planScan } from "../api/client";
import { isInt64String } from "../api/int64";
import { formatColumnType } from "../api/types";
import type { Column, DecodedBound, Int64, PartitionSpec, Table } from "../api/types";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonBlock, SkeletonRows } from "../components/Skeleton";
import { StatsStateBadge } from "../components/badges";
import { CopyButton } from "../components/CopyButton";
import { decodePartition, type PartitionDecode } from "../lib/partitions";
import {
  formatBytes,
  formatCount,
  formatPartitionField,
} from "../lib/format";

const TABS = ["schema", "files", "scan"] as const;
type Tab = (typeof TABS)[number];

/**
 * A file path, truncated from the LEFT so the distinguishing end stays
 * visible, with an icon button that copies the whole thing.
 *
 * The truncation is `direction: rtl` on the text alone, never on the
 * cell: applying it to the cell would flip the button to the wrong side,
 * and the button has to sit after the path to be found.
 */
function PathCell({ path, label }: { path: string; label?: string }) {
  return (
    <td className="path-cell">
      <span className="mono path-text" title={path}>
        {path}
      </span>
      <CopyButton text={path} label={label ?? "path"} />
    </td>
  );
}

function StatsHeader({ table }: { table: Table }) {
  return (
    <dl className="stats-header">
      <div>
        <dt>record_count</dt>
        <dd className="mono">{formatCount(table.record_count)}</dd>
      </div>
      <div>
        <dt>file_count</dt>
        <dd className="mono">{formatCount(table.file_count)}</dd>
      </div>
      <div>
        <dt>file_size_bytes</dt>
        <dd className="mono" title={`${table.file_size_bytes}`}>
          {formatBytes(table.file_size_bytes)}
        </dd>
      </div>
      <div>
        <dt>table_uuid</dt>
        <dd className="mono">
          {table.table_uuid} <CopyButton text={table.table_uuid} label="table_uuid" />
        </dd>
      </div>
    </dl>
  );
}

function SnapshotSelector({
  snapshot,
  onChange,
}: {
  snapshot: Int64 | undefined;
  onChange: (s: Int64 | undefined) => void;
}) {
  const [draft, setDraft] = useState(snapshot ?? "");
  const [invalid, setInvalid] = useState(false);
  return (
    <form
      className="snapshot-selector"
      onSubmit={(e) => {
        e.preventDefault();
        const trimmed = draft.trim();
        if (trimmed === "") {
          setInvalid(false);
          onChange(undefined);
          return;
        }
        // Snapshot ids are int64; keep them as exact decimal strings — a
        // Number() round-trip would silently retarget ids above 2^53.
        if (!isInt64String(trimmed)) {
          setInvalid(true);
          return;
        }
        setInvalid(false);
        onChange(trimmed);
      }}
    >
      <label>
        snapshot
        <input
          inputMode="numeric"
          value={draft}
          placeholder="head"
          onChange={(e) => {
            setDraft(e.target.value);
            setInvalid(false);
          }}
          aria-label="snapshot id"
          aria-invalid={invalid}
        />
      </label>
      <button type="submit">Go</button>
      {invalid && (
        <span className="field-error">snapshot id must be a non-negative integer</span>
      )}
      {snapshot !== undefined && (
        <button
          type="button"
          className="ghost"
          onClick={() => {
            setDraft("");
            setInvalid(false);
            onChange(undefined);
          }}
        >
          head
        </button>
      )}
    </form>
  );
}

/**
 * One row per column NODE, containers included: dotted name, depth for
 * indentation, and the node itself. A nested schema is otherwise
 * unreadable here — the field ids of a struct's fields are exactly what
 * an operator comes to this page for.
 */
function flattenColumns(
  columns: Column[],
  prefix = "",
  depth = 0,
): { path: string; depth: number; column: Column }[] {
  return [...columns]
    .sort((a, b) => a.ordinal - b.ordinal)
    .flatMap((c) => {
      const path = prefix ? `${prefix}.${c.name}` : c.name;
      return [
        { path, depth, column: c },
        ...flattenColumns(c.children ?? [], path, depth + 1),
      ];
    });
}

function SchemaTab({ table }: { table: Table }) {
  const columns = flattenColumns(table.columns);
  return (
    <div>
      <table className="data-table">
        <thead>
          <tr>
            <th className="num">field_id</th>
            <th>name</th>
            <th>type</th>
            <th>nullable</th>
            <th className="num">ordinal</th>
          </tr>
        </thead>
        <tbody>
          {columns.map(({ path, depth, column: c }) => (
            <tr key={c.field_id}>
              <td className="num mono">{c.field_id}</td>
              <td style={{ paddingLeft: `${depth * 1.25}rem` }}>{c.name}</td>
              <td className="mono" title={path}>
                {formatColumnType(c)}
              </td>
              <td
                className="nullable-mark"
                title={c.nullable === false ? "not null" : "nullable"}
              >
                {c.nullable === false ? "✗" : "✓"}
              </td>
              <td className="num mono">{c.ordinal}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {table.partition_spec && (
        <div className="partition-spec">
          <h3>
            Partition spec{" "}
            <span className="subtle">spec_id {table.partition_spec.spec_id}</span>
          </h3>
          {table.partition_spec.fields.length === 0 ? (
            <p className="empty">Unpartitioned.</p>
          ) : (
            <ul>
              {table.partition_spec.fields.map((f, i) => (
                <li key={i} className="mono">
                  {formatPartitionField(f, table.columns)}
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}

/**
 * One decoded bound cell. The server ships bounds already decoded
 * (GET .../files/{fileId}/stats — the webui carries no codec): strings
 * and booleans verbatim, numbers as their exact raw tokens (int64.ts),
 * and null meaning "no bound" — a real answer (all-null column, or a
 * bound the server could not decode), flagged so an operator knows it
 * forbids pruning rather than describing an empty range.
 */
function BoundCell({ bound }: { bound: DecodedBound }) {
  if (bound === null) {
    return (
      <td className="num mono subtle" title="no bound stored — do not prune">
        null
      </td>
    );
  }
  return <td className="num mono">{String(bound)}</td>;
}

/** The expanded stats panel for one file: its per-column decoded stats. */
function FileStatsPanel({
  catalog,
  namespace,
  table,
  fileId,
  snapshot,
}: {
  catalog: string;
  namespace: string;
  table: string;
  fileId: Int64;
  snapshot?: Int64;
}) {
  const { data, isPending, isError, error } = useQuery({
    queryKey: ["fileStats", catalog, namespace, table, fileId, snapshot ?? "head"],
    queryFn: () => getFileStats(catalog, namespace, table, fileId, snapshot),
  });
  if (isError) return <ErrorBox error={error} />;
  if (isPending) return <SkeletonBlock />;
  if (data.columns.length === 0) {
    return (
      <p className="empty">
        {data.no_stats_reason ?? "No column statistics recorded for this file."}
      </p>
    );
  }
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th className="num">field_id</th>
          <th>column</th>
          <th>type</th>
          <th className="num">values</th>
          <th className="num">nulls</th>
          <th className="num">nan</th>
          <th className="num">size</th>
          <th className="num">lower_bound</th>
          <th className="num">upper_bound</th>
        </tr>
      </thead>
      <tbody>
        {data.columns.map((c) => (
          <tr key={c.field_id}>
            <td className="num mono">{c.field_id}</td>
            <td className="mono">{c.path}</td>
            <td className="mono">{c.type}</td>
            <td className="num mono">{formatCount(c.value_count)}</td>
            <td className="num mono">{formatCount(c.null_count)}</td>
            <td className="num mono">
              {c.nan_count !== undefined ? formatCount(c.nan_count) : "—"}
            </td>
            <td className="num mono">
              {c.size_bytes !== undefined ? formatBytes(c.size_bytes) : "—"}
            </td>
            <BoundCell bound={c.lower_bound} />
            <BoundCell bound={c.upper_bound} />
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/**
 * A file's partition, decoded and labelled.
 *
 * `team_id=42 / month=2026-04` instead of `[42, 675]`: the tuple is the
 * pruning key and worth showing, but reading it raw costs three lookups
 * (which column, in what order, under which transform) that the page can
 * do for the reader.
 *
 * The raw tuple stays on the tooltip. Whoever is debugging spec
 * evolution needs the exact stored values, and they are the one audience
 * for whom the decoded form is the wrong answer.
 */
function PartitionCell({ decoded }: { decoded: PartitionDecode }) {
  if (decoded.kind === "unpartitioned") {
    return <td className="subtle">—</td>;
  }
  if (decoded.kind === "foreign-spec") {
    // Deliberately undecoded: see decodePartition. The badge says why,
    // so this does not read as the decoder having failed.
    return (
      <td className="mono partition-cell" title={`Stored tuple ${decoded.raw}`}>
        {decoded.raw}{" "}
        <span className="badge badge-warn" title="Written under an older partition spec; the page only holds the current one, so its fields are not labelled.">
          spec {String(decoded.specId)}
        </span>
      </td>
    );
  }
  if (decoded.kind === "mismatched") {
    return (
      <td className="mono partition-cell" title="Tuple does not match the current spec">
        {decoded.raw}
      </td>
    );
  }
  const raw = `[${decoded.values.map((v) => v.raw ?? "null").join(", ")}]`;
  return (
    <td
      className="partition-cell"
      title={decoded.values
        .map((v) => `${v.field}: ${v.transform}(${v.raw ?? "null"})`)
        .concat(`stored ${raw}`)
        .join("\n")}
    >
      {decoded.values.map((v, i) => (
        <span key={i} className="partition-part">
          <span className="partition-field">{v.field}</span>
          <span className="partition-eq">=</span>
          <span className="mono partition-value">{v.display}</span>
        </span>
      ))}
    </td>
  );
}

function FilesTab({
  catalog,
  namespace,
  table,
  snapshot,
  spec,
  columns,
}: {
  catalog: string;
  namespace: string;
  table: string;
  snapshot?: Int64;
  spec?: PartitionSpec;
  columns?: Column[];
}) {
  const { data, isPending, isError, error } = useQuery({
    queryKey: ["files", catalog, namespace, table, snapshot ?? "head"],
    queryFn: () => listFiles(catalog, namespace, table, snapshot),
  });
  const [expanded, setExpanded] = useState<Int64 | null>(null);
  if (isError) return <ErrorBox error={error} />;
  // The column appears only for a partitioned table: on an unpartitioned
  // one it would be a column of dashes.
  const partitioned = (spec?.fields.length ?? 0) > 0;
  // Fixed columns: expand toggle, id, path, record_count, size,
  // row_id_start, stats, begin_snapshot — plus partition when there is
  // one. Drives the skeleton and the empty-state colSpan, so a wrong
  // count shows as a short row rather than an error.
  const cols = partitioned ? 9 : 8;
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th />
          <th className="num">id</th>
          {partitioned && <th>partition</th>}
          <th>path</th>
          <th className="num">record_count</th>
          <th className="num">size</th>
          <th className="num">row_id_start</th>
          <th>stats</th>
          <th className="num">begin_snapshot</th>
        </tr>
      </thead>
      {isPending ? (
        <SkeletonRows rows={5} cols={cols} />
      ) : (
        <tbody>
          {data.length === 0 && (
            <tr>
              <td colSpan={cols} className="empty">
                No data files at this snapshot.
              </td>
            </tr>
          )}
          {data.map((f) => (
            <Fragment key={f.data_file_id}>
              <tr>
                <td>
                  <button
                    type="button"
                    className="expand-toggle"
                    aria-label={`toggle stats for file ${f.data_file_id}`}
                    aria-expanded={expanded === f.data_file_id}
                    onClick={() =>
                      setExpanded(
                        expanded === f.data_file_id ? null : f.data_file_id,
                      )
                    }
                  >
                    {expanded === f.data_file_id ? "▾" : "▸"}
                  </button>
                </td>
                <td className="num mono">{f.data_file_id}</td>
                {partitioned && (
                  <PartitionCell decoded={decodePartition(f, spec, columns)} />
                )}
                <PathCell path={f.path} />
                <td className="num mono">{formatCount(f.record_count)}</td>
                <td className="num mono" title={`${f.file_size_bytes}`}>
                  {formatBytes(f.file_size_bytes)}
                </td>
                <td className="num mono">{f.row_id_start}</td>
                <td>
                  <StatsStateBadge state={f.stats_state} />
                </td>
                <td className="num mono">{f.begin_snapshot}</td>
              </tr>
              {expanded === f.data_file_id && (
                <tr className="detail-row">
                  <td colSpan={cols}>
                    <FileStatsPanel
                      catalog={catalog}
                      namespace={namespace}
                      table={table}
                      fileId={f.data_file_id}
                      snapshot={snapshot}
                    />
                  </td>
                </tr>
              )}
            </Fragment>
          ))}
        </tbody>
      )}
    </table>
  );
}

function ScanTab({
  catalog,
  namespace,
  table,
  snapshot,
}: {
  catalog: string;
  namespace: string;
  table: string;
  snapshot?: Int64;
}) {
  const { data, isPending, isError, error } = useQuery({
    queryKey: ["scan", catalog, namespace, table, snapshot ?? "head"],
    queryFn: () => planScan(catalog, namespace, table, snapshot),
  });
  if (isError) return <ErrorBox error={error} />;
  return (
    <table className="data-table">
      <thead>
        <tr>
          <th className="num">data_file</th>
          <th>path</th>
          <th className="num">record_count</th>
          <th>stats</th>
          <th>deletion vector</th>
          <th className="num">delete_count</th>
        </tr>
      </thead>
      {isPending ? (
        <SkeletonRows rows={5} cols={6} />
      ) : (
        <tbody>
          {data.length === 0 && (
            <tr>
              <td colSpan={6} className="empty">
                Empty scan plan at this snapshot.
              </td>
            </tr>
          )}
          {data.map((sf) => (
            <tr
              key={sf.data_file.data_file_id}
              className={sf.delete_file ? "has-deletes" : undefined}
            >
              <td className="num mono">{sf.data_file.data_file_id}</td>
              <PathCell path={sf.data_file.path} />
              <td className="num mono">{formatCount(sf.data_file.record_count)}</td>
              <td>
                <StatsStateBadge state={sf.data_file.stats_state} />
              </td>
              {sf.delete_file ? (
                <PathCell path={sf.delete_file.path} label="delete file path" />
              ) : (
                <td className="mono path-cell">
                  <span className="subtle">none</span>
                </td>
              )}
              <td className="num mono">
                {sf.delete_file ? formatCount(sf.delete_file.delete_count) : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      )}
    </table>
  );
}

export function TablePage() {
  const { catalog, namespace, table } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();

  const tabParam = searchParams.get("tab");
  const tab: Tab = TABS.includes(tabParam as Tab) ? (tabParam as Tab) : "schema";
  // A non-numeric ?snapshot (typo, mangled link) is ignored — treated as
  // head — rather than becoming NaN on screen and on the wire. Valid ids
  // stay exact decimal strings (int64: no Number round-trip).
  const snapshotParam = searchParams.get("snapshot");
  const snapshot =
    snapshotParam !== null && snapshotParam !== "" && isInt64String(snapshotParam)
      ? snapshotParam
      : undefined;

  const enabled = Boolean(catalog && namespace && table);
  const tableQuery = useQuery({
    queryKey: ["table", catalog, namespace, table, snapshot ?? "head"],
    queryFn: () => getTable(catalog!, namespace!, table!, snapshot),
    enabled,
  });
  if (!catalog || !namespace || !table) return null;

  const setParam = (key: string, value: string | undefined) => {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        if (value === undefined) next.delete(key);
        else next.set(key, value);
        return next;
      },
      { replace: true },
    );
  };

  return (
    <section>
      <h2>
        {table} <span className="subtle">table</span>
        {snapshot !== undefined && (
          <span className="badge time-travel">@ snapshot {snapshot}</span>
        )}
      </h2>
      {tableQuery.isError ? (
        <ErrorBox error={tableQuery.error} />
      ) : tableQuery.isPending ? (
        <SkeletonBlock />
      ) : (
        <StatsHeader table={tableQuery.data} />
      )}
      <div className="tab-bar">
        <div role="tablist" className="tabs">
          {TABS.map((t) => (
            <button
              key={t}
              role="tab"
              aria-selected={tab === t}
              className={tab === t ? "tab active" : "tab"}
              onClick={() => setParam("tab", t === "schema" ? undefined : t)}
            >
              {t}
            </button>
          ))}
        </div>
        <SnapshotSelector
          snapshot={snapshot}
          onChange={(s) => setParam("snapshot", s)}
        />
      </div>
      {tab === "schema" &&
        (tableQuery.isSuccess ? <SchemaTab table={tableQuery.data} /> : null)}
      {tab === "files" && (
        <FilesTab
          catalog={catalog}
          namespace={namespace}
          table={table}
          snapshot={snapshot}
          /* The spec the tuples decode against. Already fetched for the
             schema tab, so this is a prop rather than a second request;
             undefined while it loads, which reads as "not yet decodable"
             rather than as "unpartitioned". */
          spec={tableQuery.data?.partition_spec}
          columns={tableQuery.data?.columns}
        />
      )}
      {tab === "scan" && (
        <ScanTab
          catalog={catalog}
          namespace={namespace}
          table={table}
          snapshot={snapshot}
        />
      )}
    </section>
  );
}
