import { Fragment, useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { getFileStats, getTable, listFiles, planScan } from "../api/client";
import { isInt64String } from "../api/int64";
import { formatColumnType } from "../api/types";
import type {
  Column,
  DataFile,
  DecodedBound,
  Int64,
  PartitionSpec,
  ScanFile,
  SortSpec,
  StatsState,
  Table,
} from "../api/types";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonBlock, SkeletonRows } from "../components/Skeleton";
import { StatsStateBadge } from "../components/badges";
import { CopyButton } from "../components/CopyButton";
import { decodePartition, type PartitionDecode } from "../lib/partitions";
import {
  columnPath,
  formatBytes,
  formatCount,
  formatPartitionField,
} from "../lib/format";
import {
  applySort,
  cmpInt64,
  cmpText,
  int64Column,
  isDecimalInt,
  nextSort,
  textColumn,
} from "../lib/sort";
import type { ColumnSort, SortState } from "../lib/sort";
import { SortableTh } from "../components/SortableTh";

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
 * Longest bound rendered inline. A uuid is 36 characters and a
 * microsecond timestamp 27, so everything with a fixed width stays
 * whole; past this the value is a blob and the column is better served
 * by a stable table than by the rest of the payload.
 */
const BOUND_INLINE_MAX_CHARS = 48;

/** What a null bound means wherever a column's statistics are shown. */
const NO_BOUND_TITLE = "no bound stored — do not prune";

/**
 * One decoded bound cell. The server ships bounds already decoded
 * (GET .../files/{fileId}/stats — the webui carries no codec): strings
 * and booleans verbatim, numbers as their exact raw tokens (int64.ts),
 * and null meaning "no bound" — a real answer (all-null column, or a
 * bound the server could not decode), flagged so an operator knows it
 * forbids pruning rather than describing an empty range.
 *
 * [nullTitle] overrides what that null MEANS, because it does not
 * always mean the same thing: on a column's stats it is "nothing was
 * stored, so do not prune", while on the upper end of a compaction
 * output's row-id span it is "unknown" — see [OrderingBoundCell].
 */
function BoundCell({
  bound,
  nullTitle = NO_BOUND_TITLE,
}: {
  bound: DecodedBound;
  nullTitle?: string;
}) {
  if (bound === null) {
    return (
      <td className="num mono subtle" title={nullTitle}>
        null
      </td>
    );
  }
  const text = String(bound);
  // Numbers, timestamps and ordinary strings are short, and right-aligned
  // they read as a range. A bound over a text column holding JSON is not
  // short — a properties blob's min and max are whole payloads, and left
  // unconstrained one cell pushes both bound columns off the viewport and
  // squeezes every column before them. Past this width the cell truncates
  // and keeps its full value in the tooltip.
  if (text.length <= BOUND_INLINE_MAX_CHARS) {
    return <td className="num mono">{text}</td>;
  }
  return (
    <td className="mono bound-cell">
      {/* Truncated from the RIGHT, unlike a path: an object path is
          distinguished by its end, a bound by its beginning. */}
      <span className="bound-text" title={text}>
        {text}
      </span>
    </td>
  );
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
/**
 * What each stats marker means. EVERY state gets one: the shape alone
 * says whether a row opens, not what the state is or what it costs the
 * reader, and the triangle needs explaining as much as the circle does.
 *
 * All three answer the same three things in the same order — what the
 * state is, what it means for a reader planning a scan, and what to do
 * about it — so hovering any two rows compares like with like.
 *
 * For the states with no statistics this is the ground the server gives
 * in FileStats.no_stats_reason, said in the space a tooltip has. It is
 * carried here rather than fetched because the reason belongs to the
 * STATE, not the file: every pending file has the same one. That is
 * also what makes the circle safe to leave unclickable — expanding one
 * of those rows only ever produced this sentence.
 */
const STATS_TOOLTIP: Record<StatsState, string> = {
  provided:
    "Column statistics are hydrated: per-column bounds, null counts and " +
    "sizes are recorded, so a reader can prune this file. Click to see them.",
  pending:
    "Column statistics have not been hydrated yet, so this file carries no " +
    "bounds and a reader cannot prune it. The hydrator sweep will claim it.",
  failed:
    "Stats hydration failed, so this file carries no bounds and a reader " +
    "cannot prune it. Requeue with POST .../maintenance/rehydrate.",
};

/**
 * The stats cell, which is also the row's expander.
 *
 * A file WITH statistics gets a triangle that opens them. A file without
 * gets a circle that does nothing, because there is nothing to open —
 * and a control that does nothing when clicked is worse than one that
 * never invites the click. The shape carries the state, which is what
 * the "provided" pill used to spend a column's width saying.
 *
 * Failed keeps its own colour. It is not deferred — nothing will arrive
 * on its own — and collapsing it into the pending circle would hide the
 * one stats state an operator has to act on.
 */
function StatsCell({
  state,
  expanded,
  onToggle,
  fileId,
}: {
  state: StatsState;
  expanded: boolean;
  onToggle: () => void;
  fileId: Int64;
}) {
  if (state === "provided") {
    return (
      <td>
        <button
          type="button"
          className="expand-toggle"
          aria-label={`toggle stats for file ${fileId}`}
          aria-expanded={expanded}
          onClick={onToggle}
          title={STATS_TOOLTIP.provided}
        >
          {expanded ? "▾" : "▸"}
        </button>
      </td>
    );
  }
  return (
    <td>
      {/* Not a button: there is nothing behind it. The title carries
          what a click would have revealed. */}
      <span
        className={`stats-marker stats-${state}`}
        role="img"
        aria-label={`${state}: no column statistics for file ${fileId}`}
        title={STATS_TOOLTIP[state]}
      >
        ●
      </span>
    </td>
  );
}

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
          {/* A real element, not a ::before. Generated content is
              invisible to the DOM and therefore to the tests — this
              separator shipped missing once behind a rule that looked
              right in isolation, and nothing failed. */}
          {i > 0 && <span className="partition-sep"> / </span>}
          <span className="partition-field">{v.field}</span>
          <span className="partition-eq">=</span>
          <span className="mono partition-value">{v.display}</span>
        </span>
      ))}
    </td>
  );
}

/**
 * A file's partition as one ordered sort key.
 *
 * Compared on the STORED ordinals, not on the rendered dates, because
 * the ordinals are what carry the order: `month` is months since 1970-01
 * and floors, so a pre-epoch partition is negative and sorts correctly
 * as an integer while "1969-12" sorts after "2026-04" as text. Where a
 * tuple was not decoded (a foreign spec, an arity mismatch) the rendered
 * text is all there is, and it compares naturally — `team_id=9` before
 * `team_id=10`.
 */
function cmpPartition(a: PartitionDecode, b: PartitionDecode): number {
  if (a.kind === "decoded" && b.kind === "decoded") {
    for (let i = 0; i < Math.min(a.values.length, b.values.length); i++) {
      const x = a.values[i];
      const y = b.values[i];
      const bothInts = isDecimalInt(x.raw) && isDecimalInt(y.raw);
      const c = bothInts ? cmpInt64(x.raw, y.raw) : cmpText(x.display, y.display);
      if (c !== 0) return c;
    }
    return a.values.length - b.values.length;
  }
  return cmpText(partitionSortText(a), partitionSortText(b));
}

function partitionSortText(d: PartitionDecode): string {
  switch (d.kind) {
    case "unpartitioned":
      return "";
    case "decoded":
      return d.values.map((v) => `${v.field}=${v.display}`).join(" / ");
    default:
      return d.raw;
  }
}

/** The implicit ordering key of an unsorted table, by its server name. */
const ROW_ID_COLUMN = "_hog_row_id";

/** What a null upper bound means on a row-id span (never on a sort key). */
const UNKNOWN_ROW_ID_TITLE =
  "unknown — this file carries explicit row ids (a compaction output), " +
  "so row_id_start is only min(input row ids) and the maximum cannot be " +
  "computed from it";

/**
 * The ORDERING KEY of a table's files: the column whose per-file min and
 * max say which files a predicate can skip and which files overlap.
 *
 * A sorted table's key is its LEADING sort field — the only one whose
 * per-file range is a contiguous interval, because a second key orders
 * only the rows that TIE on the first and so spans the whole column. An
 * unsorted table's is the row id: the append order, and the one
 * ordering every file has. When a sort spec exists the row id says
 * nothing about layout (a sorted rewrite remaps it), so it is not shown.
 *
 * The header is the DOTTED path, not the bare leaf name: two structs may
 * each hold a `zip`, and a column headed "zip" on a table sorted by
 * `addr.zip` names the wrong one.
 */
function orderingKeyName(sortSpec?: SortSpec, columns?: Column[]): string {
  const leading = sortSpec?.fields[0];
  if (!leading) return ROW_ID_COLUMN;
  return (
    columnPath(columns, leading.source_field_id) ??
    `field ${leading.source_field_id}`
  );
}

/**
 * One end of a file's ordering-key range — three states, which are three
 * different facts.
 *
 * NO `ordering_bounds` at all is the page's absent em dash: the server
 * had no range to state, which is what a sorted table's pending or
 * failed file has, along with a file that landed before the key column
 * existed and a file of zero records.
 *
 * A VALUE renders like any other decoded bound.
 *
 * A NULL inside the object is a stated answer, and which answer depends
 * on the key. On a sort key the bound was never stored, so the file
 * cannot be pruned on it. On the upper end of a row-id span it is
 * genuinely unknown: the file carries explicit row ids, and there is no
 * `_hog_row_id` statistics row to read a maximum out of — an invented
 * `row_id_start + record_count - 1` would be wrong by exactly however
 * non-contiguous the compaction inputs were.
 */
function OrderingBoundCell({
  file,
  end,
}: {
  file: DataFile;
  end: "lower" | "upper";
}) {
  const bounds = file.ordering_bounds;
  if (!bounds) return <td className="num mono subtle">—</td>;
  // Absent field_id IS the row-id case: the row id is not a catalog
  // column, so the server has no field id to name it with.
  const rowIdSpan = bounds.field_id === undefined;
  return (
    <BoundCell
      bound={end === "lower" ? bounds.lower_bound : bounds.upper_bound}
      nullTitle={
        rowIdSpan && end === "upper" ? UNKNOWN_ROW_ID_TITLE : NO_BOUND_TITLE
      }
    />
  );
}

type FileSortKey =
  | "id"
  | "partition"
  | "path"
  | "records"
  | "size"
  | "keyMin"
  | "keyMax"
  | "stats"
  | "snapshot";

/** A row plus its decoded partition, so the decode is done once per file. */
interface FileRow {
  file: DataFile;
  partition: PartitionDecode;
}

/**
 * An ordering-key bound column, compared as the values the bounds are:
 * exact integers when both ends are decimal integers — a long key or a
 * row id runs past 2^53, which is why these arrive as raw tokens — and
 * natural text otherwise. The same rule the partition column uses, for
 * the same reason.
 */
function boundColumn(
  get: (row: FileRow) => DecodedBound | undefined,
): ColumnSort<FileRow> {
  const text = (row: FileRow): string | undefined => {
    const bound = get(row);
    return bound === null || bound === undefined ? undefined : String(bound);
  };
  return {
    compare: (a, b) => {
      const x = text(a);
      const y = text(b);
      return isDecimalInt(x) && isDecimalInt(y) ? cmpInt64(x, y) : cmpText(x, y);
    },
    // A file with no range and a file whose bound is a stated null are
    // both "no value here", and an em dash that merely compared as
    // smallest would ride to the top of a largest-first sort.
    absent: (row) => text(row) === undefined,
  };
}

const FILE_COMPARATORS: Record<FileSortKey, ColumnSort<FileRow>> = {
  id: int64Column((r) => r.file.data_file_id),
  partition: { compare: (a, b) => cmpPartition(a.partition, b.partition) },
  path: textColumn((r) => r.file.path),
  records: int64Column((r) => r.file.record_count),
  size: int64Column((r) => r.file.file_size_bytes),
  keyMin: boundColumn((r) => r.file.ordering_bounds?.lower_bound),
  keyMax: boundColumn((r) => r.file.ordering_bounds?.upper_bound),
  // Grouping, not ranking: the point is to bring the failures together.
  stats: textColumn((r) => r.file.stats_state),
  snapshot: int64Column((r) => r.file.begin_snapshot),
};

function FilesTab({
  catalog,
  namespace,
  table,
  snapshot,
  spec,
  sortSpec,
  columns,
}: {
  catalog: string;
  namespace: string;
  table: string;
  snapshot?: Int64;
  spec?: PartitionSpec;
  sortSpec?: SortSpec;
  columns?: Column[];
}) {
  const { data, isPending, isError, error } = useQuery({
    queryKey: ["files", catalog, namespace, table, snapshot ?? "head"],
    queryFn: () => listFiles(catalog, namespace, table, snapshot),
  });
  const [expanded, setExpanded] = useState<Int64 | null>(null);
  // null = the server's order, which is the manifest's: begin_snapshot,
  // then row_id_start, then id.
  const [sort, setSort] = useState<SortState<FileSortKey> | null>(null);
  const onSort = (key: FileSortKey) => setSort((prev) => nextSort(prev, key));
  if (isError) return <ErrorBox error={error} />;
  // The column appears only for a partitioned table: on an unpartitioned
  // one it would be a column of dashes.
  const partitioned = (spec?.fields.length ?? 0) > 0;
  // The ordering key both bound columns report, named once for the two
  // headers — see orderingKeyName.
  const keyName = orderingKeyName(sortSpec, columns);
  // Fixed columns: id, path, record_count, size, the ordering key's min
  // and max, stats, begin_snapshot — plus partition when there is one.
  // The stats column doubles as the expander, so there is no separate
  // toggle column. Drives the skeleton and the empty-state colSpan, so a
  // wrong count shows as a short row rather than an error.
  const cols = partitioned ? 9 : 8;
  // Sorting is over the WHOLE table: GET /files returns every live file
  // at the snapshot, so "largest file" here is the largest file, not the
  // largest of a page.
  const rows = applySort(
    (data ?? []).map((file) => ({
      file,
      partition: decodePartition(file, spec, columns),
    })),
    sort,
    FILE_COMPARATORS,
  );
  return (
    <table className="data-table">
      <thead>
        <tr>
          <SortableTh label="id" sortKey="id" sort={sort} onSort={onSort} numeric />
          {partitioned && (
            <SortableTh
              label="partition"
              sortKey="partition"
              sort={sort}
              onSort={onSort}
            />
          )}
          <SortableTh label="path" sortKey="path" sort={sort} onSort={onSort} />
          <SortableTh
            label="record_count"
            sortKey="records"
            sort={sort}
            onSort={onSort}
            numeric
          />
          <SortableTh label="size" sortKey="size" sort={sort} onSort={onSort} numeric />
          <SortableTh
            label={`${keyName} min`}
            sortKey="keyMin"
            sort={sort}
            onSort={onSort}
            numeric
          />
          <SortableTh
            label={`${keyName} max`}
            sortKey="keyMax"
            sort={sort}
            onSort={onSort}
            numeric
          />
          <SortableTh label="stats" sortKey="stats" sort={sort} onSort={onSort} />
          <SortableTh
            label="begin_snapshot"
            sortKey="snapshot"
            sort={sort}
            onSort={onSort}
            numeric
          />
        </tr>
      </thead>
      {isPending ? (
        <SkeletonRows rows={5} cols={cols} />
      ) : (
        <tbody>
          {rows.length === 0 && (
            <tr>
              <td colSpan={cols} className="empty">
                No data files at this snapshot.
              </td>
            </tr>
          )}
          {rows.map(({ file: f, partition }) => (
            <Fragment key={f.data_file_id}>
              <tr>
                <td className="num mono">{f.data_file_id}</td>
                {partitioned && <PartitionCell decoded={partition} />}
                <PathCell path={f.path} />
                <td className="num mono">{formatCount(f.record_count)}</td>
                <td className="num mono" title={`${f.file_size_bytes}`}>
                  {formatBytes(f.file_size_bytes)}
                </td>
                <OrderingBoundCell file={f} end="lower" />
                <OrderingBoundCell file={f} end="upper" />
                <StatsCell
                  state={f.stats_state}
                  expanded={expanded === f.data_file_id}
                  onToggle={() =>
                    setExpanded(
                      expanded === f.data_file_id ? null : f.data_file_id,
                    )
                  }
                  fileId={f.data_file_id}
                />
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

type ScanSortKey = "id" | "path" | "records" | "stats" | "dv" | "deletes";

const SCAN_COMPARATORS: Record<ScanSortKey, ColumnSort<ScanFile>> = {
  id: int64Column((sf) => sf.data_file.data_file_id),
  path: textColumn((sf) => sf.data_file.path),
  records: int64Column((sf) => sf.data_file.record_count),
  stats: textColumn((sf) => sf.data_file.stats_state),
  // Files WITH a deletion vector first on the descending click, which is
  // the question this column is here to answer. Not int64Column: "no
  // deletion vector" is the answer here, not a missing value.
  dv: {
    compare: (a, b) =>
      Number(Boolean(a.delete_file)) - Number(Boolean(b.delete_file)),
  },
  // A file with no deletion vector has no delete_count, so it sorts last
  // whichever way this column points.
  deletes: int64Column((sf) => sf.delete_file?.delete_count),
};

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
  // null = the server's scan-plan order.
  const [sort, setSort] = useState<SortState<ScanSortKey> | null>(null);
  const onSort = (key: ScanSortKey) => setSort((prev) => nextSort(prev, key));
  if (isError) return <ErrorBox error={error} />;
  const rows = applySort(data ?? [], sort, SCAN_COMPARATORS);
  return (
    <table className="data-table">
      <thead>
        <tr>
          <SortableTh
            label="data_file"
            sortKey="id"
            sort={sort}
            onSort={onSort}
            numeric
          />
          <SortableTh label="path" sortKey="path" sort={sort} onSort={onSort} />
          <SortableTh
            label="record_count"
            sortKey="records"
            sort={sort}
            onSort={onSort}
            numeric
          />
          <SortableTh label="stats" sortKey="stats" sort={sort} onSort={onSort} />
          <SortableTh
            label="deletion vector"
            sortKey="dv"
            sort={sort}
            onSort={onSort}
          />
          <SortableTh
            label="delete_count"
            sortKey="deletes"
            sort={sort}
            onSort={onSort}
            numeric
          />
        </tr>
      </thead>
      {isPending ? (
        <SkeletonRows rows={5} cols={6} />
      ) : (
        <tbody>
          {rows.length === 0 && (
            <tr>
              <td colSpan={6} className="empty">
                Empty scan plan at this snapshot.
              </td>
            </tr>
          )}
          {rows.map((sf) => (
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
          /* Names the ordering key whose bounds each file row reports;
             undefined while it loads, and undefined for good on an
             unsorted table, where the key is the row id. */
          sortSpec={tableQuery.data?.sort_spec}
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
