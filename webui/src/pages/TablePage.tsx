import { Fragment, useEffect, useRef, useState } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import {
  getFileStats,
  getPartitionValues,
  getTable,
  listFiles,
  listTablePartitions,
  planScan,
} from "../api/client";
import { isInt64String } from "../api/int64";
import { formatColumnType, isIntegerColumnType } from "../api/types";
import type {
  Column,
  ColumnType,
  DataFile,
  DecodedBound,
  Int64,
  PartitionGroup,
  PartitionListing,
  PartitionSpec,
  PartitionSpecSummary,
  PartitionValues,
  ScanFile,
  SortField,
  SortSpec,
  StatsState,
  Table,
} from "../api/types";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonBlock, SkeletonRows } from "../components/Skeleton";
import { StatsStateBadge } from "../components/badges";
import { ClampedText } from "../components/ClampedText";
import { CopyButton } from "../components/CopyButton";
import { SnapshotId } from "../components/SnapshotId";
import { decodePartition, decodeValue, type PartitionDecode } from "../lib/partitions";
import {
  columnPath,
  columnType,
  formatAge,
  formatBytes,
  formatCount,
  formatPartitionField,
  formatRelativeAge,
  formatTime,
} from "../lib/format";
import { applySort, int64Column, nextSort, textColumn } from "../lib/sort";
import type { ColumnSort, SortState } from "../lib/sort";
import { SortableTh } from "../components/SortableTh";

const TABS = ["schema", "files", "scan", "partitions"] as const;
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

/**
 * One sort-spec field as `path dir nulls`, e.g. `ts asc nulls last`. The
 * column resolves to its dotted path for the same reason the partition
 * key does: two structs may each hold a `zip`, and a header that read
 * "zip desc" would sort by the wrong one.
 */
function formatSortField(f: SortField, columns?: Column[]): string {
  const source = columnPath(columns, f.source_field_id) ?? `field ${f.source_field_id}`;
  const nulls = f.null_order === "nulls_first" ? "nulls first" : "nulls last";
  return `${source} ${f.direction} ${nulls}`;
}

/** Chars of a table comment shown before the expand control takes over. */
const COMMENT_CLAMP = 120;

/**
 * Tooltip for a total that is not there. Absence has one cause on this
 * page — the console never sends `totals=false` — so it can name it
 * (#232): the maintenance sampler's published generation does not cover
 * this table. It is NOT "empty": a table the sample covered that has no
 * files reports real zeros.
 */
const NOT_SAMPLED = "not yet sampled";

/**
 * The freshness of the three totals, beside them, which is the whole
 * point of serving them from a sample instead of a live scan: a number
 * whose age is invisible is read as current.
 *
 * FOUR STATES, and the discriminator for the first one is THE REQUEST,
 * not the response. `timeTravel` is whether this page asked for a
 * snapshot, which it knows for certain; keying "exact" on
 * `totals_snapshot_id === undefined` instead would label two other
 * shapes as exact:
 *
 *  - a PRE-#232 server's head response, which carries live-aggregated
 *    totals and no freshness field at all. During a rollout the console
 *    deploys ahead of the server, so that shape is not hypothetical —
 *    it is the same version-skew reasoning `CentralMaintenancePage`
 *    applies to a missing task;
 *  - a `totals=false` response, if anyone ever adds the parameter to
 *    `getTable`: the numbers would be absent and the cell would claim
 *    "not yet sampled", which is a WRONG claim rather than a missing
 *    one.
 *
 * So: time travel ⇒ exact at the snapshot asked for; otherwise a
 * `totals_snapshot_id` ⇒ a dated sample; otherwise numbers present ⇒
 * an older server, numbers with no freshness claim at all; otherwise
 * nothing sampled.
 */
function TotalsFreshness({
  catalog,
  table,
  timeTravel,
}: {
  catalog: string;
  table: Table;
  /**
   * Whether the page asked for a past snapshot. Just `?snapshot=` today:
   * the page has no `at_timestamp` control, and the server's other
   * time-travel parameter cannot be reached from here. It is the
   * REQUEST's own state, which is the point — see this component's KDoc.
   */
  timeTravel: boolean;
}) {
  if (timeTravel) {
    return (
      <dd
        className="subtle"
        title={
          "aggregated from the manifest at the snapshot this page asked " +
          "for — a time-travel read has no sampled answer, so these are exact"
        }
      >
        exact at this snapshot
      </dd>
    );
  }
  if (table.totals_snapshot_id !== undefined) {
    return (
      <dd
        className="subtle"
        // Exact UTC in the tooltip, humanized age in the cell — the
        // snapshot-id tooltips' idiom (components/SnapshotId.tsx), and
        // the snapshot itself is a SnapshotId so hovering it dates the
        // sample against the table's own history.
        title={
          table.totals_as_of !== undefined
            ? `sampled ${formatTime(table.totals_as_of)}, at the snapshot beside it`
            : undefined
        }
      >
        as of{" "}
        {/* The fallback is DEFENSIVE and unreachable against a
            conforming server: the spec says totals_as_of is "present and
            absent exactly when totals_snapshot_id is", and the server
            sets both from one nullable source. The other three states
            here each answer a shape a real server produces; this one
            answers a malformed response without rendering "undefined". */}
        {table.totals_as_of !== undefined
          ? formatRelativeAge(table.totals_as_of)
          : "an earlier snapshot"}{" "}
        · snapshot <SnapshotId catalog={catalog} id={table.totals_snapshot_id} />
      </dd>
    );
  }
  if (table.record_count !== undefined) {
    // An older server: live numbers with nothing to date them by. Say
    // nothing rather than guess — claiming either "exact" or an age
    // would be inventing a property of a response that has none.
    return (
      <dd className="subtle" title="this server does not report the totals' freshness">
        —
      </dd>
    );
  }
  return (
    <dd className="subtle" title={NOT_SAMPLED}>
      —
    </dd>
  );
}

function StatsHeader({
  catalog,
  table,
  timeTravel,
}: {
  catalog: string;
  table: Table;
  timeTravel: boolean;
}) {
  // One test per cell would ask the same question three times; the server
  // sends the three totals together or not at all.
  const sampled = table.record_count !== undefined;
  return (
    <dl className="stats-header">
      <div>
        <dt>record_count</dt>
        <dd className="mono" title={sampled ? undefined : NOT_SAMPLED}>
          {formatCount(table.record_count)}
        </dd>
      </div>
      <div>
        <dt>file_count</dt>
        <dd className="mono" title={sampled ? undefined : NOT_SAMPLED}>
          {formatCount(table.file_count)}
        </dd>
      </div>
      <div>
        <dt>file_size_bytes</dt>
        <dd
          className="mono"
          title={sampled ? `${table.file_size_bytes}` : NOT_SAMPLED}
        >
          {formatBytes(table.file_size_bytes)}
        </dd>
      </div>
      {/* Beside the three it describes, not at the end of the header:
          these numbers are a SAMPLE and the reader has to see that in
          the same glance. */}
      <div>
        <dt>totals</dt>
        <TotalsFreshness catalog={catalog} table={table} timeTravel={timeTravel} />
      </div>
      <div>
        <dt>table_uuid</dt>
        <dd className="mono">
          {table.table_uuid} <CopyButton text={table.table_uuid} label="table_uuid" />
        </dd>
      </div>
      <div>
        <dt>partition_keys</dt>
        <dd className="mono">
          {table.partition_spec && table.partition_spec.fields.length > 0
            ? table.partition_spec.fields
                .map((f) => formatPartitionField(f, table.columns))
                .join(", ")
            : "—"}
        </dd>
      </div>
      <div>
        <dt>sort_order</dt>
        <dd className="mono">
          {table.sort_spec && table.sort_spec.fields.length > 0
            ? table.sort_spec.fields
                .map((f) => formatSortField(f, table.columns))
                .join(", ")
            : "—"}
        </dd>
      </div>
      <div>
        <dt>comment</dt>
        {/* A comment can run to 16384 chars, so it lives on its own field
            with a clamp + expand rather than inline with the short stats.
            TextContent only — it is user data, never HTML. */}
        <dd>
          {table.comment ? (
            <ClampedText
              text={table.comment}
              className="table-comment"
              maxChars={COMMENT_CLAMP}
            />
          ) : (
            "—"
          )}
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
            <th>comment</th>
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
              <td className="col-comment">
                {c.comment ? c.comment : <span className="subtle">—</span>}
              </td>
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
      <TableProperties properties={table.properties} />
    </div>
  );
}

/**
 * The table's user properties, sorted by key for a stable render. Empty
 * (the common case) reads as one line rather than an empty table. Keys
 * and values are user data — rendered as text, never HTML.
 */
function TableProperties({ properties }: { properties?: Record<string, string> }) {
  const entries = Object.entries(properties ?? {}).sort(([a], [b]) =>
    a < b ? -1 : a > b ? 1 : 0,
  );
  return (
    <div className="table-properties">
      <h3>
        Properties <span className="subtle">{entries.length}</span>
      </h3>
      {entries.length === 0 ? (
        <p className="empty">No properties.</p>
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <th>key</th>
              <th>value</th>
            </tr>
          </thead>
          <tbody>
            {entries.map(([key, value]) => (
              <tr key={key}>
                <td className="mono">{key}</td>
                <td className="mono">{value}</td>
              </tr>
            ))}
          </tbody>
        </table>
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
 *
 * [type] is the column's DECLARED type, and it is what decides whether a
 * bound of digits is digit-grouped: the decoded token for a long 626623
 * and for a string column holding "626623" are the same characters, so
 * only the type can say which one grouping would corrupt.
 */
function BoundCell({
  bound,
  nullTitle = NO_BOUND_TITLE,
  type,
}: {
  bound: DecodedBound;
  nullTitle?: string;
  /** "row_id" for the implicit ordering key, which has no catalog column. */
  type?: ColumnType | "row_id";
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
  //
  // This guard comes FIRST, before the grouping below: a conforming int64
  // cannot exceed 20 digits, but the width guard is the table's and must
  // hold for whatever a payload actually contains, not for what a type
  // promises.
  if (text.length > BOUND_INLINE_MAX_CHARS) {
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
  // Grouped like every other count in the console (626623 -> "626,623"),
  // on the exact decimal string — formatCount never round-trips through a
  // Number, so a uint64 bound past 2^53 groups without losing a digit.
  // The raw token stays in the title, for copying and for pasting into a
  // query.
  if (
    typeof bound === "string" &&
    (type === "row_id" || (type !== undefined && isIntegerColumnType(type))) &&
    /^-?\d+$/.test(bound)
  ) {
    return (
      <td className="num mono" title={bound}>
        {formatCount(bound)}
      </td>
    );
  }
  return <td className="num mono">{text}</td>;
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
            {/* The stats row states its own column's type, so the bound
                needs no schema lookup here. */}
            <BoundCell bound={c.lower_bound} type={c.type} />
            <BoundCell bound={c.upper_bound} type={c.type} />
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
          {/* ONE glyph, rotated — not ▸ swapped for ▾. The rotation is
              animatable (so open/close reads as a movement rather than a
              substitution) and it keeps the box identical in both states,
              which is what stops the column jumping. aria-expanded on the
              button already says which state it is in, so the mark itself
              is decoration. */}
          <span className="expand-chevron" aria-hidden="true">
            ▶
          </span>
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
  columns,
}: {
  file: DataFile;
  end: "lower" | "upper";
  /** The table's schema, to resolve the key's field_id to a type. */
  columns?: Column[];
}) {
  const bounds = file.ordering_bounds;
  if (!bounds) return <td className="num mono subtle">—</td>;
  // Absent field_id IS the row-id case: the row id is not a catalog
  // column, so the server has no field id to name it with.
  const rowIdSpan = bounds.field_id === undefined;
  return (
    <BoundCell
      bound={end === "lower" ? bounds.lower_bound : bounds.upper_bound}
      // A row id is a long by construction. Otherwise the key's own type,
      // which an unresolvable field_id leaves undefined — and an unknown
      // type means the bound renders verbatim rather than guessed at.
      type={rowIdSpan ? "row_id" : columnType(columns, bounds.field_id!)}
      nullTitle={
        rowIdSpan && end === "upper" ? UNKNOWN_ROW_ID_TITLE : NO_BOUND_TITLE
      }
    />
  );
}

/**
 * The columns the server can sort a file page by, each mapped to its
 * wire name. Only the raw stored columns: the listing is paginated
 * server-side, so sorting is the server's job (a client sort would only
 * order the loaded page, which is the misleading case the sort lib warns
 * about). The decoded columns — partition and the ordering-key min/max —
 * are shown but not sortable: their display order is computed from
 * encoded bytes and a text[] of ordinals, which no single SQL column's
 * order matches, so the server refuses to sort by them and neither can
 * an honest paginated client.
 */
type FileSortKey = "id" | "path" | "records" | "size" | "stats" | "snapshot";

const FILE_SORT_WIRE: Record<FileSortKey, string> = {
  id: "id",
  path: "path",
  records: "record_count",
  size: "size",
  stats: "stats",
  snapshot: "begin_snapshot",
};

/** Files shown per page; "Load more" walks offsets in the current sort. */
const FILE_PAGE_SIZE = 100;

/**
 * One dropdown per partition field, fed by GET .../partitions/values. The
 * server returns the stored, transformed values (a day ordinal, an
 * identity value); each is decoded for display exactly as the files
 * table's partition column is, and echoed back verbatim as the filter.
 * `key_index` is the field's position in the spec, which is what the
 * `partition=key_index:value` query param addresses.
 */
function PartitionFilterBar({
  spec,
  values,
  columns,
  filter,
  onChange,
}: {
  spec: PartitionSpec;
  values: PartitionValues;
  columns?: Column[];
  filter: Record<number, string>;
  onChange: (keyIndex: number, value: string) => void;
}) {
  const byFieldId = new Map(values.fields.map((f) => [String(f.source_field_id), f]));
  return (
    <div className="partition-filter">
      {spec.fields.map((field, keyIndex) => {
        const fieldValues = byFieldId.get(String(field.source_field_id));
        // The partition-spec block's label (formatPartitionField) for a real
        // transform — hour(ts) / month(ts) / bucket(16, url) — but identity
        // is just the column name, not identity(team_id).
        const label =
          field.transform === "identity"
            ? (columnPath(columns, field.source_field_id) ?? `field_${field.source_field_id}`)
            : formatPartitionField(field, columns);
        const current = filter[keyIndex] ?? "";
        if (fieldValues?.truncated) {
          // Over the distinct-value cap: a partial dropdown would mislead.
          return (
            <label key={keyIndex} className="partition-filter-field">
              {label}
              <span className="subtle">too many to list</span>
            </label>
          );
        }
        // Sort the options by their DECODED display value, so a temporal
        // field reads chronologically (2026-04, 2026-05…) and a long list
        // is browsable. The server returns most-frequent-first, which is
        // right for capping at the cap but the wrong order to browse. The
        // stored value stays the option's value; only the order changes.
        // An identity column of integers sorts NUMERICALLY (17, 42, 1042),
        // not lexically (1042, 17, 42) — the raw string is the value there.
        const raw = fieldValues?.values ?? [];
        const numeric =
          field.transform === "identity" &&
          raw.length > 0 &&
          raw.every((v) => v !== null && /^-?\d+$/.test(v));
        const options = raw
          .map((v) => ({
            stored: v,
            display: decodeValue(field.transform, v, field.transform_param),
          }))
          .sort((a, b) => {
            if (numeric) {
              const an = a.stored === null ? 0 : parseInt(a.stored, 10);
              const bn = b.stored === null ? 0 : parseInt(b.stored, 10);
              return an - bn;
            }
            return a.display < b.display ? -1 : a.display > b.display ? 1 : 0;
          });
        return (
          <label key={keyIndex} className="partition-filter-field">
            {label}
            <select
              value={current}
              onChange={(e) => onChange(keyIndex, e.target.value)}
              aria-label={`filter by ${label}`}
            >
              <option value="">all</option>
              {options.map((o, i) => (
                <option key={i} value={o.stored ?? ""}>
                  {o.display}
                </option>
              ))}
            </select>
          </label>
        );
      })}
    </div>
  );
}

function FilesTab({
  catalog,
  namespace,
  table,
  snapshot,
  spec,
  sortSpec,
  columns,
  initialFilter,
  onInitialFilterConsumed,
}: {
  catalog: string;
  namespace: string;
  table: string;
  snapshot?: Int64;
  spec?: PartitionSpec;
  sortSpec?: SortSpec;
  columns?: Column[];
  /**
   * `partition=key_index:value` params the URL arrived with — how the
   * partitions tab hands a row over. Seeds this tab's own state once,
   * after which the dropdowns own the filter. The PAGE decides whether
   * to hand them over (it passes undefined once they have been spent),
   * so a remount cannot re-seed from params the user has since
   * filtered away from; the params themselves stay on the URL, which
   * is what keeps the filtered view shareable.
   */
  initialFilter?: Record<number, string>;
  /**
   * Called once, on mount, when [initialFilter] was non-empty: the URL
   * params have been taken into this tab's own state, and the page
   * must not hand them over again on a later remount — otherwise any
   * tab switch resurrects the partition the user has since changed
   * away from. The params stay on the URL so the view is shareable;
   * the PAGE remembers that they have been spent.
   */
  onInitialFilterConsumed?: () => void;
}) {
  const [expanded, setExpanded] = useState<Int64 | null>(null);
  // null = the server's default order (manifest: begin_snapshot,
  // row_id_start, id). A set sort is in the query key, so it drives the
  // request and ordering happens server-side over the WHOLE table — a
  // client sort would only order the loaded page.
  const [sort, setSort] = useState<SortState<FileSortKey> | null>(null);
  const onSort = (key: FileSortKey) => setSort((prev) => nextSort(prev, key));
  // The active partition filter: key_index → the stored value to match.
  // One value per key; a key set to "" is unfiltered. Changing it refetches
  // from offset 0 via the query key.
  const [partitionFilter, setPartitionFilter] = useState<Record<number, string>>(
    initialFilter ?? {},
  );
  // READ ONCE. The empty dependency list is the whole point: the URL
  // seeds the dropdowns on arrival and the page then marks the params
  // SPENT (they stay on the URL, so the view is still addressable), so
  // this tab's own state is the only filter that moves afterwards.
  useEffect(() => {
    if (Object.keys(initialFilter ?? {}).length > 0) onInitialFilterConsumed?.();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  // The dropdown options, from the server. Only fetched for a partitioned
  // table; the values are stored strings, echoed back verbatim on apply.
  const valuesQuery = useQuery({
    queryKey: ["partition-values", catalog, namespace, table, snapshot ?? "head"],
    queryFn: () => getPartitionValues(catalog, namespace, table, snapshot),
    enabled: (spec?.fields.length ?? 0) > 0,
  });
  const activeFilter = Object.fromEntries(
    Object.entries(partitionFilter).filter(([, v]) => v !== ""),
  );
  const query = useInfiniteQuery({
    queryKey: ["files", catalog, namespace, table, snapshot ?? "head", sort, activeFilter],
    queryFn: ({ pageParam }) =>
      listFiles(catalog, namespace, table, snapshot, {
        sort: sort ? FILE_SORT_WIRE[sort.key] : undefined,
        order: sort ? (sort.desc ? "desc" : "asc") : undefined,
        limit: FILE_PAGE_SIZE,
        offset: pageParam,
        partition: Object.fromEntries(
          Object.entries(activeFilter).map(([k, v]) => [Number(k), v]),
        ),
      }),
    initialPageParam: 0,
    // A full page means there may be more; the next offset is the count
    // loaded so far. A short page is the end. (A table whose size is an
    // exact multiple of the page fetches one empty page to learn it has
    // stopped — the standard offset-paging cost.)
    getNextPageParam: (lastPage, allPages) =>
      lastPage.length === FILE_PAGE_SIZE
        ? allPages.length * FILE_PAGE_SIZE
        : undefined,
  });
  if (query.isError) return <ErrorBox error={query.error} />;
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
  // Decoded once per row for display; ordering is the server's.
  const rows = (query.data?.pages ?? [])
    .flat()
    .map((file) => ({ file, partition: decodePartition(file, spec, columns) }));
  return (
    <>
      {partitioned && valuesQuery.data && (
        <PartitionFilterBar
          spec={spec!}
          values={valuesQuery.data}
          columns={columns}
          filter={partitionFilter}
          onChange={(keyIndex, value) =>
            setPartitionFilter((prev) => ({ ...prev, [keyIndex]: value }))
          }
        />
      )}
      <table className="data-table">
        <thead>
          <tr>
            <SortableTh label="id" sortKey="id" sort={sort} onSort={onSort} numeric />
            {/* Partition is decoded client-side from a text[] of ordinals;
                no SQL column's order matches it, so under server paging it
                is shown but not sortable. */}
            {partitioned && <th>partition</th>}
            <SortableTh label="path" sortKey="path" sort={sort} onSort={onSort} />
            <SortableTh
              label="record_count"
              sortKey="records"
              sort={sort}
              onSort={onSort}
              numeric
            />
            <SortableTh label="size" sortKey="size" sort={sort} onSort={onSort} numeric />
            {/* Ordering-key bounds are decoded from encoded bytes, not a
                sortable SQL column — plain headers, not sortable. */}
            <th className="num">{`${keyName} min`}</th>
            <th className="num">{`${keyName} max`}</th>
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
        {query.isPending ? (
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
                  <OrderingBoundCell file={f} end="lower" columns={columns} />
                  <OrderingBoundCell file={f} end="upper" columns={columns} />
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
                  <td className="num mono">
                    <SnapshotId catalog={catalog} id={f.begin_snapshot} />
                  </td>
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
      {query.hasNextPage && (
        <button
          type="button"
          disabled={query.isFetchingNextPage}
          onClick={() => void query.fetchNextPage()}
        >
          {query.isFetchingNextPage ? "Loading…" : "Load more"}
        </button>
      )}
    </>
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

/**
 * The columns the partitions listing can be sorted by, and the
 * direction each one starts in. `partition` reads ascending, every
 * measure descending: a measure column is clicked to find the biggest.
 * The wire names ARE these keys (the server's SortColumn), so there is
 * no mapping table to drift.
 */
type PartitionSortKey =
  | "partition"
  | "files"
  | "small_files"
  | "debt"
  | "total_size"
  | "avg_size"
  | "dvs"
  | "rows"
  | "last_written";

const PARTITION_SORT_DESC_DEFAULT: Record<PartitionSortKey, boolean> = {
  partition: false,
  files: true,
  small_files: true,
  debt: true,
  total_size: true,
  avg_size: true,
  dvs: true,
  rows: true,
  last_written: true,
};

/** Partitions shown per page; "Load more" walks offsets in the current sort. */
const PARTITION_PAGE_SIZE = 100;

/**
 * How long typing settles before the filter is sent. Long enough that a
 * typed month is one request rather than seven, short enough that the
 * table does not feel detached from the box.
 */
const FILTER_DEBOUNCE_MS = 250;

/**
 * The partitions tab.
 *
 * WHAT IT SHOWS AND WHAT IT DOES NOT: every number here is the
 * maintenance sampler's, at the snapshot the footer names, not at head.
 * That is what makes the tab free — the server reads the sampler's
 * output instead of walking the manifest — and it is also why the tab
 * ignores the page's snapshot selector: there is exactly one snapshot
 * the sample can answer at, so honouring `?snapshot=` would be a
 * time-travel control that silently did nothing.
 *
 * Sorting, filtering and paging are all the SERVER's. The listing is
 * paged, so a client-side sort would order the loaded page only, which
 * is the misleading case `lib/sort` warns about; and the filter matches
 * the DECODED value (the server decodes for exactly this reason), so
 * `2026-09` finds every day of that month.
 */
function PartitionsTab({
  catalog,
  namespace,
  table,
  onOpenFiles,
}: {
  catalog: string;
  namespace: string;
  table: string;
  /** Switch to the files tab with this partition's stored values applied. */
  onOpenFiles: (group: PartitionGroup) => string;
}) {
  const [sort, setSort] = useState<SortState<PartitionSortKey>>({
    key: "partition",
    desc: false,
  });
  // What the inputs hold, and what the last debounce committed. Two
  // states on purpose: typing must not fire a request per keystroke,
  // and the committed map is what the query key is built from, so a
  // re-render while typing does not refetch.
  const [draft, setDraft] = useState<Record<number, string>>({});
  // Keys whose "is null" toggle is on. SEPARATE from `draft`, because
  // the wire form of "the null value" is an EMPTY filter text — and an
  // empty text box means "unfiltered" to every user who has ever used
  // one. Overloading the two made clearing a box silently select the
  // null partitions and empty the table, with no way back.
  const [nullOnly, setNullOnly] = useState<Record<number, boolean>>({});
  const [filter, setFilter] = useState<Record<number, string>>({});
  useEffect(() => {
    const id = setTimeout(
      () =>
        setFilter({
          // Empty entries are dropped, exactly as FilesTab does with
          // its dropdowns: a cleared box is no filter at all. The only
          // way to send `key:` is the toggle, which is explicit.
          ...Object.fromEntries(Object.entries(draft).filter(([, v]) => v !== "")),
          ...Object.fromEntries(
            Object.entries(nullOnly)
              .filter(([, on]) => on)
              .map(([k]) => [k, ""]),
          ),
        }),
      FILTER_DEBOUNCE_MS,
    );
    return () => clearTimeout(id);
  }, [draft, nullOnly]);

  const query = useInfiniteQuery({
    queryKey: ["partitions", catalog, namespace, table, sort, filter],
    queryFn: ({ pageParam }) =>
      listTablePartitions(catalog, namespace, table, {
        sort: sort.key,
        order: sort.desc ? "desc" : "asc",
        limit: PARTITION_PAGE_SIZE,
        offset: pageParam,
        filter,
      }),
    initialPageParam: 0,
    // KEEP THE LAST PAGE WHILE A NEW FILTER OR SORT LOADS. Without it
    // `query.data` goes undefined on every key change, which blanks the
    // spec this component builds its filter boxes from — so typing into
    // a box unmounted the box mid-keystroke. It also stops the table
    // flashing to a skeleton on each debounce.
    placeholderData: (previous) => previous,
    // `total` is the server's count of the whole match, so has-more is
    // exact — no empty last page, unlike the files tab's length probe.
    getNextPageParam: (lastPage, allPages) => {
      const loaded = allPages.reduce((n, p) => n + p.partitions.length, 0);
      return loaded < lastPage.total ? loaded : undefined;
    },
  });
  const first = query.data?.pages[0];
  const spec = first?.spec;
  // `placeholderData` keeps the previous pages while a new key loads,
  // so `spec` survives a filter change; on an ERROR react-query drops
  // to `data: undefined`, which would take the filter bar with it.
  const lastSpec = useRef<PartitionSpecSummary | undefined>(undefined);
  if (spec !== undefined) lastSpec.current = spec;
  const filterFields = (spec ?? lastSpec.current)?.fields ?? [];
  const rows = (query.data?.pages ?? []).flatMap((p) => p.partitions);
  const keyCount = filterFields.length;
  // partition, files, small, debt, total, avg, dvs, rows, last written.
  const cols = 9;
  const onSort = (key: PartitionSortKey) =>
    setSort((prev) =>
      prev.key === key
        ? { key, desc: !prev.desc }
        : { key, desc: PARTITION_SORT_DESC_DEFAULT[key] },
    );

  // The spec, and therefore the filter bar, survives an error: a 422
  // from an out-of-range key_index must leave the inputs that produced
  // it on screen, or the only recoverable error on this tab is
  // unrecoverable. `filterFields` falls back to the last spec seen.
  return (
    <>
      {keyCount > 0 && (
        <div className="partition-filter">
          {filterFields.map((field, keyIndex) => (
            <label key={keyIndex} className="partition-filter-field">
              {field.field}
              <input
                value={draft[keyIndex] ?? ""}
                placeholder="prefix"
                disabled={nullOnly[keyIndex] === true}
                aria-label={`filter by ${field.field}`}
                onChange={(e) =>
                  setDraft((prev) => ({ ...prev, [keyIndex]: e.target.value }))
                }
              />
              {/* The null partition value has no text to prefix-match,
                  so it needs its own control. It replaces the prefix
                  rather than combining with it, which is why the box
                  goes disabled. */}
              <span className="partition-filter-null">
                <input
                  type="checkbox"
                  checked={nullOnly[keyIndex] === true}
                  aria-label={`${field.field} is null`}
                  onChange={(e) =>
                    setNullOnly((prev) => ({ ...prev, [keyIndex]: e.target.checked }))
                  }
                />
                is null
              </span>
            </label>
          ))}
        </div>
      )}
      {query.isError && <ErrorBox error={query.error} />}
      <table className="data-table">
        <thead>
          <tr>
            <SortableTh
              label="partition"
              sortKey="partition"
              sort={sort}
              onSort={onSort}
              tooltip="Decoded partition values, in key order. Sorted server-side by the decoded tuple, nulls first."
            />
            <SortableTh label="files" sortKey="files" sort={sort} onSort={onSort} numeric />
            <SortableTh
              label="small files"
              sortKey="small_files"
              sort={sort}
              onSort={onSort}
              numeric
              tooltip="Files strictly under the compaction target size."
            />
            <SortableTh
              label="debt"
              sortKey="debt"
              sort={sort}
              onSort={onSort}
              numeric
              tooltip="Files the compaction planner would actually group — actionable debt, not just small files."
            />
            <SortableTh
              label="total size"
              sortKey="total_size"
              sort={sort}
              onSort={onSort}
              numeric
            />
            <SortableTh label="avg size" sortKey="avg_size" sort={sort} onSort={onSort} numeric />
            <SortableTh label="DVs" sortKey="dvs" sort={sort} onSort={onSort} numeric />
            <SortableTh
              label="rows"
              sortKey="rows"
              sort={sort}
              onSort={onSort}
              numeric
              tooltip="Rows in the partition. Blank when the published sample predates the server measuring them — not zero."
            />
            <SortableTh
              label="last written"
              sortKey="last_written"
              sort={sort}
              onSort={onSort}
              numeric
              tooltip="The newest snapshot that wrote a file into this partition."
            />
          </tr>
        </thead>
        {query.isPending ? (
          <SkeletonRows rows={5} cols={cols} />
        ) : (
          <tbody>
            {rows.length === 0 && (
              <tr>
                <td colSpan={cols} className="empty">
                  {query.isError
                    ? "The request above failed."
                    : first?.sampled_at === null
                      ? "No sample yet."
                      : "No partitions match this filter."}
                </td>
              </tr>
            )}
            {rows.map((p, i) => (
              <tr key={i}>
                <td className="partition-cell">
                  {/* The link carries the STORED values, which is what
                      the files listing matches on — the decoded form is
                      for the reader, never for the wire. */}
                  <Link to={onOpenFiles(p)}>
                    {p.values.length === 0 ? (
                      <span className="subtle">unpartitioned</span>
                    ) : (
                      p.values.map((v, j) => (
                        <span key={j} className="partition-part">
                          {j > 0 && <span className="partition-sep"> / </span>}
                          <span className="partition-field">{v.field}</span>
                          <span className="partition-eq">=</span>
                          <span className="mono partition-value">
                            {v.decoded ?? "null"}
                          </span>
                        </span>
                      ))
                    )}
                  </Link>{" "}
                  {spec !== undefined && String(p.spec_id) !== String(spec.spec_id) && (
                    <span
                      className="badge badge-warn"
                      title="Written under an older partition spec than the table's current one."
                    >
                      spec {p.spec_id === undefined ? "none" : String(p.spec_id)}
                    </span>
                  )}
                </td>
                <td className="num mono">{formatCount(p.file_count)}</td>
                <td className="num mono">{formatCount(p.small_file_count)}</td>
                <td className="num mono">{formatCount(p.debt_score)}</td>
                <td className="num mono" title={`${p.total_bytes}`}>
                  {formatBytes(p.total_bytes)}
                </td>
                <td className="num mono" title={`${p.avg_file_bytes}`}>
                  {formatBytes(p.avg_file_bytes)}
                </td>
                <td className="num mono">{formatCount(p.dv_count)}</td>
                {/* An em dash, not 0: the sample never measured this. */}
                <td className="num mono">
                  {p.record_count === null ? (
                    <span className="subtle" title="Not measured by this sample">
                      —
                    </span>
                  ) : (
                    formatCount(p.record_count)
                  )}
                </td>
                <td className="num mono">
                  {p.last_written_snapshot === null ? (
                    <span className="subtle" title="Not measured by this sample">
                      —
                    </span>
                  ) : (
                    <SnapshotId catalog={catalog} id={p.last_written_snapshot} />
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        )}
      </table>
      {query.hasNextPage && (
        <button
          type="button"
          disabled={query.isFetchingNextPage}
          onClick={() => void query.fetchNextPage()}
        >
          {query.isFetchingNextPage ? "Loading…" : "Load more"}
        </button>
      )}
      {first !== undefined && (
        // WHILE A REFETCH IS IN FLIGHT the rows on screen are the
        // previous filter's (placeholderData), so the counts beside
        // them are the previous filter's too. Say "loading" rather
        // than a number that belongs to a page the user has moved off.
        <PartitionsFooter
          catalog={catalog}
          listing={first}
          stale={query.isFetching}
        />
      )}
    </>
  );
}

/**
 * What the numbers above are, in one line: how many, how old, and how
 * many belong to a spec the table has since replaced. The freshness is
 * not decoration — every measure on the page is as old as this says.
 */
function PartitionsFooter({
  catalog,
  listing,
  stale,
}: {
  catalog: string;
  listing: PartitionListing;
  stale?: boolean;
}) {
  if (stale) return <p className="subtle">Loading partitions…</p>;
  if (listing.sampled_at === null) {
    return (
      <p className="subtle">
        No sample yet; the maintenance sampler has not published for this catalog.
      </p>
    );
  }
  return (
    <p
      className="subtle"
      // Both timestamps, because the gap between them IS the sample: a
      // generation runs for tens of minutes and the numbers are as old
      // as its START, not as its publish.
      title={`scan started ${listing.sample_started ?? "?"}, published ${listing.sampled_at}`}
    >
      {formatCount(listing.total)} partitions · sampled{" "}
      {formatAge(listing.sample_started ?? listing.sampled_at)} ago at snapshot{" "}
      {listing.sampled_snapshot_id === null ? (
        "—"
      ) : (
        <SnapshotId catalog={catalog} id={listing.sampled_snapshot_id} />
      )}
      {listing.stale_spec_groups > 0 &&
        ` · ${listing.stale_spec_groups} under an older spec`}
    </p>
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

  /**
   * `?partition=key_index:value` on the URL, as a filter map. This is
   * how the partitions tab hands a row to the files tab: the link
   * carries the STORED values, which is what the files listing matches
   * on, and the files tab seeds its dropdowns from them.
   */
  const urlPartitionFilter: Record<number, string> = Object.fromEntries(
    searchParams
      .getAll("partition")
      .map((p) => p.split(":"))
      .filter((parts) => parts.length >= 2 && /^\d+$/.test(parts[0]))
      .map((parts) => [Number(parts[0]), parts.slice(1).join(":")]),
  );

  /**
   * The files tab, filtered to one partition. The snapshot rides along
   * so a time-travelled page stays where it was; the partition values
   * are the stored ones, never the decoded display form.
   */
  const filesHref = (group: PartitionGroup): string => {
    const next = new URLSearchParams();
    next.set("tab", "files");
    if (snapshot !== undefined) next.set("snapshot", snapshot);
    group.values.forEach((v, keyIndex) => {
      if (v.raw !== null) next.append("partition", `${keyIndex}:${v.raw}`);
    });
    return `?${next.toString()}`;
  };

  /**
   * Whether the URL's `partition=` params have already seeded the files
   * tab in this page's lifetime.
   *
   * A REF, and it lives HERE rather than in FilesTab, because FilesTab
   * unmounts on every tab switch while this component does not — so a
   * flag inside it would reset and re-seed from a URL the user has
   * since filtered away from (the round-2 finding). The params
   * THEMSELVES stay on the URL: erasing them made the filtered files
   * view unaddressable, and `tab`/`snapshot` set the convention that
   * the URL is the state.
   */
  const partitionFilterSeeded = useRef(false);

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
          <span className="badge time-travel">
            @ snapshot <SnapshotId catalog={catalog!} id={snapshot} />
          </span>
        )}
      </h2>
      {tableQuery.isError ? (
        <ErrorBox error={tableQuery.error} />
      ) : tableQuery.isPending ? (
        <SkeletonBlock />
      ) : (
        <StatsHeader
          catalog={catalog!}
          table={tableQuery.data}
          // The REQUEST's own parameter. The page has it; the response
          // cannot be asked (see TotalsFreshness).
          timeTravel={snapshot !== undefined}
        />
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
          initialFilter={
            partitionFilterSeeded.current ? undefined : urlPartitionFilter
          }
          onInitialFilterConsumed={() => {
            partitionFilterSeeded.current = true;
          }}
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
      {tab === "partitions" && (
        <PartitionsTab
          catalog={catalog}
          namespace={namespace}
          table={table}
          onOpenFiles={filesHref}
        />
      )}
    </section>
  );
}
