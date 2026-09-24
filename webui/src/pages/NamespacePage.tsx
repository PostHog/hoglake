import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createTable, listTables } from "../api/client";
import { SCALAR_COLUMN_TYPES, type ColumnType, type TableSummary } from "../api/types";
import { ClampedText } from "../components/ClampedText";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonRows } from "../components/Skeleton";
import { formatBytes, formatCount } from "../lib/format";
import { columnNameError, identifierError } from "../lib/names";
import { applySort, int64Column, nextSort, textColumn } from "../lib/sort";
import type { ColumnSort, SortState } from "../lib/sort";
import { SortableTh } from "../components/SortableTh";

type SortKey =
  "name" | "rows" | "files" | "size" | "snapshots" | "earliest" | "comment";

const COMPARATORS: Record<SortKey, ColumnSort<TableSummary>> = {
  name: textColumn((t) => t.name),
  // int64Column marks a missing value `absent`, so a row from a server
  // that predates these fields sorts LAST rather than riding to the top
  // of a largest-first click as a phantom zero.
  rows: int64Column((t) => t.record_count),
  files: int64Column((t) => t.file_count),
  size: int64Column((t) => t.file_size_bytes),
  snapshots: int64Column((t) => t.snapshot_count),
  earliest: int64Column((t) => t.earliest_snapshot_id),
  comment: textColumn((t) => t.comment),
};

/**
 * Comment clamp for a table ROW, shorter than the table page's 120: a
 * comment runs to 16384 chars and this one shares its row with six other
 * columns, so the head has to fit a cell rather than a description list.
 */
const COMMENT_CLAMP = 60;

/**
 * A count cell: digit-GROUPED on screen, exact in the title.
 *
 * formatCount, not formatCompactCount: these are a table's own row and
 * file counts, which a reader compares between rows and reads off
 * exactly ("1,234,567", not "1.2M"). The compact form is for headline
 * tiles, where the magnitude is the whole message. The title carries
 * the ungrouped digits either way, because grouping is a display
 * convention and copy-paste should not inherit it.
 */
function CountCell({ value }: { value?: string }) {
  return (
    <td className="num mono" title={value}>
      {formatCount(value)}
    </td>
  );
}

interface ColumnRow {
  name: string;
  type: ColumnType;
  nullable: boolean;
}

const emptyColumn = (): ColumnRow => ({
  name: "",
  type: "string",
  nullable: true,
});

function CreateTableForm({
  catalog,
  namespace,
}: {
  catalog: string;
  namespace: string;
}) {
  const queryClient = useQueryClient();
  const [name, setName] = useState("");
  const [columns, setColumns] = useState<ColumnRow[]>([emptyColumn()]);

  const mutation = useMutation({
    mutationFn: () =>
      createTable(catalog, namespace, {
        name,
        columns: columns.map((c) => ({
          name: c.name,
          type: c.type,
          nullable: c.nullable,
        })),
      }),
    onSuccess: () => {
      setName("");
      setColumns([emptyColumn()]);
      void queryClient.invalidateQueries({
        queryKey: ["tables", catalog, namespace],
      });
    },
  });

  const updateColumn = (i: number, patch: Partial<ColumnRow>) =>
    setColumns((cols) =>
      cols.map((c, idx) => (idx === i ? { ...c, ...patch } : c)),
    );

  // Mirror the server's identifier pattern (422 on violation) client-side;
  // columns additionally refuse the reserved `_hog` prefix.
  const nameError = identifierError(name);
  const columnErrors = columns.map((c) => columnNameError(c.name));
  const hasErrors = nameError !== null || columnErrors.some((e) => e !== null);

  return (
    <form
      className="inline-form"
      onSubmit={(e) => {
        e.preventDefault();
        if (hasErrors) return;
        mutation.mutate();
      }}
    >
      <h3>Create table</h3>
      <div className="form-row">
        <label>
          name
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
            placeholder="pageviews"
            aria-invalid={nameError !== null}
          />
        </label>
      </div>
      {nameError && <p className="field-error">table name: {nameError}</p>}
      <div className="column-rows">
        {columns.map((col, i) => (
          <div className="form-row column-row" key={i}>
            <label>
              column
              <input
                value={col.name}
                onChange={(e) => updateColumn(i, { name: e.target.value })}
                required
                placeholder={`col_${i + 1}`}
                aria-label={`column ${i + 1} name`}
                aria-invalid={columnErrors[i] !== null}
              />
            </label>
            <label>
              type
              <select
                value={col.type}
                onChange={(e) =>
                  updateColumn(i, { type: e.target.value as ColumnType })
                }
                aria-label={`column ${i + 1} type`}
              >
                {SCALAR_COLUMN_TYPES.map((t) => (
                  <option key={t} value={t}>
                    {t}
                  </option>
                ))}
              </select>
            </label>
            <label className="check">
              <input
                type="checkbox"
                checked={col.nullable}
                onChange={(e) => updateColumn(i, { nullable: e.target.checked })}
                aria-label={`column ${i + 1} nullable`}
              />
              nullable
            </label>
            <button
              type="button"
              className="ghost"
              onClick={() =>
                setColumns((cols) => cols.filter((_, idx) => idx !== i))
              }
              disabled={columns.length === 1}
              aria-label={`remove column ${i + 1}`}
            >
              ×
            </button>
            {columnErrors[i] && (
              <p className="field-error">
                column {i + 1}: {columnErrors[i]}
              </p>
            )}
          </div>
        ))}
      </div>
      <div className="form-row">
        <button
          type="button"
          className="ghost"
          onClick={() => setColumns((cols) => [...cols, emptyColumn()])}
        >
          + Add column
        </button>
        <button type="submit" disabled={mutation.isPending || hasErrors}>
          {mutation.isPending ? "Creating…" : "Create table"}
        </button>
      </div>
      {mutation.isError && <ErrorBox error={mutation.error} />}
    </form>
  );
}

export function NamespacePage() {
  const { catalog, namespace } = useParams();
  const enabled = Boolean(catalog && namespace);
  const { data, isPending, isError, error } = useQuery({
    queryKey: ["tables", catalog, namespace],
    queryFn: () => listTables(catalog!, namespace!),
    enabled,
  });
  // null = the server's order (name).
  const [sort, setSort] = useState<SortState<SortKey> | null>(null);
  const onSort = (key: SortKey) => setSort((prev) => nextSort(prev, key));
  const rows = applySort(data ?? [], sort, COMPARATORS);
  if (!catalog || !namespace) return null;

  return (
    <section>
      <h2>
        {namespace} <span className="subtle">namespace</span>
      </h2>
      {isError ? (
        <ErrorBox error={error} />
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <SortableTh label="name" sortKey="name" sort={sort} onSort={onSort} />
              <SortableTh
                label="record_count"
                sortKey="rows"
                sort={sort}
                onSort={onSort}
                numeric
                tooltip="Rows in the files live at head, before deletion vectors: a row a live DV masks is still counted"
              />
              <SortableTh
                label="file_count"
                sortKey="files"
                sort={sort}
                onSort={onSort}
                numeric
              />
              <SortableTh
                label="file_size"
                sortKey="size"
                sort={sort}
                onSort={onSort}
                numeric
              />
              <SortableTh
                label="snapshots"
                sortKey="snapshots"
                sort={sort}
                onSort={onSort}
                numeric
                tooltip="Retained snapshots that carry a change row for this table"
              />
              <SortableTh
                label="earliest_snapshot"
                sortKey="earliest"
                sort={sort}
                onSort={onSort}
                numeric
                tooltip="Oldest snapshot this table can still be read at"
              />
              <SortableTh label="comment" sortKey="comment" sort={sort} onSort={onSort} />
            </tr>
          </thead>
          {isPending ? (
            <SkeletonRows rows={4} cols={7} />
          ) : (
            <tbody>
              {rows.length === 0 && (
                <tr>
                  <td colSpan={7} className="empty">
                    No tables in this namespace.
                  </td>
                </tr>
              )}
              {rows.map((t) => (
                <tr key={t.table_uuid}>
                  <td>
                    <Link
                      to={`/catalogs/${encodeURIComponent(catalog)}/namespaces/${encodeURIComponent(namespace)}/tables/${encodeURIComponent(t.name)}`}
                    >
                      {t.name}
                    </Link>
                  </td>
                  <CountCell value={t.record_count} />
                  <CountCell value={t.file_count} />
                  <td
                    className="num mono"
                    title={t.file_size_bytes && `${t.file_size_bytes} bytes`}
                  >
                    {formatBytes(t.file_size_bytes)}
                  </td>
                  {/* Snapshot counts and ids are plain numbers, not
                      humanized: "1.2K snapshots" and a truncated snapshot
                      id are both worse than the digits. */}
                  <td className="num mono">{t.snapshot_count ?? "—"}</td>
                  <td className="num mono">{t.earliest_snapshot_id ?? "—"}</td>
                  {/* A comment runs to 16384 chars, so the cell shows its
                      first line and keeps the row's height. Text content
                      only — it is user data, never HTML. */}
                  <td className="table-comment-cell">
                    {t.comment ? (
                      <ClampedText text={t.comment} maxChars={COMMENT_CLAMP} />
                    ) : (
                      "—"
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          )}
        </table>
      )}
      <CreateTableForm catalog={catalog} namespace={namespace} />
    </section>
  );
}
