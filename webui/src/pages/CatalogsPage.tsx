import { useState } from "react";
import { Link } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createCatalog, listCatalogs } from "../api/client";
import type { Catalog } from "../api/types";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonRows } from "../components/Skeleton";
import { formatAge, formatBytes, formatCount } from "../lib/format";
import { ageColumn, applySort, int64Column, nextSort, textColumn } from "../lib/sort";
import type { ColumnSort, SortState } from "../lib/sort";
import { SortableTh } from "../components/SortableTh";

type SortKey =
  | "name"
  | "path"
  | "tables"
  | "rows"
  | "size"
  | "head"
  | "oldest"
  | "schema";

const COMPARATORS: Record<SortKey, ColumnSort<Catalog>> = {
  name: textColumn((c) => c.name),
  path: textColumn((c) => c.data_path),
  // The three sampler totals are absent — not zero — on a catalog the
  // sampler has not reached, so int64Column keeps those rows off the top
  // of a largest-first sort.
  tables: int64Column((c) => c.table_count),
  rows: int64Column((c) => c.live_rows),
  size: int64Column((c) => c.live_size_bytes),
  head: int64Column((c) => c.head_snapshot_id),
  // Descending-first surfaces the oldest snapshot; unsampled catalogs
  // (no timestamp) sort last, not to the top.
  oldest: ageColumn((c) => c.oldest_snapshot_time),
  schema: int64Column((c) => c.schema_version),
};

function CreateCatalogForm() {
  const queryClient = useQueryClient();
  const [name, setName] = useState("");
  const [dataPath, setDataPath] = useState("");
  const mutation = useMutation({
    mutationFn: () => createCatalog({ name, data_path: dataPath }),
    onSuccess: () => {
      setName("");
      setDataPath("");
      void queryClient.invalidateQueries({ queryKey: ["catalogs"] });
    },
  });

  return (
    <form
      className="inline-form"
      onSubmit={(e) => {
        e.preventDefault();
        mutation.mutate();
      }}
    >
      <h3>Create catalog</h3>
      <div className="form-row">
        <label>
          name
          <input
            value={name}
            onChange={(e) => setName(e.target.value)}
            required
            placeholder="analytics"
          />
        </label>
        <label>
          data_path
          <input
            value={dataPath}
            onChange={(e) => setDataPath(e.target.value)}
            required
            placeholder="s3://bucket/prefix"
          />
        </label>
        <button type="submit" disabled={mutation.isPending}>
          {mutation.isPending ? "Creating…" : "Create"}
        </button>
      </div>
      {mutation.isError && <ErrorBox error={mutation.error} />}
    </form>
  );
}

export function CatalogsPage() {
  const { data, isPending, isError, error } = useQuery({
    queryKey: ["catalogs"],
    queryFn: listCatalogs,
  });
  // null = the server's order (name).
  const [sort, setSort] = useState<SortState<SortKey> | null>(null);
  const onSort = (key: SortKey) => setSort((prev) => nextSort(prev, key));
  const rows = applySort(data ?? [], sort, COMPARATORS);

  return (
    <section>
      <h2>Catalogs</h2>
      {isError ? (
        <ErrorBox error={error} />
      ) : (
        <table className="data-table">
          <thead>
            <tr>
              <SortableTh label="name" sortKey="name" sort={sort} onSort={onSort} />
              <SortableTh
                label="data_path"
                sortKey="path"
                sort={sort}
                onSort={onSort}
              />
              <SortableTh
                label="tables"
                sortKey="tables"
                sort={sort}
                onSort={onSort}
                numeric
              />
              <SortableTh
                label="rows"
                sortKey="rows"
                sort={sort}
                onSort={onSort}
                numeric
              />
              <SortableTh
                label="size"
                sortKey="size"
                sort={sort}
                onSort={onSort}
                numeric
              />
              <SortableTh
                label="head_snapshot_id"
                sortKey="head"
                sort={sort}
                onSort={onSort}
                numeric
              />
              <SortableTh
                label="oldest_snapshot"
                sortKey="oldest"
                sort={sort}
                onSort={onSort}
                numeric
                tooltip="Age of the oldest snapshot the catalog still retains"
              />
              <SortableTh
                label="schema_version"
                sortKey="schema"
                sort={sort}
                onSort={onSort}
                numeric
              />
            </tr>
          </thead>
          {isPending ? (
            <SkeletonRows rows={4} cols={8} />
          ) : (
            <tbody>
              {rows.length === 0 && (
                <tr>
                  <td colSpan={8} className="empty">
                    No catalogs yet.
                  </td>
                </tr>
              )}
              {rows.map((c) => (
                <tr key={c.name}>
                  <td>
                    <Link to={`/catalogs/${encodeURIComponent(c.name)}`}>
                      {c.name}
                    </Link>
                  </td>
                  <td className="mono">{c.data_path}</td>
                  {/* An em dash where the sampler has not reached this
                      catalog yet: zero would claim it is empty. */}
                  <td className="num mono">
                    {c.table_count === undefined ? "—" : formatCount(c.table_count)}
                  </td>
                  <td className="num mono">
                    {c.live_rows === undefined ? "—" : formatCount(c.live_rows)}
                  </td>
                  <td
                    className="num mono"
                    title={c.live_size_bytes === undefined ? undefined : `${c.live_size_bytes} bytes`}
                  >
                    {c.live_size_bytes === undefined ? "—" : formatBytes(c.live_size_bytes)}
                  </td>
                  <td className="num mono">{c.head_snapshot_id}</td>
                  {/* An em dash until the catalog has been sampled, like
                      the totals: an unknown age is not a zero age. */}
                  <td
                    className="num mono"
                    title={
                      c.oldest_snapshot_time === undefined
                        ? undefined
                        : c.oldest_snapshot_time
                    }
                  >
                    {formatAge(c.oldest_snapshot_time)}
                  </td>
                  <td className="num mono">{c.schema_version}</td>
                </tr>
              ))}
            </tbody>
          )}
        </table>
      )}
      <CreateCatalogForm />
    </section>
  );
}
