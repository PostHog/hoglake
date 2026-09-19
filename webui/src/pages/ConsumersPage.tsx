import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { listConsumers } from "../api/client";
import type { ConsumerSummary, ConsumerTableOffset } from "../api/types";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonRows } from "../components/Skeleton";
import { formatTime } from "../lib/format";
import { applySort, int64Column, nextSort, textColumn } from "../lib/sort";
import type { ColumnSort, SortState } from "../lib/sort";
import { SortableTh } from "../components/SortableTh";

type SortKey = "table" | "uuid" | "snapshot" | "updated";

const COMPARATORS: Record<SortKey, ColumnSort<ConsumerTableOffset>> = {
  table: textColumn((o) => (o.table_name ? `${o.namespace}.${o.table_name}` : "")),
  uuid: textColumn((o) => o.table_uuid),
  snapshot: int64Column((o) => o.committed_snapshot),
  updated: textColumn((o) => o.updated_at),
};

/**
 * One consumer's offsets. A component rather than JSX inside the map so
 * each table carries its own sort state — sorting one consumer's offsets
 * must not reorder another's.
 */
function ConsumerGroup({
  consumer,
  catalog,
}: {
  consumer: ConsumerSummary;
  catalog: string;
}) {
  const [sort, setSort] = useState<SortState<SortKey> | null>(null);
  const onSort = (key: SortKey) => setSort((prev) => nextSort(prev, key));
  const rows = applySort(consumer.offsets, sort, COMPARATORS);
  return (
    <div className="consumer-group">
      <h3 className="mono">{consumer.consumer_id}</h3>
      <table className="data-table">
        <thead>
          <tr>
            <SortableTh label="table" sortKey="table" sort={sort} onSort={onSort} />
            <SortableTh
              label="table_uuid"
              sortKey="uuid"
              sort={sort}
              onSort={onSort}
            />
            <SortableTh
              label="committed_snapshot"
              sortKey="snapshot"
              sort={sort}
              onSort={onSort}
              numeric
            />
            <SortableTh
              label="updated_at"
              sortKey="updated"
              sort={sort}
              onSort={onSort}
            />
          </tr>
        </thead>
        <tbody>
          {rows.map((o) => (
            <tr key={o.table_uuid}>
              <td>
                {o.table_name ? (
                  o.table_dropped ? (
                    <>
                      {o.namespace}.{o.table_name}{" "}
                      <span className="badge badge-warn">dropped</span>
                    </>
                  ) : (
                    <Link
                      to={`/catalogs/${encodeURIComponent(catalog)}/namespaces/${encodeURIComponent(o.namespace!)}/tables/${encodeURIComponent(o.table_name)}`}
                    >
                      {o.namespace}.{o.table_name}
                    </Link>
                  )
                ) : (
                  <span className="subtle">unknown table</span>
                )}
              </td>
              <td className="mono">{o.table_uuid}</td>
              <td className="num mono">{o.committed_snapshot}</td>
              <td className="mono">{formatTime(o.updated_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function ConsumersPage() {
  const { catalog } = useParams();
  const [filter, setFilter] = useState("");

  const query = useQuery({
    queryKey: ["consumers", catalog],
    queryFn: () => listConsumers(catalog!),
    enabled: Boolean(catalog),
  });
  if (!catalog) return null;

  const needle = filter.trim().toLowerCase();
  const consumers = (query.data?.consumers ?? []).filter(
    (c) => !needle || c.consumer_id.toLowerCase().includes(needle),
  );

  return (
    <section>
      <h2>
        Consumers <span className="subtle">{catalog}</span>
      </h2>
      <div className="form-row">
        <label>
          filter
          <input
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="consumer id…"
            aria-label="filter consumers"
          />
        </label>
      </div>
      {query.isError && <ErrorBox error={query.error} />}
      {query.isPending && (
        <table className="data-table">
          <SkeletonRows rows={3} cols={5} />
        </table>
      )}
      {query.isSuccess && consumers.length === 0 && (
        <p className="empty">
          {query.data.consumers.length === 0
            ? "No consumer has committed an offset in this catalog."
            : "No consumer matches the filter."}
        </p>
      )}
      {consumers.map((c) => (
        <ConsumerGroup key={c.consumer_id} consumer={c} catalog={catalog} />
      ))}
    </section>
  );
}
