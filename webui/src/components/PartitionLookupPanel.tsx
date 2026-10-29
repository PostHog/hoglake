import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { useLocation, useSearchParams } from "react-router-dom";
import { useInfiniteQuery, useQueryClient } from "@tanstack/react-query";
import { ApiError, lookupPartition } from "../api/client";
import type { Int64 } from "../api/types";
import { formatBytes, formatCount } from "../lib/format";
import {
  parsePartitionLookupInput,
  partitionLookupParams,
  readPartitionLookupRequest,
} from "../lib/partitionLookup";
import type { PartitionLookupRequest } from "../lib/partitionLookup";
import { CopyButton } from "./CopyButton";
import { ErrorBox } from "./ErrorBox";
import { SkeletonBlock } from "./Skeleton";
import { SnapshotId } from "./SnapshotId";
import { StatsStateBadge } from "./badges";

const PAGE_SIZE = 100;
type Page = { snapshot?: Int64; cursor?: string };

function LookupError({ error, onRetry, onHead }: {
  error: unknown;
  onRetry: () => void;
  onHead: () => void;
}) {
  if (error instanceof ApiError && error.status === 410) {
    return (
      <div className="error-box" role="alert">
        <strong>This snapshot has expired.</strong>
        <span>Inspect at head to start a new, explicitly dated view.</span>
        <button type="button" onClick={onHead}>Inspect at head</button>
      </div>
    );
  }
  if (error instanceof ApiError && error.status === 503) {
    const message = error.reason === "busy"
      ? "Partition inspection is busy."
      : error.reason === "timeout"
        ? "Partition inspection timed out."
        : "Partition inspection is temporarily unavailable.";
    return (
      <div className="error-box" role="alert">
        <strong>{message}</strong>
        <span>{error.retryAfter && /^\d+$/.test(error.retryAfter)
          ? `Try again after ${error.retryAfter} seconds.`
          : "Try again shortly."}</span>
        <button type="button" onClick={onRetry}>Retry inspection</button>
      </div>
    );
  }
  return <><ErrorBox error={error} /><button type="button" onClick={onRetry}>Retry inspection</button></>;
}

export function PartitionLookupPanel({ catalog, namespace, table, defaultSpecId, renderFileStats }: {
  catalog: string;
  namespace: string;
  table: string;
  defaultSpecId?: Int64;
  renderFileStats: (fileId: Int64, snapshot: Int64) => ReactNode;
}) {
  const [params, setParams] = useSearchParams();
  const location = useLocation();
  const client = useQueryClient();
  const rawParams = params.toString();
  const parsed = useMemo(() => readPartitionLookupRequest(new URLSearchParams(rawParams)), [rawParams]);
  const request = parsed.request;
  const [spec, setSpec] = useState(params.get("lookup_spec") ?? defaultSpecId ?? "");
  const specEdited = useRef(false);
  const [tuple, setTuple] = useState(params.get("lookup_values") ?? "");
  const [snapshot, setSnapshot] = useState(params.get("snapshot") ?? "");
  const [inputError, setInputError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Int64 | null>(null);
  const scope = JSON.stringify([catalog, namespace, table, request]);
  const activeScope = useRef(scope);
  activeScope.current = scope;
  const urlSpec = params.get("lookup_spec");
  const urlTuple = params.get("lookup_values");
  const urlSnapshot = params.get("snapshot");
  const queryKey = (value: PartitionLookupRequest | null) =>
    ["partitionLookup", catalog, namespace, table, value?.specId, value?.values, value?.snapshot ?? "head", PAGE_SIZE] as const;

  const query = useInfiniteQuery({
    queryKey: queryKey(request),
    enabled: request !== null,
    queryFn: ({ pageParam }) => lookupPartition(catalog, namespace, table, {
      specId: request!.specId,
      values: request!.values,
      snapshot: pageParam.snapshot,
      cursor: pageParam.cursor,
      limit: PAGE_SIZE,
    }),
    initialPageParam: { snapshot: request?.snapshot } as Page,
    getNextPageParam: (last): Page | undefined => last.next_cursor === null
      ? undefined
      : { snapshot: last.snapshot_id, cursor: last.next_cursor },
    staleTime: request?.snapshot === undefined ? 0 : Infinity,
    refetchOnMount: request?.snapshot === undefined ? "always" : false,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    retry: false,
  });

  useEffect(() => {
    specEdited.current = false;
    setSpec(urlSpec ?? defaultSpecId ?? "");
    setInputError(null);
  }, [catalog, namespace, table, urlSpec]);
  useEffect(() => {
    setTuple(urlTuple ?? "");
    setInputError(null);
  }, [catalog, namespace, table, urlTuple]);
  useEffect(() => {
    setSnapshot(urlSnapshot ?? "");
    setInputError(null);
  }, [catalog, namespace, table, urlSnapshot]);
  useEffect(() => {
    if (urlSpec === null && !specEdited.current) setSpec((current) => current === "" ? defaultSpecId ?? "" : current);
  }, [defaultSpecId, urlSpec]);
  useEffect(() => {
    setInputError(null);
    setExpanded(null);
  }, [scope]);

  const first = request?.snapshot === undefined && (!query.isSuccess || query.isFetching)
    ? undefined
    : query.data?.pages[0];
  useEffect(() => {
    if (!request || request.snapshot !== undefined || !query.data || !first || activeScope.current !== scope) return;
    const pinned = { ...request, snapshot: first.snapshot_id };
    // Seed the pinned key before changing the URL; the initial head page is never re-read.
    client.setQueryData(queryKey(pinned), {
      ...query.data,
      pageParams: query.data.pageParams.map((page) => ({ ...(page as Page), snapshot: first.snapshot_id })),
    });
    setParams((previous) => JSON.stringify(readPartitionLookupRequest(previous).request) === JSON.stringify(request)
      && activeScope.current === scope ? partitionLookupParams(previous, pinned) : previous, { replace: true });
  }, [request, first, query.data, client, setParams]);

  const apply = (next: PartitionLookupRequest) => {
    setExpanded(null);
    setInputError(null);
    void client.resetQueries({ queryKey: queryKey(next), exact: true });
    setParams((previous) => partitionLookupParams(previous, next));
  };
  const atHead = () => {
    if (request) apply({ ...request, snapshot: undefined });
  };
  const files = query.data?.pages.flatMap((page) => page.files) ?? [];
  const totals = first?.aggregates.totals;
  const error = inputError ?? parsed.error;
  const pinnedSnapshot = first?.snapshot_id;

  return (
    <section className="partition-lookup" aria-label="Exact partition inspection">
      <h3>Exact partition inspection</h3>
      <p className="subtle">Enter the spec ID and the complete stored tuple. Strings, null, an empty string, and the string "null" remain distinct. No maintenance sample is required.</p>
      <form className="form-panel" onSubmit={(event) => {
        event.preventDefault();
        try {
          apply(parsePartitionLookupInput(spec, tuple, snapshot));
        } catch (cause) {
          setInputError(cause instanceof Error ? cause.message : String(cause));
        }
      }}>
        <div className="form-row">
          <label>Spec ID<input value={spec} onChange={(event) => { specEdited.current = true; setSpec(event.target.value); }} inputMode="numeric" /></label>
          <label>Complete tuple (JSON)<input className="mono" value={tuple} onChange={(event) => setTuple(event.target.value)} placeholder={'["2026-10", null]'} /></label>
          <label>Snapshot (blank for head)<input value={snapshot} onChange={(event) => setSnapshot(event.target.value)} inputMode="numeric" /></label>
          <button type="submit" disabled={query.isFetching}>Inspect partition</button>
        </div>
        {error && <p className="field-error" role="alert">{error}</p>}
      </form>
      {request && query.isError && <LookupError error={query.error} onHead={atHead} onRetry={() => {
        if (query.isFetchNextPageError) void query.fetchNextPage();
        else void query.refetch();
      }} />}
      {request && !first && !query.isError && <SkeletonBlock />}
      {request && first && pinnedSnapshot && (
        <>
          <p>Snapshot <SnapshotId catalog={catalog} id={pinnedSnapshot} /> · spec <span className="mono">{first.spec_id}</span> · tuple <code>{JSON.stringify(first.values)}</code>{" "}
            <CopyButton text={`${window.location.origin}${location.pathname}${location.search}`} label="partition link" />{" "}
            <button type="button" onClick={atHead} disabled={query.isFetching}>Inspect at head</button>
          </p>
          {first.aggregates.status === "exact" && totals ? (
            <dl className="stats-grid">
              <div><dt>Files</dt><dd>{formatCount(totals.file_count)}</dd></div>
              <div><dt>Physical rows</dt><dd>{formatCount(totals.physical_rows)}</dd></div>
              <div><dt>Bytes</dt><dd>{formatBytes(totals.total_bytes)}</dd></div>
              <div><dt>Files with DV metadata</dt><dd>{formatCount(totals.dv_count)}</dd></div>
              <div><dt>Stats provided / pending / failed</dt><dd>{formatCount(totals.stats_state.provided)} / {formatCount(totals.stats_state.pending)} / {formatCount(totals.stats_state.failed)}</dd></div>
              <div><dt>Last file write / rewrite snapshot</dt><dd>{totals.last_file_write_snapshot === null ? "—" : <SnapshotId catalog={catalog} id={totals.last_file_write_snapshot} />}</dd></div>
            </dl>
          ) : (
            <p className="subtle">Totals unavailable: {first.aggregates.reason === "overflow" ? "the totals exceed the supported numeric range" : "the candidate scan budget was reached"}. Paged file inspection remains available.</p>
          )}
          <p className="subtle">Physical rows are not DV-adjusted logical rows. File writes and rewrites do not measure ingestion freshness. Hydration states reflect this request.</p>
          <table className="data-table">
            <thead><tr><th className="num">File ID</th><th>Path</th><th className="num">Physical rows</th><th className="num">Bytes</th><th>Stats</th><th>Written / rewritten at</th><th>Inspect</th></tr></thead>
            <tbody>
              {files.length === 0 && <tr><td colSpan={7} className="empty">{query.hasNextPage ? "No matching files in this candidate window. Continue inspecting more files." : "No files in this partition at this snapshot."}</td></tr>}
              {files.map((file) => (
                <Fragment key={file.data_file_id}>
                  <tr>
                    <td className="num mono">{file.data_file_id}</td>
                    <td className="path-cell"><span className="mono path-text" title={file.path}>{file.path}</span><CopyButton text={file.path} label={`file ${file.data_file_id} path`} /></td>
                    <td className="num mono">{formatCount(file.record_count)}</td>
                    <td className="num mono">{formatBytes(file.file_size_bytes)}</td>
                    <td><StatsStateBadge state={file.stats_state} /></td>
                    <td><SnapshotId catalog={catalog} id={file.begin_snapshot} /></td>
                    <td><button type="button" onClick={() => setExpanded(expanded === file.data_file_id ? null : file.data_file_id)} aria-expanded={expanded === file.data_file_id}>{expanded === file.data_file_id ? "Close" : "Inspect"} file {file.data_file_id}</button></td>
                  </tr>
                  {expanded === file.data_file_id && <tr className="detail-row"><td colSpan={7}>{renderFileStats(file.data_file_id, pinnedSnapshot)}</td></tr>}
                </Fragment>
              ))}
            </tbody>
          </table>
          {query.hasNextPage && <button type="button" disabled={query.isFetchingNextPage} onClick={() => void query.fetchNextPage()}>{query.isFetchingNextPage ? "Loading…" : files.length === 0 ? "Continue inspection" : "Load more files"}</button>}
        </>
      )}
    </section>
  );
}
