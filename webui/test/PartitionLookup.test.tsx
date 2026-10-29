import { useState } from "react";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider, focusManager, onlineManager } from "@tanstack/react-query";
import { MemoryRouter, useLocation, useNavigate } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { PartitionLookupPanel } from "../src/components/PartitionLookupPanel";
import * as CopyButtonModule from "../src/components/CopyButton";
import type { PartitionLookup } from "../src/api/types";
import { parsePartitionLookupInput, readPartitionLookupRequest } from "../src/lib/partitionLookup";
import { filesFixture } from "./fixtures";
import { jsonResponse } from "./helpers";

const base = "/v1/catalogs/analytics/namespaces/events/tables/pageviews/partitions/lookup";
const route = "/catalogs/analytics/namespaces/events/tables/pageviews";
const tuple = [null, "", "null"];
const big = "9007199254740993";

afterEach(() => vi.restoreAllMocks());

function link(values = tuple, snapshot?: string, spec = "1"): string {
  const params = new URLSearchParams({ tab: "partitions", lookup: "1", lookup_spec: spec, lookup_values: JSON.stringify(values) });
  if (snapshot !== undefined) params.set("snapshot", snapshot);
  return `${route}?${params}`;
}

function page(overrides: Partial<PartitionLookup> = {}): PartitionLookup {
  return {
    snapshot_id: big,
    table_uuid: "table-one",
    spec_id: "1",
    values: tuple,
    aggregates: {
      status: "exact",
      reason: null,
      totals: {
        file_count: "1", physical_rows: big, total_bytes: "100", dv_count: "1",
        stats_state: { provided: "0", pending: "1", failed: "0" }, last_file_write_snapshot: "12",
      },
    },
    files: [{ ...filesFixture[0], data_file_id: "71", record_count: big, path: "s3://bucket/original.parquet" }],
    next_cursor: null,
    ...overrides,
  };
}

function setup(initial = `${route}?tab=partitions&lookup=1`, client = new QueryClient(), nextLookup?: string) {
  function Harness() {
    const location = useLocation();
    const navigate = useNavigate();
    const [defaultSpec, setDefaultSpec] = useState<string | undefined>(undefined);
    return <>
      <output data-testid="location">{location.search}</output>
      <button onClick={() => setDefaultSpec("99")}>Metadata arrives</button>
      <button onClick={() => navigate(link(["different"], "20", "2"))}>Different lookup</button>
      <button onClick={() => navigate(link())}>Unpinned lookup</button>
      {nextLookup && <button onClick={() => navigate(nextLookup)}>Next lookup</button>}
      <PartitionLookupPanel catalog="analytics" namespace="events" table="pageviews" defaultSpecId={defaultSpec}
        renderFileStats={(id, snapshot) => <p>Inspector {id} at {snapshot}</p>} />
    </>;
  }
  const view = render(<QueryClientProvider client={client}><MemoryRouter initialEntries={[initial]}><Harness /></MemoryRouter></QueryClientProvider>);
  return { ...view, client, user: userEvent.setup() };
}

function fetches(handler: (params: URLSearchParams, call: number) => Response | Promise<Response>) {
  const urls: string[] = [];
  vi.stubGlobal("fetch", vi.fn(async (input: string) => {
    expect(input.split("?")[0]).toBe(base);
    urls.push(input);
    return handler(new URL(input, "http://localhost").searchParams, urls.length);
  }));
  return urls;
}

describe("exact partition lookup input", () => {
  it("preserves raw nullable components and int64 IDs", () => {
    expect(parsePartitionLookupInput(big, '[null,"","null","東京🚀","  a:b  "]', big)).toEqual({
      specId: big, values: [null, "", "null", "東京🚀", "  a:b  "], snapshot: big,
    });
  });

  it.each(["0", "-1", "1.2", "9223372036854775808", "1e3"])("rejects invalid spec ID %s", (spec) => {
    expect(() => parsePartitionLookupInput(spec, '["x"]', "")).toThrow(/Spec ID/);
  });

  it.each(["[]", '[1]', '["x"] true', '"x"', '["\\u0000"]', '["\\ud800"]', '["\\udc00"]', JSON.stringify(["x".repeat(8193)])])("rejects invalid tuple %s", (value) => {
    expect(() => parsePartitionLookupInput("1", value, "")).toThrow();
  });

  it("rejects malformed, duplicate or incomplete URL parameters without falling back to head", () => {
    for (const extra of ["snapshot=", "snapshot=-1", "snapshot=9223372036854775808", "snapshot=1&snapshot=2", "lookup_spec=2", "lookup_values=[]"]) {
      const params = new URLSearchParams({ lookup_spec: "1", lookup_values: '["x"]' });
      for (const [key, value] of new URLSearchParams(extra)) params.append(key, value);
      expect(readPartitionLookupRequest(params)).toMatchObject({ request: null, error: expect.any(String) });
    }
    expect(readPartitionLookupRequest(new URLSearchParams("lookup_spec=1"))).toMatchObject({ request: null, error: expect.any(String) });
  });
});

describe("PartitionLookupPanel", () => {
  it("submits only on request, pins head once, preserves raw values and inspector snapshot", async () => {
    const copyButtons = vi.spyOn(CopyButtonModule, "CopyButton");
    const urls = fetches(() => jsonResponse(page({ spec_id: big })));
    const { user } = setup();
    await user.type(screen.getByLabelText("Spec ID"), big);
    await user.click(screen.getByLabelText("Complete tuple (JSON)"));
    await user.paste(JSON.stringify(tuple));
    expect(urls).toHaveLength(0);
    await user.click(screen.getByRole("button", { name: "Inspect partition" }));
    await screen.findByText("s3://bucket/original.parquet");
    const partitionLinks = copyButtons.mock.calls.filter(([props]) => props.label === "partition link");
    expect(partitionLinks.length).toBeGreaterThan(0);
    for (const [props] of partitionLinks) {
      expect(new URL(props.text).searchParams.get("snapshot")).toBe(big);
    }
    expect(screen.getByTestId("location")).toHaveTextContent(`snapshot=${big}`);
    expect(urls).toHaveLength(1);
    const request = new URL(urls[0], "http://localhost").searchParams;
    expect(request.get("spec_id")).toBe(big);
    expect(JSON.parse(request.get("values")!)).toEqual(tuple);
    expect(request.has("snapshot")).toBe(false);
    expect(screen.getAllByText("9,007,199,254,740,993").length).toBeGreaterThan(0);
    await user.click(screen.getByRole("button", { name: "Inspect file 71" }));
    expect(screen.getByText(`Inspector 71 at ${big}`)).toBeInTheDocument();
    await act(async () => { focusManager.setFocused(false); focusManager.setFocused(true); onlineManager.setOnline(false); onlineManager.setOnline(true); });
    expect(urls).toHaveLength(1);
  });

  it("opens a pinned shareable lookup without a maintenance or metadata request", async () => {
    const urls = fetches((params) => {
      expect(params.get("snapshot")).toBe(big);
      return jsonResponse(page());
    });
    setup(link(tuple, big));
    await screen.findByText("s3://bucket/original.parquet");
    expect(urls).toHaveLength(1);
    expect(screen.getByRole("button", { name: "Copy partition link" })).toBeInTheDocument();
  });

  it("keeps dirty drafts when deferred metadata arrives", async () => {
    const urls = fetches(() => jsonResponse(page()));
    const { user } = setup();
    await user.type(screen.getByLabelText("Spec ID"), "12");
    await user.click(screen.getByLabelText("Complete tuple (JSON)"));
    await user.paste('["work in progress"]');
    await user.type(screen.getByLabelText("Snapshot (blank for head)"), "30");
    await user.click(screen.getByRole("button", { name: "Metadata arrives" }));
    expect(screen.getByLabelText("Spec ID")).toHaveValue("12");
    expect(screen.getByLabelText("Complete tuple (JSON)")).toHaveValue('["work in progress"]');
    expect(screen.getByLabelText("Snapshot (blank for head)")).toHaveValue("30");
    expect(urls).toHaveLength(0);
  });

  it("preserves an explicitly cleared spec when deferred metadata arrives", async () => {
    const urls = fetches(() => jsonResponse(page()));
    const { user } = setup();
    await user.type(screen.getByLabelText("Spec ID"), "12");
    await user.clear(screen.getByLabelText("Spec ID"));
    await user.click(screen.getByLabelText("Complete tuple (JSON)"));
    await user.paste('["x"]');
    await user.click(screen.getByRole("button", { name: "Metadata arrives" }));
    expect(screen.getByLabelText("Spec ID")).toHaveValue("");
    await user.click(screen.getByRole("button", { name: "Inspect partition" }));
    expect(screen.getByRole("alert")).toHaveTextContent("Spec ID must be a positive int64.");
    expect(urls).toHaveLength(0);
  });

  it("continues sparse pages and leaves unavailable totals explicit", async () => {
    const urls = fetches((params, call) => {
      if (call === 1) return jsonResponse(page({ files: [], aggregates: { status: "unavailable", reason: "candidate_scan_limit", totals: null }, next_cursor: "opaque-next" }));
      expect(params.get("snapshot")).toBe(big);
      expect(params.get("cursor")).toBe("opaque-next");
      return jsonResponse(page({ aggregates: { status: "unavailable", reason: "candidate_scan_limit", totals: null } }));
    });
    const { user } = setup(link());
    await user.click(await screen.findByRole("button", { name: "Continue inspection" }));
    await screen.findByText("s3://bucket/original.parquet");
    expect(screen.getByText(/Totals unavailable:/)).toBeInTheDocument();
    expect(screen.queryByText("No files in this partition at this snapshot.")).not.toBeInTheDocument();
    expect(urls).toHaveLength(2);
  });

  it("retains earlier files on a next-page timeout and retries the same pinned cursor", async () => {
    const urls = fetches((params, call) => {
      if (call === 1) return jsonResponse(page({ next_cursor: "next-1" }));
      expect(params.get("snapshot")).toBe(big);
      expect(params.get("cursor")).toBe("next-1");
      if (call === 2) return new Response(JSON.stringify({ error: "partition_lookup_unavailable", reason: "timeout" }), { status: 503, headers: { "Content-Type": "application/json", "Retry-After": "1" } });
      return jsonResponse(page({ files: [{ ...filesFixture[0], data_file_id: "72", path: "s3://bucket/second.parquet" }] }));
    });
    const { user } = setup(link());
    await user.click(await screen.findByRole("button", { name: "Load more files" }));
    await screen.findByText("Partition inspection timed out.");
    expect(screen.getByText("s3://bucket/original.parquet")).toBeInTheDocument();
    expect(screen.getByText("Try again after 1 seconds.")).toBeInTheDocument();
    expect(urls).toHaveLength(2);
    await user.click(screen.getByRole("button", { name: "Retry inspection" }));
    await screen.findByText("s3://bucket/second.parquet");
    expect(screen.getByText("s3://bucket/original.parquet")).toBeInTheDocument();
    expect(urls).toHaveLength(3);
  });

  it("distinguishes busy and empty results, with no automatic 503 retry", async () => {
    const urls = fetches((_params, call) => call === 1
      ? jsonResponse({ error: "partition_lookup_unavailable", reason: "busy" }, 503)
      : jsonResponse(page({ files: [], aggregates: { status: "exact", reason: null, totals: { file_count: "0", physical_rows: "0", total_bytes: "0", dv_count: "0", stats_state: { provided: "0", pending: "0", failed: "0" }, last_file_write_snapshot: null } } })));
    const { user } = setup(link(tuple, big));
    await screen.findByText("Partition inspection is busy.");
    expect(urls).toHaveLength(1);
    expect(screen.queryByText("No files in this partition at this snapshot.")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Retry inspection" }));
    await screen.findByText("No files in this partition at this snapshot.");
    expect(urls).toHaveLength(2);
  });

  it("requires explicit reload at head after expiry", async () => {
    const urls = fetches((params, call) => {
      if (call === 1) return jsonResponse({ error: "expired", detail: "snapshot expired" }, 410);
      expect(params.has("snapshot")).toBe(false);
      return jsonResponse(page());
    });
    const { user } = setup(link(tuple, "2"));
    await screen.findByText("This snapshot has expired.");
    expect(urls).toHaveLength(1);
    await user.click(screen.getByRole("button", { name: "Inspect at head" }));
    await screen.findByText("s3://bucket/original.parquet");
    await waitFor(() => expect(screen.getByTestId("location")).toHaveTextContent(`snapshot=${big}`));
    expect(urls).toHaveLength(2);
  });

  it("does not let an old delayed head response overwrite newer navigation", async () => {
    let resolve!: (response: Response) => void;
    const delayed = new Promise<Response>((done) => { resolve = done; });
    const urls = fetches((_params, call) => call === 1 ? delayed : jsonResponse(page({ snapshot_id: "20", spec_id: "2", values: ["different"], files: [{ ...filesFixture[0], data_file_id: "80", path: "s3://bucket/new.parquet" }] })));
    const { user } = setup(link());
    await waitFor(() => expect(urls).toHaveLength(1));
    await user.click(screen.getByRole("button", { name: "Different lookup" }));
    await screen.findByText("s3://bucket/new.parquet");
    await act(async () => { resolve(jsonResponse(page())); await delayed; });
    expect(screen.getByTestId("location")).toHaveTextContent("snapshot=20");
    expect(screen.getByTestId("location")).toHaveTextContent("lookup_spec=2");
    expect(screen.queryByText("s3://bucket/original.parquet")).not.toBeInTheDocument();
    expect(urls).toHaveLength(2);
  });

  it("revisiting an unpinned link ignores its cached old head response", async () => {
    let resolve!: (response: Response) => void;
    const delayed = new Promise<Response>((done) => { resolve = done; });
    const urls = fetches((_params, call) => call === 1 ? jsonResponse(page()) : delayed);
    const { user } = setup(link());
    await screen.findByText("s3://bucket/original.parquet");
    await waitFor(() => expect(screen.getByTestId("location")).toHaveTextContent(`snapshot=${big}`));
    await user.click(screen.getByRole("button", { name: "Unpinned lookup" }));
    await waitFor(() => expect(urls).toHaveLength(2));
    expect(screen.getByTestId("location")).not.toHaveTextContent("snapshot=");
    expect(screen.queryByText("s3://bucket/original.parquet")).not.toBeInTheDocument();
    await act(async () => { resolve(jsonResponse(page({ snapshot_id: "9007199254740995", files: [] }))); await delayed; });
    await waitFor(() => expect(screen.getByTestId("location")).toHaveTextContent("snapshot=9007199254740995"));
    expect(urls).toHaveLength(2);
  });

  it("explicitly refreshes at head and pins the new response without retaining old files", async () => {
    let resolve!: (response: Response) => void;
    const delayed = new Promise<Response>((done) => { resolve = done; });
    const urls = fetches((params, call) => {
      expect(params.has("snapshot")).toBe(false);
      expect(params.has("cursor")).toBe(false);
      return call === 1 ? jsonResponse(page()) : delayed;
    });
    const { user } = setup(link());
    await screen.findByText("s3://bucket/original.parquet");
    await waitFor(() => expect(screen.getByTestId("location")).toHaveTextContent(`snapshot=${big}`));
    await user.click(screen.getByRole("button", { name: "Inspect at head" }));
    await waitFor(() => expect(urls).toHaveLength(2));
    expect(screen.getByTestId("location")).not.toHaveTextContent("snapshot=");
    expect(screen.queryByText("s3://bucket/original.parquet")).not.toBeInTheDocument();
    await act(async () => { resolve(jsonResponse(page({ snapshot_id: "9007199254740995", files: [] }))); await delayed; });
    await screen.findByText("No files in this partition at this snapshot.");
    await waitFor(() => expect(screen.getByTestId("location")).toHaveTextContent("snapshot=9007199254740995"));
    expect(urls).toHaveLength(2);
  });

  it("does not repin a cached head result when refreshing it fails", async () => {
    const urls = fetches((params, call) => {
      expect(params.has("snapshot")).toBe(false);
      if (call === 2) return jsonResponse({ error: "partition_lookup_unavailable", reason: "timeout" }, 503);
      return jsonResponse(page(call === 1 ? {} : { snapshot_id: "9007199254740995", files: [] }));
    });
    const { user } = setup(link());
    await screen.findByText("s3://bucket/original.parquet");
    await waitFor(() => expect(screen.getByTestId("location")).toHaveTextContent(`snapshot=${big}`));
    await user.click(screen.getByRole("button", { name: "Unpinned lookup" }));
    await screen.findByText("Partition inspection timed out.");
    expect(screen.getByTestId("location")).not.toHaveTextContent("snapshot=");
    expect(screen.queryByText("s3://bucket/original.parquet")).not.toBeInTheDocument();
    expect(urls).toHaveLength(2);
    await user.click(screen.getByRole("button", { name: "Retry inspection" }));
    await screen.findByText("No files in this partition at this snapshot.");
    await waitFor(() => expect(screen.getByTestId("location")).toHaveTextContent("snapshot=9007199254740995"));
    expect(urls).toHaveLength(3);
  });

  it("resets the inspector and cursor when changing the lookup identity", async () => {
    const urls = fetches((params) => jsonResponse(page({ snapshot_id: params.get("snapshot") ?? big, spec_id: params.get("spec_id")!, values: JSON.parse(params.get("values")!) })));
    const { user } = setup(link(tuple, big));
    await user.click(await screen.findByRole("button", { name: "Inspect file 71" }));
    expect(screen.getByText(`Inspector 71 at ${big}`)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Different lookup" }));
    await waitFor(() => expect(screen.getByLabelText("Spec ID")).toHaveValue("2"));
    expect(screen.queryByText(`Inspector 71 at ${big}`)).not.toBeInTheDocument();
    expect(new URL(urls.at(-1)!, "http://localhost").searchParams.has("cursor")).toBe(false);
  });

  it.each(["spec", "tuple", "snapshot"] as const)("isolates cached results when only %s changes", async (dimension) => {
    const nextSpec = dimension === "spec" ? "2" : "1";
    const nextTuple = dimension === "tuple" ? ["different"] : tuple;
    const nextSnapshot = dimension === "snapshot" ? "9007199254740995" : big;
    const urls = fetches((params, call) => jsonResponse(page(call === 1 ? { next_cursor: "old-next" } : {
      spec_id: params.get("spec_id")!,
      values: JSON.parse(params.get("values")!),
      snapshot_id: params.get("snapshot")!,
      files: [{ ...filesFixture[0], data_file_id: "72", path: "s3://bucket/changed.parquet" }],
    })));
    const { user } = setup(link(tuple, big), new QueryClient(), link(nextTuple, nextSnapshot, nextSpec));
    await user.click(await screen.findByRole("button", { name: "Inspect file 71" }));
    expect(screen.getByText(`Inspector 71 at ${big}`)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Load more files" })).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Next lookup" }));

    await screen.findByText("s3://bucket/changed.parquet");
    expect(screen.queryByText("s3://bucket/original.parquet")).not.toBeInTheDocument();
    expect(screen.queryByText(`Inspector 71 at ${big}`)).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Load more files" })).not.toBeInTheDocument();
    expect(urls).toHaveLength(2);
    const request = new URL(urls[1], "http://localhost").searchParams;
    expect(request.get("spec_id")).toBe(nextSpec);
    expect(JSON.parse(request.get("values")!)).toEqual(nextTuple);
    expect(request.get("snapshot")).toBe(nextSnapshot);
    expect(request.has("cursor")).toBe(false);
  });

  it("rejects an invalid snapshot URL instead of requesting head", () => {
    const urls = fetches(() => jsonResponse(page()));
    setup(link() + "&snapshot=-1");
    expect(screen.getByRole("alert")).toHaveTextContent("Snapshot must be a non-negative int64");
    expect(urls).toHaveLength(0);
  });
});
