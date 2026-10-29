import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import {
  pagedPartitionListingFixture,
  partitionListingFixture,
  unpartitionedPartitionListingFixture,
  filesFixture,
  scanFixture,
  tableFixture,
  unsampledPartitionListingFixture,
} from "./fixtures";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const base = "/v1/catalogs/analytics/namespaces/events/tables/pageviews";
const route = "/catalogs/analytics/namespaces/events/tables/pageviews";

function lookupResult(params: URLSearchParams) {
  return {
    snapshot_id: params.get("snapshot") ?? "4211", table_uuid: tableFixture.table_uuid,
    spec_id: params.get("spec_id"), values: JSON.parse(params.get("values")!),
    aggregates: { status: "exact", reason: null, totals: {
      file_count: "2", physical_rows: "980000", total_bytes: "520093696", dv_count: "0",
      stats_state: { provided: "1", pending: "1", failed: "0" }, last_file_write_snapshot: "4100",
    } }, files: filesFixture, next_cursor: null,
  };
}

/** Every request the page can make, plus the URLs it made, in order. */
function harness(listing = partitionListingFixture) {
  const urls: string[] = [];
  mockFetch((url) => {
    urls.push(url);
    const [path] = url.split("?");
    if (path === base) return jsonResponse(tableFixture);
    if (path === `${base}/files`) return jsonResponse(filesFixture);
    if (path === `${base}/scan`) return jsonResponse(scanFixture);
    if (path === `${base}/partitions`) return jsonResponse(listing);
    if (path === `${base}/partitions/lookup`) {
      const params = new URL(url, "http://localhost").searchParams;
      return jsonResponse(lookupResult(params));
    }
    return undefined;
  });
  return urls;
}

const partitionsUrls = (urls: string[]) =>
  urls.filter((u) => u.split("?")[0] === `${base}/partitions`);

describe("TablePage partitions tab", () => {
  it("renders the sampled partitions, the stale-spec badge and the unmeasured row", async () => {
    harness();
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));

    // Decoded values, labelled by field, in key order.
    expect(await screen.findByText("2026-09-01")).toBeInTheDocument();
    expect(screen.getByText("bucket 7/16")).toBeInTheDocument();
    expect(screen.getAllByText("ts_day").length).toBeGreaterThan(0);

    // A null partition value reads as "null", not as a missing cell.
    expect(screen.getByText("null")).toBeInTheDocument();

    // The group under spec 0 is badged; the two under spec 1 are not.
    expect(screen.getByText("spec 0")).toBeInTheDocument();
    expect(screen.queryByText("spec 1")).not.toBeInTheDocument();

    // record_count/last_written null is an em dash with a reason, never
    // a zero — "not measured by this sample" is a different answer.
    expect(screen.getAllByTitle("Not measured by this sample")).toHaveLength(2);

    // Measures, human formatted.
    expect(screen.getByText("1.1 MiB")).toBeInTheDocument();
    expect(screen.getByText("4,200")).toBeInTheDocument();
  });

  it("states what the numbers are: count, freshness, snapshot and stale groups", async () => {
    harness();
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));

    const footer = await screen.findByText(/partitions · sampled/);
    expect(footer).toHaveTextContent("3 partitions");
    expect(footer).toHaveTextContent("at snapshot 412");
    expect(footer).toHaveTextContent("1 under an older spec");
    // FRESHNESS IS THE SCAN'S START, not its publish. The fixture
    // published 90 s ago and started 31 min before that; "1min" here
    // would be the footer telling an operator the numbers are current
    // when they are half an hour old.
    // The fixture published 90 s ago after a ~31 min scan; the exact
    // minute depends on when the module was loaded, so the assertion
    // is the half-hour, and the negative is the point.
    expect(footer.textContent).toMatch(/sampled 3\dmin ago/);
    expect(footer.textContent).not.toContain("sampled 1min ago");
    expect(footer.getAttribute("title")).toContain("scan started");
    expect(footer.getAttribute("title")).toContain("published");
  });

  it("says so plainly when the catalog has no sample yet", async () => {
    harness(unsampledPartitionListingFixture);
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));

    expect(
      await screen.findByText(
        "No sample yet; the maintenance sampler has not published for this catalog.",
      ),
    ).toBeInTheDocument();
    // ...and the table says the same thing rather than looking empty.
    expect(screen.getByText("No sample yet.")).toBeInTheDocument();
  });

  it("sorts server-side: a header click changes the request, not the loaded page", async () => {
    const urls = harness();
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));
    await screen.findByText("2026-09-01");

    // The default: the decoded partition tuple, ascending.
    expect(partitionsUrls(urls)[0]).toContain("sort=partition");
    expect(partitionsUrls(urls)[0]).toContain("order=asc");

    // A measure column starts DESCENDING — "which partition has the
    // most debt" is why it was clicked.
    await user.click(screen.getByRole("button", { name: /debt/ }));
    await waitFor(() => expect(partitionsUrls(urls).length).toBe(2));
    expect(partitionsUrls(urls)[1]).toContain("sort=debt");
    expect(partitionsUrls(urls)[1]).toContain("order=desc");

    // Clicking it again flips it, still server-side.
    await user.click(screen.getByRole("button", { name: /debt/ }));
    await waitFor(() => expect(partitionsUrls(urls).length).toBe(3));
    expect(partitionsUrls(urls)[2]).toContain("order=asc");
  });

  it("debounces seven keystrokes into exactly one request of filter= params", async () => {
    const urls = harness();
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));
    await screen.findByText("2026-09-01");
    const before = partitionsUrls(urls).length;

    await user.type(screen.getByLabelText("filter by ts_day"), "2026-09");
    await waitFor(() =>
      expect(partitionsUrls(urls).length).toBeGreaterThan(before),
    );
    const after = partitionsUrls(urls);
    // EXACTLY one, not "fewer than seven": six requests would pass as
    // "debounced" while firing per keystroke minus one.
    expect(after.length - before).toBe(1);
    // The committed filter is a decoded PREFIX on key 0 — the server
    // decodes so that a month can match a day partition.
    expect(decodeURIComponent(after[after.length - 1])).toContain("filter=0:2026-09");
  });

  it("clearing a filter box means unfiltered, not the null value", async () => {
    // The regression this pins: an empty box used to commit `0:`, the
    // server's deliberate null-value selector, so backspacing a filter
    // clear emptied the table with no way back short of leaving the tab.
    const urls = harness();
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));
    await screen.findByText("2026-09-01");

    const box = screen.getByLabelText("filter by ts_day");
    await user.type(box, "2026-09");
    await waitFor(() =>
      expect(decodeURIComponent(partitionsUrls(urls).at(-1)!)).toContain("filter=0:2026-09"),
    );
    await user.clear(box);
    await waitFor(() =>
      expect(decodeURIComponent(partitionsUrls(urls).at(-1)!)).not.toContain("filter=0:2026-09"),
    );
    expect(partitionsUrls(urls).at(-1)).not.toContain("filter=");
    // ...and the rows are back.
    expect(await screen.findByText("2026-09-01")).toBeInTheDocument();
  });

  it("the is-null toggle is the only thing that sends an empty filter text", async () => {
    const urls = harness();
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));
    await screen.findByText("2026-09-01");

    await user.click(screen.getByLabelText("url_bucket is null"));
    await waitFor(() =>
      expect(decodeURIComponent(partitionsUrls(urls).at(-1)!)).toContain("filter=1:"),
    );
    // The prefix box for that key goes disabled: the two controls are
    // alternatives, not a combination.
    expect(screen.getByLabelText("filter by url_bucket")).toBeDisabled();
  });

  it("an error leaves the filter bar standing so a bad filter is recoverable", async () => {
    // A 422 from an out-of-range key_index is the recoverable error on
    // this tab, and it used to replace the very inputs that produced it.
    mockFetch((url) => {
      const [path] = url.split("?");
      if (path === base) return jsonResponse(tableFixture);
      if (path === `${base}/partitions`) {
        return url.includes("filter=")
          ? jsonResponse(
              { error: "validation", detail: "partition key_index 9 is out of range" },
              422,
            )
          : jsonResponse(partitionListingFixture);
      }
      return undefined;
    });
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));
    await screen.findByText("2026-09-01");

    await user.type(screen.getByLabelText("filter by ts_day"), "x");
    expect(await screen.findByText(/out of range/)).toBeInTheDocument();
    // The inputs are still there, and still hold what was typed.
    expect(screen.getByLabelText("filter by ts_day")).toHaveValue("x");
    expect(screen.getByLabelText("url_bucket is null")).toBeInTheDocument();
  });

  it("an unpartitioned table shows its single group and no filter inputs", async () => {
    harness(unpartitionedPartitionListingFixture);
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));

    expect(await screen.findByText("unpartitioned")).toBeInTheDocument();
    expect(screen.queryByLabelText(/^filter by /)).not.toBeInTheDocument();
    // No spec at head means nothing to be stale against, so no badge.
    expect(screen.queryByText(/^spec /)).not.toBeInTheDocument();
  });

  it("Load more appends the next page and total stays the server's", async () => {
    const urls = harness(pagedPartitionListingFixture(0));
    // Page 2 is served for any request carrying a non-zero offset.
    mockFetch((url) => {
      urls.push(url);
      const [path, query] = url.split("?");
      if (path === base) return jsonResponse(tableFixture);
      if (path === `${base}/partitions`) {
        const offset = Number(new URLSearchParams(query).get("offset") ?? 0);
        return jsonResponse(pagedPartitionListingFixture(offset));
      }
      return undefined;
    });
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));
    await screen.findByText("1970-01-01");
    expect(screen.queryByText("1970-01-03")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Load more" }));
    // Appended, not replaced.
    expect(await screen.findByText("1970-01-03")).toBeInTheDocument();
    expect(screen.getByText("1970-01-01")).toBeInTheDocument();
    // `total` is the whole match throughout, never the loaded count.
    expect(screen.getByText(/partitions · sampled/)).toHaveTextContent("4 partitions");
    // ...and the last page ends the walk.
    expect(screen.queryByRole("button", { name: "Load more" })).not.toBeInTheDocument();
  });

  it("the files tab preserves linked filters until explicitly cleared", async () => {
    const urls = harness();
    const user = userEvent.setup();
    renderApp(`${route}?tab=files&partition=0%3A20697&partition=1%3A7`);
    await waitFor(() =>
      expect(urls.filter((u) => u.split("?")[0] === `${base}/files`).length).toBeGreaterThan(0),
    );
    const first = urls.filter((u) => u.split("?")[0] === `${base}/files`).at(-1)!;
    expect(decodeURIComponent(first)).toContain("partition=0:20697");

    expect(await screen.findByLabelText("filter by day(ts)")).toHaveValue("20697");
    await user.click(screen.getByRole("tab", { name: "schema" }));
    await user.click(screen.getByRole("tab", { name: "files" }));
    expect(await screen.findByLabelText("filter by day(ts)")).toHaveValue("20697");

    await user.click(screen.getByRole("button", { name: "Clear partition filters" }));
    await waitFor(() => {
      const last = urls.filter((u) => u.split("?")[0] === `${base}/files`).at(-1)!;
      expect(decodeURIComponent(last)).not.toContain("partition=");
    });
    await user.click(screen.getByRole("tab", { name: "schema" }));
    await user.click(screen.getByRole("tab", { name: "files" }));
    expect(await screen.findByLabelText("filter by day(ts)")).toHaveValue("");
    const last = urls.filter((u) => u.split("?")[0] === `${base}/files`).at(-1)!;
    expect(decodeURIComponent(last)).not.toContain("partition=");
  });

  it("links a partition to exact inspection with its spec and complete stored tuple", async () => {
    harness();
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));

    const row = (await screen.findByText("2026-09-01")).closest("td")!;
    const link = within(row).getByRole("link");
    const href = new URL(link.getAttribute("href")!, "http://localhost");
    expect(href.searchParams.get("lookup")).toBe("1");
    expect(href.searchParams.get("lookup_spec")).toBe("1");
    expect(JSON.parse(href.searchParams.get("lookup_values")!)).toEqual(["20697", "7"]);
    const nullRow = (await screen.findByText("2026-09-02")).closest("td")!;
    const nullHref = new URL(within(nullRow).getByRole("link").getAttribute("href")!, "http://localhost");
    expect(JSON.parse(nullHref.searchParams.get("lookup_values")!)).toEqual(["20698", null]);
  });

  it("opens exact inspection from a sampled row without a broad files listing", async () => {
    const urls = harness();
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));
    const row = (await screen.findByText("2026-09-01")).closest("td")!;
    await user.click(within(row).getByRole("link"));

    await waitFor(() =>
      expect(
        urls.filter((u) => u.split("?")[0] === `${base}/partitions/lookup`).length,
      ).toBeGreaterThan(0),
    );
    const lookupUrl = new URL(urls.find((u) => u.split("?")[0] === `${base}/partitions/lookup`)!, "http://localhost");
    expect(lookupUrl.searchParams.get("spec_id")).toBe("1");
    expect(JSON.parse(lookupUrl.searchParams.get("values")!)).toEqual(["20697", "7"]);
    expect(urls.filter((u) => u.split("?")[0] === `${base}/files`)).toHaveLength(0);
    await screen.findByRole("button", { name: "Inspect file 101" });
    expect(urls).toContain(`${base}?totals=false`);
    expect(urls.some((u) => u.includes("snapshot=4211"))).toBe(false);
    await user.click(screen.getByRole("tab", { name: "files" }));
    await waitFor(() => expect(urls.some((u) => u.split("?")[0] === `${base}/files`)).toBe(true));
    const filesUrl = new URL(urls.find((u) => u.split("?")[0] === `${base}/files`)!, "http://localhost");
    expect(filesUrl.searchParams.has("snapshot")).toBe(false);
    await user.click(screen.getByRole("tab", { name: "schema" }));
    expect(urls.filter((u) => u.split("?")[0] === base).every((u) => !new URL(u, "http://localhost").searchParams.has("snapshot"))).toBe(true);
  });

  it("preserves page time travel when returning from a separately pinned lookup to sampled partitions", async () => {
    const urls = harness();
    const user = userEvent.setup();
    const params = new URLSearchParams({ tab: "partitions", lookup: "1", lookup_spec: "1", lookup_values: '["20697","7"]', snapshot: "7", lookup_snapshot: "4211", lookup_at: "head" });
    renderApp(`${route}?${params}`);
    await screen.findByRole("button", { name: "Inspect file 101" });
    const back = screen.getByRole("link", { name: "Back to sampled partitions" });
    expect(new URL(back.getAttribute("href")!, "http://localhost").searchParams.get("snapshot")).toBe("7");
    await user.click(back);
    await screen.findByText("2026-09-01");
    await user.click(screen.getByRole("tab", { name: "files" }));
    await waitFor(() => expect(urls.some((u) => u.split("?")[0] === `${base}/files`)).toBe(true));
    const filesUrl = new URL(urls.find((u) => u.split("?")[0] === `${base}/files`)!, "http://localhost");
    expect(filesUrl.searchParams.get("snapshot")).toBe("7");
  });

  it("opens a pinned lookup without table metadata and keeps historical inspector errors local", async () => {
    const snapshot = "9007199254740993";
    const urls: string[] = [];
    mockFetch((url) => {
      urls.push(url);
      const parsed = new URL(url, "http://localhost");
      if (parsed.pathname === base) return jsonResponse({ error: "not_found", detail: "No table metadata" }, 404);
      if (parsed.pathname === `${base}/partitions/lookup`) return jsonResponse(lookupResult(parsed.searchParams));
      if (parsed.pathname === `${base}/files/101/stats`) return jsonResponse({ error: "not_found", detail: "Historical column stats unavailable" }, 404);
      return undefined;
    });
    const params = new URLSearchParams({ tab: "partitions", lookup: "1", lookup_spec: "1", lookup_values: '[null,"","null"]', snapshot });
    const user = userEvent.setup();
    renderApp(`${route}?${params}`);

    await user.click(await screen.findByRole("button", { name: "Inspect file 101" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Historical column stats unavailable");
    expect(screen.getByText("Physical rows", { selector: "dt" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Inspect file 102" })).toBeInTheDocument();
    expect(screen.queryByText("No table metadata")).not.toBeInTheDocument();
    expect(urls).toContain(`${base}/files/101/stats?snapshot=${snapshot}`);
    expect(urls.filter((url) => url.split("?")[0] === base)).toEqual([`${base}?snapshot=${snapshot}&totals=false`]);
    expect(urls.some((url) => [`${base}/partitions`, `${base}/partitions/values`, `${base}/files`].includes(url.split("?")[0]))).toBe(false);
  });

  it("keeps exact inspection reachable after the sample listing exceeds its limit", async () => {
    const urls: string[] = [];
    mockFetch((url) => {
      urls.push(url);
      const parsed = new URL(url, "http://localhost");
      if (parsed.pathname === base) return jsonResponse(tableFixture);
      if (parsed.pathname === `${base}/partitions`) return jsonResponse({ error: "partition_limit", detail: "Sample listing limit exceeded" }, 422);
      if (parsed.pathname === `${base}/partitions/lookup`) return jsonResponse(lookupResult(parsed.searchParams));
      return undefined;
    });
    const user = userEvent.setup();
    renderApp(`${route}?tab=partitions`);
    expect(await screen.findByRole("alert")).toHaveTextContent("Sample listing limit exceeded");
    expect(screen.getByText(/Use exact inspection above with a spec ID/)).toBeInTheDocument();
    await user.click(screen.getByRole("link", { name: "Inspect exact partition" }));
    await user.clear(screen.getByLabelText("Spec ID"));
    await user.type(screen.getByLabelText("Spec ID"), "1");
    await user.click(screen.getByLabelText("Complete tuple (JSON)"));
    await user.paste('["20697",null]');
    await user.click(screen.getByRole("button", { name: "Inspect partition" }));
    await screen.findByRole("button", { name: "Inspect file 101" });
    const lookup = new URL(urls.find((url) => url.split("?")[0] === `${base}/partitions/lookup`)!, "http://localhost");
    expect(JSON.parse(lookup.searchParams.get("values")!)).toEqual(["20697", null]);
    expect(partitionsUrls(urls)).toHaveLength(1);
    expect(urls.some((url) => [`${base}/partitions/values`, `${base}/files`].includes(url.split("?")[0]))).toBe(false);
  });
});

// The partitions tab carries two snapshot ids: the partition's last
// writer, and the sample's own snapshot in the footer.
describe("TablePage partitions snapshot id tooltips", () => {
  const snapshotsPath = "/v1/catalogs/analytics/snapshots";

  function tooltipHarness() {
    const urls: string[] = [];
    mockFetch((url) => {
      urls.push(url);
      const [path] = url.split("?");
      if (path === base) return jsonResponse(tableFixture);
      if (path === `${base}/partitions`)
        return jsonResponse(partitionListingFixture);
      if (path === snapshotsPath)
        return jsonResponse({
          snapshots: [
            {
              snapshot_id: url.includes("after=409") ? "410" : "412",
              snapshot_time: url.includes("after=409")
                ? "2026-09-12T03:00:00Z"
                : "2026-09-12T04:00:00Z",
              schema_version: "7",
            },
          ],
          has_more: true,
        });
      return undefined;
    });
    return urls;
  }

  it("dates last_written_snapshot and the footer's sample snapshot", async () => {
    const urls = tooltipHarness();
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));

    const lastWritten = await screen.findByText("410");
    expect(lastWritten).toHaveAttribute("data-snapshot-id", "410");
    // Nothing dated until asked: a partitions page holds one id per row
    // plus the footer's, and none of them is probed.
    expect(urls.some((u) => u.split("?")[0] === snapshotsPath)).toBe(false);

    await user.hover(lastWritten);
    const rowInstant = await screen.findByText("2026-09-12 03:00:00Z");
    expect(rowInstant.closest('[role="tooltip"]')).not.toBeNull();
    await user.unhover(lastWritten);

    // The footer's "at snapshot N" is the same component, inside prose.
    const footer = screen.getByText(/partitions · sampled/);
    expect(footer).toHaveTextContent("at snapshot 412");
    const sampled = within(footer).getByText("412");
    expect(sampled).toHaveAttribute("data-snapshot-id", "412");
    await user.hover(sampled);
    const footerInstant = await screen.findByText("2026-09-12 04:00:00Z");
    expect(footerInstant.closest('[role="tooltip"]')).not.toBeNull();
  });
});
