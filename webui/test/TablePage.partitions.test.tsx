import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import {
  pagedPartitionListingFixture,
  partitionListingFixture,
  partitionValuesFixture,
  unpartitionedPartitionListingFixture,
  filesFixture,
  scanFixture,
  tableFixture,
  unsampledPartitionListingFixture,
} from "./fixtures";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const base = "/v1/catalogs/analytics/namespaces/events/tables/pageviews";
const route = "/catalogs/analytics/namespaces/events/tables/pageviews";

/** Every request the page can make, plus the URLs it made, in order. */
function harness(listing = partitionListingFixture) {
  const urls: string[] = [];
  mockFetch((url) => {
    urls.push(url);
    const [path] = url.split("?");
    if (path === base) return jsonResponse(tableFixture);
    if (path === `${base}/files`) return jsonResponse(filesFixture);
    if (path === `${base}/scan`) return jsonResponse(scanFixture);
    if (path === `${base}/partitions/values`) return jsonResponse(partitionValuesFixture);
    if (path === `${base}/partitions`) return jsonResponse(listing);
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

  it("the files tab seeds from the link once, and the URL keeps the params", async () => {
    const urls = harness();
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));
    const row = (await screen.findByText("2026-09-01")).closest("td")!;
    await user.click(within(row).getByRole("link"));
    await waitFor(() =>
      expect(urls.filter((u) => u.split("?")[0] === `${base}/files`).length).toBeGreaterThan(0),
    );
    const first = urls.filter((u) => u.split("?")[0] === `${base}/files`).at(-1)!;
    expect(decodeURIComponent(first)).toContain("partition=0:20697");

    // Leaving and coming back must NOT re-seed from the URL, even
    // though the params are still on it — the page remembers that they
    // have been spent, so the view stays addressable while a filter the
    // user has since changed cannot be resurrected. (MemoryRouter, so
    // `window.location` is not the router's location and asserting on
    // it would pass whatever the code did.)
    await user.click(screen.getByRole("tab", { name: "schema" }));
    await user.click(screen.getByRole("tab", { name: "files" }));
    await waitFor(() =>
      expect(urls.filter((u) => u.split("?")[0] === `${base}/files`).length).toBeGreaterThan(1),
    );
    const second = urls.filter((u) => u.split("?")[0] === `${base}/files`).at(-1)!;
    expect(decodeURIComponent(second)).not.toContain("partition=");
  });

  it("links a partition to its own files, carrying the STORED values", async () => {
    harness();
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));

    const row = (await screen.findByText("2026-09-01")).closest("td")!;
    const link = within(row).getByRole("link");
    // 20697 and 7, not "2026-09-01" and "bucket 7/16": the files
    // listing matches the stored value exactly.
    expect(link).toHaveAttribute(
      "href",
      expect.stringContaining("partition=0%3A20697"),
    );
    expect(link.getAttribute("href")).toContain("partition=1%3A7");
    expect(link.getAttribute("href")).toContain("tab=files");

    // A null value contributes NO param: `partition=1:` would mean the
    // empty string to the files listing, which is a different filter.
    const nullRow = (await screen.findByText("2026-09-02")).closest("td")!;
    expect(within(nullRow).getByRole("link").getAttribute("href")).not.toContain(
      "partition=1",
    );
  });

  it("the files tab picks the linked partition up from the URL", async () => {
    const urls = harness();
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "partitions" }));
    const row = (await screen.findByText("2026-09-01")).closest("td")!;
    await user.click(within(row).getByRole("link"));

    await waitFor(() =>
      expect(
        urls.filter((u) => u.split("?")[0] === `${base}/files`).length,
      ).toBeGreaterThan(0),
    );
    const filesUrl = urls.filter((u) => u.split("?")[0] === `${base}/files`).at(-1)!;
    expect(decodeURIComponent(filesUrl)).toContain("partition=0:20697");
    expect(decodeURIComponent(filesUrl)).toContain("partition=1:7");
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
      if (path === `${base}/partitions/values`)
        return jsonResponse(partitionValuesFixture);
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
