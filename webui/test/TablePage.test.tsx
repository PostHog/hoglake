import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import {
  filesFixture,
  notFoundError,
  scanFixture,
  sortedFilesFixture,
  sortedTableFixture,
  tableFixture,
} from "./fixtures";
import { bigIntFilesWireBody } from "./fixtures.adversarial";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const base = "/v1/catalogs/analytics/namespaces/events/tables/pageviews";
const route = "/catalogs/analytics/namespaces/events/tables/pageviews";

function happyHandler(url: string): Response | undefined {
  const [path] = url.split("?");
  if (path === base) return jsonResponse(tableFixture);
  if (path === `${base}/files`) return jsonResponse(filesFixture);
  if (path === `${base}/scan`) return jsonResponse(scanFixture);
  return undefined;
}

describe("TablePage", () => {
  it("renders the stats header and schema tab with partition spec", async () => {
    mockFetch(happyHandler);
    renderApp(route);

    // Stats header, human formatted.
    expect(await screen.findByText("1,234,567")).toBeInTheDocument();
    expect(screen.getByText("42")).toBeInTheDocument();
    expect(screen.getByText("5.0 GiB")).toBeInTheDocument();
    expect(
      screen.getByText("3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71"),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /copy/i })).toBeInTheDocument();

    // Schema columns.
    expect(screen.getByText("ts")).toBeInTheDocument();
    expect(screen.getByText("timestamptz")).toBeInTheDocument();
    expect(screen.getByText("user_id")).toBeInTheDocument();
    // Nullability renders as marks (✗ not null / ✓ nullable); the words
    // live on the cell tooltip.
    expect(screen.getAllByTitle("not null")).toHaveLength(2);
    for (const cell of screen.getAllByTitle("not null")) {
      expect(cell).toHaveTextContent("✗");
    }

    // Partition spec rendering, source field resolved to column names.
    expect(screen.getByText("day(ts)")).toBeInTheDocument();
    expect(screen.getByText("bucket(16, url)")).toBeInTheDocument();
  });

  it("shows data files with a stats marker per state on the Files tab", async () => {
    mockFetch(happyHandler);
    renderApp(route);
    const user = userEvent.setup();

    await user.click(await screen.findByRole("tab", { name: "files" }));

    expect(await screen.findByText("101")).toBeInTheDocument();
    expect(
      screen.getByText("s3://hog-lake/analytics/events/pageviews/data-00101.parquet"),
    ).toBeInTheDocument();
    expect(screen.getByText("500,000")).toBeInTheDocument();

    // The stats state is the SHAPE now, not a pill spending a column's
    // width on the word "provided": a file with statistics gets a
    // triangle that opens them, one without gets a circle that does not
    // pretend to be clickable.
    const withStats = screen.getByRole("button", {
      name: "toggle stats for file 101",
    });
    expect(withStats).toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /toggle stats for file 102/ }),
    ).not.toBeInTheDocument();

    // Pending and failed both get a circle, in their own colours —
    // failed is not deferred, and must not read as if it were.
    const pendingMark = screen.getByLabelText(
      "pending: no column statistics for file 102",
    );
    const failedMark = screen.getByLabelText(
      "failed: no column statistics for file 103",
    );
    expect(pendingMark).toHaveClass("stats-pending");
    expect(failedMark).toHaveClass("stats-failed");
    expect(pendingMark.tagName).not.toBe("BUTTON");
    expect(failedMark.tagName).not.toBe("BUTTON");

    // The word the pill used to carry is gone from the table.
    expect(screen.queryByText("provided")).not.toBeInTheDocument();

    // partition_values render when present, em-dash when absent.
    // Decoded against the spec rather than shown as a raw tuple: the day
    // ordinal 20697 is a date, and 7 is a bucket index, not a value.
    expect(screen.getByText("2026-09-01")).toBeInTheDocument();
    expect(screen.getByText("bucket 7/16")).toBeInTheDocument();
    expect(screen.getByText("2026-09-02")).toBeInTheDocument();
    // A null element stays visible rather than collapsing the tuple.
    expect(screen.getByText("null")).toBeInTheDocument();
    // Field names come from the spec, carrying the transform: "ts_day",
    // not a bare "ts" that would suggest the column holds a date.
    expect(screen.getAllByText("ts_day").length).toBeGreaterThan(0);
    // The WHOLE cell, separator included. Asserting the parts
    // individually is what let a missing separator ship: every value was
    // present and the rendering still read "team_id=42ts_day=…".
    const cell = screen.getAllByText("ts_day")[0].closest("td");
    expect(cell?.textContent).toBe("ts_day=2026-09-01 / url_bucket=bucket 7/16");
  });

  /** The cells of the file row whose id cell reads [id]. */
  function fileRowCells(id: string): string[] {
    const row = screen
      .getAllByRole("row")
      .find((r) => r.querySelector("td:first-child")?.textContent === id);
    if (!row) throw new Error(`no file row ${id}`);
    return [...row.querySelectorAll("td")].map((c) => c.textContent ?? "");
  }

  it("bounds an unsorted table's files by their row-id span", async () => {
    mockFetch(happyHandler);
    renderApp(route);
    const user = userEvent.setup();

    await user.click(await screen.findByRole("tab", { name: "files" }));
    await screen.findByText("101");

    // No sort spec means the row id IS the ordering key, and the
    // headers say so by naming it. row_id_start alone is gone: a start
    // with no end never said which files overlap.
    // The key-bound columns are shown but not sortable (decoded from
    // encoded bytes, no SQL column to sort by), so plain headers.
    expect(
      screen.getByRole("columnheader", { name: /_hog_row_id min/ }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("columnheader", { name: /_hog_row_id max/ }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("columnheader", { name: /row_id_start/ }),
    ).not.toBeInTheDocument();

    // Both ends of every file's span, the pending and failed files
    // included: row ids need no statistics.
    expect(fileRowCells("101")).toContain("0");
    expect(fileRowCells("101")).toContain("499999");
    expect(fileRowCells("102")).toContain("979999");
    expect(fileRowCells("103")).toContain("1234566");
  });

  it("bounds a sorted table's files by its leading sort field", async () => {
    mockFetch((url) => {
      const [path] = url.split("?");
      if (path === base) return jsonResponse(sortedTableFixture);
      if (path === `${base}/files`) return jsonResponse(sortedFilesFixture);
      return undefined;
    });
    renderApp(route);
    const user = userEvent.setup();

    await user.click(await screen.findByRole("tab", { name: "files" }));
    await screen.findByText("201");

    // The LEADING sort field names both columns — user_id, not the `ts`
    // tiebreaker and not the row id, which a sorted rewrite remaps and
    // which therefore describes nothing here.
    expect(
      screen.getByRole("columnheader", { name: /user_id min/ }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("columnheader", { name: /user_id max/ }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("columnheader", { name: /_hog_row_id/ }),
    ).not.toBeInTheDocument();

    expect(fileRowCells("201")).toContain("1000");
    expect(fileRowCells("201")).toContain("4999");

    // A stored NULL bound is an answer — the column is all-null, so
    // nothing can be pruned on it — and it is labelled as one.
    expect(fileRowCells("202")).toContain("null");
    expect(
      screen.getByTitle("no bound stored — do not prune"),
    ).toBeInTheDocument();

    // A file whose statistics are pending has no bounds AT ALL, which is
    // a different fact from a null bound: em dashes, not zeros, and not
    // the row ids either.
    expect(fileRowCells("203")).toContain("—");
    expect(fileRowCells("203")).not.toContain("980000");
  });

  it("pairs data files with deletion vectors on the Scan tab", async () => {
    mockFetch(happyHandler);
    renderApp(route);
    const user = userEvent.setup();

    await user.click(await screen.findByRole("tab", { name: "scan" }));

    expect(
      await screen.findByText("s3://hog-lake/analytics/events/pageviews/dv-00900.puffin"),
    ).toBeInTheDocument();
    expect(screen.getByText("1,250")).toBeInTheDocument();
    // The file without a DV shows "none".
    expect(screen.getByText("none")).toBeInTheDocument();
  });

  it("drives ?snapshot through the selector (time travel)", async () => {
    const fetchMock = mockFetch(happyHandler);
    renderApp(route);
    const user = userEvent.setup();

    await screen.findByText("1,234,567");
    await user.type(screen.getByLabelText("snapshot id"), "4100");
    await user.click(screen.getByRole("button", { name: "Go" }));

    expect(await screen.findByText("@ snapshot 4100")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith(
      `${base}?snapshot=4100`,
      expect.anything(),
    );

    // Files tab inherits the snapshot.
    await user.click(screen.getByRole("tab", { name: "files" }));
    await screen.findByText("500,000");
    // The files request carries the snapshot (plus the page params).
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining(`${base}/files?snapshot=4100`),
      expect.anything(),
    );
  });

  it("offers a copy button per file path, carrying the FULL path", async () => {
    const user = userEvent.setup();
    const written: string[] = [];
    // navigator.clipboard is getter-only in jsdom, so it has to be
    // defined rather than assigned.
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: {
        writeText: (t: string) => {
          written.push(t);
          return Promise.resolve();
        },
      },
    });
    mockFetch(happyHandler);
    renderApp(route);
    await user.click(screen.getByRole("tab", { name: "files" }));
    await screen.findByText("500,000");

    const copies = screen.getAllByRole("button", { name: "Copy path" });
    expect(copies.length).toBe(filesFixture.length);
    await user.click(copies[0]);

    // The whole path, not the truncated rendering: the cell shows the
    // tail only, and copying what is on screen would be useless.
    expect(written).toEqual([filesFixture[0].path]);
    expect(written[0]).toContain("s3://");
  });

  it("offers copy buttons on the scan tab, for data AND delete file paths", async () => {
    const user = userEvent.setup();
    const written: string[] = [];
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: {
        writeText: (t: string) => {
          written.push(t);
          return Promise.resolve();
        },
      },
    });
    mockFetch(happyHandler);
    renderApp(route);
    await user.click(screen.getByRole("tab", { name: "scan" }));
    await screen.findByText("1,250");

    // The delete-file path gets its own button and its own label, so the
    // two are distinguishable to a screen reader and in the tooltip.
    // No table in the dev stack carries a deletion vector, so this is the
    // only coverage the delete-file path has.
    const dataCopies = screen.getAllByRole("button", { name: "Copy path" });
    const dvCopies = screen.getAllByRole("button", { name: "Copy delete file path" });
    expect(dataCopies.length).toBe(scanFixture.length);
    expect(dvCopies.length).toBe(
      scanFixture.filter((sf) => sf.delete_file).length,
    );

    await user.click(dvCopies[0]);
    expect(written).toEqual([scanFixture[0].delete_file?.path]);
    expect(written[0]).toContain(".puffin");
  });

  it("renders int64 file values above 2^53 exactly on the Files tab", async () => {
    mockFetch((url) => {
      const [path] = url.split("?");
      if (path === base) return jsonResponse(tableFixture);
      if (path === `${base}/files`)
        return new Response(bigIntFilesWireBody, {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      return undefined;
    });
    renderApp(route);
    const user = userEvent.setup();

    await user.click(await screen.findByRole("tab", { name: "files" }));

    // record_count 2^53+1, grouped exactly (odd, not the even neighbour).
    expect(
      await screen.findByText("9,007,199,254,740,993"),
    ).toBeInTheDocument();
    // The row-id span, both ends verbatim: lower is row_id_start 2^53+3,
    // upper is row_id_start + record_count - 1. These are the ordering
    // key an unsorted table's files are read by, and both ends are past
    // the point where a Number round-trip would start lying.
    expect(screen.getByText("9007199254740995")).toBeInTheDocument();
    expect(screen.getByText("18014398509481987")).toBeInTheDocument();
    // file_size_bytes 2^62+1: humanized cell, exact value in the tooltip.
    const sizeCell = screen.getByTitle("4611686018427387905");
    expect(sizeCell).toBeInTheDocument();
    expect(sizeCell.textContent).not.toContain("NaN");
  });

  /** The id cell of every file row, in render order. */
  // The most recent GET .../files URL the app issued. The listing is
  // paginated server-side, so the request — not the rendered order of a
  // fixed mock — is where the sort lives now.
  function lastFilesUrl(mock: ReturnType<typeof mockFetch>): string {
    const calls = mock.mock.calls
      .map((c) => String(c[0]))
      .filter((u) => u.split("?")[0].endsWith("/files"));
    return calls[calls.length - 1] ?? "";
  }

  it("sorts server-side: each header drives the query with sort, order, and offset 0", async () => {
    // Ordering moved to the server with pagination — a client sort would
    // only order the loaded page. So clicking a header must REQUEST the
    // sort (from the first page), not reshuffle what is on screen.
    const fetchMock = mockFetch(happyHandler);
    renderApp(route);
    const user = userEvent.setup();
    await user.click(await screen.findByRole("tab", { name: "files" }));
    await screen.findByText("101");

    // Initial load: manifest order (no sort), and paged.
    expect(lastFilesUrl(fetchMock)).not.toContain("sort=");
    expect(lastFilesUrl(fetchMock)).toContain("limit=100");
    expect(lastFilesUrl(fetchMock)).toContain("offset=0");

    // First click is DESCENDING — "which are the big ones" in one click —
    // and pages from the top again.
    const size = screen.getByRole("button", { name: /^size/ });
    await user.click(size);
    await waitFor(() => expect(lastFilesUrl(fetchMock)).toContain("sort=size"));
    expect(lastFilesUrl(fetchMock)).toContain("order=desc");
    expect(lastFilesUrl(fetchMock)).toContain("offset=0");
    expect(size.closest("th")).toHaveAttribute("aria-sort", "descending");

    // Second click flips direction.
    await user.click(size);
    await waitFor(() => expect(lastFilesUrl(fetchMock)).toContain("order=asc"));
    expect(size.closest("th")).toHaveAttribute("aria-sort", "ascending");

    // The header label maps to the server's column name.
    await user.click(screen.getByRole("button", { name: /^record_count/ }));
    await waitFor(() =>
      expect(lastFilesUrl(fetchMock)).toContain("sort=record_count"),
    );
    // Only one column is ever the active sort.
    expect(size.closest("th")).toHaveAttribute("aria-sort", "none");
  });

  it("shows the decoded columns but does not offer sort on them", async () => {
    // partition and the ordering-key min/max are decoded client-side (a
    // text[] of ordinals; encoded bound bytes) and match no SQL column's
    // order, so under server paging they render as plain, unsortable
    // headers while the raw columns stay sortable.
    mockFetch(happyHandler);
    renderApp(route);
    const user = userEvent.setup();
    await user.click(await screen.findByRole("tab", { name: "files" }));
    await screen.findByText("101");

    for (const label of [/^partition/, /_hog_row_id min/, /_hog_row_id max/]) {
      expect(screen.queryByRole("button", { name: label })).not.toBeInTheDocument();
      expect(screen.getByRole("columnheader", { name: label })).toBeInTheDocument();
    }
    for (const label of [/^size/, /^record_count/, /^id/, /^path/]) {
      expect(screen.getByRole("button", { name: label })).toBeInTheDocument();
    }
  });

  it("pages the files with Load more, fetching the next offset", async () => {
    // A full first page means there may be more; a short second page ends
    // it. Rows accumulate across pages.
    const oneFile = (id: number) => ({ ...filesFixture[0], data_file_id: String(id) });
    const fetchMock = mockFetch((url) => {
      const [path, qs] = url.split("?");
      if (path === base) return jsonResponse(tableFixture);
      if (path === `${base}/files`) {
        const offset = Number(new URLSearchParams(qs).get("offset") ?? "0");
        return jsonResponse(
          offset === 0
            ? Array.from({ length: 100 }, (_, i) => oneFile(1000 + i))
            : [oneFile(2000), oneFile(2001)],
        );
      }
      return undefined;
    });
    renderApp(route);
    const user = userEvent.setup();
    await user.click(await screen.findByRole("tab", { name: "files" }));
    await screen.findByText("1000"); // first page landed

    // The first page was full, so Load more is offered.
    const more = await screen.findByRole("button", { name: /load more/i });
    await user.click(more);

    // The second page is fetched at the next offset and appended.
    await waitFor(() => expect(lastFilesUrl(fetchMock)).toContain("offset=100"));
    await screen.findByText("2000");
    expect(screen.getByText("1000")).toBeInTheDocument(); // page 1 still there

    // The short page ends paging: no more button.
    expect(
      screen.queryByRole("button", { name: /load more/i }),
    ).not.toBeInTheDocument();
  });

  it("sorts the scan table, keeping files without a deletion vector last", async () => {
    mockFetch(happyHandler);
    renderApp(route);
    const user = userEvent.setup();
    await user.click(await screen.findByRole("tab", { name: "scan" }));

    const ids = () =>
      screen
        .getAllByRole("row")
        .slice(1)
        .map((r) => r.querySelector("td:first-child")?.textContent ?? "");
    const before = ids();
    expect(before.length).toBeGreaterThan(1);

    // delete_count is absent on a file with no deletion vector. Absent
    // is not "zero deletes", so those rows stay at the bottom whichever
    // way the column points.
    const deletes = screen.getByRole("button", { name: /^delete_count/ });
    await user.click(deletes);
    const desc = ids();
    await user.click(deletes);
    const asc = ids();
    const noDv = before.filter(
      (id) => !scanFixture.find((sf) => sf.data_file.data_file_id === id)?.delete_file,
    );
    for (const id of noDv) {
      expect(desc.slice(-noDv.length)).toContain(id);
      expect(asc.slice(-noDv.length)).toContain(id);
    }
  });

  // 300-odd characters, the shape a $properties column's bounds actually
  // take: long enough that an untruncated cell pushes the bound columns
  // off the viewport.
  const LONG_BOUND =
    '{"$lib":"web","$lib_version":"1.4.0","$screen":"1440x900",' +
    '"$payload":"0010528486358554e5f1a4aa5242fafd2a4da0502be3cf9c3dcdafdf' +
    '007c9195673777557b8266051f97fd3b24e73dce83a5b19a5570d78376e7a03bb717' +
    'ee4a15b37b8053"}';

  it("expands a file row into decoded per-column stats", async () => {
    // RAW wire body: the decoded bounds are JSON numbers on the wire, and
    // long/uint64/decimal must reach the screen digit-for-digit (never
    // through a double) — same raw-token discipline as the int64 fields.
    const statsWireBody =
      `{"data_file_id":101,"stats_state":"provided","columns":[` +
      `{"field_id":11,"name":"user_id","path":"user_id","type":"long",` +
      `"value_count":500000,"null_count":0,"lower_bound":9007199254740993,` +
      `"upper_bound":9223372036854775807},` +
      `{"field_id":12,"name":"amount","path":"amount","type":"decimal",` +
      `"type_params":{"precision":10,"scale":2},"value_count":500000,` +
      `"null_count":3,"lower_bound":1.50,"upper_bound":999.99},` +
      `{"field_id":13,"name":"element","path":"tags.element","type":"string",` +
      `"value_count":500000,"null_count":0,"size_bytes":4096,` +
      `"lower_bound":"aardvark","upper_bound":"🦔"},` +
      `{"field_id":14,"name":"note","path":"note","type":"string",` +
      `"value_count":500000,"null_count":500000,"lower_bound":null,` +
      `"upper_bound":null},` +
      `{"field_id":15,"name":"active","path":"active","type":"boolean",` +
      `"value_count":500000,"null_count":0,"lower_bound":false,` +
      `"upper_bound":true},` +
      `{"field_id":16,"name":"score","path":"score","type":"double",` +
      `"value_count":500000,"null_count":0,"lower_bound":-0.0,` +
      `"upper_bound":"Infinity"},` +
      `{"field_id":17,"name":"seen_at","path":"seen_at","type":"timestamptz",` +
      `"value_count":500000,"null_count":0,` +
      `"lower_bound":"1970-01-01T00:00:00Z",` +
      `"upper_bound":"2026-09-05T12:00:00Z"},` +
      // A properties blob: min and max are whole JSON payloads, which is
      // what blew the table's width open on a real stream table.
      `{"field_id":18,"name":"properties","path":"properties","type":"string",` +
      `"value_count":500000,"null_count":0,` +
      `"lower_bound":${JSON.stringify(LONG_BOUND)},` +
      `"upper_bound":${JSON.stringify(LONG_BOUND)}}]}`;
    mockFetch((url) => {
      const [path] = url.split("?");
      if (path === `${base}/files/101/stats`)
        return new Response(statsWireBody, {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      return happyHandler(url);
    });
    renderApp(route);
    const user = userEvent.setup();

    await user.click(await screen.findByRole("tab", { name: "files" }));
    await user.click(
      await screen.findByRole("button", { name: "toggle stats for file 101" }),
    );

    // long bounds, exact — 2^53+1 would round to ...992 through a double.
    expect(await screen.findByText("9007199254740993")).toBeInTheDocument();
    expect(screen.getByText("9223372036854775807")).toBeInTheDocument();
    // decimal at the column scale, token verbatim.
    expect(screen.getByText("1.50")).toBeInTheDocument();
    expect(screen.getByText("999.99")).toBeInTheDocument();
    // strings verbatim; a nested leaf shows its dotted path.
    expect(screen.getByText("aardvark")).toBeInTheDocument();
    expect(screen.getByText("🦔")).toBeInTheDocument();
    expect(screen.getByText("tags.element")).toBeInTheDocument();
    // All-null column: explicit null bounds, flagged as "no bound".
    expect(screen.getAllByTitle(/no bound stored/)).toHaveLength(2);
    // Boolean bounds render their JSON tokens, never a placeholder.
    expect(screen.getByText("false")).toBeInTheDocument();

    // A blob bound renders truncated rather than at full width — the
    // cell keeps the whole value in its tooltip, so nothing is lost,
    // and the bound columns stay on screen.
    const blobs = screen.getAllByTitle(LONG_BOUND);
    expect(blobs).toHaveLength(2);
    for (const blob of blobs) {
      expect(blob).toHaveClass("bound-text");
      expect(blob.textContent).toBe(LONG_BOUND);
    }
    // Short bounds keep the plain numeric cell: no truncation wrapper.
    expect(screen.getByText("aardvark")).not.toHaveClass("bound-text");
    expect(screen.getByText("true")).toBeInTheDocument();
    // The Infinity sentinel STRING reaches the cell verbatim.
    expect(screen.getByText("Infinity")).toBeInTheDocument();
    // -0.0 keeps its sign: the raw token is the value (String(-0) in a
    // double round-trip would render "0").
    expect(screen.getByText("-0.0")).toBeInTheDocument();
    // Temporal bounds are the server's ISO strings, verbatim.
    expect(screen.getByText("2026-09-05T12:00:00Z")).toBeInTheDocument();

    // Collapse hides the panel again.
    await user.click(
      screen.getByRole("button", { name: "toggle stats for file 101" }),
    );
    expect(screen.queryByText("aardvark")).not.toBeInTheDocument();
  });

  it("explains every stats marker on hover, without a click or a request", async () => {
    // It used to take a click and a round trip to learn why a file has
    // no statistics. The answer is a property of the STATE — every
    // pending file has the same one — so it now rides the marker's
    // tooltip, and the stats endpoint is never called for a file that
    // has none to give.
    const seen: string[] = [];
    mockFetch((url) => {
      seen.push(url);
      return happyHandler(url);
    });
    renderApp(route);
    const user = userEvent.setup();

    await user.click(await screen.findByRole("tab", { name: "files" }));
    await screen.findByText("101");

    // EVERY marker explains itself, the expandable one included: the
    // shape says whether a row opens, never what the state costs a
    // reader planning a scan.
    const tips = {
      provided: screen
        .getByRole("button", { name: "toggle stats for file 101" })
        .getAttribute("title"),
      pending: screen
        .getByLabelText("pending: no column statistics for file 102")
        .getAttribute("title"),
      failed: screen
        .getByLabelText("failed: no column statistics for file 103")
        .getAttribute("title"),
    };
    expect(tips.provided).toMatch(/hydrated/);
    expect(tips.provided).toMatch(/click to see them/i);
    expect(tips.pending).toMatch(/not been hydrated yet/);
    expect(tips.failed).toMatch(/rehydrate/);

    // Each says what the state costs a reader, in the same words, so
    // hovering two rows compares like with like.
    expect(tips.provided).toMatch(/can prune this file/);
    expect(tips.pending).toMatch(/cannot prune it/);
    expect(tips.failed).toMatch(/cannot prune it/);

    // Three distinct explanations, not one text reused.
    expect(new Set(Object.values(tips)).size).toBe(3);

    // All of it is on screen, and none of it cost a request.
    expect(seen.filter((u) => u.includes("/stats"))).toEqual([]);
  });

  it("shows the 404 detail when the table does not exist", async () => {
    mockFetch((url) =>
      url.startsWith(base)
        ? jsonResponse(
            { error: "not_found", detail: "table 'pageviews' does not exist" },
            404,
          )
        : undefined,
    );
    renderApp(route);
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("not_found");
    expect(alert).toHaveTextContent("table 'pageviews' does not exist");
  });

  it("shows an error on the Files tab without killing the page", async () => {
    mockFetch((url) => {
      const [path] = url.split("?");
      if (path === `${base}/files`) return jsonResponse(notFoundError, 404);
      return happyHandler(url);
    });
    renderApp(route);
    const user = userEvent.setup();

    await screen.findByText("1,234,567");
    await user.click(screen.getByRole("tab", { name: "files" }));
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("not_found");
    // Stats header still up.
    expect(screen.getByText("1,234,567")).toBeInTheDocument();
  });
});
