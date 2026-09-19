import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { filesFixture, notFoundError, scanFixture, tableFixture } from "./fixtures";
import {
  bigIntFilesWireBody,
  closeBigIntFilesWireBody,
} from "./fixtures.adversarial";
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
    expect(fetchMock).toHaveBeenCalledWith(
      `${base}/files?snapshot=4100`,
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
    // row_id_start 2^53+3 verbatim — the lineage anchor an operator reads.
    expect(screen.getByText("9007199254740995")).toBeInTheDocument();
    // file_size_bytes 2^62+1: humanized cell, exact value in the tooltip.
    const sizeCell = screen.getByTitle("4611686018427387905");
    expect(sizeCell).toBeInTheDocument();
    expect(sizeCell.textContent).not.toContain("NaN");
  });

  /** The id cell of every file row, in render order. */
  function fileIdOrder(): string[] {
    return screen
      .getAllByRole("row")
      .slice(1) // drop the header
      .map((r) => r.querySelector("td:first-child")?.textContent ?? "")
      .filter((t) => t !== "");
  }

  it("sorts the files table by any column, and back to the server's order", async () => {
    mockFetch(happyHandler);
    renderApp(route);
    const user = userEvent.setup();
    await user.click(await screen.findByRole("tab", { name: "files" }));

    // Opens in the server's order, which is the manifest's: by
    // begin_snapshot, then row_id_start, then id.
    expect(fileIdOrder()).toEqual(["101", "102", "103"]);

    // First click is DESCENDING: "which are the big ones" in one click.
    const size = screen.getByRole("button", { name: /^size/ });
    await user.click(size);
    expect(fileIdOrder()).toEqual(["101", "102", "103"]);
    expect(size.closest("th")).toHaveAttribute("aria-sort", "descending");

    await user.click(size);
    expect(fileIdOrder()).toEqual(["103", "102", "101"]);
    expect(size.closest("th")).toHaveAttribute("aria-sort", "ascending");

    // record_count and row_id_start order numerically, not as text.
    await user.click(screen.getByRole("button", { name: /^record_count/ }));
    expect(fileIdOrder()).toEqual(["101", "102", "103"]);

    // A different column starts descending again, and only one column
    // is ever marked as the sort.
    const rowId = screen.getByRole("button", { name: /^row_id_start/ });
    await user.click(rowId);
    expect(fileIdOrder()).toEqual(["103", "102", "101"]);
    expect(rowId.closest("th")).toHaveAttribute("aria-sort", "descending");
    expect(
      screen.getByRole("button", { name: /^record_count/ }).closest("th"),
    ).toHaveAttribute("aria-sort", "none");
  });

  it("sorts file sizes that are the same double exactly", async () => {
    // Three sizes one apart above 2^53. Through Number they all compare
    // equal, so a lossy sort returns them untouched — and untouched is
    // indistinguishable from a correct descending sort unless the input
    // is ASCENDING, as it is here.
    mockFetch((url) => {
      const [path] = url.split("?");
      if (path === base) return jsonResponse(tableFixture);
      if (path === `${base}/files`)
        return new Response(closeBigIntFilesWireBody, {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      return undefined;
    });
    renderApp(route);
    const user = userEvent.setup();
    await user.click(await screen.findByRole("tab", { name: "files" }));

    expect(fileIdOrder()).toEqual(["1", "2", "3"]);
    await user.click(screen.getByRole("button", { name: /^size/ }));
    expect(fileIdOrder()).toEqual(["3", "2", "1"]);
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

  it("gives the no-stats reason without a click, and asks the server for nothing", async () => {
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

    expect(
      screen.getByLabelText("pending: no column statistics for file 102"),
    ).toHaveAttribute("title", expect.stringContaining("not been hydrated yet"));
    expect(
      screen.getByLabelText("failed: no column statistics for file 103"),
    ).toHaveAttribute("title", expect.stringContaining("rehydrate"));

    // Both reasons are on screen, and neither cost a request.
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
