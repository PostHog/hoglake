import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { filesFixture, notFoundError, scanFixture, tableFixture } from "./fixtures";
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

  it("shows data files with color-coded stats_state badges on the Files tab", async () => {
    mockFetch(happyHandler);
    renderApp(route);
    const user = userEvent.setup();

    await user.click(await screen.findByRole("tab", { name: "files" }));

    expect(await screen.findByText("101")).toBeInTheDocument();
    expect(
      screen.getByText("s3://hog-lake/analytics/events/pageviews/data-00101.parquet"),
    ).toBeInTheDocument();
    expect(screen.getByText("500,000")).toBeInTheDocument();

    const provided = screen.getByText("provided");
    const pending = screen.getByText("pending");
    const failed = screen.getByText("failed");
    expect(provided).toHaveClass("stats-provided");
    expect(pending).toHaveClass("stats-pending");
    expect(failed).toHaveClass("stats-failed");

    // partition_values render when present, em-dash when absent.
    expect(screen.getByText("[2026-09-01, 7]")).toBeInTheDocument();
    expect(screen.getByText("[2026-09-02, null]")).toBeInTheDocument();
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
      `"upper_bound":"2026-09-05T12:00:00Z"}]}`;
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

  it("shows the no-stats reason for a pending file", async () => {
    mockFetch((url) => {
      const [path] = url.split("?");
      if (path === `${base}/files/102/stats`)
        return jsonResponse({
          data_file_id: "102",
          stats_state: "pending",
          columns: [],
          no_stats_reason:
            "stats_state is 'pending': column statistics have not been " +
            "hydrated yet, so no per-column rows exist; with no bounds, " +
            "callers must not prune this file",
        });
      return happyHandler(url);
    });
    renderApp(route);
    const user = userEvent.setup();

    await user.click(await screen.findByRole("tab", { name: "files" }));
    await user.click(
      await screen.findByRole("button", { name: "toggle stats for file 102" }),
    );

    expect(
      await screen.findByText(/callers must not prune this file/),
    ).toBeInTheDocument();
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
