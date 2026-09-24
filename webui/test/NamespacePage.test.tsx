import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { notFoundError, tableFixture, tablesFixture, unprocessableError } from "./fixtures";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const tablesUrl = "/v1/catalogs/analytics/namespaces/events/tables";

describe("NamespacePage", () => {
  it("lists tables with their head rollup and offers the create-table form", async () => {
    mockFetch((url) => (url === tablesUrl ? jsonResponse(tablesFixture) : undefined));
    renderApp("/catalogs/analytics/namespaces/events");

    expect(await screen.findByText("pageviews")).toBeInTheDocument();
    expect(screen.getByText("clicks")).toBeInTheDocument();

    // table_uuid stays on the wire — consumers key on it — but it is no
    // longer a COLUMN: the table page shows it with its copy button.
    expect(
      screen.queryByText("3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71"),
    ).not.toBeInTheDocument();

    // Rollup: counts digit-grouped, bytes through the size formatter.
    expect(screen.getByText("1,234,567")).toBeInTheDocument();
    expect(screen.getByText("12")).toBeInTheDocument();
    expect(screen.getByText("942 MiB")).toBeInTheDocument();
    // Snapshot count and earliest id stay plain digits.
    expect(screen.getByText("41")).toBeInTheDocument();
    expect(screen.getByText("7")).toBeInTheDocument();

    // Dynamic column row: name input, 23-type dropdown, nullable toggle.
    expect(screen.getByLabelText("column 1 name")).toBeInTheDocument();
    const typeSelect = screen.getByLabelText("column 1 type");
    expect(typeSelect.querySelectorAll("option")).toHaveLength(23);
    expect(screen.getByLabelText("column 1 nullable")).toBeChecked();
  });

  it("offers the DuckLake scalar types and never the ones the server refuses", async () => {
    mockFetch((url) => (url === tablesUrl ? jsonResponse([]) : undefined));
    renderApp("/catalogs/analytics/namespaces/events");

    await screen.findByText("No tables in this namespace.");
    const options = Array.from(
      screen.getByLabelText("column 1 type").querySelectorAll("option"),
    ).map((o) => o.value);

    expect(options).toEqual(expect.arrayContaining([
      "int8", "int16", "uint8", "uint16", "uint32", "uint64",
      "timestamp_s", "timestamp_ms", "timestamp_ns", "json",
    ]));
    // Permanently unsupported server-side (int128/uint128 exceed Iceberg's
    // decimal(38), timetz/interval have no Iceberg mapping, geometry is out
    // of scope): offering them would be offering a guaranteed 422.
    expect(options).not.toEqual(expect.arrayContaining(["int128"]));
    expect(options).not.toEqual(expect.arrayContaining(["uint128"]));
    expect(options).not.toEqual(expect.arrayContaining(["timetz"]));
    expect(options).not.toEqual(expect.arrayContaining(["interval"]));
    expect(options).not.toEqual(expect.arrayContaining(["point"]));
    expect(options).not.toEqual(expect.arrayContaining(["geometrycollection"]));
    // list/struct/map are real server types now, and still absent here:
    // they REQUIRE children and this form has no child editor, so picking
    // one would be a guaranteed 422 exactly like the refused names above.
    expect(options).not.toEqual(expect.arrayContaining(["list"]));
    expect(options).not.toEqual(expect.arrayContaining(["struct"]));
    expect(options).not.toEqual(expect.arrayContaining(["map"]));
  });

  it("adds and removes column rows dynamically", async () => {
    mockFetch((url) => (url === tablesUrl ? jsonResponse([]) : undefined));
    renderApp("/catalogs/analytics/namespaces/events");
    const user = userEvent.setup();

    await screen.findByText("No tables in this namespace.");
    await user.click(screen.getByRole("button", { name: "+ Add column" }));
    expect(screen.getByLabelText("column 2 name")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "remove column 2" }));
    expect(screen.queryByLabelText("column 2 name")).not.toBeInTheDocument();
  });

  it("POSTs the typed column defs and refreshes the list", async () => {
    let posted: unknown = null;
    const fetchMock = mockFetch((url, init) => {
      if (url === tablesUrl && init?.method === "POST") {
        posted = JSON.parse(init.body as string);
        return jsonResponse(tableFixture, 201);
      }
      if (url === tablesUrl) {
        // After the create lands, the invalidated list refetch sees the table.
        return jsonResponse(
          posted
            ? [
                {
                  name: "pageviews",
                  table_uuid: tableFixture.table_uuid,
                  record_count: "0",
                  file_count: "0",
                  file_size_bytes: "0",
                  snapshot_count: "1",
                },
              ]
            : [],
        );
      }
      return undefined;
    });
    renderApp("/catalogs/analytics/namespaces/events");
    const user = userEvent.setup();

    await screen.findByText("No tables in this namespace.");
    await user.type(screen.getByLabelText("name"), "pageviews");
    await user.type(screen.getByLabelText("column 1 name"), "ts");
    await user.selectOptions(screen.getByLabelText("column 1 type"), "timestamptz");
    await user.click(screen.getByLabelText("column 1 nullable"));
    await user.click(screen.getByRole("button", { name: "Create table" }));

    await screen.findByText("pageviews");
    expect(posted).toEqual({
      name: "pageviews",
      columns: [{ name: "ts", type: "timestamptz", nullable: false }],
    });
    expect(fetchMock).toHaveBeenCalledWith(
      tablesUrl,
      expect.objectContaining({ method: "POST" }),
    );
  });

  it("shows a 422 validation error from create-table inline", async () => {
    mockFetch((url, init) => {
      if (url === tablesUrl && init?.method === "POST")
        return jsonResponse(unprocessableError, 422);
      if (url === tablesUrl) return jsonResponse([]);
      return undefined;
    });
    renderApp("/catalogs/analytics/namespaces/events");
    const user = userEvent.setup();

    await screen.findByText("No tables in this namespace.");
    await user.type(screen.getByLabelText("name"), "bad");
    await user.type(screen.getByLabelText("column 1 name"), "x");
    await user.click(screen.getByRole("button", { name: "Create table" }));

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("validation_failed");
    expect(alert).toHaveTextContent("unknown field ids in column_stats: [99]");
  });

  it("rejects invalid table and column names client-side without a POST", async () => {
    const fetchMock = mockFetch((url) =>
      url === tablesUrl ? jsonResponse([]) : undefined,
    );
    renderApp("/catalogs/analytics/namespaces/events");
    const user = userEvent.setup();

    await screen.findByText("No tables in this namespace.");
    await user.type(screen.getByLabelText("name"), "9bad-table");
    await user.type(screen.getByLabelText("column 1 name"), "col.dotted");
    expect(await screen.findByText(/table name:/)).toBeInTheDocument();
    expect(await screen.findByText(/column 1:/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Create table" })).toBeDisabled();

    const posts = fetchMock.mock.calls.filter(
      (c) => (c[1] as RequestInit | undefined)?.method === "POST",
    );
    expect(posts).toHaveLength(0);

    // Fixing the names clears the errors and re-enables the form.
    await user.clear(screen.getByLabelText("name"));
    await user.type(screen.getByLabelText("name"), "good_table");
    await user.clear(screen.getByLabelText("column 1 name"));
    await user.type(screen.getByLabelText("column 1 name"), "col_1");
    expect(screen.queryByText(/table name:/)).not.toBeInTheDocument();
    expect(screen.queryByText(/column 1:/)).not.toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: "Create table" }),
    ).toBeEnabled();
  });

  it("refuses reserved _hog column names but not _hog table names", async () => {
    const fetchMock = mockFetch((url) =>
      url === tablesUrl ? jsonResponse([]) : undefined,
    );
    renderApp("/catalogs/analytics/namespaces/events");
    const user = userEvent.setup();

    await screen.findByText("No tables in this namespace.");
    // A _hog-prefixed TABLE name is fine — the reservation is columns-only.
    await user.type(screen.getByLabelText("name"), "_hog_shadow");
    expect(screen.queryByText(/table name:/)).not.toBeInTheDocument();

    await user.type(screen.getByLabelText("column 1 name"), "_hog_row_id");
    expect(await screen.findByText(/column 1:.*reserved prefix/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Create table" })).toBeDisabled();

    // _hogx is refused too (prefix match, not just the exact carrier name).
    await user.clear(screen.getByLabelText("column 1 name"));
    await user.type(screen.getByLabelText("column 1 name"), "_hogx");
    expect(await screen.findByText(/column 1:.*reserved prefix/)).toBeInTheDocument();

    // No POST ever left the form.
    const posts = fetchMock.mock.calls.filter(
      (c) => (c[1] as RequestInit | undefined)?.method === "POST",
    );
    expect(posts).toHaveLength(0);

    // A plain leading underscore is still a valid column name.
    await user.clear(screen.getByLabelText("column 1 name"));
    await user.type(screen.getByLabelText("column 1 name"), "_leading");
    expect(screen.queryByText(/column 1:/)).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Create table" })).toBeEnabled();
  });

  it("clamps a comment to its first line and expands it on demand", async () => {
    mockFetch((url) => (url === tablesUrl ? jsonResponse(tablesFixture) : undefined));
    renderApp("/catalogs/analytics/namespaces/events");
    const user = userEvent.setup();

    // Collapsed: the first line only. The second must not be on screen,
    // or the clamp is doing nothing and the row grows with the comment.
    expect(await screen.findByText("raw pageview events")).toBeInTheDocument();
    expect(screen.queryByText(/kept for 90 days/)).not.toBeInTheDocument();

    const more = screen.getByRole("button", { name: "more" });
    expect(more).toHaveAttribute("aria-expanded", "false");
    await user.click(more);
    expect(screen.getByText(/kept for 90 days/)).toBeInTheDocument();

    // ...and back again, so the row returns to one line.
    await user.click(screen.getByRole("button", { name: "less" }));
    expect(screen.queryByText(/kept for 90 days/)).not.toBeInTheDocument();
  });

  it("renders an em dash for a table with no comment", async () => {
    mockFetch((url) => (url === tablesUrl ? jsonResponse(tablesFixture) : undefined));
    renderApp("/catalogs/analytics/namespaces/events");

    const clicks = (await screen.findByText("clicks")).closest("tr")!;
    const cells = Array.from(clicks.querySelectorAll("td"));
    // NAME, RECORD_COUNT, FILE_COUNT, FILE_SIZE, SNAPSHOTS,
    // EARLIEST_SNAPSHOT, COMMENT — the comment column is last.
    expect(cells).toHaveLength(7);
    expect(cells[6]).toHaveTextContent("—");
    // `clicks` also has no retained earliest snapshot: absent, not zero.
    expect(cells[5]).toHaveTextContent("—");
    // ...and no expand control, because there is nothing to expand.
    expect(cells[6].querySelector("button")).toBeNull();
  });

  it("keeps a comment as text, never as markup", async () => {
    mockFetch((url) =>
      url === tablesUrl
        ? jsonResponse([
            {
              name: "xss",
              table_uuid: "b7e6d9a2-15f3-4b08-a4c9-0e8f7d6c5b4a",
              comment: "<img src=x onerror=alert(1)>",
              record_count: "1",
              file_count: "1",
              file_size_bytes: "1",
              snapshot_count: "1",
            },
          ])
        : undefined,
    );
    renderApp("/catalogs/analytics/namespaces/events");

    expect(
      await screen.findByText("<img src=x onerror=alert(1)>"),
    ).toBeInTheDocument();
    expect(document.querySelector("img")).toBeNull();
  });

  // S3: snapshot_count must ride the INT64_FIELDS reviver, like every
  // other int64 wire field. Raw body text with an UNQUOTED 2^53+1, the
  // way CatalogsPage.test.tsx pins live_rows: a number literal that
  // JSON.parse would round to 9007199254740992, so a field missing from
  // the set renders the even neighbour and this fails.
  it("carries snapshot_count losslessly above 2^53", async () => {
    mockFetch((url) =>
      url === tablesUrl
        ? new Response(
            '[{"name":"ancient","table_uuid":"b7e6d9a2-15f3-4b08-a4c9-0e8f7d6c5b4a",' +
              '"record_count":0,"file_count":0,"file_size_bytes":0,' +
              '"snapshot_count":9007199254740993,"earliest_snapshot_id":9007199254740993}]',
            { status: 200, headers: { "content-type": "application/json" } },
          )
        : undefined,
    );
    renderApp("/catalogs/analytics/namespaces/events");

    await screen.findByText("ancient");
    // 9007199254740993 is odd, so a double round-trip cannot produce it.
    expect(screen.getAllByText("9007199254740993")).toHaveLength(2);
    expect(screen.queryByText("9007199254740992")).not.toBeInTheDocument();
  });

  it("surfaces a 404 when the namespace does not exist", async () => {
    mockFetch((url) =>
      url === tablesUrl ? jsonResponse(notFoundError, 404) : undefined,
    );
    renderApp("/catalogs/analytics/namespaces/events");
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("not_found");
  });
});
