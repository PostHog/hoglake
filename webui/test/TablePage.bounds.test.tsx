import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { INTEGER_COLUMN_TYPES } from "../src/api/types";
import type { Column, DataFile, Table } from "../src/api/types";
import { filesFixture, sortedFilesFixture, tableFixture } from "./fixtures";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const base = "/v1/catalogs/analytics/namespaces/events/tables/pageviews";
const route = "/catalogs/analytics/namespaces/events/tables/pageviews";

/**
 * A raw stats wire body for file 101: one column per case, bounds as JSON
 * NUMBERS where the type is numeric, so the raw-token discipline is under
 * test too (a double round-trip would ruin the uint64 before grouping
 * ever saw it).
 */
function statsBody(
  columns: { name: string; type: string; lower: string; upper: string }[],
): string {
  const cols = columns.map(
    (c, i) =>
      `{"field_id":${100 + i},"name":"${c.name}","path":"${c.name}",` +
      `"type":"${c.type}","value_count":10,"null_count":0,` +
      `"lower_bound":${c.lower},"upper_bound":${c.upper}}`,
  );
  return `{"data_file_id":101,"stats_state":"provided","columns":[${cols.join(
    ",",
  )}]}`;
}

function mockStats(body: string) {
  return mockFetch((url) => {
    const [path] = url.split("?");
    if (path === base) return jsonResponse(tableFixture);
    if (path === `${base}/files`) return jsonResponse(filesFixture);
    if (path === `${base}/files/101/stats`)
      return new Response(body, {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    return undefined;
  });
}

async function openStats() {
  const user = userEvent.setup();
  renderApp(route);
  await user.click(await screen.findByRole("tab", { name: "files" }));
  await user.click(
    await screen.findByRole("button", { name: "toggle stats for file 101" }),
  );
}

/** The cells of the file row with this id, in render order. */
function fileRowCells(id: string): string[] {
  const row = screen
    .getAllByRole("row")
    .find((r) => r.querySelector("td:first-child")?.textContent === id);
  if (!row) throw new Error(`no file row ${id}`);
  return [...row.querySelectorAll("td")].map((c) => c.textContent ?? "");
}

// A bound of digits is grouped only when the COLUMN says it is a whole
// number: the decoded token for a long 626623 and for a string column
// holding "626623" are the same characters.
describe("integer bounds are digit-grouped by column type", () => {
  it("groups a long bound, negatives included, and keeps the token in the title", async () => {
    mockStats(
      statsBody([
        { name: "offset", type: "long", lower: "-626623", upper: "626623" },
      ]),
    );
    await openStats();

    expect(await screen.findByText("626,623")).toBeInTheDocument();
    expect(screen.getByText("-626,623")).toBeInTheDocument();
    // The exact token an operator pastes into a query.
    expect(screen.getByTitle("626623")).toBeInTheDocument();
    expect(screen.getByTitle("-626623")).toBeInTheDocument();
  });

  it("groups a uint64 bound above 2^53 without losing a digit", async () => {
    // 2^53+1 is odd: any Number round-trip on the way to the grouping
    // shows up as ...992.
    mockStats(
      statsBody([
        {
          name: "seq",
          type: "uint64",
          lower: "9007199254740993",
          upper: "18446744073709551615",
        },
      ]),
    );
    await openStats();

    expect(await screen.findByText("9,007,199,254,740,993")).toBeInTheDocument();
    expect(
      screen.getByText("18,446,744,073,709,551,615"),
    ).toBeInTheDocument();
  });

  it("groups every integer type and no other type", async () => {
    // Table-driven off the exported set, so a type added to it cannot
    // quietly miss this behaviour.
    mockStats(
      statsBody([
        ...INTEGER_COLUMN_TYPES.map((type) => ({
          name: `c_${type}`,
          type,
          lower: "626623",
          upper: "626623",
        })),
        // The same DIGITS through types that must stay verbatim.
        { name: "c_string", type: "string", lower: '"626623"', upper: '"626623"' },
        { name: "c_decimal", type: "decimal", lower: "1234.5", upper: "1234.5" },
      ]),
    );
    await openStats();

    // Two cells per integer column, every one grouped.
    expect(await screen.findAllByText("626,623")).toHaveLength(
      INTEGER_COLUMN_TYPES.length * 2,
    );
    // The string column's identical digits are NOT grouped: grouping them
    // would corrupt the value.
    expect(screen.getAllByText("626623")).toHaveLength(2);
    // A decimal keeps its fraction digits and its scale.
    expect(screen.getAllByText("1234.5")).toHaveLength(2);
    expect(screen.queryByText("1,234.5")).not.toBeInTheDocument();
  });

  it("leaves digits alone for every type that only looks numeric", async () => {
    // Each of these would be corrupted, or merely misrepresented, by
    // grouping — and each is reachable from a real column.
    mockStats(
      statsBody([
        // A decimal whose value happens to have no fraction digits. The
        // TYPE is what excludes it; the token alone is indistinguishable
        // from a long.
        { name: "amount", type: "decimal", lower: "1234", upper: "1234" },
        // A string column holding a zero-padded code: grouping it would
        // both reformat and mislead ("000,123" is not a number anyone
        // stored).
        { name: "code", type: "string", lower: '"000123"', upper: '"000123"' },
        // The float sentinels: strings, and rejected by the digit test.
        {
          name: "score",
          type: "double",
          lower: '"-Infinity"',
          upper: '"Infinity"',
        },
      ]),
    );
    await openStats();

    expect(await screen.findAllByText("1234")).toHaveLength(2);
    expect(screen.queryByText("1,234")).not.toBeInTheDocument();
    expect(screen.getAllByText("000123")).toHaveLength(2);
    expect(screen.queryByText("000,123")).not.toBeInTheDocument();
    expect(screen.getByText("-Infinity")).toBeInTheDocument();
    expect(screen.getByText("Infinity")).toBeInTheDocument();
  });

  it("leaves a long blob bound on the truncating path", async () => {
    // A grouped integer is always short, so the inline/truncate split is
    // unchanged — but the blob case has to stay proven.
    const blob = "x".repeat(200);
    mockStats(
      statsBody([
        { name: "props", type: "string", lower: `"${blob}"`, upper: `"${blob}"` },
      ]),
    );
    await openStats();

    const cells = await screen.findAllByTitle(blob);
    expect(cells).toHaveLength(2);
    for (const cell of cells) expect(cell).toHaveClass("bound-text");
  });

  it("truncates an over-long bound even when its type would group it", async () => {
    // The width guard is the TABLE's, so it runs before the grouping: a
    // conforming int64 cannot reach 48 digits, but the guard has to hold
    // for what a payload actually contains, not for what a type promises.
    const absurd = "1".repeat(60);
    mockStats(
      statsBody([
        { name: "seq", type: "long", lower: absurd, upper: absurd },
      ]),
    );
    await openStats();

    const cells = await screen.findAllByTitle(absurd);
    expect(cells).toHaveLength(2);
    for (const cell of cells) {
      expect(cell).toHaveClass("bound-text");
      // Untouched by the grouping, which never saw it.
      expect(cell.textContent).toBe(absurd);
    }
  });
});

// The files table's ordering-key columns have no type beside them: the
// type comes from the table's schema, or from the row id being a long by
// construction.
describe("ordering-key bounds resolve their type from the schema", () => {
  it("groups a row-id span and keeps the unknown upper end's explanation", async () => {
    // A compaction output: explicit row ids, so the maximum is genuinely
    // unknown and stays a labelled null rather than becoming a number.
    const file: DataFile = {
      ...filesFixture[0],
      data_file_id: "901",
      explicit_row_ids: true,
      ordering_bounds: { lower_bound: "626623", upper_bound: null },
    };
    mockFetch((url) => {
      const [path] = url.split("?");
      if (path === base) return jsonResponse(tableFixture);
      if (path === `${base}/files`) return jsonResponse([file]);
      return undefined;
    });
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "files" }));

    await screen.findByText("901");
    expect(fileRowCells("901")).toContain("626,623");
    expect(
      screen.getByTitle(/unknown — this file carries explicit row ids/),
    ).toHaveTextContent("null");
  });

  it("groups a nested long ordering key, found through the struct", async () => {
    // The leading sort field is a struct CHILD, so the type lookup has to
    // walk children exactly as the column-path lookup does.
    const columns: Column[] = [
      ...tableFixture.columns,
      {
        field_id: "9",
        ordinal: 3,
        name: "addr",
        type: "struct",
        children: [
          { field_id: "10", ordinal: 0, name: "zip", type: "long" },
        ],
      },
    ];
    const table: Table = {
      ...tableFixture,
      columns,
      sort_spec: {
        sort_id: "4",
        fields: [
          { source_field_id: "10", direction: "asc", null_order: "nulls_last" },
        ],
      },
    };
    const file: DataFile = {
      ...sortedFilesFixture[0],
      data_file_id: "902",
      ordering_bounds: {
        field_id: "10",
        lower_bound: "626623",
        upper_bound: "999999",
      },
    };
    mockFetch((url) => {
      const [path] = url.split("?");
      if (path === base) return jsonResponse(table);
      if (path === `${base}/files`) return jsonResponse([file]);
      return undefined;
    });
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "files" }));

    await screen.findByText("902");
    // The column is named by its dotted path, and its bounds are grouped.
    expect(
      screen.getByRole("columnheader", { name: /addr\.zip min/ }),
    ).toBeInTheDocument();
    expect(fileRowCells("902")).toContain("626,623");
    expect(fileRowCells("902")).toContain("999,999");
  });

  it("renders a bound verbatim when the key's field_id is not in the schema", async () => {
    // A key column dropped since the file landed: no type, so no claim
    // about what the digits mean.
    const file: DataFile = {
      ...sortedFilesFixture[0],
      data_file_id: "903",
      ordering_bounds: {
        field_id: "4242",
        lower_bound: "626623",
        upper_bound: "999999",
      },
    };
    mockFetch((url) => {
      const [path] = url.split("?");
      if (path === base) return jsonResponse(tableFixture);
      if (path === `${base}/files`) return jsonResponse([file]);
      return undefined;
    });
    const user = userEvent.setup();
    renderApp(route);
    await user.click(await screen.findByRole("tab", { name: "files" }));

    await screen.findByText("903");
    expect(fileRowCells("903")).toContain("626623");
    expect(fileRowCells("903")).not.toContain("626,623");
  });
});
