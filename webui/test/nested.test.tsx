import { screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Column, Table } from "../src/api/types";
import { formatColumnType } from "../src/api/types";
import { formatPartitionField } from "../src/lib/format";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

/**
 * The console's read side for nested columns. The create form stays
 * scalar-only (see NamespacePage.test.tsx), but a table can have nested
 * columns whoever made it, and a schema tab that showed `list` with no
 * element — or hid a struct's fields, and with them their field ids — is
 * the page failing at the one thing an operator opens it for.
 */
const nestedTable: Table = {
  name: "events",
  namespace: "ns",
  table_uuid: "1f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71",
  columns: [
    { field_id: "1", ordinal: 0, name: "id", type: "long", nullable: false },
    {
      field_id: "2",
      ordinal: 1,
      name: "tags",
      type: "list",
      nullable: true,
      children: [
        { field_id: "3", ordinal: 0, name: "element", type: "string", nullable: true },
      ],
    },
    {
      field_id: "4",
      ordinal: 2,
      name: "addr",
      type: "struct",
      nullable: true,
      children: [
        { field_id: "5", ordinal: 0, name: "city", type: "string", nullable: true },
        { field_id: "6", ordinal: 1, name: "zip", type: "int", nullable: true },
      ],
    },
    {
      field_id: "7",
      ordinal: 3,
      name: "props",
      type: "map",
      nullable: true,
      children: [
        { field_id: "8", ordinal: 0, name: "key", type: "string", nullable: false },
        { field_id: "9", ordinal: 1, name: "value", type: "long", nullable: true },
      ],
    },
  ],
  record_count: "4",
  file_count: "1",
  file_size_bytes: "1024",
};

describe("formatColumnType", () => {
  it("renders each container as one readable signature", () => {
    expect(formatColumnType(nestedTable.columns[0])).toBe("long");
    expect(formatColumnType(nestedTable.columns[1])).toBe("list<string>");
    expect(formatColumnType(nestedTable.columns[2])).toBe(
      "struct<city: string, zip: int>",
    );
    expect(formatColumnType(nestedTable.columns[3])).toBe("map<string, long>");
  });

  it("recurses, so a nested container reads as one signature too", () => {
    expect(
      formatColumnType({
        name: "deep",
        type: "struct",
        children: [
          {
            name: "runs",
            type: "list",
            children: [
              {
                name: "element",
                type: "struct",
                children: [{ name: "score", type: "double" }],
              },
            ],
          },
        ],
      }),
    ).toBe("struct<runs: list<struct<score: double>>>");
  });

  it("does not pretend a malformed container is well-formed", () => {
    // A server that somehow returned a childless list is a bug, and the
    // page must not paper over it with `list<>`.
    expect(formatColumnType({ name: "l", type: "list" })).toBe("list<?>");
    expect(formatColumnType({ name: "m", type: "map", children: [] })).toBe("map<?>");
  });
});

describe("TablePage schema tab", () => {
  it("shows every nested node, indented, with its own field id", async () => {
    mockFetch((url) =>
      url === "/v1/catalogs/analytics/namespaces/ns/tables/events"
        ? jsonResponse(nestedTable)
        : jsonResponse([]),
    );
    renderApp("/catalogs/analytics/namespaces/ns/tables/events");

    const idCell = await screen.findByText("id");
    const rows = idCell.closest("tbody")!.querySelectorAll("tr");
    const shown = Array.from(rows).map((r) => {
      const cells = r.querySelectorAll("td");
      return [cells[0].textContent, cells[1].textContent, cells[2].textContent];
    });

    // Parents before children, containers as signatures, every field id
    // visible — an operator reading this page is usually here to find
    // exactly one of those ids.
    expect(shown).toEqual([
      ["1", "id", "long"],
      ["2", "tags", "list<string>"],
      ["3", "element", "string"],
      ["4", "addr", "struct<city: string, zip: int>"],
      ["5", "city", "string"],
      ["6", "zip", "int"],
      ["7", "props", "map<string, long>"],
      ["8", "key", "string"],
      ["9", "value", "long"],
    ]);

    // The map key's required-ness is real schema, not decoration.
    const keyRow = rows[7];
    expect(within(keyRow).getByTitle("not null")).toBeInTheDocument();
  });
});

describe("nested partition sources", () => {
  it("labels a struct leaf by its dotted path, not its bare name", () => {
    // Two structs each holding a `zip`: by bare name both render "zip"
    // and the page shows one table partitioned twice by the same
    // apparent column. The server's partition-stats endpoint labels
    // them by path for exactly this reason.
    const columns: Column[] = [
      {
        field_id: "1",
        ordinal: 0,
        name: "home",
        type: "struct",
        children: [
          { field_id: "2", ordinal: 0, name: "zip", type: "string" },
        ],
      },
      {
        field_id: "3",
        ordinal: 1,
        name: "work",
        type: "struct",
        children: [
          { field_id: "4", ordinal: 0, name: "zip", type: "string" },
        ],
      },
    ];
    expect(
      formatPartitionField(
        { source_field_id: "2", transform: "identity" },
        columns,
      ),
    ).toBe("identity(home.zip)");
    expect(
      formatPartitionField(
        { source_field_id: "4", transform: "bucket", transform_param: 16 },
        columns,
      ),
    ).toBe("bucket(16, work.zip)");
    // A source the schema does not hold still degrades to the field id.
    expect(
      formatPartitionField(
        { source_field_id: "99", transform: "identity" },
        columns,
      ),
    ).toBe("identity(field 99)");
  });

  it("renders a struct signature in ordinal order, not array order", () => {
    // ordinal is the contract; array order is not.
    expect(
      formatColumnType({
        name: "s",
        type: "struct",
        children: [
          { field_id: "3", ordinal: 1, name: "b", type: "string" },
          { field_id: "2", ordinal: 0, name: "a", type: "int" },
        ] as Column[],
      }),
    ).toBe("struct<a: int, b: string>");
  });
});
