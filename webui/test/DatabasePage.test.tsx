import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

/**
 * The database page renders what the server reports and, crucially, does
 * not invent severity of its own: the findings are the server's reading,
 * and a console that disagreed with them would be worse than one that
 * showed nothing.
 */

const server = {
  version: "16.13",
  database: "hoglake",
  size_bytes: "107374182400",
  started_at: "2026-09-01T00:00:00Z",
  connections_used: 12,
  connections_max: 100,
  cache_hit_ratio: 0.994,
  deadlocks: "0",
  committed: "1000000",
  rolled_back: "12",
  xid_age: "1000000",
  xid_freeze_max_age: "200000000",
  autovacuum_enabled: true,
  temp_files: "0",
  temp_bytes: "0",
  checkpoints_timed: "100",
  checkpoints_requested: "2",
};

const activity = {
  active: 2,
  idle: 9,
  idle_in_transaction: 0,
  waiting: 0,
  longest_transaction_seconds: 0.4,
};

/**
 * WIRE-SHAPED, and the shape is the assertion. Every int64 field here is
 * a bare JSON NUMBER, because that is what the server sends: the client
 * turns it into an exact decimal string only for names registered in
 * `api/int64.ts`'s INT64_FIELDS, and leaves everything else a `number`.
 *
 * This fixture used to hand the page pre-stringified values, which is a
 * runtime the page never sees — and it hid a real bug: six of the tables
 * list's seven numeric columns were not registered, so `int64Column`
 * (which requires `typeof v === "string"`) marked every row absent and
 * those headers sorted nothing while showing an arrow. Found in review
 * of #240, which added the `toast` column and inherited it. With numbers
 * here, that failure is visible to `sorts by total bytes…` below.
 */
const table = {
  name: "hog_data_file",
  live_tuples: 2000000,
  dead_tuples: 800000,
  dead_ratio: 0.2857,
  table_bytes: 4294967296,
  index_bytes: 1073741824,
  toast_bytes: 0,
  total_bytes: 5368709120,
  seq_scans: 2,
  index_scans: 5000000,
  last_autovacuum: "2026-09-17T12:00:00Z",
  autovacuum_count: 40,
};

const indexes = [
  {
    table: "hog_data_file",
    name: "hog_data_file_live",
    size_bytes: "536870912",
    scans: "5000000",
    constraint_backing: false,
  },
  {
    table: "hog_catalog",
    name: "hog_catalog_pkey",
    size_bytes: "8192",
    scans: "0",
    constraint_backing: true,
  },
];

function health(overrides: Record<string, unknown> = {}) {
  return {
    server,
    activity,
    tables: [table],
    indexes,
    findings: [],
    commit_locks: [],
    replication_slots: [],
    blind_spots: [
      "Disk fullness is invisible from SQL on a managed instance (RDS).",
      "Orphaned catalog rows are not counted here; maintenance owns that check.",
    ],
    ...overrides,
  };
}

function mount(body: Record<string, unknown>) {
  mockFetch((url) => {
    if (url === "/v1/database/health") return jsonResponse(body);
    if (url === "/v1/info") return jsonResponse({ name: "gigahog-dev" });
    if (url === "/v1/catalogs") return jsonResponse([]);
    return undefined;
  });
  renderApp("/database");
}

describe("database page", () => {
  it("says who closes a finding: maintenance with its task, or an operator", async () => {
    mount(
      health({
        findings: [
          {
            severity: "warn",
            code: "index_bloat",
            title: "12 index(es) are over the bloat threshold",
            detail: "hog_data_file.hog_data_file_path: 8.0 GiB.",
            hoglake_impact: "Autovacuum never shrinks an index.",
            resolution: {
              kind: "maintenance",
              task: "reindex",
              text: "The reindex task rebuilds the largest once a day at 03:00 UTC.",
            },
          },
          {
            severity: "critical",
            code: "prepared_transactions",
            title: "1 orphaned prepared transaction(s)",
            detail: "Oldest prepared 2 h ago.",
            hoglake_impact: "A prepared transaction pins the vacuum horizon.",
            resolution: {
              kind: "operator",
              text: "COMMIT PREPARED or ROLLBACK PREPARED each one by hand.",
            },
          },
        ],
      }),
    );
    const bloat = (await screen.findByText("12 index(es) are over the bloat threshold")).closest("li")!;
    const maint = within(bloat).getByText("reindex task handles it");
    expect(maint).toHaveClass("badge", "resolution-maintenance");
    expect(within(bloat).getByText(/once a day at 03:00 UTC/)).toBeInTheDocument();
    const prepared = screen.getByText("1 orphaned prepared transaction(s)").closest("li")!;
    expect(within(prepared).getByText("operator")).toHaveClass("resolution-operator");
    expect(within(prepared).getByText(/ROLLBACK PREPARED each one by hand/)).toBeInTheDocument();
  });

  it("renders a finding from a server that predates resolutions", async () => {
    mount(
      health({
        findings: [
          {
            severity: "warn",
            code: "deadlocks",
            title: "3 deadlock(s) recorded",
            detail: "Cumulative since the last statistics reset.",
            hoglake_impact: "Something took locks outside the discipline.",
          } as never,
        ],
      }),
    );
    expect(await screen.findByText("3 deadlock(s) recorded")).toBeInTheDocument();
    expect(screen.getByText(/outside the discipline/)).toBeInTheDocument();
    expect(screen.queryByText(/no action|operator|task handles it/)).not.toBeInTheDocument();
  });

  it("shows a finding with both the measurement and the hoglake reading", async () => {
    mount(
      health({
        findings: [
          {
            severity: "critical",
            code: "dead_tuples",
            title: "hog_data_file is 29% dead tuples",
            detail: "800000 dead against 2000000 live.",
            hoglake_impact:
              "The live-file index is partial WHERE end_snapshot IS NULL.",
            resolution: {
              kind: "watch",
              text: "Autovacuum reclaims the heap side when the table crosses its threshold.",
            },
          },
        ],
      }),
    );

    expect(
      await screen.findByText("hog_data_file is 29% dead tuples"),
    ).toBeInTheDocument();
    // The third part: what happens next, and by whom.
    expect(screen.getByText("no action")).toHaveClass("resolution-watch");
    expect(screen.getByText(/Autovacuum reclaims the heap side/)).toBeInTheDocument();
    // Both halves render: the measurement alone is a generic dashboard.
    expect(screen.getByText(/800000 dead against/)).toBeInTheDocument();
    expect(
      screen.getByText(/partial WHERE end_snapshot IS NULL/),
    ).toBeInTheDocument();
    expect(screen.getByText("critical")).toBeInTheDocument();
  });

  /**
   * The column #240 needed and the page did not have.
   *
   * hog_commit_receipt was 58.2 GiB in production, 58.1 of it TOAST, and
   * this list showed it as 61.8 MiB for a year — because it carried heap
   * and indexes, and TOAST is neither. The column is the fix; the default
   * sort on `total` is what makes the size the page is ALREADY ordered by
   * (the server's own ORDER BY) legible in the header.
   */
  it("shows TOAST and total bytes, with the table sorted by total", async () => {
    // THREE TABLES IN A DELIBERATE PROP ORDER, because
    // `applySort(tables, …)` always sorts the PROP rather than the
    // previous render's output: with two rows, or with an order that
    // happens to match, every ordering assertion below is satisfied by
    // the server order alone and asserts nothing. Prop order is
    // [file, receipt, upload]; total-descending is [receipt, file,
    // upload]; toast-ascending is [file, upload, receipt]. No two of
    // the three agree.
    mount(
      health({
        tables: [
          { ...table, name: "hog_data_file" },
          {
            // #240's numbers: a table that is almost entirely TOAST.
            ...table,
            name: "hog_commit_receipt",
            table_bytes: 64800000,
            index_bytes: 12000000,
            toast_bytes: 62400000000,
            total_bytes: 62476800000,
          },
          {
            ...table,
            name: "hog_upload",
            table_bytes: 1048576,
            index_bytes: 524288,
            toast_bytes: 8192,
            total_bytes: 1581056,
          },
        ],
      }),
    );
    expect(await screen.findByText("hog_commit_receipt")).toBeInTheDocument();
    const receiptRow = screen.getByText("hog_commit_receipt").closest("tr")!;
    const cells = within(receiptRow).getAllByRole("cell").map((c) => c.textContent);
    // The TOAST column is the one that makes the 58 GiB visible at all,
    // and the total is the one that makes it comparable.
    expect(cells).toContain("58.1 GiB");
    expect(cells).toContain("58.2 GiB");

    // Scoped to THIS table: the page renders an index list below whose
    // first column is also a hog_* name, so an unscoped row query would
    // be asserting about whichever table happened to come first.
    const tables = receiptRow.closest("table")!;
    const order = () =>
      within(tables)
        .getAllByRole("row")
        .slice(1)
        .map((r) => within(r).getAllByRole("cell")[0]?.textContent);

    // The table OPENS sorted on total, descending: the header says which
    // column the order is on, and it agrees with the server's.
    const total = within(tables).getByRole("button", { name: /^total/ }).closest("th")!;
    expect(total.getAttribute("aria-sort")).toBe("descending");
    expect(order()).toEqual(["hog_commit_receipt", "hog_data_file", "hog_upload"]);

    // And TOAST is explained rather than left as a word: an operator who
    // has never met out-of-line storage needs the tooltip to know why a
    // 61.8 MiB table costs 58 GiB.
    const toast = within(tables).getByRole("button", { name: /^toast/ });
    expect(toast.getAttribute("title")).toMatch(/jsonb and text/);

    // THE COLUMN ALSO HAS TO SORT, and it only does because
    // "toast_bytes" is registered in `api/int64.ts`'s INT64_FIELDS. The
    // fixture's values are wire-shaped NUMBERS; unregistered, they stay
    // numbers, `int64Column`'s `isDecimalInt` marks every row absent,
    // `applySort` returns the prop order, and the click reorders nothing
    // while the header shows an arrow. Two clicks = ascending.
    await userEvent.click(toast);
    await userEvent.click(toast);
    expect(toast.closest("th")!.getAttribute("aria-sort")).toBe("ascending");
    expect(order()).toEqual(["hog_data_file", "hog_upload", "hog_commit_receipt"]);
  });

  it("says so plainly when there is nothing to report", async () => {
    mount(health());
    expect(await screen.findByText(/No findings/)).toBeInTheDocument();
  });

  it("renders instance and activity statistics", async () => {
    mount(health());
    expect(await screen.findByText("12 / 100")).toBeInTheDocument();
    expect(screen.getByText("99%")).toBeInTheDocument(); // cache hit
    expect(screen.getByText("on")).toBeInTheDocument(); // autovacuum
  });

  it("marks a constraint-backing index so it does not read as dead weight", async () => {
    mount(health());
    expect(await screen.findByText("hog_catalog_pkey")).toBeInTheDocument();
    // The pkey has zero scans but is doing a job; the label is what stops
    // an operator "cleaning it up".
    expect(screen.getByText("constraint")).toBeInTheDocument();
  });

  it("renders an absent cache ratio as unknown, not as zero", async () => {
    // Before any block has been read the server omits the field; showing
    // 0% would read as a catastrophically cold cache.
    const { cache_hit_ratio: _omitted, ...withoutRatio } = server;
    mount(health({ server: withoutRatio }));
    // Scoped to the cache stat: unknown durations render an em dash too,
    // so a bare text match would pass without proving anything.
    const label = await screen.findByText("cache hit");
    expect(label.parentElement?.textContent).toContain("—");
    expect(label.parentElement?.textContent).not.toContain("0%");
  });

  it("shows a held commit lock with its catalog and queue depth", async () => {
    mount(
      health({
        commit_locks: [
          {
            catalog_id: "42",
            catalog: "gigahog-ev",
            pid: 4711,
            granted: true,
            held_seconds: 31.5,
            waiters: 7,
          },
        ],
      }),
    );
    // The catalog is what makes this actionable — "some advisory lock" is
    // not something an operator can chase.
    expect(await screen.findByText("gigahog-ev")).toBeInTheDocument();
    expect(screen.getByText("4711")).toBeInTheDocument();
    expect(screen.getByText("7")).toBeInTheDocument();
  });

  it("flags an inactive replication slot and the WAL it pins", async () => {
    mount(
      health({
        replication_slots: [
          {
            name: "bg_upgrade_slot",
            slot_type: "logical",
            active: false,
            retained_wal_bytes: "8589934592",
          },
        ],
      }),
    );
    expect(await screen.findByText("bg_upgrade_slot")).toBeInTheDocument();
    expect(screen.getByText("inactive")).toBeInTheDocument();
    expect(screen.getByText("8.0 GiB")).toBeInTheDocument();
  });

  it("always states what it cannot see", async () => {
    // Present even when every finding is clear, so a green page is never
    // read as "the volume is fine".
    mount(health());
    expect(await screen.findByText(/Disk fullness is invisible/)).toBeInTheDocument();
    expect(screen.getByText(/Orphaned catalog rows/)).toBeInTheDocument();
  });

  it("renders absent checkpoint counters as unknown", async () => {
    // PG 17 moved the columns; the tile must degrade, not show zeros.
    const { checkpoints_timed: _t, checkpoints_requested: _r, ...older } = server;
    mount(health({ server: older }));
    const label = await screen.findByText("checkpoints (req/timed)");
    expect(label.parentElement?.textContent).toContain("—");
  });

  it("surfaces a failing endpoint rather than rendering an empty page", async () => {
    mockFetch((url) => {
      if (url === "/v1/database/health")
        return jsonResponse({ error: "boom", detail: "no" }, 500);
      if (url === "/v1/catalogs") return jsonResponse([]);
      return undefined;
    });
    renderApp("/database");
    expect(await screen.findByText(/boom|Error|failed/i)).toBeInTheDocument();
  });
});
