import { screen } from "@testing-library/react";
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

const table = {
  name: "hog_data_file",
  live_tuples: "2000000",
  dead_tuples: "800000",
  dead_ratio: 0.2857,
  table_bytes: "4294967296",
  index_bytes: "1073741824",
  toast_bytes: "0",
  total_bytes: "5368709120",
  seq_scans: "2",
  index_scans: "5000000",
  last_autovacuum: "2026-09-17T12:00:00Z",
  autovacuum_count: "40",
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
          },
        ],
      }),
    );

    expect(
      await screen.findByText("hog_data_file is 29% dead tuples"),
    ).toBeInTheDocument();
    // Both halves render: the measurement alone is a generic dashboard.
    expect(screen.getByText(/800000 dead against/)).toBeInTheDocument();
    expect(
      screen.getByText(/partial WHERE end_snapshot IS NULL/),
    ).toBeInTheDocument();
    expect(screen.getByText("critical")).toBeInTheDocument();
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
