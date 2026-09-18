import { act, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { isLoopDisabled, isQuietRun, RunOutcomeBadge, RunsTable, RunSummary } from "../src/components/maintenance";
import type { MaintenanceRun } from "../src/api/types";
import { maintenanceRunsFixture } from "./fixtures";
import { jsonResponse, mockFetch } from "./helpers";

const compaction = maintenanceRunsFixture.find(
  (r): r is Extract<MaintenanceRun, { task: "compaction" }> => r.task === "compaction",
)!;
const cleanup = maintenanceRunsFixture.find(
  (r): r is Extract<MaintenanceRun, { task: "cleanup" }> => r.task === "cleanup",
)!;
const verify = maintenanceRunsFixture.find(
  (r): r is Extract<MaintenanceRun, { task: "verify" }> => r.task === "verify",
)!;
const expiry = maintenanceRunsFixture.find(
  (r): r is Extract<MaintenanceRun, { task: "expiry" }> =>
    r.task === "expiry" && r.status === "ok",
)!;
const hydrator = maintenanceRunsFixture.find(
  (r): r is Extract<MaintenanceRun, { task: "hydrator" }> => r.task === "hydrator",
)!;
const failedRun = maintenanceRunsFixture.find((r) => r.status === "failed")!;

describe("maintenance outcome and history", () => {
  afterEach(() => vi.useRealTimers());

  it.each<MaintenanceRun>([
    { ...compaction, result: { ...compaction.result!, failed_groups: "1" } },
    { ...cleanup, result: { ...cleanup.result!, still_referenced: "1" } },
    { ...verify, result: { ...verify.result!, status: "fail" } },
  ])("flags unsuccessful %s work even when the invocation returned ok", (run) => {
    render(<RunOutcomeBadge run={run} />);
    expect(screen.getByText("issues")).toHaveClass("stats-failed");
    expect(screen.queryByText("ok")).not.toBeInTheDocument();
  });

  it("renders no skip badge for a pre-upgrade compaction row with no invalid_data", () => {
    // A ledger row recorded before the counter existed is handed back
    // as its stored raw JSON. `!== "0"` is true for `undefined`, so the
    // old guard showed the badge on every historical run — reading
    // "invalid-data —", which is worse than silence because it looks
    // like a value nobody could compute.
    const { invalid_data: _dropped, ...older } = compaction.result!;
    render(
      <RunSummary
        run={{ ...compaction, result: older } as MaintenanceRun}
      />,
    );
    expect(screen.queryByText(/invalid-data/)).not.toBeInTheDocument();
    expect(screen.queryByText(/—/)).not.toBeInTheDocument();
  });

  it("renders the skip badge when invalid_data is nonzero", () => {
    render(
      <RunSummary
        run={{ ...compaction, result: { ...compaction.result!, invalid_data: "3" } }}
      />,
    );
    expect(screen.getByText(/invalid-data 3/)).toBeInTheDocument();
  });

  it.each(["0", "-1", "-9223372036854775808"])("treats interval %s as disabled", (interval) => {
    expect(isLoopDisabled(interval)).toBe(true);
    expect(isLoopDisabled("1")).toBe(false);
    expect(isLoopDisabled(undefined)).toBe(false);
  });

  function table() {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <MemoryRouter><RunsTable catalog="analytics" /></MemoryRouter>
      </QueryClientProvider>,
    );
    return client;
  }

  it("refreshes an empty feed when new runs arrive", async () => {
    vi.useFakeTimers();
    let ready = false;
    mockFetch(() => jsonResponse({ runs: ready ? [compaction] : [], has_more: false }));
    const client = table();
    try {
      await act(async () => { await vi.advanceTimersByTimeAsync(50); });
      expect(screen.getByText("No runs recorded yet.")).toBeInTheDocument();
      ready = true;
      await act(async () => { await vi.advanceTimersByTimeAsync(5100); });
      expect(screen.getByText(compaction.run_id)).toBeInTheDocument();
      expect(screen.queryByText("No runs recorded yet.")).not.toBeInTheDocument();
    } finally { client.clear(); }
  });

  it("keeps fetched history visible and retries a failed next page", async () => {
    let attempts = 0;
    mockFetch((url) => {
      if (url.includes("before=")) {
        attempts++;
        if (attempts === 1) return jsonResponse({ error: "unavailable" }, 503);
        return jsonResponse({ runs: [{ ...compaction, run_id: "90" }], has_more: false });
      }
      return jsonResponse({ runs: [compaction], has_more: true });
    });
    const client = table();
    try {
      await screen.findByText(compaction.run_id);
      const user = userEvent.setup();
      await user.click(screen.getByRole("button", { name: "Load more" }));
      await screen.findByRole("alert");
      expect(screen.getByText(compaction.run_id)).toBeInTheDocument();
      await user.click(screen.getByRole("button", { name: "Retry loading more" }));
      expect(await screen.findByText("90")).toBeInTheDocument();
      expect(screen.getByText(compaction.run_id)).toBeInTheDocument();
      expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    } finally { client.clear(); }
  });
});

// ---- the runs table's two filters ----------------------------------------

/** A sweep that ran and found nothing to do, per task. */
const quiet = {
  hydrator: {
    ...hydrator,
    run_id: "201",
    result: { claimed: "0", hydrated: "0", failed: "0", transient: "0" },
  } as MaintenanceRun,
  expiry: {
    ...expiry,
    run_id: "202",
    result: {
      snapshots_expired: "0",
      data_files_queued: "0",
      delete_files_queued: "0",
      // A floor POSITION, not activity: expiry reports where the floor
      // stands every sweep, whether or not it moved.
      new_earliest_snapshot_id: "4099",
    },
  } as MaintenanceRun,
  cleanup: {
    ...cleanup,
    run_id: "203",
    result: { removed: "0", missing: "0", still_referenced: "0" },
  } as MaintenanceRun,
  compaction: {
    ...compaction,
    run_id: "204",
    result: {
      groups_compacted: "0",
      files_in: "0",
      files_out: "0",
      bytes_in: "0",
      bytes_out: "0",
      skipped_conflicts: "0",
      dv_superseded: "0",
      unconvertible_schema: "0",
      invalid_data: "0",
      failed_groups: "0",
    },
  } as MaintenanceRun,
  verify: { ...verify, run_id: "205" } as MaintenanceRun,
};

/**
 * Rebuild a quiet run with one result field overridden. The override is
 * deliberately untyped — several cases set a field that the task's own
 * result type does not declare, which is exactly what a ledger row from a
 * skewed server build looks like on the wire.
 */
function withField(run: MaintenanceRun, field: string, value: unknown): MaintenanceRun {
  return {
    ...run,
    result: { ...(run.result as object), [field]: value },
  } as unknown as MaintenanceRun;
}

describe("isQuietRun", () => {
  it.each(Object.entries(quiet))(
    "calls a %s sweep that changed nothing quiet",
    (_task, run) => {
      expect(isQuietRun(run)).toBe(true);
    },
  );

  // Every counter the server can report per task, enumerated from the
  // OpenAPI result schemas: any one of them moving makes the run loud.
  it.each([
    ["hydrator", "claimed"],
    ["hydrator", "hydrated"],
    ["hydrator", "failed"],
    ["hydrator", "transient"],
    ["expiry", "snapshots_expired"],
    ["expiry", "data_files_queued"],
    ["expiry", "delete_files_queued"],
    ["cleanup", "removed"],
    ["cleanup", "missing"],
    ["cleanup", "still_referenced"],
    ["compaction", "groups_compacted"],
    ["compaction", "files_in"],
    ["compaction", "files_out"],
    ["compaction", "bytes_in"],
    ["compaction", "bytes_out"],
    ["compaction", "skipped_conflicts"],
    ["compaction", "dv_superseded"],
    ["compaction", "unconvertible_schema"],
    ["compaction", "invalid_data"],
    ["compaction", "failed_groups"],
  ] as const)("keeps a %s run whose %s is nonzero", (task, field) => {
    expect(isQuietRun(withField(quiet[task], field, "1"))).toBe(false);
  });

  it("keeps an expiry sweep capped by the consumer floor", () => {
    expect(
      isQuietRun(withField(quiet.expiry, "floored_by_consumer", "hedgerow-events")),
    ).toBe(false);
  });

  it("ignores the expiry floor position, which every sweep reports", () => {
    expect(
      isQuietRun(withField(quiet.expiry, "new_earliest_snapshot_id", "99999")),
    ).toBe(true);
  });

  it("calls a manual rehydrate that requeued nothing quiet, and one that did loud", () => {
    const none = { ...hydrator, trigger: "manual", result: { requeued: "0" } } as MaintenanceRun;
    expect(isQuietRun(none)).toBe(true);
    expect(isQuietRun(withField(none, "requeued", "7"))).toBe(false);
  });

  it("keeps a verify report that failed, or that counted violations", () => {
    expect(isQuietRun(withField(quiet.verify, "status", "fail"))).toBe(false);
    expect(
      isQuietRun(
        withField(quiet.verify, "checks", [
          { check: "row_id_tiling", status: "pass", violations: "3", samples: [] },
        ]),
      ),
    ).toBe(false);
  });

  it("never calls a failed run quiet, whatever its counts say", () => {
    expect(isQuietRun(failedRun)).toBe(false);
    expect(
      isQuietRun({ ...quiet.cleanup, status: "failed", error: "boom" } as MaintenanceRun),
    ).toBe(false);
  });

  it("never calls a run with no result quiet — absence of evidence is not evidence", () => {
    expect(isQuietRun({ ...quiet.cleanup, result: null } as MaintenanceRun)).toBe(false);
  });
});

describe("RunsTable filters", () => {
  // A realistic screenful: two sweeps that did work, three that did not,
  // one outright failure, and one all-zero compaction that skipped a group.
  const busyCompaction = { ...compaction, run_id: "301" } as MaintenanceRun;
  const busyCleanup = { ...cleanup, run_id: "302" } as MaintenanceRun;
  const skippedCompaction = withField(
    { ...quiet.compaction, run_id: "303" } as MaintenanceRun,
    "unconvertible_schema",
    "2",
  );
  const feed: MaintenanceRun[] = [
    busyCompaction,
    quiet.compaction,
    skippedCompaction,
    busyCleanup,
    quiet.cleanup,
    quiet.expiry,
    failedRun,
  ];

  beforeEach(() => window.localStorage.clear());
  afterEach(() => vi.useRealTimers());

  function renderTable(runs: MaintenanceRun[] = feed) {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const view = render(
      <QueryClientProvider client={client}>
        <MemoryRouter><RunsTable catalog="analytics" /></MemoryRouter>
      </QueryClientProvider>,
    );
    return { client, view, runs };
  }

  function bodyRows(): HTMLElement[] {
    const table = screen.getAllByRole("table")[0];
    return screen.getAllByRole("row").filter((r) => r.closest("tbody") && table.contains(r));
  }

  const hideToggle = () =>
    screen.getByRole("checkbox", { name: /hide runs that did nothing/i });
  const taskSelect = () => screen.getByRole("combobox", { name: /task/i });

  it("shows every run by default — the toggle is off until asked", async () => {
    mockFetch(() => jsonResponse({ runs: feed, has_more: false }));
    const { client } = renderTable();
    try {
      await screen.findByText("301");
      expect(bodyRows()).toHaveLength(7);
      expect(hideToggle()).not.toBeChecked();
      expect(screen.queryByText(/quiet runs? hidden/)).not.toBeInTheDocument();
    } finally { client.clear(); }
  });

  it("hides only runs that succeeded AND changed nothing, and says how many", async () => {
    mockFetch(() => jsonResponse({ runs: feed, has_more: false }));
    const { client } = renderTable();
    try {
      await screen.findByText("301");
      await userEvent.setup().click(hideToggle());
      // 3 quiet sweeps gone; the failure and the skip-flagged sweep stay.
      expect(bodyRows()).toHaveLength(4);
      expect(screen.getByText("3 quiet runs hidden")).toBeInTheDocument();
      expect(screen.queryByText("204")).not.toBeInTheDocument();
      expect(screen.getByText("303")).toBeInTheDocument();
      expect(screen.getByText(failedRun.run_id)).toBeInTheDocument();
    } finally { client.clear(); }
  });

  it("narrows to one task, and composes with the quiet filter", async () => {
    mockFetch(() => jsonResponse({ runs: feed, has_more: false }));
    const { client } = renderTable();
    try {
      await screen.findByText("301");
      const user = userEvent.setup();
      await user.selectOptions(taskSelect(), "compaction");
      expect(bodyRows()).toHaveLength(3); // 301, 204, 303
      expect(screen.queryByText("302")).not.toBeInTheDocument();

      await user.click(hideToggle());
      expect(bodyRows()).toHaveLength(2); // 301, 303
      expect(screen.getByText("1 quiet run hidden")).toBeInTheDocument();
    } finally { client.clear(); }
  });

  it("says the list is filtered, not empty, when nothing survives", async () => {
    mockFetch(() => jsonResponse({ runs: [quiet.verify], has_more: false }));
    const { client } = renderTable();
    try {
      await screen.findByText("205");
      await userEvent.setup().click(hideToggle());
      expect(screen.getByText("No runs match these filters.")).toBeInTheDocument();
      expect(screen.queryByText("No runs recorded yet.")).not.toBeInTheDocument();
    } finally { client.clear(); }
  });

  it("keeps the selection, and re-applies it, across a poll refresh", async () => {
    vi.useFakeTimers();
    // fireEvent, not userEvent: userEvent's own waits never resolve under
    // fake timers, and the poll is what this test needs fake timers for.
    let runs = feed;
    mockFetch(() => jsonResponse({ runs, has_more: false }));
    const { client } = renderTable();
    try {
      await act(async () => { await vi.advanceTimersByTimeAsync(50); });
      fireEvent.change(taskSelect(), { target: { value: "compaction" } });
      fireEvent.click(hideToggle());
      expect(bodyRows()).toHaveLength(2);

      // A new quiet compaction sweep lands on the next poll.
      runs = [{ ...quiet.compaction, run_id: "304" } as MaintenanceRun, ...feed];
      await act(async () => { await vi.advanceTimersByTimeAsync(5100); });

      expect(taskSelect()).toHaveValue("compaction");
      expect(hideToggle()).toBeChecked();
      expect(bodyRows()).toHaveLength(2);
      expect(screen.getByText("2 quiet runs hidden")).toBeInTheDocument();
    } finally { client.clear(); }
  });

  it("remembers both choices for the next visit", async () => {
    mockFetch(() => jsonResponse({ runs: feed, has_more: false }));
    const first = renderTable();
    try {
      await screen.findByText("301");
      const user = userEvent.setup();
      await user.click(hideToggle());
      await user.selectOptions(taskSelect(), "cleanup");
    } finally { first.client.clear(); }
    first.view.unmount();

    const second = renderTable();
    try {
      await screen.findByText("302");
      expect(hideToggle()).toBeChecked();
      expect(taskSelect()).toHaveValue("cleanup");
      expect(bodyRows()).toHaveLength(1);
    } finally { second.client.clear(); }
  });
});
