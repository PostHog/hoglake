import { act, fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  isQuietRun,
  loopCadence,
  noRunningLoop,
  RunOutcomeBadge,
  RunsTable,
  RunSummary,
} from "../src/components/maintenance";
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

  describe("loopCadence", () => {
    /**
     * The header's whole job is answering "is this task running?" from
     * the ledger. Each of these is a DIFFERENT state that the old
     * config-derived header collapsed into "disabled" or a cadence it
     * could not know (#114).
     */
    beforeEach(() => {
      vi.useFakeTimers({ shouldAdvanceTime: true });
      vi.setSystemTime(new Date("2026-09-11T10:00:00Z"));
    });
    afterEach(() => {
      vi.useRealTimers();
    });

    it("reports the observed cadence, rounded", () => {
      // A loop sleeps AFTER its body, so a 60s loop with an 8.8s sweep
      // is measured at 68.8s. Rounding it keeps the header readable
      // without claiming a precision the measurement does not have.
      expect(loopCadence({ observed_interval_ms: "68800" })).toBe("every ~69s");
      // "min", never a bare "m": these land in headers CSS upper-cases,
      // where "~61M" beside a table of counts reads as 61 million.
      expect(loopCadence({ observed_interval_ms: "3630000" })).toBe("every ~61min");
      expect(loopCadence({ observed_interval_ms: "450" })).toBe("every ~450ms");
    });

    it("never abbreviates minutes to a bare m, at any minute value", () => {
      // The headers are upper-cased by CSS, so "M" is what an operator
      // actually reads — the SI mega prefix, next to columns of counts.
      // Checked across the whole minutes band rather than at one value,
      // since the unit is chosen by a threshold.
      for (const ms of ["120000", "600000", "3600000", "7139000"]) {
        const text = loopCadence({ observed_interval_ms: ms })!;
        expect(text).toMatch(/\d+min\b/);
        expect(text.toUpperCase()).not.toMatch(/\d+M\b/);
      }
    });

    it("falls back to the last run when no cadence can be derived", () => {
      expect(loopCadence({ last_run_at: "2026-09-11T09:58:00Z" })).toBe(
        "last loop run 2min ago",
      );
    });

    it("reads an empty ledger the way the response says to", () => {
      // For a task that records every sweep, no rows means nothing is
      // running it.
      expect(loopCadence({ records_every_sweep: true })).toBe("no loop runs");
      // For the hydrator it means no work arrived here. Its loop may be
      // perfectly healthy, so saying "no loop runs" would be the bug
      // this change exists to remove, moved to a new place.
      expect(loopCadence({ records_every_sweep: false })).toBe("nothing to do here");
    });

    it("distinguishes a task with no loop from a server that did not answer", () => {
      // null = verify, which has no loop at all. undefined = an older
      // server. Rendering the second as the first would put a claim on
      // screen that the response never made.
      expect(loopCadence(null)).toBe("manual only");
      expect(loopCadence(undefined)).toBeNull();
    });
  });

  describe("noRunningLoop", () => {
    it("is true only on positive evidence that nothing is running", () => {
      expect(noRunningLoop({})).toBe(true);
      expect(noRunningLoop({ last_run_at: "2026-09-11T08:00:00Z" })).toBe(true);
      expect(noRunningLoop({ observed_interval_ms: "60000" })).toBe(false);
      // Neither an absent observation nor a task that has no loop is
      // evidence of an unattended queue, so neither may raise a warning.
      expect(noRunningLoop(undefined)).toBe(false);
      expect(noRunningLoop(null)).toBe(false);
    });
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

/**
 * A mock /maintenance/runs that pages the way the server does: newest first
 * over the ledger the caller supplies (read fresh on every request, so a test
 * can let new runs arrive between polls), honouring `?task`, the exclusive
 * `?before` run_id cursor and `?limit`.
 *
 * Worth the twenty lines: a mock that answers every request with the same
 * rows cannot tell a table that pages from one that filters a single fetched
 * window, which is exactly the bug these tests are about. `seen` collects the
 * request URLs so a test can assert WHERE the filtering happened.
 */
function mockLedger(ledger: () => MaintenanceRun[], seen?: string[]) {
  return mockFetch((url) => {
    if (!url.includes("/maintenance/runs")) return undefined;
    seen?.push(url);
    const q = new URL(url, "http://hoglake.test").searchParams;
    const task = q.get("task");
    const limit = Number(q.get("limit") ?? 50);
    const before = q.get("before");
    let rows = ledger();
    if (task) rows = rows.filter((r) => r.task === task);
    if (before) rows = rows.filter((r) => Number(r.run_id) < Number(before));
    return jsonResponse({
      runs: rows.slice(0, limit),
      has_more: rows.length > limit,
    });
  });
}

function bodyRows(): HTMLElement[] {
  const table = screen.getAllByRole("table")[0];
  return screen.getAllByRole("row").filter((r) => r.closest("tbody") && table.contains(r));
}

const hideToggle = () =>
  screen.getByRole("checkbox", { name: /hide runs that did nothing/i });
const taskSelect = () => screen.getByRole("combobox", { name: /task/i });

function renderTable() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const view = render(
    <QueryClientProvider client={client}>
      <MemoryRouter><RunsTable catalog="analytics" /></MemoryRouter>
    </QueryClientProvider>,
  );
  return { client, view };
}

describe("RunsTable filters", () => {
  beforeEach(() => window.localStorage.clear());
  afterEach(() => vi.useRealTimers());

  it("shows every run by default — the toggle is off until asked", async () => {
    mockLedger(() => feed);
    const { client } = renderTable();
    try {
      await screen.findByText("301");
      expect(bodyRows()).toHaveLength(7);
      expect(hideToggle()).not.toBeChecked();
      expect(screen.queryByText(/quiet runs? hidden/)).not.toBeInTheDocument();
    } finally { client.clear(); }
  });

  it("hides only runs that succeeded AND changed nothing, and says how many", async () => {
    mockLedger(() => feed);
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
    const seen: string[] = [];
    mockLedger(() => feed, seen);
    const { client } = renderTable();
    try {
      await screen.findByText("301");
      const user = userEvent.setup();
      await user.selectOptions(taskSelect(), "compaction");
      // The narrowing is the SERVER's: the dropdown re-queries.
      await screen.findByText("303");
      expect(seen.some((u) => u.includes("task=compaction"))).toBe(true);
      expect(bodyRows()).toHaveLength(3); // 301, 204, 303
      expect(screen.queryByText("302")).not.toBeInTheDocument();

      await user.click(hideToggle());
      expect(bodyRows()).toHaveLength(2); // 301, 303
      expect(screen.getByText("1 quiet run hidden")).toBeInTheDocument();
    } finally { client.clear(); }
  });

  it("says the list is filtered, not empty, when nothing survives", async () => {
    mockLedger(() => [quiet.verify]);
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
    mockLedger(() => runs);
    const { client } = renderTable();
    try {
      await act(async () => { await vi.advanceTimersByTimeAsync(50); });
      fireEvent.change(taskSelect(), { target: { value: "compaction" } });
      fireEvent.click(hideToggle());
      await act(async () => { await vi.advanceTimersByTimeAsync(50); });
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
    mockLedger(() => feed);
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

// ---- the filters against the WHOLE ledger, not one fetched page ----------
//
// #120's filters ran client-side over the newest page the server had already
// returned, so a run that did something dropped out of view the moment enough
// quiet sweeps landed on top of it, and no amount of toggling brought it back:
// the client had never asked the server for it. These tests page a ledger
// deeper than one window on purpose.

/** A quiet compaction sweep with the given run id. */
function quietAt(id: number): MaintenanceRun {
  return { ...quiet.compaction, run_id: String(id) } as MaintenanceRun;
}

/** Newest-first ids `hi` down to `lo`, all quiet. */
function quietRange(hi: number, lo: number): MaintenanceRun[] {
  const out: MaintenanceRun[] = [];
  for (let id = hi; id >= lo; id--) out.push(quietAt(id));
  return out;
}

describe("RunsTable paging under the filters", () => {
  beforeEach(() => window.localStorage.clear());
  afterEach(() => vi.useRealTimers());

  it("reaches a run that did something from beyond the first page", async () => {
    // One compaction that moved files, buried under 30 quiet sweeps — past
    // the 20-run first page, so a client-side filter over that page shows an
    // empty table and cannot do anything about it.
    const interesting = { ...compaction, run_id: "470" } as MaintenanceRun;
    const ledger = [...quietRange(500, 471), interesting, ...quietRange(469, 440)];
    mockLedger(() => ledger);
    const { client } = renderTable();
    try {
      await screen.findByText("500");
      await userEvent.setup().click(hideToggle());
      expect(await screen.findByText("470")).toBeInTheDocument();
      expect(screen.queryByText("No runs match these filters.")).not.toBeInTheDocument();
    } finally { client.clear(); }
  });

  it("does not let newly arrived quiet sweeps evict it again", async () => {
    vi.useFakeTimers();
    const interesting = { ...compaction, run_id: "470" } as MaintenanceRun;
    let ledger = [...quietRange(500, 471), interesting, ...quietRange(469, 440)];
    mockLedger(() => ledger);
    const { client } = renderTable();
    try {
      await act(async () => { await vi.advanceTimersByTimeAsync(50); });
      fireEvent.click(hideToggle());
      await act(async () => { await vi.advanceTimersByTimeAsync(200); });
      expect(screen.getByText("470")).toBeInTheDocument();

      // Two more windows' worth of quiet sweeps land on top of it.
      ledger = [...quietRange(540, 501), ...ledger];
      await act(async () => { await vi.advanceTimersByTimeAsync(5100); });
      await act(async () => { await vi.advanceTimersByTimeAsync(500); });

      expect(screen.getByText("470")).toBeInTheDocument();
    } finally { client.clear(); }
  });

  it("asks the server for the task rather than filtering a fetched page", async () => {
    // Only the newest 20 rows are cleanup; the compaction row the dropdown
    // must find is older than any of them, so a client-side task filter over
    // the first page finds nothing.
    const cleanups = Array.from({ length: 25 }, (_, i) =>
      ({ ...cleanup, run_id: String(600 - i) }) as MaintenanceRun,
    );
    const ledger = [...cleanups, { ...compaction, run_id: "570" } as MaintenanceRun];
    const seen: string[] = [];
    mockLedger(() => ledger, seen);
    const { client } = renderTable();
    try {
      await screen.findByText("600");
      await userEvent.setup().selectOptions(taskSelect(), "compaction");
      expect(await screen.findByText("570")).toBeInTheDocument();
      expect(bodyRows()).toHaveLength(1);
      expect(seen.some((u) => u.includes("task=compaction"))).toBe(true);
    } finally { client.clear(); }
  });

  it("keeps Load more working with both filters on", async () => {
    // 60 compaction runs, every other one loud. Two 20-row pages already
    // satisfy the quiet filter's screenful, so the auto-search settles with
    // a third page still unread — which is what Load more is for.
    const ledger = Array.from({ length: 60 }, (_, i) => {
      const id = 700 - i;
      return i % 2 === 0
        ? ({ ...compaction, run_id: String(id) } as MaintenanceRun)
        : quietAt(id);
    });
    const seen: string[] = [];
    mockLedger(() => ledger, seen);
    const { client } = renderTable();
    try {
      await screen.findByText("700");
      const user = userEvent.setup();
      await user.selectOptions(taskSelect(), "compaction");
      await user.click(hideToggle());
      // Settled on the second page: 20 loud rows, id 662 the oldest.
      await screen.findByText("662");
      const shown = bodyRows().length;
      expect(screen.queryByText("660")).not.toBeInTheDocument();

      await user.click(screen.getByRole("button", { name: "Load more" }));
      expect(await screen.findByText("660")).toBeInTheDocument();
      expect(bodyRows().length).toBeGreaterThan(shown);
      // The cursor page carries the task with it — the two compose.
      expect(seen.some((u) => u.includes("task=compaction") && u.includes("before="))).toBe(true);
    } finally { client.clear(); }
  });

  it("stops after a bounded number of requests when the ledger is all quiet", async () => {
    // A catalog whose ledger is nothing but no-op sweeps: the search for a
    // loud run must give up rather than walk the whole history.
    const seen: string[] = [];
    mockLedger(() => quietRange(9000, 1), seen);
    const { client } = renderTable();
    try {
      await screen.findByText("9000");
      seen.length = 0;
      await userEvent.setup().click(hideToggle());
      await screen.findByText("No runs match these filters.");
      await new Promise((r) => setTimeout(r, 300));
      // It looked past the first window — and then it stopped. Both halves
      // matter: no search at all is the bug, and an unbounded one is worse.
      expect(seen.length).toBeGreaterThan(1);
      expect(seen.length).toBeLessThanOrEqual(10);
      const settled = seen.length;
      await new Promise((r) => setTimeout(r, 300));
      expect(seen.length).toBe(settled);
    } finally { client.clear(); }
  });

  it("scopes the hidden count to what it actually searched", async () => {
    // "20 quiet runs hidden" over an empty table reads as "the filter works";
    // it must instead say how far back the number goes.
    mockLedger(() => quietRange(9000, 1));
    const { client } = renderTable();
    try {
      await screen.findByText("9000");
      await userEvent.setup().click(hideToggle());
      await screen.findByText("No runs match these filters.");
      const readout = screen.getByText(/quiet runs? hidden/);
      expect(readout).toHaveTextContent(/of the newest \d+ runs searched/);
      expect(readout).toHaveTextContent(/load more/i);
    } finally { client.clear(); }
  });
});
