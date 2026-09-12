import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import {
  maintenanceRunPageFixture,
  maintenanceRunsFixture,
  maintenanceStatusFixture,
} from "./fixtures";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const statusUrl = "/v1/catalogs/analytics/maintenance/status";
const runsUrl = "/v1/catalogs/analytics/maintenance/runs?limit=20";

function mockMaintenance(
  overrides: {
    status?: unknown;
    statusCode?: number;
    runs?: unknown;
  } = {},
) {
  return mockFetch((url) => {
    if (url === statusUrl) {
      return jsonResponse(
        overrides.status ?? maintenanceStatusFixture,
        overrides.statusCode ?? 200,
      );
    }
    if (url === runsUrl) {
      return jsonResponse(overrides.runs ?? maintenanceRunPageFixture);
    }
    return undefined;
  });
}

describe("MaintenancePage", () => {
  it("renders every task panel with loop state, backlog, and last run", async () => {
    mockMaintenance();
    renderApp("/catalogs/analytics/maintenance");

    // Wait for the status query to land (the runs table's h3 renders first).
    await screen.findByText("every 5s");

    // All five tasks, in the server's fixed order.
    const panels = screen
      .getAllByRole("heading", { level: 3 })
      .map((h) => h.textContent ?? "")
      .filter((t) => !t.startsWith("Recent runs"));
    expect(panels).toEqual([
      expect.stringContaining("hydrator"),
      expect.stringContaining("expiry"),
      expect.stringContaining("cleanup"),
      expect.stringContaining("compaction"),
      expect.stringContaining("verify"),
    ]);

    // Panels by their heading (the runs table repeats task names in cells).
    const hydrator = screen
      .getByRole("heading", { name: /hydrator/ })
      .closest(".task-panel")!;
    expect(within(hydrator as HTMLElement).getByText("every 5s")).toBeInTheDocument();
    expect(within(hydrator as HTMLElement).getByText("4")).toBeInTheDocument(); // pending
    expect(within(hydrator as HTMLElement).getByText("1")).toBeInTheDocument(); // failed

    const expiry = screen
      .getByRole("heading", { name: /expiry/ })
      .closest(".task-panel")!;
    // 604800s retention = 7d; the floored-by badge from the last run.
    expect(within(expiry as HTMLElement).getByText("7d")).toBeInTheDocument();
    expect(
      within(expiry as HTMLElement).getByText(/floored by hedgerow-events/),
    ).toBeInTheDocument();

    const compaction = screen
      .getByRole("heading", { name: /compaction/ })
      .closest(".task-panel")!;
    expect(
      within(compaction as HTMLElement).getByText("loop disabled"),
    ).toBeInTheDocument();
    expect(within(compaction as HTMLElement).getByText("42")).toBeInTheDocument();
    // The panel links through to the debt page.
    expect(
      within(compaction as HTMLElement).getByRole("link", {
        name: "compaction debt",
      }),
    ).toHaveAttribute("href", "/catalogs/analytics/partitions");

    const verify = screen
      .getByRole("heading", { name: /verify/ })
      .closest(".task-panel")!;
    expect(within(verify as HTMLElement).getByText("manual only")).toBeInTheDocument();
  });

  it("renders the runs table newest-first with outcomes and failures", async () => {
    mockMaintenance();
    renderApp("/catalogs/analytics/maintenance");

    // Wait for the first real row (skeleton rows render 5 while loading).
    await screen.findByText("104");

    const table = screen.getAllByRole("table")[0];
    const rows = within(table).getAllByRole("row").slice(1); // drop header
    expect(rows).toHaveLength(6);

    // Newest first: run 104 (expiry) on top, 99 (failed expiry) last.
    expect(within(rows[0]).getByText("104")).toBeInTheDocument();
    expect(within(rows[0]).getByText("expiry")).toBeInTheDocument();
    expect(
      within(rows[0]).getByText(/expired 12 snapshots, queued 3 files/),
    ).toBeInTheDocument();

    // The failed run shows its error, not a result.
    const last = rows[5];
    expect(within(last).getByText("failed")).toBeInTheDocument();
    expect(
      within(last).getByText("FATAL: connection to server lost"),
    ).toBeInTheDocument();

    // No has_more -> no pager.
    expect(
      screen.queryByRole("button", { name: "Load more" }),
    ).not.toBeInTheDocument();
  });

  it("pages the runs table with the before cursor", async () => {
    const firstPage = {
      runs: maintenanceRunsFixture.slice(0, 2),
      has_more: true,
    };
    const cursor = maintenanceRunsFixture[1].run_id; // "103"
    // The client builds the query string in field order: before, then limit.
    const page2Url = `/v1/catalogs/analytics/maintenance/runs?before=${cursor}&limit=20`;
    const secondPage = {
      runs: maintenanceRunsFixture.slice(2),
      has_more: false,
    };
    const fetchMock = mockFetch((url) => {
      if (url === statusUrl) return jsonResponse(maintenanceStatusFixture);
      if (url === runsUrl) return jsonResponse(firstPage);
      if (url === page2Url) return jsonResponse(secondPage);
      return undefined;
    });
    renderApp("/catalogs/analytics/maintenance");

    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "Load more" }));

    // Page 2's rows landed (run 102 visible), paged via the exclusive cursor.
    expect(await screen.findByText("102")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith(page2Url, expect.anything());
    expect(
      screen.queryByRole("button", { name: "Load more" }),
    ).not.toBeInTheDocument();
  });

  it("surfaces a status-endpoint API error inline", async () => {
    mockMaintenance({
      status: { error: "not_found", detail: "catalog 'nope' does not exist" },
      statusCode: 404,
    });
    renderApp("/catalogs/analytics/maintenance");

    const alert = (await screen.findAllByRole("alert"))[0];
    expect(alert).toHaveTextContent("not_found");
  });

  it("shows one notice, not raw 404s, on a server that predates the ledger", async () => {
    // An old server build has no /maintenance/* routes: Ktor answers its
    // bare unmatched-route 404 (plain text, no ApiError JSON).
    const fetchMock = mockFetch((url) => {
      if (url === statusUrl) return new Response("Not Found", { status: 404 });
      return undefined;
    });
    renderApp("/catalogs/analytics/maintenance");

    const notice = await screen.findByRole("status");
    expect(notice).toHaveTextContent("predates the maintenance ledger");
    // No red error boxes, and the runs query was never fired into the void.
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(
      fetchMock.mock.calls.some(([input]) => String(input).includes("/runs")),
    ).toBe(false);
  });

  it("renders a friendly empty state when no runs are recorded yet", async () => {
    const emptyStatus = {
      ...maintenanceStatusFixture,
      tasks: maintenanceStatusFixture.tasks.map((t) => ({
        ...t,
        last_run: null,
      })),
    };
    mockMaintenance({
      status: emptyStatus,
      runs: { runs: [], has_more: false },
    });
    renderApp("/catalogs/analytics/maintenance");

    await screen.findByText("every 5s");
    expect(screen.getAllByText("No recorded run yet.")).toHaveLength(5);
    expect(
      await screen.findByText("No runs recorded yet."),
    ).toBeInTheDocument();
  });

  it("flags a compaction run with failed groups in red", async () => {
    // The "silently chokes on S3" regression guard, web side: a sweep
    // whose groups failed shows the failure badge, not a green "ok".
    const failingRun = {
      ...maintenanceRunsFixture[3],
      run_id: "110",
      result: { ...maintenanceRunsFixture[3].result!, failed_groups: "2" },
    };
    mockMaintenance({
      runs: { runs: [failingRun], has_more: false },
    });
    renderApp("/catalogs/analytics/maintenance");

    expect(
      await screen.findByText("2 failed (logged; retried next run)"),
    ).toBeInTheDocument();
  });

  it("carries >2^53 ledger values losslessly", async () => {
    // Raw body with a run_id above 2^53 as a bare JSON number, exactly as
    // the server emits it; the reviver must carry it as a decimal string.
    const body =
      '{"runs":[{"run_id":9007199254740993027,"task":"expiry","trigger":"loop",' +
      '"started_at":"2026-09-11T10:00:00Z","finished_at":"2026-09-11T10:00:00.1Z",' +
      '"status":"ok","result":null}],"has_more":false}';
    mockFetch((url) => {
      if (url === statusUrl) return jsonResponse(maintenanceStatusFixture);
      if (url === runsUrl) {
        return new Response(body, {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }
      return undefined;
    });
    renderApp("/catalogs/analytics/maintenance");

    expect(
      await screen.findByText("9007199254740993027"),
    ).toBeInTheDocument();
  });
});
