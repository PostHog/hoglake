import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import {
  instanceMaintenanceStatusFixture,
  maintenanceRunPageFixture,
} from "./fixtures";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const statusUrl = "/v1/maintenance/status";
const runsUrl = "/v1/maintenance/runs?limit=20";

function mockCentral(
  overrides: { status?: unknown; statusCode?: number; runs?: unknown } = {},
) {
  return mockFetch((url) => {
    if (url === statusUrl) {
      return jsonResponse(
        overrides.status ?? instanceMaintenanceStatusFixture,
        overrides.statusCode ?? 200,
      );
    }
    if (url === runsUrl) {
      return jsonResponse(overrides.runs ?? maintenanceRunPageFixture);
    }
    return undefined;
  });
}

describe("CentralMaintenancePage", () => {
  it("renders a warming summary as unknown and pages catalog summaries", async () => {
    const first = {
      ...instanceMaintenanceStatusFixture.catalogs[0],
      tasks: instanceMaintenanceStatusFixture.catalogs[0].tasks.map((task) => ({
        ...task,
        backlog: task.task === "expiry" ? task.backlog : task.task === "compaction" ? { target_bytes: "536870912" } : {},
      })),
    };
    const second = { ...instanceMaintenanceStatusFixture.catalogs[1], sampled_at: "2026-09-12T04:00:00Z" };
    const fetch = mockFetch((url) => {
      if (url === statusUrl) return jsonResponse({ catalogs: [first], has_more: true, next_after: first.catalog });
      if (url === `${statusUrl}?after=${first.catalog}`) return jsonResponse({ catalogs: [second], has_more: false });
      if (url === runsUrl) return jsonResponse({ runs: [], has_more: false });
      return undefined;
    });
    renderApp("/maintenance");
    expect(await screen.findByText("Summary warming up")).toBeInTheDocument();
    expect(screen.getByText("— pending, — failed")).not.toHaveClass("backlog-bad");
    expect(screen.getByText("— queued")).not.toHaveClass("backlog-bad");
    await userEvent.setup().click(screen.getByRole("button", { name: "Load more catalogs" }));
    expect(await screen.findByText("Sampled 2026-09-12 04:00:00Z")).toBeInTheDocument();
    expect(fetch).toHaveBeenCalledWith(`${statusUrl}?after=${first.catalog}`, expect.anything());
    expect(screen.queryByRole("button", { name: "Load more catalogs" })).not.toBeInTheDocument();
  });

  it("renders the catalog x task matrix with cadences and backlog numbers", async () => {
    mockCentral();
    renderApp("/maintenance");

    // Wait for the matrix to land (the runs table renders immediately).
    const scratchLink = await screen.findByRole("link", { name: "scratch" });

    // Column headers carry the instance-wide cadence.
    const headerRow = screen.getAllByRole("row")[0];
    expect(within(headerRow).getByText(/every 5s/)).toBeInTheDocument();
    expect(within(headerRow).getByText(/manual/)).toBeInTheDocument(); // verify
    expect(within(headerRow).getByText(/disabled/)).toBeInTheDocument(); // compaction

    // analytics row: last-run badges + backlog numbers from the fixture.
    const matrix = screen.getAllByRole("table")[0];
    const analyticsRow = within(matrix)
      .getAllByRole("link", { name: "analytics" })[0]
      .closest("tr")!;
    expect(within(analyticsRow).getByText("4 pending, 1 failed")).toBeInTheDocument();
    expect(within(analyticsRow).getByText("113 snapshots kept")).toBeInTheDocument();
    expect(within(analyticsRow).getByText("0 queued")).toBeInTheDocument();
    expect(within(analyticsRow).getByText("42 small files")).toBeInTheDocument();

    // scratch row: never ran anything -> the never-ran dash badge.
    const scratchRow = scratchLink.closest("tr")!;
    expect(within(scratchRow).getAllByText("—").length).toBeGreaterThan(0);
    expect(within(scratchRow).getByText("retention off")).toBeInTheDocument();

    // Cells link into the per-catalog maintenance pages.
    const cellLinks = within(analyticsRow).getAllByRole("link");
    expect(
      cellLinks.some(
        (l) => l.getAttribute("href") === "/catalogs/analytics/maintenance",
      ),
    ).toBe(true);
  });

  it("renders the instance-wide run feed with catalog links", async () => {
    mockCentral();
    renderApp("/maintenance");

    // The runs table (the one with an outcome column) carries a catalog
    // column here — the per-catalog page's table doesn't.
    await screen.findByText("104");
    const table = screen
      .getAllByRole("table")
      .find((t) => within(t).queryByText("outcome"))!;
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows).toHaveLength(6);
    expect(within(rows[0]).getByText("104")).toBeInTheDocument();
    expect(within(rows[1]).getByText("cleanup")).toBeInTheDocument();
    const catalogLinks = within(rows[1]).getByRole("link", { name: "analytics" });
    expect(catalogLinks).toHaveAttribute(
      "href",
      "/catalogs/analytics/maintenance",
    );
  });

  it("shows one notice on a server that predates the ledger", async () => {
    const fetchMock = mockFetch((url) => {
      if (url === statusUrl) return new Response("Not Found", { status: 404 });
      return undefined;
    });
    renderApp("/maintenance");

    const notice = await screen.findByRole("status");
    expect(notice).toHaveTextContent("predates the maintenance ledger");
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(
      fetchMock.mock.calls.some(([input]) => String(input).includes("/runs")),
    ).toBe(false);
  });

  it("surfaces other API errors inline", async () => {
    mockCentral({
      status: { error: "internal_error", detail: "internal server error" },
      statusCode: 500,
    });
    renderApp("/maintenance");

    const alert = (await screen.findAllByRole("alert"))[0];
    expect(alert).toHaveTextContent("internal_error");
  });

  it("renders an empty state when the instance has no catalogs", async () => {
    mockCentral({ status: { catalogs: [] }, runs: { runs: [], has_more: false } });
    renderApp("/maintenance");

    expect(await screen.findByText("No catalogs yet.")).toBeInTheDocument();
    expect(await screen.findByText("No runs recorded yet.")).toBeInTheDocument();
  });
});
