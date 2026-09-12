import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { isLoopDisabled, RunOutcomeBadge, RunsTable } from "../src/components/maintenance";
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
