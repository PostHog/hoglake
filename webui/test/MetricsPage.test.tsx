import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { mockFetch, renderApp } from "./helpers";
import { plots } from "./uplotMock";

function textResponse(body: string, status = 200): Response {
  return new Response(body, {
    status,
    headers: { "Content-Type": "text/plain; version=0.0.4" },
  });
}

const exposition = [
  "# HELP hoglake_commits_total Commits by result",
  "# TYPE hoglake_commits_total counter",
  'hoglake_commits_total{catalog="analytics",result="committed"} 34.0',
  'hoglake_commits_total{catalog="analytics",result="conflict"} 1.0',
  "# HELP hoglake_table_count Live tables",
  "# TYPE hoglake_table_count gauge",
  "hoglake_table_count 42.0",
  "# TYPE hoglake_stats_pending_files gauge", // HELP-less family
  "hoglake_stats_pending_files 7.0",
  "# HELP hoglake_commit_lock_wait_seconds Lock wait",
  "# TYPE hoglake_commit_lock_wait_seconds histogram",
  'hoglake_commit_lock_wait_seconds_bucket{le="0.001"} 10',
  'hoglake_commit_lock_wait_seconds_bucket{le="0.01"} 30',
  'hoglake_commit_lock_wait_seconds_bucket{le="+Inf"} 40',
  "hoglake_commit_lock_wait_seconds_count 40",
  "hoglake_commit_lock_wait_seconds_sum 0.2",
  "# HELP jvm_memory_used_bytes Used heap",
  "# TYPE jvm_memory_used_bytes gauge",
  'jvm_memory_used_bytes{area="heap"} 1073741824.0',
  "# TYPE http_seconds summary",
  'http_seconds{route="/v1",quantile="0.5"} 0.01',
  'http_seconds{route="/v1",quantile="0.99"} 0.04',
  "",
].join("\n");

const servedMetrics = (body: string) => (url: string) =>
  url === "/metrics" ? textResponse(body) : undefined;

describe("MetricsPage", () => {
  beforeEach(() => {
    window.localStorage.clear();
    plots.length = 0;
  });

  it("renders labeled families as bars, scalars as stat tiles, others collapsed", async () => {
    mockFetch(servedMetrics(exposition));
    renderApp("/metrics");

    // Labeled counter family → its series behind a closed disclosure;
    // open it for horizontal bars with chips + exact values.
    const commits = (
      await screen.findByText("hoglake_commits_total")
    ).closest("section")!;
    expect(within(commits).queryByText("result=conflict")).not.toBeInTheDocument();
    await userEvent.setup().click(within(commits).getByRole("button", { name: "show 2 series" }));
    expect(
      within(commits).getAllByText("catalog=analytics").length,
    ).toBeGreaterThan(0);
    expect(within(commits).getByText("result=conflict")).toBeInTheDocument();
    expect(within(commits).getByText("34")).toBeInTheDocument();
    expect(within(commits).getByText("1")).toBeInTheDocument();
    expect(within(commits).getByText("Commits by result")).toBeInTheDocument();

    // Scalar gauge → stat tile with the big value.
    const tile = screen.getByText("hoglake_table_count").closest("section")!;
    expect(within(tile).getByText("42")).toBeInTheDocument();
    expect(tile.className).toContain("stat-tile");
    expect(within(tile).getByText("Live tables")).toBeInTheDocument();

    // HELP-less family still renders.
    expect(screen.getByText("hoglake_stats_pending_files")).toBeInTheDocument();

    // Non-hoglake families are collapsed under "everything else".
    const rest = screen.getByText("everything else (2 families)");
    const details = rest.closest("details")!;
    const jvm = within(details).getByText("jvm_memory_used_bytes").closest("section")!;
    await userEvent.setup().click(within(jvm).getByRole("button", { name: "show 1 series" }));
    expect(within(jvm).getByText("1.0 GiB")).toBeInTheDocument();
  });

  it("renders histogram buckets as a strip with count and mean", async () => {
    mockFetch(servedMetrics(exposition));
    renderApp("/metrics");

    const hist = (
      await screen.findByText("hoglake_commit_lock_wait_seconds")
    ).closest("section")!;
    await userEvent.setup().click(within(hist).getByRole("button", { name: "show 1 series" }));
    const strip = within(hist).getByTestId("bucket-strip");
    // Cumulative 10/30/40 → three per-bucket bars (10, 20, 10).
    expect(strip.children).toHaveLength(3);
    expect(strip.children[1]).toHaveAttribute("title", "≤ 0.01: 20");
    expect(within(hist).getByText("40")).toBeInTheDocument(); // count
    expect(within(hist).getByText("5.00 ms")).toBeInTheDocument(); // mean 0.2/40
  });

  it("renders summary quantiles as labeled bars", async () => {
    mockFetch(servedMetrics(exposition));
    renderApp("/metrics");

    await screen.findByText("hoglake_commits_total");
    const details = screen
      .getByText("everything else (2 families)")
      .closest("details")!;
    const summary = within(details).getByText("http_seconds").closest("section")!;
    await userEvent.setup().click(within(summary).getByRole("button", { name: "show 1 series" }));
    expect(within(summary).getByText("q=0.5")).toBeInTheDocument();
    expect(within(summary).getByText("q=0.99")).toBeInTheDocument();
    expect(within(summary).getByText("40.00 ms")).toBeInTheDocument();
    expect(within(summary).getByText("route=/v1", { exact: false })).toBeInTheDocument();
  });

  it("filters families by name", async () => {
    mockFetch(servedMetrics(exposition));
    renderApp("/metrics");
    await screen.findByText("hoglake_commits_total");

    const user = userEvent.setup();
    await user.type(screen.getByLabelText("filter metrics"), "commits");

    expect(screen.getByText("hoglake_commits_total")).toBeInTheDocument();
    expect(screen.queryByText("hoglake_table_count")).not.toBeInTheDocument();
    expect(screen.queryByText(/everything else/)).not.toBeInTheDocument();
  });

  it("refetches via the Refresh button and counts each snapshot", async () => {
    let calls = 0;
    const fetchMock = mockFetch((url) => {
      if (url !== "/metrics") return undefined;
      calls++;
      return textResponse(
        calls === 1
          ? "# TYPE hoglake_table_count gauge\nhoglake_table_count 42.0\n"
          : "# TYPE hoglake_table_count gauge\nhoglake_table_count 43.0\n",
      );
    });
    renderApp("/metrics");

    expect(await screen.findByText("42")).toBeInTheDocument();
    expect(screen.getByText(/^fetched /)).toBeInTheDocument();

    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Refresh" }));

    expect(await screen.findByText("43")).toBeInTheDocument();
    const metricCalls = fetchMock.mock.calls.filter(
      ([input]) => String(input) === "/metrics",
    );
    expect(metricCalls).toHaveLength(2);
  });

  it("draws a family's history once two snapshots exist: gauge values, counter rates", async () => {
    let calls = 0;
    mockFetch((url) => {
      if (url !== "/metrics") return undefined;
      calls++;
      const n = calls === 1 ? 100 : 160;
      return textResponse(
        [
          "# TYPE hoglake_table_count gauge",
          `hoglake_table_count ${40 + calls}`,
          "# TYPE hoglake_commits_total counter",
          `hoglake_commits_total{result="committed"} ${n}`,
          "",
        ].join("\n"),
      );
    });
    renderApp("/metrics");
    expect(await screen.findByText("41")).toBeInTheDocument();
    // One snapshot: nothing to draw yet.
    expect(screen.queryAllByTestId("timeseries")).toHaveLength(0);
    expect(screen.getByText(/1 sample in this tab.*charts appear with the next sample/)).toBeInTheDocument();

    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh" }));
    expect(await screen.findByText("42")).toBeInTheDocument();
    expect(screen.getByText(/2 samples in this tab, 4 h window/)).toBeInTheDocument();
    expect(screen.getAllByTestId("timeseries")).toHaveLength(2);

    const byLabel = (label: string) =>
      plots.find((p) => p.opts.series.some((s) => s.label === label))!;
    // The gauge chart holds both values; the counter chart one rate,
    // (160-100) over the real elapsed time, so positive and finite.
    const gauge = byLabel("value");
    expect(gauge.data[1]).toEqual([41, 42]);
    const counter = byLabel("result=committed");
    expect(counter.data[0]).toHaveLength(1);
    const rate = counter.data[1][0] as number;
    expect(rate).toBeGreaterThan(0);
    expect(Number.isFinite(rate)).toBe(true);
  });

  it("says so when a sampled histogram has nothing to draw yet", async () => {
    // Identical buckets on both snapshots: no observations in the interval,
    // so no quantile and no line, but the family HAS been sampled twice.
    const hist = [
      "# TYPE hoglake_commit_lock_wait_seconds histogram",
      'hoglake_commit_lock_wait_seconds_bucket{le="1"} 5',
      'hoglake_commit_lock_wait_seconds_bucket{le="+Inf"} 5',
      "hoglake_commit_lock_wait_seconds_count 5",
      "",
    ].join("\n");
    mockFetch(servedMetrics(hist));
    renderApp("/metrics");
    const card = (await screen.findByText("hoglake_commit_lock_wait_seconds")).closest("section")!;
    expect(within(card).queryByText(/no observations/)).not.toBeInTheDocument();
    await userEvent.setup().click(screen.getByRole("button", { name: "Refresh" }));
    await screen.findByText(/2 samples in this tab/);
    expect(within(card).getByText("no observations in the window yet")).toBeInTheDocument();
    expect(within(card).queryByTestId("timeseries")).not.toBeInTheDocument();
  });

  it("polls on the chosen interval and remembers the choice", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      let calls = 0;
      mockFetch((url) => {
        if (url !== "/metrics") return undefined;
        calls++;
        return textResponse(`# TYPE hoglake_table_count gauge\nhoglake_table_count ${calls}\n`);
      });
      renderApp("/metrics");
      expect(await screen.findByText("1")).toBeInTheDocument();
      const select = screen.getByRole("combobox", { name: "poll interval" });
      expect(select).toHaveValue("15");
      await userEvent.setup({ advanceTimers: vi.advanceTimersByTime }).selectOptions(select, "5");
      expect(window.localStorage.getItem("hoglake-metrics-poll")).toBe("5");
      await vi.advanceTimersByTimeAsync(5_500);
      expect(await screen.findByText("2")).toBeInTheDocument();
      await vi.advanceTimersByTimeAsync(5_500);
      expect(await screen.findByText("3")).toBeInTheDocument();
      expect(screen.getByText(/3 samples in this tab, 1.3 h window/)).toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  });

  it("pins a family to the top and keeps the pin across reloads", async () => {
    mockFetch(servedMetrics(exposition));
    const view = renderApp("/metrics");
    await screen.findByText("hoglake_commits_total");
    expect(screen.queryByTestId("metrics-pinned")).not.toBeInTheDocument();

    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "pin hoglake_table_count" }));
    const pinned = screen.getByTestId("metrics-pinned");
    expect(within(pinned).getByText("hoglake_table_count")).toBeInTheDocument();
    expect(within(pinned).queryByText("hoglake_commits_total")).not.toBeInTheDocument();
    expect(JSON.parse(window.localStorage.getItem("hoglake-metrics-pins")!)).toEqual([
      "hoglake_table_count",
    ]);
    // A pinned family stays in view whatever the filter says.
    await user.type(screen.getByLabelText("filter metrics"), "commits");
    expect(within(screen.getByTestId("metrics-pinned")).getByText("hoglake_table_count")).toBeInTheDocument();

    view.unmount();
    renderApp("/metrics");
    await screen.findByText("hoglake_commits_total");
    expect(within(screen.getByTestId("metrics-pinned")).getByText("hoglake_table_count")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "unpin hoglake_table_count" }));
    expect(screen.queryByTestId("metrics-pinned")).not.toBeInTheDocument();
    expect(JSON.parse(window.localStorage.getItem("hoglake-metrics-pins")!)).toEqual([]);
  });

  it("keeps a wide family's series closed until asked, largest first when opened", async () => {
    const lines = ["# TYPE hoglake_table_files gauge"];
    for (let i = 1; i <= 20; i++) lines.push(`hoglake_table_files{table="t${i}"} ${i * 10}`);
    mockFetch(servedMetrics(lines.join("\n") + "\n"));
    renderApp("/metrics");
    const card = (await screen.findByText("hoglake_table_files")).closest("section")!;
    expect(within(card).queryAllByText(/^table=t\d+$/)).toHaveLength(0);
    const toggle = within(card).getByRole("button", { name: "show 20 series" });
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    await userEvent.setup().click(toggle);
    const chips = within(card).getAllByText(/^table=t\d+$/);
    expect(chips).toHaveLength(20);
    expect(chips[0]).toHaveTextContent("table=t20");
    expect(within(card).getByRole("button", { name: "hide 20 series" })).toHaveAttribute(
      "aria-expanded",
      "true",
    );
  });

  it("expands one family to a full-size view and closes it with Esc", async () => {
    mockFetch(servedMetrics(exposition));
    renderApp("/metrics");
    await screen.findByText("hoglake_commits_total");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();

    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "expand hoglake_commits_total" }));
    const dialog = screen.getByRole("dialog", { name: "hoglake_commits_total" });
    expect(within(dialog).getByText("Commits by result")).toBeInTheDocument();
    // The expanded card has no expand button of its own, but keeps its pin.
    expect(within(dialog).queryByRole("button", { name: /^expand / })).not.toBeInTheDocument();
    expect(within(dialog).getByRole("button", { name: "pin hoglake_commits_total" })).toBeInTheDocument();

    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "expand hoglake_table_count" }));
    await user.click(screen.getByRole("button", { name: "close hoglake_table_count" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("shows a clear error state when the server is down", async () => {
    mockFetch((url) =>
      url === "/metrics" ? textResponse("boom", 500) : undefined,
    );
    renderApp("/metrics");

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("GET /metrics failed: HTTP 500");
  });
});
