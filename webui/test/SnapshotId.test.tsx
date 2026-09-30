import type { ReactElement } from "react";
import { act, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SnapshotId } from "../src/components/SnapshotId";
import type { SnapshotPage } from "../src/api/types";
import { jsonResponse, mockFetch } from "./helpers";

// Pinned clock: the tooltip's second line is measured back from "now", so
// the age it prints has to be a fact of the test, not of the hour it runs
// in. shouldAdvanceTime keeps React Query's async plumbing alive under the
// fake clock (a frozen one never resolves a query).
const NOW = new Date("2026-09-29T22:34:07Z");
const COMMIT = "2026-09-29T22:31:07Z"; // 3 min before NOW

/**
 * Past the component's hover-intent delay, which is what opens the
 * tooltip AND arms the lookup: before it, a hover has produced nothing.
 */
async function dwell() {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(200);
  });
}

/** Past the close delay, so a left tooltip is actually gone. */
async function settleClose() {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(150);
  });
}

/** One retained snapshot, as the `after=id-1&limit=1` probe returns it. */
function page(id: string, time = COMMIT): SnapshotPage {
  return {
    snapshots: [{ snapshot_id: id, snapshot_time: time, schema_version: "7" }],
    has_more: true,
  };
}

/**
 * The component alone, with its own query client. No router: a snapshot id
 * is not a link, so nothing here needs one.
 */
function renderIds(ui: ReactElement) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}

const probe = "/v1/catalogs/analytics/snapshots?after=480820&limit=1";

/** The rendered id itself — the element the hover is aimed at. */
const trigger = (id = "480821") =>
  screen.getByText(id).closest("[data-snapshot-id]") as HTMLElement;

/** The wrapper, which is what carries the hover handlers. */
const hoverTarget = (id = "480821") =>
  trigger(id).closest(".snapshot-id") as HTMLElement;

function setup() {
  return userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
}

beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true });
  vi.setSystemTime(NOW);
});
afterEach(() => {
  vi.useRealTimers();
});

describe("SnapshotId", () => {
  it("fetches nothing on render — a listing can hold hundreds of ids", async () => {
    const fetchMock = mockFetch(() => undefined);
    renderIds(<SnapshotId catalog="analytics" id="480821" />);

    expect(screen.getByText("480821")).toBeInTheDocument();
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("dates the id on hover: UTC instant plus a relative age", async () => {
    const fetchMock = mockFetch((url) =>
      url === probe ? jsonResponse(page("480821")) : undefined,
    );
    const user = setup();
    renderIds(<SnapshotId catalog="analytics" id="480821" />);

    await user.hover(hoverTarget());
    await dwell();

    const tip = await screen.findByRole("tooltip");
    // Two lines: the fact, then the reading of it.
    expect(tip).toHaveTextContent("2026-09-29 22:31:07Z");
    expect(tip).toHaveTextContent("3 min ago");
    // The lookup is the cheapest form of the listing endpoint: one row,
    // starting just below the id, since `after` is exclusive.
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith(probe, expect.anything());
    // The tooltip is wired to the id for a screen reader.
    expect(trigger().getAttribute("aria-describedby")).toBe(tip.id);
    // No native title: the custom tooltip is the only one. Two tips
    // stacked on one hover is what the browser would otherwise do, and AT
    // would announce the instant twice.
    expect(trigger().getAttribute("title")).toBeNull();
  });

  it("neither fetches nor opens anything for a hover that never settles", async () => {
    // Sweeping the pointer down a column crosses five ids and stops on
    // none of them. Two things must not happen: five requests (the cache
    // dedupes repeats of ONE id and can do nothing about five different
    // ones), and five open boxes trailing the cursor, each claiming a
    // lookup is in flight when none is.
    const fetchMock = mockFetch(() => undefined);
    const user = setup();
    const ids = ["480821", "480822", "480823", "480824", "480825"];
    renderIds(
      <>
        {ids.map((id) => (
          <SnapshotId key={id} catalog="analytics" id={id} />
        ))}
      </>,
    );

    for (const id of ids) await user.hover(hoverTarget(id));

    expect(screen.queryAllByRole("tooltip")).toHaveLength(0);
    expect(fetchMock).not.toHaveBeenCalled();

    // Stopping on the last one opens exactly that one.
    await dwell();
    const tips = screen.getAllByRole("tooltip");
    expect(tips).toHaveLength(1);
    expect(tips[0].closest(".snapshot-id")).toBe(hoverTarget("480825"));
  });

  it("asks once per id: a second hover is free, a different id is not", async () => {
    const other = "/v1/catalogs/analytics/snapshots?after=480999&limit=1";
    const fetchMock = mockFetch((url) => {
      if (url === probe) return jsonResponse(page("480821"));
      if (url === other) return jsonResponse(page("481000"));
      return undefined;
    });
    const user = setup();
    // The same id twice (one cache entry) plus a THIRD, different id,
    // which must not be answered out of that entry.
    renderIds(
      <>
        <SnapshotId catalog="analytics" id="480821" />
        <SnapshotId catalog="analytics" id="480821" />
        <SnapshotId catalog="analytics" id="481000" />
      </>,
    );

    const [first, second] = screen
      .getAllByText("480821")
      .map((n) => n.closest(".snapshot-id") as HTMLElement);

    // An unhover followed by a hover in the same tick cancels the close
    // and keeps the one box; either way the cache entry is asked once.
    await user.hover(first);
    await dwell();
    await screen.findByRole("tooltip");
    await user.unhover(first);
    await user.hover(first);
    await dwell();
    await screen.findByRole("tooltip");
    await user.unhover(first);
    // The other ROW showing the same id resolves out of the same entry.
    await user.hover(second);
    await dwell();
    expect(await screen.findByRole("tooltip")).toHaveTextContent("3 min ago");
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await user.unhover(second);

    // A different id is its own cache entry and its own request.
    await user.hover(hoverTarget("481000"));
    await dwell();
    await screen.findByRole("tooltip");
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock).toHaveBeenCalledWith(other, expect.anything());
  });

  it("says a snapshot is expired only when the floor has passed it", async () => {
    // The floor is above the id, so the first row the listing can return
    // is a LATER snapshot — the id itself has been expired away.
    const fetchMock = mockFetch((url) =>
      url === probe ? jsonResponse(page("481000")) : undefined,
    );
    const user = setup();
    renderIds(<SnapshotId catalog="analytics" id="480821" />);

    await user.hover(hoverTarget());
    await dwell();

    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "expired: below the catalog's retention floor",
    );
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("says not found — not expired — for an id above head", async () => {
    // An empty page means nothing at or above the id: it is past head, or
    // the catalog has no snapshots. Calling that "expired" would invent a
    // retention event that never happened.
    mockFetch((url) =>
      url === probe
        ? jsonResponse({ snapshots: [], has_more: false })
        : undefined,
    );
    const user = setup();
    renderIds(<SnapshotId catalog="analytics" id="480821" />);

    await user.hover(hoverTarget());
    await dwell();

    const tip = await screen.findByRole("tooltip");
    expect(tip).toHaveTextContent("not found (above head)");
    expect(tip.textContent).not.toContain("expired");
  });

  it("treats id 0 as the never-committed sentinel: no probe, no expiry claim", async () => {
    // V1__init.sql defaults last_snapshot_id and earliest_snapshot_id to
    // 0 and a commit allocates last+1, so 0 is not an id — it is what a
    // fresh catalog's head and an un-expired catalog's floor both read.
    // Probing it would return snapshot 1 and "prove" an expiry that never
    // ran, on the most common state in the fleet.
    const fetchMock = mockFetch(() => undefined);
    const user = setup();
    renderIds(<SnapshotId catalog="analytics" id="0" />);

    expect(screen.getByText("0")).toBeInTheDocument();
    await user.hover(hoverTarget("0"));
    await dwell();

    const tip = await screen.findByRole("tooltip");
    expect(tip).toHaveTextContent("no snapshot yet");
    expect(tip.textContent).not.toContain("expired");
    expect(tip.textContent).not.toContain("retention");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("treats a leading-zero id as the sentinel too", async () => {
    // `?snapshot=000` passes isInt64String, so it reaches the badge. A
    // textual compare would miss it, put `after=-1` on the wire and claim
    // an expiry that never happened.
    const fetchMock = mockFetch(() => undefined);
    const user = setup();
    renderIds(<SnapshotId catalog="analytics" id="000" />);

    await user.hover(hoverTarget("000"));
    await dwell();

    const tip = await screen.findByRole("tooltip");
    expect(tip).toHaveTextContent("no snapshot yet");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("owns up when the lookup fails", async () => {
    mockFetch((url) =>
      url === probe
        ? jsonResponse({ error: "internal", detail: "boom" }, 500)
        : undefined,
    );
    const user = setup();
    renderIds(<SnapshotId catalog="analytics" id="480821" />);

    await user.hover(hoverTarget());
    await dwell();

    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "could not look up snapshot",
    );
  });

  it("skips the lookup entirely when the caller already holds the time", async () => {
    // The snapshot timeline prints snapshot_time in the next column;
    // asking the server for it again would be absurd.
    const fetchMock = mockFetch(() => undefined);
    const user = setup();
    renderIds(
      <SnapshotId catalog="analytics" id="480821" time="2026-09-27T22:34:07Z" />,
    );

    await user.hover(hoverTarget());
    await dwell();

    const tip = await screen.findByRole("tooltip");
    expect(tip).toHaveTextContent("2026-09-27 22:34:07Z");
    expect(tip).toHaveTextContent("2 days ago");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("seeds the cache from a known time, so another id on the page is free", async () => {
    // The catalog page's header id IS the timeline's top row. One
    // setQueryData from the row that already holds the instant spares the
    // header a request for a time rendered a few hundred pixels below.
    const fetchMock = mockFetch(() => undefined);
    const user = setup();
    renderIds(
      <>
        <SnapshotId catalog="analytics" id="480821" time={COMMIT} />
        <SnapshotId catalog="analytics" id="480821" />
      </>,
    );

    const plain = screen
      .getAllByText("480821")
      .map((n) => n.closest(".snapshot-id") as HTMLElement)[1];
    await user.hover(plain);
    await dwell();

    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "2026-09-29 22:31:07Z",
    );
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("is not a tab stop", async () => {
    // 200 tables would otherwise mean 200 tab stops that do nothing but
    // open a tooltip. The commit times are reachable by keyboard on the
    // catalog page's snapshot timeline, which prints them as a column.
    mockFetch(() => undefined);
    const user = setup();
    renderIds(
      <>
        <button type="button">before</button>
        <SnapshotId catalog="analytics" id="480821" />
        <button type="button">after</button>
      </>,
    );

    await user.tab();
    expect(screen.getByRole("button", { name: "before" })).toHaveFocus();
    await user.tab();
    // Focus skips the id entirely.
    expect(screen.getByRole("button", { name: "after" })).toHaveFocus();
    expect(trigger()).not.toHaveAttribute("tabindex");
  });

  it("closes on Escape even though the id never had focus", async () => {
    // The tooltip was opened by the pointer, so a keydown handler on the
    // trigger would never see the key (WCAG 1.4.13 Dismissible).
    mockFetch((url) =>
      url === probe ? jsonResponse(page("480821")) : undefined,
    );
    const user = setup();
    renderIds(<SnapshotId catalog="analytics" id="480821" />);

    await user.hover(hoverTarget());
    await dwell();
    await screen.findByRole("tooltip");
    expect(document.body).toHaveFocus();

    await user.keyboard("{Escape}");
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("stays open while the pointer is inside the tooltip", async () => {
    // Hoverable (WCAG 1.4.13): the tooltip is a DOM child of the wrapper
    // that carries the handlers, so a pointer inside it has not left the
    // id and nothing closes. NOTE: this does NOT exercise CLOSE_DELAY_MS —
    // the gap that delay exists for is the 4px margin between the two
    // boxes, which is geometry jsdom does not have. See the constant.
    mockFetch((url) =>
      url === probe ? jsonResponse(page("480821")) : undefined,
    );
    const user = setup();
    renderIds(<SnapshotId catalog="analytics" id="480821" />);

    await user.hover(hoverTarget());
    await dwell();
    const tip = await screen.findByRole("tooltip");

    await user.hover(tip);
    await settleClose();
    expect(screen.getByRole("tooltip")).toBeInTheDocument();

    // And once the pointer leaves the pair, it closes.
    await user.unhover(hoverTarget());
    await settleClose();
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("anchors the tooltip to the right edge when the id sits in the right half", async () => {
    // A left-anchored nowrap box on the files table's LAST column widens
    // the document and raises a horizontal scrollbar on hover.
    mockFetch((url) =>
      url === probe ? jsonResponse(page("480821")) : undefined,
    );
    const user = setup();
    renderIds(<SnapshotId catalog="analytics" id="480821" />);

    // jsdom lays nothing out, so the trigger's box is stated here.
    vi.spyOn(trigger(), "getBoundingClientRect").mockReturnValue({
      left: window.innerWidth - 60,
      width: 40,
    } as DOMRect);
    await user.hover(hoverTarget());
    await dwell();
    const tip = await screen.findByRole("tooltip");
    expect(tip).toHaveClass("snapshot-tooltip-right");
  });

  it("anchors left when the id sits in the left half", async () => {
    mockFetch((url) =>
      url === probe ? jsonResponse(page("480821")) : undefined,
    );
    const user = setup();
    renderIds(<SnapshotId catalog="analytics" id="480821" />);

    vi.spyOn(trigger(), "getBoundingClientRect").mockReturnValue({
      left: 20,
      width: 40,
    } as DOMRect);
    await user.hover(hoverTarget());
    await dwell();
    const tip = await screen.findByRole("tooltip");
    expect(tip).not.toHaveClass("snapshot-tooltip-right");
  });

  it("shows a placeholder while the lookup is in flight", async () => {
    // The response is withheld until the assertion has run, so the
    // loading line is observed rather than raced past.
    let release: (() => void) | undefined;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        if (String(input) !== probe) throw new Error(`Unhandled ${input}`);
        await held;
        return jsonResponse(page("480821"));
      }),
    );
    const user = setup();
    renderIds(<SnapshotId catalog="analytics" id="480821" />);

    await user.hover(hoverTarget());
    await dwell();
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "looking up snapshot…",
    );

    release!();
    expect(await screen.findByText("3 min ago")).toBeInTheDocument();
  });

  it("keeps an id above 2^53 exact, in the digits and in the probe", async () => {
    // Snapshot ids are int64 decimal strings end-to-end; a Number
    // round-trip here would hover one snapshot and date another.
    const big = "9007199254740993";
    const bigProbe =
      "/v1/catalogs/analytics/snapshots?after=9007199254740992&limit=1";
    const fetchMock = mockFetch((url) =>
      url === bigProbe ? jsonResponse(page(big)) : undefined,
    );
    const user = setup();
    renderIds(<SnapshotId catalog="analytics" id={big} />);

    expect(screen.getByText(big)).toBeInTheDocument();
    await user.hover(hoverTarget(big));
    await dwell();
    await screen.findByRole("tooltip");
    expect(fetchMock).toHaveBeenCalledWith(bigProbe, expect.anything());
  });
});
