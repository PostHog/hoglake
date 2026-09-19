import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { catalogsFixture, notFoundError } from "./fixtures";
import { bigIntCatalogsWireBody } from "./fixtures.adversarial";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

describe("CatalogsPage", () => {
  it("lists catalogs with data_path, head snapshot, and schema version", async () => {
    mockFetch((url) =>
      url === "/v1/catalogs" ? jsonResponse(catalogsFixture) : undefined,
    );
    renderApp("/");
    expect(await screen.findByText("analytics")).toBeInTheDocument();
    expect(screen.getByText("s3://hog-lake/analytics")).toBeInTheDocument();
    expect(screen.getByText("4211")).toBeInTheDocument();
    expect(screen.getByText("scratch")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Create catalog" })).toBeInTheDocument();
  });

  it("shows the empty state when no catalogs exist", async () => {
    mockFetch((url) => (url === "/v1/catalogs" ? jsonResponse([]) : undefined));
    renderApp("/");
    expect(await screen.findByText("No catalogs yet.")).toBeInTheDocument();
  });

  it("surfaces the ApiError detail when the list fails", async () => {
    mockFetch((url) =>
      url === "/v1/catalogs" ? jsonResponse(notFoundError, 404) : undefined,
    );
    renderApp("/");
    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("not_found");
    expect(alert).toHaveTextContent("catalog 'nope' does not exist");
    expect(alert).toHaveTextContent("404");
  });
});

describe("catalog totals", () => {
  it("shows tables, rows and size from the sampler", async () => {
    mockFetch((url) =>
      url === "/v1/catalogs" ? jsonResponse(catalogsFixture) : undefined,
    );
    renderApp("/");

    expect(await screen.findByText("analytics")).toBeInTheDocument();
    expect(screen.getByText("42")).toBeInTheDocument();
    expect(screen.getByText("5.0 GiB")).toBeInTheDocument();
  });

  it("renders a row count above 2^53 exactly", async () => {
    // Fed as RAW WIRE TEXT with an unquoted number, because the
    // precision is lost (or kept) in the JSON parse: a fixture holding
    // the string already would pass even with the key missing from
    // INT64_FIELDS, proving nothing. 9007199254740993 is odd, so any
    // trip through a JS number shows up as ...92.
    mockFetch((url) =>
      url === "/v1/catalogs"
        ? new Response(bigIntCatalogsWireBody, {
            status: 200,
            headers: { "Content-Type": "application/json" },
          })
        : undefined,
    );
    renderApp("/");

    expect(await screen.findByText("analytics")).toBeInTheDocument();
    expect(screen.getByText("9,007,199,254,740,993")).toBeInTheDocument();
  });

  it("renders an em dash for a catalog the sampler has not reached", async () => {
    // Absent totals mean "not sampled yet", not "empty" — showing 0
    // would assert the catalog holds nothing, which is a different and
    // possibly false claim.
    mockFetch((url) =>
      url === "/v1/catalogs" ? jsonResponse(catalogsFixture) : undefined,
    );
    renderApp("/");

    const row = (await screen.findByText("scratch")).closest("tr");
    expect(row?.textContent).toContain("—");
    expect(row?.textContent).not.toContain("0 B");
  });

  it("keeps an unsampled catalog out of the top of a largest-first sort", async () => {
    // An em dash is not a size. Sorting by size descending must show the
    // biggest catalog first — a blank that merely compares as "smallest"
    // would flip to the TOP here and read as the largest catalog having
    // no size at all.
    mockFetch((url) =>
      url === "/v1/catalogs" ? jsonResponse(catalogsFixture) : undefined,
    );
    renderApp("/");
    const user = userEvent.setup();
    await screen.findByText("analytics");

    const names = () =>
      screen
        .getAllByRole("row")
        .slice(1)
        .map((r) => r.querySelector("td:first-child")?.textContent ?? "")
        .filter((t) => t !== "");

    const size = screen.getByRole("button", { name: /^size/ });
    await user.click(size);
    expect(names()).toEqual(["analytics", "scratch"]);
    // And still last the other way round, where "smallest first" would
    // otherwise be its natural home.
    await user.click(size);
    expect(names()).toEqual(["analytics", "scratch"]);
  });

  it("renders the oldest snapshot as a live age, em dash until sampled", async () => {
    // Pinned so the fixture's 2026-09-16 instant reads as a clean single
    // unit (3d 5h → "3d"). shouldAdvanceTime lets React Query's async
    // polling still run under fake timers (otherwise findByText never
    // resolves); the clock advances only by the test's real ms.
    vi.useFakeTimers({ shouldAdvanceTime: true });
    vi.setSystemTime(new Date("2026-09-19T05:00:00Z"));
    try {
      mockFetch((url) =>
        url === "/v1/catalogs" ? jsonResponse(catalogsFixture) : undefined,
      );
      renderApp("/");

      const analytics = (await screen.findByText("analytics")).closest("tr")!;
      // The oldest-snapshot cell (second-to-last, before schema_version)
      // shows a single rounded unit — no finer part after it.
      const cells = analytics.querySelectorAll("td");
      const ageCell = cells[cells.length - 2];
      expect(ageCell.textContent).toBe("3d");
      // The exact instant is in that cell's title, for the operator who
      // wants it.
      expect(ageCell.getAttribute("title")).toBe("2026-09-16T00:00:00Z");

      // Unsampled: an unknown age is an em dash, never "0s".
      const scratch = screen.getByText("scratch").closest("tr")!;
      const lastCells = scratch.querySelectorAll("td");
      // second-to-last cell is the oldest-snapshot column
      expect(lastCells[lastCells.length - 2].textContent).toBe("—");
    } finally {
      vi.useRealTimers();
    }
  });

  it("sorts by oldest snapshot: first click surfaces the oldest, unsampled last", async () => {
    mockFetch((url) =>
      url === "/v1/catalogs"
        ? jsonResponse([
            {
              name: "newer",
              data_path: "s3://hog-lake/newer",
              head_snapshot_id: "3",
              schema_version: "1",
              table_count: "1",
              live_rows: "1",
              live_size_bytes: "1",
              oldest_snapshot_time: "2026-09-18T00:00:00Z",
            },
            ...catalogsFixture, // analytics (older) + scratch (unsampled)
          ])
        : undefined,
    );
    renderApp("/");
    const user = userEvent.setup();
    await screen.findByText("newer");

    const names = () =>
      screen
        .getAllByRole("row")
        .slice(1)
        .map((r) => r.querySelector("td:first-child")?.textContent ?? "")
        .filter((t) => t !== "");

    await user.click(screen.getByRole("button", { name: /^oldest_snapshot/ }));
    // analytics (2026-09-16) is older than newer (2026-09-18); scratch
    // has no snapshot time and stays last, not at the "oldest" top.
    expect(names()).toEqual(["analytics", "newer", "scratch"]);
  });

  it("sorts row counts that are the same double, in the order the data says", async () => {
    // "smaller" holds 2^53 and analytics 2^53+1 — ONE apart, and the
    // same double. It is listed FIRST, so a comparator that ties them
    // leaves it first (the sort is stable) and the wrong answer is the
    // one that looks untouched.
    mockFetch((url) =>
      url === "/v1/catalogs"
        ? jsonResponse([
            {
              name: "smaller",
              data_path: "s3://hog-lake/smaller",
              head_snapshot_id: "9",
              schema_version: "1",
              table_count: "1",
              live_rows: "9007199254740992",
              live_size_bytes: "1024",
            },
            ...catalogsFixture,
          ])
        : undefined,
    );
    renderApp("/");
    const user = userEvent.setup();
    await screen.findByText("smaller");

    await user.click(screen.getByRole("button", { name: /^rows/ }));
    const names = screen
      .getAllByRole("row")
      .slice(1)
      .map((r) => r.querySelector("td:first-child")?.textContent ?? "")
      .filter((t) => t !== "");
    // analytics (2^53+1) above smaller (2^53) — a distinction no double
    // can make — and unsampled scratch last regardless.
    expect(names).toEqual(["analytics", "smaller", "scratch"]);
  });
});
