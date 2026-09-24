import { screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import {
  catalogsFixture,
  namespacesFixture,
  snapshotsPage1,
  tableFixture,
  tablesFixture,
} from "./fixtures";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const catBase = "/v1/catalogs/analytics";
const nsBase = `${catBase}/namespaces/events`;

function catalogHandler(url: string): Response | undefined {
  const [path] = url.split("?");
  if (path === catBase) return jsonResponse(catalogsFixture[0]);
  if (path === `${catBase}/namespaces`) return jsonResponse(namespacesFixture);
  if (url === `${catBase}/snapshots?before=4212&limit=50`)
    return jsonResponse(snapshotsPage1);
  if (path === nsBase) return jsonResponse({ tables: tablesFixture });
  if (path === `${nsBase}/tables/pageviews`) return jsonResponse(tableFixture);
  return undefined;
}

describe("breadcrumbs", () => {
  it("shows the linked trail on a catalog page", async () => {
    mockFetch(catalogHandler);
    renderApp("/catalogs/analytics");

    const nav = await screen.findByRole("navigation", { name: "Breadcrumb" });
    // catalogs links home, the catalog links to itself; no namespace segment.
    const catalogs = screen.getByRole("link", { name: "catalogs" });
    expect(catalogs).toHaveAttribute("href", "/");
    const catalog = screen.getByRole("link", { name: "analytics" });
    expect(catalog).toHaveAttribute("href", "/catalogs/analytics");
    expect(nav).not.toHaveTextContent("events");
  });

  it("extends the trail with the namespace on a namespace page", async () => {
    mockFetch(catalogHandler);
    renderApp("/catalogs/analytics/namespaces/events");

    await screen.findByRole("navigation", { name: "Breadcrumb" });
    const ns = screen.getByRole("link", { name: "events" });
    expect(ns).toHaveAttribute(
      "href",
      "/catalogs/analytics/namespaces/events",
    );
  });

  it("shows the table as the current, unlinked segment on a table page", async () => {
    mockFetch(catalogHandler);
    renderApp("/catalogs/analytics/namespaces/events/tables/pageviews");

    const nav = await screen.findByRole("navigation", { name: "Breadcrumb" });
    // The leaf is plain text inside the nav, not a link — you're already here.
    expect(within(nav).queryByRole("link", { name: "pageviews" })).toBeNull();
    expect(within(nav).getByText("pageviews")).toBeInTheDocument();
  });

  it("renders no breadcrumb on non-catalog pages", async () => {
    mockFetch((url) => {
      if (url === "/v1/catalogs") return jsonResponse(catalogsFixture);
      return undefined;
    });
    renderApp("/");

    expect(await screen.findByText("analytics")).toBeInTheDocument();
    expect(screen.queryByRole("navigation", { name: "Breadcrumb" })).toBeNull();
  });
});
