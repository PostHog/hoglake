import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { Namespace, Table } from "../src/api/types";
import {
  catalogOptionsFixture,
  catalogsFixture,
  filesFixture,
  scanFixture,
  snapshotsPage1,
  tableFixture,
  tablesFixture,
} from "./fixtures";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

// A remote schema is read-only and served by another Hoglake, which this
// catalog relays for four routes only: get-namespace, list tables, get
// table and scan. `team_7` is a LOCAL namespace with a team-shaped name
// (today's team schemas are local), so nothing may classify by name.
const cat = "/v1/catalogs/analytics";
const remoteNs = `${cat}/namespaces/team_42`;
const remoteTableUrl = `${remoteNs}/tables/events`;
const RELAYED = [remoteNs, `${remoteNs}/tables`, remoteTableUrl, `${remoteTableUrl}/scan`];
const LINE = "Read-only: this schema is served by another Hoglake.";

const listing: Namespace[] = [
  { name: "events" },
  { name: "team_7" },
  { name: "team_42", remote: true },
];
// The holding catalog's snapshots: none of them means anything here.
const remoteTable: Table = {
  ...tableFixture,
  name: "events",
  namespace: "team_42",
  remote: true,
  read_snapshot_id: "900123",
  totals_snapshot_id: "900100",
};

function catalogHandler(url: string): Response | undefined {
  if (url === cat) return jsonResponse(catalogsFixture[0]);
  if (url === `${cat}/options`) return jsonResponse(catalogOptionsFixture);
  if (url === `${cat}/namespaces`) return jsonResponse(listing);
  if (url === `${cat}/snapshots?before=4212&limit=50`) return jsonResponse(snapshotsPage1);
  return undefined;
}

function remoteHandler(url: string): Response | undefined {
  const [path] = url.split("?");
  if (path === remoteNs) return jsonResponse({ name: "team_42", remote: true });
  if (path === `${remoteNs}/tables`) return jsonResponse(tablesFixture);
  if (path === remoteTableUrl) return jsonResponse(remoteTable);
  if (path === `${remoteTableUrl}/scan`) return jsonResponse(scanFixture);
  return catalogHandler(url);
}

/** Every API request the page made, as "METHOD url", in order. */
function recorder(fetchMock: ReturnType<typeof mockFetch>) {
  return () =>
    fetchMock.mock.calls
      .map(([u, init]) => `${init?.method ?? "GET"} ${String(u)}`)
      .filter((r) => r.includes(" /v1/catalogs/"));
}

const pause = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("remote schemas on the catalog page", () => {
  it("badges the remote namespace only", async () => {
    mockFetch(catalogHandler);
    renderApp("/catalogs/analytics");

    const row = (await screen.findByRole("link", { name: "team_42" })).closest("tr")!;
    const badge = within(row).getByText("remote");
    expect(badge).toHaveAttribute("title", "Read-only: served by another Hoglake");
    expect(screen.getAllByText("remote")).toHaveLength(1);
  });
});

describe("remote schemas on the namespace page", () => {
  it("asks get-namespace once, offers no form and links no snapshot id", async () => {
    const asked = recorder(mockFetch(remoteHandler));
    renderApp("/catalogs/analytics");
    const user = userEvent.setup();
    await user.click(await screen.findByRole("link", { name: "team_42" }));

    expect(await screen.findByText(LINE)).toBeInTheDocument();
    expect(within(screen.getByRole("heading", { level: 2, name: /team_42/ })).getByText("remote")).toBeInTheDocument();
    const earliest = await screen.findByText("7");
    expect(screen.queryByRole("heading", { name: "Create table" })).not.toBeInTheDocument();
    expect(earliest).not.toHaveAttribute("data-snapshot-id");
    expect(document.querySelector("[data-snapshot-id='7']")).toBeNull();
    await user.hover(earliest);
    await pause(250);
    expect(asked().filter((r) => r.includes("after="))).toEqual([]);
    expect(asked().filter((r) => r === `GET ${remoteNs}`)).toHaveLength(1);
  });

  it("keeps a local namespace exactly as it was, team-shaped name and all", async () => {
    const asked = recorder(mockFetch((url) =>
      url === `${cat}/namespaces/team_7/tables` ? jsonResponse(tablesFixture) : undefined,
    ));
    renderApp("/catalogs/analytics/namespaces/team_7");

    expect(await screen.findByRole("heading", { name: "Create table" })).toBeInTheDocument();
    expect(screen.getByText("7")).toHaveAttribute("data-snapshot-id", "7");
    expect(screen.queryByText("remote")).not.toBeInTheDocument();
    expect(screen.queryByText(LINE)).not.toBeInTheDocument();
    expect(asked().sort()).toEqual([
      `GET ${cat}/namespaces/team_7`,
      `GET ${cat}/namespaces/team_7/tables`,
    ]);
  });

  it("classifies a schema the listing leaves out from get-namespace", async () => {
    const asked = recorder(mockFetch(remoteHandler));
    // Reached by URL, as a caller holding `*` for teams would: the listing
    // has no team_42 for it, and the page never asks the listing anyway.
    renderApp("/catalogs/analytics/namespaces/team_42");

    expect(await screen.findByText(LINE)).toBeInTheDocument();
    await screen.findByText("pageviews");
    expect(screen.queryByRole("heading", { name: "Create table" })).not.toBeInTheDocument();
    expect(asked().sort()).toEqual([`GET ${remoteNs}`, `GET ${remoteNs}/tables`]);
  });
});

describe("remote tables on the table page", () => {
  it("reads at head, ignores ?snapshot= and requests no unserved route", async () => {
    const asked = recorder(mockFetch(remoteHandler));
    renderApp("/catalogs/analytics/namespaces/team_42/tables/events?snapshot=7&tab=files");
    const user = userEvent.setup();

    expect(await screen.findByText("Files: not available for a remote table.")).toBeInTheDocument();
    expect(screen.getByText(LINE)).toBeInTheDocument();
    expect(
      screen.getByText("Time travel by snapshot id is not available for a remote table: showing head."),
    ).toBeInTheDocument();
    expect(screen.queryByLabelText("snapshot id")).not.toBeInTheDocument();
    expect(screen.queryByText(/@ snapshot/)).not.toBeInTheDocument();
    // The holding catalog's ids are shown, never looked up here.
    expect(await screen.findByText("900123")).toBeInTheDocument();
    expect(screen.getByText(/900100/)).toBeInTheDocument();
    expect(document.querySelector("[data-snapshot-id]")).toBeNull();

    await user.click(screen.getByRole("tab", { name: "partitions" }));
    expect(screen.getByText("Partitions: not available for a remote table.")).toBeInTheDocument();
    await user.click(screen.getByRole("tab", { name: "scan" }));
    await screen.findByText(/dv-00900\.puffin/);

    expect(asked().filter((r) => r.startsWith(`GET ${remoteTableUrl}?`) || r === `GET ${remoteTableUrl}`))
      .toEqual([`GET ${remoteTableUrl}`]);
    for (const r of asked()) {
      expect(RELAYED, r).toContain(r.replace(/^\w+ /, "").split("?")[0]);
      expect(r).not.toContain("snapshot=7");
    }
  });

  it("plans the scan at the answer's read_snapshot_id", async () => {
    const asked = recorder(mockFetch(remoteHandler));
    renderApp("/catalogs/analytics/namespaces/team_42/tables/events?snapshot=7&tab=scan");

    await screen.findByText(/dv-00900\.puffin/);
    expect(asked().filter((r) => r.includes("/scan"))).toEqual([
      `GET ${remoteTableUrl}/scan?snapshot=900123`,
    ]);
  });

  it("shows exact partition inspection as unavailable without a lookup", async () => {
    const asked = recorder(mockFetch(remoteHandler));
    renderApp(
      "/catalogs/analytics/namespaces/team_42/tables/events?tab=partitions&lookup=1&lookup_spec=1&lookup_values=%5B%2220697%22%5D",
    );

    expect(await screen.findByText("Partitions: not available for a remote table.")).toBeInTheDocument();
    await screen.findByText("900123");
    expect(asked().filter((r) => r.includes("/partitions"))).toEqual([]);
  });

  it("turns remote on a table answer under a namespace classed local", async () => {
    const base = `${cat}/namespaces/events/tables/pageviews`;
    const asked = recorder(mockFetch((url) =>
      url.split("?")[0] === base
        ? jsonResponse({ ...tableFixture, remote: true, read_snapshot_id: "900123" })
        : undefined,
    ));
    renderApp("/catalogs/analytics/namespaces/events/tables/pageviews?snapshot=7");

    expect(await screen.findByText(LINE)).toBeInTheDocument();
    await waitFor(() => expect(asked()).toContain(`GET ${base}`));
    expect(screen.queryByLabelText("snapshot id")).not.toBeInTheDocument();
    expect(await screen.findByText("900123")).toBeInTheDocument();
    await pause(50);
    expect(asked().filter((r) => r.includes("snapshot="))).toEqual([`GET ${base}?snapshot=7`]);
  });

  it("keeps a local table's requests exactly as they were", async () => {
    const base = `${cat}/namespaces/events/tables/pageviews`;
    const asked = recorder(mockFetch((url) => {
      const [path] = url.split("?");
      if (path === base) return jsonResponse(tableFixture);
      if (path === `${base}/files`) return jsonResponse(filesFixture);
      return undefined;
    }));
    renderApp("/catalogs/analytics/namespaces/events/tables/pageviews?snapshot=7&tab=files");

    await screen.findByText(/data-00101\.parquet/);
    expect(screen.getByLabelText("snapshot id")).toHaveValue("7");
    expect(screen.queryByText("remote")).not.toBeInTheDocument();
    // Today's two requests, plus the get-namespace they now wait for.
    expect(asked()[0]).toBe(`GET ${cat}/namespaces/events`);
    expect(asked().slice(1).sort()).toEqual([
      `GET ${base}/files?snapshot=7&limit=100&offset=0`,
      `GET ${base}?snapshot=7`,
    ]);
  });
});

describe("remote schemas on the compaction-debt page", () => {
  it("does not ask partition statistics for a remote namespace filter", async () => {
    const asked = recorder(mockFetch(remoteHandler));
    renderApp("/catalogs/analytics/partitions?namespace=team_42");

    expect(
      await screen.findByText("Compaction debt: not available for a remote schema."),
    ).toBeInTheDocument();
    expect(asked()).toEqual([`GET ${remoteNs}`]);
  });
});
