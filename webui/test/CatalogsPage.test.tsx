import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
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
});
