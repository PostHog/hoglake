import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const catalogsFixture: unknown[] = [];

function withInfo(info: Record<string, unknown>) {
  mockFetch((url) => {
    if (url === "/v1/info")
      return jsonResponse({ version: "1.3.12-dev", ...info });
    if (url === "/v1/catalogs") return jsonResponse(catalogsFixture);
    return undefined;
  });
  renderApp("/");
}

describe("topbar instance totals", () => {
  it("renders the sums when every catalog is covered", async () => {
    withInfo({
      total_rows: 1234567,
      total_size_bytes: 2147483648,
      unsampled_catalogs: 0,
    });
    const badge = await screen.findByText(/2\.0 GiB · 1\.2M rows/);
    expect(badge).toHaveClass("instance-totals");
    expect(badge.textContent).not.toContain("unsampled");
  });

  it("marks a partial sum with the count it leaves out", async () => {
    withInfo({ total_rows: 10, total_size_bytes: 1024, unsampled_catalogs: 3 });
    const badge = await screen.findByText(/1\.0 KiB · 10 rows · 3 unsampled/);
    expect(badge.title).toContain("3 catalogs not yet sampled");
  });

  it("says it is sampling when catalogs exist but none has totals yet", async () => {
    // The post-upgrade window: the server withholds the sums rather
    // than publishing 0 B over a full instance.
    withInfo({ unsampled_catalogs: 2 });
    const badge = await screen.findByText("sampling 2 catalogs…");
    expect(badge).toHaveClass("instance-totals");
    expect(badge.textContent).not.toContain("0 B");
  });

  it("singular when one catalog is being sampled", async () => {
    withInfo({ unsampled_catalogs: 1 });
    expect(await screen.findByText("sampling 1 catalog…")).toBeInTheDocument();
  });

  it("renders nothing in the boot window before the first sample", async () => {
    withInfo({});
    expect(await screen.findByText("hoglake")).toBeInTheDocument();
    expect(document.querySelector(".instance-totals")).toBeNull();
  });
});
