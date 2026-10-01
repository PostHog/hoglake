import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const catalogsFixture: unknown[] = [];

function renderWithInfo(info: unknown, status = 200) {
  mockFetch((url) => {
    if (url === "/v1/info") return jsonResponse(info, status);
    if (url === "/v1/catalogs") return jsonResponse(catalogsFixture);
    return undefined;
  });
  renderApp("/");
}

describe("footer", () => {
  it("shows the copyright line", async () => {
    renderWithInfo({ version: "1.3.6" });
    expect(
      await screen.findByText("Copyright 2026 PostHog, Inc."),
    ).toBeInTheDocument();
  });

  it("links a released version to its own release notes", async () => {
    renderWithInfo({ version: "1.3.6" });
    await screen.findByText("v1.3.6");
    expect(screen.getByRole("link", { name: "Release notes" })).toHaveAttribute(
      "href",
      "https://github.com/PostHog/hoglake/releases/tag/v1.3.6",
    );
  });

  it.each([
    ["a dev version", { version: "1.3.7-dev" }, "v1.3.7-dev"],
    ["an unknown version", { version: "unknown" }, "vunknown"],
  ])("links %s to the latest release", async (_, info, badge) => {
    renderWithInfo(info);
    await screen.findByText(badge);
    expect(screen.getByRole("link", { name: "Release notes" })).toHaveAttribute(
      "href",
      "https://github.com/PostHog/hoglake/releases/latest",
    );
  });

  it("links to the latest release when the info endpoint fails", async () => {
    renderWithInfo({ error: "nope", detail: "old server" }, 500);
    expect(
      await screen.findByRole("link", { name: "Release notes" }),
    ).toHaveAttribute("href", "https://github.com/PostHog/hoglake/releases/latest");
  });
});
