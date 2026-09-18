import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

const catalogsFixture: unknown[] = [];

describe("topbar instance name", () => {
  it("shows the configured instance name as a badge", async () => {
    mockFetch((url) => {
      if (url === "/v1/info") return jsonResponse({ name: "GigaHog" });
      if (url === "/v1/catalogs") return jsonResponse(catalogsFixture);
      return undefined;
    });
    renderApp("/");
    expect(await screen.findByText("GigaHog")).toBeInTheDocument();
  });

  it("renders no badge when the instance is unnamed", async () => {
    mockFetch((url) =>
      url === "/v1/catalogs" ? jsonResponse(catalogsFixture) : undefined,
    );
    renderApp("/");
    expect(await screen.findByText("hoglake")).toBeInTheDocument();
    expect(document.querySelector(".instance-name")).toBeNull();
  });

  it("renders no badge when the info endpoint fails", async () => {
    mockFetch((url) => {
      if (url === "/v1/info")
        return jsonResponse({ error: "nope", detail: "old server" }, 500);
      if (url === "/v1/catalogs") return jsonResponse(catalogsFixture);
      return undefined;
    });
    renderApp("/");
    expect(await screen.findByText("hoglake")).toBeInTheDocument();
    expect(document.querySelector(".instance-name")).toBeNull();
  });
});

describe("topbar server version", () => {
  it("shows the running server version beside the brand", async () => {
    mockFetch((url) => {
      if (url === "/v1/info")
        return jsonResponse({ name: "GigaHog", version: "1.0.1-dev" });
      if (url === "/v1/catalogs") return jsonResponse(catalogsFixture);
      return undefined;
    });
    renderApp("/");
    expect(await screen.findByText("v1.0.1-dev")).toBeInTheDocument();
  });

  it("shows the server's own answer when it does not know its version", async () => {
    // BuildInfo reports "unknown" rather than omitting the field; the
    // badge must not silently hide that.
    mockFetch((url) => {
      if (url === "/v1/info") return jsonResponse({ version: "unknown" });
      if (url === "/v1/catalogs") return jsonResponse(catalogsFixture);
      return undefined;
    });
    renderApp("/");
    expect(await screen.findByText("vunknown")).toBeInTheDocument();
  });

  it("shows the build stamp beside the version when the image was stamped", async () => {
    mockFetch((url) => {
      if (url === "/v1/info")
        return jsonResponse({ version: "1.0.1-dev", build: "20260915T2104Z" });
      if (url === "/v1/catalogs") return jsonResponse(catalogsFixture);
      return undefined;
    });
    renderApp("/");
    expect(await screen.findByText("+20260915T2104Z")).toBeInTheDocument();
    expect(screen.getByText("v1.0.1-dev")).toBeInTheDocument();
  });

  it("shows a bare version for a locally built server", async () => {
    // No stamp is the ordinary local build, not a fault: the badge must
    // read as a plain version rather than showing an empty suffix.
    mockFetch((url) => {
      if (url === "/v1/info") return jsonResponse({ version: "1.0.1-dev" });
      if (url === "/v1/catalogs") return jsonResponse(catalogsFixture);
      return undefined;
    });
    renderApp("/");
    expect(await screen.findByText("v1.0.1-dev")).toBeInTheDocument();
    expect(document.querySelector(".server-build")).toBeNull();
  });

  it("renders no version badge against a server that predates the field", async () => {
    mockFetch((url) => {
      if (url === "/v1/info") return jsonResponse({ name: "GigaHog" });
      if (url === "/v1/catalogs") return jsonResponse(catalogsFixture);
      return undefined;
    });
    renderApp("/");
    expect(await screen.findByText("GigaHog")).toBeInTheDocument();
    expect(document.querySelector(".server-version")).toBeNull();
  });
});

describe("browser tab title", () => {
  it("leads with the instance name so tabs stay distinguishable", async () => {
    mockFetch((url) => {
      if (url === "/v1/info") return jsonResponse({ name: "gigahog-dev" });
      if (url === "/v1/catalogs") return jsonResponse(catalogsFixture);
      return undefined;
    });
    renderApp("/");
    await screen.findByText("gigahog-dev");
    // Name first: tabs truncate from the right, so the discriminator has
    // to survive a narrow tab.
    expect(document.title).toBe("gigahog-dev · hoglake");
  });

  it("keeps the bare product name when the instance is unnamed", async () => {
    mockFetch((url) =>
      url === "/v1/catalogs" ? jsonResponse(catalogsFixture) : undefined,
    );
    renderApp("/");
    await screen.findByText("hoglake");
    expect(document.title).toBe("hoglake");
  });

  it("keeps the bare product name when the info endpoint fails", async () => {
    mockFetch((url) => {
      if (url === "/v1/info") return jsonResponse({ error: "nope" }, 500);
      if (url === "/v1/catalogs") return jsonResponse(catalogsFixture);
      return undefined;
    });
    renderApp("/");
    await screen.findByText("hoglake");
    expect(document.title).toBe("hoglake");
  });
});
