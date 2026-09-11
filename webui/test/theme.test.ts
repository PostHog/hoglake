import { beforeEach, describe, expect, it, vi } from "vitest";
import { applyTheme, initialTheme, persistTheme } from "../src/lib/theme";

function mockMatchMedia(lightMatches: boolean) {
  vi.stubGlobal(
    "matchMedia",
    vi.fn((query: string) => ({
      matches: query.includes("light") && lightMatches,
    })),
  );
}

describe("theme", () => {
  beforeEach(() => {
    window.localStorage.clear();
    delete document.documentElement.dataset.theme;
    vi.unstubAllGlobals();
  });

  it("defaults to dark when nothing is stored and the OS has no light preference", () => {
    mockMatchMedia(false);
    expect(initialTheme()).toBe("dark");
  });

  it("follows the OS light preference when nothing is stored", () => {
    mockMatchMedia(true);
    expect(initialTheme()).toBe("light");
  });

  it("a stored choice beats the OS preference", () => {
    mockMatchMedia(true);
    persistTheme("dark");
    expect(initialTheme()).toBe("dark");
  });

  it("ignores garbage in storage", () => {
    mockMatchMedia(false);
    window.localStorage.setItem("hoglake-theme", "hotdog");
    expect(initialTheme()).toBe("dark");
  });

  it("applyTheme stamps the html element", () => {
    applyTheme("light");
    expect(document.documentElement.dataset.theme).toBe("light");
    applyTheme("dark");
    expect(document.documentElement.dataset.theme).toBe("dark");
  });

  it("survives a missing matchMedia (non-browser environments)", () => {
    vi.stubGlobal("matchMedia", undefined);
    expect(initialTheme()).toBe("dark");
  });
});
