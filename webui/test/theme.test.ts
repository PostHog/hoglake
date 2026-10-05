import { readFileSync } from "node:fs";
import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  applyColorTheme,
  applyTheme,
  COLOR_THEME_COOKIE,
  COLOR_THEMES,
  initialTheme,
  persistColorTheme,
  persistTheme,
  resolveColorTheme,
} from "../src/lib/theme";

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
    delete document.documentElement.dataset.colorTheme;
    vi.unstubAllGlobals();
    document.cookie = `${COLOR_THEME_COOKIE}=; path=/; max-age=0`;
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

  it("normalizes configured names without changing the display mode", () => {
    applyTheme("light");
    applyColorTheme(" NORD ");
    expect(document.documentElement.dataset.colorTheme).toBe("nord");
    expect(document.documentElement.dataset.theme).toBe("light");
  });

  it.each([undefined, "", " ", "default", "missing", "__proto__", "constructor"])(
    "restores the default palette for %s",
    (name) => {
      applyColorTheme("dracula");
      applyColorTheme(name);
      expect(document.documentElement.dataset.colorTheme).toBeUndefined();
    },
  );
});

describe("color theme cookie", () => {
  it("overrides the instance palette when it names one", () => {
    document.cookie = `${COLOR_THEME_COOKIE}=Nord; path=/`;
    expect(resolveColorTheme("dracula")).toBe("nord");
    expect(resolveColorTheme(undefined)).toBe("nord");
  });

  it.each(["", "default", "missing", "__proto__", "%E2%9C%93"])("ignores %s and keeps the instance's choice", (value) => {
    document.cookie = `${COLOR_THEME_COOKIE}=${value}; path=/`;
    expect(resolveColorTheme("dracula")).toBe("dracula");
    expect(resolveColorTheme(undefined)).toBeUndefined();
  });

  it("persists a palette and expires the cookie for anything else", () => {
    persistColorTheme("Nord");
    expect(document.cookie).toContain(`${COLOR_THEME_COOKIE}=nord`);
    persistColorTheme(undefined);
    expect(document.cookie).not.toContain(`${COLOR_THEME_COOKIE}=`);
    persistColorTheme("nord");
    persistColorTheme("missing");
    expect(document.cookie).not.toContain(`${COLOR_THEME_COOKIE}=`);
  });

  it("does not read another cookie that shares the prefix", () => {
    document.cookie = `${COLOR_THEME_COOKIE}-other=nord; path=/`;
    expect(resolveColorTheme("dracula")).toBe("dracula");
    document.cookie = `${COLOR_THEME_COOKIE}-other=; path=/; max-age=0`;
  });
});

// Read the shipped CSS so a palette edit cannot bypass the contrast checks.
const css = readFileSync("src/color-themes.css", "utf8");

function luminance(hex: string): number {
  const channels = hex.match(/[a-f\d]{2}/gi)!.map((part) => {
    const value = parseInt(part, 16) / 255;
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  });
  return channels[0] * 0.2126 + channels[1] * 0.7152 + channels[2] * 0.0722;
}

function contrast(a: string, b: string): number {
  const [low, high] = [luminance(a), luminance(b)].sort((x, y) => x - y);
  return (high + 0.05) / (low + 0.05);
}

describe.each(COLOR_THEMES)("%s palette", (name) => {
  it.each(["dark", "light"])("has readable text in %s mode", (mode) => {
    const selector =
      `:root[data-color-theme="${name}"]` +
      (mode === "dark" ? ':not([data-theme="light"])' : '[data-theme="light"]');
    expect(css).toContain(selector);
    const block = css.split(selector)[1].split("}")[0];
    const colors = Object.fromEntries(
      [...block.matchAll(/--([\w-]+): (#[a-f\d]{6});/g)].map((match) => [
        match[1], match[2],
      ]),
    );
    for (const background of ["bg", "bg-raised", "bg-hover"]) {
      for (const text of ["text", "text-dim", "accent", "ok", "warn", "bad"]) {
        expect(
          contrast(colors[text], colors[background]),
          `${text} on ${background}`,
        ).toBeGreaterThanOrEqual(4.5);
      }
    }
    expect(
      contrast(colors.accent, colors["accent-contrast"]),
    ).toBeGreaterThanOrEqual(4.5);
    if (mode === "light") expect(luminance(colors.bg)).toBeGreaterThan(0.5);
    else expect(luminance(colors.bg)).toBeLessThan(0.1);
  });
});
