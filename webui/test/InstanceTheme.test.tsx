import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { applyColorTheme } from "../src/lib/theme";
import { jsonResponse, mockFetch, renderApp } from "./helpers";

beforeEach(() => {
  window.localStorage.clear();
  delete document.documentElement.dataset.theme;
  delete document.documentElement.dataset.colorTheme;
});

function instance(theme?: string, status = 200) {
  return mockFetch((url) => {
    if (url === "/v1/info") return jsonResponse({ ui_theme: theme, name: "test-instance" }, status);
    if (url === "/v1/catalogs") return jsonResponse([]);
    return undefined;
  });
}

describe("instance color theme", () => {
  it("keeps the configured palette when the user changes display mode", async () => {
    instance("catppuccin");
    renderApp("/");
    await waitFor(() => expect(document.documentElement.dataset.colorTheme).toBe("catppuccin"));
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Switch to light mode" }));
    expect(document.documentElement.dataset.theme).toBe("light");
    expect(document.documentElement.dataset.colorTheme).toBe("catppuccin");
    expect(window.localStorage.getItem("hoglake-theme")).toBe("light");
    await user.click(screen.getByRole("button", { name: "Switch to dark mode" }));
    expect(document.documentElement.dataset.theme).toBe("dark");
    expect(document.documentElement.dataset.colorTheme).toBe("catppuccin");
  });

  it.each([undefined, "unavailable", ""])("uses default colors for %s", async (theme) => {
    applyColorTheme("nord");
    instance(theme);
    renderApp("/");
    await screen.findByText("test-instance");
    expect(document.documentElement.dataset.colorTheme).toBeUndefined();
  });

  it("uses default colors if instance information fails", async () => {
    applyColorTheme("nord");
    const fetch = instance("dracula", 500);
    renderApp("/");
    await waitFor(() => expect(fetch).toHaveBeenCalledWith("/v1/info", expect.anything()));
    expect(document.documentElement.dataset.colorTheme).toBeUndefined();
    expect(screen.getByRole("button", { name: "Switch to light mode" })).toBeEnabled();
  });

  it("follows system changes until the user saves a mode", async () => {
    let light = true;
    const media = new EventTarget();
    vi.stubGlobal("matchMedia", () => Object.assign(media, { matches: light }));
    instance("nord");
    const view = renderApp("/");
    await waitFor(() => expect(document.documentElement.dataset.colorTheme).toBe("nord"));
    expect(document.documentElement.dataset.theme).toBe("light");
    light = false;
    act(() => media.dispatchEvent(new Event("change")));
    expect(document.documentElement.dataset.theme).toBe("dark");
    await userEvent.setup().click(screen.getByRole("button", { name: "Switch to light mode" }));
    act(() => media.dispatchEvent(new Event("change")));
    expect(document.documentElement.dataset.theme).toBe("light");
    expect(document.documentElement.dataset.colorTheme).toBe("nord");
    view.unmount();
    renderApp("/");
    expect(document.documentElement.dataset.theme).toBe("light");
  });
});
