export type Theme = "dark" | "light";

const STORAGE_KEY = "hoglake-theme";

export const COLOR_THEMES = [
  "3024",
  "ayu",
  "catppuccin",
  "dracula",
  "everforest",
  "github",
  "gruvbox",
  "iceberg",
  "kanagawa",
  "monokai",
  "night-owl",
  "nord",
  "oceanic-next",
  "one",
  "rose-pine",
  "snazzy",
  "solarized",
  "synthwave",
  "tokyo-night",
  "tomorrow",
] as const;

/** The server selects the palette; the browser selects light or dark mode. */
export function applyColorTheme(value?: string): void {
  const name = value?.trim().toLowerCase();
  if (COLOR_THEMES.some((theme) => theme === name)) {
    document.documentElement.dataset.colorTheme = name;
  } else {
    delete document.documentElement.dataset.colorTheme;
  }
}

/** Stored choice wins; otherwise follow the OS preference (dark default). */
export function initialTheme(): Theme {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY);
    if (stored === "dark" || stored === "light") return stored;
  } catch {
    // storage unavailable (private mode etc.) — fall through
  }
  return window.matchMedia?.("(prefers-color-scheme: light)")?.matches
    ? "light"
    : "dark";
}

/** Stamp <html data-theme>; styles.css keys its token swap on this. */
export function applyTheme(theme: Theme): void {
  document.documentElement.dataset.theme = theme;
}

export function persistTheme(theme: Theme): void {
  try {
    window.localStorage.setItem(STORAGE_KEY, theme);
  } catch {
    // persistence is best-effort
  }
}
