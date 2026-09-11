export type Theme = "dark" | "light";

const STORAGE_KEY = "hoglake-theme";

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
