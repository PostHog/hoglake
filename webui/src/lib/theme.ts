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

/**
 * A per-browser override of the instance palette. Set it from the console
 * (`document.cookie = "hoglake-color-theme=nord; path=/; max-age=31536000"`);
 * Any value that is not a palette name (`default`, empty, a typo) is ignored
 * and the instance's `ui_theme` applies. A cookie rather than localStorage so it can
 * also be set outside the app (a proxy, a bookmarklet, curl -b for a
 * screenshot) and so a reload applies it before `/v1/info` answers.
 */
export const COLOR_THEME_COOKIE = "hoglake-color-theme";

function normalize(value?: string): string | undefined {
  const name = value?.trim().toLowerCase();
  return COLOR_THEMES.some((theme) => theme === name) ? name : undefined;
}

export function cookieColorTheme(): string | undefined {
  try {
    const prefix = `${COLOR_THEME_COOKIE}=`;
    for (const part of document.cookie.split(";")) {
      const entry = part.trim();
      if (entry.startsWith(prefix)) return decodeURIComponent(entry.slice(prefix.length));
    }
  } catch {
    // cookies unavailable — fall through to the instance theme
  }
  return undefined;
}

/** Write (or, for undefined, expire) the override cookie. Best-effort, like persistTheme. */
export function persistColorTheme(name: string | undefined): void {
  try {
    const value = normalize(name);
    document.cookie = value
      ? `${COLOR_THEME_COOKIE}=${value}; path=/; max-age=31536000; SameSite=Lax`
      : `${COLOR_THEME_COOKIE}=; path=/; max-age=0; SameSite=Lax`;
  } catch {
    // cookies unavailable — the choice lasts for this page only
  }
}

/** The cookie wins when it names a palette; otherwise the instance's choice. */
export function resolveColorTheme(instanceTheme?: string): string | undefined {
  return normalize(cookieColorTheme()) ?? instanceTheme;
}

/** The server selects the palette; the browser selects light or dark mode. */
export function applyColorTheme(value?: string): void {
  const name = normalize(value);
  if (name) {
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
