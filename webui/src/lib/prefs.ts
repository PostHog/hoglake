// Small localStorage-backed view preferences, in the same best-effort
// posture as lib/theme.ts: a browser that refuses storage (private mode,
// hardened settings) must still render — it just forgets the choice.
//
// Every preference is a member of a closed set of strings, and the set is
// re-checked on read: a stale key left by an older build (or a hand-edited
// one) must fall back to the default rather than put the UI in a state it
// has no code for.

import { useState } from "react";

export function readPref<T extends string>(
  key: string,
  allowed: readonly T[],
  fallback: T,
): T {
  try {
    const stored = window.localStorage.getItem(key);
    if (stored !== null && (allowed as readonly string[]).includes(stored)) {
      return stored as T;
    }
  } catch {
    // storage unavailable — fall through to the default
  }
  return fallback;
}

export function writePref(key: string, value: string): void {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // persistence is best-effort
  }
}

/**
 * useState seeded from localStorage and written back on every change —
 * the hook form of ThemeToggle's initialTheme/persistTheme pair. The seed
 * is lazy, so storage is read once per mount and never during a re-render
 * (a poll refresh must not be able to reset the control).
 */
export function useStoredPref<T extends string>(
  key: string,
  allowed: readonly T[],
  fallback: T,
): [T, (next: T) => void] {
  const [value, setValue] = useState<T>(() => readPref(key, allowed, fallback));
  return [
    value,
    (next: T) => {
      writePref(key, next);
      setValue(next);
    },
  ];
}
