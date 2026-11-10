import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach, vi } from "vitest";
import { resetAuthForTests } from "../src/lib/auth";

// Node's experimental localStorage global (inert without
// --localstorage-file) preempts jsdom's implementation, leaving
// window.localStorage undefined under vitest. Back it with a Map.
// sessionStorage (the sign-in token's home) gets the same treatment.
function mapStorage(): Storage {
  const store = new Map<string, string>();
  return {
    getItem: (k: string) => store.get(k) ?? null,
    setItem: (k: string, v: string) => void store.set(k, String(v)),
    removeItem: (k: string) => void store.delete(k),
    clear: () => store.clear(),
    key: (i: number) => [...store.keys()][i] ?? null,
    get length() {
      return store.size;
    },
  };
}
if (typeof window !== "undefined") {
  for (const name of ["localStorage", "sessionStorage"] as const) {
    if (!window[name]) {
      Object.defineProperty(window, name, { configurable: true, value: mapStorage() });
    }
  }
}

vi.mock("uplot", async () => {
  const { FakeUPlot } = await import("./uplotMock");
  return { default: FakeUPlot };
});
vi.mock("uplot/dist/uPlot.min.css", () => ({}));

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  // The sign-in state is module-level: a test that signs in must not leak
  // its token into the next one.
  resetAuthForTests();
});
