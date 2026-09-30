import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  formatAge,
  formatBytes,
  formatCompactCount,
  formatCount,
  formatRelativeAge,
  ageParts,
} from "../src/lib/format";
import { identifierError } from "../src/lib/names";

describe("formatCount", () => {
  it("groups digits", () => {
    expect(formatCount("0")).toBe("0");
    expect(formatCount("999")).toBe("999");
    expect(formatCount("1234567")).toBe("1,234,567");
  });

  it("is exact above 2^53 (no Number round-trip)", () => {
    expect(formatCount("9007199254740993")).toBe("9,007,199,254,740,993");
    expect(formatCount("4611686018427387905")).toBe(
      "4,611,686,018,427,387,905",
    );
  });

  it("never renders NaN for missing or malformed input", () => {
    expect(formatCount(undefined)).toBe("—");
    expect(formatCount(null)).toBe("—");
    expect(formatCount("")).toBe("—");
    expect(formatCount("NaN")).toBe("—");
    expect(formatCount("12garbage")).toBe("—");
    expect(formatCount(Number.NaN)).toBe("—");
  });
});

describe("formatBytes", () => {
  it("humanizes byte counts", () => {
    expect(formatBytes("512")).toBe("512 B");
    expect(formatBytes("268435456")).toBe("256 MiB");
    expect(formatBytes("5368709120")).toBe("5.0 GiB");
  });

  it("never renders NaN for missing or malformed input", () => {
    expect(formatBytes(undefined)).toBe("—");
    expect(formatBytes(null)).toBe("—");
    expect(formatBytes("pending")).toBe("—");
    expect(formatBytes(Number.NaN)).toBe("—");
  });
});

describe("identifierError (server 422 pattern mirror)", () => {
  it("accepts spec-valid identifiers", () => {
    expect(identifierError("events")).toBeNull();
    expect(identifierError("_x")).toBeNull();
    expect(identifierError("A9-b_c")).toBeNull();
    expect(identifierError("a".repeat(128))).toBeNull(); // max length
    expect(identifierError("")).toBeNull(); // blank is `required`'s job
  });

  it("rejects pattern violations", () => {
    expect(identifierError("9starts")).not.toBeNull();
    expect(identifierError("-lead")).not.toBeNull();
    expect(identifierError("has space")).not.toBeNull();
    expect(identifierError("dotted.name")).not.toBeNull();
    expect(identifierError("ns<script>alert(1)</script>")).not.toBeNull();
    expect(identifierError("a".repeat(129))).not.toBeNull(); // too long
  });
});

describe("formatCompactCount", () => {
  it("passes small counts through", () => {
    expect(formatCompactCount(0)).toBe("0");
    expect(formatCompactCount("999")).toBe("999");
    expect(formatCompactCount(-42)).toBe("-42");
  });

  it("scales by magnitude tier", () => {
    expect(formatCompactCount(4886)).toBe("4.8K");
    expect(formatCompactCount("324017331")).toBe("324M");
    expect(formatCompactCount("8200000000000")).toBe("8.2T");
    expect(formatCompactCount("-1500000")).toBe("-1.5M");
  });

  it("truncates (never rounds up across a tier) and stays lossless above 2^53", () => {
    // 999_950 must not become "1000.0K" or "1.0M" via rounding surprises.
    expect(formatCompactCount("999950")).toBe("999K");
    // int64 max: 19 digits, exact leading digits, Qi tier.
    expect(formatCompactCount("9223372036854775807")).toBe("9.2Qi");
  });

  it("guards non-integers like the other humanizers", () => {
    expect(formatCompactCount(undefined)).toBe("—");
    expect(formatCompactCount("12.5")).toBe("—");
  });
});

describe("formatAge", () => {
  // Pinned so "now" is fixed; ages below are measured back from here.
  const NOW = new Date("2026-09-19T12:00:00Z");
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  const ago = (ms: number) => new Date(NOW.getTime() - ms).toISOString();
  const S = 1000;
  const MIN = 60 * S;
  const H = 60 * MIN;
  const D = 24 * H;

  it("shows a single rounded unit — approximate on purpose", () => {
    // "19h", never "19h 11min": the finer unit is noise for placing a
    // snapshot in time.
    expect(formatAge(ago(19 * H + 11 * MIN))).toBe("19h");
    expect(formatAge(ago(3 * D + 12 * H))).toBe("4d"); // 3.5d rounds up
    expect(formatAge(ago(3 * D + 2 * H))).toBe("3d");
    expect(formatAge(ago(34 * MIN + 40 * S))).toBe("35min");
    expect(formatAge(ago(8 * S))).toBe("8s");
  });

  it("switches unit at 2x thresholds, like the maintenance page", () => {
    // Up to 119min stays minutes; 120min becomes hours. Up to 47h stays
    // hours; 48h becomes days.
    expect(formatAge(ago(119 * MIN))).toBe("119min");
    expect(formatAge(ago(120 * MIN))).toBe("2h");
    expect(formatAge(ago(47 * H))).toBe("47h");
    expect(formatAge(ago(48 * H))).toBe("2d");
  });

  it("says min, never a bare m", () => {
    // Consistency with the maintenance page: 'm' next to counts reads
    // as mega. Checked across the minutes band, not one value.
    for (const mins of [2, 5, 59, 119]) {
      expect(formatAge(ago(mins * MIN))).toMatch(/^\d+min$/);
    }
  });

  it("clamps a future instant to 0s instead of a negative age", () => {
    // Client clock skew, or a snapshot dated slightly ahead. The offset
    // is deliberately NOT a whole minute: at an exact minute the seconds
    // term is -0 and prints "0s" even without the clamp, so a round
    // offset would pass whether or not the clamp exists.
    expect(formatAge(new Date(NOW.getTime() + 90 * S).toISOString())).toBe("0s");
  });

  it("is an em dash for a missing or unparseable instant", () => {
    expect(formatAge(undefined)).toBe("—");
    expect(formatAge(null)).toBe("—");
    expect(formatAge("not-a-date")).toBe("—");
  });
});

// The snapshot tooltip's second line. Same LADDER as formatAge (shared
// `ageParts`), different words — see the agreement test at the end.
describe("formatRelativeAge", () => {
  const NOW = new Date("2026-09-29T22:34:07Z");
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  const ago = (ms: number) => new Date(NOW.getTime() - ms).toISOString();
  const S = 1000;
  const MIN = 60 * S;
  const H = 60 * MIN;
  const D = 24 * H;

  it("walks the ladder in words", () => {
    const cases: [number, string][] = [
      [0, "just now"],
      [44 * S, "just now"],
      // The seconds bucket runs to the ladder's 120s switch, and all of it
      // reads "just now": a snapshot committed a minute ago has, in the
      // only sense a reader cares about, just happened.
      [119 * S, "just now"],
      [120 * S, "2 min ago"],
      [3 * MIN, "3 min ago"],
      [119 * MIN, "119 min ago"],
      [120 * MIN, "2 hours ago"],
      [2 * H + 30 * MIN, "3 hours ago"], // rounded, like formatAge
      [47 * H, "47 hours ago"],
      [48 * H, "2 days ago"],
      [3 * D + 5 * H, "3 days ago"],
      [400 * D, "400 days ago"],
    ];
    for (const [elapsed, want] of cases) {
      expect(formatRelativeAge(ago(elapsed)), `${elapsed}ms`).toBe(want);
    }
  });

  it("pluralizes the spelled-out units", () => {
    expect(formatRelativeAge(ago(3 * H))).toBe("3 hours ago");
    expect(formatRelativeAge(ago(3 * D))).toBe("3 days ago");
    // "min" has no plural form — it is the abbreviation, not the word,
    // for the same reason formatAge uses it: "m" beside counts reads as
    // the SI mega prefix.
    expect(formatRelativeAge(ago(5 * MIN))).toBe("5 min ago");
  });

  it("clamps a future instant to just now instead of a negative age", () => {
    // Client clock skew, or a snapshot dated slightly ahead of this
    // browser. "-3 min ago" is never the right thing to print.
    expect(formatRelativeAge(ago(-3 * MIN))).toBe("just now");
    expect(formatRelativeAge(ago(-40 * D))).toBe("just now");
  });

  it("is an em dash for a missing or unparseable instant, like formatAge", () => {
    expect(formatRelativeAge(undefined)).toBe("—");
    expect(formatRelativeAge(null)).toBe("—");
    expect(formatRelativeAge("not-a-date")).toBe("—");
  });
});

// The two renderers sit side by side on real screens: the partitions
// footer prints formatAge and the snapshot id beside it prints
// formatRelativeAge. They must never name different units for one
// instant — "sampled 4d ago at snapshot 412" beside a tooltip saying
// "3 days ago" is the bug this pins shut.
describe("formatAge and formatRelativeAge share one ladder", () => {
  const NOW = new Date("2026-09-29T22:34:07Z");
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(NOW);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("agrees on the unit and the number at every threshold", () => {
    const S = 1000;
    const MIN = 60 * S;
    const H = 60 * MIN;
    const D = 24 * H;
    const cases: [number, string, string][] = [
      // elapsed, formatAge, formatRelativeAge
      [8 * S, "8s", "just now"],
      [119 * S, "119s", "just now"],
      [120 * S, "2min", "2 min ago"],
      [110 * MIN, "110min", "110 min ago"],
      [119 * MIN, "119min", "119 min ago"],
      [120 * MIN, "2h", "2 hours ago"],
      [19 * H + 11 * MIN, "19h", "19 hours ago"],
      [47 * H, "47h", "47 hours ago"],
      [48 * H, "2d", "2 days ago"],
      [3 * D + 12 * H, "4d", "4 days ago"], // 3.5d rounds up in both
    ];
    for (const [elapsed, compact, prose] of cases) {
      const iso = new Date(NOW.getTime() - elapsed).toISOString();
      expect(formatAge(iso), `${elapsed}ms compact`).toBe(compact);
      expect(formatRelativeAge(iso), `${elapsed}ms prose`).toBe(prose);
      // The number and unit agree wherever the prose names one at all.
      const parts = ageParts(iso)!;
      expect(compact).toBe(`${parts.value}${parts.unit}`);
      if (parts.unit !== "s") {
        expect(prose.startsWith(`${parts.value} `)).toBe(true);
      }
    }
  });
});
