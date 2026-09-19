import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  formatAge,
  formatBytes,
  formatCompactCount,
  formatCount,
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
