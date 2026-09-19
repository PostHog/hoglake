import { describe, expect, it } from "vitest";
import {
  applySort,
  cmpInt64,
  cmpText,
  int64Column,
  isDecimalInt,
  nextSort,
  textColumn,
} from "../src/lib/sort";

/**
 * Column sorting is a claim about the data: "these are the biggest
 * files". Every case here is one where the obvious implementation makes
 * that claim wrongly — lossy doubles, lexicographic digits, blanks
 * riding to the top of a largest-first sort — and looks right doing it.
 */

describe("cmpInt64", () => {
  it("orders past 2^53, where Number would tie", () => {
    const a = "9007199254740993";
    const b = "9007199254740992";
    // The premise: these two distinct row ids are the SAME double, which
    // is why the wire carries them as strings at all.
    expect(Number(a) === Number(b)).toBe(true);
    expect(cmpInt64(a, b)).toBe(1);
    expect(cmpInt64(b, a)).toBe(-1);
  });

  it("orders by magnitude, not lexically", () => {
    expect(cmpInt64("9", "10")).toBe(-1);
    expect(cmpInt64("100", "99")).toBe(1);
    expect(cmpInt64("007", "7")).toBe(0);
  });

  it("orders negatives below zero and by magnitude", () => {
    // Partition ordinals floor, so a pre-1970 year/month/day/hour is
    // negative on the wire.
    expect(cmpInt64("-1", "0")).toBe(-1);
    expect(cmpInt64("-20", "-3")).toBe(-1);
    expect(cmpInt64("-1", "1")).toBe(-1);
  });

  it("puts unusable values after real ones instead of throwing", () => {
    // BigInt() throws on these; a table must not blank itself because
    // one row carries an em dash or an absent field.
    expect(cmpInt64(undefined, "5")).toBe(1);
    expect(cmpInt64("5", undefined)).toBe(-1);
    expect(cmpInt64("—", "5")).toBe(1);
    expect(cmpInt64(null, undefined)).toBe(0);
  });
});

describe("cmpText", () => {
  it("compares digits inside text numerically", () => {
    // Plain < would put team_id=10 before team_id=9 and read as broken.
    expect(cmpText("team_id=9", "team_id=10")).toBeLessThan(0);
    expect(cmpText("part-2", "part-10")).toBeLessThan(0);
  });

  it("keeps ISO timestamps in chronological order", () => {
    expect(cmpText("2026-09-18T09:00:00Z", "2026-09-18T10:00:00Z")).toBeLessThan(0);
    expect(cmpText("2026-09-18T23:00:00Z", "2026-09-19T01:00:00Z")).toBeLessThan(0);
  });

  it("treats absent text as empty rather than as 'undefined'", () => {
    expect(cmpText(undefined, "")).toBe(0);
    expect(cmpText(undefined, "a")).toBeLessThan(0);
  });
});

describe("isDecimalInt", () => {
  it("accepts signed decimals and rejects everything else", () => {
    expect(isDecimalInt("0")).toBe(true);
    expect(isDecimalInt("-12")).toBe(true);
    expect(isDecimalInt(" 7 ")).toBe(true);
    expect(isDecimalInt("1.5")).toBe(false);
    expect(isDecimalInt("1e3")).toBe(false);
    expect(isDecimalInt("—")).toBe(false);
    expect(isDecimalInt(undefined)).toBe(false);
    expect(isDecimalInt(null)).toBe(false);
  });
});

interface Row {
  id: string;
  size?: string;
}

const COLUMNS = {
  id: int64Column<Row>((r) => r.id),
  size: int64Column<Row>((r) => r.size),
  label: textColumn<Row>((r) => r.id),
};

describe("applySort", () => {
  const rows: Row[] = [
    { id: "1", size: "500" },
    { id: "2" }, // never sampled: the page renders an em dash
    { id: "3", size: "900" },
    { id: "4", size: "100" },
  ];

  it("leaves the server's order alone when nothing is sorted", () => {
    const got = applySort(rows, null, COLUMNS);
    expect(got.map((r) => r.id)).toEqual(["1", "2", "3", "4"]);
    // A copy, so a later in-place sort cannot reorder the query cache.
    expect(got).not.toBe(rows);
  });

  it("keeps rows with no value LAST in both directions", () => {
    // The trap: a blank compares as smallest, so flipping the comparator
    // for a descending sort floats it to the top — em dashes sitting
    // where the largest catalogs should be.
    const desc = applySort(rows, { key: "size", desc: true }, COLUMNS);
    expect(desc.map((r) => r.id)).toEqual(["3", "1", "4", "2"]);
    const asc = applySort(rows, { key: "size", desc: false }, COLUMNS);
    expect(asc.map((r) => r.id)).toEqual(["4", "1", "3", "2"]);
  });

  it("is stable, so ties keep the server's order", () => {
    const tied: Row[] = [
      { id: "10", size: "5" },
      { id: "11", size: "5" },
      { id: "12", size: "5" },
    ];
    expect(applySort(tied, { key: "size", desc: true }, COLUMNS).map((r) => r.id))
      .toEqual(["10", "11", "12"]);
  });

  it("sorts int columns numerically, not as text", () => {
    const wide: Row[] = [{ id: "9" }, { id: "10" }, { id: "100" }];
    expect(applySort(wide, { key: "id", desc: false }, COLUMNS).map((r) => r.id))
      .toEqual(["9", "10", "100"]);
    // Same values through a TEXT column still read naturally.
    expect(applySort(wide, { key: "label", desc: false }, COLUMNS).map((r) => r.id))
      .toEqual(["9", "10", "100"]);
  });
});

describe("nextSort", () => {
  it("opens a column descending and flips it after that", () => {
    // Every numeric column here answers "which are the big ones", so
    // ascending-first would make that question cost two clicks.
    expect(nextSort(null, "size")).toEqual({ key: "size", desc: true });
    expect(nextSort({ key: "size", desc: true }, "size")).toEqual({
      key: "size",
      desc: false,
    });
    expect(nextSort({ key: "size", desc: false }, "size")).toEqual({
      key: "size",
      desc: true,
    });
  });

  it("starts descending again on a different column", () => {
    expect(nextSort({ key: "size", desc: false }, "id")).toEqual({
      key: "id",
      desc: true,
    });
  });
});
