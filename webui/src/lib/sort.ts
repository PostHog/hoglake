// Client-side column sorting for the data tables.
//
// Sorting here is honest only because these tables hold the WHOLE result:
// GET /files and /scan return every live file at the snapshot, and the
// catalogs listing every catalog. Sorting one page of a truncated list
// would answer "the largest of the first N" while looking like "the
// largest" — so a table that grows a server-side limit has to grow
// server-side sorting with it, not reuse this.

import { compareInt64 } from "../api/int64";

/** A signed decimal integer — the shape every int64 wire field has. */
export function isDecimalInt(v: string | null | undefined): v is string {
  return typeof v === "string" && /^-?\d+$/.test(v.trim());
}

/**
 * Two int64 wire values, compared as exact integers.
 *
 * These arrive as decimal STRINGS precisely because they can exceed 2^53
 * (see api/int64.ts), so `Number(a) - Number(b)` would order two distinct
 * row_id_starts by their rounded doubles — the one thing the string
 * representation exists to prevent. [compareInt64] does the arithmetic in
 * BigInt; this adds only the tolerance a table needs, since a value that
 * is not a decimal integer must not throw and blank the whole table.
 */
export function cmpInt64(
  a: string | null | undefined,
  b: string | null | undefined,
): number {
  const okA = isDecimalInt(a);
  const okB = isDecimalInt(b);
  if (!okA || !okB) return okA === okB ? 0 : okA ? -1 : 1;
  return compareInt64(a.trim(), b.trim());
}

const COLLATOR = new Intl.Collator(undefined, {
  numeric: true,
  sensitivity: "base",
});

/**
 * Display text, compared in natural order: `team_id=9` before
 * `team_id=10`, and `part-2` before `part-10`.
 *
 * Plain lexicographic ordering puts "10" before "9", which on a
 * partition or path column reads as the table being sorted wrong rather
 * than as a subtlety of string comparison.
 */
export function cmpText(
  a: string | null | undefined,
  b: string | null | undefined,
): number {
  return COLLATOR.compare(a ?? "", b ?? "");
}

/**
 * How one column orders rows.
 *
 * [absent] exists because a blank is not a value. These tables render an
 * em dash for a count the sampler has not produced and for a file with
 * no deletion vector, and an em dash that merely compares as "smallest"
 * would ride to the TOP of a largest-first sort — the opposite of the
 * question being asked. Rows it marks sort last in both directions.
 */
export interface ColumnSort<T> {
  compare: (a: T, b: T) => number;
  absent?: (row: T) => boolean;
}

/** An int64 column, addressed by the field it reads. */
export function int64Column<T>(
  get: (row: T) => string | null | undefined,
): ColumnSort<T> {
  return {
    compare: (a, b) => cmpInt64(get(a), get(b)),
    absent: (row) => !isDecimalInt(get(row)),
  };
}

/** A text column, compared in natural order. */
export function textColumn<T>(
  get: (row: T) => string | null | undefined,
): ColumnSort<T> {
  return { compare: (a, b) => cmpText(get(a), get(b)) };
}

export interface SortState<K extends string> {
  key: K;
  desc: boolean;
}

/**
 * Apply [sort] to [rows] using [columns].
 *
 * `null` means the server's own order, which is never arbitrary — files
 * come back in (begin_snapshot, row_id_start, data_file_id) order, which
 * is the manifest's, and partition stats in debt order. The table opens
 * in that order and can return to it, rather than imposing a default the
 * server did not choose.
 *
 * The sort is stable (guaranteed since ES2019), so rows that tie keep the
 * server's relative order instead of shuffling between renders.
 */
export function applySort<T, K extends string>(
  rows: readonly T[],
  sort: SortState<K> | null,
  columns: Record<K, ColumnSort<T>>,
): T[] {
  if (!sort) return [...rows];
  const column = columns[sort.key];
  const absent = column.absent;
  return [...rows].sort((a, b) => {
    if (absent) {
      const missingA = absent(a);
      const missingB = absent(b);
      // Deliberately NOT flipped with the direction: see ColumnSort.
      if (missingA || missingB) {
        return missingA === missingB ? 0 : missingA ? 1 : -1;
      }
    }
    const c = column.compare(a, b);
    return sort.desc ? -c : c;
  });
}

/**
 * Next sort state for a click on [key]: a new column starts DESCENDING,
 * and clicking the active column flips it.
 *
 * Descending-first because every numeric column here answers a "which are
 * the big ones" question — largest files, most rows, worst debt.
 * Ascending-first would make every such question take two clicks.
 */
export function nextSort<K extends string>(
  prev: SortState<K> | null,
  key: K,
): SortState<K> {
  return prev?.key === key ? { key, desc: !prev.desc } : { key, desc: true };
}
