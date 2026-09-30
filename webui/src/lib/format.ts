import type { Column, ColumnType, Int64, PartitionField } from "../api/types";

const DECIMAL_INT_RE = /^-?\d+$/;

/**
 * Guard for the int64-string humanizers: missing values and anything that is
 * not an exact decimal integer render as an em-dash, never "NaN"/"undefined".
 */
function asDecimalInt(n: string | number | null | undefined): string | null {
  if (n === null || n === undefined) return null;
  const s = typeof n === "number" ? String(n) : n;
  return DECIMAL_INT_RE.test(s) ? s : null;
}

export function formatBytes(n: string | number | null | undefined): string {
  const s = asDecimalInt(n);
  if (s === null) return "—";
  // Number() here only picks the humanized magnitude; the exact value is the
  // decimal string itself (shown wherever exactness matters, e.g. `title`).
  const abs = Number(s);
  if (abs < 1024) return `${s} B`;
  const units = ["KiB", "MiB", "GiB", "TiB", "PiB"];
  let v = abs;
  let i = -1;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v >= 100 ? v.toFixed(0) : v.toFixed(1)} ${units[i]}`;
}

/**
 * Compact count for headline numbers: 4886 -> "4.8K", 324_017_331 ->
 * "324M". Pure string truncation on the decimal string: no Number
 * round-trip (lossless above 2^53) and no rounding (which could cross
 * a tier boundary). The exact value belongs in a `title`, via
 * formatCount.
 */
export function formatCompactCount(n: string | number | null | undefined): string {
  const s = asDecimalInt(n);
  if (s === null) return "—";
  const neg = s.startsWith("-");
  const digits = neg ? s.slice(1) : s;
  const tier = Math.floor((digits.length - 1) / 3);
  if (tier === 0) return neg ? `-${digits}` : digits;
  const suffixes = ["", "K", "M", "B", "T", "Qa", "Qi"];
  const suffix = suffixes[Math.min(tier, suffixes.length - 1)];
  const cut = digits.length - 3 * Math.min(tier, suffixes.length - 1);
  // Pure string truncation: rounding could cross the tier boundary
  // ("999950" must stay "999K", never "1000K").
  const whole = digits.slice(0, cut);
  const shown = whole.length >= 3 ? whole : `${whole}.${digits[cut]}`;
  return `${neg ? "-" : ""}${shown}${suffix}`;
}

export function formatCount(n: string | number | null | undefined): string {
  const s = asDecimalInt(n);
  if (s === null) return "—";
  // Group digits on the exact decimal string: values above 2^53 must not
  // round-trip through Number/toLocaleString.
  const neg = s.startsWith("-");
  const digits = neg ? s.slice(1) : s;
  const grouped = digits.replace(/\B(?=(\d{3})+$)/g, ",");
  return neg ? `-${grouped}` : grouped;
}

export function formatTime(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toISOString().replace("T", " ").replace(/\.\d+Z$/, "Z");
}

/**
 * ONE elapsed-time ladder, shared by every age the console prints.
 *
 * Returns the single unit an instant is best described in, rounded, with
 * 2x thresholds (up to 119min before switching to hours, 47h before
 * days) matching the maintenance page's formatSeconds. The difference
 * between 19h and 19h 11min never matters for placing a snapshot in
 * time, and the second unit is just noise.
 *
 * It exists as one function because the two RENDERERS below sit side by
 * side on the same screen: the partitions footer prints
 * `formatAge(sample)` and the snapshot id beside it prints
 * `formatRelativeAge(...)`. Two ladders meant "sampled 4d ago at
 * snapshot 412" could sit next to a tooltip saying "3 days ago" for the
 * same instant. Thresholds and rounding live here so that cannot happen;
 * only the wording differs.
 *
 * `null` for a missing or unparseable instant. A future instant (client
 * clock skew) clamps to zero rather than going negative. Computed from
 * `Date.now()` at call time, so ages stay live between refetches.
 */
export function ageParts(
  iso: string | null | undefined,
): { value: number; unit: "s" | "min" | "h" | "d" } | null {
  if (iso === null || iso === undefined) return null;
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return null;
  const secs = Math.max(0, (Date.now() - t) / 1000);
  if (secs < 120) return { value: Math.round(secs), unit: "s" };
  const mins = secs / 60;
  if (mins < 120) return { value: Math.round(mins), unit: "min" };
  const hours = mins / 60;
  if (hours < 48) return { value: Math.round(hours), unit: "h" };
  return { value: Math.round(hours / 24), unit: "d" };
}

/**
 * Elapsed time since `iso` as a COMPACT age: "8s", "12min", "19h", "3d".
 * The column form — it sits in table cells and headers where width is
 * the constraint. "min", not a bare "m" that reads as mega. `—` for a
 * missing or unparseable instant.
 */
export function formatAge(iso: string | null | undefined): string {
  const age = ageParts(iso);
  if (age === null) return "—";
  return `${age.value}${age.unit}`;
}

/**
 * Elapsed time since `iso` as a PHRASE: "just now", "7 min ago",
 * "3 hours ago", "3 days ago". The prose form — it reads inside a
 * sentence (a snapshot tooltip's second line), so it spells the unit and
 * carries the "ago" that makes it a statement about the past.
 *
 * Same ladder as formatAge, so the two can never disagree about which
 * unit an instant belongs in; only the words are different. The seconds
 * bucket collapses to "just now" rather than counting them out: a
 * snapshot committed under two minutes ago has, in the only sense the
 * reader cares about, just happened — and that is also where a
 * future-dated instant lands. `—` for a missing or unparseable instant.
 */
export function formatRelativeAge(iso: string | null | undefined): string {
  const age = ageParts(iso);
  if (age === null) return "—";
  switch (age.unit) {
    case "s":
      return "just now";
    case "min":
      return `${age.value} min ago`;
    case "h":
      // The singular guards the wording against a threshold change; the
      // 2x ladder above cannot currently produce 1 here.
      return `${age.value} hour${age.value === 1 ? "" : "s"} ago`;
    case "d":
      return `${age.value} day${age.value === 1 ? "" : "s"} ago`;
  }
}

/**
 * The DOTTED PATH of the column carrying `fieldId`, searched through
 * nested children, or undefined when the schema does not hold it.
 *
 * Struct leaves are legal partition sources, and two structs may each
 * hold a `zip`: by bare name both render "zip" and the page shows one
 * table partitioned twice by the same apparent column. The server's
 * partition-stats endpoint labels them by path for the same reason.
 */
export function columnPath(
  columns: Column[] | undefined,
  fieldId: Int64,
  prefix = "",
): string | undefined {
  for (const c of columns ?? []) {
    const path = prefix ? `${prefix}.${c.name}` : c.name;
    if (c.field_id === fieldId) return path;
    const nested = columnPath(c.children, fieldId, path);
    if (nested) return nested;
  }
  return undefined;
}

/**
 * The column NODE carrying `fieldId`, nested children included, or
 * undefined when the schema does not hold it.
 *
 * Kept separate from columnPath rather than folded into it: that one
 * accumulates a dotted prefix on the way down and returns a string, and
 * a single walk returning both would make every caller unpack a pair it
 * does not want.
 */
export function findColumn(
  columns: Column[] | undefined,
  fieldId: Int64,
): Column | undefined {
  for (const c of columns ?? []) {
    if (c.field_id === fieldId) return c;
    const nested = findColumn(c.children, fieldId);
    if (nested) return nested;
  }
  return undefined;
}

/**
 * The declared TYPE of the column carrying `fieldId`, or undefined when
 * the schema does not hold it (a key column dropped since the file
 * landed, a field id from an older spec).
 */
export function columnType(
  columns: Column[] | undefined,
  fieldId: Int64,
): ColumnType | undefined {
  return findColumn(columns, fieldId)?.type;
}

/** Render a partition field, e.g. "bucket(16, field 3)" or "identity(addr.zip)". */
export function formatPartitionField(
  f: PartitionField,
  columns?: Column[],
): string {
  const source = columnPath(columns, f.source_field_id) ?? `field ${f.source_field_id}`;
  if (f.transform === "bucket" && f.transform_param !== undefined) {
    return `bucket(${f.transform_param}, ${source})`;
  }
  return `${f.transform}(${source})`;
}
