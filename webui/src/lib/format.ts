import type { Column, Int64, PartitionField } from "../api/types";

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
 * Elapsed time since `iso`, to a SINGLE rounded unit: "8s", "12min",
 * "19h", "3d". An age here places a snapshot in time — the difference
 * between 19h and 19h 11min never matters, and the second unit is just
 * noise. Computed from `Date.now()` at render, so it stays live between
 * refetches. `—` for a missing or unparseable instant.
 *
 * Units and 2x thresholds match the maintenance page's formatSeconds
 * (up to 119min before switching to hours, 47h before days): "min", not
 * a bare "m" that reads as mega. A future instant (client clock skew)
 * clamps to "0s" rather than a negative age.
 */
export function formatAge(iso: string | null | undefined): string {
  if (iso === null || iso === undefined) return "—";
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return "—";
  const secs = Math.max(0, (Date.now() - t) / 1000);
  if (secs < 120) return `${Math.round(secs)}s`;
  const mins = secs / 60;
  if (mins < 120) return `${Math.round(mins)}min`;
  const hours = mins / 60;
  if (hours < 48) return `${Math.round(hours)}h`;
  return `${Math.round(hours / 24)}d`;
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
