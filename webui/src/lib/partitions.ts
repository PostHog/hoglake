import type {
  Column,
  DataFile,
  Int64,
  PartitionField,
  PartitionSpec,
} from "../api/types";
import { columnPath } from "./format";

/**
 * Decoding a file's partition tuple against the spec that produced it.
 *
 * The stored tuple is compact and correct — `[42, 675]` — and unreadable
 * without three lookups the page would otherwise leave to the reader:
 * which column each element belongs to, in what order, and under which
 * transform. `675` is not a number anyone can act on; `month=2026-04`
 * is.
 *
 * The transform semantics are pyhoglake's (`transforms.py`), which is
 * the writer that produces these values:
 *
 *   year   calendar year - 1970
 *   month  (year - 1970) * 12 + (month - 1)   — months since 1970-01
 *   day    days since the epoch
 *   hour   hours since the epoch
 *
 * All four floor, so pre-epoch values are negative and must render as
 * dates before 1970 rather than as nonsense.
 */

/** One decoded element: the column it came from, and a readable value. */
export interface DecodedPartitionValue {
  /** Column name where resolvable, else `field <id>` — never blank. */
  field: string;
  transform: string;
  /** The stored element, exactly as the server sent it. */
  raw: string | null;
  /** Human form: a date for temporal transforms, the value otherwise. */
  display: string;
}

export type PartitionDecode =
  | { kind: "unpartitioned" }
  /** Decoded against the spec that matches the file's spec_id. */
  | { kind: "decoded"; values: DecodedPartitionValue[] }
  /**
   * The tuple exists but must not be labelled: the only spec the page
   * holds is head's, and this file was written under a different one.
   * Mislabelling a field is worse than showing the raw tuple, so the
   * raw tuple is what this carries.
   */
  | { kind: "foreign-spec"; raw: string; specId: Int64 }
  /** Arity disagrees with the spec — the join would be a guess. */
  | { kind: "mismatched"; raw: string };

const MS_PER_DAY = 86_400_000;
const MS_PER_HOUR = 3_600_000;

/** `YYYY-MM-DD` in UTC. Partition ordinals are epoch-relative, never local. */
function isoDate(ms: number): string {
  return new Date(ms).toISOString().slice(0, 10);
}

/**
 * Render one element under its transform. Returns the raw string when a
 * value cannot be interpreted — a malformed ordinal must degrade to what
 * was stored, never to a wrong date.
 */
export function decodeValue(
  transform: string,
  raw: string | null,
  transformParam?: number,
): string {
  if (raw === null) return "null";
  const n = Number(raw);
  const usable = Number.isSafeInteger(n);
  switch (transform) {
    case "year":
      return usable ? String(1970 + n) : raw;
    case "month": {
      if (!usable) return raw;
      // Floor division, so months before 1970-01 land in the right year:
      // -1 is 1969-12, not 1970--1.
      const year = 1970 + Math.floor(n / 12);
      const month = ((n % 12) + 12) % 12;
      return `${year}-${String(month + 1).padStart(2, "0")}`;
    }
    case "day":
      return usable ? isoDate(n * MS_PER_DAY) : raw;
    case "hour": {
      if (!usable) return raw;
      const at = new Date(n * MS_PER_HOUR);
      return `${isoDate(n * MS_PER_HOUR)}T${String(at.getUTCHours()).padStart(2, "0")}`;
    }
    case "bucket":
      // The bucket INDEX, not a value: say so, or it reads as data.
      return transformParam === undefined
        ? `bucket ${raw}`
        : `bucket ${raw}/${transformParam}`;
    default:
      // identity, and anything a newer server adds: the stored value is
      // already the answer.
      return raw;
  }
}

/**
 * The partition field's name, using the SERVER's convention
 * (PartitionStatsService.partitionFieldNames): the bare column for an
 * identity transform, `column_transform` otherwise.
 *
 * Matching it matters twice over — the partitions page already labels
 * values this way, so the two pages read alike; and `ts_month` carries
 * the transform, without which `ts=2026-04` invites the reader to think
 * the column holds a month.
 */
function fieldLabel(field: PartitionField, columns?: Column[]): string {
  const column =
    columnPath(columns, field.source_field_id) ?? `field_${field.source_field_id}`;
  return field.transform === "identity" ? column : `${column}_${field.transform}`;
}

function rawTuple(values: (string | null)[]): string {
  return `[${values.map((v) => v ?? "null").join(", ")}]`;
}

/**
 * Decode a file's partition tuple, or explain why it was not decoded.
 *
 * [spec] is the table's CURRENT partition spec, which is all the webui
 * has: there is no endpoint that fetches a spec by id. So a file whose
 * `spec_id` differs is reported as `foreign-spec` rather than decoded
 * against a spec it was not written under — the failure mode the issue
 * names, and the one where a confident wrong label would be worse than
 * the tuple it replaced.
 */
export function decodePartition(
  file: DataFile,
  spec: PartitionSpec | undefined,
  columns?: Column[],
): PartitionDecode {
  const values = file.partition_values;
  if (!values || values.length === 0) return { kind: "unpartitioned" };
  if (!spec || spec.fields.length === 0) {
    return { kind: "mismatched", raw: rawTuple(values) };
  }
  // Int64 values arrive as strings or numbers depending on magnitude;
  // compare as strings so 3 and "3" agree.
  if (file.spec_id !== undefined && String(file.spec_id) !== String(spec.spec_id)) {
    return { kind: "foreign-spec", raw: rawTuple(values), specId: file.spec_id };
  }
  if (values.length !== spec.fields.length) {
    return { kind: "mismatched", raw: rawTuple(values) };
  }
  return {
    kind: "decoded",
    values: spec.fields.map((field, i) => ({
      field: fieldLabel(field, columns),
      transform: field.transform,
      raw: values[i],
      display: decodeValue(field.transform, values[i], field.transform_param),
    })),
  };
}
