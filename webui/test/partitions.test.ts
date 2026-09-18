import { describe, expect, it } from "vitest";
import { decodePartition, decodeValue } from "../src/lib/partitions";
import type { Column, DataFile, PartitionSpec } from "../src/api/types";

/**
 * The decoder turns a stored tuple into something an operator can act
 * on, so a WRONG label here is worse than the raw tuple it replaces:
 * the reader has no way to tell a confident mislabel from the truth.
 * These cases pin the arithmetic against the writer's own definitions
 * (pyhoglake/transforms.py) and the refusals against the cases where the
 * page does not hold enough information to decode honestly.
 */

const columns: Column[] = [
  { name: "team_id", type: "long", field_id: "1", ordinal: 0, nullable: false },
  { name: "timestamp", type: "timestamptz", field_id: "2", ordinal: 1, nullable: false },
];

const spec: PartitionSpec = {
  spec_id: "7",
  fields: [
    { source_field_id: "1", transform: "identity" },
    { source_field_id: "2", transform: "month" },
  ],
};

function file(over: Partial<DataFile> = {}): DataFile {
  return {
    data_file_id: "13",
    path: "s3://b/t/f.parquet",
    file_format: "parquet",
    record_count: "1",
    file_size_bytes: "1",
    row_id_start: "0",
    stats_state: "provided",
    begin_snapshot: "20",
    spec_id: "7",
    partition_values: ["42", "675"],
    ...over,
  };
}

describe("transform decoding", () => {
  it("month is months since 1970-01, matching the writer", () => {
    // The value from the screenshot that prompted this: 675 reads as a
    // number and means April 2026.
    expect(decodeValue("month", "675")).toBe("2026-04");
    expect(decodeValue("month", "0")).toBe("1970-01");
    expect(decodeValue("month", "11")).toBe("1970-12");
    expect(decodeValue("month", "12")).toBe("1971-01");
  });

  it("month floors below the epoch instead of producing month 0 or -1", () => {
    // Negative ordinals are legal (the writer floors), and naive
    // arithmetic yields "1970--1" or month 0 here.
    expect(decodeValue("month", "-1")).toBe("1969-12");
    expect(decodeValue("month", "-12")).toBe("1969-01");
    expect(decodeValue("month", "-13")).toBe("1968-12");
  });

  it("year is the calendar year, not an offset", () => {
    expect(decodeValue("year", "56")).toBe("2026");
    expect(decodeValue("year", "0")).toBe("1970");
    expect(decodeValue("year", "-1")).toBe("1969");
  });

  it("day and hour are epoch-relative", () => {
    expect(decodeValue("day", "0")).toBe("1970-01-01");
    expect(decodeValue("day", "20713")).toBe("2026-09-17");
    expect(decodeValue("day", "-1")).toBe("1969-12-31");
    expect(decodeValue("hour", "0")).toBe("1970-01-01T00");
    expect(decodeValue("hour", "25")).toBe("1970-01-02T01");
  });

  it("bucket says it is a bucket index, not a value", () => {
    // Rendered bare, "3" reads as data rather than as a hash bucket.
    expect(decodeValue("bucket", "3", 16)).toBe("bucket 3/16");
    expect(decodeValue("bucket", "3")).toBe("bucket 3");
  });

  it("identity and unknown transforms pass the value through", () => {
    expect(decodeValue("identity", "42")).toBe("42");
    // A transform a newer server added: the stored value is still the
    // best answer available, and inventing one would be worse.
    expect(decodeValue("truncate", "abc")).toBe("abc");
  });

  it("nulls and unusable values degrade to what was stored", () => {
    expect(decodeValue("month", null)).toBe("null");
    expect(decodeValue("identity", null)).toBe("null");
    // A non-numeric ordinal must not become a date; showing the raw
    // string is the honest failure.
    expect(decodeValue("month", "not-a-number")).toBe("not-a-number");
    // Beyond Number.MAX_SAFE_INTEGER the arithmetic would silently lose
    // precision, so it is refused too.
    expect(decodeValue("day", "9007199254740993")).toBe("9007199254740993");
  });
});

describe("decoding a file against a spec", () => {
  it("labels each element with its column and transform", () => {
    const got = decodePartition(file(), spec, columns);
    expect(got.kind).toBe("decoded");
    if (got.kind !== "decoded") return;
    expect(got.values).toEqual([
      { field: "team_id", transform: "identity", raw: "42", display: "42" },
      // "timestamp_month", not "timestamp": the server names partition
      // fields this way, and the bare column would suggest the column
      // itself holds a month.
      { field: "timestamp_month", transform: "month", raw: "675", display: "2026-04" },
    ]);
  });

  it("refuses to decode a file written under a different spec", () => {
    // The page only ever holds head's spec. Labelling this file's tuple
    // with head's field names could name the wrong column entirely,
    // which is the one outcome worse than the raw tuple.
    const got = decodePartition(file({ spec_id: "3" }), spec, columns);
    expect(got.kind).toBe("foreign-spec");
    if (got.kind !== "foreign-spec") return;
    expect(got.raw).toBe("[42, 675]");
    expect(got.specId).toBe("3");
  });

  it("treats a file with no spec_id as decodable against the current spec", () => {
    // Older servers omit spec_id. Refusing to decode then would make the
    // column useless against them, and the single-spec case — no
    // evolution — is both the common one and safe.
    expect(decodePartition(file({ spec_id: undefined }), spec, columns).kind).toBe(
      "decoded",
    );
  });

  it("refuses when the tuple's arity disagrees with the spec", () => {
    const got = decodePartition(file({ partition_values: ["42"] }), spec, columns);
    expect(got.kind).toBe("mismatched");
    if (got.kind !== "mismatched") return;
    expect(got.raw).toBe("[42]");
  });

  it("reports an unpartitioned file as such, not as an empty decode", () => {
    expect(decodePartition(file({ partition_values: undefined }), spec, columns).kind).toBe(
      "unpartitioned",
    );
    expect(decodePartition(file({ partition_values: [] }), spec, columns).kind).toBe(
      "unpartitioned",
    );
  });

  it("falls back to the raw tuple when no spec is available at all", () => {
    expect(decodePartition(file(), undefined, columns).kind).toBe("mismatched");
  });

  it("names a field by id when the column cannot be resolved", () => {
    // A dropped column keeps its field id in old files' specs;
    // "field_9" is honest where a blank or a wrong name would not be,
    // and matches the server's own fallback.
    const orphan: PartitionSpec = {
      spec_id: "7",
      fields: [
        { source_field_id: "9", transform: "identity" },
        { source_field_id: "2", transform: "month" },
      ],
    };
    const got = decodePartition(file(), orphan, columns);
    if (got.kind !== "decoded") throw new Error("expected decoded");
    expect(got.values[0].field).toBe("field_9");
  });

  it("renders a null partition value as null, not as a missing element", () => {
    const got = decodePartition(file({ partition_values: [null, "675"] }), spec, columns);
    if (got.kind !== "decoded") throw new Error("expected decoded");
    expect(got.values[0].display).toBe("null");
    expect(got.values[0].raw).toBeNull();
  });
});
