import { readFileSync } from "node:fs";
import { resolve } from "node:path";
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
 *
 * THE decodeValue CASES ARE NOT BELOW — they are in
 * `server/src/test/resources/vectors/partition_decode_vectors.json`,
 * read by this file and by
 * `server/src/test/kotlin/com/posthog/hoglake/model/PartitionValueDecodingTest.kt`,
 * both of which pin its `count`. There are two implementations of one
 * rule because the partitions tab's `filter=` and its default sort are
 * decoded SERVER-side (they happen before paging) while the files tab
 * decodes in the browser; the one file is what keeps them equal, and
 * `.github/workflows/webui.yml` triggers this suite on a change to it.
 * Add a case to the JSON, not to either test.
 *
 * ONE DELIBERATE DIFFERENCE: a null stored value renders as the literal
 * "null" here, because a table cell has to show something, and comes
 * back as a real null from the server, because the listing sorts nulls
 * first and matches them with an empty filter.
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

/**
 * THE SHARED TABLE, parsed rather than restated. The same file drives
 * `server/src/test/kotlin/com/posthog/hoglake/model/PartitionValueDecodingTest.kt`,
 * and both sides pin `count`, so a vector added to the file and to only
 * one side's expectations cannot pass unnoticed.
 */
const VECTORS: {
  count: number;
  vectors: {
    transform: string;
    raw: string;
    transform_param?: number;
    expected: string;
  }[];
} = JSON.parse(
  // Resolved from the vitest root (`webui/`), not from import.meta.url:
  // vitest serves transformed modules over a non-file URL, so
  // `new URL(..., import.meta.url)` is not a path here.
  readFileSync(
    resolve(
      process.cwd(),
      "../server/src/test/resources/vectors/partition_decode_vectors.json",
    ),
    "utf8",
  ),
);

describe("transform decoding, against the shared vectors", () => {
  it("the vector file's count matches its contents", () => {
    expect(VECTORS.vectors).toHaveLength(VECTORS.count);
  });

  it.each(VECTORS.vectors)(
    "$transform($raw) = $expected",
    ({ transform, raw, transform_param, expected }) => {
      expect(decodeValue(transform, raw, transform_param)).toBe(expected);
    },
  );

  it("renders a null stored value as the literal 'null'", () => {
    // The one deliberate divergence from the server, which returns a
    // real null so the listing can sort nulls first and match them
    // with an empty filter. A table cell has to show something.
    expect(decodeValue("month", null)).toBe("null");
    expect(decodeValue("identity", null)).toBe("null");
    expect(decodeValue("bucket", null, 16)).toBe("null");
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
