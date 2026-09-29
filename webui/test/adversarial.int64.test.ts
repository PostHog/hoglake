// Int64 precision audit.
//
// The OpenAPI spec types snapshot_id, row_id_start, record_count,
// file_size_bytes, head_snapshot_id, committed_snapshot (and more) as
// `integer, format: int64`, and the live server really emits values above
// 2^53 as bare JSON numbers (verified 2026-09-05: a commit with
// record_count=2^53+1 / file_size_bytes=2^62+1 round-trips intact through
// the server). The client therefore parses response bodies from the raw
// text (JSON.parse reviver with source access, src/api/int64.ts) and
// carries every int64 field as its exact decimal string — never through
// Number. These are the pinned regressions.

import { describe, expect, it, vi } from "vitest";
import {
  createTable,
  listFiles,
  listSnapshots,
  listTablePartitions,
} from "../src/api/client";
import { addInt64, compareInt64, parseInt64Json } from "../src/api/int64";
import { formatCount } from "../src/lib/format";
import {
  bigIntFilesWireBody,
  bigIntSnapshotsPage1WireBody,
  bigIntTableWireBody,
} from "./fixtures.adversarial";

function stubFetchRaw(body: string) {
  const fn = vi.fn(
    async () =>
      new Response(body, {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
  );
  vi.stubGlobal("fetch", fn);
  return fn;
}

describe("int64 wire values above 2^53", () => {
  it("record_count survives JSON parsing losslessly", async () => {
    stubFetchRaw(bigIntFilesWireBody);
    const files = await listFiles("c", "ns", "t");
    expect(String(files[0].record_count)).toBe("9007199254740993");
  });

  it("file_size_bytes survives JSON parsing losslessly", async () => {
    stubFetchRaw(bigIntFilesWireBody);
    const files = await listFiles("c", "ns", "t");
    expect(String(files[0].file_size_bytes)).toBe("4611686018427387905");
  });

  // snapshot_id is the post-DDL read pin (#35): a client reads at exactly
  // this snapshot, so rounding it points the read at the wrong one. 2^53+1
  // flips to 2^53+2 under a Number parse, so the exact string is the proof.
  // Exercised through createTable — the DDL path the server actually sends
  // snapshot_id on (getTable, a read, never carries it).
  it("snapshot_id survives JSON parsing losslessly", async () => {
    stubFetchRaw(bigIntTableWireBody);
    const table = await createTable("c", "ns", {
      name: "events",
      columns: [{ name: "id", type: "long" }],
    });
    expect(table.snapshot_id).toBe("9007199254740993");
  });

  // The partitions listing's own int64s. `last_written_snapshot` is a
  // snapshot id the console displays verbatim, and `record_count` and
  // `total_bytes` are per-partition sums that exceed 2^53 at fleet
  // scale. Removing any of them from INT64_FIELDS reds this.
  it("the partitions listing's int64 fields survive JSON parsing losslessly", async () => {
    // A RAW body, hand-written: JSON.stringify would round these
    // through Number before the client ever saw them, which is the
    // very loss this test exists to catch.
    stubFetchRaw(`{
      "sampled_at": "2026-09-29T12:00:00Z",
      "sample_started": "2026-09-29T11:32:00Z",
      "sampled_snapshot_id": 9007199254740993,
      "total": 1,
      "stale_spec_groups": 0,
      "partitions": [
        {
          "spec_id": 1,
          "values": [{ "field": "ts_day", "raw": "20713", "decoded": "2026-09-17" }],
          "file_count": 3,
          "small_file_count": 2,
          "total_bytes": 4611686018427387905,
          "small_file_bytes": 100,
          "avg_file_bytes": 1537228672809129301,
          "dv_count": 0,
          "debt_score": 2,
          "record_count": 9007199254740995,
          "last_written_snapshot": 9007199254740993
        }
      ]
    }`);
    const listing = await listTablePartitions("c", "ns", "t");
    expect(listing.sampled_snapshot_id).toBe("9007199254740993");
    const p = listing.partitions[0];
    expect(p.last_written_snapshot).toBe("9007199254740993");
    expect(p.record_count).toBe("9007199254740995");
    expect(p.total_bytes).toBe("4611686018427387905");
    expect(p.avg_file_bytes).toBe("1537228672809129301");
  });

  // A null on those two is "the sample never measured this" and must
  // survive as null rather than becoming the string "null".
  it("a null record_count and last_written_snapshot stay null", async () => {
    stubFetchRaw(
      JSON.stringify({
        sampled_at: null,
        sample_started: null,
        sampled_snapshot_id: null,
        total: 0,
        stale_spec_groups: 0,
        partitions: [
          {
            values: [],
            file_count: 1,
            small_file_count: 1,
            total_bytes: 1,
            small_file_bytes: 1,
            avg_file_bytes: 1,
            dv_count: 0,
            debt_score: 0,
            record_count: null,
            last_written_snapshot: null,
          },
        ],
      }),
    );
    const listing = await listTablePartitions("c", "ns", "t");
    expect(listing.sampled_snapshot_id).toBeNull();
    expect(listing.partitions[0].record_count).toBeNull();
    expect(listing.partitions[0].last_written_snapshot).toBeNull();
  });

  // row_id_start is the lineage anchor; 2^53+3 must not round to 2^53+4.
  it("row_id_start survives JSON parsing losslessly", async () => {
    stubFetchRaw(bigIntFilesWireBody);
    const files = await listFiles("c", "ns", "t");
    expect(String(files[0].row_id_start)).toBe("9007199254740995");
  });

  it("formats counts above 2^53 exactly (no toLocaleString double round-trip)", async () => {
    stubFetchRaw(bigIntFilesWireBody);
    const files = await listFiles("c", "ns", "t");
    expect(formatCount(files[0].record_count)).toBe("9,007,199,254,740,993");
  });

  // A snapshot id from a response page must be echoed EXACTLY when used as
  // the next `before` cursor — the whole point of string-typed ids.
  it("echoes a >2^53 snapshot id verbatim as the next before cursor", async () => {
    const fetchMock = stubFetchRaw(bigIntSnapshotsPage1WireBody);
    const page = await listSnapshots("bigcat", {
      before: addInt64("9007199254740997", 1), // head+1, exact
      limit: 50,
    });
    expect(fetchMock).toHaveBeenCalledWith(
      "/v1/catalogs/bigcat/snapshots?before=9007199254740998&limit=50",
      expect.anything(),
    );
    const cursor = page.snapshots[page.snapshots.length - 1].snapshot_id;
    expect(cursor).toBe("9007199254740995");
    await listSnapshots("bigcat", { before: cursor, limit: 50 });
    expect(fetchMock).toHaveBeenLastCalledWith(
      "/v1/catalogs/bigcat/snapshots?before=9007199254740995&limit=50",
      expect.anything(),
    );
  });

  // Documents the exact corruption mode being defended against: bare
  // JSON.parse rounds to the nearest double, so odd values above 2^53
  // become even neighbours. If this ever fails, the JS engine changed.
  it("JSON.parse rounds int64 (the mechanism being pinned)", () => {
    expect(JSON.parse("9007199254740993")).toBe(9007199254740992);
    expect(JSON.parse("4611686018427387905")).toBe(4611686018427387904);
  });
});

describe("parseInt64Json", () => {
  it("stringifies int64 fields, leaves other types alone", () => {
    const parsed = parseInt64Json(
      '{"snapshot_id":9007199254740993,"ordinal":3,"name":"x",' +
        '"nullable":true,"record_count":"7","object_id":null}',
    ) as Record<string, unknown>;
    expect(parsed.snapshot_id).toBe("9007199254740993");
    expect(parsed.ordinal).toBe(3); // int32 field stays a number
    expect(parsed.name).toBe("x");
    expect(parsed.nullable).toBe(true);
    expect(parsed.record_count).toBe("7"); // already-string passes through
    expect(parsed.object_id).toBeNull();
  });

  it("does not rewrite int64-looking text inside string values", () => {
    const parsed = parseInt64Json(
      '{"message":"saw {\\"record_count\\":9007199254740993} upstream"}',
    ) as Record<string, unknown>;
    expect(parsed.message).toBe(
      'saw {"record_count":9007199254740993} upstream',
    );
  });
});

describe("int64 string helpers", () => {
  it("addInt64 is exact above 2^53", () => {
    expect(addInt64("9007199254740993", 1)).toBe("9007199254740994");
    expect(addInt64("4611686018427387905", 1)).toBe("4611686018427387906");
  });

  it("compareInt64 orders numerically, not lexically", () => {
    expect(compareInt64("9", "10")).toBe(-1);
    expect(compareInt64("9007199254740993", "9007199254740992")).toBe(1);
    expect(compareInt64("5", "5")).toBe(0);
  });
});
