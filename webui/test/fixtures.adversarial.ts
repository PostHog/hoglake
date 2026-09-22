// Adversarial fixtures. Every string here was VERIFIED accepted by a live
// hoglake server (2026-09-05, catalog ui-qe-adv-1): only catalog names are
// validated (^[a-z0-9][a-z0-9_-]{0,62}$); namespace names, table names, column
// names, commit authors and commit messages all pass through unvalidated, so
// HTML-shaped payloads really do arrive on the wire.

import type { Namespace, SnapshotPage, Table } from "../src/api/types";

export const XSS_NS = "ns<script>alert(1)</script>";
export const XSS_AUTHOR = "<img src=x onerror=alert(1)>";
export const XSS_MESSAGE = 'msg " onmouseover="alert(2)" <svg/onload=alert(3)>';

export const hostileNamespacesFixture: Namespace[] = [
  { name: XSS_NS },
  { name: "qens" },
];

export const hostileSnapshotsFixture: SnapshotPage = {
  snapshots: [
    {
      snapshot_id: "5",
      snapshot_time: "2026-09-05T05:42:27.607655Z",
      schema_version: "4",
      author: XSS_AUTHOR,
      message: XSS_MESSAGE,
      changes: [{ kind: "table_inserted_into", object_id: "1" }],
    },
  ],
  has_more: false,
};

export const hostileTableFixture: Table = {
  name: "T<script>",
  namespace: "qens",
  table_uuid: "05af085d-5446-46d2-b080-6a9b8afc09f7",
  columns: [
    {
      field_id: "1",
      ordinal: 0,
      name: "c1 with spaces <b>",
      type: "long",
      nullable: true,
    },
  ],
  record_count: "0",
  file_count: "0",
  file_size_bytes: "0",
};

/**
 * Raw wire body for GET .../files carrying int64 values above 2^53, exactly
 * as the live server emits them (bare JSON numbers, not strings). This MUST
 * stay a string literal: writing these numbers as JS literals would already
 * corrupt them before the test runs.
 *
 * record_count   = 2^53 + 1 = 9007199254740993
 * file_size_bytes= 2^62 + 1 = 4611686018427387905
 * row_id_start   = 2^53 + 3 = 9007199254740995
 *
 * The table is unsorted, so the ordering key is the row id and the
 * server states the span it computes from those two: lower
 * 9007199254740995, upper row_id_start + record_count - 1 =
 * 18014398509481987. Both ends are the columns the files table renders,
 * and both are past 2^53. The file's stats are PENDING and it has a
 * span anyway — row_id_start is assigned at commit for every file,
 * deferred stats included.
 */
export const bigIntFilesWireBody =
  '[{"data_file_id":1,' +
  '"path":"s3://ui-qe/adv1/qens/t1/f1.parquet",' +
  '"file_format":"parquet",' +
  '"record_count":9007199254740993,' +
  '"file_size_bytes":4611686018427387905,' +
  '"row_id_start":9007199254740995,' +
  '"stats_state":"pending",' +
  '"ordering_bounds":{"lower_bound":9007199254740995,' +
  '"upper_bound":18014398509481987},' +
  '"begin_snapshot":5}]';

/**
 * Three files in ASCENDING size order, two of which are the SAME double:
 * 2^53 and 2^53+1 both round to 2^53, while 2^53+3 rounds to 2^53+4.
 *
 * The collision is the whole point and it has to be checked rather than
 * assumed — consecutive integers up here do NOT generally collide
 * (2^53+1, +2, +3 land on three distinct doubles), so a fixture picked
 * by eye passes against a lossy comparator and proves nothing.
 *
 * Sorting descending must give f3, f2, f1. A comparator that goes
 * through Number sees f1 and f2 as equal, and the stable sort then
 * leaves them in the order they arrived: f3, f1, f2.
 */
export const closeBigIntFilesWireBody =
  '[{"data_file_id":1,"path":"s3://ui-qe/adv/f1.parquet",' +
  '"file_format":"parquet","record_count":10,' +
  '"file_size_bytes":9007199254740992,"row_id_start":0,' +
  '"stats_state":"provided",' +
  '"ordering_bounds":{"lower_bound":0,"upper_bound":9},' +
  '"begin_snapshot":1},' +
  '{"data_file_id":2,"path":"s3://ui-qe/adv/f2.parquet",' +
  '"file_format":"parquet","record_count":10,' +
  '"file_size_bytes":9007199254740993,"row_id_start":10,' +
  '"stats_state":"provided",' +
  '"ordering_bounds":{"lower_bound":10,"upper_bound":19},' +
  '"begin_snapshot":2},' +
  '{"data_file_id":3,"path":"s3://ui-qe/adv/f3.parquet",' +
  '"file_format":"parquet","record_count":10,' +
  '"file_size_bytes":9007199254740995,"row_id_start":20,' +
  '"stats_state":"provided",' +
  '"ordering_bounds":{"lower_bound":20,"upper_bound":29},' +
  '"begin_snapshot":3}]';

/**
 * Raw wire bodies for a catalog whose head snapshot id exceeds 2^53 —
 * head = 2^53+5 (odd: any Number round-trip would flip it to 2^53+4).
 * Same string-literal rule as above.
 *
 * head_snapshot_id = 9007199254740997
 * page 1 (before=9007199254740998): ids 9007199254740997, 9007199254740995
 */
export const bigIntCatalogWireBody =
  '{"name":"bigcat",' +
  '"data_path":"s3://ui-qe/bigcat",' +
  '"head_snapshot_id":9007199254740997,' +
  '"schema_version":3}';

export const bigIntSnapshotsPage1WireBody =
  '{"snapshots":[' +
  '{"snapshot_id":9007199254740997,' +
  '"snapshot_time":"2026-09-05T00:00:02Z","schema_version":3},' +
  '{"snapshot_id":9007199254740995,' +
  '"snapshot_time":"2026-09-05T00:00:01Z","schema_version":3}' +
  '],"has_more":true}';

/**
 * Raw catalog-listing wire body whose live_rows exceeds 2^53, as an
 * UNQUOTED JSON number — the form the server actually sends.
 *
 * A fixture that already holds the string would prove nothing: the
 * precision is lost (or not) in the JSON parse, so the test has to go
 * through it. 9007199254740993 is odd, so any trip through a JS number
 * flips it to ...92 and the assertion fails.
 */
export const bigIntCatalogsWireBody =
  '[{"name":"analytics",' +
  '"data_path":"s3://hog-lake/analytics",' +
  '"head_snapshot_id":4211,' +
  '"schema_version":7,' +
  '"table_count":42,' +
  '"live_rows":9007199254740993,' +
  '"live_size_bytes":5368709120}]';
