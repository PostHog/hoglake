"""Cross-client partition-wire assertion.

The wire contract groups partition values by OPAQUE STRING EQUALITY, so
two clients writing the same logical partition value must register
byte-identical partition_values or the partition silently fragments
(compaction never merges across the split; /stats/partitions
double-counts). read_fixture.py writes the xclient_* tables via
pyhoglake; the extension's hoglake_types.test inserts the SAME logical
key values; this script asserts each table's files collapse to exactly
the expected distinct partition strings, i.e. both clients landed in
the same groups.

Run after the sqllogictests (see run-live-tests.sh).
"""

import os
import sys
from datetime import datetime

from pyhoglake import HoglakeClient
from pyhoglake.transforms import wire_string

HOGLAKE_URL = os.environ.get("HOGLAKE_URL", "http://localhost:8080")
CATALOG = "duckext-read"

# per table: column type, the logical key values BOTH clients wrote
EXPECTED = {
    "xclient_int": ("long", [7, 42]),
    "xclient_date": ("date", [datetime(2026, 3, 1).date(), datetime(2026, 3, 2).date()]),
    "xclient_ts": (
        "timestamp",
        [datetime(2026, 1, 2, 3, 4, 5, 900000), datetime(2026, 1, 2, 3, 4, 5)],
    ),
    "xclient_bool": ("boolean", [True, False]),
}


def main() -> int:
    failures = 0
    with HoglakeClient(HOGLAKE_URL) as client:
        ns = client.catalog(CATALOG).namespace("ns1")
        for tname, (col_type, values) in EXPECTED.items():
            expected_strings = {wire_string(col_type, "identity", v) for v in values}
            table = ns.table(tname)
            files = table.files()
            seen: dict[str, int] = {}
            compacted = False
            for f in files:
                assert f.partition_values is not None, f"{tname}: unpartitioned file {f.path}"
                if getattr(f, "explicit_row_ids", False):
                    compacted = True
                key = f.partition_values[0]
                seen[key] = seen.get(key, 0) + 1
            # both clients wrote every value => NO unexpected string
            # exists (a divergent encoding would show up as an extra
            # group) and every expected string is present. The >=2-files
            # check additionally proves both clients landed in ONE
            # group, but only while the background compactor has not
            # merged them (compaction outputs carry explicit_row_ids and
            # collapse each group to one file — which is itself proof
            # the strings were byte-identical, since compaction groups
            # by exact spec+value equality).
            extra = set(seen) - expected_strings
            missing = expected_strings - set(seen)
            single = {} if compacted else {k: v for k, v in seen.items() if v < 2}
            if extra or missing or single:
                failures += 1
                print(f"FAIL {tname}:")
                if extra:
                    print(f"  unexpected partition strings (client divergence): {sorted(extra)}")
                if missing:
                    print(f"  missing partition strings: {sorted(missing)}")
                if single:
                    print(f"  groups with files from only one client: {single}")
            else:
                print(f"ok {tname}: {len(files)} files in {len(seen)} shared groups {sorted(seen)}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
