# Foreign-writer fixtures

Generated with DuckDB 1.5.5. These small files carry what another writer
actually emits — Parquet annotations and footer statistics pyarrow will not
reproduce — without adding DuckDB to pyhoglake's runtime/test dependencies.

```sql
COPY (SELECT 1::BIGINT id, {'a':42,'nested':[true,NULL]}::VARIANT properties)
TO 'native_variant.parquet' (FORMAT PARQUET, FIELD_IDS {id:1,properties:2});
COPY (SELECT NULL::BIGINT id, {'a':42}::VARIANT properties)
TO 'native_variant_null_id.parquet' (FORMAT PARQUET, FIELD_IDS {id:1,properties:2});
COPY (SELECT 1::BIGINT id, {'a':42,'nested':[true,NULL]}::VARIANT properties,
      {'x':7::INT} s)
TO 'native_variant_struct.parquet'
  (FORMAT PARQUET, FIELD_IDS {id:1,properties:2,s:{__duckdb_field_id:3,x:4}});
```

The third fixture holds a variant and a container in one file, which pyarrow
cannot write: it reads the VARIANT annotation but drops it on write. Every
level is OPTIONAL, as DuckDB always writes, so it also exercises the footer
proof that a required catalog column has no nulls.

The server's `variant/native_variant.parquet` resource is byte-identical to the
first fixture and exercises parquet-java's physical schema handling.

Ten files are that first fixture with its footer edited by
`tests/footer_oracle.py` (Thrift's writer, so no pyarrow or DuckDB version
enters the bytes), run from `tests/`:

```python
import footer_oracle as fo
from thrift.Thrift import TType

raw = open("data/native_variant.parquet", "rb").read()
edits = {
    # A VariantType with no specification_version.
    "unversioned": (
        ("properties",),
        lambda e: fo.put(e, 10, TType.STRUCT, [[16, TType.STRUCT, []]]),
    ),
    # A converted_type MAP beside the VARIANT logical type.
    "converted_map": (("properties",), lambda e: fo.put(e, 6, TType.I32, 1)),
    # A physical type (BYTE_ARRAY) beside the group's children.
    "typed_group": (("properties",), lambda e: fo.put(e, 1, TType.I32, 6)),
    # A converted_type ConvertedType does not define (it has 0 to 21).
    "converted_undefined": (("properties",), lambda e: fo.put(e, 6, TType.I32, 22)),
    # No repetition_type on the group's metadata.
    "no_repetition": (("properties", "metadata"), lambda e: fo.drop(e, 3)),
    # A physical type (BYTE_ARRAY) on a group below the VARIANT group.
    "typed_subgroup": (
        ("properties", "typed_value"),
        lambda e: fo.put(e, 1, TType.I32, 6),
    ),
    # A LogicalType union of two fields: a STRING spelled as a bool, which
    # Thrift's generated readers skip, then VARIANT(1).
    "union_two_fields": (
        ("properties",),
        lambda e: fo.put(
            e,
            10,
            TType.STRUCT,
            [[1, TType.BOOL, True], [16, TType.STRUCT, [[1, TType.BYTE, 1]]]],
        ),
    ),
    # id's LogicalType an INTEGER spelled as a bool, alone, which Thrift's
    # generated readers skip: a union with no member set.
    "lone_member": (
        ("id",),
        lambda e: fo.put(e, 10, TType.STRUCT, [[10, TType.BOOL, True]]),
    ),
    # A logical TIMESTAMP(true, MICROS) beside id's converted INT_64.
    "logical_beside_converted": (
        ("id",),
        lambda e: fo.put(
            e,
            10,
            TType.STRUCT,
            [
                [
                    8,
                    TType.STRUCT,
                    [
                        [1, TType.BOOL, True],
                        [2, TType.STRUCT, [[2, TType.STRUCT, []]]],
                    ],
                ]
            ],
        ),
    ),
    # The int32 field a's converted INT_32 made UTF8, beside a logical
    # INT(32, true).
    "converted_unbuildable": (
        ("properties", "typed_value", "a", "typed_value"),
        lambda e: (
            fo.put(e, 6, TType.I32, 0),
            fo.put(
                e,
                10,
                TType.STRUCT,
                [
                    [
                        10,
                        TType.STRUCT,
                        [[1, TType.BYTE, 32], [2, TType.BOOL, True]],
                    ]
                ],
            ),
        ),
    ),
}
for name, (path, edit) in edits.items():
    forged = fo.edit_element(raw, path, edit)
    open(f"data/native_variant_{name}.parquet", "wb").write(forged)
```

pyarrow shows every one of the ten groups as Variant(1). It reads
`no_repetition`'s metadata as REQUIRED, `typed_subgroup`'s typed_value as a
group, `union_two_fields`' union as VARIANT(1), skipping the bool,
`lone_member`'s id as a plain int64, `logical_beside_converted`'s as a
timestamp, and `converted_unbuildable`'s field a as an int32: the logical
type, where there is one. The server's hydrator, through parquet-java,
reads `unversioned` as VARIANT(0) and `converted_map` as a MAP, and warns
of both, and reads `converted_undefined` as VARIANT(1), since
`ConvertedType.findByValue` reads 22 as unset; `lone_member`'s id as the
INTEGER(64, true) its converted type gives, as it reads a union with no
member set as none; and `logical_beside_converted`'s id as INTEGER(64,
true) too, letting the converted type win over a logical type that spells
another. It cannot parse the other five: an element with a physical type
is a primitive to it, whatever children it has ("VARIANT(1) can not be
applied to a primitive type" for `typed_group`, "Arrived at primitive
node" for `typed_subgroup`); `repetition_type.name()` on null is a
NullPointerException (`no_repetition`), as in the Trino connector's
`ParquetMetadata`; libthrift's `TUnion` reads a union's first field and
takes the next field header for its end, so `union_two_fields`' footer is
read out of step ("can not read class FileMetaData"); and the converted
UTF8 that wins in `converted_unbuildable` is one its schema builder will
not put on an INT32 ("STRING can only annotate BINARY"). The connector
cannot read `lone_member` either: it switches over the union's member with
no case for none. pyhoglake accepts `converted_undefined` and refuses the
other nine on every path. All ten are also server resources under
`variant/`, byte-identical: `FooterStatsTest` asserts the hydrator's
reading of each, and
`test_variant_schema.test_the_server_reads_the_same_variant_files` that the
copies stay the same.

```sql
COPY (
  SELECT id, v FROM (VALUES
    (1, '{"z":1,"a":{"y":true,"b":"x"},"m":[{"q":2,"c":null}]}'::JSON::VARIANT),
    (2, {'z': 3, 'a': {'y': false, 'b': 'w'}, 'm': [{'q': 4, 'c': 'v'}]}::VARIANT),
    (3, '{"z":5,"a":{"y":null,"b":"u","k":7},"m":[{"q":6,"c":[1]}],"e":0}'::JSON::VARIANT),
    (4, '[{"z":{"y":1,"b":2},"a":2}]'::JSON::VARIANT),
    (5, NULL)
  ) t(id, v) ORDER BY id
) TO 'duckdb_unsorted_variant.parquet' (FORMAT PARQUET, FIELD_IDS {id: 1, v: 2});
```

`duckdb_unsorted_variant.parquet` holds keys inserted out of order. DuckDB
1.5.5 sets `sorted_strings` on every metadata dictionary, sorted or not, and
lists an object's fields in insertion order, both of which the spec forbids
a writer and the Trino connector reads anyway (its `VariantRepairs`): here in
shredded rows, in an object inside an array (row 4, unshredded), and in an
object inside that. It writes the SQL NULL of row 5 as a present `00`.
`test_duckdb_dictionaries_and_field_order_read` pins DuckDB's own `v::JSON`
of each row.

```sql
COPY (
  SELECT id, v::JSON::VARIANT v FROM (VALUES
    (1, '[1]'), (2, '[2]'), (3, '[3]'), (4, '[4]'), (5, '[5]'),
    (6, '{"k":"' || repeat('x', 64) || '","n":1}'),
    (7, '{"a":{"k":"' || repeat('x', 64) || '"}}'),
    (8, '[1,{"k":"' || repeat('x', 64) || '"}]'),
    (9, '["' || repeat('x', 64) || '",1]'),
    (10, '"' || repeat('x', 64) || '"'),
    (11, '{"k":"' || repeat('x', 63) || '","n":1}'),
    (12, '{"k":"' || repeat('x', 65) || '","n":1}'),
    (13, '{"a":["' || repeat('x', 64) || '",1]}')
  ) t(id, v) ORDER BY id
) TO 'duckdb_64_byte_strings.parquet' (FORMAT PARQUET, FIELD_IDS {id: 1, v: 2});
```

`duckdb_64_byte_strings.parquet` holds DuckDB 1.5.5's spelling of a
string of exactly 64 bytes: an empty short string followed by the 64
bytes, which DuckDB itself reads back as `""`. The five lists first make
DuckDB shred the top level as a list, so the objects of rows 6-8 and 13
stay in `value`, where the string sits in an object, in an object in an
object, in an object in an array, and in an array in an object; row 9's
is a shredded element and row 10's the whole value. Rows 11 and 12, of
63 and 65 bytes, are written correctly.
`test_duckdbs_64_byte_strings_are_refused_wherever_they_sit` pins the
refusal of each.

```sql
COPY (SELECT id, v FROM (VALUES (1, 7::VARIANT), (2, NULL), (3, 9::VARIANT)) t(id, v) ORDER BY id)
TO 'duckdb_variant_sql_null.parquet' (FORMAT PARQUET, FIELD_IDS {id: 1, v: 2});
COPY (SELECT id, v FROM (VALUES (1, 7::VARIANT), (2, 8::VARIANT), (3, 9::VARIANT)) t(id, v) ORDER BY id)
TO 'duckdb_variant_int.parquet' (FORMAT PARQUET, FIELD_IDS {id: 1, v: 2});
```

In both, DuckDB shreds the top level as `INT32` (with the legacy `INT_32`
converted type alone), a layout the spec and the Trino connector accept,
so `strict_variant` gets past the layout to the DuckDB NOT NULL rule.
`duckdb_variant_sql_null.parquet` writes the SQL NULL of row 2 as a
present group whose `value` is `00`, which DuckDB reads back as NULL: its
`metadata` has no null, so a NOT NULL column passes the default check, and
the `value` statistics (min `00`) are what the strict rule refuses.
`duckdb_variant_int.parquet` holds no NULL, and every row is typed: its
`value` column is all null and its `typed_value` has no null, so each row
has exactly one side, which is the proof the rule accepts. Both pinned in
`test_variant_schema.py`.

```sql
COPY (SELECT 0.0::DOUBLE x, 0.0::FLOAT y FROM range(2))
TO 'duckdb_zero_bounds.parquet' (FORMAT PARQUET, FIELD_IDS {x:1,y:2});
```

`duckdb_zero_bounds.parquet` reports min = max = **+0.0** for both columns.
pyarrow normalizes its own zero statistics to (-0.0, +0.0) on write, so no
file this library produces can exercise the signed-zero bound rule; DuckDB
normalizes neither, which is what a caller-written prepared file looks like.

# Variant vectors: apache/parquet-testing

`parquet-testing/variant/` and `parquet-testing/shredded_variant/` are copied
unmodified from https://github.com/apache/parquet-testing at commit
`56653c437c8092f704a092d0d1d4e600124cd49f` (2026-09-15), with that
repository's `LICENSE.txt`; `parquet-testing/NOTICE` records the provenance
and the Apache License 2.0 notice. Spark (and Iceberg's test code, for the
types Spark lacks) wrote `variant/`, 29 metadata/value pairs and a
`data_dictionary.json` of their values; Iceberg wrote `shredded_variant/`,
138 cases in `cases.json` with a Parquet file each and the expected variant
as `.variant.bin` (metadata then value). They are the ground truth of the
codec's decoder (`tests/variant_conformance/test_parquet_testing.py`), which
shares no code with either writer. `data_dictionary.json` ends with a
trailing comma, which the test strips rather than the copy. To refresh:

```sh
git clone --filter=blob:none --no-checkout https://github.com/apache/parquet-testing pt
cd pt && git sparse-checkout set --no-cone variant shredded_variant && git checkout <sha>
cp -R variant shredded_variant ../pyhoglake/tests/data/parquet-testing/
```

then update the commit in this file and in `NOTICE`, and the counts the
test pins (a silently shrunken copy must not pass).

# variant/golden_storage.arrow

The Variant encoder's output, as an Arrow IPC file, over the fixed corpus of
declarations and edge rows in `tests/variant_conformance/variant_corpus.py`.
`test_golden_storage.py` requires the encoder to reproduce it exactly (D19 of
the README's policy table), comparing Arrow arrays, so a pyarrow upgrade does
not move it. Regenerate it only when the output changes on purpose, and say
why in the commit:

```sh
flox activate -- uv run python tests/data/variant/regen_golden_storage.py
```

Written with pyarrow 25.0.1 on CPython 3.13.

# variant/corpus_stamped.parquet

The footer splice's fixed expected output: the same corpus as Parquet, one
variant column per declaration (field ids 1 to 20), its decimal4/decimal8
leaves viewed as int32/int64, written by pyarrow with `store_schema=False`
and zstd, then stamped by the test oracle (`tests/footer_oracle.py`, a
generic compact-Thrift codec), not by the code under test: VARIANT(1) on
each group, DECIMAL(p, s) on each decimal leaf. The written file is not
stored; the oracle's `unstamp` gives it back byte for byte.
`test_variant_footer.py` requires `stamp_variant_footer` to turn that into
this file exactly, and every pyarrow of the CI matrix to read this file as
each plan's read form. Regenerate it when the corpus changes:

```sh
flox activate -- uv run python tests/data/variant/regen_footer_fixtures.py
```

Written with pyarrow 25.0.1 on CPython 3.13.
