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
