# Native VARIANT fixtures

Generated with DuckDB 1.5.5. These small files test actual Parquet annotations,
without adding DuckDB to pyhoglake's runtime/test dependencies.

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
