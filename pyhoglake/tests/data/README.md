# Native VARIANT fixtures

Generated with DuckDB 1.5.5. These small files test actual Parquet annotations,
without adding DuckDB to pyhoglake's runtime/test dependencies.

```sql
COPY (SELECT 1::BIGINT id, {'a':42,'nested':[true,NULL]}::VARIANT properties)
TO 'native_variant.parquet' (FORMAT PARQUET, FIELD_IDS {id:1,properties:2});
COPY (SELECT NULL::BIGINT id, {'a':42}::VARIANT properties)
TO 'native_variant_null_id.parquet' (FORMAT PARQUET, FIELD_IDS {id:1,properties:2});
```

The server's `variant/native_variant.parquet` resource is byte-identical to the
first fixture and exercises parquet-java's physical schema handling.
