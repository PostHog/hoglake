package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("integration")
class V25PackedMergetreeFormatMigrationIntegrationTest {
    @Test
    fun `existing parquet rows remain valid and packed rows are admitted`() {
        PgTestSupport.freshDatabaseAt("24").use { db ->
            val catalogId =
                db.jdbi.withHandle<Long, Exception> { h ->
                    h.createQuery(
                        "INSERT INTO hog_catalog (name, data_path) VALUES ('v25', 's3://b/v25') RETURNING catalog_id",
                    ).mapTo(Long::class.java).one()
                }
            db.jdbi.useHandle<Exception> { h ->
                h.execute(
                    "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                    catalogId,
                )
                h.execute(
                    """
                    INSERT INTO hog_data_file
                        (catalog_id, data_file_id, table_id, begin_snapshot, path,
                         record_count, file_size_bytes, row_id_start)
                    VALUES (?, 1, 1, 0, 's3://b/v25/a.parquet', 1, 10, 0)
                    """,
                    catalogId,
                )
            }

            Database.migrate(db.dataSource)

            db.jdbi.useHandle<Exception> { h ->
                assertThat(
                    h.createQuery("SELECT file_format FROM hog_data_file WHERE data_file_id = 1")
                        .mapTo(String::class.java)
                        .one(),
                ).isEqualTo("parquet")
                h.execute(
                    "INSERT INTO hog_namespace (catalog_id, namespace_id, name) VALUES (?, 1, 'ns')",
                    catalogId,
                )
                h.execute(
                    """
                    INSERT INTO hog_table
                        (catalog_id, table_id, created_snapshot, file_format)
                    VALUES (?, 2, 0, 'clickhouse-mergetree-packed')
                    """,
                    catalogId,
                )
                h.execute(
                    """
                    INSERT INTO hog_table_version
                        (catalog_id, table_id, begin_snapshot, namespace_id, name, properties)
                    VALUES (?, 2, 0, 1, 'packed',
                            '{"write.format.default":"clickhouse-mergetree-packed"}'::jsonb)
                    """,
                    catalogId,
                )
                h.execute(
                    """
                    INSERT INTO hog_column
                        (catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal)
                    VALUES (?, 2, 1, 0, 'id', 'long', 0)
                    """,
                    catalogId,
                )
                h.execute(
                    """
                    INSERT INTO hog_data_file
                        (catalog_id, data_file_id, table_id, begin_snapshot, path, file_format,
                         record_count, file_size_bytes, row_id_start)
                    VALUES (?, 2, 2, 0, 's3://b/v25/b.packed',
                            'clickhouse-mergetree-packed', 1, 10, 1)
                    """,
                    catalogId,
                )

                h.execute(
                    """
                    INSERT INTO hog_upload
                        (catalog_id, upload_id, owner, prefix, path, file_kind, file_format)
                    VALUES (?, '00000000-0000-4000-8000-000000000001',
                            '00000000-0000-4000-8000-000000000002', 's3://b/v25',
                            's3://b/v25/trino-upload/claimed.packed', 'data',
                            'clickhouse-mergetree-packed')
                    """,
                    catalogId,
                )
                assertThatThrownBy {
                    h.execute(
                        """
                        INSERT INTO hog_data_file
                            (catalog_id, data_file_id, table_id, begin_snapshot, path,
                             record_count, file_size_bytes, row_id_start)
                        VALUES (?, 20, 1, 0,
                                's3://b/v25/trino-upload/claimed.packed', 1, 10, 20)
                        """,
                        catalogId,
                    )
                }.isInstanceOf(UnableToExecuteStatementException::class.java)

                data class InvalidFile(val id: Long, val tableId: Long, val format: String, val path: String)
                for ((id, tableId, format, path) in listOf(
                    InvalidFile(3, 1, "clickhouse-mergetree-packed", "s3://b/v25/mixed.packed"),
                    InvalidFile(4, 2, "parquet", "s3://b/v25/mixed.parquet"),
                    InvalidFile(5, 1, "orc", "s3://b/v25/c.orc"),
                )) {
                    assertThatThrownBy {
                        h.createUpdate(
                            """
                            INSERT INTO hog_data_file
                                (catalog_id, data_file_id, table_id, begin_snapshot, path, file_format,
                                 record_count, file_size_bytes, row_id_start)
                            VALUES (:catalog, :id, :table, 0, :path, :format, 1, 10, :id)
                            """,
                        )
                            .bind("catalog", catalogId)
                            .bind("id", id)
                            .bind("table", tableId)
                            .bind("path", path)
                            .bind("format", format)
                            .execute()
                    }.isInstanceOf(UnableToExecuteStatementException::class.java)
                }
                val forbiddenMutations =
                    listOf(
                        "UPDATE hog_column SET end_snapshot = 1 WHERE catalog_id = $catalogId AND table_id = 2",
                        "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot) " +
                            "VALUES ($catalogId, 2, 1, 1)",
                        "UPDATE hog_data_file SET end_snapshot = 1 " +
                            "WHERE catalog_id = $catalogId AND data_file_id = 2",
                        "INSERT INTO hog_delete_file " +
                            "(catalog_id, delete_file_id, table_id, data_file_id, begin_snapshot, path, " +
                            "delete_count, file_size_bytes) VALUES " +
                            "($catalogId, 10, 2, 2, 1, 's3://b/v25/dv.puffin', 1, 10)",
                        "UPDATE hog_table_version SET properties = '{}'::jsonb " +
                            "WHERE catalog_id = $catalogId AND table_id = 2 AND end_snapshot IS NULL",
                    )
                forbiddenMutations.forEach { sql ->
                    assertThatThrownBy { h.execute(sql) }
                        .isInstanceOf(UnableToExecuteStatementException::class.java)
                }
                h.execute(
                    "UPDATE hog_table SET dropped_snapshot = 2 WHERE catalog_id = ? AND table_id = 2",
                    catalogId,
                )
                assertThat(
                    h.execute(
                        "UPDATE hog_column SET end_snapshot = 2 WHERE catalog_id = ? AND table_id = 2",
                        catalogId,
                    ),
                ).isEqualTo(1)
                assertThat(
                    h.createQuery(
                        """
                        SELECT is_nullable FROM information_schema.columns
                         WHERE table_name = 'hog_upload' AND column_name = 'file_format'
                        """,
                    ).mapTo(String::class.java).one(),
                ).isEqualTo("YES")
            }
        }
    }

    @Test
    fun `a preexisting use of the reserved property blocks migration`() {
        PgTestSupport.freshDatabaseAt("24").use { db ->
            db.jdbi.useHandle<Exception> { h ->
                val catalogId =
                    h.createQuery(
                        "INSERT INTO hog_catalog (name, data_path) VALUES ('v25-reserved', 's3://b/v25r') " +
                            "RETURNING catalog_id",
                    ).mapTo(Long::class.java).one()
                h.execute(
                    "INSERT INTO hog_namespace (catalog_id, namespace_id, name) VALUES (?, 1, 'ns')",
                    catalogId,
                )
                h.execute(
                    "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                    catalogId,
                )
                h.execute(
                    """
                    INSERT INTO hog_table_version
                        (catalog_id, table_id, begin_snapshot, namespace_id, name, properties)
                    VALUES (?, 1, 0, 1, 't',
                            '{"write.format.default":"clickhouse-mergetree-packed"}'::jsonb)
                    """,
                    catalogId,
                )
            }

            assertThatThrownBy { Database.migrate(db.dataSource) }
                .hasMessageContaining("write.format.default")
        }
    }
}
