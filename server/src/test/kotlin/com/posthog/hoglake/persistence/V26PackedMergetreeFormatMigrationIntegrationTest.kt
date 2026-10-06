package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("integration")
class V26PackedMergetreeFormatMigrationIntegrationTest {
    @Test
    fun `existing parquet rows remain valid and packed rows are admitted`() {
        PgTestSupport.freshDatabaseAt("25").use { db ->
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

                assertThatThrownBy {
                    h.createUpdate(
                        """
                        INSERT INTO hog_table
                            (catalog_id, table_id, created_snapshot, file_format)
                        VALUES (:catalog, 3, 0, 'orc')
                        """,
                    )
                        .bind("catalog", catalogId)
                        .execute()
                }.isInstanceOf(UnableToExecuteStatementException::class.java)
                    .hasMessageContaining("hog_table_file_format_check")

                assertThatThrownBy {
                    h.createUpdate(
                        """
                        INSERT INTO hog_data_file
                            (catalog_id, data_file_id, table_id, begin_snapshot, path, file_format,
                             record_count, file_size_bytes, row_id_start)
                        VALUES (:catalog, 3, 1, 0, 's3://b/v25/invalid.orc', 'orc', 1, 10, 3)
                        """,
                    )
                        .bind("catalog", catalogId)
                        .execute()
                }.isInstanceOf(UnableToExecuteStatementException::class.java)
                    .hasMessageContaining("hog_data_file_file_format_check")

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
        PgTestSupport.freshDatabaseAt("25").use { db ->
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
