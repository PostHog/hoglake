package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

/**
 * V18 against a POPULATED hog_data_file, which the schema-equivalence
 * gate never sees (it folds the chain onto an empty database). The
 * column must arrive as a nullable `bigint[]` with no default, so every
 * existing file reads NULL — "offsets unknown, cut evenly", which is what
 * every reader did for it before — and nothing is backfilled or
 * rewritten.
 */
@Tag("integration")
class V18SplitOffsetsMigrationIntegrationTest {
    @Test
    fun `existing files read NULL and new rows round-trip a bigint array`() {
        PgTestSupport.freshDatabaseAt("17").use { db ->
            db.jdbi.useHandleUnchecked { h ->
                h.execute("INSERT INTO hog_catalog (name, data_path) VALUES ('v18', 's3://b/v18')")
                val cat =
                    h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = 'v18'")
                        .mapTo(Long::class.java).one()
                h.execute("INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) VALUES (?, 0, 0)", cat)
                h.execute(
                    """
                    INSERT INTO hog_table (catalog_id, table_id, table_uuid, created_snapshot)
                    VALUES (?, 1, gen_random_uuid(), 0)
                    """,
                    cat,
                )
                h.execute(
                    """
                    INSERT INTO hog_data_file (catalog_id, table_id, data_file_id, row_id_start,
                        begin_snapshot, path, record_count, file_size_bytes, stats_state)
                    SELECT ?, 1, n, n * 100, 0, 's3://b/v18/f' || n, 100, 4096, 'provided'
                    FROM generate_series(1, 50) AS n
                    """,
                    cat,
                )
            }

            Database.migrate(db.dataSource)

            db.jdbi.useHandleUnchecked { h ->
                val (udt, nullable, default) =
                    h.createQuery(
                        """
                        SELECT udt_name, is_nullable, column_default FROM information_schema.columns
                        WHERE table_name = 'hog_data_file' AND column_name = 'split_offsets'
                        """,
                    ).map { rs, _ -> Triple(rs.getString(1), rs.getString(2), rs.getString(3)) }.one()
                assertThat(udt).isEqualTo("_int8")
                assertThat(nullable).isEqualTo("YES")
                assertThat(default).isNull()
                assertThat(
                    h.createQuery("SELECT count(*) FROM hog_data_file WHERE split_offsets IS NOT NULL")
                        .mapTo(Long::class.java).one(),
                ).isZero()
                h.createUpdate("UPDATE hog_data_file SET split_offsets = :o WHERE data_file_id = 1")
                    .bindBigintArrayOrNull("o", listOf(4L, 2048L))
                    .execute()
            }
            val stored =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT split_offsets FROM hog_data_file WHERE data_file_id = 1")
                        .map { rs, _ -> rs.getBigintListOrNull("split_offsets") }
                        .one()
                }
            assertThat(stored).containsExactly(4L, 2048L)

            // Re-entry over its own result (what the V14-V17 tests do when
            // they rewind the history): a no-op that keeps stored lists.
            db.jdbi.useHandleUnchecked { h ->
                h.execute("DELETE FROM flyway_schema_history WHERE version::numeric >= 18")
            }
            Database.migrate(db.dataSource)
            val kept =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT split_offsets FROM hog_data_file WHERE data_file_id = 1")
                        .map { rs, _ -> rs.getBigintListOrNull("split_offsets") }
                        .one()
                }
            assertThat(kept).containsExactly(4L, 2048L)
        }
    }
}
