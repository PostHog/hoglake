package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

/**
 * V10 against a POPULATED catalog, and against the states a retry can
 * find the database in.
 *
 * V10 is not shaped like V2 and V3. Those drop-and-recreate their index
 * unconditionally; `hog_data_file` is the largest table here and
 * `CREATE INDEX CONCURRENTLY` waits out every older transaction, so V10
 * is written to be PRE-BUILT out of band and then be a no-op. That makes
 * three distinct entry states, none of which the schema-equivalence gate
 * exercises (it only ever folds migrations onto a virgin database):
 *
 *  1. ordinary upgrade — index absent, catalog already has files;
 *  2. re-entry after the index was pre-built by an operator — must be a
 *     no-op and must NOT drop the valid index;
 *  3. re-entry over an INVALID remnant of an interrupted concurrent
 *     build — must clear it, because `CREATE INDEX CONCURRENTLY IF NOT
 *     EXISTS` would otherwise see the invalid index, skip, and leave it
 *     invalid forever.
 *
 * State 3 is the one the DO block exists for and the one no other test
 * reaches.
 */
@Tag("integration")
class V10CompactionBinPackingMigrationIntegrationTest {
    private val index = "hog_data_file_maintenance_size_scan"
    private val column = "pending_max_bytes"

    private fun indexState(db: PgTestSupport.TestDb): Pair<Boolean, Boolean> =
        db.jdbi.withHandleUnchecked { h ->
            val row =
                h.createQuery(
                    """
                    SELECT i.indisvalid FROM pg_class c
                    JOIN pg_index i ON i.indexrelid = c.oid
                    WHERE c.relname = :name
                    """,
                ).bind("name", index).mapTo(Boolean::class.javaObjectType).findOne().orElse(null)
            (row != null) to (row == true)
        }

    private fun hasColumn(db: PgTestSupport.TestDb): Boolean =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT count(*) FROM information_schema.columns
                WHERE table_name = 'hog_maintenance_summary_tier' AND column_name = :col
                """,
            ).bind("col", column).mapTo(Int::class.java).one() > 0
        }

    @Test
    fun `upgrading a populated catalog builds the index and adds the carry column`() {
        PgTestSupport.freshDatabase().use { db ->
            db.jdbi.useHandleUnchecked { h ->
                h.execute(
                    "INSERT INTO hog_catalog (name, data_path) VALUES ('v10', 's3://b/v10')",
                )
                val cat =
                    h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = 'v10'")
                        .mapTo(Long::class.java).one()
                h.execute("INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) VALUES (?, 0, 0)", cat)
                h.execute(
                    """
                    INSERT INTO hog_table (catalog_id, table_id, table_uuid, created_snapshot)
                    VALUES (?, 1, gen_random_uuid(), 0)
                    """,
                    cat,
                )
                // Rows present BEFORE the index exists: a concurrent
                // build over a populated table is the production case.
                h.execute(
                    """
                    INSERT INTO hog_data_file (catalog_id, table_id, data_file_id, row_id_start,
                        begin_snapshot, path, record_count, file_size_bytes, stats_state)
                    SELECT ?, 1, n, n * 100, 0, 's3://b/v10/f' || n, 100, n * 1024, 'provided'
                    FROM generate_series(1, 500) AS n
                    """,
                    cat,
                )
            }
            val (exists, valid) = indexState(db)
            assertThat(exists).describedAs("index built").isTrue()
            assertThat(valid).describedAs("index valid").isTrue()
            assertThat(hasColumn(db)).describedAs("carry column added").isTrue()

            // The size-ordered keyset the sampler walks must actually use
            // it, which is the whole reason the migration exists.
            val plan =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        """
                        EXPLAIN (COSTS OFF) SELECT data_file_id FROM hog_data_file
                        WHERE catalog_id = 1
                          AND (table_id, file_size_bytes, data_file_id) > (0, 0, 0)
                        ORDER BY table_id, file_size_bytes, data_file_id LIMIT 10
                        """,
                    ).mapTo(String::class.java).list().joinToString("\n")
                }
            assertThat(plan).describedAs("keyset scan uses the new index").contains(index)
        }
    }

    @Test
    fun `re-entry over a pre-built index is a no-op and keeps it`() {
        PgTestSupport.freshDatabase().use { db ->
            val before =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT relfilenode FROM pg_class WHERE relname = :n")
                        .bind("n", index).mapTo(Long::class.java).one()
                }
            // Flyway records V10 as applied, so re-running migrate() is
            // the no-op path an operator hits on every pod restart.
            Database.migrate(db.dataSource)
            val after =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT relfilenode FROM pg_class WHERE relname = :n")
                        .bind("n", index).mapTo(Long::class.java).one()
                }
            assertThat(after)
                .describedAs("a valid index is not dropped and rebuilt")
                .isEqualTo(before)
            assertThat(indexState(db).second).isTrue()
        }
    }

    @Test
    fun `an INVALID remnant is cleared rather than skipped forever`() {
        PgTestSupport.freshDatabase().use { db ->
            // Exactly what an interrupted CREATE INDEX CONCURRENTLY
            // leaves: the index exists, and indisvalid is false. Forged
            // here because killing a real concurrent build mid-flight is
            // not reproducible in a test.
            db.jdbi.useHandleUnchecked { h ->
                h.execute(
                    """
                    UPDATE pg_index SET indisvalid = false
                    WHERE indexrelid = (SELECT oid FROM pg_class WHERE relname = ?)
                    """,
                    index,
                )
            }
            assertThat(indexState(db)).isEqualTo(true to false)

            // Re-run the migration body the way a Flyway repair-and-retry
            // would. IF NOT EXISTS alone would see the invalid index and
            // skip it, leaving it invalid forever -- the DO block is what
            // stops that.
            db.jdbi.useHandleUnchecked { h ->
                h.execute(
                    """
                    DO $$
                    BEGIN
                        IF EXISTS (
                            SELECT 1 FROM pg_class c
                            JOIN pg_index i ON i.indexrelid = c.oid
                            WHERE c.relname = 'hog_data_file_maintenance_size_scan' AND NOT i.indisvalid
                        ) THEN
                            EXECUTE 'DROP INDEX hog_data_file_maintenance_size_scan';
                        END IF;
                    END $$
                    """,
                )
                h.execute(
                    """
                    CREATE INDEX IF NOT EXISTS hog_data_file_maintenance_size_scan
                        ON hog_data_file (catalog_id, table_id, file_size_bytes, data_file_id)
                    """,
                )
            }
            assertThat(indexState(db))
                .describedAs("remnant cleared and rebuilt valid")
                .isEqualTo(true to true)
        }
    }
}
