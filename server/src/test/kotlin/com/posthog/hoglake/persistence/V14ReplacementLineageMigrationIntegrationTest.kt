package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * V14 against real pre-migration state, and against the states a retry
 * can find the database in.
 *
 * Every test here RUNS THE MIGRATION FILE. The first arrives at V13 and
 * populates the rows V14's backfill has to find, because a migration
 * test that only inspects a fully-migrated database asserts nothing
 * about what the migration did — and one that hand-copies the migration
 * body asserts only that the copy works. The re-entry cases delete the
 * V14 history row and call [Database.migrate] again rather than
 * re-executing the SQL by hand, for the same reason.
 *
 * Three entry states, none of which the schema-equivalence gate reaches
 * (it only folds migrations onto a virgin database):
 *
 *  1. ordinary upgrade over a catalog that already holds replacement
 *     chains and unrelated dropped tables;
 *  2. re-entry over the index this migration already built — a no-op
 *     that must not drop and rebuild it;
 *  3. re-entry over an INVALID remnant of an interrupted concurrent
 *     build, which `CREATE INDEX CONCURRENTLY IF NOT EXISTS` would
 *     otherwise skip, leaving it invalid forever.
 */
@Tag("integration")
class V14ReplacementLineageMigrationIntegrationTest {
    private val index = "hog_table_replacement_lineage"

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

    /** Re-run the migration the way a Flyway repair-and-retry does. */
    private fun reapplyV14(db: PgTestSupport.TestDb) {
        db.jdbi.useHandleUnchecked { h -> h.execute("DELETE FROM flyway_schema_history WHERE version = '14'") }
        Database.migrate(db.dataSource)
    }

    /**
     * A catalog at V13 holding: a replacement CHAIN (t1 -> t2 -> t3, each
     * retired in the snapshot that created its successor), a table
     * dropped on its own with no successor, and a plain live table. Only
     * the chain rows are lineage edges. Returns the catalog id.
     */
    private fun seedPreMigrationCatalog(db: PgTestSupport.TestDb): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.execute("INSERT INTO hog_catalog (name, data_path) VALUES ('v14', 's3://b/v14')")
            val cat =
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = 'v14'")
                    .mapTo(Long::class.java).one()
            h.execute(
                "INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) " +
                    "SELECT ?, n, 0 FROM generate_series(0, 12) AS n",
                cat,
            )

            fun table(
                id: Long,
                created: Long,
                dropped: Long?,
            ) = h.createUpdate(
                """
                INSERT INTO hog_table (catalog_id, table_id, table_uuid, created_snapshot, dropped_snapshot)
                VALUES (:cat, :id, :uuid, :created, :dropped)
                """,
            ).bind("cat", cat).bind("id", id).bind("uuid", UUID.randomUUID())
                .bind("created", created).bind("dropped", dropped).execute()

            // The chain: 1 retired at 4 where 2 was created, 2 retired at
            // 7 where 3 was created, 3 still live.
            table(1, 1, 4)
            table(2, 4, 7)
            table(3, 7, null)
            // A lone drop: nothing was created in snapshot 9, so it is not
            // a replacement and must stay NULL.
            table(4, 2, 9)
            // A live table created in a snapshot where nothing was dropped.
            table(5, 11, null)
            // Created and dropped in the SAME snapshot. Only the
            // `o.table_id <> n.table_id` guard stops this row matching
            // ITSELF and recording a self-edge — which the recursive walks
            // would then have to survive, and which V14's CHECK now also
            // refuses outright.
            // Snapshot 3 is used by nothing else, so the only row this can
            // pair with is itself.
            table(6, 3, 3)
            // Volume, so the plan below is a real choice rather than a
            // seq scan the planner would pick anyway.
            h.execute(
                """
                INSERT INTO hog_table (catalog_id, table_id, table_uuid, created_snapshot, dropped_snapshot)
                SELECT ?, 100 + n, gen_random_uuid(), 0, CASE WHEN n % 5 = 0 THEN NULL ELSE 12 END
                FROM generate_series(1, 5000) AS n
                """,
                cat,
            )
            cat
        }

    @Test
    fun `upgrading from V13 backfills only genuine replacement edges`() {
        PgTestSupport.freshDatabaseAt("13").use { db ->
            val cat = seedPreMigrationCatalog(db)
            assertThat(
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "SELECT count(*) FROM information_schema.columns " +
                            "WHERE table_name = 'hog_table' AND column_name = 'replaced_table_id'",
                    ).mapTo(Int::class.java).one()
                },
            ).describedAs("column absent before V14").isZero()

            Database.migrate(db.dataSource)

            val edges =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        """
                        SELECT table_id, replaced_table_id FROM hog_table
                        WHERE catalog_id = :cat AND replaced_table_id IS NOT NULL
                        ORDER BY table_id
                        """,
                    ).bind("cat", cat)
                        .map { rs, _ -> rs.getLong(1) to rs.getLong(2) }
                        .list()
                }
            // Exactly the chain: 2 replaced 1, 3 replaced 2. The lone drop
            // (4), the unrelated live table (5), the same-snapshot
            // create-and-drop (6) and all 5000 filler rows carry no edge.
            assertThat(edges).containsExactly(2L to 1L, 3L to 2L)

            // Partial index, not a full one: the predicate is the point.
            val (exists, valid) = indexState(db)
            assertThat(exists).isTrue()
            assertThat(valid).isTrue()
            val (indexDef, partial) =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        """
                        SELECT pg_get_indexdef(i.indexrelid) AS def, (i.indpred IS NOT NULL) AS partial
                        FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                        WHERE c.relname = :name
                        """,
                    ).bind("name", index)
                        .map { rs, _ -> rs.getString("def") to rs.getBoolean("partial") }.one()
                }
            assertThat(partial).describedAs("pg_index.indpred").isTrue()
            assertThat(indexDef).contains("replaced_table_id IS NOT NULL")

            // And the REAL walk uses it. This EXPLAINs the statement
            // OffsetRepo actually issues, not a hand-written lookalike —
            // the previous version of this test EXPLAINed a shape no code
            // ever sends and was green by construction.
            db.jdbi.useHandleUnchecked { h -> h.execute("ANALYZE hog_table") }

            fun assertWalkUsesIndex(
                label: String,
                sql: String,
            ) {
                val plan =
                    db.jdbi.withHandleUnchecked { h ->
                        h.createQuery("EXPLAIN (COSTS OFF) $sql")
                            .bind("catalogId", cat)
                            .mapTo(String::class.java).list().joinToString("\n")
                    }
                assertThat(plan).describedAs("%s plan:%n%s", label, plan).contains(index)
                assertThat(plan).describedAs("%s plan:%n%s", label, plan).doesNotContain("Seq Scan on hog_table")
            }
            // Only the sweep's FORWARD walk depends on this index. The
            // per-commit backward walk follows replaced_table_id from a
            // known row, which is a primary-key lookup per hop and needs
            // no index of its own — so it is deliberately not asserted here.
            assertWalkUsesIndex("sweep, all consumers", OffsetRepo.RELEASE_ALL_CONSUMERS)
        }
    }

    @Test
    fun `re-entry over the built index is a no-op and keeps it`() {
        PgTestSupport.freshDatabase().use { db ->
            val before =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT relfilenode FROM pg_class WHERE relname = :n")
                        .bind("n", index).mapTo(Long::class.java).one()
                }
            reapplyV14(db)
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
            // Exactly what an interrupted CREATE INDEX CONCURRENTLY leaves.
            // Forged, because killing a real concurrent build mid-flight is
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

            // The migration file itself, not a copy of its DO block: IF NOT
            // EXISTS alone would see the invalid index and skip it.
            reapplyV14(db)

            assertThat(indexState(db))
                .describedAs("remnant cleared and rebuilt valid")
                .isEqualTo(true to true)
        }
    }

    @Test
    fun `the backfill is idempotent and never overwrites a recorded edge`() {
        PgTestSupport.freshDatabaseAt("13").use { db ->
            val cat = seedPreMigrationCatalog(db)
            Database.migrate(db.dataSource)
            // An edge createTable recorded that the snapshot convention
            // would NOT have produced. A retry must leave it alone.
            db.jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    "UPDATE hog_table SET replaced_table_id = 4 WHERE catalog_id = :cat AND table_id = 5",
                ).bind("cat", cat).execute()
            }
            reapplyV14(db)
            val edges =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        """
                        SELECT table_id, replaced_table_id FROM hog_table
                        WHERE catalog_id = :cat AND replaced_table_id IS NOT NULL ORDER BY table_id
                        """,
                    ).bind("cat", cat).map { rs, _ -> rs.getLong(1) to rs.getLong(2) }.list()
                }
            assertThat(edges).containsExactly(2L to 1L, 3L to 2L, 5L to 4L)
        }
    }
}
