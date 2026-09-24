package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.compaction.CompactionClaimRepo
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

/**
 * V15 against the states a real upgrade and a real retry find.
 *
 * The migration RUNS here — arriving at V14 first and then calling
 * [Database.migrate] — rather than being inspected on a fully-migrated
 * database, because the schema-equivalence gate already folds every
 * migration onto a virgin database and a test that only looks at the
 * result asserts nothing about what this file did.
 *
 * Two things it has to get right beyond creating the table:
 *
 *  1. **Every statement the code issues has an access path, and every
 *    index earns its keep.** Both of [CompactionClaimRepo]'s queries
 *    (exposed `internal` for exactly this) are EXPLAINed over a claim
 *    table large enough that a sequential scan would be the planner's
 *    honest choice. V14's first index was on the wrong column of a join
 *    and its test was green because it EXPLAINed a predicate no code
 *    path sends. This migration's first draft had the opposite defect —
 *    an index for the planner's read that the primary key already
 *    served, and that the planner accordingly never chose — and the
 *    EXPLAIN below is what found it.
 *  2. **It is re-runnable.** The file carries
 *    `executeInTransaction=false`, so a failure part way through leaves
 *    the statements before it applied; every one of them is
 *    `IF NOT EXISTS` and a retry must be a clean no-op rather than a
 *    Flyway repair.
 */
@Tag("integration")
class V15CompactionClaimsMigrationIntegrationTest {
    private val expiryIndex = "hog_compaction_claim_expiry"

    private fun tableExists(db: PgTestSupport.TestDb): Boolean =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT count(*) FROM information_schema.tables " +
                    "WHERE table_schema = 'public' AND table_name = 'hog_compaction_claim'",
            ).mapTo(Int::class.java).one() == 1
        }

    /**
     * A catalog, and enough claim rows across enough tables that the
     * planner has a real choice to make: 200 catalogs x 100 claims over
     * 200 table ids, a third of them already expired. That is the shape
     * production has — one table shared by every catalog, each read
     * wanting one catalog's or one table's slice of it — and it is what
     * makes the EXPLAINs below a decision rather than a formality.
     */
    private fun seedClaims(db: PgTestSupport.TestDb): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.execute("INSERT INTO hog_catalog (name, data_path) VALUES ('v15', 's3://b/v15')")
            val cat =
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = 'v15'")
                    .mapTo(Long::class.java).one()
            for (i in 2..200) {
                h.execute("INSERT INTO hog_catalog (name, data_path) VALUES (?, ?)", "v15-$i", "s3://b/v15-$i")
            }
            h.execute(
                """
                INSERT INTO hog_compaction_claim
                       (catalog_id, table_id, group_key, input_file_ids, claimant, expires_at)
                SELECT c.catalog_id, n % 200, md5(c.catalog_id::text || ':' || n::text),
                       ARRAY[n, n + 1]::bigint[], gen_random_uuid(),
                       now() + make_interval(secs => CASE WHEN n % 3 = 0 THEN -600 ELSE 600 END)
                FROM hog_catalog c, generate_series(1, 100) AS n
                """,
            )
            h.execute("ANALYZE hog_compaction_claim")
            cat
        }

    private fun explain(
        db: PgTestSupport.TestDb,
        sql: String,
        catalogId: Long,
        tableId: Long?,
    ): String =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("EXPLAIN (COSTS OFF) $sql")
                .bind("catalogId", catalogId)
                .also { q -> if (tableId != null) q.bind("tableId", tableId) }
                .mapTo(String::class.java).list().joinToString("\n")
        }

    @Test
    fun `upgrading from V14 creates the claim table and indexes that serve the repo's own queries`() {
        PgTestSupport.freshDatabaseAt("14").use { db ->
            assertThat(tableExists(db)).describedAs("table absent before V15").isFalse()

            Database.migrate(db.dataSource)

            assertThat(tableExists(db)).isTrue()
            val catalogId = seedClaims(db)

            // The planner's read rides the PRIMARY KEY: (catalog_id,
            // table_id) is its prefix. Asserted, rather than left
            // implicit, because the first draft of this migration added
            // a THIRD index for it — and this EXPLAIN is what showed the
            // planner ignoring it. An index nothing chooses is write
            // cost on the hottest statement the table has.
            val plannerPlan = explain(db, CompactionClaimRepo.LIVE_CLAIMS_SQL, catalogId, tableId = 7)
            assertThat(plannerPlan)
                .describedAs("planner read plan:%n%s", plannerPlan)
                .contains("hog_compaction_claim_pkey")
            assertThat(plannerPlan)
                .describedAs("planner read plan:%n%s", plannerPlan)
                .doesNotContain("Seq Scan on hog_compaction_claim")

            // The purge is per CATALOG across its tables, which the
            // primary key cannot serve: its second column is a gap in
            // this predicate, so the key alone would scan every claim
            // the catalog holds.
            val purgePlan = explain(db, CompactionClaimRepo.PURGE_EXPIRED_SQL, catalogId, tableId = null)
            assertThat(purgePlan).describedAs("purge plan:%n%s", purgePlan).contains(expiryIndex)
            assertThat(purgePlan)
                .describedAs("purge plan:%n%s", purgePlan)
                .doesNotContain("Seq Scan on hog_compaction_claim")
        }
    }

    @Test
    fun `the claim key is unique per table and the catalog cascade reaches the claims`() {
        // The two structural rules the code depends on. The primary key
        // is what makes `INSERT ... ON CONFLICT` the whole claim
        // protocol; without it two maintainers would both "take" the
        // same group. And the cascade is the only thing that removes a
        // deleted catalog's claims, since nothing else knows they exist.
        PgTestSupport.freshDatabase().use { db ->
            val cat =
                db.jdbi.withHandleUnchecked { h ->
                    h.execute("INSERT INTO hog_catalog (name, data_path) VALUES ('v15-pk', 's3://b/pk')")
                    h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = 'v15-pk'")
                        .mapTo(Long::class.java).one()
                }

            fun insert() =
                db.jdbi.useHandleUnchecked { h ->
                    h.execute(
                        """
                        INSERT INTO hog_compaction_claim
                               (catalog_id, table_id, group_key, input_file_ids, claimant, expires_at)
                        VALUES (?, 1, 'k', ARRAY[1]::bigint[], gen_random_uuid(), now() + interval '1 hour')
                        """,
                        cat,
                    )
                }
            insert()
            org.assertj.core.api.Assertions.assertThatThrownBy { insert() }
                .describedAs("(catalog_id, table_id, group_key) must be the primary key")
                .hasMessageContaining("hog_compaction_claim_pkey")

            db.jdbi.useHandleUnchecked { h ->
                h.execute("DELETE FROM hog_catalog WHERE catalog_id = ?", cat)
                assertThat(
                    h.createQuery("SELECT count(*) FROM hog_compaction_claim").mapTo(Int::class.java).one(),
                ).describedAs("ON DELETE CASCADE from hog_catalog").isZero()
            }
        }
    }

    @Test
    fun `re-entry is a clean no-op - the file runs outside a transaction and must be retryable`() {
        PgTestSupport.freshDatabase().use { db ->
            val before = indexNodes(db)
            db.jdbi.useHandleUnchecked { h ->
                h.execute("DELETE FROM flyway_schema_history WHERE version = '15'")
            }
            Database.migrate(db.dataSource)
            assertThat(tableExists(db)).isTrue()
            assertThat(indexNodes(db))
                .describedAs("an existing table and its indexes are not dropped and rebuilt")
                .isEqualTo(before)
        }
    }

    private fun indexNodes(db: PgTestSupport.TestDb): Map<String, Long> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT relname, relfilenode FROM pg_class WHERE relname IN (:a, :b)",
            )
                .bind("a", expiryIndex)
                .bind("b", "hog_compaction_claim_pkey")
                .map { rs, _ -> rs.getString("relname") to rs.getLong("relfilenode") }
                .list()
                .toMap()
        }
}
