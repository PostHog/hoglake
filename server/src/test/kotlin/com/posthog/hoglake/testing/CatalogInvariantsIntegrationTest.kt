package com.posthog.hoglake.testing

import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * BOTH POLARITIES OF EVERY [CatalogInvariants] HELPER, which is the only
 * thing that makes them worth calling.
 *
 * An oracle nothing tests rots silently, and these four are called from
 * compaction fixtures that are supposed to be CLEAN — so every one of
 * them passes trivially at every call site, and a helper whose predicate
 * never matched anything would look exactly as green. That is not a
 * hypothetical: the first draft of
 * [CatalogInvariants.assertRemovalQueueUnreferenced] was vacuous at both
 * of its call sites (the group commit settles its staging ticket in the
 * same statement, so no undrained row survives), and the first draft of
 * [CatalogInvariants.assertVisibilityBounds] promised `[0, head]` in its
 * KDoc while enforcing only `<= head`. Both were found by flipping a
 * predicate, not by reading the code.
 *
 * So each case here plants ONE violation of the shape the helper claims
 * to catch and asserts it throws, then asserts the clean catalog passes.
 * A helper that stops matching — or a predicate deleted from one of the
 * disjuncts — reds here rather than going quiet at twelve call sites.
 *
 * All seeding is direct SQL: these are assertions ABOUT rows, so the
 * rows are written the way a defect would write them, bypassing the
 * commit tail that exists to make them impossible.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogInvariantsIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi

    @AfterAll
    fun tearDown() = db.close()

    /** Catalog with snapshots 0..head dense, one live table, one live file. */
    private fun seed(
        name: String,
        head: Long = 4,
    ): Long =
        jdbi.withHandleUnchecked { h ->
            val catalogId =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path, last_snapshot_id, earliest_snapshot_id) " +
                        "VALUES (:n, 's3://b/p', :head, 0) RETURNING catalog_id",
                ).bind("n", name).bind("head", head).mapTo(Long::class.java).one()
            for (s in 0..head) {
                h.execute(
                    "INSERT INTO hog_snapshot (catalog_id, snapshot_id, snapshot_time, schema_version) " +
                        "VALUES (?, ?, now(), 0)",
                    catalogId,
                    s,
                )
            }
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                catalogId,
            )
            h.execute(
                """
                INSERT INTO hog_data_file
                    (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, path,
                     record_count, file_size_bytes, row_id_start)
                VALUES (?, 1, 1, 1, NULL, 's3://b/live.parquet', 10, 100, 0)
                """,
                catalogId,
            )
            catalogId
        }

    private fun sql(
        statement: String,
        vararg args: Any,
    ) = jdbi.useHandleUnchecked { h -> h.execute(statement, *args) }

    // ---- assertVisibilityBounds (invariant 6) -------------------------------

    @Test
    fun `visibility bounds pass on a clean catalog and catch a begin above head`() {
        val id = seed("inv-vis-above")
        assertThatCode { CatalogInvariants.assertVisibilityBounds(jdbi, "inv-vis-above") }
            .doesNotThrowAnyException()
        sql(
            """
            INSERT INTO hog_data_file
                (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, path,
                 record_count, file_size_bytes, row_id_start)
            VALUES (?, 2, 1, 99, NULL, 's3://b/above-head.parquet', 1, 1, 100)
            """,
            id,
        )
        assertThatThrownBy { CatalogInvariants.assertVisibilityBounds(jdbi, "inv-vis-above") }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("invariant 6")
            .hasMessageContaining("begin=99")
    }

    @Test
    fun `visibility bounds catch a NEGATIVE begin_snapshot, which the upper bound cannot see`() {
        // The lower half of `[0, head]`, which the KDoc promised and the
        // predicate did not enforce until #279. Snapshot ids are
        // non-negative by construction, so a negative one is a row some
        // writer produced without going through the allocator — and
        // `begin_snapshot <= head` is true of every negative value, so
        // the upper bound alone is blind to all of them.
        //
        // MUTATION: drop `x.begin_snapshot < 0` from the helper and this
        // reds while every other case here stays green.
        val id = seed("inv-vis-negative")
        sql(
            """
            INSERT INTO hog_data_file
                (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, path,
                 record_count, file_size_bytes, row_id_start)
            VALUES (?, 2, 1, -1, NULL, 's3://b/negative-begin.parquet', 1, 1, 100)
            """,
            id,
        )
        assertThatThrownBy { CatalogInvariants.assertVisibilityBounds(jdbi, "inv-vis-negative") }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("begin=-1")
    }

    @Test
    fun `a negative end_snapshot is only reachable under a negative begin, and that is what catches it`() {
        // WHY THE HELPER HAS NO `end_snapshot < 0` ARM, pinned rather
        // than argued. `CHECK (end_snapshot IS NULL OR end_snapshot >
        // begin_snapshot)` means a negative end forces a negative begin
        // under it — with `begin >= 0` the row below is not even
        // INSERTable — so the `begin_snapshot < 0` arm is the one that
        // fires, and a dedicated end arm could never fire alone. The
        // first draft of #279's fix had one; this case is what showed it
        // was dead SQL.
        //
        // MUTATION: drop `x.begin_snapshot < 0` and this reds too, which
        // is the point: ONE arm covers both columns.
        val id = seed("inv-vis-negative-end")
        sql(
            """
            INSERT INTO hog_data_file
                (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, path,
                 record_count, file_size_bytes, row_id_start)
            VALUES (?, 2, 1, -5, -2, 's3://b/negative-end.parquet', 1, 1, 100)
            """,
            id,
        )
        assertThatThrownBy { CatalogInvariants.assertVisibilityBounds(jdbi, "inv-vis-negative-end") }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("begin=-5")
            .hasMessageContaining("end=-2")
    }

    @Test
    fun `visibility bounds catch a NEGATIVE created_snapshot on the identity row`() {
        // `hog_table` is the identity row, not a versioned row, and it
        // carries the same shape under other names — so it needs its own
        // lower bound and its own case.
        //
        // MUTATION: drop `t.created_snapshot < 0` and this reds alone.
        val id = seed("inv-vis-negative-table")
        sql(
            "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 2, -3)",
            id,
        )
        assertThatThrownBy { CatalogInvariants.assertVisibilityBounds(jdbi, "inv-vis-negative-table") }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("hog_table")
            .hasMessageContaining("created=-3")
    }

    @Test
    fun `visibility bounds catch a dropped_snapshot before the table was created`() {
        // Also why there is no `dropped_snapshot < 0` arm: `hog_table`
        // has no CHECK coupling the two, but a negative dropped is
        // either below a non-negative created (this arm) or above a
        // created that is itself negative (the arm in the case above).
        // Removed from the first draft for the same reason as the end
        // arm — it could not fire alone.
        val id = seed("inv-vis-dropped")
        sql(
            "INSERT INTO hog_table (catalog_id, table_id, created_snapshot, dropped_snapshot) " +
                "VALUES (?, 2, 3, 2)",
            id,
        )
        assertThatThrownBy { CatalogInvariants.assertVisibilityBounds(jdbi, "inv-vis-dropped") }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("dropped=2")
    }

    // ---- assertRemovalQueueUnreferenced (invariant 4) -----------------------

    @Test
    fun `the removal queue arm catches an undrained row over a path a file row still names`() {
        // NO LIVENESS FILTER is the load-bearing part: an END-SNAPSHOTTED
        // row still fences its object until retirement deletes it, so the
        // planted file row here is deliberately dead. A helper carrying
        // `end_snapshot IS NULL` — which the first draft did — passes
        // this case and loses two thirds of the check's reach.
        //
        // MUTATION: add `AND f.end_snapshot IS NULL` to either EXISTS and
        // this reds.
        val id = seed("inv-queue")
        assertThatCode { CatalogInvariants.assertRemovalQueueUnreferenced(jdbi, "inv-queue") }
            .doesNotThrowAnyException()
        sql(
            """
            INSERT INTO hog_data_file
                (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, path,
                 record_count, file_size_bytes, row_id_start)
            VALUES (?, 2, 1, 1, 2, 's3://b/fenced.parquet', 1, 1, 100)
            """,
            id,
        )
        sql(
            "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, scheduled_at) " +
                "VALUES (?, 's3://b/fenced.parquet', 'data', 'snapshot_expiry', now())",
            id,
        )
        assertThatThrownBy { CatalogInvariants.assertRemovalQueueUnreferenced(jdbi, "inv-queue") }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("invariant 4")
            .hasMessageContaining("s3://b/fenced.parquet")
    }

    @Test
    fun `a DRAINED queue row over a live path is not flagged, because the object is already gone`() {
        // The `drained_at IS NULL` half. A settled row is history: its
        // object was deleted (or found absent) and a file row naming that
        // path afterwards is a different concern — the staging arm's, not
        // this one's. Flagging it would make every healthy catalog red
        // once cleanup had run.
        //
        // MUTATION: drop `q.drained_at IS NULL` and this reds.
        val id = seed("inv-queue-drained")
        sql(
            "INSERT INTO hog_file_removal " +
                "(catalog_id, path, file_kind, reason, scheduled_at, drained_at, drained_outcome) " +
                "VALUES (?, 's3://b/live.parquet', 'data', 'snapshot_expiry', now(), now(), 'deleted')",
            id,
        )
        assertThatCode { CatalogInvariants.assertRemovalQueueUnreferenced(jdbi, "inv-queue-drained") }
            .doesNotThrowAnyException()
    }

    // ---- assertNoAbsentTicketOverLivePath (staging_tickets arm c) -----------

    @Test
    fun `the staging arm catches a ticket settled absent over a path the catalog registered`() {
        // MUTATION: drop `q.drained_outcome = 'absent'` or the reason
        // filter and this reds.
        val id = seed("inv-staging")
        assertThatCode { CatalogInvariants.assertNoAbsentTicketOverLivePath(jdbi, "inv-staging") }
            .doesNotThrowAnyException()
        sql(
            "INSERT INTO hog_file_removal " +
                "(catalog_id, path, file_kind, reason, scheduled_at, drained_at, drained_outcome) " +
                "VALUES (?, 's3://b/live.parquet', 'data', 'compaction_staging', now(), now(), 'absent')",
            id,
        )
        assertThatThrownBy { CatalogInvariants.assertNoAbsentTicketOverLivePath(jdbi, "inv-staging") }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("was drained 'absent' but the catalog holds a file row")
    }

    @Test
    fun `a ticket settled absent over a path nothing registered is the ORDINARY outcome`() {
        // What `'absent'` is for: the drain probed, found nothing, and
        // said so. Only a path the catalog DOES hold a row for makes it a
        // violation, and conflating the two would red every reclaimed
        // staging ticket.
        val id = seed("inv-staging-ok")
        sql(
            "INSERT INTO hog_file_removal " +
                "(catalog_id, path, file_kind, reason, scheduled_at, drained_at, drained_outcome) " +
                "VALUES (?, 's3://b/never-existed.parquet', 'data', 'compaction_staging', now(), now(), 'absent')",
            id,
        )
        assertThatCode { CatalogInvariants.assertNoAbsentTicketOverLivePath(jdbi, "inv-staging-ok") }
            .doesNotThrowAnyException()
    }

    // ---- assertSnapshotsDense (invariant 1) ---------------------------------

    @Test
    fun `density passes on a dense range and catches a hole in it`() {
        // MUTATION: `head - earliest + 2` and the clean half reds.
        val id = seed("inv-dense", head = 4)
        assertThatCode { CatalogInvariants.assertSnapshotsDense(jdbi, "inv-dense") }
            .doesNotThrowAnyException()
        sql("DELETE FROM hog_snapshot WHERE catalog_id = ? AND snapshot_id = 2", id)
        assertThatThrownBy { CatalogInvariants.assertSnapshotsDense(jdbi, "inv-dense") }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("invariant 1")
    }

    @Test
    fun `density names the catalog rather than throwing from the mapper when there are none`() {
        // The `findOne`/un-joined shape, which exists because an INNER
        // JOIN with GROUP BY and `.one()` threw IllegalStateException on a
        // snapshot-less catalog — an error that says nothing about the
        // invariant. A clean assertion failure is the contract.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_catalog (name, data_path, last_snapshot_id, earliest_snapshot_id) " +
                    "VALUES ('inv-dense-empty', 's3://b/p', 4, 0)",
            )
        }
        assertThatThrownBy { CatalogInvariants.assertSnapshotsDense(jdbi, "inv-dense-empty") }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("invariant 1")
        assertThatThrownBy { CatalogInvariants.assertSnapshotsDense(jdbi, "inv-no-such-catalog") }
            .isInstanceOf(AssertionError::class.java)
            .hasMessageContaining("no catalog named")
    }
}
