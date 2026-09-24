package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * [FileRepo.providedColumnStatsAt] called DIRECTLY, against the two
 * rules its SQL carries that no wire test can red.
 *
 * Both rules are also enforced one layer up — `ScanService` skips any
 * file the plan did not call `provided`, and passes the plan's own
 * snapshot — so a test that drives GET .../scan passes with either one
 * of them deleted from the statement. The redundancy is deliberate (a
 * wrong bound reaching a reader is a wrong ANSWER, not a slow one), and
 * redundancy that nothing pins is redundancy that gets deleted as dead
 * code. So these call the repo function itself:
 *
 *  1. **`AND df.stats_state = 'provided'`.** A `pending` file's stats
 *     rows are a half-written set: the hydrator inserts them and flips
 *     the state in one transaction, but a file re-registered for
 *     rehydration, or one whose rows an operator planted, can hold rows
 *     while the state says its statistics are not vouched for. A reader
 *     that prunes on them prunes on bounds no writer stands behind. The
 *     fixture puts real rows under a `pending` file precisely so the
 *     Kotlin guard cannot be what makes this pass.
 *
 *  2. **The snapshot is the PLAN's.** The statement re-applies
 *     [FileRepo.visibleAt] at `:snapshot`, the same predicate text the
 *     plan's own file query uses, so stats can never be read under
 *     different visibility semantics than the files they are attached
 *     to. Passing head — or `Long.MAX_VALUE` — instead would attach a
 *     file's bounds to a time-travel plan taken before the file
 *     existed.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FileColumnStatsRepoIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)

    private var catalogId = 0L
    private var tableId = 0L

    /** Snapshot at which only [earlyFileId] exists. */
    private var earlySnapshot = 0L
    private var earlyFileId = 0L

    /** Registered one snapshot later; invisible at [earlySnapshot]. */
    private var lateFileId = 0L
    private var lateSnapshot = 0L

    /** Carries stats rows AND `stats_state = 'pending'`. */
    private var pendingFileId = 0L

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seed() {
        catalogs.createCatalog("statsrepo", "s3://statsrepo")
        catalogs.createNamespace("statsrepo", "ns")
        catalogs.createTable("statsrepo", "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        db.jdbi.useHandleUnchecked { h ->
            catalogId =
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = 'statsrepo'")
                    .mapTo(Long::class.java).one()
            tableId =
                h.createQuery("SELECT table_id FROM hog_table WHERE catalog_id = :c")
                    .bind("c", catalogId).mapTo(Long::class.java).one()

            fun snapshot(id: Long) =
                h.createUpdate(
                    "INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) VALUES (:c, :s, 1)",
                ).bind("c", catalogId).bind("s", id).execute()

            val base =
                h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE catalog_id = :c")
                    .bind("c", catalogId).mapTo(Long::class.java).one()
            earlySnapshot = base + 1
            lateSnapshot = base + 2
            snapshot(earlySnapshot)
            snapshot(lateSnapshot)
            h.createUpdate("UPDATE hog_catalog SET last_snapshot_id = :s WHERE catalog_id = :c")
                .bind("c", catalogId).bind("s", lateSnapshot).execute()

            fun file(
                id: Long,
                begin: Long,
                state: String,
            ) {
                h.createUpdate(
                    """
                    INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                               path, record_count, file_size_bytes, row_id_start,
                                               stats_state)
                    VALUES (:c, :id, :t, :begin, 's3://statsrepo/' || :id || '.parquet', 10, 1024,
                            :id * 10, :state)
                    """,
                ).bind("c", catalogId).bind("id", id).bind("t", tableId)
                    .bind("begin", begin).bind("state", state).execute()
                // One stats row per column on EVERY file, the pending one
                // included: the point of this fixture is that rows exist
                // where the state says they must not be read.
                for (fieldId in 1L..2L) {
                    h.createUpdate(
                        """
                        INSERT INTO hog_file_column_stats (catalog_id, data_file_id, field_id,
                                                           value_count, null_count,
                                                           lower_bound, upper_bound)
                        VALUES (:c, :f, :fid, 10, 0, '\x0000000000000001'::bytea,
                                '\x0000000000000002'::bytea)
                        """,
                    ).bind("c", catalogId).bind("f", id).bind("fid", fieldId).execute()
                }
            }
            earlyFileId = 1
            lateFileId = 2
            pendingFileId = 3
            file(earlyFileId, earlySnapshot, "provided")
            file(lateFileId, lateSnapshot, "provided")
            file(pendingFileId, earlySnapshot, "pending")
        }
    }

    private fun statsAt(
        snapshot: Long,
        fieldIds: Set<Long>? = null,
    ) = db.jdbi.withHandleUnchecked { h ->
        FileRepo.providedColumnStatsAt(h, catalogId, tableId, snapshot, fieldIds)
    }

    @Test
    fun `a pending file's stats rows are not returned, even though it has them`() {
        // The fixture check first: without it this test would pass on an
        // empty table and prove nothing about the filter.
        val plantedRows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_file_column_stats WHERE catalog_id = :c AND data_file_id = :f",
                ).bind("c", catalogId).bind("f", pendingFileId).mapTo(Int::class.java).one()
            }
        assertThat(plantedRows)
            .describedAs("the pending file must HAVE stats rows or this test asserts nothing")
            .isEqualTo(2)

        val rows = statsAt(lateSnapshot)
        assertThat(rows.keys)
            .describedAs(
                "the `provided` filter lives in the SQL, not only in ScanService: a pending file's " +
                    "rows are bounds no writer vouched for and must not reach a reader by any route",
            )
            .containsExactlyInAnyOrder(earlyFileId, lateFileId)
        // ...and narrowing must not become a way around it either.
        assertThat(statsAt(lateSnapshot, setOf(1L)).keys)
            .containsExactlyInAnyOrder(earlyFileId, lateFileId)
    }

    @Test
    fun `stats are read at the plan's snapshot, so a file that did not exist yet has none`() {
        // Time travel to before the late file was registered. At head
        // both files answer; one snapshot earlier only the early one
        // does. A statement that passed head, or Long.MAX_VALUE, instead
        // of the plan's snapshot would return both here — attaching
        // bounds to a plan whose file list cannot contain the file they
        // belong to.
        assertThat(statsAt(lateSnapshot).keys).containsExactlyInAnyOrder(earlyFileId, lateFileId)

        val travelled = statsAt(earlySnapshot)
        assertThat(travelled.keys)
            .describedAs("at snapshot %d the late file is not yet registered", earlySnapshot)
            .containsExactly(earlyFileId)
        assertThat(travelled.getValue(earlyFileId).map { it.fieldId }).containsExactly(1L, 2L)
    }

    @Test
    fun `an end-snapshotted file drops out at the snapshot that retired it`() {
        // The other half of invariant 6's predicate. Asserted on its own
        // because a statement that kept only `begin_snapshot <= :snapshot`
        // passes the test above unchanged.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_data_file SET end_snapshot = :s WHERE catalog_id = :c AND data_file_id = :f",
            ).bind("c", catalogId).bind("s", lateSnapshot).bind("f", earlyFileId).execute()
        }
        try {
            assertThat(statsAt(earlySnapshot).keys)
                .describedAs("still live one snapshot before it was retired")
                .containsExactly(earlyFileId)
            assertThat(statsAt(lateSnapshot).keys)
                .describedAs("retired AT lateSnapshot, so invisible at it")
                .containsExactly(lateFileId)
        } finally {
            db.jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    "UPDATE hog_data_file SET end_snapshot = NULL WHERE catalog_id = :c AND data_file_id = :f",
                ).bind("c", catalogId).bind("f", earlyFileId).execute()
            }
        }
    }

    @Test
    fun `the scan read does not carry size_bytes, and says so by leaving it null`() {
        // The statement does not SELECT the column (ScanColumnStats drops
        // it), so null here means NOT READ. Pinned because the shared
        // mapper does read it and a future merge of the two would put the
        // column back on the wire for every file x column silently.
        fun setSizeBytes(value: Long?) =
            db.jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    "UPDATE hog_file_column_stats SET size_bytes = :v " +
                        "WHERE catalog_id = :c AND data_file_id = :f",
                ).bind("v", value).bind("c", catalogId).bind("f", earlyFileId).execute()
            }
        // Restored in `finally`, as its sibling above does: the fixture
        // is built once for the class, so a mutation left behind is a
        // mutation every later test inherits.
        try {
            setSizeBytes(4096L)
            assertThat(statsAt(lateSnapshot).getValue(earlyFileId))
                .allSatisfy { assertThat(it.sizeBytes).isNull() }
            // The per-file read, which does select it, still sees the
            // value — so this is an omission by the scan statement and
            // not a lost write.
            assertThat(
                db.jdbi.withHandleUnchecked { h -> FileRepo.columnStats(h, catalogId, earlyFileId) },
            ).allSatisfy { assertThat(it.sizeBytes).isEqualTo(4096L) }
        } finally {
            setSizeBytes(null)
        }
    }
}
