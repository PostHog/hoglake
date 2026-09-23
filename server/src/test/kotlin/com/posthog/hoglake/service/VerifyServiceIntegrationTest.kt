package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.VerifyReport
import com.posthog.hoglake.persistence.OffsetRepo
import com.posthog.hoglake.service.VerifyService.Companion.MAX_SAMPLES
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

/**
 * B3 — the invariant scan: a healthy catalog passes every check, and
 * each violation class, seeded via raw SQL (the service layer refuses
 * to produce these states), trips exactly its own check.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VerifyServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val expiry = ExpiryService(db.jdbi)
    private val verify = VerifyService(db.jdbi)
    private val creations = TableCreationService(db.jdbi, catalogs, commits)

    /** Ids for synthetic rows, well above anything the services allocate. */
    private val syntheticId = AtomicLong(900)

    /**
     * The service's OWN table list, so a table added to or removed from
     * it is covered (or uncovered) here without a second edit. Restating
     * the list would assert only that this file compiles.
     */
    @Suppress("unused")
    private fun belowFloorTables(): List<Arguments> =
        VerifyService.BELOW_FLOOR_TABLES.map { Arguments.of(it.table, it.idColumn) }

    private fun headOf(catalog: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE name = ?")
                .bind(0, catalog).mapTo(Long::class.java).one()
        }

    @AfterAll
    fun tearDown() = db.close()

    private fun catalogId(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = ?")
                .bind(0, name).mapTo(Long::class.java).one()
        }

    /** catalog + ns + table "t", plus [files] appended row counts. */
    private fun seed(
        catalog: String,
        files: List<Long> = emptyList(),
    ): Long {
        catalogs.createCatalog(catalog, "s3://vfy/$catalog")
        catalogs.createNamespace(catalog, "ns")
        catalogs.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        if (files.isNotEmpty()) {
            commits.commit(
                catalog,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                files.mapIndexed { i, rc ->
                                    FileRegistration("s3://vfy/$catalog/f$i.parquet", rc, rc * 8)
                                },
                            ),
                        ),
                ),
            )
        }
        return catalogId(catalog)
    }

    private fun VerifyReport.check(name: String) = checks.single { it.check == name }

    @Test
    fun `a freshly created never-committed catalog passes every check`() {
        // Pinned for bug hunt #14 (a FALSE finding, verified here at the
        // service level): createCatalog seeds snapshot 0, so a
        // never-committed catalog has head=0, earliest=0, count(*)=1 —
        // exactly head - earliest + 1. Formula and seed agree.
        catalogs.createCatalog("vfy-fresh", "s3://vfy/vfy-fresh")
        val report = verify.runOnce("vfy-fresh")
        assertThat(report.status).isEqualTo("pass")
        assertThat(report.check("snapshot_density").violations).isZero()
    }

    private fun assertOnlyFails(
        report: VerifyReport,
        vararg failing: String,
    ) {
        assertThat(report.status).isEqualTo(if (failing.isEmpty()) "pass" else "fail")
        for (c in report.checks) {
            if (c.check in failing) {
                assertThat(c.status).describedAs("check %s should fail", c.check).isEqualTo("fail")
                assertThat(c.violations).isGreaterThan(0)
                assertThat(c.samples).isNotEmpty()
            } else {
                assertThat(c.status).describedAs("check %s should pass", c.check).isEqualTo("pass")
                assertThat(c.violations).isEqualTo(0)
            }
        }
    }

    @Test
    fun `a healthy catalog passes every check - commits, DVs, drop, and expiry included`() {
        val catalog = "vfy-healthy"
        seed(catalog, files = listOf(10L, 5L, 0L))
        // A DV, a supersession, a dropped table, and an expiry sweep: the
        // realistic states verify must NOT flag.
        val head =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE name = ?")
                    .bind(0, catalog).mapTo(Long::class.java).one()
            }
        commits.commit(
            catalog,
            CommitRequest(
                readSnapshot = head,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(DeleteFileRegistration(1, "s3://vfy/$catalog/dv1.puffin", 2, 16)),
                        ),
                    ),
            ),
        )
        commits.commit(
            catalog,
            CommitRequest(
                readSnapshot = head + 1,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(DeleteFileRegistration(1, "s3://vfy/$catalog/dv2.puffin", 4, 16)),
                        ),
                    ),
            ),
        )
        catalogs.createTable(catalog, "ns", "doomed", listOf(ColumnDef("id", ColType.LONG)))
        commits.commit(
            catalog,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "doomed",
                            listOf(FileRegistration("s3://vfy/$catalog/doomed.parquet", 3, 24)),
                        ),
                    ),
            ),
        )
        catalogs.dropTable(catalog, "ns", "doomed")
        // Age + retention so an expiry sweep really moves the floor.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_snapshot SET snapshot_time = now() - make_interval(secs => 3600)
                WHERE catalog_id = :c AND snapshot_id < 3
                """,
            ).bind("c", catalogId(catalog)).execute()
            h.createUpdate(
                "UPDATE hog_catalog SET snapshot_retention_seconds = 60, consumer_floor = false " +
                    "WHERE name = :n",
            ).bind("n", catalog).execute()
        }
        val swept = expiry.runOnce(catalog, batchSize = 1000)
        assertThat(swept.snapshotsExpired).isGreaterThan(0)

        val report = verify.runOnce(catalog)
        assertThat(report.catalog).isEqualTo(catalog)
        assertThat(report.checks.map { it.check }).containsExactly(
            "row_id_tiling",
            "delete_vectors",
            "orphans",
            "removal_queue",
            "snapshot_density",
            "next_row_id",
            "expiry_floor",
            "visibility_bounds",
            "offset_release",
            "staging_tickets",
            "upload_claims",
        )
        // Every check states its own invariant: a report is readable
        // without the spec, and an empty description is a check that
        // says only that something broke.
        assertThat(report.checks.map { it.description }).allSatisfy {
            assertThat(it).isNotBlank()
        }
        assertOnlyFails(report)
    }

    @Test
    fun `an overlapping positional row-id range trips row_id_tiling only`() {
        val cid = seed("vfy-tiling", files = listOf(10L, 10L)) // ranges [0,10) [10,20)
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT :c, 999, table_id, 3, 's3://vfy/vfy-tiling/overlap.parquet', 10, 80, 5
                FROM hog_table_stats WHERE catalog_id = :c
                """,
            ).bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce("vfy-tiling"), "row_id_tiling")
        val sample = verify.runOnce("vfy-tiling").check("row_id_tiling").samples.first()
        assertThat(sample).contains("overlaps")
    }

    @Test
    fun `a compaction-style explicit_row_ids file sharing input ids is NOT a tiling violation`() {
        val cid = seed("vfy-explicit", files = listOf(10L, 10L))
        // A compacted output covering both inputs' ids: row_id_start =
        // min(input starts), record_count = sum — positionally it would
        // "overlap" everything, which is exactly why the sweep exempts it.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start,
                                           explicit_row_ids)
                SELECT :c, 998, table_id, 3, 's3://vfy/vfy-explicit/compacted.parquet', 20, 160, 0, true
                FROM hog_table_stats WHERE catalog_id = :c
                """,
            ).bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce("vfy-explicit"))
    }

    @Test
    fun `a shrinking deletion-vector chain trips delete_vectors only`() {
        val catalog = "vfy-dv"
        val cid = seed(catalog, files = listOf(10L))
        commits.commit(
            catalog,
            CommitRequest(
                readSnapshot = 3,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(DeleteFileRegistration(1, "s3://vfy/$catalog/dv.puffin", 2, 16)),
                        ),
                    ),
            ),
        )
        // A fabricated HISTORICAL DV with a HIGHER count than the live
        // one that superseded it: the chain shrank — invariant 3 broken.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                                             begin_snapshot, end_snapshot, path, delete_count,
                                             file_size_bytes)
                SELECT :c, 997, table_id, 1, 1, 4, 's3://vfy/vfy-dv/old.puffin', 5, 16
                FROM hog_table_stats WHERE catalog_id = :c
                """,
            ).bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce(catalog), "delete_vectors")
        assertThat(verify.runOnce(catalog).check("delete_vectors").samples.first())
            .contains("shrank")
    }

    @Test
    fun `a live file row on a dropped table trips orphans only`() {
        val catalog = "vfy-orphan"
        val cid = seed(catalog) // no files: nothing for tiling/next_row_id to see
        catalogs.dropTable(catalog, "ns", "t")
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT :c, 996, table_id, 1, 's3://vfy/vfy-orphan/live-on-dropped.parquet', 0, 0, 0
                FROM hog_table_stats WHERE catalog_id = :c
                """,
            ).bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce(catalog), "orphans")
    }

    @Test
    fun `an undrained removal-queue row whose path is still referenced trips removal_queue only`() {
        val catalog = "vfy-queue"
        val cid = seed(catalog, files = listOf(5L))
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                VALUES (:c, 's3://vfy/$catalog/f0.parquet', 'data', 'snapshot_expiry')
                """,
            ).bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce(catalog), "removal_queue")
        // A DRAINED ledger row with the same path is history, not a violation.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_file_removal SET drained_at = now(), drained_outcome = 'absent' " +
                    "WHERE catalog_id = :c",
            ).bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce(catalog))
    }

    @Test
    fun `a hole in the snapshot sequence trips snapshot_density only`() {
        val cid = seed("vfy-density", files = listOf(2L)) // snapshots 0..3
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("DELETE FROM hog_snapshot WHERE catalog_id = :c AND snapshot_id = 1")
                .bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce("vfy-density"), "snapshot_density")
        assertThat(verify.runOnce("vfy-density").check("snapshot_density").samples.first())
            .contains("count(*)")
    }

    @Test
    fun `an allocator behind its handed-out ranges trips next_row_id only`() {
        val cid = seed("vfy-alloc", files = listOf(10L)) // range [0,10), next_row_id = 10
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_table_stats SET next_row_id = 5 WHERE catalog_id = :c")
                .bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce("vfy-alloc"), "next_row_id")
    }

    // ---- 7: expiry_floor (invariant 5) -------------------------------------

    /**
     * Age every snapshot out of a 60s retention window and sweep, so the
     * floor really moves; consumer_floor is off for the sweep and set by
     * the caller afterwards if the test needs it.
     */
    private fun advanceFloor(catalog: String): Long {
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_snapshot SET snapshot_time = now() - make_interval(secs => 3600)
                WHERE catalog_id = :c
                """,
            ).bind("c", catalogId(catalog)).execute()
            h.createUpdate(
                "UPDATE hog_catalog SET snapshot_retention_seconds = 60, consumer_floor = false " +
                    "WHERE name = :n",
            ).bind("n", catalog).execute()
        }
        val swept = expiry.runOnce(catalog, batchSize = 1000)
        assertThat(swept.snapshotsExpired).isGreaterThan(0)
        return swept.newEarliestSnapshotId
    }

    @Test
    fun `an expiry floor past head trips expiry_floor`() {
        val cid = seed("vfy-floor-head", files = listOf(3L))
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_catalog SET earliest_snapshot_id = last_snapshot_id + 1 WHERE catalog_id = :c",
            ).bind("c", cid).execute()
        }
        // Not assertOnlyFails: a floor above head also makes the dense
        // range arithmetic wrong, which is snapshot_density's job to say.
        val report = verify.runOnce("vfy-floor-head")
        assertThat(report.status).isEqualTo("fail")
        val floor = report.check("expiry_floor")
        assertThat(floor.violations).isEqualTo(1)
        assertThat(floor.samples.single()).contains("is past head=")
    }

    @Test
    fun `a consumer offset below the floor trips expiry_floor only`() {
        val catalog = "vfy-floor-consumer"
        val cid = seed(catalog, files = listOf(3L))
        val uuid = catalogs.getTable(catalog, "ns", "t").tableUuid
        val floor = advanceFloor(catalog)
        assertThat(floor).isGreaterThan(0)
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_catalog SET consumer_floor = true WHERE catalog_id = :c")
                .bind("c", cid).execute()
            h.createUpdate(
                """
                INSERT INTO hog_consumer_offset (catalog_id, consumer_id, table_uuid, committed_snapshot)
                VALUES (:c, 'lagging', :uuid, 0)
                """,
            ).bind("c", cid).bind("uuid", uuid).execute()
        }
        assertOnlyFails(verify.runOnce(catalog), "expiry_floor")
        assertThat(verify.runOnce(catalog).check("expiry_floor").samples.single())
            .contains("consumer 'lagging'")
            .contains("below the expiry floor")

        // consumer_floor OFF is not a violation: the sweep does not read
        // offsets at all, so the floor was never theirs to hold.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_catalog SET consumer_floor = false WHERE catalog_id = :c")
                .bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce(catalog))
    }

    /**
     * One synthetic row of [table] with the given snapshot bounds — the
     * fixture behind the parametrized survivor and bounds tests.
     *
     * Every branch is a compile-time literal (invariant 9 intact). The
     * rows are deliberately inert for the OTHER checks: zero-record data
     * files occupy no row ids, the delete file is the only DV on its
     * data file and deletes fewer rows than that file holds, and nothing
     * is live, so no partial unique index and no `orphans` predicate
     * sees them. That is what lets these tests assert `assertOnlyFails`.
     */
    private fun insertVersionedRow(
        catalog: String,
        table: String,
        begin: Long,
        end: Long?,
    ): Long {
        val cid = catalogId(catalog)
        val id = syntheticId.getAndIncrement()
        val sql =
            when (table) {
                "hog_data_file" ->
                    """
                    INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                               end_snapshot, path, record_count, file_size_bytes,
                                               row_id_start)
                    SELECT :c, :id, table_id, :begin, :end, :path, 0, 0, 0
                    FROM hog_table_stats WHERE catalog_id = :c
                    """
                "hog_delete_file" ->
                    """
                    INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                                                 begin_snapshot, end_snapshot, path, delete_count,
                                                 file_size_bytes)
                    SELECT :c, :id, f.table_id, f.data_file_id, :begin, :end, :path, 1, 16
                    FROM hog_data_file f
                    WHERE f.catalog_id = :c AND NOT EXISTS (
                        SELECT 1 FROM hog_delete_file d
                        WHERE d.catalog_id = f.catalog_id AND d.data_file_id = f.data_file_id)
                    ORDER BY f.data_file_id LIMIT 1
                    """
                "hog_table_version" ->
                    """
                    INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot,
                                                   end_snapshot, namespace_id, name)
                    SELECT :c, v.table_id, :begin, :end, v.namespace_id, 'synthetic'
                    FROM hog_table_version v WHERE v.catalog_id = :c
                    ORDER BY v.table_id, v.begin_snapshot LIMIT 1
                    """
                "hog_column" ->
                    """
                    INSERT INTO hog_column (catalog_id, table_id, field_id, begin_snapshot,
                                            end_snapshot, name, col_type, ordinal)
                    SELECT :c, table_id, :id, :begin, :end, 'synthetic', 'long', :id
                    FROM hog_table_stats WHERE catalog_id = :c
                    """
                "hog_partition_spec" ->
                    """
                    INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot,
                                                    end_snapshot)
                    SELECT :c, table_id, :id, :begin, :end FROM hog_table_stats WHERE catalog_id = :c
                    """
                "hog_sort_spec" ->
                    """
                    INSERT INTO hog_sort_spec (catalog_id, table_id, sort_id, begin_snapshot,
                                               end_snapshot)
                    SELECT :c, table_id, :id, :begin, :end FROM hog_table_stats WHERE catalog_id = :c
                    """
                "hog_view" ->
                    """
                    INSERT INTO hog_view (catalog_id, view_id, namespace_id, name, sql,
                                          begin_snapshot, end_snapshot)
                    SELECT :c, :id, namespace_id, 'synthetic_view', 'SELECT 1', :begin, :end
                    FROM hog_namespace WHERE catalog_id = :c LIMIT 1
                    """
                else -> error("no fixture for $table")
            }
        db.jdbi.useHandleUnchecked { h ->
            val inserted =
                h.createUpdate(sql)
                    .bind("c", cid).bind("id", id).bind("begin", begin)
                    .bind("path", "s3://vfy/$catalog/synthetic-$id.parquet")
                    .apply { if (end == null) bindNull("end", java.sql.Types.BIGINT) else bind("end", end) }
                    .execute()
            assertThat(inserted).describedAs("fixture row for %s", table).isEqualTo(1)
        }
        return id
    }

    /**
     * EVERY table the sweep clears below the floor, not just the first.
     *
     * The loop body is one SQL string, so a per-table bug is not the
     * failure mode — a bug in the TABLE LIST is, and a test that only
     * ever seeded hog_data_file could not see six of the seven entries
     * go missing. It also pins the boundary: `end_snapshot` EQUAL to the
     * floor must trip, because a row ending at S is invisible at S (the
     * visibility rule is `S < end_snapshot`), which is exactly why the
     * sweep deletes with `<=`.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("belowFloorTables")
    fun `a versioned row ending AT the floor trips expiry_floor only, for every swept table`(
        table: String,
        idColumn: String,
    ) {
        val catalog = "vfy-floor-survivor-$table"
        seed(catalog, files = listOf(3L))
        val floor = advanceFloor(catalog)
        assertThat(floor).isGreaterThan(1)
        val id = insertVersionedRow(catalog, table, begin = 0, end = floor)

        assertOnlyFails(verify.runOnce(catalog), "expiry_floor")
        val sample = verify.runOnce(catalog).check("expiry_floor").samples.single()
        assertThat(sample)
            .contains("$table $idColumn=")
            .contains("row [0, $floor) survived the floor advance to $floor")
        if (table != "hog_table_version") {
            // That one is keyed by table alone, so its sample names the
            // table rather than the synthetic id; the rest name the row.
            assertThat(sample).contains("$idColumn=$id")
        }
    }

    // ---- 8: visibility_bounds (invariant 6) --------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("belowFloorTables")
    fun `a versioned row ending past head trips visibility_bounds only, for every table`(
        table: String,
        idColumn: String,
    ) {
        val catalog = "vfy-bounds-end-$table"
        seed(catalog, files = listOf(3L))
        val head = headOf(catalog)
        insertVersionedRow(catalog, table, begin = 1, end = head + 5)

        assertOnlyFails(verify.runOnce(catalog), "visibility_bounds")
        assertThat(verify.runOnce(catalog).check("visibility_bounds").samples.single())
            .contains("$table $idColumn=")
            .contains("begin_snapshot=1")
            .contains("end_snapshot=${head + 5}")
            .contains("is outside [0, head=$head]")
    }

    @Test
    fun `a live versioned row beginning past head trips visibility_bounds only`() {
        // begin > head is only separable from end > head on a row with
        // NO end_snapshot: the table CHECK forces end > begin, so any
        // ended row that begins past head also ends past it. A live row
        // hits the partial unique indexes on most of these tables, so
        // this one is pinned on hog_data_file, whose live index is not
        // unique — the loop body is the same string for all seven.
        val catalog = "vfy-bounds-begin"
        seed(catalog, files = listOf(3L))
        val head = headOf(catalog)
        val id = insertVersionedRow(catalog, "hog_data_file", begin = head + 5, end = null)

        assertOnlyFails(verify.runOnce(catalog), "visibility_bounds")
        assertThat(verify.runOnce(catalog).check("visibility_bounds").samples.single())
            .isEqualTo(
                "hog_data_file data_file_id=$id begin_snapshot=${head + 5} end_snapshot=null " +
                    "is outside [0, head=$head]",
            )
    }

    @Test
    fun `a table dropped before it was created trips visibility_bounds only`() {
        val catalog = "vfy-bounds-table"
        val cid = seed(catalog)
        // Drop through the real path first, so the table's columns and
        // version rows are end-snapshotted and `orphans` stays quiet —
        // only the identity row's created/dropped pair is wrong.
        catalogs.dropTable(catalog, "ns", "t")
        val created =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT created_snapshot FROM hog_table WHERE catalog_id = :c")
                    .bind("c", cid).mapTo(Long::class.java).one()
            }
        val tableId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT table_id FROM hog_table WHERE catalog_id = :c")
                    .bind("c", cid).mapTo(Long::class.java).one()
            }
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_table SET dropped_snapshot = created_snapshot - 1 WHERE catalog_id = :c",
            ).bind("c", cid).execute()
        }
        assertOnlyFails(verify.runOnce(catalog), "visibility_bounds")
        assertThat(verify.runOnce(catalog).check("visibility_bounds").samples.single())
            .isEqualTo(
                "hog_table table_id=$tableId created_snapshot=$created " +
                    "dropped_snapshot=${created - 1} is outside [0, head=${headOf(catalog)}]",
            )
    }

    @Test
    fun `a table created or dropped past head trips visibility_bounds only`() {
        val catalog = "vfy-bounds-table-head"
        val cid = seed(catalog)
        val head = headOf(catalog)
        val tableId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT table_id FROM hog_table WHERE catalog_id = :c")
                    .bind("c", cid).mapTo(Long::class.java).one()
            }
        // dropped_snapshot past head: a drop the catalog has no snapshot
        // for. The columns and version rows stay live, and the table is
        // now "dropped", so orphans legitimately fires too — this
        // assertion is about visibility_bounds seeing it at all.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_table SET dropped_snapshot = :d WHERE catalog_id = :c",
            ).bind("c", cid).bind("d", head + 5).execute()
        }
        val droppedReport = verify.runOnce(catalog)
        assertThat(droppedReport.check("visibility_bounds").violations).isEqualTo(1)
        assertThat(droppedReport.check("visibility_bounds").samples.single())
            .contains("hog_table table_id=$tableId")
            .contains("dropped_snapshot=${head + 5}")
            .contains("head=$head")

        // created_snapshot past head, on its own.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_table SET dropped_snapshot = NULL, created_snapshot = :cs WHERE catalog_id = :c",
            ).bind("c", cid).bind("cs", head + 5).execute()
        }
        assertOnlyFails(verify.runOnce(catalog), "visibility_bounds")
        assertThat(verify.runOnce(catalog).check("visibility_bounds").samples.single())
            .contains("created_snapshot=${head + 5}")
            .contains("dropped_snapshot=null")
    }

    // ---- 9: offset_release (#167) ------------------------------------------

    @Test
    fun `an unreleased offset on a replaced incarnation trips offset_release only`() {
        val catalog = "vfy-offset-release"
        val cid = seed(catalog)
        val old = catalogs.getTable(catalog, "ns", "t").tableUuid
        // A REAL atomic replacement, so the lineage edge is the one the
        // replacement path records rather than one the test invented.
        val prepared =
            creations.prepare(
                catalog,
                UUID.randomUUID(),
                TableCreationDefinition(
                    "ns",
                    "t",
                    listOf(ColumnDef("id", ColType.LONG)),
                    ReplacementTarget(old, catalogs.getCatalog(catalog).headSnapshotId),
                ),
            )
        val replacement = creations.publish(catalog, prepared.operationId, emptyList()).snapshotId!!
        // Both rows written straight to SQL: going through commitOffset
        // would RELEASE the retired row, which is the behaviour this
        // check exists to notice the absence of.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_consumer_offset (catalog_id, consumer_id, table_uuid, committed_snapshot)
                VALUES (:c, 'cdc', :old, 0), (:c, 'cdc', :new, :replacement)
                """,
            ).bind("c", cid).bind("old", old).bind("new", prepared.tableUuid)
                .bind("replacement", replacement).execute()
        }
        assertOnlyFails(verify.runOnce(catalog), "offset_release")
        assertThat(verify.runOnce(catalog).check("offset_release").samples.single())
            .contains("consumer 'cdc' still holds an offset on retired table_uuid=$old")

        // The release is what clears it: running the sweep's own forward
        // form deletes the row, and the check goes quiet.
        db.jdbi.useHandleUnchecked { h -> OffsetRepo.releaseSupersededOffsets(h, cid) }
        assertOnlyFails(verify.runOnce(catalog))
    }

    // ---- 10: staging_tickets (#174) ----------------------------------------

    private fun stagingTicket(
        cid: Long,
        path: String,
        outcome: String?,
        scheduledAgoSeconds: Long = 0,
        reason: String = "compaction_staging",
    ) = db.jdbi.useHandleUnchecked { h ->
        h.createUpdate(
            """
            INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, scheduled_at,
                                          drained_at, drained_outcome)
            VALUES (:c, :path, 'data', :reason, now() - make_interval(secs => :ago),
                    CASE WHEN CAST(:outcome AS text) IS NULL THEN NULL ELSE now() END,
                    CAST(:outcome AS text))
            """,
        ).bind("c", cid).bind("path", path).bind("reason", reason)
            .bind("ago", scheduledAgoSeconds).bind("outcome", outcome).execute()
    }

    @Test
    fun `a staging ticket settled registered with nothing registered trips staging_tickets only`() {
        val catalog = "vfy-staging-ghost"
        val cid = seed(catalog)
        val path = "s3://vfy/$catalog/ghost.parquet"
        stagingTicket(cid, path, outcome = "registered")
        assertOnlyFails(verify.runOnce(catalog), "staging_tickets")
        assertThat(verify.runOnce(catalog).check("staging_tickets").samples.single())
            .contains("settled 'registered' but no file row")

        // A LATER ledger row for the same path is the normal lifecycle —
        // the output was registered, then compacted or expired away, and
        // whatever removed it left its own row. Not a violation.
        stagingTicket(cid, path, outcome = "deleted", reason = "snapshot_expiry")
        assertOnlyFails(verify.runOnce(catalog))
    }

    @Test
    fun `an undrained staging ticket older than the bound trips staging_tickets only`() {
        val catalog = "vfy-staging-leak"
        val cid = seed(catalog)
        val path = "s3://vfy/$catalog/leaked.parquet"
        // Fresh: a group could still be in flight, or the cleanup drain
        // simply has not reached it. Silence is correct.
        stagingTicket(cid, path, outcome = null, scheduledAgoSeconds = 60)
        assertOnlyFails(verify.runOnce(catalog))

        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_file_removal SET scheduled_at = now() - make_interval(secs => :ago) " +
                    "WHERE catalog_id = :c",
            ).bind("c", cid)
                .bind("ago", VerifyService.DEFAULT_STAGING_TICKET_MAX_AGE_SECONDS + 60).execute()
        }
        assertOnlyFails(verify.runOnce(catalog), "staging_tickets")
        assertThat(verify.runOnce(catalog).check("staging_tickets").samples.single())
            .contains("is an undrained compaction_staging ticket")
    }

    @Test
    fun `a staging ticket drained absent whose path is registered trips staging_tickets only`() {
        val catalog = "vfy-staging-absent"
        val cid = seed(catalog, files = listOf(4L))
        stagingTicket(cid, "s3://vfy/$catalog/f0.parquet", outcome = "absent")
        assertOnlyFails(verify.runOnce(catalog), "staging_tickets")
        assertThat(verify.runOnce(catalog).check("staging_tickets").samples.single())
            .contains("was drained 'absent' but the catalog holds a file row")
    }

    // ---- 11: upload_claims (#162/#167) -------------------------------------

    private fun uploadClaim(
        cid: Long,
        path: String,
        state: String,
    ) = db.jdbi.useHandleUnchecked { h ->
        h.createUpdate(
            """
            INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind, state)
            VALUES (:c, gen_random_uuid(), gen_random_uuid(), 's3://vfy/prefix', :path, 'data', :state)
            """,
        ).bind("c", cid).bind("path", path).bind("state", state).execute()
    }

    @Test
    fun `a registered claim queued for upload reclamation trips upload_claims only`() {
        val catalog = "vfy-claim-queued"
        val cid = seed(catalog)
        val path = "s3://vfy/$catalog/claimed.parquet"
        uploadClaim(cid, path, "registered")
        // An expiry-queued path is the normal lifecycle of a registered
        // claim whose file row has aged out: NOT a violation.
        stagingTicket(cid, path, outcome = null, reason = "snapshot_expiry")
        assertOnlyFails(verify.runOnce(catalog))

        // A trino_upload ticket for the same path says the sweep fenced a
        // claim that a publication had already settled.
        stagingTicket(cid, path, outcome = null, reason = "trino_upload")
        assertOnlyFails(verify.runOnce(catalog), "upload_claims")
        assertThat(verify.runOnce(catalog).check("upload_claims").samples.single())
            .contains("is 'registered' but an undrained trino_upload removal row")
    }

    @Test
    fun `active and abandoned claims on registered paths trip upload_claims only`() {
        val catalog = "vfy-claim-unsettled"
        val cid = seed(catalog, files = listOf(4L, 4L))
        uploadClaim(cid, "s3://vfy/$catalog/f0.parquet", "active")
        uploadClaim(cid, "s3://vfy/$catalog/f1.parquet", "abandoned")
        val report = verify.runOnce(catalog)
        assertOnlyFails(report, "upload_claims")
        val claims = report.check("upload_claims")
        assertThat(claims.violations).isEqualTo(2)
        assertThat(claims.samples)
            .anySatisfy { assertThat(it).contains("registered without the claim being settled") }
            .anySatisfy { assertThat(it).contains("fenced a path the catalog has registered") }
    }

    // ---- bounded work ------------------------------------------------------

    @Test
    fun `twenty-five violations report the true count and only twenty samples`() {
        val catalog = "vfy-bounded"
        val cid = seed(catalog, files = listOf(4L))
        db.jdbi.useHandleUnchecked { h ->
            repeat(25) {
                h.createUpdate(
                    """
                    INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                    VALUES (:c, 's3://vfy/$catalog/f0.parquet', 'data', 'snapshot_expiry')
                    """,
                ).bind("c", cid).execute()
            }
        }
        val queue = verify.runOnce(catalog).check("removal_queue")
        // The count is count(*), never the sample length: a badly broken
        // catalog reports how broken it is AND stays a bounded response.
        assertThat(queue.violations).isEqualTo(25)
        assertThat(queue.samples).hasSize(MAX_SAMPLES)
        assertThat(MAX_SAMPLES).isEqualTo(20)
    }

    // ---- counter-cases: the exclusions the checks are built on --------------

    @Test
    fun `an aged undrained snapshot_expiry row with no file row is NOT a staging violation`() {
        // The state a cleanup drain that keeps failing on S3 leaves
        // behind: an expiry-queued path, older than any bound, whose
        // file row expiry already deleted. It is a backlog — the
        // removal-queue depth gauge and the ledger's attempts counter
        // are what say so — and calling it a leaked compaction claim
        // would page on every catalog whose bucket is unreachable.
        // `reason = 'compaction_staging'` is the whole of what keeps
        // this quiet, so it is pinned here rather than assumed.
        val catalog = "vfy-staging-not-expiry"
        val cid = seed(catalog)
        stagingTicket(
            cid,
            "s3://vfy/$catalog/long-gone.parquet",
            outcome = null,
            scheduledAgoSeconds = VerifyService.DEFAULT_STAGING_TICKET_MAX_AGE_SECONDS * 10,
            reason = "snapshot_expiry",
        )
        assertOnlyFails(verify.runOnce(catalog))

        // The same row, same age, no file row — only the reason differs.
        stagingTicket(
            cid,
            "s3://vfy/$catalog/staged.parquet",
            outcome = null,
            scheduledAgoSeconds = VerifyService.DEFAULT_STAGING_TICKET_MAX_AGE_SECONDS * 10,
            reason = "compaction_staging",
        )
        assertOnlyFails(verify.runOnce(catalog), "staging_tickets")
    }

    @Test
    fun `an offset whose table identity is gone cannot floor expiry and is not flagged`() {
        // ExpiryService.FLOOR_CANDIDATE_OFFSETS joins hog_table on
        // purpose: an offset naming a uuid the catalog no longer holds
        // cannot be advanced by anyone AND cannot pin retention, because
        // the sweep does not see it either. Flagging it would report a
        // violation of a rule the sweep does not enforce.
        val catalog = "vfy-floor-orphan-offset"
        val cid = seed(catalog, files = listOf(3L))
        val floor = advanceFloor(catalog)
        assertThat(floor).isGreaterThan(0)
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_catalog SET consumer_floor = true WHERE catalog_id = :c")
                .bind("c", cid).execute()
            h.createUpdate(
                """
                INSERT INTO hog_consumer_offset (catalog_id, consumer_id, table_uuid, committed_snapshot)
                VALUES (:c, 'ghost', :uuid, 0)
                """,
            ).bind("c", cid).bind("uuid", UUID.randomUUID()).execute()
        }
        assertOnlyFails(verify.runOnce(catalog))

        // And the sweep agrees: it is not floored by a row it cannot see.
        assertThat(expiry.runOnce(catalog, batchSize = 1000).flooredByConsumer).isNull()
    }

    @Test
    fun `a superseded offset below the floor is offset_release's, never expiry_floor's`() {
        // The two checks partition the same population and the split is
        // load-bearing. A row the #167 release would delete is dead: the
        // sweep releases it BEFORE it measures, so it never floors
        // anything, and expiry_floor must stay quiet about it while
        // offset_release says exactly what is wrong.
        val catalog = "vfy-floor-superseded"
        val cid = seed(catalog)
        val old = catalogs.getTable(catalog, "ns", "t").tableUuid
        val prepared =
            creations.prepare(
                catalog,
                UUID.randomUUID(),
                TableCreationDefinition(
                    "ns",
                    "t",
                    listOf(ColumnDef("id", ColType.LONG)),
                    ReplacementTarget(old, catalogs.getCatalog(catalog).headSnapshotId),
                ),
            )
        val replacement = creations.publish(catalog, prepared.operationId, emptyList()).snapshotId!!
        commits.commit(
            catalog,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(FileRegistration("s3://vfy/$catalog/after.parquet", 2, 16)),
                            expectedTableUuid = prepared.tableUuid,
                        ),
                    ),
            ),
        )
        // Retention ON and the floor advanced past the stranded offset,
        // so the row really is below the floor.
        val floor = advanceFloor(catalog)
        assertThat(floor).isGreaterThan(0)
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_catalog SET consumer_floor = true WHERE catalog_id = :c")
                .bind("c", cid).execute()
            h.createUpdate(
                """
                INSERT INTO hog_consumer_offset (catalog_id, consumer_id, table_uuid, committed_snapshot)
                VALUES (:c, 'cdc', :old, 0), (:c, 'cdc', :new, :head)
                """,
            ).bind("c", cid).bind("old", old).bind("new", prepared.tableUuid)
                // The LIVE row sits at the floor, so the only offset
                // below the floor is the superseded one — which is the
                // whole point. (A live row below the floor is a real
                // expiry_floor violation, and has its own test.)
                .bind("head", headOf(catalog)).execute()
        }
        assertThat(replacement).isLessThanOrEqualTo(headOf(catalog))
        assertOnlyFails(verify.runOnce(catalog), "offset_release")
    }

    // ---- upload claims, through the real service ---------------------------

    @Test
    fun `a claim taken, registered and committed through the real services passes upload_claims`() {
        // The healthy end state, produced by UploadService + a real
        // commit rather than by SQL: a claim settled 'registered' in the
        // same transaction that made its path a live data file. If the
        // check's state list ever grew 'registered', this is what would
        // red — see the mutation note on the state list.
        val catalog = "vfy-claim-real"
        seed(catalog)
        val uploads = UploadService(db.jdbi)
        // The commit path settles claims owned by its IDEMPOTENCY KEY
        // (CommitService hands req.idempotencyKey to UploadService.register),
        // so the claim is taken under that same uuid.
        val owner = UUID.randomUUID()
        val claim = uploads.claim(catalog, UUID.randomUUID(), owner, "s3://vfy/$catalog", "data")
        assertThat(claim.state).isEqualTo("active")

        commits.commit(
            catalog,
            CommitRequest(
                idempotencyKey = owner,
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(FileRegistration(claim.path, 4, 32)),
                        ),
                    ),
            ),
        )
        val state =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT state FROM hog_upload WHERE catalog_id = :c")
                    .bind("c", catalogId(catalog)).mapTo(String::class.java).one()
            }
        assertThat(state).describedAs("register settles the claim in the commit").isEqualTo("registered")
        assertOnlyFails(verify.runOnce(catalog))
    }

    // ---- descriptions are per-process, and say what the check checks ------

    @Test
    fun `the staging bound in the description is the one the check enforces`() {
        // The description NAMES the bound, and the bound is a knob, so a
        // service built with a different one must not quote the default
        // — and the number it quotes has to be the number it enforces.
        val catalog = "vfy-staging-bound"
        val cid = seed(catalog)
        val tight = VerifyService(db.jdbi, stagingTicketMaxAgeSeconds = 5)
        assertThat(tight.runOnce(catalog).check("staging_tickets").description)
            .contains("older than 5 seconds")
            .doesNotContain("${VerifyService.DEFAULT_STAGING_TICKET_MAX_AGE_SECONDS} seconds")

        stagingTicket(cid, "s3://vfy/$catalog/staged.parquet", outcome = null, scheduledAgoSeconds = 10)
        // Ten seconds old: past the 5s service, comfortably inside the
        // default one. Same row, two answers, both correct.
        assertOnlyFails(tight.runOnce(catalog), "staging_tickets")
        assertOnlyFails(verify.runOnce(catalog))
    }

    @Test
    fun `the staging description quotes this process's compaction and cleanup knobs`() {
        val catalog = "vfy-staging-knobs"
        seed(catalog)
        val tuned =
            VerifyService(
                db.jdbi,
                compactionTargetBytes = 64L * 1024 * 1024,
                cleanupIntervalMs = 120_000,
            )
        assertThat(tuned.runOnce(catalog).check("staging_tickets").description)
            .contains("HOGLAKE_COMPACTION_TARGET_BYTES, 64 MiB here")
            .contains("HOGLAKE_CLEANUP_INTERVAL_MS, every 120 seconds here")
        val off = VerifyService(db.jdbi, cleanupIntervalMs = 0)
        assertThat(off.runOnce(catalog).check("staging_tickets").description)
            .describedAs("a drain that is off must not be quoted as a cadence")
            .contains("HOGLAKE_CLEANUP_INTERVAL_MS, disabled here")
    }

    @Test
    fun `each check's description is about that check and no other`() {
        // isNotBlank() would pass on eleven copies of the same
        // paragraph, which is exactly the failure mode of a map built by
        // copy-paste. Each entry is a phrase that appears in ONE
        // description and in none of the others.
        val unique =
            mapOf(
                "row_id_tiling" to "tile [0, total) per table",
                "delete_vectors" to "one live deletion vector per data file",
                "orphans" to "outlived its parent",
                "removal_queue" to "never authorized by the queue",
                "snapshot_density" to "dense per catalog",
                "next_row_id" to "allocator can never have handed out a range it does not remember",
                "expiry_floor" to "never passes head",
                "visibility_bounds" to "visible at S iff",
                "offset_release" to "mints a new identity releases the old one's consumers",
                "staging_tickets" to "compaction_staging",
                "upload_claims" to "upload-claim ledger",
            )
        seed("vfy-descriptions")
        val report = verify.runOnce("vfy-descriptions")
        assertThat(report.checks.map { it.check })
            .describedAs("every check needs an entry; a new one without a phrase is the gap")
            .containsExactlyInAnyOrderElementsOf(unique.keys)
        val byName = report.checks.associate { it.check to it.description!! }
        for ((name, phrase) in unique) {
            assertThat(byName.getValue(name))
                .describedAs("%s must state its own invariant", name)
                .contains(phrase)
            assertThat(byName.filterKeys { it != name }.values)
                .describedAs("'%s' must belong to %s alone", phrase, name)
                .noneSatisfy { assertThat(it).contains(phrase) }
        }
    }

    // ---- the table list itself, and sample fairness ------------------------

    @Test
    fun `BELOW_FLOOR_TABLES is every versioned table the schema has, with its id column`() {
        // The parametrized tests above take their cases FROM this list,
        // so they cannot see an entry go missing — deleting one deletes
        // its test case too. This asserts the list against the SCHEMA:
        // every table carrying the versioned-row shape (begin_snapshot +
        // end_snapshot) is covered, nothing else is claimed, and each
        // id column really exists on its table.
        val versioned =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT table_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND column_name = 'end_snapshot'
                    INTERSECT
                    SELECT table_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND column_name = 'begin_snapshot'
                    """,
                ).mapTo(String::class.java).list()
            }
        assertThat(versioned).isNotEmpty()
        assertThat(VerifyService.BELOW_FLOOR_TABLES.map { it.table })
            .describedAs("a versioned table the expiry sweep clears but verify never looks at")
            .containsExactlyInAnyOrderElementsOf(versioned)

        val columns =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT table_name || '.' || column_name FROM information_schema.columns
                    WHERE table_schema = 'public'
                    """,
                ).mapTo(String::class.java).toSet()
            }
        for (v in VerifyService.BELOW_FLOOR_TABLES) {
            assertThat(columns)
                .describedAs("%s has no column %s to name a row by", v.table, v.idColumn)
                .contains("${v.table}.${v.idColumn}")
            assertThat(columns)
                .describedAs("%s has no column %s to scope its rows by", v.table, v.scopeColumn)
                .contains("${v.table}.${v.scopeColumn}")
        }

        // And the sweep's own list is a subset: every table expiry
        // clears below the floor is a table verify checks for survivors.
        assertThat(VerifyService.BELOW_FLOOR_TABLES.map { it.table })
            .containsAll(ExpiryService.VERSIONED_RETENTION_TABLES)
    }

    @Test
    fun `a noisy violation class cannot crowd its siblings out of the samples`() {
        // expiry_floor runs nine sub-queries. Concatenating their
        // samples and taking the first 20 means the loudest class fills
        // the quota and the rest are invisible: you would read
        // "expiry_floor failed, here are twenty lagging consumers" and
        // never learn that rows also survived below the floor. The trim
        // is round-robin for exactly this.
        val catalog = "vfy-sample-fairness"
        val cid = seed(catalog, files = listOf(3L))
        val uuid = catalogs.getTable(catalog, "ns", "t").tableUuid
        val floor = advanceFloor(catalog)
        assertThat(floor).isGreaterThan(1)
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_catalog SET consumer_floor = true WHERE catalog_id = :c")
                .bind("c", cid).execute()
            // Thirty lagging consumers: more than the whole sample cap,
            // all from ONE sub-query.
            repeat(30) { i ->
                h.createUpdate(
                    """
                    INSERT INTO hog_consumer_offset (catalog_id, consumer_id, table_uuid, committed_snapshot)
                    VALUES (:c, :consumer, :uuid, 0)
                    """,
                ).bind("c", cid).bind("consumer", "lagger-%02d".format(i)).bind("uuid", uuid).execute()
            }
        }
        // ...and exactly one survivor, from a different sub-query.
        insertVersionedRow(catalog, "hog_data_file", begin = 0, end = floor)

        val check = verify.runOnce(catalog).check("expiry_floor")
        assertThat(check.violations).isEqualTo(31)
        assertThat(check.samples).hasSize(MAX_SAMPLES)
        assertThat(check.samples)
            .describedAs("the one survivor must survive the trim: %s", check.samples)
            .anySatisfy { assertThat(it).contains("survived the floor advance") }
        assertThat(check.samples.count { it.startsWith("consumer '") })
            .describedAs("the noisy class still gets most of the room, just not all of it")
            .isGreaterThan(10)
    }
}
