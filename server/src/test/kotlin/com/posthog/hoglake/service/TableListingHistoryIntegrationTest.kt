package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableSummaryInfo
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * The two listing numbers that are about a table's HISTORY rather than
 * its state — `snapshot_count` and `earliest_snapshot_id` — driven
 * through `CatalogService.listTables` and the real `ExpiryService`.
 *
 * Both reasons this class exists were mutations that the whole 1,599-
 * test suite waved through:
 *
 *  - passing `0L` instead of `cat.earliestSnapshotId` for the floor.
 *    Nothing else in the suite ever moves a catalog's expiry floor AND
 *    then lists its tables, so every other test's floor is 0 and the
 *    bound is a no-op. The fix is a test that expires snapshots for
 *    real and watches both numbers move, through the SERVICE — reading
 *    the repo directly would have asserted the query while leaving the
 *    service's own call unpinned, which is exactly where the bug was.
 *  - selecting version rows by `end_snapshot IS NULL` ("live now")
 *    while every other number answered for the catalog head. Modelling
 *    that needs a head other than the current one, and since the
 *    statement reads its own bounds out of `hog_catalog`, the way to
 *    supply one is to MOVE that column — which is the same seam
 *    production uses, not a test-only parameter.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TableListingHistoryIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val expiry = ExpiryService(db.jdbi)
    private val counter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    private fun fixture(): String {
        val cat = "hist-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        return cat
    }

    private fun append(
        cat: String,
        path: String,
    ) = commits.commit(
        cat,
        CommitRequest(appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(path, 10, 100))))),
    )

    /** Age every snapshot past [retentionSeconds] and enable expiry. */
    private fun ageAndEnableRetention(
        cat: String,
        retentionSeconds: Long = 3600,
    ) = db.jdbi.useHandleUnchecked { h ->
        val id = catalogs.getCatalog(cat).catalogId
        h.execute(
            "UPDATE hog_snapshot SET snapshot_time = now() - interval '2 days' WHERE catalog_id = ?",
            id,
        )
        h.execute(
            "UPDATE hog_catalog SET snapshot_retention_seconds = ? WHERE catalog_id = ?",
            retentionSeconds,
            id,
        )
    }

    private fun listed(cat: String) = catalogs.listTables(cat, "ns").single()

    // ---- B2: the floor the service passes --------------------------------

    @Test
    fun `expiry moves snapshot_count and earliest_snapshot_id with the catalog floor`() {
        val cat = fixture() // S1 namespace, S2 table_created
        val a = append(cat, "s3://bucket/$cat/a.parquet") // S3
        val b = append(cat, "s3://bucket/$cat/b.parquet") // S4
        val c = append(cat, "s3://bucket/$cat/c.parquet") // S5

        // Before expiry: every commit that named the table is retained.
        val before = listed(cat)
        assertThat(before.snapshotCount).isEqualTo(4)
        assertThat(before.earliestSnapshotId)
            .describedAs("the table_created snapshot is the oldest readable point")
            .isEqualTo(a.snapshotId - 1)

        // Expire everything the retention window lets go. The floor lands
        // at head, so only head's change row survives.
        ageAndEnableRetention(cat)
        val result = expiry.runOnce(cat, batchSize = 1000)
        assertThat(result.snapshotsExpired).isGreaterThan(0)
        val floor = catalogs.getCatalog(cat).earliestSnapshotId
        assertThat(floor).isEqualTo(c.snapshotId)

        val after = listed(cat)
        assertThat(after.snapshotCount)
            .describedAs("retained snapshots only — the count SHRINKS as the floor advances")
            .isEqualTo(1)
        assertThat(after.earliestSnapshotId)
            .describedAs("the oldest point the table can still be read at is the floor")
            .isEqualTo(floor)
        // The rollup is unaffected: expiry retires snapshots, not files.
        assertThat(after.fileCount).isEqualTo(3)
        assertThat(after.recordCount).isEqualTo(30)
        // And b's snapshot, which was readable a moment ago, is not
        // counted any more — the whole point of the floor bound.
        assertThat(after.earliestSnapshotId!!).isGreaterThan(b.snapshotId)
    }

    @Test
    fun `a partial floor advance retains exactly the snapshots above it`() {
        // The test above drives the floor to head, where "count the rows
        // at or above the floor" and "count the head row" coincide. This
        // one leaves the floor in the MIDDLE, so the two answers differ
        // and only the floor-bounded one is right.
        val cat = fixture() // S1 ns, S2 created
        append(cat, "s3://bucket/$cat/a.parquet") // S3
        val b = append(cat, "s3://bucket/$cat/b.parquet") // S4
        val c = append(cat, "s3://bucket/$cat/c.parquet") // S5

        // Age only the snapshots strictly below b, so retention can
        // release those and nothing else.
        db.jdbi.useHandleUnchecked { h ->
            val id = catalogs.getCatalog(cat).catalogId
            h.execute(
                "UPDATE hog_snapshot SET snapshot_time = now() - interval '2 days' " +
                    "WHERE catalog_id = ? AND snapshot_id < ?",
                id,
                b.snapshotId,
            )
            h.execute("UPDATE hog_catalog SET snapshot_retention_seconds = 3600 WHERE catalog_id = ?", id)
        }
        expiry.runOnce(cat, batchSize = 1000)

        val floor = catalogs.getCatalog(cat).earliestSnapshotId
        assertThat(floor).isEqualTo(b.snapshotId)
        val after = listed(cat)
        // b and c: two retained snapshots naming the table, not four and
        // not one.
        assertThat(after.snapshotCount).isEqualTo(2)
        assertThat(after.earliestSnapshotId).isEqualTo(b.snapshotId)
        assertThat(c.snapshotId).isGreaterThan(b.snapshotId)
    }

    @Test
    fun `the floor bound holds even when change rows survive below the floor`() {
        // WHY THIS IS SYNTHETIC, and why it is still the right test.
        //
        // The two tests above assert that the numbers move with the
        // floor, and they do — but they pass with the floor argument
        // replaced by a literal 0. That is not a hole in them; it is a
        // fact about ExpiryService: its sweep advances
        // earliest_snapshot_id and DELETES the snapshots below it in one
        // transaction, and hog_snapshot_change cascades from
        // hog_snapshot. After a sweep there is nothing below the floor
        // left to exclude, so `snapshot_id >= floor` and
        // `snapshot_id >= 0` agree on every catalog the sweep produced.
        //
        // The bound is therefore DEFENCE, and the only way to test
        // defence is to build the state it defends against: a floor that
        // sits above snapshots whose rows are still present. Nothing in
        // the server produces that today — it is what a floor advanced
        // ahead of its cascade looks like, which is a repair, a restore
        // from a partial backup, or a future sweep that moves the floor
        // in one transaction and reclaims in another. Invariant 5 says
        // those snapshots are gone; the listing must agree with the
        // invariant rather than with whatever rows happen to remain,
        // because reporting them would offer a consumer a
        // time-travel point that answers 410.
        val cat = fixture()
        append(cat, "s3://bucket/$cat/a.parquet")
        val b = append(cat, "s3://bucket/$cat/b.parquet")
        append(cat, "s3://bucket/$cat/c.parquet")

        val withoutFloor = listed(cat)
        assertThat(withoutFloor.snapshotCount).isEqualTo(4)

        // Advance ONLY the floor. Every snapshot and change row stays.
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_catalog SET earliest_snapshot_id = ? WHERE catalog_id = ?",
                b.snapshotId,
                catalogs.getCatalog(cat).catalogId,
            )
        }
        val rowsBelowFloor =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_snapshot_change WHERE catalog_id = :c AND snapshot_id < :f",
                ).bind("c", catalogs.getCatalog(cat).catalogId).bind("f", b.snapshotId)
                    .mapTo(Long::class.java).one()
            }
        assertThat(rowsBelowFloor)
            .describedAs("the fixture must actually leave rows below the floor, or this proves nothing")
            .isGreaterThan(0)

        val withFloor = listed(cat)
        assertThat(withFloor.snapshotCount)
            .describedAs(
                "%d change rows still sit below the floor; the listing must count only the " +
                    "retained ones",
                rowsBelowFloor,
            )
            .isEqualTo(2)
        assertThat(withFloor.earliestSnapshotId).isEqualTo(b.snapshotId)
    }

    // ---- S1: the version row is resolved AT the snapshot ------------------

    /**
     * The listing as it would read with the catalog's head at [snapshot].
     *
     * The head is moved in `hog_catalog` and put back, rather than
     * passed as an argument: the statement reads its own bounds from
     * that row, so this is the only seam there is — and it is the same
     * column production reads, which is the point. Restores on the way
     * out so the catalog stays usable.
     */
    private fun listedAtHead(
        cat: String,
        snapshot: Long,
    ): List<TableSummaryInfo> {
        val id = catalogs.getCatalog(cat).catalogId
        val real = catalogs.getCatalog(cat).headSnapshotId
        return try {
            db.jdbi.useHandleUnchecked { h ->
                h.execute("UPDATE hog_catalog SET last_snapshot_id = ? WHERE catalog_id = ?", snapshot, id)
            }
            catalogs.listTables(cat, "ns")
        } finally {
            db.jdbi.useHandleUnchecked { h ->
                h.execute("UPDATE hog_catalog SET last_snapshot_id = ? WHERE catalog_id = ?", real, id)
            }
        }
    }

    @Test
    fun `a table created after the read snapshot is absent, not described as fully expired`() {
        val cat = fixture()
        val headWhenRead = catalogs.getCatalog(cat).headSnapshotId

        // The race: a table lands between the caller's head read and the
        // listing statement.
        catalogs.createTable(cat, "ns", "latecomer", listOf(ColumnDef("id", ColType.LONG)))

        val atOldHead = listedAtHead(cat, headWhenRead)
        assertThat(atOldHead.map { it.name })
            .describedAs(
                "a table whose creating snapshot is above the read snapshot did not exist there; " +
                    "listing it with snapshot_count 0 and a null earliest_snapshot_id would " +
                    "describe it as a table whose every commit has expired — the opposite fact, " +
                    "and indistinguishable from it on the wire",
            )
            .containsExactly("t")

        // At the new head it is there, and correctly described.
        val atNewHead = catalogs.listTables(cat, "ns")
        assertThat(atNewHead.map { it.name }).containsExactly("latecomer", "t")
        val late = atNewHead.single { it.name == "latecomer" }
        assertThat(late.snapshotCount).isEqualTo(1)
        assertThat(late.earliestSnapshotId).isNotNull()
    }

    @Test
    fun `a dropped table is absent at head and present at a snapshot before the drop`() {
        val cat = fixture()
        val beforeDrop = catalogs.getCatalog(cat).headSnapshotId
        catalogs.dropTable(cat, "ns", "t")

        assertThat(catalogs.listTables(cat, "ns"))
            .describedAs("a dropped table is not live at head")
            .isEmpty()
        // ...and the version row is still resolvable below the drop, which
        // `end_snapshot IS NULL` could not express.
        assertThat(listedAtHead(cat, beforeDrop).map { it.name }).containsExactly("t")
    }
}
