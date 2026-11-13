package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.CatalogService.Companion.MAX_DATA_PATH_LENGTH
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant
import java.util.UUID

/** Catalog / namespace / table DDL lifecycle, incl. time travel. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val svc = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val scan = ScanService(db.jdbi)

    private val idCol = ColumnDef("id", ColType.LONG, nullable = false)
    private val nameCol = ColumnDef("name", ColType.STRING)

    @AfterAll
    fun tearDown() = db.close()

    // ---- catalogs --------------------------------------------------------

    @Test
    fun `createCatalog mints snapshot 0 with no changes`() {
        val cat = svc.createCatalog("cat-a", "s3://bucket/a")
        assertThat(cat.headSnapshotId).isEqualTo(0)
        assertThat(cat.schemaVersion).isEqualTo(0)
        assertThat(cat.dataPath).isEqualTo("s3://bucket/a")

        assertThat(svc.getCatalog("cat-a")).isEqualTo(cat)

        val (page, hasMore) = svc.listSnapshots("cat-a", after = -1, limit = 10)
        assertThat(hasMore).isFalse()
        assertThat(page).hasSize(1)
        assertThat(page[0].snapshotId).isEqualTo(0)
        assertThat(page[0].schemaVersion).isEqualTo(0)
        assertThat(page[0].changes).isEmpty()
    }

    @Test
    fun `duplicate catalog name conflicts`() {
        svc.createCatalog("cat-dup", "s3://bucket/d1")
        assertThatThrownBy { svc.createCatalog("cat-dup", "s3://bucket/d2") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
    }

    @Test
    fun `catalog name failing the schema check is a validation error`() {
        assertThatThrownBy { svc.createCatalog("Bad_Name", "s3://bucket/x") }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `unknown catalog is NotFound`() {
        assertThatThrownBy { svc.getCatalog("no-such-catalog") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { svc.listNamespaces("no-such-catalog") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `listCatalogs returns all catalogs ordered by name`() {
        svc.createCatalog("list-b", "s3://bucket/lb")
        svc.createCatalog("list-a", "s3://bucket/la")
        val names = svc.listCatalogs().map { it.name }
        assertThat(names).containsSubsequence("list-a", "list-b")
    }

    // ---- namespaces ------------------------------------------------------

    @Test
    fun `createNamespace advances head and records a typed change`() {
        svc.createCatalog("ns-cat", "s3://bucket/ns")
        val ns = svc.createNamespace("ns-cat", "analytics")
        assertThat(ns.name).isEqualTo("analytics")

        val cat = svc.getCatalog("ns-cat")
        assertThat(cat.headSnapshotId).isEqualTo(1)
        assertThat(cat.schemaVersion).isEqualTo(1)

        val (page, _) = svc.listSnapshots("ns-cat", after = 0, limit = 10)
        assertThat(page).hasSize(1)
        assertThat(page[0].changes).hasSize(1)
        assertThat(page[0].changes[0].kind).isEqualTo(ChangeKind.NAMESPACE_CREATED)
        assertThat(page[0].changes[0].objectId).isEqualTo(ns.namespaceId)

        assertThat(svc.listNamespaces("ns-cat").map { it.name }).containsExactly("analytics")
    }

    @Test
    fun `duplicate namespace conflicts`() {
        svc.createCatalog("ns-dup-cat", "s3://bucket/nd")
        svc.createNamespace("ns-dup-cat", "ns")
        assertThatThrownBy { svc.createNamespace("ns-dup-cat", "ns") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
    }

    // ---- tables ----------------------------------------------------------

    @Test
    fun `createTable assigns 1-based field ids and positional ordinals`() {
        svc.createCatalog("tbl-cat", "s3://bucket/t")
        svc.createNamespace("tbl-cat", "ns")
        val decimalCol =
            ColumnDef(
                "amount",
                ColType.DECIMAL,
                typeParams = mapOf("precision" to 38, "scale" to 9),
                nullable = false,
            )
        val t = svc.createTable("tbl-cat", "ns", "events", listOf(idCol, nameCol, decimalCol))

        assertThat(t.columns).hasSize(3)
        assertThat(t.columns.map { it.fieldId }).containsExactly(1L, 2L, 3L)
        assertThat(t.columns.map { it.ordinal }).containsExactly(0, 1, 2)
        assertThat(t.columns.map { it.def.name }).containsExactly("id", "name", "amount")
        assertThat(t.recordCount).isZero()
        assertThat(t.fileCount).isZero()
        assertThat(t.fileSizeBytes).isZero()

        // Head advanced (catalog=S0, ns=S1, table=S2) with a typed change.
        val cat = svc.getCatalog("tbl-cat")
        assertThat(cat.headSnapshotId).isEqualTo(2)
        val (page, _) = svc.listSnapshots("tbl-cat", after = 1, limit = 10)
        assertThat(page[0].changes).hasSize(1)
        assertThat(page[0].changes[0].kind).isEqualTo(ChangeKind.TABLE_CREATED)
        assertThat(page[0].changes[0].objectId).isEqualTo(t.tableId)

        // Round trip through getTable, incl. jsonb type params.
        val fetched = svc.getTable("tbl-cat", "ns", "events")
        assertThat(fetched.tableUuid).isEqualTo(t.tableUuid)
        assertThat(fetched.columns).isEqualTo(t.columns)
        assertThat(fetched.columns[2].def.typeParams)
            .isEqualTo(mapOf("precision" to 38, "scale" to 9))

        // The field-id allocator advanced past the assigned ids.
        val nextFieldId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT next_field_id FROM hog_table WHERE catalog_id = :cid AND table_id = :tid",
                ).bind("cid", cat.catalogId).bind("tid", t.tableId)
                    .mapTo(Long::class.javaObjectType).one()
            }
        assertThat(nextFieldId).isEqualTo(4)

        // hog_table_stats row exists.
        val statsRows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_table_stats WHERE catalog_id = :cid AND table_id = :tid",
                ).bind("cid", cat.catalogId).bind("tid", t.tableId)
                    .mapTo(Long::class.javaObjectType).one()
            }
        assertThat(statsRows).isEqualTo(1)
    }

    @Test
    fun `duplicate table name in a namespace conflicts, other namespace does not`() {
        svc.createCatalog("tbl-dup-cat", "s3://bucket/td")
        svc.createNamespace("tbl-dup-cat", "ns1")
        svc.createNamespace("tbl-dup-cat", "ns2")
        svc.createTable("tbl-dup-cat", "ns1", "t", listOf(idCol))
        assertThatThrownBy { svc.createTable("tbl-dup-cat", "ns1", "t", listOf(idCol)) }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
        // Same name in a different namespace is fine.
        val other = svc.createTable("tbl-dup-cat", "ns2", "t", listOf(idCol))
        assertThat(other.namespace).isEqualTo("ns2")
    }

    @Test
    fun `createTable input validation`() {
        svc.createCatalog("tbl-val-cat", "s3://bucket/tv")
        svc.createNamespace("tbl-val-cat", "ns")
        assertThatThrownBy { svc.createTable("tbl-val-cat", "ns", "t", emptyList()) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { svc.createTable("tbl-val-cat", "ns", "t", listOf(idCol, idCol)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { svc.createTable("tbl-val-cat", "nope", "t", listOf(idCol)) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `listTables returns live tables name-ordered, with rollups and no columns`() {
        svc.createCatalog("tbl-list-cat", "s3://bucket/tl")
        svc.createNamespace("tbl-list-cat", "ns")
        val bbb = svc.createTable("tbl-list-cat", "ns", "bbb", listOf(idCol))
        val aaa = svc.createTable("tbl-list-cat", "ns", "aaa", listOf(idCol))
        val tables = svc.listTables("tbl-list-cat", "ns")
        assertThat(tables.map { it.name }).containsExactly("aaa", "bbb")
        // The "empty column lists" this used to assert is now a property
        // of the TYPE: a listing row is a TableSummaryInfo, which has no
        // columns field at all, so the assertion could not be written and
        // does not need to be. What replaces it is the rollup the row
        // gained, on a namespace where every number is knowable: two
        // freshly created tables, no appends.
        assertThat(tables).allSatisfy {
            assertThat(it.recordCount).isZero()
            assertThat(it.fileCount).isZero()
            assertThat(it.fileSizeBytes).isZero()
            assertThat(it.comment).isNull()
            // One retained snapshot each: its own table_created.
            assertThat(it.snapshotCount).isEqualTo(1)
        }
        // ...and that snapshot is the create's own, read off the create
        // receipt rather than counted out by hand.
        assertThat(tables.map { it.earliestSnapshotId })
            .containsExactly(aaa.snapshotId, bbb.snapshotId)
    }

    // ---- drop + time travel ----------------------------------------------

    @Test
    fun `dropTable ends the DDL rows, leaves the file rows, and enables time travel`() {
        svc.createCatalog("drop-cat", "s3://bucket/dr")
        svc.createNamespace("drop-cat", "ns") // S1
        val t = svc.createTable("drop-cat", "ns", "t", listOf(idCol, nameCol)) // S2
        val createdAt = svc.getCatalog("drop-cat").headSnapshotId
        val catId = svc.getCatalog("drop-cat").catalogId

        // A live data file, inserted directly (the commit service is
        // another agent's scope).
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file
                    (catalog_id, data_file_id, table_id, begin_snapshot, path,
                     record_count, file_size_bytes, row_id_start)
                VALUES (:cid, 100, :tid, :snap, 's3://bucket/dr/f.parquet', 5, 512, 0)
                """,
            ).bind("cid", catId).bind("tid", t.tableId).bind("snap", createdAt).execute()
        }

        val drop =
            svc.dropTable(
                "drop-cat",
                "ns",
                "t",
                author = "integration-test",
                message = "drop table fixture",
            ) // S3
        assertThat(drop.snapshotId).isEqualTo(createdAt + 1)

        // Head: gone.
        assertThatThrownBy { svc.getTable("drop-cat", "ns", "t") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(svc.listTables("drop-cat", "ns")).isEmpty()
        assertThatThrownBy {
            svc.dropTable(
                "drop-cat",
                "ns",
                "t",
                author = "integration-test",
                message = "drop table fixture",
            )
        }
            .isInstanceOf(HoglakeException.NotFound::class.java)

        // Time travel to the creation snapshot: fully visible.
        val back = svc.getTable("drop-cat", "ns", "t", snapshot = createdAt)
        assertThat(back.tableUuid).isEqualTo(t.tableUuid)
        assertThat(back.columns.map { it.def.name }).containsExactly("id", "name")
        assertThat(back.fileCount).isEqualTo(1)
        assertThat(svc.listFiles("drop-cat", "ns", "t", snapshot = createdAt))
            .hasSize(1)

        // Before creation: not visible.
        assertThatThrownBy { svc.getTable("drop-cat", "ns", "t", snapshot = createdAt - 1) }
            .isInstanceOf(HoglakeException.NotFound::class.java)

        // The drop marked the identity row and end-snapshotted the DDL
        // rows — and DELIBERATELY LEFT THE FILE ROW ALONE.
        //
        // `hog_data_file` used to be in the list below. It is not any
        // more, and the row it names is asserted to be STILL LIVE a few
        // lines down, because that is the change #193 made: a drop is
        // three O(columns) UPDATEs, the table is what says its rows are
        // unreachable, and the rows themselves leave through the paced
        // retirement sweep once the drop snapshot sinks under the
        // catalog's expiry floor. Ending them here was 44.6 s and
        // 3.9 GB of WAL at 3,008,849 rows, under the commit lock, on a
        // drop that then rolled back on the statement timeout.
        db.jdbi.withHandleUnchecked { h ->
            val droppedSnapshot =
                h.createQuery(
                    "SELECT dropped_snapshot FROM hog_table WHERE catalog_id = :cid AND table_id = :tid",
                ).bind("cid", catId).bind("tid", t.tableId)
                    .mapTo(Long::class.javaObjectType).one()
            assertThat(droppedSnapshot).isEqualTo(drop.snapshotId)

            // MUTATION: put `FileRepo.endLiveFiles` back into
            // CatalogService.dropTable and this reds — the row is live,
            // and it must stay live until retirement.
            val liveFiles =
                h.createQuery(
                    """
                    SELECT count(*) FROM hog_data_file
                    WHERE catalog_id = :cid AND table_id = :tid AND end_snapshot IS NULL
                    """,
                ).bind("cid", catId).bind("tid", t.tableId)
                    .mapTo(Long::class.javaObjectType).one()
            assertThat(liveFiles)
                .describedAs("the drop must not touch file rows; retirement deletes them later")
                .isEqualTo(1)

            for (table in listOf("hog_table_version", "hog_column")) {
                val liveRows =
                    h.createQuery(
                        """
                    SELECT count(*) FROM $table
                    WHERE catalog_id = :cid AND table_id = :tid AND end_snapshot IS NULL
                    """,
                    ).bind("cid", catId).bind("tid", t.tableId)
                        .mapTo(Long::class.javaObjectType).one()
                assertThat(liveRows).describedAs("live rows left in %s", table).isZero()

                val endedAtDrop =
                    h.createQuery(
                        """
                    SELECT count(*) FROM $table
                    WHERE catalog_id = :cid AND table_id = :tid AND end_snapshot = :snap
                    """,
                    ).bind("cid", catId).bind("tid", t.tableId).bind("snap", drop.snapshotId)
                        .mapTo(Long::class.javaObjectType).one()
                assertThat(endedAtDrop).describedAs("rows ended at drop in %s", table)
                    .isGreaterThan(0)
            }
        }

        // The drop snapshot carries the typed change.
        val (page, _) = svc.listSnapshots("drop-cat", after = drop.snapshotId - 1, limit = 1)
        assertThat(page[0].changes[0].kind).isEqualTo(ChangeKind.TABLE_DROPPED)
        assertThat(page[0].changes[0].objectId).isEqualTo(t.tableId)
    }

    @Test
    fun `recreating a dropped table mints a new identity`() {
        svc.createCatalog("recreate-cat", "s3://bucket/rc")
        svc.createNamespace("recreate-cat", "ns")
        val v1 = svc.createTable("recreate-cat", "ns", "t", listOf(idCol))
        val preDrop = svc.getCatalog("recreate-cat").headSnapshotId
        svc.dropTable("recreate-cat", "ns", "t", author = "integration-test", message = "drop table fixture")
        val v2 = svc.createTable("recreate-cat", "ns", "t", listOf(idCol, nameCol))

        assertThat(v2.tableId).isNotEqualTo(v1.tableId)
        assertThat(v2.tableUuid).isNotEqualTo(v1.tableUuid)

        // Head sees the new incarnation; time travel sees the old one.
        assertThat(svc.getTable("recreate-cat", "ns", "t").tableUuid).isEqualTo(v2.tableUuid)
        assertThat(svc.getTable("recreate-cat", "ns", "t", snapshot = preDrop).tableUuid)
            .isEqualTo(v1.tableUuid)
    }

    // A pinned getTable resolves the namespace incarnation at its snapshot, as a pinned listTables does.

    @Test
    fun `a pinned getTable resolves the namespace incarnation at its snapshot`() {
        val catalog = "pinned-ns-recreate-cat"
        svc.createCatalog(catalog, "s3://bucket/pnr")
        val first = svc.createNamespace(catalog, "ns")
        val table = svc.createTable(catalog, "ns", "t", listOf(idCol))
        val pinned = table.snapshotId!!
        assertThat(svc.getTable(catalog, "ns", "t", snapshot = pinned).tableUuid).isEqualTo(table.tableUuid)

        // A namespace drops only when empty; the recreate is a new incarnation.
        svc.dropTable(catalog, "ns", "t", expectedTableUuid = table.tableUuid, author = "test", message = "drop")
        val dropped = svc.dropNamespace(catalog, "ns", author = "test", message = "drop").snapshotId
        val second = svc.createNamespace(catalog, "ns")
        assertThat(second.namespaceId).isNotEqualTo(first.namespaceId)
        val t2 = svc.createTable(catalog, "ns", "t2", listOf(idCol))
        assertThat(svc.getTable(catalog, "ns", "t2").tableUuid).isEqualTo(t2.tableUuid)

        val read = svc.getTable(catalog, "ns", "t", snapshot = pinned)
        assertThat(read.tableUuid).isEqualTo(table.tableUuid)
        assertThat(read.readSnapshotId).isEqualTo(pinned)
        assertThat(svc.listTables(catalog, "ns", snapshot = pinned).map { it.name }).containsExactly("t")

        // At the drop snapshot no incarnation is alive: the old one is dropped, the new one not yet created.
        assertThatThrownBy { svc.getTable(catalog, "ns", "t", snapshot = dropped) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessageContaining("namespace 'ns' in catalog '$catalog' at snapshot $dropped")
    }

    @Test
    fun `a pinned getTable picks the incarnation at its snapshot when both hold the same table name`() {
        val catalog = "pinned-ns-same-name-cat"
        svc.createCatalog(catalog, "s3://bucket/pns")
        svc.createNamespace(catalog, "ns")
        val first = svc.createTable(catalog, "ns", "t", listOf(idCol))
        val pinned = first.snapshotId!!

        svc.dropTable(catalog, "ns", "t", expectedTableUuid = first.tableUuid, author = "test", message = "drop")
        svc.dropNamespace(catalog, "ns", author = "test", message = "drop")
        svc.createNamespace(catalog, "ns")
        val recreated = svc.getCatalog(catalog).headSnapshotId
        val second = svc.createTable(catalog, "ns", "t", listOf(idCol))
        assertThat(second.tableUuid).isNotEqualTo(first.tableUuid)

        assertThat(svc.getTable(catalog, "ns", "t").tableUuid).isEqualTo(second.tableUuid)
        val read = svc.getTable(catalog, "ns", "t", snapshot = pinned)
        assertThat(read.tableUuid).isEqualTo(first.tableUuid)
        assertThat(read.readSnapshotId).isEqualTo(pinned)
        assertThat(svc.listTables(catalog, "ns", snapshot = pinned).map { it.name }).containsExactly("t")

        // The new incarnation is alive at its creation snapshot, but its "t" comes later: the table is missing.
        assertThatThrownBy { svc.getTable(catalog, "ns", "t", snapshot = recreated) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessageContaining("table 'ns.t' in catalog '$catalog' at snapshot $recreated")
    }

    @Test
    fun `a pinned getTable finds a table whose namespace was dropped and never recreated`() {
        val catalog = "pinned-ns-dropped-cat"
        svc.createCatalog(catalog, "s3://bucket/pnd")
        svc.createNamespace(catalog, "ns")
        val table = svc.createTable(catalog, "ns", "t", listOf(idCol))
        val pinned = table.snapshotId!!
        svc.dropTable(catalog, "ns", "t", expectedTableUuid = table.tableUuid, author = "test", message = "drop")
        svc.dropNamespace(catalog, "ns", author = "test", message = "drop")

        // Unlike a live lookup, which is NotFound, a pinned read sees the table as it was at its snapshot.
        assertThatThrownBy { svc.getTable(catalog, "ns", "t") }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(svc.getTable(catalog, "ns", "t", snapshot = pinned).tableUuid).isEqualTo(table.tableUuid)
    }

    @Test
    fun `a pinned getTable before the namespace existed is not found at that snapshot`() {
        val catalog = "pinned-ns-before-cat"
        svc.createCatalog(catalog, "s3://bucket/pnb")
        val before = svc.getCatalog(catalog).headSnapshotId
        svc.createNamespace(catalog, "ns")
        svc.createTable(catalog, "ns", "t", listOf(idCol))

        assertThatThrownBy { svc.getTable(catalog, "ns", "t", snapshot = before) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessageContaining("namespace 'ns' in catalog '$catalog' at snapshot $before")
    }

    // Every pinned read resolves the namespace incarnation at its snapshot: the
    // file reads, the changefeed and scan planning agree with getTable/listTables.

    /** ns.t holds one file, then t and ns are dropped; with [recreated], ns comes back with a new, empty t. */
    private class PinnedHistory(
        val catalog: String,
        val tableUuid: UUID,
        val fileId: Long,
        /** The head before ns existed. */
        val before: Long,
        /** The snapshot that landed the file. */
        val pinned: Long,
        /** [pinned]'s snapshot time; every later snapshot is strictly after it. */
        val pinnedAt: Instant,
        /** The snapshot that dropped ns. */
        val dropped: Long,
        /** The snapshot that recreated ns, before its t exists. */
        val recreated: Long?,
    )

    private fun pinnedHistory(
        catalog: String,
        recreate: Boolean,
    ): PinnedHistory {
        svc.createCatalog(catalog, "s3://bucket/$catalog")
        val before = svc.getCatalog(catalog).headSnapshotId
        svc.createNamespace(catalog, "ns")
        val table = svc.createTable(catalog, "ns", "t", listOf(idCol))
        val file = FileRegistration("s3://bucket/$catalog/f1.parquet", 10, 1000)
        val pinned =
            commits.commit(catalog, CommitRequest(appends = listOf(TableAppend("ns", "t", listOf(file))))).snapshotId
        val fileId = svc.listFiles(catalog, "ns", "t").single().dataFileId
        val pinnedAt =
            svc.listSnapshots(catalog, after = 0, limit = 100).first.first { it.snapshotId == pinned }.snapshotTime
        // The drop must land strictly after the pin's time, so at_timestamp = pinnedAt resolves to the pin.
        while (!Instant.now().isAfter(pinnedAt.plusMillis(1))) Thread.sleep(1)
        svc.dropTable(catalog, "ns", "t", expectedTableUuid = table.tableUuid, author = "test", message = "drop")
        val dropped = svc.dropNamespace(catalog, "ns", author = "test", message = "drop").snapshotId
        var recreated: Long? = null
        if (recreate) {
            svc.createNamespace(catalog, "ns")
            recreated = svc.getCatalog(catalog).headSnapshotId
            svc.createTable(catalog, "ns", "t", listOf(idCol))
        }
        return PinnedHistory(catalog, table.tableUuid, fileId, before, pinned, pinnedAt, dropped, recreated)
    }

    /** The reads that take `at_timestamp`, by name, each pinned at [at]. */
    private fun timestampReads(
        h: PinnedHistory,
        at: Instant,
    ): Map<String, () -> Any> =
        mapOf(
            "getTable" to { svc.getTable(h.catalog, "ns", "t", atTimestamp = at) },
            "listTables" to { svc.listTables(h.catalog, "ns", atTimestamp = at) },
            "listFiles" to { svc.listFiles(h.catalog, "ns", "t", atTimestamp = at) },
            "partitionValues" to { svc.partitionValues(h.catalog, "ns", "t", atTimestamp = at) },
            "fileStats" to { svc.fileStats(h.catalog, "ns", "t", h.fileId, atTimestamp = at) },
            "planScan" to { scan.planScan(h.catalog, "ns", "t", atTimestamp = at) },
        )

    /** The table-level reads that take a pin, by name, each pinned at [at] (null: a head read). */
    private fun tableReads(
        h: PinnedHistory,
        at: Long?,
    ): Map<String, () -> Any> =
        mapOf(
            "getTable" to { svc.getTable(h.catalog, "ns", "t", snapshot = at) },
            "listFiles" to { svc.listFiles(h.catalog, "ns", "t", snapshot = at) },
            "partitionValues" to { svc.partitionValues(h.catalog, "ns", "t", snapshot = at) },
            "fileStats" to { svc.fileStats(h.catalog, "ns", "t", h.fileId, snapshot = at) },
            "planScan" to { scan.planScan(h.catalog, "ns", "t", snapshot = at) },
            "changes" to { svc.changes(h.catalog, "ns", "t", fromSnapshot = 0, toSnapshot = at) },
        )

    private fun assertReadsNotFound(
        reads: Map<String, () -> Any>,
        message: String,
    ) {
        for ((name, read) in reads) {
            assertThatThrownBy { read() }
                .describedAs(name)
                .isInstanceOf(HoglakeException.NotFound::class.java)
                .hasMessageContaining(message)
        }
    }

    /** At the pin, every path serves the first incarnation's t and its one file. */
    private fun assertPinnedReadsServeTheFile(h: PinnedHistory) {
        assertThat(svc.getTable(h.catalog, "ns", "t", snapshot = h.pinned).tableUuid).isEqualTo(h.tableUuid)
        assertThat(svc.listTables(h.catalog, "ns", snapshot = h.pinned).map { it.tableUuid })
            .containsExactly(h.tableUuid)
        assertThat(svc.listFiles(h.catalog, "ns", "t", snapshot = h.pinned).map { it.dataFileId })
            .containsExactly(h.fileId)
        assertThat(svc.partitionValues(h.catalog, "ns", "t", snapshot = h.pinned).fields).isEmpty()
        assertThat(svc.fileStats(h.catalog, "ns", "t", h.fileId, snapshot = h.pinned).dataFileId).isEqualTo(h.fileId)
        assertThat(scan.planScan(h.catalog, "ns", "t", snapshot = h.pinned).map { it.dataFile.dataFileId })
            .containsExactly(h.fileId)
        val feed = svc.changes(h.catalog, "ns", "t", fromSnapshot = 0, toSnapshot = h.pinned)
        assertThat(feed.tableUuid).isEqualTo(h.tableUuid)
        assertThat(feed.files.map { it.dataFileId }).containsExactly(h.fileId)
    }

    @Test
    fun `pinned file reads, changefeed and scan planning resolve the namespace incarnation at their snapshot`() {
        val h = pinnedHistory("pinned-ns-reads-cat", recreate = true)
        assertPinnedReadsServeTheFile(h)

        // Head reads see the new incarnation: a t with no files, so the pinned file is not there.
        assertThat(svc.getTable(h.catalog, "ns", "t").tableUuid).isNotEqualTo(h.tableUuid)
        assertThat(svc.listFiles(h.catalog, "ns", "t")).isEmpty()
        assertThat(scan.planScan(h.catalog, "ns", "t")).isEmpty()
        assertThatThrownBy { svc.fileStats(h.catalog, "ns", "t", h.fileId) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessageContaining("data file ${h.fileId}")
    }

    @Test
    fun `an at_timestamp pin resolves the namespace incarnation at its snapshot, as the snapshot pin does`() {
        val h = pinnedHistory("pinned-ns-reads-ts-cat", recreate = true)
        assertThat(svc.resolveSnapshot(h.catalog, h.pinnedAt).snapshotId).isEqualTo(h.pinned)
        assertThat(svc.getTable(h.catalog, "ns", "t", atTimestamp = h.pinnedAt).tableUuid).isEqualTo(h.tableUuid)
        val bySnapshot =
            tableReads(h, h.pinned) + ("listTables" to { svc.listTables(h.catalog, "ns", snapshot = h.pinned) })
        for ((name, read) in timestampReads(h, h.pinnedAt)) {
            assertThat(read()).describedAs(name).isEqualTo(bySnapshot.getValue(name)())
        }
    }

    @Test
    fun `pinned file reads at the drop snapshot and before the namespace existed are NotFound naming the snapshot`() {
        val h = pinnedHistory("pinned-ns-reads-gap-cat", recreate = true)
        // At the drop snapshot no incarnation is alive; before `before` none existed yet.
        for (at in listOf(h.dropped, h.before)) {
            val message = "namespace 'ns' in catalog '${h.catalog}' at snapshot $at"
            assertReadsNotFound(tableReads(h, at), message)
            assertReadsNotFound(mapOf("listTables" to { svc.listTables(h.catalog, "ns", snapshot = at) }), message)
        }
    }

    @Test
    fun `pinned file reads at the recreate snapshot see the new incarnation, which has no t yet`() {
        val h = pinnedHistory("pinned-ns-reads-recreate-cat", recreate = true)
        val at = h.recreated!!
        assertThat(svc.listTables(h.catalog, "ns", snapshot = at)).isEmpty()
        assertReadsNotFound(tableReads(h, at), "table 'ns.t' in catalog '${h.catalog}' at snapshot $at")
    }

    @Test
    fun `pinned file reads find a table whose namespace was dropped and never recreated`() {
        val h = pinnedHistory("pinned-ns-reads-gone-cat", recreate = false)
        // Live, the namespace is gone ...
        assertReadsNotFound(tableReads(h, at = null), "namespace 'ns' in catalog '${h.catalog}'")
        // ... but a pinned reader still sees it as it was.
        assertPinnedReadsServeTheFile(h)
    }

    @Test
    fun `snapshot parameter out of range is a validation error`() {
        svc.createCatalog("range-cat", "s3://bucket/rg")
        svc.createNamespace("range-cat", "ns")
        svc.createTable("range-cat", "ns", "t", listOf(idCol))
        val head = svc.getCatalog("range-cat").headSnapshotId
        assertThatThrownBy { svc.getTable("range-cat", "ns", "t", snapshot = head + 1) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { svc.getTable("range-cat", "ns", "t", snapshot = -1) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    // ---- data_path validation (QE review 2026-09-11) ---------------------

    @Test
    fun `degenerate and hostile data_paths are 422 - the commit path guard depends on this`() {
        // Each would defeat or poison downstream machinery: "s3://"
        // normalizes to a prefix every s3 URI matches; bucket-only admits
        // sibling catalogs; non-s3 schemes poison the removal queue;
        // whitespace/dot segments survive into reader URIs.
        for (bad in listOf(
            "s3://",
            "s3:///prefix",
            "gs://bucket/prefix",
            "/var/data",
            "s3://bucket/pre fix",
            "s3://bucket/a/../b",
            "s3://bucket/a	b",
        )) {
            assertThatThrownBy { svc.createCatalog("dp-bad", bad) }
                .`as`("data_path %s", bad)
                .isInstanceOf(HoglakeException.Validation::class.java)
        }
    }

    @Test
    fun `a data_path is length-bounded, and its refusals do not echo one whole`() {
        // Nothing bounded data_path: `text` with no CHECK, a shape-only
        // validator, a bare string in the schema. So a 100 KB path
        // registered fine — and then rode into every overlap 422 any
        // later catalog triggered, because the message quotes BOTH
        // sides. Capping the echo alone would have left the value in the
        // table and in the audit log.
        val huge = "s3://ovl-huge/" + "p".repeat(100_000)
        assertThatThrownBy { svc.createCatalog("dp-huge", huge) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("over the maximum")
        // The refusal itself does not quote the 100 KB back.
        val thrown = catchThrowable { svc.createCatalog("dp-huge", huge) }
        assertThat(thrown.message!!.length).describedAs("length refusal").isLessThan(200)

        // Both sides of the OVERLAP message are capped too: the incoming
        // path and the stored one it collides with.
        // Just under the bound, so the overlap check is what fires.
        val long = "s3://ovl-long/" + "q".repeat(MAX_DATA_PATH_LENGTH - 30)
        svc.createCatalog("dp-long", long)
        val overlap = catchThrowable { svc.createCatalog("dp-long-2", "$long/deeper") }
        assertThat(overlap).isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(overlap.message!!)
            .describedAs("names the catalog it collided with")
            .contains("overlaps catalog 'dp-long'")
        assertThat(overlap.message!!.length)
            .describedAs("overlap refusal quotes neither path whole")
            .isLessThan(300)
    }

    @Test
    fun `overlapping data_paths are refused in both directions`() {
        svc.createCatalog("dp-base", "s3://ovl/base")
        // Equal, nested-under, and parent-of are all refused; the
        // trailing-slash form is the same prefix.
        for (bad in listOf("s3://ovl/base", "s3://ovl/base/", "s3://ovl/base/deeper")) {
            assertThatThrownBy { svc.createCatalog("dp-clash", bad) }
                .`as`("overlap %s", bad)
                .isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("overlaps catalog 'dp-base'")
        }
        // Bucket-root parent overlaps everything in the bucket.
        assertThatThrownBy { svc.createCatalog("dp-parent", "s3://ovl") }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("overlaps")
        // Sibling prefixes stay legal, including the sneaky
        // shares-a-string-prefix sibling.
        svc.createCatalog("dp-sib", "s3://ovl/base2")
        // Bucket-root data_path is legal on its own bucket — the fleet
        // convention (megaduck-shaped catalogs own a bucket).
        svc.createCatalog("dp-root", "s3://own-bucket")
        svc.createCatalog("dp-root-slash", "s3://own-bucket-2/")
    }
}
