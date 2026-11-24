package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.Transform
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * Head reads of the hoglite adapter while commits land, against the live
 * engine (#537).
 *
 * `table_totals` and the head (`meta.next_snapshot`) are the only state
 * that no begin/end snapshot pins: both are exact at head only. A head
 * `getTable`/`listTables` that reads the head in one engine snapshot and
 * `table_totals` in a later one sees totals of a snapshot it does not
 * return. The aggregate over `files` that repairs this is O(files of a
 * table) per table and crosses the engine's 5 s query timeout on a large
 * table, at a race that ~1.7 commits/s makes common.
 *
 * Each test lands a commit through [HogliteInterposer] just before the
 * operation's `table_totals` read reaches the engine. The read must then
 * still come from one snapshot with the head it returns, with no
 * aggregate over `files`.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteHeadReadRaceIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine
    private lateinit var proxy: HogliteInterposer

    /** Straight to the engine: the hooks commit through it, and the pinned references read through it. */
    private val store get() = engine.store

    /** Through the interposer. */
    private val pstore get() = proxy.store

    @BeforeAll
    fun start() {
        engine = HogliteConformanceEngine()
        proxy = HogliteInterposer(engine.url())
    }

    @AfterAll
    fun stop() {
        if (::proxy.isInitialized) proxy.close()
        if (::engine.isInitialized) engine.close()
    }

    // ---- helpers ------------------------------------------------------

    private fun setup(vararg tables: String): Pair<String, List<TableInfo>> {
        val c = HogliteConformanceEngine.uniqueName("headread")
        store.createCatalog(c, "s3://hoglake-it/$c", null, null)
        store.createNamespace(c, "ns", null, null)
        return c to tables.map { store.createTable(c, "ns", it, listOf(ColumnDef("id", ColType.LONG)), null, null) }
    }

    private fun append(
        c: String,
        table: String,
        path: String,
        rows: Long,
    ) = store.commit(
        c,
        CommitRequest(
            idempotencyKey = UUID.randomUUID(),
            appends =
                listOf(
                    TableAppend(
                        "ns",
                        table,
                        listOf(FileRegistration("s3://hoglake-it/$c/data/$path", rows, rows * 10, 50)),
                    ),
                ),
        ),
        null,
    )

    /** Before the first read whose body names `table_totals`, run [action]. */
    private fun beforeTotalsRead(action: () -> Unit) =
        proxy.hook({ m, _, b -> m == "POST" && "table_totals" in b }, action)

    /**
     * The request bodies since [from] that aggregate the manifest per
     * table: the pinned getTable's and the pinned listing's
     * `SUM(r.record_count)`, and the alter receipt's `FROM files WHERE
     * table_id = ?`.
     */
    private fun aggregateReads(from: Int) =
        proxy.requests.drop(from).filter { "FROM files WHERE table_id = ?" in it || "SUM(r.record_count)" in it }

    private fun TableInfo.totals() = listOf(recordCount, fileCount, fileSizeBytes)

    // ---- getTable -----------------------------------------------------

    @Test
    fun `a commit before the head totals read never forces the getTable aggregate`() {
        val (c, _) = setup("t")
        append(c, "t", "a.parquet", 10)
        var landed = -1L
        val hook = beforeTotalsRead { landed = append(c, "t", "b.parquet", 7).snapshotId }
        val from = proxy.requests.size

        val info = pstore.getTable(c, "ns", "t", null, null, totals = true)

        assertThat(hook.fired).isTrue()
        assertThat(landed).isPositive()
        // The totals read came from the snapshot the head was read in, so
        // nothing had to re-derive them from the files.
        assertThat(aggregateReads(from)).isEmpty()
        val pinned = store.getTable(c, "ns", "t", info.readSnapshotId, null, totals = true)
        assertThat(info.totals()).isEqualTo(pinned.totals())
    }

    // ---- listTables ---------------------------------------------------

    @Test
    fun `a truncate before the head listing never forces the listTables aggregate`() {
        val (c, tables) = setup("a", "b")
        append(c, "a", "a.parquet", 10)
        append(c, "b", "b.parquet", 20)
        val before = store.getCatalog(c).headSnapshotId
        var landed = -1L
        val hook =
            beforeTotalsRead {
                landed = store.truncateTable(c, "ns", "a", tables[0].tableUuid, "test", "race").snapshotId
            }
        val from = proxy.requests.size

        val listing = pstore.listTables(c, "ns", null, null)

        assertThat(hook.fired).isTrue()
        assertThat(landed).isGreaterThan(before)
        assertThat(aggregateReads(from)).isEmpty()
        // A listing returns no snapshot id, so it must equal the pinned
        // listing at one of the two heads: never a mix of both.
        assertThat(listing).isIn(store.listTables(c, "ns", before, null), store.listTables(c, "ns", landed, null))
    }

    // ---- paged head listings (#686) -----------------------------------

    /**
     * [count] namespaces `n0001`.. and [count] tables `a0001`.. in `ns`,
     * written as rows: more than a listing page of each, fast.
     */
    private fun fillers(
        c: String,
        count: Int,
    ) {
        val ns = store.getNamespace(c, "ns")
        val head = store.getCatalog(c).headSnapshotId
        val n = "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i < ?) "
        store.catalogOperation(c) {
            store.runTx(
                c,
                HogliteCatalogStore.freshTxid("listing-fixture"),
                null,
                HogliteTx().apply {
                    add(
                        n + "INSERT INTO namespaces(id, name, created_snapshot) " +
                            "SELECT 1000000 + i, printf('n%04d', i), ? FROM n",
                        count,
                        head,
                    )
                    add(
                        n + "INSERT INTO tables(id, namespace_id, namespace, name, table_uuid, created_snapshot) " +
                            "SELECT 1000000 + i, ?, 'ns', printf('a%04d', i), " +
                            "printf('00000000-0000-4000-8000-%012d', i), ? FROM n",
                        count,
                        ns.namespaceId,
                        head,
                    )
                    add(
                        n + "INSERT INTO table_versions(table_id, namespace_id, namespace, name, begin_snapshot) " +
                            "SELECT 1000000 + i, ?, 'ns', printf('a%04d', i), ? FROM n",
                        count,
                        ns.namespaceId,
                        head,
                    )
                },
            )
        }
    }

    @Test
    fun `commits between head listing pages never mix two heads`() {
        val (c, tables) = setup("z1", "z2", "z4", "z5")
        fillers(c, HogliteCatalogStore.LISTING_PAGE_SIZE + 200)
        append(c, "z1", "a.parquet", 10)
        append(c, "z2", "b.parquet", 20)
        val before = store.getCatalog(c).headSnapshotId
        // The z tables are on the second page: an append, a truncate, a
        // create, a drop, and a file that begins and ends (z5) land
        // between the pages.
        val hook =
            proxy.hook({ m, _, b -> m == "POST" && "WITH ended AS MATERIALIZED" in b }) {
                append(c, "z1", "c.parquet", 7)
                store.truncateTable(c, "ns", "z2", tables[1].tableUuid, "test", "race")
                store.createTable(c, "ns", "z3", listOf(ColumnDef("id", ColType.LONG)), null, null)
                store.dropTable(c, "ns", "z4", tables[2].tableUuid, "test", "race")
                append(c, "z5", "d.parquet", 6)
                store.truncateTable(c, "ns", "z5", tables[3].tableUuid, "test", "race")
            }

        val listing = pstore.listTables(c, "ns", null, null)

        assertThat(hook.fired).isTrue()
        assertThat(store.getCatalog(c).headSnapshotId).isGreaterThan(before)
        assertThat(listing).isEqualTo(store.listTables(c, "ns", before, null))
        assertThat(listing.filter { it.name.startsWith("z") }.map { listOf(it.name, it.recordCount, it.fileCount) })
            .containsExactly(
                listOf("z1", 10L, 1L),
                listOf("z2", 20L, 1L),
                listOf("z4", 0L, 0L),
                listOf("z5", 0L, 0L),
            )
    }

    @Test
    fun `a later head listing page reads only the files ended since the first page`() {
        val (c, _) = setup("z1")
        fillers(c, HogliteCatalogStore.LISTING_PAGE_SIZE + 200)
        append(c, "z1", "a.parquet", 10)
        val from = proxy.requests.size

        pstore.listTables(c, "ns", null, null)

        // hoglited keeps no sqlite_stat1, so without INDEXED BY the planner
        // scans every file of the catalog through files_table.
        val page = store.json.readTree(proxy.requests.drop(from).single { "WITH ended AS MATERIALIZED" in it })
        val plan =
            store.catalogOperation(c) {
                store.queryCatalog(
                    c,
                    "EXPLAIN QUERY PLAN ${page["sql"].asText()}",
                    *page["args"].map { store.json.convertValue(it, Any::class.java) }.toTypedArray(),
                ).rows.map { it.last().asString() }
            }
        assertThat(plan).anyMatch { it.startsWith("SEARCH files USING INDEX files_ended") }
        assertThat(plan).noneMatch { it.startsWith("SCAN files") }
    }

    @Test
    fun `a namespace created or dropped between head listing pages is listed as at the first page`() {
        val (c, _) = setup()
        fillers(c, HogliteCatalogStore.LISTING_PAGE_SIZE + 200)
        val before = store.getCatalog(c).headSnapshotId
        // n0005 is on the first page and n1100 on the second, so one is
        // dropped after its page and one before.
        val hook =
            proxy.hook({ m, _, b -> m == "POST" && LIVE_NAMESPACE_PAGE in b && "AND name > ?" in b }) {
                store.createNamespace(c, "zz", null, null)
                store.dropNamespace(c, "n0005", null, "test", "race")
                store.dropNamespace(c, "n1100", null, "test", "race")
            }

        val listing = pstore.listNamespaces(c, null, null)

        assertThat(hook.fired).isTrue()
        assertThat(listing).isEqualTo(store.listNamespaces(c, before, null))
        assertThat(listing.map { it.name }).containsOnlyOnce("n0005", "n1100").doesNotContain("zz")
    }

    @Test
    fun `a table renamed between consumer listing pages is listed once`() {
        val (c, tables) = setup("a", "b", "c", "zz")
        for (t in tables) store.commitOffset(c, "reader", t.tableUuid, t.readSnapshotId)
        val from = proxy.requests.size
        // Pages of two: zz, renamed after the first page to sort before
        // it, is still read once.
        val hook =
            proxy.hook({ m, _, b -> m == "POST" && "FROM offsets o LEFT JOIN" in b && ") > (?" in b }) {
                store.alters.alterTable(c, "ns", "zz", listOf(AlterOp.RenameTable("aa")))
            }
        pstore.offsetPageSize = 2

        val listing =
            try {
                pstore.listConsumers(c)
            } finally {
                pstore.offsetPageSize = HogliteCatalogStore.LISTING_PAGE_SIZE
            }

        assertThat(hook.fired).isTrue()
        assertThat(listing.map { it.tableUuid }).containsExactlyInAnyOrderElementsOf(tables.map { it.tableUuid })
        assertThat(listing.map { it.tableName }).isSorted()
        // Each page walks the primary key, with no sort of the consumer's offsets.
        val pagePlans = plans(c, from, "FROM offsets o LEFT JOIN")
        assertThat(pagePlans).hasSize(3).allSatisfy { plan ->
            assertThat(plan).noneMatch { "TEMP B-TREE" in it }
        }
        // Pages after the first seek on the primary key, not a scan from its start.
        assertThat(pagePlans.drop(1)).allSatisfy { plan ->
            assertThat(plan).anyMatch { "(consumer_id,table_uuid)>(?,?)" in it }
        }
    }

    /** The plans of the requests since [from] whose body has [marker], each run through EXPLAIN QUERY PLAN. */
    private fun plans(
        c: String,
        from: Int,
        marker: String,
    ): List<List<String>> =
        proxy.requests.drop(from).filter { marker in it }.map { body ->
            val read = store.json.readTree(body)
            store.catalogOperation(c) {
                store.queryCatalog(
                    c,
                    "EXPLAIN QUERY PLAN ${read["sql"].asText()}",
                    *read["args"].map { store.json.convertValue(it, Any::class.java) }.toTypedArray(),
                ).rows.map { it.last().asString() }
            }
        }

    @Test
    fun `a head namespace listing walks namespaces_live`() {
        val (c, _) = setup()
        fillers(c, HogliteCatalogStore.LISTING_PAGE_SIZE + 200)
        val from = proxy.requests.size

        pstore.listNamespaces(c, null, null)

        val pages = plans(c, from, LIVE_NAMESPACE_PAGE)
        assertThat(pages).hasSize(2).allSatisfy { plan ->
            assertThat(plan).anyMatch { "namespaces USING INDEX namespaces_live" in it }
            assertThat(plan).noneMatch { "TEMP B-TREE" in it }
        }
        assertThat(pages[1]).anyMatch { "(name>?)" in it }
    }

    @Test
    fun `a head listing reads again from the new head when expiry passes the first page's head`() {
        val (c, _) = setup("z1")
        fillers(c, HogliteCatalogStore.LISTING_PAGE_SIZE + 200)
        append(c, "z1", "a.parquet", 10)

        // A commit, then an expiry sweep whose floor passes the first
        // page's head, between that page and the next.
        fun expireTo(path: String): Long {
            append(c, "z1", path, 7)
            val head = store.getCatalog(c).headSnapshotId
            store.catalogOperation(c) {
                store.runTx(
                    c,
                    HogliteCatalogStore.freshTxid("test"),
                    null,
                    HogliteTx().apply { add("UPDATE meta SET earliest_snapshot_id = ? WHERE id = 1", head) },
                )
            }
            return head
        }
        var tablesHead = 0L
        val tables =
            proxy.hook({ m, _, b -> m == "POST" && "WITH ended AS MATERIALIZED" in b }) {
                tablesHead = expireTo("b.parquet")
            }

        val listing = pstore.listTables(c, "ns", null, null)

        assertThat(tables.fired).isTrue()
        assertThat(listing).isEqualTo(store.listTables(c, "ns", tablesHead, null))
        assertThat(listing.single { it.name == "z1" }.recordCount).isEqualTo(17L)

        var namespacesHead = 0L
        val namespaces =
            proxy.hook({ m, _, b -> m == "POST" && LIVE_NAMESPACE_PAGE in b && "AND name > ?" in b }) {
                namespacesHead = expireTo("c.parquet")
                store.createNamespace(c, "zz", null, null)
            }

        val names = pstore.listNamespaces(c, null, null)

        assertThat(namespaces.fired).isTrue()
        assertThat(names).isEqualTo(store.listNamespaces(c, namespacesHead + 1, null))
        assertThat(names.map { it.name }).contains("zz")
    }

    @Test
    fun `pinned totals ranges seek inside a snapshot's files`() {
        val (c, tables) = setup("z1")
        val at = store.getCatalog(c).headSnapshotId
        // 2,500 files in one snapshot: ranges of 1,000 end inside it.
        store.catalogOperation(c) {
            store.runTx(
                c,
                HogliteCatalogStore.freshTxid("test"),
                null,
                HogliteTx().apply {
                    add(
                        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i < 2500) " +
                            "INSERT INTO files(id, table_id, begin_snapshot, record_count, file_size_bytes, " +
                            "row_id_start, stats_state, path) SELECT 1000000 + i, ?, ?, 1, 10, i, 'pending', " +
                            "'s3://hoglake-it/$c/f' || i FROM n",
                        tables[0].tableId,
                        at,
                    )
                },
            )
        }
        val from = proxy.requests.size
        pstore.totalsRangeSize = 1000

        val info =
            try {
                pstore.getTable(c, "ns", "z1", at)
            } finally {
                pstore.totalsRangeSize = HogliteCatalogStore.TOTALS_RANGE_SIZE
            }

        assertThat(listOf(info.recordCount, info.fileCount, info.fileSizeBytes)).containsExactly(2500L, 2500L, 25000L)
        // Each range-end read and each sum: rowid seeks inside the
        // snapshot's files, no scan, no sort of the files.
        val reads = plans(c, from, "FROM files f").flatten()
        assertThat(reads).anyMatch { "begin_snapshot=? AND rowid>?" in it }
        assertThat(reads).anyMatch { "begin_snapshot=? AND rowid>? AND rowid<?" in it }
        assertThat(reads).noneMatch { it.startsWith("SCAN f") || "TEMP B-TREE FOR ORDER BY" in it }
    }

    @Test
    fun `storage usage reads ranges of rows, not of ids`() {
        val (c, tables) = setup("z1")
        val at = store.getCatalog(c).headSnapshotId
        // 2,500 files and 1,200 DVs, their ids 100 apart: the id space a
        // long life of expiry leaves, mostly empty.
        store.catalogOperation(c) {
            store.runTx(
                c,
                HogliteCatalogStore.freshTxid("test"),
                null,
                HogliteTx().apply {
                    add(
                        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i < 2500) " +
                            "INSERT INTO files(id, table_id, begin_snapshot, record_count, file_size_bytes, " +
                            "row_id_start, stats_state, path) SELECT 100 * i, ?, ?, 1, 10, i, 'pending', " +
                            "'s3://hoglake-it/$c/f' || i FROM n",
                        tables[0].tableId,
                        at,
                    )
                    add(
                        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i < 1200) " +
                            "INSERT INTO dvs(rowid, delete_file_id, data_file_id, table_id, path, delete_count, " +
                            "file_size_bytes, begin_snapshot, epoch) SELECT 100 * i, i, 100 * i, ?, " +
                            "'s3://hoglake-it/$c/dv' || i, 1, 3, ?, 1 FROM n",
                        tables[0].tableId,
                        at,
                    )
                },
            )
        }
        val from = proxy.requests.size
        pstore.storageRangeSize = 1000

        val usage =
            try {
                pstore.storageUsage(c)
            } finally {
                pstore.storageRangeSize = HogliteCatalogStore.STORAGE_RANGE_SIZE
            }

        assertThat(usage.trackedBytes).isEqualTo(2500L * 10 + 1200L * 3)
        // 1,000, 1,000 and 500 files, with 1,000 and 200 DVs beside the
        // first two: three statements, where ranges of ids are 250.
        assertThat(proxy.requests.drop(from).count { "FROM dvs WHERE rowid > ?" in it }).isEqualTo(3)
    }

    @Test
    fun `partition values stop reading once every field has stopped counting`() {
        val (c, _) = setup()
        store.createTable(c, "ns", "p", listOf(ColumnDef("a", ColType.LONG), ColumnDef("b", ColType.LONG)))
        val spec = listOf(PartitionFieldDef(1, Transform.IDENTITY), PartitionFieldDef(2, Transform.IDENTITY))
        val table = store.alters.alterTable(c, "ns", "p", listOf(AlterOp.SetPartitionSpec(spec)))
        // 2,500 files, each with its own value of both fields.
        store.catalogOperation(c) {
            store.runTx(
                c,
                HogliteCatalogStore.freshTxid("test"),
                null,
                HogliteTx().apply {
                    add(
                        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i < 2500) " +
                            "INSERT INTO files(id, table_id, begin_snapshot, record_count, file_size_bytes, " +
                            "row_id_start, stats_state, path, spec_id, partition_values_json) " +
                            "SELECT 1000000 + i, ?, ?, 1, 10, i, 'pending', 's3://hoglake-it/$c/f' || i, ?, " +
                            "json_array('a' || i, 'b' || i) FROM n",
                        table.tableId,
                        table.readSnapshotId,
                        table.partitionSpec!!.specId,
                    )
                },
            )
        }
        val from = proxy.requests.size
        pstore.partitionRangeSize = 1000
        pstore.partitionValuesCounted = 500

        val values =
            try {
                pstore.partitionValues(c, "ns", "p", null, null)
            } finally {
                pstore.partitionRangeSize = HogliteCatalogStore.PARTITION_RANGE_SIZE
                pstore.partitionValuesCounted = HogliteCatalogStore.PARTITION_VALUES_COUNTED
            }

        assertThat(values.fields.map { it.truncated }).containsExactly(true, true)
        // The first range passes 500 values of both fields: one range end,
        // one statement for both fields, and no range after it.
        val reads = proxy.requests.drop(from)
        assertThat(reads.count { "INDEXED BY files_table" in it }).isEqualTo(1)
        assertThat(reads.count { "json_each" in it }).isEqualTo(1)
    }

    private companion object {
        /** A request body's mark of a page of the live namespaces. */
        const val LIVE_NAMESPACE_PAGE = "FROM namespaces INDEXED BY namespaces_live"
    }
}
