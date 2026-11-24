package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.PartitionValues
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.PartitionListingService
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/** Engine limits and adapter paging use real responses, without raised engine limits. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteReadLimitsIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine
    private val store get() = engine.store

    @BeforeAll
    fun start() {
        engine = HogliteConformanceEngine()
    }

    @AfterAll
    fun close() {
        if (::engine.isInitialized) engine.close()
    }

    private fun catalog(): String {
        val cat = HogliteConformanceEngine.uniqueName("limits")
        store.createCatalog(cat, "s3://hoglake-it/$cat")
        return cat
    }

    @Test
    fun `file listings and changefeeds return more than the engine row cap without gaps`() {
        val cat = catalog()
        store.createNamespace(cat, "ns")
        val table = store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val count = 100_001
        val to = table.readSnapshotId
        store.catalogOperation(cat) {
            for (first in 1..count step 5000) {
                val last = minOf(first + 4999, count)
                store.runTx(
                    cat,
                    HogliteCatalogStore.freshTxid("paging-fixture"),
                    null,
                    HogliteTx().apply {
                        add(
                            """WITH RECURSIVE n(i) AS (VALUES(?) UNION ALL SELECT i+1 FROM n WHERE i < ?)
                            INSERT INTO files(id,table_id,path,record_count,file_size_bytes,row_id_start,
                                              stats_state,begin_snapshot)
                            SELECT i,?,'s3://hoglake-it/$cat/'||i,2,100,i/2,'pending',? FROM n""",
                            first,
                            last,
                            table.tableId,
                            to,
                        )
                        add(
                            """WITH RECURSIVE n(i) AS (VALUES(?) UNION ALL SELECT i+1 FROM n WHERE i < ?)
                            INSERT INTO dvs(delete_file_id,data_file_id,table_id,path,delete_count,
                                            file_size_bytes,begin_snapshot,epoch)
                            SELECT i,i,?,'s3://hoglake-it/$cat/dv-'||i,1,10,?,1 FROM n""",
                            first,
                            last,
                            table.tableId,
                            to,
                        )
                    },
                )
            }
        }
        val expected = (1..count).map(Int::toLong)
        val listed = store.listFiles(cat, "ns", "t")
        assertThat(listed.map { it.dataFileId }).containsExactlyElementsOf(expected)
        val plan = store.changes(cat, "ns", "t", to - 1, to)
        assertThat(plan.files.map { it.dataFileId }).containsExactlyElementsOf(expected)
        assertThat(plan.deleteFiles.map { it.deleteFileId }).containsExactlyElementsOf(expected)
        assertThat(plan.fromSnapshot).isEqualTo(to - 1)
        assertThat(plan.toSnapshot).isEqualTo(to)
        // The footprint sums ranges of rows: one at production size, then
        // ranges of 7,919 files and DVs, the last of each a partial one.
        val footprint = count * 110L
        assertThat(store.storageUsage(cat).trackedBytes).isEqualTo(footprint)
        try {
            store.storageRangeSize = 7_919
            assertThat(store.storageUsage(cat).trackedBytes).isEqualTo(footprint)
        } finally {
            store.storageRangeSize = HogliteCatalogStore.STORAGE_RANGE_SIZE
        }
    }

    /** Runs [sql] over `i` in [first]..[last], 5,000 at a time: the engine caps a write's time too. */
    private fun fill(
        cat: String,
        first: Int,
        last: Int,
        vararg sql: Pair<String, List<Any?>>,
    ) {
        store.catalogOperation(cat) {
            for (from in first..last step 5000) {
                val to = minOf(from + 4999, last)
                store.runTx(
                    cat,
                    HogliteCatalogStore.freshTxid("listing-fixture"),
                    null,
                    HogliteTx().apply {
                        for ((statement, args) in sql) {
                            add(
                                "WITH RECURSIVE n(i) AS (VALUES(?) UNION ALL SELECT i+1 FROM n WHERE i < ?) $statement",
                                from,
                                to,
                                *args.toTypedArray(),
                            )
                        }
                    },
                )
            }
        }
    }

    /** The table uuid of fixture row [i]: below any random uuid, and in [i] order. */
    private fun fixtureUuid(i: Int) = UUID.fromString("00000000-0000-4000-8000-%012d".format(i))

    @Test
    fun `namespace and table listings return more than the engine row cap in order`() {
        val cat = catalog()
        val ns = store.createNamespace(cat, "ns")
        val head = store.getCatalog(cat).headSnapshotId
        val count = 100_001
        // Ids far above the allocators', names in i order, every fifth
        // table with head totals so a page boundary cannot drop them.
        fill(
            cat,
            1,
            count,
            "INSERT INTO namespaces(id, name, created_snapshot) SELECT 1000000 + i, printf('n%06d', i), ? FROM n" to
                listOf(head),
            "INSERT INTO tables(id, namespace_id, namespace, name, table_uuid, created_snapshot) " +
                "SELECT 1000000 + i, ?, 'ns', printf('t%06d', i), " +
                "printf('00000000-0000-4000-8000-%012d', i), ? FROM n" to listOf(ns.namespaceId, head),
            "INSERT INTO table_versions(table_id, namespace_id, namespace, name, begin_snapshot) " +
                "SELECT 1000000 + i, ?, 'ns', printf('t%06d', i), ? FROM n" to listOf(ns.namespaceId, head),
            "INSERT INTO table_totals(table_id, record_count, file_count, file_size_bytes) " +
                "SELECT 1000000 + i, i, 1, 10 * i FROM n WHERE i % 5 = 0" to emptyList(),
        )

        // BINARY order: "ns" after every "n0…".
        val names = (1..count).map { "n%06d".format(it) } + "ns"
        assertThat(store.listNamespaces(cat).map { it.name }).containsExactlyElementsOf(names)
        assertThat(store.listNamespaces(cat, head).map { it.name }).containsExactlyElementsOf(names)

        val tables = (1..count).map { "t%06d".format(it) }
        val listed = store.listTables(cat, "ns")
        assertThat(listed.map { it.name }).containsExactlyElementsOf(tables)
        assertThat(listed.map { it.tableUuid }).containsExactlyElementsOf((1..count).map { fixtureUuid(it) })
        assertThat(listed.map { it.recordCount })
            .containsExactlyElementsOf((1..count).map { if (it % 5 == 0) it.toLong() else 0L })
        // A pinned listing aggregates the (empty) manifest, so only names and ids are compared.
        val pinned = store.listTables(cat, "ns", head)
        assertThat(pinned.map { it.name to it.tableId }).containsExactlyElementsOf(listed.map { it.name to it.tableId })
    }

    @Test
    fun `a view listing returns more than a page in order`() {
        val cat = catalog()
        val ns = store.createNamespace(cat, "ns")
        val head = store.getCatalog(cat).headSnapshotId
        val count = HogliteViews.VIEW_PAGE_SIZE * 2 + 5
        fill(
            cat,
            1,
            count,
            "INSERT INTO views(id, namespace_id, namespace, name, view_uuid, dialect, sql_text, created_snapshot) " +
                "SELECT 1000000 + i, ?, 'ns', printf('v%04d', i), printf('00000000-0000-4000-8000-%012d', i), " +
                "'duckdb', 'SELECT ' || i, ? FROM n" to listOf(ns.namespaceId, head),
            // Dropped views, in the pages' name range: never listed.
            "INSERT INTO views(id, namespace_id, namespace, name, view_uuid, dialect, sql_text, created_snapshot, " +
                "dropped_snapshot) SELECT 2000000 + i, ?, 'ns', printf('v%04d', i), " +
                "printf('00000000-0000-4000-9000-%012d', i), 'duckdb', 'SELECT 0', ? - 1, ? FROM n " +
                "WHERE i % 50 = 0" to listOf(ns.namespaceId, head, head),
        )

        val listed = store.views.list(cat, "ns")

        assertThat(listed.map { it.name }).containsExactlyElementsOf((1..count).map { "v%04d".format(it) })
        assertThat(listed.map { it.sql }).containsExactlyElementsOf((1..count).map { "SELECT $it" })
    }

    @Test
    fun `consumer and offset listings return more than the engine row cap in order`() {
        val cat = catalog()
        store.createNamespace(cat, "ns")
        // Real tables named against their uuid order: the listing orders a
        // consumer's offsets by table name, and their random uuids sort
        // after every fixture uuid, onto the primary key's last page.
        val b = store.createTable(cat, "ns", "b", listOf(ColumnDef("id", ColType.LONG)))
        val a = store.createTable(cat, "ns", "a", listOf(ColumnDef("id", ColType.LONG)))
        val count = 100_001
        fill(
            cat,
            1,
            count,
            "INSERT INTO offsets(consumer_id, table_uuid, committed_snapshot, updated_at) " +
                "SELECT 'c1', printf('00000000-0000-4000-8000-%012d', i), 1, '2026-01-01T00:00:00Z' FROM n" to
                emptyList(),
            // An offset whose table is gone: no name, so after the named ones.
            "INSERT INTO offsets(consumer_id, table_uuid, committed_snapshot, updated_at) " +
                "SELECT 'c2', printf('00000000-0000-4000-8000-%012d', i), 1, '2026-01-01T00:00:00Z' FROM n " +
                "WHERE i = 7" to emptyList(),
        )
        for (t in listOf(a, b)) {
            store.commitOffset(cat, "c1", t.tableUuid, t.readSnapshotId)
            store.commitOffset(cat, "c0", t.tableUuid, t.readSnapshotId)
            store.commitOffset(cat, "c2", t.tableUuid, t.readSnapshotId)
        }

        val fixtures = (1..count).map { fixtureUuid(it) }
        assertThat(store.listOffsets(cat, "c1").map { it.tableUuid })
            .containsExactlyElementsOf((fixtures + listOf(a.tableUuid, b.tableUuid)).sortedBy { it.toString() })
        val consumers = store.listConsumers(cat)
        val tableFirst = listOf(a.tableUuid, b.tableUuid)
        assertThat(consumers.map { it.consumerId to it.tableUuid }).containsExactlyElementsOf(
            tableFirst.map { "c0" to it } +
                (tableFirst + fixtures).map { "c1" to it } +
                (tableFirst + fixtureUuid(7)).map { "c2" to it },
        )
        assertThat(consumers.filter { it.tableUuid == a.tableUuid }.map { it.tableName }).containsOnly("a")
        assertThat(consumers.filter { it.tableUuid == fixtureUuid(7) }.map { it.namespace }).containsOnly(null)
    }

    @Test
    fun `pinned totals, listings and partition reports read the manifest in ranges with one statement's result`() {
        val cat = catalog()
        store.createNamespace(cat, "ns")
        // Listed around t: a few files (a), none (e), and a snapshot's
        // worth of files in one begin_snapshot (u), so a range spans
        // tables and ends inside a tie.
        val id = listOf(ColumnDef("id", ColType.LONG))
        val others = listOf("a", "e", "u").map { store.createTable(cat, "ns", it, id) }
        store.createTable(cat, "ns", "t", listOf(ColumnDef("a", ColType.LONG), ColumnDef("b", ColType.LONG)))
        val spec = listOf(PartitionFieldDef(1, Transform.IDENTITY), PartitionFieldDef(2, Transform.IDENTITY))
        val table = store.alters.alterTable(cat, "ns", "t", listOf(AlterOp.SetPartitionSpec(spec)))
        // Two snapshots after the spec's, for the files to begin at.
        store.createNamespace(cat, "other1")
        store.createNamespace(cat, "other2")
        val head = store.getCatalog(cat).headSnapshotId
        assertThat(head).isEqualTo(table.readSnapshotId + 2)
        // A multiple of 44, so field b's NULL, "～" and "😀" tie at both
        // snapshots: SQLite's BINARY order puts U+FF5E before U+1F600,
        // which UTF-16 order reverses. Files begin at two snapshots with
        // their ids interleaved, and every fourth ended at the head.
        val count = 25_036
        fill(
            cat,
            1,
            count,
            "INSERT INTO files(id, table_id, begin_snapshot, end_snapshot, record_count, file_size_bytes, " +
                "row_id_start, stats_state, path, spec_id, partition_values_json) " +
                "SELECT i, ?, ? - 1 - i % 2, CASE WHEN i % 4 = 0 THEN ? END, i % 7, 10 * i, i, 'pending', " +
                "'s3://hoglake-it/$cat/' || i, ?, json_array('v' || (i % 700), " +
                "CASE i % 11 WHEN 0 THEN NULL WHEN 1 THEN ? WHEN 2 THEN ? ELSE 'x' END) FROM n" to
                listOf(table.tableId, head, head, table.partitionSpec!!.specId, "～", "😀"),
            "INSERT INTO files(id, table_id, begin_snapshot, record_count, file_size_bytes, row_id_start, " +
                "stats_state, path) SELECT 100000 + i, ?, ? - 2, 1, i, i, 'pending', 's3://hoglake-it/$cat/a' || i " +
                "FROM n WHERE i <= 3" to listOf(others[0].tableId, head),
            "INSERT INTO files(id, table_id, begin_snapshot, end_snapshot, record_count, file_size_bytes, " +
                "row_id_start, stats_state, path) SELECT 200000 + i, ?, ? - 1, CASE WHEN i % 3 = 0 THEN ? END, 2, i, " +
                "i, 'pending', 's3://hoglake-it/$cat/u' || i FROM n WHERE i <= 1500" to
                listOf(others[2].tableId, head, head),
        )

        /** The single statement this change replaced, over one table. */
        fun totals(
            tableId: Long,
            at: Long,
        ): List<Long?> =
            store.catalogOperation(cat) {
                store.queryCatalog(
                    cat,
                    "SELECT COALESCE(SUM(record_count),0), COUNT(*), COALESCE(SUM(file_size_bytes),0) " +
                        "FROM files WHERE table_id = ? AND begin_snapshot <= ? " +
                        "AND (end_snapshot IS NULL OR end_snapshot > ?)",
                    tableId,
                    at,
                    at,
                ).firstRow()!!.map { it.asLong() }
            }

        // The one-statement reads this change replaced, as the reference.
        fun oracle(at: Long): Pair<List<Long?>, List<List<String?>>> =
            store.catalogOperation(cat) {
                val totals = totals(table.tableId, at)
                val values =
                    spec.indices.map { key ->
                        store.queryCatalog(
                            cat,
                            "SELECT pv.value, COUNT(*) AS n FROM files f, json_each(f.partition_values_json) pv " +
                                "WHERE f.table_id = ? AND f.begin_snapshot <= ? " +
                                "AND (f.end_snapshot IS NULL OR f.end_snapshot > ?) " +
                                "AND pv.key = ? GROUP BY pv.value ORDER BY n DESC, pv.value LIMIT ?",
                            table.tableId,
                            at,
                            at,
                            key,
                            PartitionValues.PARTITION_VALUES_CAP + 1,
                        ).rows.map { it[0] as String? }
                    }
                totals to values
            }

        fun read(at: Long): Pair<List<Long?>, List<List<String?>>> {
            val info = store.getTable(cat, "ns", "t", at)
            val pv = store.partitionValues(cat, "ns", "t", at)
            assertThat(pv.fields.map { it.truncated }).containsExactly(true, false)
            return listOf(info.recordCount, info.fileCount, info.fileSizeBytes) to pv.fields.map { it.values }
        }

        fun listing(at: Long) =
            store.listTables(
                cat,
                "ns",
                at,
            ).associate { it.name to listOf(it.recordCount, it.fileCount, it.fileSizeBytes) }

        // The partition reports read the files at head, so one range of
        // every file is their single statement.
        fun reports() =
            store.partitions.partitionStats(cat, null, null, 500) to
                store.partitions.listPartitions(
                    cat,
                    "ns",
                    "t",
                    PartitionListingService.SortColumn.PARTITION,
                    null,
                    100_000,
                    0,
                    emptyList(),
                )
        store.partitionRangeSize = 1_000_000
        val oneRange =
            try {
                reports()
            } finally {
                store.partitionRangeSize = HogliteCatalogStore.PARTITION_RANGE_SIZE
            }
        assertThat(oneRange.second.total).isGreaterThan(PartitionValues.PARTITION_VALUES_CAP)
        assertThat(reports()).isEqualTo(oneRange)

        for (at in listOf(head, head - 1)) {
            val expected = oracle(at)
            assertThat(expected.first[1]).isEqualTo(if (at == head) count * 3L / 4 else count.toLong())
            assertThat(expected.second[1]).containsExactly("x", null, "～", "😀")
            assertThat(expected.second[0]).hasSize(PartitionValues.PARTITION_VALUES_CAP + 1)
            val oneCap = expected.first to expected.second.map { it.take(PartitionValues.PARTITION_VALUES_CAP) }
            val tables = (others + table).associate { it.name to totals(it.tableId, at) }
            assertThat(tables.getValue("u")[1]).isEqualTo(if (at == head) 1000L else 1500L)
            assertThat(tables.getValue("e")).containsExactly(0L, 0L, 0L)
            // Production sizes (one totals range, three partition ranges),
            // then ranges that span tables and end inside each
            // begin_snapshot's files.
            assertThat(read(at)).isEqualTo(oneCap)
            assertThat(listing(at)).isEqualTo(tables)
            try {
                store.totalsRangeSize = 997
                store.partitionRangeSize = 997
                assertThat(read(at)).isEqualTo(oneCap)
                assertThat(listing(at)).isEqualTo(tables)
                assertThat(reports()).isEqualTo(oneRange)
            } finally {
                store.totalsRangeSize = HogliteCatalogStore.TOTALS_RANGE_SIZE
                store.partitionRangeSize = HogliteCatalogStore.PARTITION_RANGE_SIZE
            }
        }
    }

    @Test
    fun `invalid query and read transaction SQL have final validation errors`() {
        val cat = catalog()
        store.catalogOperation(cat) {
            assertThatThrownBy { store.queryCatalog(cat, "SELECT missing_column FROM meta") }
                .isExactlyInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("missing_column")
            assertThatThrownBy {
                store.readCatalog(cat, listOf(HogliteStatement("SELECT missing_column FROM meta")))
            }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("missing_column")
        }
    }

    @Test
    fun `a query above the engine row cap has a final result limit error`() {
        val cat = catalog()
        store.catalogOperation(cat) {
            assertThatThrownBy {
                store.queryCatalog(
                    cat,
                    "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<100001) SELECT i FROM n",
                )
            }.isExactlyInstanceOf(HoglakeException.BackendResultTooLarge::class.java)
        }
    }

    @Test
    fun `a write request above the engine body cap has a final validation error`() {
        val cat = catalog()
        val before = store.getCatalog(cat).headSnapshotId
        store.catalogOperation(cat) {
            assertThatThrownBy {
                store.runTx(
                    cat,
                    HogliteCatalogStore.freshTxid("body-cap"),
                    null,
                    HogliteTx().apply { add("SELECT ?", "x".repeat(64 * 1024 * 1024)) },
                )
            }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
        }
        assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(before)
    }

    @Test
    fun `an engine query deadline has a final timeout error`() {
        val cat = catalog()
        // The engine deadline is five seconds. Let it answer before the HTTP timeout.
        HogliteClient(engine.url(), readTimeoutMs = 15_000, retryBudgetMs = 20_000).use { client ->
            val selected = HogliteCatalogStore(client, HogliteConformanceEngine.TENANTS)
            selected.catalogOperation(cat) {
                assertThatThrownBy {
                    selected.queryCatalog(
                        cat,
                        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<1000000000) " +
                            "SELECT sum(i) FROM n",
                    )
                }.isExactlyInstanceOf(HoglakeException.BackendQueryTimeout::class.java)
            }
        }
    }
}
