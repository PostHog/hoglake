package com.posthog.hoglake.hoglite

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.TableFileEnds
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.PostgresCatalogStore
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.util.UUID

/**
 * A commit that ends whole files and appends in ONE snapshot
 * (`POST /commit/replacements/prepared`'s service contract), run against
 * both backends with identical assertions. Neither backend is mocked.
 *
 * The table is partitioned by identity on `day` (a day number as a decimal
 * string, as a `day` transform spells it), so a tuple is `["20401"]`.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReplacementCommitParityIntegrationTest {
    enum class Backend { POSTGRES, HOGLITE }

    private var pg: PgTestSupport.TestDb? = null
    private var live: HogliteConformanceEngine? = null
    private lateinit var selected: Backend
    private lateinit var store: CatalogStore
    private lateinit var alters: AlterBackend

    private fun select(backend: Backend) {
        selected = backend
        when (backend) {
            Backend.POSTGRES -> {
                val db = pg ?: PgTestSupport.freshDatabase().also { pg = it }
                store = PostgresCatalogStore(CatalogService(db.jdbi), CommitService(db.jdbi), ScanService(db.jdbi))
                alters = AlterService(db.jdbi)
            }
            Backend.HOGLITE -> {
                val engine = live ?: HogliteConformanceEngine().also { live = it }
                store = engine.store
                alters = HogliteAlters(engine.store)
            }
        }
    }

    @AfterAll
    fun close() {
        pg?.close()
        live?.close()
    }

    private val identityDay = PartitionFieldDef(1, Transform.IDENTITY)

    /** A catalog with a day-partitioned `ns.ev` (columns day, v) and an unpartitioned `ns.plain`. */
    private fun fixture(): String {
        val cat = "repl" + UUID.randomUUID().toString().replace("-", "")
        store.createCatalog(cat, "s3://hoglake-it/$cat")
        store.createNamespace(cat, "ns")
        val columns = listOf(ColumnDef("day", ColType.LONG), ColumnDef("v", ColType.LONG))
        store.createTable(cat, "ns", "ev", columns)
        alters.alterTable(cat, "ns", "ev", listOf(AlterOp.SetPartitionSpec(listOf(identityDay))))
        store.createTable(cat, "ns", "plain", columns)
        return cat
    }

    private fun head(cat: String) = store.getCatalog(cat).headSnapshotId

    private fun uuid(
        cat: String,
        table: String = "ev",
    ): UUID = store.getTable(cat, "ns", table).tableUuid

    private fun dayFile(
        cat: String,
        name: String,
        day: String?,
        records: Long = 10,
    ) = FileRegistration("s3://hoglake-it/$cat/$name", records, 100, 50, partitionValues = listOf(day))

    private fun seed(
        cat: String,
        vararg files: FileRegistration,
    ): CommitResult =
        store.commit(
            cat,
            CommitRequest(
                readSnapshot = head(cat),
                idempotencyKey = UUID.randomUUID(),
                appends = listOf(TableAppend("ns", "ev", files.toList())),
            ),
        )

    private fun replacement(
        cat: String,
        ends: List<TableFileEnds>,
        appends: List<FileRegistration> = emptyList(),
        read: Long = head(cat),
    ) = CommitRequest(
        readSnapshot = read,
        idempotencyKey = UUID.randomUUID(),
        author = "tester",
        message = "replace",
        appends = if (appends.isEmpty()) emptyList() else listOf(TableAppend("ns", "ev", appends)),
        fileEnds = ends,
    )

    private fun days(
        cat: String,
        vararg days: String,
    ) = TableFileEnds("ns", "ev", uuid(cat), partitions = days.map { listOf<String?>(it) })

    private fun all(cat: String) = TableFileEnds("ns", "ev", uuid(cat), allFiles = true)

    private fun livePaths(
        cat: String,
        snapshot: Long? = null,
    ): List<String> = store.listFiles(cat, "ns", "ev", snapshot = snapshot).map { it.path.substringAfterLast('/') }

    private fun count(
        cat: String,
        lite: String,
        postgres: String,
    ): Long =
        when (selected) {
            Backend.POSTGRES ->
                pg!!.jdbi.withHandle<Long, Exception> { h ->
                    h.createQuery(postgres.replace(":catalog", store.getCatalog(cat).catalogId.toString()))
                        .mapTo(Long::class.java).one()
                }
            Backend.HOGLITE ->
                live!!.store.catalogOperation(cat) { live!!.store.queryCatalog(cat, lite).firstRow()!![0].asLong() }
        }

    private fun liveDvs(cat: String) =
        count(
            cat,
            "SELECT COUNT(*) FROM dvs WHERE end_snapshot IS NULL",
            "SELECT COUNT(*) FROM hog_delete_file WHERE catalog_id = :catalog AND end_snapshot IS NULL",
        )

    private fun state(cat: String) =
        listOf(
            head(cat),
            count(cat, "SELECT COUNT(*) FROM files", "SELECT COUNT(*) FROM hog_data_file WHERE catalog_id = :catalog"),
            count(
                cat,
                "SELECT COUNT(*) FROM files WHERE end_snapshot IS NULL",
                "SELECT COUNT(*) FROM hog_data_file WHERE catalog_id = :catalog AND end_snapshot IS NULL",
            ),
            count(
                cat,
                "SELECT COUNT(*) FROM receipts",
                "SELECT COUNT(*) FROM hog_commit_receipt WHERE catalog_id = :catalog",
            ),
        )

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `ends a day and appends its replacement in one snapshot`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1-a", "20401"), dayFile(cat, "d1-b", "20401"), dayFile(cat, "d2-a", "20402"))
        val before = head(cat)
        assertThat(livePaths(cat)).containsExactlyInAnyOrder("d1-a", "d1-b", "d2-a")

        val result =
            store.commit(
                cat,
                replacement(cat, listOf(days(cat, "20401")), listOf(dayFile(cat, "d1-new", "20401"))),
            )

        assertThat(result.snapshotId).isEqualTo(before + 1)
        assertThat(result.endedFiles).isEqualTo(2)
        // A reader at the old snapshot still sees the old day; at the new one only the new, and the
        // file this commit appended into the ended day is not itself ended.
        assertThat(livePaths(cat, before)).containsExactlyInAnyOrder("d1-a", "d1-b", "d2-a")
        assertThat(livePaths(cat, result.snapshotId)).containsExactlyInAnyOrder("d2-a", "d1-new")
        assertThat(livePaths(cat)).containsExactlyInAnyOrder("d2-a", "d1-new")
        // The change tracking a DV publication plus an append record.
        val snapshot = store.listSnapshots(cat, before, 10).first.single()
        assertThat(snapshot.changes.map { it.kind })
            .containsExactlyInAnyOrder(ChangeKind.TABLE_INSERTED_INTO, ChangeKind.TABLE_DELETED_FROM)
        assertThat(snapshot.author).isEqualTo("tester")
        assertThat(snapshot.message).isEqualTo("replace")
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `several tuples end only their days and a null partition value is addressable`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(
            cat,
            dayFile(cat, "d1", "20401"),
            dayFile(cat, "d2", "20402"),
            dayFile(cat, "d3", "20403"),
            dayFile(cat, "dn", null),
        )
        val first = store.commit(cat, replacement(cat, listOf(days(cat, "20401", "20403"))))
        assertThat(first.endedFiles).isEqualTo(2)
        assertThat(livePaths(cat)).containsExactlyInAnyOrder("d2", "dn")
        val nullTuple = TableFileEnds("ns", "ev", uuid(cat), partitions = listOf(listOf<String?>(null)))
        assertThat(store.commit(cat, replacement(cat, listOf(nullTuple))).endedFiles).isEqualTo(1)
        assertThat(livePaths(cat)).containsExactly("d2")
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `all_files ends every live file and the live deletion vectors with them`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"), dayFile(cat, "d2", "20402"))
        val target = store.listFiles(cat, "ns", "ev").first { it.path.endsWith("d1") }
        store.commit(
            cat,
            CommitRequest(
                readSnapshot = head(cat),
                idempotencyKey = UUID.randomUUID(),
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "ev",
                            listOf(DeleteFileRegistration(target.dataFileId, "s3://hoglake-it/$cat/dv", 2, 30)),
                            uuid(cat),
                        ),
                    ),
            ),
        )
        assertThat(liveDvs(cat)).isEqualTo(1)
        val before = head(cat)

        val result = store.commit(cat, replacement(cat, listOf(all(cat)), listOf(dayFile(cat, "fresh", "20401"))))

        assertThat(result.endedFiles).isEqualTo(2)
        assertThat(livePaths(cat)).containsExactly("fresh")
        assertThat(livePaths(cat, before)).containsExactlyInAnyOrder("d1", "d2")
        assertThat(liveDvs(cat)).isZero()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a tuple matching nothing is a no-op and a drop-only commit still publishes`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"))
        val before = head(cat)

        val noop = store.commit(cat, replacement(cat, listOf(days(cat, "29999")), listOf(dayFile(cat, "d9", "29999"))))
        assertThat(noop.endedFiles).isZero()
        assertThat(noop.snapshotId).isEqualTo(before + 1)
        assertThat(livePaths(cat)).containsExactlyInAnyOrder("d1", "d9")

        val dropOnly = store.commit(cat, replacement(cat, listOf(days(cat, "29999"))))
        assertThat(dropOnly.endedFiles).isEqualTo(1)
        assertThat(livePaths(cat)).containsExactly("d1")
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `re-registering a live path of an ended table is refused`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"))
        val written = state(cat)
        assertThatThrownBy {
            store.commit(cat, replacement(cat, listOf(days(cat, "20401")), listOf(dayFile(cat, "d1", "20401"))))
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
        assertThat(state(cat)).isEqualTo(written)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a recreated table is a table_recreated conflict and nothing is written`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"))
        val stale = TableFileEnds("ns", "ev", UUID.randomUUID(), partitions = listOf(listOf("20401")))
        val written = state(cat)
        assertThatThrownBy { store.commit(cat, replacement(cat, listOf(stale))) }
            .isExactlyInstanceOf(HoglakeException.TableRecreated::class.java)
        assertThat(state(cat)).isEqualTo(written)
        assertThat(livePaths(cat)).containsExactly("d1")
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a data change after read_snapshot is a conflict and an unrelated table's is not`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"))
        val read = head(cat)
        // A change to another table does not conflict.
        store.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend("ns", "plain", listOf(FileRegistration("s3://hoglake-it/$cat/p", 1, 100, 50))),
                    ),
            ),
        )
        // Another day of the SAME table does: the read set moved.
        seed(cat, dayFile(cat, "d2", "20402"))
        val written = state(cat)
        assertThatThrownBy { store.commit(cat, replacement(cat, listOf(days(cat, "20401")), read = read)) }
            .isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(state(cat)).isEqualTo(written)
        // An end since the read conflicts too, and a fresh read goes through.
        val concurrent = head(cat)
        store.commit(cat, replacement(cat, listOf(days(cat, "20402"))))
        assertThatThrownBy { store.commit(cat, replacement(cat, listOf(days(cat, "20401")), read = concurrent)) }
            .isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(store.commit(cat, replacement(cat, listOf(days(cat, "20401")))).endedFiles).isEqualTo(1)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `files under another spec refuse a partition end as partition_spec_mismatch but not all_files`(
        backend: Backend,
    ) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "old", "20401"))
        alters.alterTable(
            cat,
            "ns",
            "ev",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY)))),
        )
        val written = state(cat)

        assertThatThrownBy { store.commit(cat, replacement(cat, listOf(days(cat, "20401")))) }
            .isExactlyInstanceOf(HoglakeException.PartitionSpecMismatch::class.java)
        assertThat(state(cat)).isEqualTo(written)

        val result = store.commit(cat, replacement(cat, listOf(all(cat))))
        assertThat(result.endedFiles).isEqualTo(1)
        assertThat(livePaths(cat)).isEmpty()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a tuple of the wrong length and a partition end on an unpartitioned table are refused`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"))
        val written = state(cat)
        val wide = TableFileEnds("ns", "ev", uuid(cat), partitions = listOf(listOf("20401", "x")))
        assertThatThrownBy { store.commit(cat, replacement(cat, listOf(wide))) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
        val plain = TableFileEnds("ns", "plain", uuid(cat, "plain"), partitions = listOf(listOf("20401")))
        assertThatThrownBy { store.commit(cat, replacement(cat, listOf(plain))) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
        assertThat(state(cat)).isEqualTo(written)
        // all_files works on an unpartitioned table.
        val ok = TableFileEnds("ns", "plain", uuid(cat, "plain"), allFiles = true)
        assertThat(store.commit(cat, replacement(cat, listOf(ok))).endedFiles).isZero()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a replacement that is not guarded, attributed or well formed is refused before anything is written`(
        backend: Backend,
    ) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"))
        val written = state(cat)
        val good = replacement(cat, listOf(days(cat, "20401")))
        val dv =
            TableDeletes("ns", "ev", listOf(DeleteFileRegistration(1, "s3://hoglake-it/$cat/dv", 1, 10)), uuid(cat))
        val refused =
            mapOf(
                "deletes" to good.copy(deletes = listOf(dv)),
                "require_unchanged_tables" to good.copy(requireUnchangedTables = true),
                "no read_snapshot" to good.copy(readSnapshot = null),
                "no idempotency_key" to good.copy(idempotencyKey = null),
                "no author" to good.copy(author = null),
                "blank message" to good.copy(message = " "),
                "empty file_ends" to good.copy(fileEnds = emptyList()),
                "both partitions and all_files" to
                    good.copy(fileEnds = listOf(days(cat, "20401").copy(allFiles = true))),
                "neither partitions nor all_files" to
                    good.copy(fileEnds = listOf(TableFileEnds("ns", "ev", uuid(cat)))),
                "empty partitions" to good.copy(fileEnds = listOf(days(cat))),
                "ragged tuples" to
                    good.copy(
                        fileEnds =
                            listOf(
                                TableFileEnds(
                                    "ns",
                                    "ev",
                                    uuid(cat),
                                    partitions = listOf(listOf("1"), listOf("1", "2")),
                                ),
                            ),
                    ),
                "a table twice" to good.copy(fileEnds = listOf(days(cat, "20401"), all(cat))),
            )
        for ((what, request) in refused) {
            assertThatThrownBy { store.commit(cat, request) }
                .describedAs(what)
                .isExactlyInstanceOf(HoglakeException.Validation::class.java)
        }
        assertThat(state(cat)).isEqualTo(written)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a replay returns the original snapshot, the receipt serves it, and a changed request is refused`(
        backend: Backend,
    ) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"))
        val request = replacement(cat, listOf(days(cat, "20401")), listOf(dayFile(cat, "d1-new", "20401")))

        val first = store.commit(cat, request)
        assertThat(first.endedFiles).isEqualTo(1)
        val written = state(cat)

        val replay = store.commit(cat, request)
        assertThat(replay.snapshotId).isEqualTo(first.snapshotId)
        assertThat(replay.schemaVersion).isEqualTo(first.schemaVersion)
        assertThat(state(cat)).isEqualTo(written)
        assertThat(store.receipt(cat, request.idempotencyKey!!).snapshotId).isEqualTo(first.snapshotId)

        // A repeated tuple is not part of the request's identity; its content is.
        assertThat(
            store.commit(
                cat,
                request.copy(
                    fileEnds = listOf(days(cat, "20401").copy(partitions = listOf(listOf("20401"), listOf("20401")))),
                ),
            ).snapshotId,
        ).isEqualTo(first.snapshotId)
        assertThatThrownBy { store.commit(cat, request.copy(fileEnds = listOf(days(cat, "20402")))) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
        assertThat(state(cat)).isEqualTo(written)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a changefeed window crossing a replacement requires reconciliation`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"))
        val before = head(cat)
        store.commit(cat, replacement(cat, listOf(days(cat, "20401")), listOf(dayFile(cat, "d1-new", "20401"))))
        assertThatThrownBy { store.changes(cat, "ns", "ev", before) }
            .isExactlyInstanceOf(HoglakeException.ReconciliationRequired::class.java)
        // A window after it is ordinary.
        assertThat(store.changes(cat, "ns", "ev", head(cat)).files).isEmpty()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `the table's live totals follow the ended files`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401", records = 7), dayFile(cat, "d2", "20402", records = 5))
        store.commit(
            cat,
            replacement(cat, listOf(days(cat, "20401")), listOf(dayFile(cat, "d1-new", "20401", records = 3))),
        )
        // Hoglite keeps transactional head totals (table_totals), so a head read exercises the
        // decrement; PostgreSQL answers head from a sampler, so it is read pinned.
        val table = store.getTable(cat, "ns", "ev", snapshot = if (backend == Backend.HOGLITE) null else head(cat))
        assertThat(table.recordCount).isEqualTo(8)
        assertThat(table.fileCount).isEqualTo(2)
    }
}
