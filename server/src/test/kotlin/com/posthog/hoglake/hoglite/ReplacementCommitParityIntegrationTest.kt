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
import org.assertj.core.api.Assertions.catchThrowable
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

    /** A deletion vector on the live file named [name], in its own commit. */
    private fun addDv(
        cat: String,
        name: String,
        table: String = "ev",
    ) {
        val target = store.listFiles(cat, "ns", table).first { it.path.endsWith("/$name") }
        store.commit(
            cat,
            CommitRequest(
                readSnapshot = head(cat),
                idempotencyKey = UUID.randomUUID(),
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            table,
                            listOf(
                                DeleteFileRegistration(target.dataFileId, "s3://hoglake-it/$cat/dv-$name", 2, 30),
                            ),
                            uuid(cat, table),
                        ),
                    ),
            ),
        )
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
            .containsExactlyInAnyOrder(
                ChangeKind.TABLE_INSERTED_INTO,
                ChangeKind.TABLE_DELETED_FROM,
                ChangeKind.TABLE_REPLACED,
            )
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
    fun `a partition end conflicts only with changes to its own tuples`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"), dayFile(cat, "d2", "20402"))
        val read = head(cat)
        // A change to another table, and appends and DVs in another day, since the read: no conflict.
        store.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend("ns", "plain", listOf(FileRegistration("s3://hoglake-it/$cat/p", 1, 100, 50))),
                    ),
            ),
        )
        seed(cat, dayFile(cat, "d3", "20403"))
        addDv(cat, "d2")
        val written = state(cat)
        val ok = store.commit(cat, replacement(cat, listOf(days(cat, "20401")), read = read))
        assertThat(ok.endedFiles).isEqualTo(1)
        assertThat(state(cat)).isNotEqualTo(written)
        assertThat(livePaths(cat)).containsExactlyInAnyOrder("d2", "d3")

        // Each kind of touch of the target tuple since the read is a conflict.
        val conflicts =
            listOf<Pair<String, () -> Unit>>(
                "an append" to { seed(cat, dayFile(cat, "d2-more", "20402")) },
                "a deletion vector" to { addDv(cat, "d3") },
                "an end" to { store.commit(cat, replacement(cat, listOf(days(cat, "20403")))) },
            )
        for ((what, touch) in conflicts) {
            val since = head(cat)
            touch()
            val before = state(cat)
            val tuple = if (what == "an append") "20402" else "20403"
            assertThatThrownBy { store.commit(cat, replacement(cat, listOf(days(cat, tuple)), read = since)) }
                .describedAs(what)
                .isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
                .hasMessageContaining("ns.ev")
            assertThat(state(cat)).describedAs(what).isEqualTo(before)
        }
        // A fresh read goes through.
        assertThat(store.commit(cat, replacement(cat, listOf(days(cat, "20402")))).endedFiles).isEqualTo(2)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `an all_files end stays table-wide and DDL conflicts with either end`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"))
        val read = head(cat)
        // An append to ANY day since the read conflicts with an all_files end.
        seed(cat, dayFile(cat, "d9", "20409"))
        assertThatThrownBy { store.commit(cat, replacement(cat, listOf(all(cat)), read = read)) }
            .isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("ns.ev")
        // A replacement of another table-wide kind does too: it ended a file.
        val afterAppend = head(cat)
        store.commit(cat, replacement(cat, listOf(days(cat, "20409"))))
        assertThatThrownBy { store.commit(cat, replacement(cat, listOf(all(cat)), read = afterAppend)) }
            .isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)

        // DDL since the read is the non-retryable refusal for a partition end as well.
        val ddlRead = head(cat)
        alters.alterTable(cat, "ns", "ev", listOf(AlterOp.SetSortOrder(emptyList())))
        assertThatThrownBy { store.commit(cat, replacement(cat, listOf(days(cat, "20401")), read = ddlRead)) }
            .isExactlyInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
        assertThatThrownBy { store.commit(cat, replacement(cat, listOf(all(cat)), read = ddlRead)) }
            .isExactlyInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
        // A mixed window (DDL and a touch of the tuple) is the retryable conflict.
        seed(cat, dayFile(cat, "d1-more", "20401"))
        assertThatThrownBy { store.commit(cat, replacement(cat, listOf(days(cat, "20401")), read = ddlRead)) }
            .isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `files appended by the commit itself are never ended`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"), dayFile(cat, "d2", "20402"))
        // By tuple: the new file lands in the very partition being ended.
        val byTuple =
            store.commit(cat, replacement(cat, listOf(days(cat, "20401")), listOf(dayFile(cat, "n1", "20401"))))
        assertThat(byTuple.endedFiles).isEqualTo(1)
        assertThat(livePaths(cat)).containsExactlyInAnyOrder("d2", "n1")
        // By all_files: every old file ends, and only the old ones.
        val byAll =
            store.commit(
                cat,
                replacement(cat, listOf(all(cat)), listOf(dayFile(cat, "n2", "20401"), dayFile(cat, "n3", "20402"))),
            )
        assertThat(byAll.endedFiles).isEqualTo(2)
        assertThat(livePaths(cat, byAll.snapshotId)).containsExactlyInAnyOrder("n2", "n3")
        assertThat(
            count(
                cat,
                "SELECT COUNT(*) FROM files WHERE begin_snapshot = ${byAll.snapshotId} AND end_snapshot IS NOT NULL",
                "SELECT COUNT(*) FROM hog_data_file WHERE catalog_id = :catalog " +
                    "AND begin_snapshot = ${byAll.snapshotId} AND end_snapshot IS NOT NULL",
            ),
        ).isZero()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a partition end takes a file's live deletion vector with it and leaves the others`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"), dayFile(cat, "d2", "20402"))
        addDv(cat, "d1")
        addDv(cat, "d2")
        assertThat(liveDvs(cat)).isEqualTo(2)
        val before = head(cat)
        assertThat(store.commit(cat, replacement(cat, listOf(days(cat, "20401")))).endedFiles).isEqualTo(1)
        assertThat(liveDvs(cat)).isEqualTo(1)
        assertThat(livePaths(cat)).containsExactly("d2")
        assertThat(livePaths(cat, before)).containsExactlyInAnyOrder("d1", "d2")
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `an object-prefix table is replaced under its prefix`(backend: Backend) {
        select(backend)
        val cat = fixture()
        val columns = listOf(ColumnDef("day", ColType.LONG), ColumnDef("v", ColType.LONG))
        store.createTable(cat, "ns", "obj", columns)
        alters.alterTable(
            cat,
            "ns",
            "obj",
            listOf(AlterOp.SetPartitionSpec(listOf(identityDay)), AlterOp.SetObjectPrefixField("day")),
        )

        fun objFile(
            name: String,
            day: String,
        ) = FileRegistration(
            "s3://hoglake-it/$cat/data/ns/obj/day=$day/$name",
            10,
            100,
            50,
            partitionValues = listOf(day),
        )

        fun obj(
            vararg appends: FileRegistration,
            ends: List<List<String?>> = listOf(listOf("20401")),
        ) = CommitRequest(
            readSnapshot = head(cat),
            idempotencyKey = UUID.randomUUID(),
            author = "t",
            message = "m",
            appends = listOf(TableAppend("ns", "obj", appends.toList())),
            fileEnds = listOf(TableFileEnds("ns", "obj", uuid(cat, "obj"), partitions = ends)),
        )
        store.commit(cat, obj(objFile("a", "20401"), ends = listOf(listOf("29999"))))
        val written = state(cat)
        // A replacement written outside its day's prefix is refused, and nothing is written.
        val stray =
            FileRegistration(
                "s3://hoglake-it/$cat/data/ns/obj/day=20402/b",
                10,
                100,
                50,
                partitionValues = listOf("20401"),
            )
        assertThatThrownBy { store.commit(cat, obj(stray)) }.isInstanceOf(HoglakeException::class.java)
        assertThat(state(cat)).isEqualTo(written)
        val ok = store.commit(cat, obj(objFile("c", "20401")))
        assertThat(ok.endedFiles).isEqualTo(1)
        assertThat(store.listFiles(cat, "ns", "obj").map { it.path.substringAfterLast('/') }).containsExactly("c")
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `an appended path the catalog has recorded is refused, live or ended, naming the path`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "x", "20401"), dayFile(cat, "keep", "20402"))
        // A ends x and appends y (a new path, fine).
        store.commit(cat, replacement(cat, listOf(days(cat, "20401")), listOf(dayFile(cat, "y", "20401"))))
        val written = state(cat)
        // B ends y and re-appends x: x's bytes would be overwritten under the snapshots that read it.
        assertThatThrownBy {
            store.commit(cat, replacement(cat, listOf(days(cat, "20401")), listOf(dayFile(cat, "x", "20401"))))
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("$cat/x")
        // A live path of ANY table of the catalog is refused too, and so is one the request ends.
        assertThatThrownBy {
            store.commit(cat, replacement(cat, listOf(days(cat, "20401")), listOf(dayFile(cat, "keep", "20405"))))
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("$cat/keep")
        assertThat(state(cat)).isEqualTo(written)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a recorded path is refused with the same words on both backends`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"))
        val e =
            catchThrowable {
                store.commit(cat, replacement(cat, listOf(days(cat, "20401")), listOf(dayFile(cat, "d1", "20401"))))
            }
        assertThat(e).isExactlyInstanceOf(HoglakeException.Validation::class.java)
        assertThat(e.message).isEqualTo(
            "appended path s3://hoglake-it/$cat/d1 is already recorded as a data file of this catalog, " +
                "live or ended; write the replacement to a new path",
        )
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a date, month, year, hour or bucket value must be a base-10 integer`(backend: Backend) {
        select(backend)
        val cat = fixture()
        val columns = listOf(ColumnDef("d", ColType.DATE), ColumnDef("k", ColType.LONG), ColumnDef("s", ColType.STRING))
        store.createTable(cat, "ns", "dt", columns)
        alters.alterTable(
            cat,
            "ns",
            "dt",
            listOf(
                AlterOp.SetPartitionSpec(
                    listOf(
                        PartitionFieldDef(1, Transform.DAY),
                        PartitionFieldDef(2, Transform.BUCKET, 16),
                        PartitionFieldDef(3, Transform.IDENTITY),
                    ),
                ),
            ),
        )

        fun dt(vararg tuple: String?) =
            replacement(cat, listOf(TableFileEnds("ns", "dt", uuid(cat, "dt"), partitions = listOf(tuple.toList()))))
        val written = state(cat)
        for ((value, field) in listOf("2025-11-10" to 0, "x" to 0, "+5" to 0, " 5" to 0, "5.0" to 0, "" to 0)) {
            assertThatThrownBy { store.commit(cat, dt(value, "3", "anything")) }
                .describedAs("'$value'")
                .isExactlyInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("ns.dt")
                .hasMessageContaining("field $field")
                .hasMessageContaining("'$value'")
        }
        assertThatThrownBy { store.commit(cat, dt("20401", "three", "2025-11-10")) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("field 1")
        assertThat(state(cat)).isEqualTo(written)
        // Integers, negatives, nulls and any identity spelling are accepted (and end nothing here).
        for (tuple in listOf(
            arrayOf<String?>("20401", "3", "2025-11-10"),
            arrayOf<String?>(null, null, null),
            arrayOf<String?>("-5", "0", ""),
        )) {
            assertThat(store.commit(cat, dt(*tuple)).endedFiles).isZero()
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a multi-field spec matches whole tuples and nulls in either position`(backend: Backend) {
        select(backend)
        val cat = fixture()
        val columns = listOf(ColumnDef("a", ColType.LONG), ColumnDef("b", ColType.LONG))
        store.createTable(cat, "ns", "mf", columns)
        alters.alterTable(
            cat,
            "ns",
            "mf",
            listOf(
                AlterOp.SetPartitionSpec(
                    listOf(PartitionFieldDef(1, Transform.IDENTITY), PartitionFieldDef(2, Transform.IDENTITY)),
                ),
            ),
        )

        fun mf(
            name: String,
            a: String?,
            b: String?,
        ) = FileRegistration("s3://hoglake-it/$cat/mf-$name", 5, 50, 20, partitionValues = listOf(a, b))

        store.commit(
            cat,
            CommitRequest(
                readSnapshot = head(cat),
                idempotencyKey = UUID.randomUUID(),
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "mf",
                            listOf(
                                mf("1-1", "1", "1"),
                                mf("1-2", "1", "2"),
                                mf("n-1", null, "1"),
                                mf("1-n", "1", null),
                                mf("n-n", null, null),
                            ),
                        ),
                    ),
            ),
        )

        fun end(vararg tuples: List<String?>) =
            replacement(cat, listOf(TableFileEnds("ns", "mf", uuid(cat, "mf"), partitions = tuples.toList())))

        fun live() = store.listFiles(cat, "ns", "mf").map { it.path.substringAfterLast("mf-") }
        assertThat(store.commit(cat, end(listOf("1", "1"))).endedFiles).isEqualTo(1)
        assertThat(store.commit(cat, end(listOf(null, "1"))).endedFiles).isEqualTo(1)
        // (1, null) does not match (null, null), and a one-sided match on a prefix of the tuple ends nothing.
        assertThat(store.commit(cat, end(listOf("1", null))).endedFiles).isEqualTo(1)
        assertThat(live()).containsExactlyInAnyOrder("1-2", "n-n")
        assertThat(store.commit(cat, end(listOf("1", "9"), listOf(null, null))).endedFiles).isEqualTo(1)
        assertThat(live()).containsExactly("1-2")
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `string partition values with JSON-special characters are matched exactly`(backend: Backend) {
        select(backend)
        val cat = fixture()
        store.createTable(cat, "ns", "str", listOf(ColumnDef("k", ColType.STRING), ColumnDef("v", ColType.LONG)))
        alters.alterTable(
            cat,
            "ns",
            "str",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
        )
        val values =
            listOf(
                "quote\"d",
                "back\\slash",
                "caf\u00e9 \u2603 \uD83D\uDE00",
                "{\"k\":[1,2]}",
                "tab\there",
                "new\nline",
                "a,b]",
                "ctl\u0001",
                "",
            )
        store.commit(
            cat,
            CommitRequest(
                readSnapshot = head(cat),
                idempotencyKey = UUID.randomUUID(),
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "str",
                            values.mapIndexed { i, v ->
                                FileRegistration("s3://hoglake-it/$cat/str-$i", 1, 100, 50, partitionValues = listOf(v))
                            },
                        ),
                    ),
            ),
        )
        for ((i, v) in values.withIndex()) {
            val end = TableFileEnds("ns", "str", uuid(cat, "str"), partitions = listOf(listOf<String?>(v)))
            assertThat(
                store.commit(cat, replacement(cat, listOf(end))).endedFiles,
            ).describedAs("value #$i").isEqualTo(1)
        }
        assertThat(store.listFiles(cat, "ns", "str")).isEmpty()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a table partitioned after it had files refuses a partition end but not all_files`(backend: Backend) {
        select(backend)
        val cat = fixture()
        val legacy = FileRegistration("s3://hoglake-it/$cat/legacy", 3, 30, 10)
        store.commit(
            cat,
            CommitRequest(appends = listOf(TableAppend("ns", "plain", listOf(legacy)))),
        )
        alters.alterTable(cat, "ns", "plain", listOf(AlterOp.SetPartitionSpec(listOf(identityDay))))
        val tuple = TableFileEnds("ns", "plain", uuid(cat, "plain"), partitions = listOf(listOf("20401")))
        // Its one spec began after the file did, so the file is under no spec the tuple can address.
        assertThatThrownBy { store.commit(cat, replacement(cat, listOf(tuple))) }
            .isExactlyInstanceOf(HoglakeException.PartitionSpecMismatch::class.java)
        val all = TableFileEnds("ns", "plain", uuid(cat, "plain"), allFiles = true)
        assertThat(store.commit(cat, replacement(cat, listOf(all))).endedFiles).isEqualTo(1)
        // With no file under another spec left, the same tuple is accepted.
        assertThat(store.commit(cat, replacement(cat, listOf(tuple))).endedFiles).isZero()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a replacement that ended nothing, a DV delete and an append do not trip the changefeed`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"))
        val before = head(cat)
        addDv(cat, "d1")
        seed(cat, dayFile(cat, "d2", "20402"))
        store.commit(cat, replacement(cat, listOf(days(cat, "29999")), listOf(dayFile(cat, "d9", "29999"))))
        // None of these ended a data file, so the feed carries them without a barrier.
        assertThat(store.changes(cat, "ns", "ev", before).files.map { it.path.substringAfterLast('/') })
            .containsExactlyInAnyOrder("d2", "d9")
        val snapshot = store.listSnapshots(cat, head(cat) - 1, 10).first.single()
        assertThat(snapshot.changes.map { it.kind }).doesNotContain(ChangeKind.TABLE_REPLACED)
        // One that ended a file does, however old the window.
        val ended = store.commit(cat, replacement(cat, listOf(days(cat, "20402"))))
        assertThat(ended.endedFiles).isEqualTo(1)
        assertThatThrownBy { store.changes(cat, "ns", "ev", before) }
            .isExactlyInstanceOf(HoglakeException.ReconciliationRequired::class.java)
        assertThatThrownBy { store.changes(cat, "ns", "ev", ended.snapshotId - 1) }
            .isExactlyInstanceOf(HoglakeException.ReconciliationRequired::class.java)
        assertThat(store.changes(cat, "ns", "ev", ended.snapshotId).files).isEmpty()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `the receipt keeps the ended count for a replay and the receipt lookup`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"), dayFile(cat, "d2", "20401"))
        val request = replacement(cat, listOf(days(cat, "20401")), listOf(dayFile(cat, "n", "20401")))
        val first = store.commit(cat, request)
        assertThat(first.endedFiles).isEqualTo(2)
        assertThat(store.commit(cat, request).endedFiles).isEqualTo(2)
        assertThat(store.receipt(cat, request.idempotencyKey!!).endedFiles).isEqualTo(2)
        // A receipt of any other commit carries no count.
        val plainKey = UUID.randomUUID()
        store.commit(
            cat,
            CommitRequest(
                readSnapshot = head(cat),
                idempotencyKey = plainKey,
                appends = listOf(TableAppend("ns", "ev", listOf(dayFile(cat, "q", "20407")))),
            ),
        )
        assertThat(store.receipt(cat, plainKey).endedFiles).isNull()
        // A replacement that ended nothing records zero, not absence.
        val noop = replacement(cat, listOf(days(cat, "29999")))
        assertThat(store.commit(cat, noop).endedFiles).isZero()
        assertThat(store.receipt(cat, noop.idempotencyKey!!).endedFiles).isZero()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `time travel to before a replacement reads the old files, the old DVs and the old totals`(backend: Backend) {
        select(backend)
        val cat = fixture()
        seed(cat, dayFile(cat, "d1", "20401"), dayFile(cat, "d2", "20402"))
        addDv(cat, "d1")
        val before = head(cat)
        val after = store.commit(cat, replacement(cat, listOf(days(cat, "20401")), listOf(dayFile(cat, "n", "20401"))))
        assertThat(livePaths(cat, before)).containsExactlyInAnyOrder("d1", "d2")
        assertThat(livePaths(cat, after.snapshotId)).containsExactlyInAnyOrder("d2", "n")
        assertThat(store.getTable(cat, "ns", "ev", snapshot = before).fileCount).isEqualTo(2)
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

        // Mixed specs: a file written under the new one does not make the old one addressable.
        seed(cat, FileRegistration("s3://hoglake-it/$cat/new", 4, 40, 10, partitionValues = listOf("7")))
        val mixed = state(cat)
        val newTuple = TableFileEnds("ns", "ev", uuid(cat), partitions = listOf(listOf("7")))
        assertThatThrownBy { store.commit(cat, replacement(cat, listOf(newTuple))) }
            .isExactlyInstanceOf(HoglakeException.PartitionSpecMismatch::class.java)
        assertThat(state(cat)).isEqualTo(mixed)

        val result = store.commit(cat, replacement(cat, listOf(all(cat))))
        assertThat(result.endedFiles).isEqualTo(2)
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
