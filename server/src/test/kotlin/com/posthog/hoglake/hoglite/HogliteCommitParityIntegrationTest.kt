package com.posthog.hoglake.hoglite

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.service.UploadBackend
import com.posthog.hoglake.service.UploadService
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

/** Shared commit assertions use PostgreSQL as the contract reference. Neither backend mocks publication. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteCommitParityIntegrationTest {
    enum class Backend { POSTGRES, HOGLITE }

    private var pg: PgTestSupport.TestDb? = null
    private var live: HogliteConformanceEngine? = null
    private lateinit var selected: Backend
    private lateinit var store: CatalogStore
    private lateinit var alters: AlterBackend
    private lateinit var uploads: UploadBackend

    private fun select(backend: Backend) {
        selected = backend
        when (backend) {
            Backend.POSTGRES -> {
                val db = pg ?: PgTestSupport.freshDatabase().also { pg = it }
                store = PostgresCatalogStore(CatalogService(db.jdbi), CommitService(db.jdbi), ScanService(db.jdbi))
                alters = AlterService(db.jdbi)
                uploads = UploadService(db.jdbi)
            }
            Backend.HOGLITE -> {
                val engine = live ?: HogliteConformanceEngine().also { live = it }
                store = engine.store
                alters = HogliteAlters(engine.store)
                uploads = engine.store.uploads
            }
        }
    }

    @AfterAll
    fun close() {
        pg?.close()
        live?.close()
    }

    private fun fixture(): String {
        val cat = "parity" + UUID.randomUUID().toString().replace("-", "")
        store.createCatalog(cat, "s3://hoglake-it/$cat")
        store.createNamespace(cat, "ns")
        store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG), ColumnDef("extra", ColType.LONG)))
        return cat
    }

    private fun head(cat: String) = store.getCatalog(cat).headSnapshotId

    private fun file(
        cat: String,
        name: String = "data",
    ) = FileRegistration("s3://hoglake-it/$cat/$name", 10, 100, 50)

    private fun request(
        cat: String,
        file: FileRegistration = file(cat),
        read: Long? = head(cat),
    ) = CommitRequest(
        readSnapshot = read,
        idempotencyKey = UUID.randomUUID(),
        appends = listOf(TableAppend("ns", "t", listOf(file))),
    )

    private fun publish(
        cat: String,
        request: CommitRequest,
    ): com.posthog.hoglake.model.CommitResult {
        val result = runCatching { store.commit(cat, request) }
        assertThat(result.exceptionOrNull()).isNull()
        return result.getOrThrow()
    }

    // These fixture adapters normalize only storage names. Assertions remain identical for both backends.
    private fun number(
        cat: String,
        lite: String,
        postgres: String,
    ): Long =
        when (selected) {
            Backend.POSTGRES ->
                pg!!.jdbi.withHandle<Long, Exception> { h ->
                    h.createQuery(
                        postgres.replace(":catalog", store.getCatalog(cat).catalogId.toString()),
                    ).mapTo(Long::class.java).one()
                }
            Backend.HOGLITE ->
                live!!.store.catalogOperation(cat) {
                    live!!.store.queryCatalog(cat, lite).firstRow()!![0].asLong()
                }
        }

    private fun write(
        cat: String,
        lite: String,
        postgres: String,
    ) {
        when (selected) {
            Backend.POSTGRES ->
                pg!!.jdbi.useHandle<Exception> { h ->
                    h.execute(postgres.replace(":catalog", store.getCatalog(cat).catalogId.toString()))
                }
            Backend.HOGLITE ->
                live!!.store.catalogOperation(cat) {
                    live!!.store.runTx(
                        cat,
                        HogliteCatalogStore.freshTxid("fixture"),
                        null,
                        HogliteTx().apply { add(lite) },
                    )
                }
        }
    }

    private fun state(cat: String) =
        listOf(
            head(cat),
            number(
                cat,
                "SELECT next_file_id FROM meta",
                "SELECT next_file_id FROM hog_catalog WHERE catalog_id = :catalog",
            ),
            number(
                cat,
                "SELECT COALESCE(SUM(next_row_id),0) FROM tables",
                "SELECT COALESCE(SUM(next_row_id),0) FROM hog_table_stats WHERE catalog_id = :catalog",
            ),
            number(cat, "SELECT COUNT(*) FROM files", "SELECT COUNT(*) FROM hog_data_file WHERE catalog_id = :catalog"),
            number(cat, "SELECT COUNT(*) FROM dvs", "SELECT COUNT(*) FROM hog_delete_file WHERE catalog_id = :catalog"),
            number(
                cat,
                "SELECT COUNT(*) FROM receipts",
                "SELECT COUNT(*) FROM hog_commit_receipt WHERE catalog_id = :catalog",
            ),
            number(
                cat,
                "SELECT COUNT(*) FROM uploads WHERE state = 'registered'",
                "SELECT COUNT(*) FROM hog_upload WHERE catalog_id = :catalog AND state = 'registered'",
            ),
        )

    private fun ddl(
        cat: String,
        read: Long,
        request: CommitRequest,
    ) {
        val before = state(cat)
        val error = runCatching { store.commit(cat, request) }.exceptionOrNull()
        assertThat(error).isExactlyInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
        error as HoglakeException.DdlSinceReadSnapshot
        assertThat(error.tables).containsExactly("ns.t")
        assertThat(error.readSnapshot).isEqualTo(read)
        assertThat(state(cat)).isEqualTo(before)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `dropped field stats report the DDL cause before structural validation`(backend: Backend) {
        select(backend)
        val cat = fixture()
        val read = head(cat)
        val stale =
            request(cat, file(cat).copy(columnStats = listOf(ColumnStats(2, 10, 0, null, null, null, null))), read)
        alters.alterTable(cat, "ns", "t", listOf(AlterOp.DropColumn("extra")))
        ddl(cat, read, stale)
        val before = state(cat)
        assertThatThrownBy { store.commit(cat, stale.copy(readSnapshot = head(cat))) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
        assertThat(state(cat)).isEqualTo(before)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `partition declaration changes precede stale values but fresh malformed values still refuse`(backend: Backend) {
        select(backend)
        val identity = PartitionFieldDef(1, Transform.IDENTITY)
        val bucket = PartitionFieldDef(1, Transform.BUCKET, 8)
        val extra = PartitionFieldDef(2, Transform.IDENTITY)
        for ((old, new) in listOf(
            emptyList<PartitionFieldDef>() to listOf(identity),
            listOf(identity) to emptyList(),
            listOf(identity) to listOf(identity, extra),
            listOf(identity) to listOf(bucket),
        )) {
            val cat = fixture()
            if (old.isNotEmpty()) alters.alterTable(cat, "ns", "t", listOf(AlterOp.SetPartitionSpec(old)))
            val read = head(cat)
            val stale =
                request(cat, file(cat).copy(partitionValues = old.takeIf { it.isNotEmpty() }?.map { "1" }), read)
            alters.alterTable(cat, "ns", "t", listOf(AlterOp.SetPartitionSpec(new)))
            ddl(cat, read, stale)
            val before = state(cat)
            val malformed =
                stale.copy(
                    readSnapshot = head(cat),
                    appends =
                        listOf(
                            TableAppend("ns", "t", listOf(file(cat).copy(partitionValues = listOf("1", "2", "3")))),
                        ),
                )
            assertThatThrownBy {
                store.commit(
                    cat,
                    malformed,
                )
            }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
            assertThat(state(cat)).isEqualTo(before)
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `DDL precedes foreign and abandoned upload claims`(backend: Backend) {
        select(backend)
        for (abandoned in listOf(false, true)) {
            val cat = fixture()
            val owner = UUID.randomUUID()
            val claim = uploads.claim(cat, UUID.randomUUID(), owner, "s3://hoglake-it/$cat/up", "data")
            if (abandoned) uploads.abandon(cat, owner, listOf(claim.path))
            val read = head(cat)
            alters.alterTable(cat, "ns", "t", listOf(AlterOp.AddColumn(ColumnDef("later", ColType.LONG))))
            val req = request(cat, file(cat).copy(path = claim.path), read)
            ddl(cat, read, req)
            val before = state(cat)
            assertThatThrownBy { store.commit(cat, req.copy(readSnapshot = head(cat))) }
                .isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
            assertThat(state(cat)).isEqualTo(before)
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `mixed DDL and content windows remain ordinary commit conflicts`(backend: Backend) {
        select(backend)
        val cat = fixture()
        store.commit(cat, request(cat))
        val target = store.listFiles(cat, "ns", "t").single().dataFileId
        val table = store.getTable(cat, "ns", "t")
        val read = head(cat)
        store.commit(cat, request(cat, file(cat, "concurrent")))
        alters.alterTable(cat, "ns", "t", listOf(AlterOp.DropColumn("extra")))
        val req =
            request(
                cat,
                file(cat, "stale").copy(columnStats = listOf(ColumnStats(2, 10, 0, null, null, null, null))),
                read,
            ).copy(
                requireUnchangedTables = true,
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(
                                file(
                                    cat,
                                    "stale",
                                ).copy(columnStats = listOf(ColumnStats(2, 10, 0, null, null, null, null))),
                            ),
                            table.tableUuid,
                        ),
                    ),
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(DeleteFileRegistration(target, file(cat, "dv").path, 1, 10)),
                            table.tableUuid,
                        ),
                    ),
            )
        val before = state(cat)
        assertThatThrownBy { store.commit(cat, req) }.isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(state(cat)).isEqualTo(before)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `duplicate append paths preserve every registration span total and owned claim`(backend: Backend) {
        select(backend)
        for (merged in listOf(false, true)) {
            val cat = fixture()
            val owner = UUID.randomUUID()
            val claim = uploads.claim(cat, UUID.randomUUID(), owner, "s3://hoglake-it/$cat/up", "data")
            val first = file(cat).copy(path = claim.path, recordCount = 3)
            val second = first.copy(recordCount = 7, fileSizeBytes = 200)
            val groups =
                if (merged) {
                    listOf(
                        TableAppend("ns", "t", listOf(first)),
                        TableAppend("ns", "t", listOf(second)),
                    )
                } else {
                    listOf(TableAppend("ns", "t", listOf(first, second)))
                }
            val req = request(cat).copy(idempotencyKey = owner, appends = groups)
            val result = publish(cat, req)
            val files = store.listFiles(cat, "ns", "t").sortedBy { it.rowIdStart }
            assertThat(files.map { it.path }).containsExactly(claim.path, claim.path)
            assertThat(files.map { it.dataFileId }.distinct()).hasSize(2)
            assertThat(files.map { it.rowIdStart }).containsExactly(0L, 3L)
            assertThat(files.map { it.recordCount }).containsExactly(3L, 7L)
            assertThat(files.map { it.beginSnapshot }).containsOnly(result.snapshotId)
            val totals = store.getTable(cat, "ns", "t", snapshot = result.snapshotId)
            assertThat(totals.recordCount).isEqualTo(10)
            assertThat(totals.fileSizeBytes).isEqualTo(300)
            val after = state(cat)
            assertThat(after[1]).isEqualTo(3)
            assertThat(after[2]).isEqualTo(10)
            assertThat(after[6]).isEqualTo(1)
            assertThat(store.commit(cat, req)).isEqualTo(result)
            assertThat(state(cat)).isEqualTo(after)
            // A registered, retained same-kind path remains reusable by another operation.
            store.commit(cat, req.copy(idempotencyKey = UUID.randomUUID(), readSnapshot = head(cat)))
            assertThat(store.listFiles(cat, "ns", "t")).hasSize(4)
            assertThat(state(cat)[6]).isEqualTo(1)
        }
    }

    private fun stagedRequest(
        cat: String,
        duplicate: Boolean,
        otherTable: Boolean = false,
    ): CommitRequest {
        val t = store.getTable(cat, "ns", "t")
        val p = file(cat)
        val appends = mutableListOf(TableAppend("ns", "t", if (duplicate) listOf(p, p) else listOf(p), t.tableUuid))
        if (otherTable) appends += TableAppend("ns", "u", listOf(p), store.getTable(cat, "ns", "u").tableUuid)
        return CommitRequest(
            idempotencyKey = UUID.randomUUID(),
            readSnapshot = head(cat),
            allowPendingDeletes = true,
            requireUnchangedTables = true,
            appends = appends,
            deletes =
                appends.map {
                    TableDeletes(
                        "ns",
                        it.table,
                        listOf(
                            DeleteFileRegistration(0, file(cat, "${it.table}.dv").path, 1, 10, dataFilePath = p.path),
                        ),
                        it.expectedTableUuid,
                    )
                },
        )
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `ambiguous pending targets refuse atomically while same path in different tables binds locally`(
        backend: Backend,
    ) {
        select(backend)
        val cat = fixture()
        val before = state(cat)
        assertThatThrownBy { store.commit(cat, stagedRequest(cat, duplicate = true)) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessage("pending delete must target exactly one same-table append in this commit")
        assertThat(state(cat)).isEqualTo(before)
        store.createTable(cat, "ns", "u", listOf(ColumnDef("id", ColType.LONG)))
        val result = publish(cat, stagedRequest(cat, duplicate = false, otherTable = true))
        val ids = mutableSetOf<Long>()
        for (table in listOf("t", "u")) {
            val scan = store.planScan(cat, "ns", table).single()
            assertThat(scan.dataFile.path).isEqualTo(file(cat).path)
            assertThat(scan.deleteFile!!.path).isEqualTo(file(cat, "$table.dv").path)
            assertThat(scan.dataFile.beginSnapshot).isEqualTo(result.snapshotId)
            ids += scan.dataFile.dataFileId
        }
        assertThat(ids).hasSize(2)
        val unique = fixture()
        val valid = stagedRequest(unique, duplicate = false)
        val beforeUnique = state(unique)
        assertThatThrownBy { store.commit(unique, valid.copy(deletes = valid.deletes + valid.deletes)) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
        assertThat(state(unique)).isEqualTo(beforeUnique)
        publish(unique, valid)
        assertThat(store.planScan(unique, "ns", "t").single().deleteFile!!.deleteCount).isEqualTo(1)
    }

    private fun rows(
        cat: String,
        lite: String,
        postgres: String,
    ): String =
        when (selected) {
            Backend.POSTGRES ->
                pg!!.jdbi.withHandle<String, Exception> { h ->
                    h.createQuery(postgres.replace(":catalog", store.getCatalog(cat).catalogId.toString()))
                        .mapToMap().list().toString()
                }
            Backend.HOGLITE ->
                live!!.store.catalogOperation(cat) {
                    live!!.store.queryCatalog(cat, lite).rows.toString()
                }
        }

    private fun persistentState(cat: String) =
        listOf(
            state(cat),
            rows(cat, "SELECT * FROM meta", "SELECT * FROM hog_catalog WHERE catalog_id = :catalog"),
            rows(
                cat,
                "SELECT * FROM tables ORDER BY id",
                "SELECT * FROM hog_table WHERE catalog_id = :catalog ORDER BY table_id",
            ),
            rows(
                cat,
                "SELECT * FROM files ORDER BY id",
                "SELECT * FROM hog_data_file WHERE catalog_id = :catalog ORDER BY data_file_id",
            ),
            rows(
                cat,
                "SELECT * FROM dvs ORDER BY data_file_id,epoch",
                "SELECT * FROM hog_delete_file WHERE catalog_id = :catalog ORDER BY delete_file_id",
            ),
            rows(
                cat,
                "SELECT * FROM receipts ORDER BY idempotency_key",
                "SELECT * FROM hog_commit_receipt WHERE catalog_id = :catalog ORDER BY idempotency_key",
            ),
            rows(
                cat,
                "SELECT * FROM uploads ORDER BY path",
                "SELECT * FROM hog_upload WHERE catalog_id = :catalog ORDER BY path",
            ),
            rows(
                cat,
                "SELECT * FROM removals ORDER BY path",
                "SELECT * FROM hog_file_removal WHERE catalog_id = :catalog ORDER BY path",
            ),
        )

    private fun protectedAmbiguity(
        kind: String,
        mode: String,
    ): Pair<String, CommitRequest> {
        val cat = fixture()
        val owner = UUID.randomUUID()
        val path =
            if (mode == "foreign" || mode == "abandoned") {
                uploads.claim(cat, UUID.randomUUID(), owner, "s3://hoglake-it/$cat/up", kind).path
            } else {
                file(cat, "protected-$kind").path
            }
        if (mode == "abandoned") uploads.abandon(cat, owner, listOf(path))
        if (mode == "removal" || mode == "claimed-removal") {
            val claimed = mode == "claimed-removal"
            write(
                cat,
                "INSERT INTO removals(path,file_kind,reason,queued_snapshot,queued_at," +
                    "claimed_at,claimed_by) " +
                    "VALUES ('$path','$kind','snapshot_expiry',0,'2026-11-03'," +
                    "${if (claimed) "'2026-11-03','worker'" else "NULL,NULL"})",
                "INSERT INTO hog_file_removal(catalog_id,path,file_kind,reason,claimed_at,claimed_by) " +
                    "VALUES (:catalog,'$path','$kind','snapshot_expiry'," +
                    "${if (claimed) "now(),'worker'" else "NULL,NULL"})",
            )
        }
        val base = stagedRequest(cat, duplicate = true)
        val dataPath = if (kind == "data") path else file(cat).path
        val req =
            base.copy(
                appends =
                    base.appends.map {
                            a ->
                        a.copy(files = a.files.map { it.copy(path = dataPath) })
                    },
                deletes =
                    base.deletes.map { d ->
                        d.copy(
                            files =
                                d.files.map {
                                    it.copy(
                                        dataFilePath = dataPath,
                                        path = if (kind == "delete") path else it.path,
                                    )
                                },
                        )
                    },
            )
        return cat to req
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `ambiguous pending targets preserve claim and removal conflict precedence`(backend: Backend) {
        select(backend)
        org.assertj.core.api.SoftAssertions.assertSoftly { softly ->
            for (kind in listOf("data", "delete")) {
                for (mode in listOf("foreign", "abandoned", "removal", "claimed-removal")) {
                    val (cat, req) = protectedAmbiguity(kind, mode)
                    val before = persistentState(cat)
                    val error = runCatching { store.commit(cat, req) }.exceptionOrNull()
                    softly.assertThat(
                        error,
                    ).describedAs("$kind $mode").isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
                    softly.assertThat(persistentState(cat)).describedAs("$kind $mode state").isEqualTo(before)
                    alters.alterTable(cat, "ns", "t", listOf(AlterOp.AddColumn(ColumnDef("later", ColType.LONG))))
                    val afterDdl = persistentState(cat)
                    val stale = runCatching { store.commit(cat, req) }.exceptionOrNull()
                    softly.assertThat(
                        stale,
                    ).describedAs(
                        "$kind $mode DDL",
                    ).isExactlyInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
                    if (stale is HoglakeException.DdlSinceReadSnapshot) {
                        softly.assertThat(stale.tables).containsExactly("ns.t")
                        softly.assertThat(stale.readSnapshot).isEqualTo(req.readSnapshot)
                    }
                    softly.assertThat(persistentState(cat)).isEqualTo(afterDdl)
                }
            }
        }
        val unprotected = fixture()
        val before = persistentState(unprotected)
        assertThatThrownBy { store.commit(unprotected, stagedRequest(unprotected, duplicate = true)) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessage("pending delete must target exactly one same-table append in this commit")
        assertThat(persistentState(unprotected)).isEqualTo(before)
        val malformed = stagedRequest(unprotected, duplicate = true)
        assertThatThrownBy {
            store.commit(
                unprotected,
                malformed.copy(
                    deletes =
                        malformed.deletes.map { d ->
                            d.copy(files = d.files.map { it.copy(fileSizeBytes = -1) })
                        },
                ),
            )
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessage("negative file_size_bytes for delete of data_file_id 0 in ns.t")
        assertThat(persistentState(unprotected)).isEqualTo(before)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `staged count validation retains its target position around ambiguity`(backend: Backend) {
        select(backend)
        org.assertj.core.api.SoftAssertions.assertSoftly { softly ->
            for (countFirst in listOf(true, false)) {
                val cat = fixture()
                val base = stagedRequest(cat, duplicate = true)
                val counted = file(cat, "counted").copy(recordCount = 2)
                val countDv =
                    base.deletes.single().files.single().copy(
                        path = counted.path + ".dv",
                        dataFilePath = counted.path,
                        deleteCount = 3,
                    )
                val pending = base.deletes.single().files.single()
                val registrations = if (countFirst) listOf(countDv, pending) else listOf(pending, countDv)
                val req =
                    base.copy(
                        appends = base.appends.map { it.copy(files = it.files + counted) },
                        deletes = registrations.map { base.deletes.single().copy(files = listOf(it)) },
                    )
                val before = persistentState(cat)
                val error = runCatching { store.commit(cat, req) }.exceptionOrNull()
                softly.assertThat(error).isExactlyInstanceOf(HoglakeException.Validation::class.java)
                if (countFirst) {
                    softly.assertThat(error).hasMessageContaining("delete_count 3 exceeds record_count")
                } else {
                    softly.assertThat(
                        error,
                    ).hasMessage("pending delete must target exactly one same-table append in this commit")
                }
                softly.assertThat(persistentState(cat)).isEqualTo(before)
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `duplicate ambiguous targets remain registration validation before path protection`(backend: Backend) {
        select(backend)
        org.assertj.core.api.SoftAssertions.assertSoftly { softly ->
            for (kind in listOf("data", "delete")) {
                for (mode in listOf("foreign", "abandoned", "removal", "claimed-removal", "unprotected")) {
                    val (cat, single) = protectedAmbiguity(kind, mode)
                    val duplicate =
                        single.copy(
                            deletes =
                                single.deletes.map { d ->
                                    val first = d.files.single()
                                    d.copy(files = listOf(first, first.copy(path = first.path + ".second")))
                                },
                        )
                    val before = persistentState(cat)
                    val error = runCatching { store.commit(cat, duplicate) }.exceptionOrNull()
                    softly.assertThat(error).describedAs("$kind $mode duplicate target")
                        .isExactlyInstanceOf(HoglakeException.Validation::class.java)
                        .hasMessageNotContaining("exactly one same-table append")
                    softly.assertThat(persistentState(cat)).isEqualTo(before)
                }
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `duplicate durable DV paths are legal but duplicate targets and mixed kinds refuse`(backend: Backend) {
        select(backend)
        val cat = fixture()
        store.commit(
            cat,
            request(cat).copy(appends = listOf(TableAppend("ns", "t", listOf(file(cat, "a"), file(cat, "b"))))),
        )
        val targets = store.listFiles(cat, "ns", "t").sortedBy { it.dataFileId }
        val shared = file(cat, "shared.dv").path
        val dv = targets.map { DeleteFileRegistration(it.dataFileId, shared, 1, 10) }
        val req =
            CommitRequest(
                readSnapshot = head(cat),
                idempotencyKey = UUID.randomUUID(),
                deletes = listOf(TableDeletes("ns", "t", dv)),
            )
        val before = state(cat)
        assertThatThrownBy {
            store.commit(
                cat,
                req.copy(deletes = listOf(TableDeletes("ns", "t", listOf(dv[0], dv[0])))),
            )
        }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy {
            store.commit(cat, req.copy(appends = listOf(TableAppend("ns", "t", listOf(file(cat).copy(path = shared))))))
        }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
        assertThat(state(cat)).isEqualTo(before)
        publish(cat, req)
        val scan = store.planScan(cat, "ns", "t")
        assertThat(scan.map { it.deleteFile!!.path }).containsExactly(shared, shared)
        assertThat(scan.map { it.deleteFile!!.deleteFileId }.distinct()).hasSize(2)
        assertThat(state(cat)[4]).isEqualTo(2)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `duplicate append paths retain foreign abandoned and wrong kind claim guards`(backend: Backend) {
        select(backend)
        for (mode in listOf("foreign", "abandoned", "wrong-kind")) {
            val cat = fixture()
            val owner = UUID.randomUUID()
            val claim =
                uploads.claim(
                    cat,
                    UUID.randomUUID(),
                    owner,
                    "s3://hoglake-it/$cat/up",
                    if (mode == "wrong-kind") "delete" else "data",
                )
            if (mode == "abandoned") uploads.abandon(cat, owner, listOf(claim.path))
            val p = file(cat).copy(path = claim.path)
            val req =
                request(
                    cat,
                ).copy(
                    idempotencyKey = if (mode == "foreign") UUID.randomUUID() else owner,
                    appends = listOf(TableAppend("ns", "t", listOf(p, p))),
                )
            val before = state(cat)
            assertThatThrownBy {
                store.commit(
                    cat,
                    req,
                )
            }.isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
            assertThat(state(cat)).isEqualTo(before)
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `duplicate append paths refuse all undrained removals including claimed rows`(backend: Backend) {
        select(backend)
        for (claimed in listOf(false, true)) {
            val cat = fixture()
            val p = file(cat)
            write(
                cat,
                "INSERT INTO removals(path,file_kind,reason,queued_snapshot,queued_at,claimed_at,claimed_by) " +
                    "VALUES ('${p.path}','data','snapshot_expiry',0,'2026-11-03'," +
                    "${if (claimed) "'2026-11-03','worker'" else "NULL,NULL"})",
                "INSERT INTO hog_file_removal(catalog_id,path,file_kind,reason,claimed_at,claimed_by) " +
                    "VALUES (:catalog,'${p.path}','data','snapshot_expiry'," +
                    "${if (claimed) "now(),'worker'" else "NULL,NULL"})",
            )
            val req = request(cat).copy(appends = listOf(TableAppend("ns", "t", listOf(p, p))))
            val before = state(cat)
            assertThatThrownBy {
                store.commit(
                    cat,
                    req,
                )
            }.isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
            assertThat(state(cat)).isEqualTo(before)
            write(
                cat,
                "UPDATE removals SET drained_at = '2026-11-03', drained_outcome = 'absent'",
                "UPDATE hog_file_removal SET drained_at = now(), drained_outcome = 'absent' " +
                    "WHERE catalog_id = :catalog",
            )
            publish(cat, req)
            assertThat(store.listFiles(cat, "ns", "t")).hasSize(2)
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `every UUID expectation resolves against the live table in request order`(backend: Backend) {
        select(backend)
        for (kind in listOf("append", "delete", "mixed")) {
            for (reverse in listOf(false, true)) {
                val cat = fixture()
                store.commit(cat, request(cat))
                val target = store.listFiles(cat, "ns", "t").single().dataFileId
                val current = store.getTable(cat, "ns", "t").tableUuid
                val stale = UUID.randomUUID()
                val expectations = if (reverse) listOf(stale, current) else listOf(current, stale)
                val a = expectations.mapIndexed { i, uuid -> TableAppend("ns", "t", listOf(file(cat, "new-$i")), uuid) }
                val d =
                    expectations.map {
                            uuid ->
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(DeleteFileRegistration(target, file(cat, "dv").path, 1, 10)),
                            uuid,
                        )
                    }
                val req =
                    request(cat).copy(
                        appends =
                            when (kind) {
                                "append" -> a
                                "mixed" -> listOf(a[0])
                                else -> emptyList()
                            },
                        deletes =
                            when (kind) {
                                "delete" -> d
                                "mixed" -> listOf(d[1])
                                else -> emptyList()
                            },
                    )
                val before = state(cat)
                val error = runCatching { store.commit(cat, req) }.exceptionOrNull()
                assertThat(error).isExactlyInstanceOf(HoglakeException.TableRecreated::class.java)
                error as HoglakeException.TableRecreated
                assertThat(error.table).isEqualTo("ns.t")
                assertThat(error.expectedTableUuid).isEqualTo(stale)
                assertThat(error.currentTableUuid).isEqualTo(current)
                assertThat(state(cat)).isEqualTo(before)
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `null equal stale and missing UUID controls preserve resolution precedence`(backend: Backend) {
        select(backend)
        val cat = fixture()
        val current = store.getTable(cat, "ns", "t").tableUuid
        for (pair in listOf(
            listOf(null, current),
            listOf(current, null),
            listOf(current, current),
            listOf(null, null),
        )) {
            store.commit(
                cat,
                request(cat).copy(
                    appends =
                        pair.mapIndexed {
                                i,
                                uuid,
                            ->
                            TableAppend("ns", "t", listOf(file(cat, "control-$i")), uuid)
                        },
                ),
            )
        }
        val stale = listOf(UUID.randomUUID(), UUID.randomUUID())
        for (expectations in listOf(stale, stale.reversed())) {
            val req = request(cat).copy(appends = expectations.map { TableAppend("ns", "t", listOf(file(cat)), it) })
            val before = state(cat)
            val error = runCatching { store.commit(cat, req) }.exceptionOrNull()
            assertThat(error).isExactlyInstanceOf(HoglakeException.TableRecreated::class.java)
            assertThat((error as HoglakeException.TableRecreated).expectedTableUuid).isEqualTo(expectations.first())
            assertThat(error.currentTableUuid).isEqualTo(current)
            assertThat(state(cat)).isEqualTo(before)
        }
        // Resolve the missing name before comparing its supplied expectations.
        val missing = request(cat).copy(appends = stale.map { TableAppend("ns", "absent", listOf(file(cat)), it) })
        val beforeMissing = state(cat)
        assertThatThrownBy { store.commit(cat, missing) }
            .isNotInstanceOf(HoglakeException.TableRecreated::class.java)
            .isNotInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("absent")
        assertThat(state(cat)).isEqualTo(beforeMissing)
        store.dropTable(cat, "ns", "t", current, "test", "drop")
        val before = state(cat)
        assertThatThrownBy {
            store.commit(cat, missing.copy(appends = stale.map { TableAppend("ns", "t", listOf(file(cat)), it) }))
        }
            .isExactlyInstanceOf(HoglakeException.TableDropped::class.java)
        assertThat(state(cat)).isEqualTo(before)
    }
}
