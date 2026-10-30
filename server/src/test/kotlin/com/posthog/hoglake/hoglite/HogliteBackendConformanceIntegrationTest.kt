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
import com.posthog.hoglake.model.PartitionLookupInvalidRequest
import com.posthog.hoglake.model.StatsState
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ExpiryBackend
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.OptionsBackend
import com.posthog.hoglake.service.OptionsService
import com.posthog.hoglake.service.PartitionLookupBackend
import com.posthog.hoglake.service.PartitionLookupService
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.service.TableCreationBackend
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationService
import com.posthog.hoglake.service.ViewBackend
import com.posthog.hoglake.service.ViewService
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
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.util.UUID

/**
 * One caller contract, two real implementations. Assertions below MUST NOT branch
 * on backend. Add a case here when adding a service guard, not a backend-only twin.
 * PostgreSQL uses the ordinary migrated Testcontainers fixture; hoglite uses the
 * release binary via HOGLITE_BINARY. See HogliteConformance.md for scope/exclusions.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteBackendConformanceIntegrationTest {
    enum class Backend { POSTGRES, HOGLITE }

    private var pg: PgTestSupport.TestDb? = null
    private var live: HogliteConformanceEngine? = null
    private lateinit var store: CatalogStore
    private lateinit var alters: AlterBackend
    private lateinit var creations: TableCreationBackend
    private lateinit var views: ViewBackend
    private lateinit var options: OptionsBackend
    private lateinit var expiry: ExpiryBackend
    private lateinit var lookup: PartitionLookupBackend
    private lateinit var selected: Backend

    private fun engine(): HogliteConformanceEngine = live ?: HogliteConformanceEngine().also { live = it }

    private fun select(backend: Backend) {
        selected = backend
        when (backend) {
            Backend.POSTGRES -> {
                val db = pg ?: PgTestSupport.freshDatabase().also { pg = it }
                val catalogs = CatalogService(db.jdbi)
                val commits = CommitService(db.jdbi)
                store = PostgresCatalogStore(catalogs, commits, ScanService(db.jdbi))
                alters = AlterService(db.jdbi)
                creations = TableCreationService(db.jdbi, catalogs, commits)
                views = ViewService(db.jdbi)
                options = OptionsService(db.jdbi)
                expiry = ExpiryService(db.jdbi)
                lookup = PartitionLookupService(db.jdbi)
            }
            Backend.HOGLITE -> {
                val s = engine().store
                store = s
                alters = HogliteAlters(s)
                creations = HogliteTableCreations(s)
                views = HogliteViews(s)
                options = HogliteOptions(s)
                expiry = HogliteExpiry(s, purgePage = 100, purgeBudgetMs = 10_000)
                lookup = HoglitePartitionLookup(s)
            }
        }
    }

    @AfterAll
    fun close() {
        pg?.close()
        live?.close()
    }

    private fun freshCatalog(): String {
        val name = "contract" + UUID.randomUUID().toString().replace("-", "")
        store.createCatalog(name, "s3://hoglake-it/$name")
        return name
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `catalog namespace table lifecycle`(backend: Backend) {
        select(backend)
        val catalog = freshCatalog()
        // a same-name create is the 409, whatever the data_path.
        assertThatThrownBy { store.createCatalog(catalog, "s3://hoglake-it/$catalog") }
            .isExactlyInstanceOf(HoglakeException.AlreadyExists::class.java)
        assertThatThrownBy { store.createCatalog(catalog, "s3://hoglake-it/other-$catalog") }
            .isExactlyInstanceOf(HoglakeException.AlreadyExists::class.java)
        // prefix overlap with an existing catalog's data_path is refused.
        assertThatThrownBy { store.createCatalog("${catalog}sub", "s3://hoglake-it/$catalog/sub") }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)

        assertThat(store.getCatalog(catalog).headSnapshotId).isEqualTo(0)
        assertThat(store.listCatalogs().map { it.name }).contains(catalog)

        val ns = store.createNamespace(catalog, "ns")
        assertThat(store.getNamespace(catalog, "ns").namespaceId).isEqualTo(ns.namespaceId)
        assertThatThrownBy { store.createNamespace(catalog, "ns") }
            .isExactlyInstanceOf(HoglakeException.AlreadyExists::class.java)

        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        assertThat(table.snapshotId).isNotNull()
        assertThat(store.getTable(catalog, "ns", "t").tableUuid).isEqualTo(table.tableUuid)
        assertThat(store.listTables(catalog, "ns").map { it.name }).containsExactly("t")

        store.dropTable(catalog, "ns", "t", expectedTableUuid = table.tableUuid, author = "test", message = "drop")
        assertThatThrownBy { store.getTable(catalog, "ns", "t") }
            .isExactlyInstanceOf(HoglakeException.NotFound::class.java)
        // time travel: the table is still visible at its live snapshot.
        assertThat(
            store.getTable(catalog, "ns", "t", snapshot = table.snapshotId).tableUuid,
        ).isEqualTo(table.tableUuid)

        store.dropNamespace(catalog, "ns", author = "test", message = "drop")
        assertThat(store.listNamespaces(catalog)).isEmpty()

        store.dropCatalog(catalog, "test", "drop")
        assertThatThrownBy { store.getCatalog(catalog) }
            .isExactlyInstanceOf(HoglakeException.NotFound::class.java)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `commit appends land with row lineage and replay idempotently`(backend: Backend) {
        select(backend)
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val head = table.readSnapshotId

        val key = UUID.randomUUID()
        val request =
            CommitRequest(
                readSnapshot = head,
                idempotencyKey = key,
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(
                                FileRegistration("s3://hoglake-it/$catalog/data/a.parquet", 100, 1000, 500),
                                FileRegistration("s3://hoglake-it/$catalog/data/b.parquet", 50, 800, 400),
                            ),
                            expectedTableUuid = table.tableUuid,
                        ),
                    ),
            )
        val committed = store.commit(catalog, request)
        assertThat(committed.snapshotId).isEqualTo(head + 1)
        // The same key + payload replays the receipt; it does not re-land.
        val replayed = store.commit(catalog, request)
        assertThat(replayed.snapshotId).isEqualTo(committed.snapshotId)
        assertThat(store.getCatalog(catalog).headSnapshotId).isEqualTo(head + 1)
        assertThat(store.receipt(catalog, key).snapshotId).isEqualTo(committed.snapshotId)
        // The same key with a DIFFERENT payload is the 422.
        assertThatThrownBy {
            store.commit(
                catalog,
                request.copy(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration("s3://hoglake-it/$catalog/data/c.parquet", 1, 1, 1)),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)

        // Row lineage is server-assigned and contiguous.
        val files = store.listFiles(catalog, "ns", "t")
        assertThat(files.map { it.recordCount }).containsExactly(100, 50)
        assertThat(files.map { it.rowIdStart }).containsExactly(0L, 100L)
        val scan = store.planScan(catalog, "ns", "t")
        assertThat(scan.map { it.dataFile.path }).containsExactly(
            "s3://hoglake-it/$catalog/data/a.parquet",
            "s3://hoglake-it/$catalog/data/b.parquet",
        )
        // The changefeed carries the insert's files in the window.
        val changes = store.changes(catalog, "ns", "t", fromSnapshot = head)
        assertThat(changes.files.map { it.path }).containsExactly(
            "s3://hoglake-it/$catalog/data/a.parquet",
            "s3://hoglake-it/$catalog/data/b.parquet",
        )
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a prepared delete against a stale read snapshot is the typed conflict`(backend: Backend) {
        select(backend)
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val head = table.readSnapshotId
        val first =
            store.commit(
                catalog,
                CommitRequest(
                    readSnapshot = head,
                    idempotencyKey = UUID.randomUUID(),
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration("s3://hoglake-it/$catalog/data/a.parquet", 10, 100, 50)),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        val fileId = store.listFiles(catalog, "ns", "t").single().dataFileId

        // A writer that read at `first.snapshotId` commits a data change;
        // a second writer's GUARDED delete against the same read snapshot
        // must conflict.
        store.commit(
            catalog,
            CommitRequest(
                readSnapshot = first.snapshotId,
                idempotencyKey = UUID.randomUUID(),
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(FileRegistration("s3://hoglake-it/$catalog/data/b.parquet", 5, 50, 25)),
                            expectedTableUuid = table.tableUuid,
                        ),
                    ),
            ),
        )
        assertThatThrownBy {
            store.commit(
                catalog,
                CommitRequest(
                    readSnapshot = first.snapshotId,
                    idempotencyKey = UUID.randomUUID(),
                    requireUnchangedTables = true,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(
                                    DeleteFileRegistration(
                                        fileId,
                                        "s3://hoglake-it/$catalog/data/a.puffin",
                                        1,
                                        10,
                                    ),
                                ),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        }.isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)

        // A delete that re-reads lands, and the scan pairs the DV.
        val head2 = store.getCatalog(catalog).headSnapshotId
        val deleted =
            store.commit(
                catalog,
                CommitRequest(
                    readSnapshot = head2,
                    idempotencyKey = UUID.randomUUID(),
                    requireUnchangedTables = true,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(
                                    DeleteFileRegistration(
                                        fileId,
                                        "s3://hoglake-it/$catalog/data/a.puffin",
                                        1,
                                        10,
                                    ),
                                ),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        val scan = store.planScan(catalog, "ns", "t")
        val withDv = scan.filter { it.deleteFile != null }
        assertThat(withDv).hasSize(1)
        assertThat(withDv.single().deleteFile!!.deleteCount).isEqualTo(1)
        assertThat(deleted.snapshotId).isEqualTo(head2 + 1)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a guarded commit after drop plus recreate is the TableRecreated 409`(backend: Backend) {
        select(backend)
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        store.dropTable(catalog, "ns", "t", expectedTableUuid = table.tableUuid, author = "test", message = "drop")
        val recreated = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        assertThatThrownBy {
            store.commit(
                catalog,
                CommitRequest(
                    readSnapshot = recreated.readSnapshotId,
                    idempotencyKey = UUID.randomUUID(),
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration("s3://hoglake-it/$catalog/data/a.parquet", 1, 1, 1)),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        }.isExactlyInstanceOf(HoglakeException.TableRecreated::class.java)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `offsets are monotonic and reject regression`(backend: Backend) {
        select(backend)
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val offset =
            store.commitOffset(catalog, "consumer-1", table.tableUuid, table.readSnapshotId)
        assertThat(offset.committedSnapshot).isEqualTo(table.readSnapshotId)
        // An EQUAL re-commit is Postgres's upsert no-op arm: accepted, so a
        // client retry of a timed-out-but-applied call is safe.
        assertThat(store.commitOffset(catalog, "consumer-1", table.tableUuid, table.readSnapshotId).committedSnapshot)
            .isEqualTo(table.readSnapshotId)
        // Backwards is the 409.
        append(catalog, "offset.parquet")
        val head = store.getCatalog(catalog).headSnapshotId
        store.commitOffset(catalog, "consumer-1", table.tableUuid, head)
        assertThatThrownBy {
            store.commitOffset(catalog, "consumer-1", table.tableUuid, head - 1)
        }.isExactlyInstanceOf(HoglakeException.OffsetRegression::class.java)
        assertThat(store.getOffset(catalog, "consumer-1", table.tableUuid).committedSnapshot)
            .isEqualTo(head)
        assertThatThrownBy {
            store.commitOffset(catalog, "consumer-1", UUID.randomUUID(), 0)
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `prepare publish and the durable rejections`(backend: Backend) {
        select(backend)
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val op = UUID.randomUUID()
        val definition =
            TableCreationDefinition(
                "ns",
                "created",
                listOf(ColumnDef("id", ColType.LONG)),
            )
        val prepared = creations.prepare(catalog, op, definition)
        assertThat(prepared.state).isEqualTo("prepared")
        // idempotent replay
        assertThat(creations.prepare(catalog, op, definition).tableUuid)
            .isEqualTo(prepared.tableUuid)
        // a different definition under the same id is the 409
        assertThatThrownBy {
            creations.prepare(catalog, op, definition.copy(name = "other"))
        }.isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)

        val files =
            listOf(
                FileRegistration(
                    "${prepared.writePath}part-0.parquet",
                    10,
                    100,
                    50,
                ),
            )
        val published = creations.publish(catalog, op, files)
        assertThat(published.state).isEqualTo("committed")
        assertThat(published.snapshotId).isNotNull()
        assertThat(store.getTable(catalog, "ns", "created").tableUuid).isEqualTo(prepared.tableUuid)
        assertThat(store.listFiles(catalog, "ns", "created").single().recordCount).isEqualTo(10)
        // publish replay: same files return the receipt; different files 409.
        assertThat(creations.publish(catalog, op, files).state).isEqualTo("committed")
        assertThatThrownBy {
            creations.publish(
                catalog,
                op,
                listOf(FileRegistration("${prepared.writePath}part-1.parquet", 1, 1, 1)),
            )
        }.isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)

        // target_exists: a prepared creation whose name was taken in the
        // meantime rejects DURABLY, and the receipt says why.
        val op2 = UUID.randomUUID()
        creations.prepare(catalog, op2, definition.copy(name = "taken"))
        store.createTable(catalog, "ns", "taken", listOf(ColumnDef("id", ColType.LONG)))
        val rejected =
            creations.publish(
                catalog,
                op2,
                emptyList(),
            )
        assertThat(rejected.state).isEqualTo("rejected")
        assertThat(rejected.reason).isEqualTo("target_exists")

        // abort fences a prepared receipt.
        val op3 = UUID.randomUUID()
        creations.prepare(catalog, op3, definition.copy(name = "aborted"))
        assertThat(creations.abort(catalog, op3).state).isEqualTo("aborted")
        assertThat(creations.publish(catalog, op3, emptyList()).state).isEqualTo("aborted")
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `rename and add column, guarded and unguarded`(backend: Backend) {
        select(backend)
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val renamed =
            alters.alterTable(
                catalog,
                "ns",
                "t",
                listOf(com.posthog.hoglake.model.AlterOp.RenameTable("t2")),
                expectedTableUuid = table.tableUuid,
            )
        assertThat(renamed.name).isEqualTo("t2")
        assertThat(store.getTable(catalog, "ns", "t2").tableUuid).isEqualTo(table.tableUuid)
        assertThatThrownBy { store.getTable(catalog, "ns", "t") }
            .isExactlyInstanceOf(HoglakeException.NotFound::class.java)

        val added =
            alters.alterTable(
                catalog,
                "ns",
                "t2",
                listOf(
                    com.posthog.hoglake.model.AlterOp.AddColumn(ColumnDef("extra", ColType.STRING)),
                ),
            )
        assertThat(added.columns.map { it.def.name }).containsExactly("id", "extra")
        // a guarded alter against a stale read snapshot is the typed 409
        assertThatThrownBy {
            alters.alterTable(
                catalog,
                "ns",
                "t2",
                listOf(com.posthog.hoglake.model.AlterOp.RenameTable("t3")),
                readSnapshot = table.readSnapshotId,
            )
        }.isExactlyInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `views create list drop with change rows`(backend: Backend) {
        select(backend)
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))

        val view = views.create(catalog, "ns", "v", "SELECT 1", "trino", "test", "create")
        assertThat(views.get(catalog, "ns", "v").viewUuid).isEqualTo(view.viewUuid)
        assertThat(views.list(catalog, "ns").map { it.name }).containsExactly("v")
        assertThatThrownBy { views.create(catalog, "ns", "v", "SELECT 2", "trino", "test", "dup") }
            .isExactlyInstanceOf(HoglakeException.AlreadyExists::class.java)

        views.drop(catalog, "ns", "v", "test", "drop")
        assertThat(views.list(catalog, "ns")).isEmpty()
        val viewChanges =
            store.listSnapshots(catalog, 0, 100).first.flatMap { it.changes }
                .filter {
                    it.kind == com.posthog.hoglake.model.ChangeKind.VIEW_CREATED ||
                        it.kind == com.posthog.hoglake.model.ChangeKind.VIEW_DROPPED
                }
        assertThat(viewChanges.map { it.kind }).containsExactly(
            com.posthog.hoglake.model.ChangeKind.VIEW_CREATED,
            com.posthog.hoglake.model.ChangeKind.VIEW_DROPPED,
        )
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `options patch validates and attributes retention decreases`(backend: Backend) {
        select(backend)
        val catalog = freshCatalog()
        val options = options
        val initial = options.get(catalog)
        assertThat(initial.snapshotRetentionSeconds).isNull()

        // Setting retention where none was needs operator attribution.
        assertThatThrownBy {
            options.patch(catalog, com.posthog.hoglake.service.PatchField.Set(3600L), null, null, null)
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
        val set =
            options.patch(
                catalog,
                com.posthog.hoglake.service.PatchField.Set(3600L),
                null,
                "test",
                "set retention",
            )
        assertThat(set.snapshotRetentionSeconds).isEqualTo(3600L)
        // Raising it never needs attribution.
        val raised = options.patch(catalog, com.posthog.hoglake.service.PatchField.Set(7200L), null, null, null)
        assertThat(raised.snapshotRetentionSeconds).isEqualTo(7200L)
        // Lowering it does.
        assertThatThrownBy {
            options.patch(catalog, com.posthog.hoglake.service.PatchField.Set(60L), null, null, null)
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
        // A non-positive value is a 422 regardless.
        assertThatThrownBy {
            options.patch(catalog, com.posthog.hoglake.service.PatchField.Set(0L), null, "test", "bad")
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
        // consumer_floor toggles without attribution.
        val floored = options.patch(catalog, com.posthog.hoglake.service.PatchField.Absent, false, null, null)
        assertThat(floored.consumerFloor).isFalse()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a DV against a retired file is the typed conflict, both directions`(backend: Backend) {
        select(backend)
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val t = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        append(catalog, "a.parquet")
        val f = store.listFiles(catalog, "ns", "t").single().dataFileId
        val afterAppend = store.getCatalog(catalog).headSnapshotId
        store.truncateTable(catalog, "ns", "t", t.tableUuid, "test", "retire the file")
        // Retired AFTER the read snapshot: the retryable 409.
        assertThatThrownBy { dv(catalog, f, afterAppend, "dv.puffin", 1) }
            .isExactlyInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
        // Retired BEFORE the read snapshot: the caller can see it, 422.
        val now = store.getCatalog(catalog).headSnapshotId
        assertThatThrownBy { dv(catalog, f, now, "dv.puffin", 1) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `an unkeyed commit is never deduplicated, even byte-identical after truncate`(backend: Backend) {
        select(backend)
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val t = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val first = append(catalog, "a.parquet", key = null)
        store.truncateTable(catalog, "ns", "t", t.tableUuid, "test", "clear the table")
        assertThat(store.listFiles(catalog, "ns", "t")).isEmpty()
        // The identical unkeyed commit must APPLY, not replay the old receipt.
        val second = append(catalog, "a.parquet", key = null)
        assertThat(second.snapshotId).isGreaterThan(first.snapshotId)
        assertThat(store.listFiles(catalog, "ns", "t")).hasSize(1)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a staged DV cannot reference another table's append by path`(backend: Backend) {
        select(backend)
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val target = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val wrong = store.createTable(catalog, "ns", "u", listOf(ColumnDef("id", ColType.LONG)))
        val path = "s3://hoglake-it/$catalog/data/shared.parquet"
        // One commit: append the path to t, reference it from u's delete.
        assertThatThrownBy {
            store.commit(
                catalog,
                CommitRequest(
                    idempotencyKey = UUID.randomUUID(),
                    readSnapshot = wrong.readSnapshotId,
                    requireUnchangedTables = true,
                    allowPendingDeletes = true,
                    appends =
                        listOf(
                            TableAppend("ns", "t", listOf(FileRegistration(path, 10, 100, 50)), target.tableUuid),
                        ),
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "u",
                                listOf(
                                    DeleteFileRegistration(
                                        0,
                                        "s3://hoglake-it/$catalog/data/dv.puffin",
                                        1,
                                        10,
                                        dataFilePath = path,
                                    ),
                                ),
                                expectedTableUuid = wrong.tableUuid,
                            ),
                        ),
                ),
            )
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
        // Nothing landed.
        assertThat(store.listFiles(catalog, "ns", "t")).isEmpty()
        assertThat(store.listFiles(catalog, "ns", "u")).isEmpty()
    }

    private fun append(
        catalog: String,
        path: String,
        rows: Long = 10,
        key: UUID? = UUID.randomUUID(),
        read: Long? = null,
        table: String = "t",
    ) = store.commit(
        catalog,
        CommitRequest(
            readSnapshot = read,
            idempotencyKey = key,
            appends =
                listOf(
                    TableAppend(
                        "ns",
                        table,
                        listOf(FileRegistration("s3://hoglake-it/$catalog/data/$path", rows, 100, 50)),
                    ),
                ),
        ),
    )

    private fun dv(
        catalog: String,
        fileId: Long,
        read: Long,
        path: String,
        count: Long,
        table: String = "t",
    ) = store.commit(
        catalog,
        CommitRequest(
            readSnapshot = read,
            idempotencyKey = UUID.randomUUID(),
            deletes =
                listOf(
                    TableDeletes(
                        "ns",
                        table,
                        listOf(
                            DeleteFileRegistration(fileId, "s3://hoglake-it/$catalog/data/$path", count, 10),
                        ),
                    ),
                ),
        ),
    )

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `set_object_prefix_field sets the layout, getTable reports it, and the Postgres refusals hold`(
        backend: Backend,
    ) {
        select(backend)
        val cat = fixture()
        assertThat(store.getTable(cat, "ingest", "events").objectPrefixField).isEqualTo("team_id")
        assertThat(store.getTable(cat, "ingest", "plain").objectPrefixField).isNull()
        // The layout is versioned: before the set, the table had none.
        val setAt = store.getTable(cat, "ingest", "events").readSnapshotId
        assertThat(store.getTable(cat, "ingest", "events", snapshot = setAt - 1).objectPrefixField).isNull()

        // Same field again: a no-op, not a second row.
        val head = store.getCatalog(cat).headSnapshotId
        alters.alterTable(cat, "ingest", "events", listOf(AlterOp.SetObjectPrefixField("team_id")))
        assertThat(store.getCatalog(cat).headSnapshotId).isGreaterThanOrEqualTo(head)

        // A different field is refused while one is set.
        assertThatThrownBy {
            alters.alterTable(cat, "ingest", "events", listOf(AlterOp.SetObjectPrefixField("v")))
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)

        // A field without exactly one identity key in the live spec is refused.
        store.createTable(cat, "ingest", "unpartitioned", columns)
        assertThatThrownBy {
            alters.alterTable(cat, "ingest", "unpartitioned", listOf(AlterOp.SetObjectPrefixField("team_id")))
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("identity")

        // A non-empty table is refused (its files were never held to a prefix).
        append(cat, "plain", file(root(cat, "plain") + "a.parquet", "4", null))
        assertThatThrownBy {
            alters.alterTable(cat, "ingest", "plain", listOf(AlterOp.SetObjectPrefixField("team_id")))
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("empty")
        assertThat(store.getTable(cat, "ingest", "plain").objectPrefixField).isNull()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a layout table refuses the DDL that would move its prefix`(backend: Backend) {
        select(backend)
        val cat = fixture()
        val refusals =
            listOf(
                AlterOp.DropColumn("team_id"),
                AlterOp.RenameColumn("team_id", "team"),
                AlterOp.RenameTable("events2"),
                AlterOp.SetPartitionSpec(emptyList()),
                AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY))),
            )
        for (op in refusals) {
            assertThatThrownBy { alters.alterTable(cat, "ingest", "events", listOf(op)) }
                .describedAs(op.toString())
                .isExactlyInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("object-prefix")
        }
        // The twin without the option accepts the same rename.
        alters.alterTable(cat, "ingest", "plain", listOf(AlterOp.RenameColumn("team_id", "team")))
        // A spec that keeps the single identity key on the field is fine.
        alters.alterTable(
            cat,
            "ingest",
            "events",
            listOf(AlterOp.SetPartitionSpec(teamSpec + PartitionFieldDef(2, Transform.IDENTITY))),
        )
        // An atomic replacement is refused at prepare (TableCreationService's message).
        val events = store.getTable(cat, "ingest", "events")
        assertThatThrownBy {
            creations.prepare(
                cat,
                UUID.randomUUID(),
                TableCreationDefinition(
                    namespace = "ingest",
                    name = "events",
                    columns = columns,
                    replacement =
                        com.posthog.hoglake.service.ReplacementTarget(
                            events.tableUuid,
                            events.readSnapshotId,
                        ),
                    author = "test",
                    message = "replace",
                ),
            )
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("cannot replace table 'ingest.events': it has the object-prefix layout")
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `commits on a layout table must sit under the value's prefix and prove it, the twin accepts all`(
        backend: Backend,
    ) {
        select(backend)
        val cat = fixture()
        // Admitted: under the prefix with proving stats, and deferred stats.
        append(cat, "events", good(cat))
        append(cat, "events", file(under(cat, "events", "4") + "deferred.parquet", "4", null))
        assertThat(store.listFiles(cat, "ingest", "events").count { it.statsState == StatsState.PENDING }).isEqualTo(1)

        val refused =
            listOf(
                // At the table root.
                file(root(cat, "events") + "root.parquet", "4", stats(le8(4), le8(4))),
                // Under another value's prefix.
                file(under(cat, "events", "5") + "other.parquet", "4", stats(le8(4), le8(4))),
                // Non-canonical value.
                file(under(cat, "events", "04") + "pad.parquet", "04", stats(le8(4), le8(4))),
                // Inline stats that do not prove the value.
                file(under(cat, "events", "4") + "wide.parquet", "4", stats(le8(4), le8(5))),
                file(under(cat, "events", "4") + "nulls.parquet", "4", stats(le8(4), le8(4), nulls = 1)),
            )
        for (f in refused) {
            assertThatThrownBy { append(cat, "events", f) }
                .describedAs(f.path)
                .isExactlyInstanceOf(HoglakeException.ObjectPrefixViolation::class.java)
                .hasMessageContaining("lives under ${root(cat, "events")}team_id=<value>/")
            // The twin: same shape on the option-off table, accepted.
            append(cat, "plain", f.copy(path = f.path.replace("/events/", "/plain/")))
        }
        // A refusal registers nothing.
        assertThat(store.listFiles(cat, "ingest", "events")).hasSize(2)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `exact partition lookup pages, aggregates and validates like Postgres`(backend: Backend) {
        select(backend)
        val cat = fixture()
        val fours = (1..3).map { good(cat, value = 4) }
        for (f in fours) append(cat, "events", f)
        append(cat, "events", good(cat, value = 5))
        val lookup = lookup

        val all = lookup.lookup(cat, "ingest", "events", 1, listOf("4"))
        assertThat(all.files.map { it.path }).containsExactlyElementsOf(fours.map { it.path })
        assertThat(all.aggregates.status).isEqualTo("exact")
        assertThat(all.aggregates.totals!!.fileCount).isEqualTo(3L)
        assertThat(all.aggregates.totals!!.physicalRows).isEqualTo(30L)
        assertThat(all.aggregates.totals!!.totalBytes).isEqualTo(3_000L)
        assertThat(all.aggregates.totals!!.statsState.provided).isEqualTo(3L)
        assertThat(all.nextCursor).isNull()

        // Paging: two, then the last one with continuation aggregates.
        val first = lookup.lookup(cat, "ingest", "events", 1, listOf("4"), limit = 2)
        assertThat(first.files).hasSize(2)
        assertThat(first.nextCursor).isNotNull()
        val second = lookup.lookup(cat, "ingest", "events", 1, listOf("4"), cursor = first.nextCursor)
        assertThat(second.files.map { it.path }).containsExactly(fours[2].path)
        assertThat(second.aggregates.status).isEqualTo("unavailable")
        assertThat(second.aggregates.reason).isEqualTo("continuation")
        assertThat(second.nextCursor).isNull()
        // A cursor is bound to its lookup.
        assertThatThrownBy { lookup.lookup(cat, "ingest", "events", 1, listOf("5"), cursor = first.nextCursor) }
            .isExactlyInstanceOf(PartitionLookupInvalidRequest::class.java)

        // At an older snapshot, only what was live there.
        val atTwo = store.getTable(cat, "ingest", "events").readSnapshotId - 2
        assertThat(lookup.lookup(cat, "ingest", "events", 1, listOf("4"), snapshot = atTwo).files).hasSize(2)

        // Validation, Postgres's messages.
        assertThatThrownBy { lookup.lookup(cat, "ingest", "events", 1, listOf("4", "1")) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("values must contain the complete 1-field tuple for spec 1")
        assertThatThrownBy { lookup.lookup(cat, "ingest", "events", 9, listOf("4")) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("partition spec 9 is unavailable")
        assertThatThrownBy { lookup.lookup(cat, "ingest", "events", 1, listOf("4"), snapshot = 1_000_000) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("snapshot must be between 0 and catalog head")
        assertThatThrownBy { lookup.lookup(cat, "ingest", "events", 1, listOf("4"), limit = 0) }
            .isExactlyInstanceOf(PartitionLookupInvalidRequest::class.java)
        assertThatThrownBy { lookup.lookup(cat, "ingest", "nope", 1, listOf("4")) }
            .isExactlyInstanceOf(HoglakeException.NotFound::class.java)
    }

    private val columns = listOf(ColumnDef("team_id", ColType.LONG), ColumnDef("v", ColType.LONG))
    private val teamSpec = listOf(PartitionFieldDef(1, Transform.IDENTITY))

    /** `ingest.events` partitioned by identity(team_id) with the option on it, and `ingest.plain` without. */
    private fun fixture(): String {
        val cat = freshCatalog()
        store.createNamespace(cat, "ingest")
        for (table in listOf("events", "plain")) {
            store.createTable(cat, "ingest", table, columns)
            alters.alterTable(cat, "ingest", table, listOf(AlterOp.SetPartitionSpec(teamSpec)))
        }
        alters.alterTable(cat, "ingest", "events", listOf(AlterOp.SetObjectPrefixField("team_id")))
        return cat
    }

    private fun root(
        cat: String,
        table: String,
    ) = "s3://hoglake-it/$cat/data/ingest/$table/"

    private fun under(
        cat: String,
        table: String,
        value: String,
    ) = root(cat, table) + "team_id=$value/"

    private fun le8(v: Long): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array()

    private fun stats(
        lower: ByteArray?,
        upper: ByteArray?,
        nulls: Long = 0,
    ) = listOf(
        ColumnStats(1, 10, nulls, null, null, lower, upper),
        ColumnStats(2, 10, 0, null, null, le8(1), le8(9)),
    )

    private fun file(
        path: String,
        value: String?,
        columnStats: List<ColumnStats>?,
        rows: Long = 10,
        size: Long = 1_000,
        footer: Long? = 20,
    ) = FileRegistration(path, rows, size, footer, columnStats, listOf(value))

    private fun good(
        cat: String,
        table: String = "events",
        value: Long = 4,
    ) = file(under(cat, table, "$value") + "${UUID.randomUUID()}.parquet", "$value", stats(le8(value), le8(value)))

    private fun append(
        cat: String,
        table: String,
        vararg files: FileRegistration,
    ) = store.commit(
        cat,
        CommitRequest(
            idempotencyKey = UUID.randomUUID(),
            appends = listOf(TableAppend("ingest", table, files.toList())),
        ),
    )

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `refusals consume no snapshots and acknowledgements are visible in order`(backend: Backend) {
        select(backend)
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val start = store.getCatalog(cat).headSnapshotId
        val receipts =
            (1..3).map { i ->
                assertThatThrownBy { append(cat, "bad$i.parquet", rows = -1) }
                    .isExactlyInstanceOf(HoglakeException.Validation::class.java)
                val ack = append(cat, "good$i.parquet")
                assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(ack.snapshotId)
                assertThat(store.planScan(cat, "ns", "t", snapshot = ack.snapshotId)).hasSize(i)
                ack.snapshotId
            }
        assertThat(receipts).containsExactly(start + 1, start + 2, start + 3)
        assertThat(store.listSnapshots(cat, start, 100).first.map { it.snapshotId }).containsExactlyElementsOf(receipts)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `DV supersession preserves pinned epochs and refuses a stale epoch`(backend: Backend) {
        select(backend)
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val initial = append(cat, "a.parquet").snapshotId
        val id = store.listFiles(cat, "ns", "t").single().dataFileId
        val first = dv(cat, id, initial, "one.puffin", 1).snapshotId
        val second = dv(cat, id, first, "two.puffin", 3).snapshotId
        assertThat(store.planScan(cat, "ns", "t", snapshot = initial).single().deleteFile).isNull()
        assertThat(store.planScan(cat, "ns", "t", snapshot = first).single().deleteFile!!.deleteCount).isEqualTo(1)
        val live = store.planScan(cat, "ns", "t", snapshot = second).single().deleteFile!!
        assertThat(live.path).endsWith("two.puffin")
        assertThat(live.deleteCount).isEqualTo(3)
        assertThatThrownBy { dv(cat, id, first, "stale.puffin", 4) }
            .isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(second)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `replacement of an absent target creates without a retirement fence`(backend: Backend) {
        select(backend)
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        val read = store.getCatalog(cat).headSnapshotId
        val op = UUID.randomUUID()
        val prepared =
            creations.prepare(
                cat,
                op,
                TableCreationDefinition(
                    "ns",
                    "fresh",
                    listOf(ColumnDef("id", ColType.LONG)),
                    replacement = com.posthog.hoglake.service.ReplacementTarget(null, read),
                    author = "test",
                    message = "replace absent",
                ),
            )
        val result = creations.publish(cat, op, listOf(FileRegistration("${prepared.writePath}a.parquet", 3, 100, 50)))
        assertThat(result.state).isEqualTo("committed")
        assertThat(result.reason).isNull()
        assertThat(store.getTable(cat, "ns", "fresh").tableUuid).isEqualTo(prepared.tableUuid)
        assertThat(store.getTable(cat, "ns", "fresh", snapshot = result.snapshotId).recordCount).isEqualTo(3)
        assertThat(store.listTables(cat, "ns", snapshot = read)).isEmpty()
        assertThat(
            creations.publish(
                cat,
                op,
                listOf(FileRegistration("${prepared.writePath}a.parquet", 3, 100, 50)),
            ).snapshotId,
        )
            .isEqualTo(result.snapshotId)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `replacement atomically swaps identities and preserves the old snapshot`(backend: Backend) {
        select(backend)
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        val old = store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val read = append(cat, "old.parquet").snapshotId
        val op = UUID.randomUUID()
        val prepared =
            creations.prepare(
                cat,
                op,
                TableCreationDefinition(
                    "ns",
                    "t",
                    listOf(ColumnDef("id", ColType.LONG)),
                    replacement = com.posthog.hoglake.service.ReplacementTarget(old.tableUuid, read),
                    author = "test",
                    message = "replace existing",
                ),
            )
        assertThat(store.getTable(cat, "ns", "t").tableUuid).isEqualTo(old.tableUuid)
        assertThat(creations.publish(cat, op, emptyList()).state).isEqualTo("committed")
        assertThat(store.getTable(cat, "ns", "t").tableUuid).isEqualTo(prepared.tableUuid).isNotEqualTo(old.tableUuid)
        assertThat(store.listFiles(cat, "ns", "t")).isEmpty()
        assertThat(store.getTable(cat, "ns", "t", snapshot = read).tableUuid).isEqualTo(old.tableUuid)
        assertThat(store.listFiles(cat, "ns", "t", snapshot = read)).hasSize(1)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `expiry respects retention consumer floor and typed expired reads and writes`(backend: Backend) {
        select(backend)
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        val table = store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val pin = append(cat, "first.parquet").snapshotId
        val head = append(cat, "second.parquet").snapshotId
        // Retention is opt-in: no history expires by default.
        assertThat(expiry.runOnce(cat, 100).newEarliestSnapshotId).isZero()
        store.commitOffset(cat, "reader", table.tableUuid, pin)
        options.patch(cat, com.posthog.hoglake.service.PatchField.Set(1L), null, "test", "short")
        Thread.sleep(1_100)
        val pinned = expiry.runOnce(cat, 100)
        assertThat(pinned.newEarliestSnapshotId).isEqualTo(pin)
        assertThat(pinned.flooredByConsumer).isEqualTo("reader")
        assertThat(store.planScan(cat, "ns", "t", snapshot = pin)).hasSize(1)
        assertThatThrownBy { store.planScan(cat, "ns", "t", snapshot = pin - 1) }
            .isExactlyInstanceOf(HoglakeException.Expired::class.java)
        assertThatThrownBy { append(cat, "stale.parquet", read = pin - 1) }
            .isExactlyInstanceOf(HoglakeException.Expired::class.java)
        options.patch(cat, com.posthog.hoglake.service.PatchField.Absent, false)
        assertThat(expiry.runOnce(cat, 100).newEarliestSnapshotId).isEqualTo(head)
        assertThat(store.planScan(cat, "ns", "t")).hasSize(2)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `retired partition specs remain usable after expiry passes their retirement`(backend: Backend) {
        select(backend)
        val cat = fixture()
        val old = file(root(cat, "plain") + "old.parquet", "4", null)
        append(cat, "plain", old)
        val retiredAt =
            alters.alterTable(
                cat,
                "ingest",
                "plain",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY))),
                ),
            ).readSnapshotId
        append(cat, "plain", file(root(cat, "plain") + "new.parquet", "9", null))
        options.patch(cat, com.posthog.hoglake.service.PatchField.Set(1L), null, "test", "short")
        Thread.sleep(1_100)
        expiry.runOnce(cat, 100)
        assertThat(store.getCatalog(cat).earliestSnapshotId).isGreaterThan(retiredAt)
        assertThat(
            lookup.lookup(cat, "ingest", "plain", 1, listOf("4")).files.map { it.path },
        ).containsExactly(old.path)
        assertThat(lookup.lookup(cat, "ingest", "plain", 2, listOf("9")).files).hasSize(1)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `partition specs allocate after retirement and coalesce same-alter changes`(backend: Backend) {
        select(backend)
        val cat = fixture()
        alters.alterTable(cat, "ingest", "plain", listOf(AlterOp.SetPartitionSpec(emptyList())))
        val again = alters.alterTable(cat, "ingest", "plain", listOf(AlterOp.SetPartitionSpec(teamSpec)))
        assertThat(again.partitionSpec!!.specId).isEqualTo(2)
        val twice =
            alters.alterTable(
                cat,
                "ingest",
                "plain",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY))),
                    AlterOp.SetPartitionSpec(teamSpec + PartitionFieldDef(2, Transform.IDENTITY)),
                ),
            )
        assertThat(twice.partitionSpec!!.specId).isEqualTo(3)
        assertThat(store.getTable(cat, "ingest", "plain").partitionSpec).isEqualTo(twice.partitionSpec)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `hydration and compaction preserve rows snapshot visibility and exact head totals`(backend: Backend) {
        select(backend)
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        store.createTable(cat, "ns", "t", columns)
        for (i in 1..2) {
            val (path, size) = putParquet(cat, "s3://hoglake-it/$cat/data/$i.parquet", i.toLong())
            store.commit(
                cat,
                CommitRequest(appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(path, 2, size))))),
            )
        }
        val before = store.getCatalog(cat).headSnapshotId
        assertThat(store.listFiles(cat, "ns", "t").map { it.statsState }).containsOnly(StatsState.PENDING)
        hydrate()
        assertThat(store.listFiles(cat, "ns", "t").map { it.statsState }).containsOnly(StatsState.PROVIDED)
        for (f in store.listFiles(cat, "ns", "t")) {
            assertThat(store.fileStats(cat, "ns", "t", f.dataFileId).columns.map { it.fieldId }).containsExactly(1L, 2L)
        }
        val result = compact(cat)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.filesIn).isEqualTo(2)
        assertThat(result.filesOut).isEqualTo(1)
        val output = store.listFiles(cat, "ns", "t").single()
        assertThat(output.recordCount).isEqualTo(4)
        assertThat(output.explicitRowIds).isTrue()
        assertThat(output.statsState).isEqualTo(StatsState.PROVIDED)
        assertThat(store.listFiles(cat, "ns", "t", snapshot = before)).hasSize(2)
        val head = store.getCatalog(cat).headSnapshotId
        val unpinned = store.getTable(cat, "ns", "t")
        // Head totals may be absent until sampled. Undated totals, if supplied, must be exact.
        if (unpinned.recordCount != null && unpinned.totalsSnapshotId == null) {
            assertThat(unpinned.recordCount).isEqualTo(4)
            assertThat(unpinned.fileCount).isEqualTo(1)
        }
        assertThat(store.getTable(cat, "ns", "t", snapshot = head).recordCount).isEqualTo(4)
        assertThat(store.getTable(cat, "ns", "t", snapshot = before).fileCount).isEqualTo(2)
        assertThat(compact(cat).groupsCompacted).isZero()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `hydrator quarantine excludes contradictory prefixes from compaction`(backend: Backend) {
        select(backend)
        val cat = fixture()
        for ((value, actual) in listOf(4L to 4L, 8L to 9L)) {
            for (i in 1..2) {
                val (path, size) = putParquet(cat, under(cat, "events", "$value") + "$i.parquet", actual)
                append(cat, "events", file(path, "$value", null, rows = 2, size = size, footer = null))
            }
        }
        // Pending files cannot be compacted before verification.
        assertThat(compact(cat).groupsCompacted).isZero()
        hydrate()
        assertThat(store.listFiles(cat, "ingest", "events").map { it.statsState }).containsOnly(StatsState.PROVIDED)
        val result = compact(cat)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.invalidData).isZero()
        val files = store.listFiles(cat, "ingest", "events")
        assertThat(files).hasSize(3)
        val output = files.single { it.explicitRowIds }
        assertThat(output.path).startsWith(under(cat, "events", "4"))
        assertThat(output.recordCount).isEqualTo(4)
        assertThat(
            files.filterNot { it.explicitRowIds }.map { it.path },
        ).allMatch { it.startsWith(under(cat, "events", "8")) }
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `compaction refuses a footer that contradicts inline prefix proof without publishing`(backend: Backend) {
        select(backend)
        val cat = fixture()
        for (i in 1..2) {
            val (path, size) = putParquet(cat, under(cat, "events", "4") + "$i.parquet", 5)
            append(cat, "events", file(path, "4", stats(le8(4), le8(4)), rows = 2, size = size, footer = null))
        }
        val before = store.getCatalog(cat).headSnapshotId
        val result = compact(cat)
        assertThat(result.invalidData).isEqualTo(1)
        assertThat(result.groupsCompacted).isZero()
        assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(before)
        assertThat(store.listFiles(cat, "ingest", "events")).hasSize(2)
    }

    // Backend choices belong only in fixture adapters, never in contract assertions.
    private fun hydrate() {
        com.posthog.hoglake.hydrator.ObjectStore(engine().config()).use { objects ->
            when (selected) {
                Backend.POSTGRES -> com.posthog.hoglake.hydrator.Hydrator(pg!!.jdbi, objects).runOnce()
                Backend.HOGLITE -> HogliteHydrator(engine().store, objects).runOnce()
            }
        }
    }

    private fun compact(cat: String): com.posthog.hoglake.model.CompactionResult {
        val config =
            com.posthog.hoglake.compaction.CompactionConfig(
                targetBytes = 1L shl 30,
                minInputFiles = 2,
                maxInputFiles = 2,
                maxFanIn = 2,
                maxGroupsPerRun = 10,
                maxCandidates = 100,
            )
        return com.posthog.hoglake.hydrator.ObjectStore(engine().config()).use { objects ->
            val compactor: com.posthog.hoglake.service.CompactionBackend =
                when (selected) {
                    Backend.POSTGRES -> com.posthog.hoglake.compaction.CompactionService(pg!!.jdbi, objects, config)
                    Backend.HOGLITE -> HogliteCompaction(engine().store, objects, config)
                }
            compactor.runOnce(cat)
        }
    }

    private val parquetSchema =
        org.apache.parquet.schema.Types
            .buildMessage()
            .addField(
                org.apache.parquet.schema.Types
                    .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64)
                    .id(1)
                    .named("team_id"),
            ).addField(
                org.apache.parquet.schema.Types
                    .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64)
                    .id(2)
                    .named("v"),
            ).named("t")

    /** Two rows of team_id [team] at [path] (an s3://hoglake-it/ URI); returns (path, size). */
    private fun putParquet(
        cat: String,
        path: String,
        team: Long,
    ): Pair<String, Long> {
        check(path.startsWith("s3://hoglake-it/$cat/"))
        val tmp = Files.createTempFile("hoglite-prefix", ".parquet")
        val bytes =
            try {
                Files.delete(tmp)
                val factory = org.apache.parquet.example.data.simple.SimpleGroupFactory(parquetSchema)
                org.apache.parquet.hadoop.example.ExampleParquetWriter
                    .builder(org.apache.parquet.io.LocalOutputFile(tmp))
                    .withType(parquetSchema)
                    .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED)
                    .build()
                    .use { w ->
                        for (v in listOf(1L, 2L)) {
                            val g = factory.newGroup()
                            g.add("team_id", team)
                            g.add("v", v)
                            w.write(g)
                        }
                    }
                Files.readAllBytes(tmp)
            } finally {
                Files.deleteIfExists(tmp)
            }
        engine().s3Client().use { s3 ->
            s3.putObject(
                { it.bucket("hoglake-it").key(path.removePrefix("s3://hoglake-it/")) },
                software.amazon.awssdk.core.sync.RequestBody.fromBytes(bytes),
            )
        }
        return path to bytes.size.toLong()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `concurrent acknowledgements allocate one dense total snapshot order`(backend: Backend) {
        select(backend)
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val start = store.getCatalog(cat).headSnapshotId
        val gate = java.util.concurrent.CountDownLatch(1)
        java.util.concurrent.Executors.newFixedThreadPool(4).use { pool ->
            val futures =
                (1..4).map { i ->
                    pool.submit<Long> {
                        gate.await()
                        val ack = append(cat, "concurrent$i.parquet")
                        assertThat(store.planScan(cat, "ns", "t", snapshot = ack.snapshotId).map { it.dataFile.path })
                            .contains("s3://hoglake-it/$cat/data/concurrent$i.parquet")
                        ack.snapshotId
                    }
                }
            gate.countDown()
            val snapshots = futures.map { it.get(30, java.util.concurrent.TimeUnit.SECONDS) }.sorted()
            assertThat(snapshots).containsExactly(start + 1, start + 2, start + 3, start + 4)
        }
        assertThat(store.listFiles(cat, "ns", "t").map { it.rowIdStart }.sorted()).containsExactly(0L, 10L, 20L, 30L)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `stale schema append and future snapshot retain distinct typed refusals`(backend: Backend) {
        select(backend)
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        val table = store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        alters.alterTable(cat, "ns", "t", listOf(AlterOp.AddColumn(ColumnDef("extra", ColType.STRING))))
        assertThatThrownBy { append(cat, "stale.parquet", read = table.readSnapshotId) }
            .isExactlyInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
        assertThatThrownBy { append(cat, "future.parquet", read = store.getCatalog(cat).headSnapshotId + 1) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
        assertThat(store.listFiles(cat, "ns", "t")).isEmpty()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `object prefix DVs validate both durable and same-commit targets atomically`(backend: Backend) {
        select(backend)
        val cat = fixture()
        val target = good(cat)
        append(cat, "events", target)
        val id = store.listFiles(cat, "ingest", "events").single().dataFileId

        fun commitDv(path: String) =
            store.commit(
                cat,
                CommitRequest(
                    readSnapshot = store.getCatalog(cat).headSnapshotId,
                    deletes = listOf(TableDeletes("ingest", "events", listOf(DeleteFileRegistration(id, path, 1, 10)))),
                ),
            )
        for (path in listOf(root(cat, "events") + "bad.puffin", under(cat, "events", "5") + "bad.puffin")) {
            assertThatThrownBy {
                commitDv(
                    path,
                )
            }.isExactlyInstanceOf(HoglakeException.ObjectPrefixViolation::class.java)
        }
        assertThat(store.planScan(cat, "ingest", "events").single().deleteFile).isNull()
        commitDv(under(cat, "events", "4") + "ok.puffin")
        assertThat(store.planScan(cat, "ingest", "events").single().deleteFile!!.deleteCount).isEqualTo(1)
        val staged = good(cat, value = 7)
        val expected = store.getTable(cat, "ingest", "events").tableUuid

        fun stagedDv(path: String) =
            store.commit(
                cat,
                CommitRequest(
                    allowPendingDeletes = true,
                    requireUnchangedTables = true,
                    idempotencyKey = UUID.randomUUID(),
                    readSnapshot = store.getCatalog(cat).headSnapshotId,
                    appends = listOf(TableAppend("ingest", "events", listOf(staged), expectedTableUuid = expected)),
                    deletes =
                        listOf(
                            TableDeletes(
                                "ingest",
                                "events",
                                listOf(
                                    DeleteFileRegistration(0, path, 1, 10, dataFilePath = staged.path),
                                ),
                                expectedTableUuid = expected,
                            ),
                        ),
                ),
            )
        assertThatThrownBy { stagedDv(under(cat, "events", "4") + "staged.puffin") }
            .isExactlyInstanceOf(HoglakeException.ObjectPrefixViolation::class.java)
        assertThat(store.listFiles(cat, "ingest", "events")).hasSize(1)
        stagedDv(under(cat, "events", "7") + "staged.puffin")
        assertThat(store.planScan(cat, "ingest", "events").map { it.deleteFile!!.deleteCount }).containsExactly(1L, 1L)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `sort spec receipts match reads after retirement and same-alter changes`(backend: Backend) {
        select(backend)
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        store.createTable(cat, "ns", "t", columns)
        val order =
            listOf(
                com.posthog.hoglake.model.SortFieldDef(
                    1,
                    com.posthog.hoglake.model.SortDirection.ASC,
                    com.posthog.hoglake.model.NullOrder.NULLS_FIRST,
                ),
            )
        alters.alterTable(cat, "ns", "t", listOf(AlterOp.SetSortOrder(order)))
        alters.alterTable(cat, "ns", "t", listOf(AlterOp.SetSortOrder(emptyList())))
        val restored = alters.alterTable(cat, "ns", "t", listOf(AlterOp.SetSortOrder(order)))
        assertThat(restored.sortSpec!!.sortId).isEqualTo(2)
        assertThat(store.getTable(cat, "ns", "t").sortSpec).isEqualTo(restored.sortSpec)
        val twice =
            alters.alterTable(
                cat,
                "ns",
                "t",
                listOf(
                    AlterOp.SetSortOrder(
                        order.map { it.copy(direction = com.posthog.hoglake.model.SortDirection.DESC) },
                    ),
                    AlterOp.SetSortOrder(order),
                ),
            )
        assertThat(twice.sortSpec!!.sortId).isEqualTo(3)
        assertThat(store.getTable(cat, "ns", "t").sortSpec).isEqualTo(twice.sortSpec)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `pending delete targets require guarded idempotent transaction publication`(backend: Backend) {
        select(backend)
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        val table = store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val path = "s3://hoglake-it/$cat/data/staged.parquet"
        val valid =
            CommitRequest(
                allowPendingDeletes = true,
                requireUnchangedTables = true,
                idempotencyKey = UUID.randomUUID(),
                readSnapshot = table.readSnapshotId,
                appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(path, 10, 100, 50)), table.tableUuid)),
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(
                                DeleteFileRegistration(
                                    0,
                                    "s3://hoglake-it/$cat/data/staged.puffin",
                                    1,
                                    10,
                                    dataFilePath = path,
                                ),
                            ),
                            table.tableUuid,
                        ),
                    ),
            )
        for (request in listOf(
            valid.copy(allowPendingDeletes = false),
            valid.copy(requireUnchangedTables = false),
            valid.copy(idempotencyKey = null),
            valid.copy(readSnapshot = null),
            valid.copy(appends = valid.appends.map { it.copy(expectedTableUuid = null) }),
            valid.copy(deletes = valid.deletes.map { it.copy(expectedTableUuid = null) }),
        )) {
            assertThatThrownBy { store.commit(cat, request) }
                .isExactlyInstanceOf(HoglakeException.Validation::class.java)
            assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(table.readSnapshotId)
            assertThat(store.listFiles(cat, "ns", "t")).isEmpty()
        }
        store.commit(cat, valid)
        assertThat(store.planScan(cat, "ns", "t").single().deleteFile!!.deleteCount).isEqualTo(1)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `nonempty drops and commits to dropped tables have distinct typed refusals`(backend: Backend) {
        select(backend)
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        val table = store.createTable(cat, "ns", "t", columns)
        assertThatThrownBy { store.dropNamespace(cat, "ns", author = "test", message = "drop") }
            .isExactlyInstanceOf(HoglakeException.NamespaceNotEmpty::class.java)
        assertThatThrownBy { store.dropCatalog(cat, "test", "drop") }
            .isExactlyInstanceOf(HoglakeException.CatalogNotEmpty::class.java)
        store.dropTable(cat, "ns", "t", expectedTableUuid = table.tableUuid, author = "test", message = "drop")
        assertThatThrownBy { append(cat, "dropped.parquet") }
            .isExactlyInstanceOf(HoglakeException.TableDropped::class.java)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `truncate changefeed barriers require reconciliation not blind replay`(backend: Backend) {
        select(backend)
        val cat = freshCatalog()
        store.createNamespace(cat, "ns")
        val table = store.createTable(cat, "ns", "t", columns)
        val before = append(cat, "before.parquet").snapshotId
        val truncated = store.truncateTable(cat, "ns", "t", table.tableUuid, "test", "truncate").snapshotId
        val after = append(cat, "after.parquet").snapshotId
        assertThatThrownBy { store.changes(cat, "ns", "t", fromSnapshot = before, toSnapshot = after) }
            .isExactlyInstanceOf(HoglakeException.ReconciliationRequired::class.java)
        assertThat(store.changes(cat, "ns", "t", fromSnapshot = 0, toSnapshot = before).files).hasSize(1)
        assertThat(store.changes(cat, "ns", "t", fromSnapshot = truncated, toSnapshot = after).files).hasSize(1)
    }
}
