package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TableCreationIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val creations = TableCreationService(db.jdbi, catalogs, CommitService(db.jdbi))
    private val definition = TableCreationDefinition("test", "target", listOf(ColumnDef("id", ColType.LONG)))

    @AfterAll
    fun close() = db.close()

    private fun catalog(): String {
        val name = "creation-" + UUID.randomUUID().toString().replace("-", "")
        catalogs.createCatalog(name, "s3://bucket/$name")
        catalogs.createNamespace(name, "test")
        return name
    }

    private fun file(operation: TableCreation) = FileRegistration(operation.writePath + "part.parquet", 7, 100, 20)

    /** One of every container shape, plus a three-level combination. */
    private val nestedColumns =
        listOf(
            ColumnDef("id", ColType.LONG, nullable = false),
            ColumnDef(
                "addr",
                ColType.STRUCT,
                children = listOf(ColumnDef("zip", ColType.STRING), ColumnDef("city", ColType.STRING)),
            ),
            ColumnDef("tags", ColType.LIST, children = listOf(ColumnDef("element", ColType.STRING))),
        )

    @Test
    fun `a NESTED definition survives prepare, and publish creates exactly the receipt's columns`() {
        // The whole chain, because every link broke at once when the
        // codec dropped `children`: prepare accepted the definition
        // (the wire and the OpenAPI both advertise children), stored a
        // gutted one, handed back a receipt promising FLAT field ids —
        // id=1, addr=2, tags=3 where the real assignment is depth-first
        // — and publish then 422'd forever on a definition nobody had
        // sent, with the operation stuck `prepared` and the client's
        // uploaded objects orphaned.
        val catalog = catalog()
        val operation = UUID.randomUUID()
        val prepared =
            creations.prepare(catalog, operation, TableCreationDefinition("test", "nested", nestedColumns))
        assertThat(prepared.state).isEqualTo("prepared")

        // The receipt's definition still has its children...
        assertThat(prepared.definition.columns.map { it.children?.size })
            .containsExactly(null, 2, 1)
        // ...and its promised ids are DEPTH-FIRST over the whole forest.
        assertThat(prepared.columns.flatMap { it.selfAndDescendants() }.map { it.def.name to it.fieldId })
            .containsExactly(
                "id" to 1L,
                "addr" to 2L,
                "zip" to 3L,
                "city" to 4L,
                "tags" to 5L,
                "element" to 6L,
            )

        val published = creations.publish(catalog, operation, listOf(file(prepared)))
        assertThat(published.state).isEqualTo("committed")

        // The PROMISE and the TABLE agree — which is the receipt's whole
        // reason to exist.
        val live = catalogs.getTable(catalog, "test", "nested")
        assertThat(live.columns.flatMap { it.selfAndDescendants() }.map { it.def.name to it.fieldId })
            .isEqualTo(prepared.columns.flatMap { it.selfAndDescendants() }.map { it.def.name to it.fieldId })
        assertThat(live.columns.first { it.def.name == "addr" }.children.map { it.def.name })
            .containsExactly("zip", "city")
    }

    @Test
    fun `replaying a prepare with DIFFERENT children is a conflict, not a match`() {
        // requireSame normalises the stored blob through the codec, so
        // whatever the codec cannot represent is invisible to the
        // comparison. With children dropped, two definitions differing
        // only in their struct's fields compared EQUAL and the second
        // caller silently adopted the first one's table.
        val catalog = catalog()
        val operation = UUID.randomUUID()
        creations.prepare(catalog, operation, TableCreationDefinition("test", "replay", nestedColumns))

        val different =
            nestedColumns.map { column ->
                if (column.name == "addr") {
                    column.copy(children = listOf(ColumnDef("zip", ColType.STRING)))
                } else {
                    column
                }
            }
        assertThatThrownBy {
            creations.prepare(catalog, operation, TableCreationDefinition("test", "replay", different))
        }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("different definition")

        // The control: an IDENTICAL replay still replays.
        val again =
            creations.prepare(catalog, operation, TableCreationDefinition("test", "replay", nestedColumns))
        assertThat(again.state).isEqualTo("prepared")
    }

    @Test
    fun `the column cap counts NODES, not top-level columns`() {
        // A forest counted by its roots hides its real cost by a factor
        // of its fan-out: each node is a hog_column row and a field id,
        // so 10000 two-field structs is 30000 rows under a cap reading
        // 10000.
        val catalog = catalog()

        fun structs(n: Int) =
            (0 until n).map {
                ColumnDef(
                    "s$it",
                    ColType.STRUCT,
                    children = listOf(ColumnDef("a", ColType.INT), ColumnDef("b", ColType.INT)),
                )
            }

        // 3333 structs = 9999 nodes: under the cap.
        val ok = structs(3333)
        assertThat(com.posthog.hoglake.model.nodeCount(ok)).isEqualTo(9999)
        creations.prepare(
            catalog,
            UUID.randomUUID(),
            TableCreationDefinition("test", "capok", ok),
        )

        // 3334 structs = 10002 nodes: over it, and refused by NODE count.
        val tooMany = structs(3334)
        assertThat(com.posthog.hoglake.model.nodeCount(tooMany)).isEqualTo(10002)
        assertThatThrownBy {
            creations.prepare(
                catalog,
                UUID.randomUUID(),
                TableCreationDefinition("test", "capbad", tooMany),
            )
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("10002 nested column nodes")
    }

    @Test
    fun `append validates supplied footer bounds and still accepts omitted metadata`() {
        val catalog = catalog()
        catalogs.createTable(catalog, "test", "target", definition.columns)
        val head = catalogs.getCatalog(catalog)
        val commits = CommitService(db.jdbi)
        val valid = FileRegistration(head.dataPath + "/part.parquet", 1, 100, 20)
        for (file in listOf(
            valid.copy(footerSize = -1),
            valid.copy(footerSize = 93),
            valid.copy(fileSizeBytes = 7, footerSize = 0),
            valid.copy(fileSizeBytes = Long.MIN_VALUE, footerSize = 0),
        )) {
            assertThatThrownBy {
                commits.commit(
                    catalog,
                    com.posthog.hoglake.model.CommitRequest(
                        appends =
                            listOf(
                                com.posthog.hoglake.model.TableAppend("test", "target", listOf(file)),
                            ),
                    ),
                )
            }.isInstanceOf(HoglakeException.Validation::class.java)
            assertThat(catalogs.getCatalog(catalog)).isEqualTo(head)
        }
        commits.commit(
            catalog,
            com.posthog.hoglake.model.CommitRequest(
                appends =
                    listOf(
                        com.posthog.hoglake.model.TableAppend("test", "target", listOf(valid.copy(footerSize = null))),
                    ),
            ),
        )
        assertThat(catalogs.getTable(catalog, "test", "target").fileCount).isEqualTo(1)
    }

    @Test
    fun `versioned definitions use wire names and legacy receipts still retry and publish`() {
        val catalog = catalog()
        val def =
            definition.copy(
                columns =
                    listOf(
                        ColumnDef("id", ColType.LONG),
                        ColumnDef("uuid", ColType.UUID_T),
                        ColumnDef("amount", ColType.DECIMAL, mapOf("precision" to 38, "scale" to 2)),
                    ),
            )
        val prepared = creations.prepare(catalog, UUID.randomUUID(), def)
        db.jdbi.open().use { h ->
            val encoded =
                h.createQuery("SELECT definition::text FROM hog_table_creation WHERE operation_id = :id")
                    .bind("id", prepared.operationId).mapTo(String::class.java).one()
            val tree = com.fasterxml.jackson.databind.ObjectMapper().readTree(encoded)
            assertThat(tree["version"].asInt()).isEqualTo(1)
            assertThat(tree["columns"].map { it["type"].asText() }).containsExactly("long", "uuid", "decimal")
            val legacy = """{"namespace":"test","name":"target","columns":[
                {"name":"id","type":"LONG","typeParams":null,"nullable":true},
                {"name":"uuid","type":"UUID_T","typeParams":null,"nullable":true},
                {"name":"amount","type":"DECIMAL","typeParams":{"precision":38,"scale":2},"nullable":true}]}"""
            h.createUpdate(
                "UPDATE hog_table_creation SET definition = CAST(:definition AS jsonb) WHERE operation_id = :id",
            )
                .bind("definition", legacy).bind("id", prepared.operationId).execute()
        }
        assertThat(creations.status(catalog, prepared.operationId)).isEqualTo(prepared)
        assertThat(creations.prepare(catalog, prepared.operationId, def)).isEqualTo(prepared)
        val committed = creations.publish(catalog, prepared.operationId, emptyList())
        assertThat(committed.state).isEqualTo("committed")
        assertThat(creations.publish(catalog, prepared.operationId, emptyList())).isEqualTo(committed)
    }

    @Test
    fun `prepared and directly created schemas share initial field identities`() {
        val catalog = catalog()
        val columns =
            listOf(
                ColumnDef("number", ColType.LONG, nullable = false),
                ColumnDef("amount", ColType.DECIMAL, mapOf("precision" to 38, "scale" to 2)),
                ColumnDef("text", ColType.STRING),
            )
        val prepared = creations.prepare(catalog, UUID.randomUUID(), definition.copy(columns = columns))
        creations.publish(catalog, prepared.operationId, emptyList())
        val published = catalogs.getTable(catalog, "test", "target")
        val direct = catalogs.createTable(catalog, "test", "direct", columns)
        assertThat(prepared.columns).isEqualTo(published.columns).isEqualTo(direct.columns)
        assertThat(prepared.columns.map { it.fieldId }).containsExactly(1L, 2L, 3L)
    }

    @Test
    fun `prepare status and abort do not wait for unrelated catalog publication`() {
        val catalog = catalog()
        val operation = creations.prepare(catalog, UUID.randomUUID(), definition)
        val id = catalogs.getCatalog(catalog).catalogId
        db.jdbi.open().use { lock ->
            lock.begin()
            com.posthog.hoglake.persistence.Locks.acquireCatalogCommitLock(lock, id)
            try {
                Executors.newVirtualThreadPerTaskExecutor().use { executor ->
                    executor.submit {
                        assertThat(creations.status(catalog, operation.operationId).state).isEqualTo("prepared")
                        assertThat(creations.abort(catalog, operation.operationId).state).isEqualTo("aborted")
                        assertThat(
                            creations.prepare(catalog, UUID.randomUUID(), definition).state,
                        ).isEqualTo("prepared")
                    }.get(5, TimeUnit.SECONDS)
                }
                val bounded = TableCreationService(db.jdbi, catalogs, CommitService(db.jdbi), 100)
                assertThatThrownBy { bounded.publish(catalog, operation.operationId, emptyList()) }
                    .isInstanceOf(HoglakeException.CommitQueueTimeout::class.java)
            } finally {
                lock.rollback()
            }
        }
    }

    @Test
    fun `concurrent preparation returns one identity and rejects different definitions`() {
        val catalog = catalog()
        val id = UUID.randomUUID()
        Executors.newVirtualThreadPerTaskExecutor().use { executor ->
            val futures = (1..8).map { executor.submit<TableCreation> { creations.prepare(catalog, id, definition) } }
            val results = futures.map { it.get(10, TimeUnit.SECONDS) }
            assertThat(results.map { it.tableUuid }.toSet()).hasSize(1)
        }
        assertThatThrownBy { creations.prepare(catalog, id, definition.copy(name = "different")) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
    }

    @Test
    fun `prepare is invisible and retries publish exactly once`() {
        val catalog = catalog()
        val head = catalogs.getCatalog(catalog)
        val id = UUID.randomUUID()
        val prepared = creations.prepare(catalog, id, definition)
        assertThat(creations.prepare(catalog, id, definition)).isEqualTo(prepared)
        assertThat(prepared.columns.map { it.fieldId }).containsExactly(1L)
        assertThat(catalogs.getCatalog(catalog)).isEqualTo(head)
        assertThat(catalogs.listTables(catalog, "test")).isEmpty()
        val committed = creations.publish(catalog, id, listOf(file(prepared)))
        assertThat(committed.state).isEqualTo("committed")
        assertThat(committed.snapshotId).isEqualTo(head.headSnapshotId + 1)
        assertThat(committed.schemaVersion).isEqualTo(head.schemaVersion + 1)
        val table = catalogs.getTable(catalog, "test", "target")
        assertThat(table.tableUuid).isEqualTo(prepared.tableUuid)
        assertThat(table.recordCount).isEqualTo(7)
        assertThat(table.fileCount).isEqualTo(1)
        assertThat(table.fileSizeBytes).isEqualTo(100)
        assertThat(creations.publish(catalog, id, listOf(file(prepared)))).isEqualTo(committed)
        assertThat(creations.abort(catalog, id)).isEqualTo(committed)
        assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(committed.snapshotId)
        assertThatThrownBy {
            creations.publish(catalog, id, emptyList())
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThatThrownBy {
            creations.prepare(catalog, id, definition.copy(name = "different"))
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
        catalogs.dropTable(catalog, "test", "target")
        val replacement = catalogs.createTable(catalog, "test", "target", definition.columns)
        assertThat(creations.publish(catalog, id, listOf(file(prepared)))).isEqualTo(committed)
        assertThat(catalogs.getTable(catalog, "test", "target").tableUuid).isEqualTo(replacement.tableUuid)
    }

    @Test
    fun `invalid registration rolls back table snapshot and receipt`() {
        val catalog = catalog()
        val operation = creations.prepare(catalog, UUID.randomUUID(), definition)
        val head = catalogs.getCatalog(catalog)
        // Negative count is checked by shared registration code, after table rows are written.
        assertThatThrownBy {
            creations.publish(
                catalog,
                operation.operationId,
                listOf(file(operation).copy(recordCount = -1)),
            )
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(catalogs.listTables(catalog, "test")).isEmpty()
        assertThat(catalogs.getCatalog(catalog)).isEqualTo(head)
        assertThat(creations.status(catalog, operation.operationId).state).isEqualTo("prepared")
        assertThat(creations.publish(catalog, operation.operationId, emptyList()).state).isEqualTo("committed")
        assertThat(catalogs.getTable(catalog, "test", "target").fileCount).isZero()
    }

    @Test
    fun `same target has one winner and permanent rejection`() {
        val catalog = catalog()
        val first = creations.prepare(catalog, UUID.randomUUID(), definition)
        val second = creations.prepare(catalog, UUID.randomUUID(), definition)
        Executors.newVirtualThreadPerTaskExecutor().use { executor ->
            val gate = CountDownLatch(1)
            val futures =
                listOf(first, second).map { operation ->
                    executor.submit<TableCreation> {
                        gate.await()
                        creations.publish(catalog, operation.operationId, emptyList())
                    }
                }
            gate.countDown()
            val results = futures.map { it.get(10, TimeUnit.SECONDS) }
            assertThat(results.map { it.state }).containsExactlyInAnyOrder("committed", "rejected")
            val rejected = results.single { it.state == "rejected" }
            catalogs.dropTable(catalog, "test", "target")
            assertThat(creations.publish(catalog, rejected.operationId, emptyList())).isEqualTo(rejected)
            assertThat(catalogs.listTables(catalog, "test")).isEmpty()
        }
    }

    @Test
    fun `concurrent duplicate commit produces one snapshot`() {
        val catalog = catalog()
        val operation = creations.prepare(catalog, UUID.randomUUID(), definition)
        val head = catalogs.getCatalog(catalog).headSnapshotId
        Executors.newVirtualThreadPerTaskExecutor().use { executor ->
            val gate = CountDownLatch(1)
            val results =
                (1..8).map {
                    executor.submit<TableCreation> {
                        gate.await()
                        creations.publish(catalog, operation.operationId, listOf(file(operation)))
                    }
                }
            gate.countDown()
            assertThat(results.map { it.get(10, TimeUnit.SECONDS) }.distinct()).hasSize(1)
        }
        assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(head + 1)
        assertThat(catalogs.getTable(catalog, "test", "target").recordCount).isEqualTo(7)
    }

    @Test
    fun `abort races publication without deleting a committed table`() {
        repeat(10) {
            val catalog = catalog()
            val operation = creations.prepare(catalog, UUID.randomUUID(), definition)
            Executors.newVirtualThreadPerTaskExecutor().use { executor ->
                val gate = CountDownLatch(1)
                val publish =
                    executor.submit<TableCreation> {
                        gate.await()
                        creations.publish(catalog, operation.operationId, emptyList())
                    }
                val abort =
                    executor.submit<TableCreation> {
                        gate.await()
                        creations.abort(catalog, operation.operationId)
                    }
                gate.countDown()
                assertThat(publish.get(10, TimeUnit.SECONDS).state).isEqualTo(abort.get(10, TimeUnit.SECONDS).state)
                val state = creations.status(catalog, operation.operationId).state
                assertThat(state).isIn("committed", "aborted")
                assertThat(catalogs.listTables(catalog, "test").size).isEqualTo(if (state == "committed") 1 else 0)
            }
        }
    }

    @Test
    fun `expiry fences late publication and namespace identity is pinned`() {
        val catalog = catalog()
        val expired = creations.prepare(catalog, UUID.randomUUID(), definition)
        db.jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                """
                UPDATE hog_table_creation SET expires_at = clock_timestamp() - interval '1 second'
                WHERE operation_id = :id
                """,
            )
                .bind("id", expired.operationId).execute()
        }
        assertThat(creations.publish(catalog, expired.operationId, emptyList()).state).isEqualTo("aborted")
        val pending = creations.prepare(catalog, UUID.randomUUID(), definition)
        db.jdbi.useHandle<Exception> { h ->
            h.createUpdate("UPDATE hog_namespace SET name = 'old' WHERE catalog_id = :catalog")
                .bind("catalog", catalogs.getCatalog(catalog).catalogId).execute()
        }
        catalogs.createNamespace(catalog, "test")
        assertThat(creations.publish(catalog, pending.operationId, emptyList()).reason).isEqualTo("namespace_changed")
        assertThat(catalogs.listTables(catalog, "test")).isEmpty()
    }

    @Test
    fun `readers cannot observe creation before publication transaction commits`() {
        val catalog = catalog()
        val prepared = creations.prepare(catalog, UUID.randomUUID(), definition)
        val head = catalogs.getCatalog(catalog).headSnapshotId
        db.jdbi.useHandle<Exception> { h ->
            h.execute(
                """
                CREATE FUNCTION block_creation_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  IF NEW.state = 'committed' THEN PERFORM pg_advisory_xact_lock(836251); END IF;
                  RETURN NEW;
                END $$;
                CREATE TRIGGER block_receipt BEFORE UPDATE ON hog_table_creation
                FOR EACH ROW EXECUTE FUNCTION block_creation_receipt();
            """,
            )
            h.begin()
            h.execute("SELECT pg_advisory_xact_lock(836251)")
            Executors.newVirtualThreadPerTaskExecutor().use { executor ->
                val publish =
                    executor.submit<TableCreation> {
                        creations.publish(catalog, prepared.operationId, listOf(file(prepared)))
                    }
                try {
                    await().atMost(Duration.ofSeconds(10)).until {
                        db.jdbi.withHandle<Boolean, Exception> { check ->
                            check.createQuery(
                                """
                                SELECT EXISTS(SELECT 1 FROM pg_locks
                                WHERE locktype = 'advisory' AND objid = 836251 AND NOT granted)
                                """,
                            )
                                .mapTo(Boolean::class.java).one()
                        }
                    }
                    assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(head)
                    assertThat(catalogs.listTables(catalog, "test")).isEmpty()
                } finally {
                    h.commit()
                }
                assertThat(publish.get(10, TimeUnit.SECONDS).state).isEqualTo("committed")
            }
            h.execute("DROP TRIGGER block_receipt ON hog_table_creation; DROP FUNCTION block_creation_receipt()")
        }
        assertThat(catalogs.getTable(catalog, "test", "target").recordCount).isEqualTo(7)
    }
}
