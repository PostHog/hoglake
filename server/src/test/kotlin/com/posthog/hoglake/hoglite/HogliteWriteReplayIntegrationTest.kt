package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.WriteReplay
import com.posthog.hoglake.service.PatchField
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Which hoglite writes may invite a replay of their unknown outcome
 * (#682), on the live engine. A relay in front of hoglited applies ONE
 * chosen write and then loses its answer, so the store sees exactly what
 * an owner that failed after durability shows it. The write that
 * converges on replay keeps `replaySafe` (and so its Retry-After); the
 * keyless commit, the unconditional set and the DDL, whose replay would
 * apply twice, undo a write that landed in between, or be refused, do
 * not, and the replay itself is run here to show what the invitation
 * would have met. The catalog drop has its own cases: its hazard is a
 * catalog recreated under the name. The relay can also let a DDL's
 * answer through and then fail every read after it, so a DDL that read
 * its answer after the commit would fail for a read that spent its retry
 * budget, or answer one chosen engine request with a fixed status.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteWriteReplayIntegrationTest {
    private val engine by lazy { HogliteConformanceEngine() }
    private var started = false
    private val json = jacksonObjectMapper()

    @AfterAll
    fun close() {
        if (started) engine.close()
    }

    /** A store behind the relay, its catalog, and the switch that loses one answer. */
    private inner class Fixture(
        val store: HogliteCatalogStore,
        val catalog: String,
        private val lose: AtomicReference<String?>,
        private val downAfter: AtomicReference<String?>,
        private val down: AtomicBoolean,
        private val refuse: AtomicReference<Pair<Regex, Int>?>,
    ) {
        /**
         * Runs [write] with every engine request whose `METHOD path`
         * matches [request] answered [status] by the relay, never
         * forwarded, and returns what [write] threw.
         */
        fun refusing(
            request: Regex,
            status: Int,
            write: () -> Unit,
        ): Throwable? {
            refuse.set(request to status)
            try {
                return catchThrowable(write)
            } finally {
                refuse.set(null)
            }
        }

        /**
         * Runs [write] with the answer of its first `prefix:` transaction
         * lost after the engine applied it, and returns the unknown outcome
         * the store raised for it.
         */
        fun losing(
            prefix: String,
            write: () -> Unit,
        ): HoglakeException.CommitOutcomeUnknown {
            lose.set(prefix)
            val thrown = catchThrowable(write)
            assertThat(lose.get()).describedAs("a '$prefix' transaction ran").isNull()
            assertThat(thrown).isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            val unknown = thrown as HoglakeException.CommitOutcomeUnknown
            assertThat(unknown.txid).startsWith("$prefix:")
            return unknown
        }

        /**
         * Runs [write] with every request after its first `prefix:`
         * transaction answered 503 `not_applied`, so the transaction
         * commits and any read after it spends its retry budget, and
         * returns what [write] answered.
         */
        fun <T> readsFailingAfter(
            prefix: String,
            write: () -> T,
        ): T {
            downAfter.set(prefix)
            try {
                val answered = write()
                assertThat(downAfter.get()).describedAs("a '$prefix' transaction committed").isNull()
                return answered
            } finally {
                downAfter.set(null)
                down.set(false)
            }
        }

        /** Rows straight from the engine, past the relay. */
        fun rows(
            sql: String,
            vararg args: Any?,
        ): List<List<Any?>> =
            engine.client.query(HogliteConformanceEngine.TENANTS.tenantFor(catalog), sql, args.toList()).rows

        fun append(
            path: String,
            key: UUID? = null,
        ) = store.commit(
            catalog,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(FileRegistration("s3://hoglake-it/$catalog/data/ns/t/$path", 10, 100, 50)),
                        ),
                    ),
                idempotencyKey = key,
            ),
        )

        /** The live file paths of ns.t, duplicates included: what a commit's replay must not add to. */
        fun paths(): List<String> = store.listFiles(catalog, "ns", "t").map { it.path }
    }

    private fun fixture(body: (Fixture) -> Unit) {
        val running = engine
        started = true
        val lose = AtomicReference<String?>(null)
        val downAfter = AtomicReference<String?>(null)
        val down = AtomicBoolean(false)
        val refuse = AtomicReference<Pair<Regex, Int>?>(null)
        val upstream = HttpClient.newHttpClient()
        val relay = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        relay.createContext("/") { ex ->
            val raw = ex.requestBody.readBytes()
            if (down.get()) {
                // Retried by the client as not applied until its budget is spent.
                val bytes = "{\"code\":\"not_applied\",\"error\":\"reads down\"}".toByteArray()
                ex.sendResponseHeaders(503, bytes.size.toLong())
                ex.responseBody.use { it.write(bytes) }
                return@createContext
            }
            val refusal = refuse.get()
            if (refusal != null && refusal.first.matches("${ex.requestMethod} ${ex.requestURI.path}")) {
                val bytes = "{\"error\":\"refused by the relay\"}".toByteArray()
                ex.sendResponseHeaders(refusal.second, bytes.size.toLong())
                ex.responseBody.use { it.write(bytes) }
                return@createContext
            }
            val txid = if (raw.isEmpty()) "" else json.readTree(raw)["txid"]?.asText().orEmpty()
            val prefix = lose.get()
            val losing = prefix != null && txid.startsWith("$prefix:") && lose.compareAndSet(prefix, null)
            val answer =
                upstream.send(
                    HttpRequest.newBuilder(URI.create(running.url() + ex.requestURI))
                        .header("Content-Type", "application/json")
                        .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(raw)).build(),
                    HttpResponse.BodyHandlers.ofByteArray(),
                )
            // The ambiguity is injected AFTER the apply: an unlabelled 422,
            // which HogliteClient reads as an outcome it cannot know (a
            // 504 would be retried under the same txid and answered from
            // the dedup table, which is the server's own exactly-once and
            // not what this test is about).
            if (losing) check(answer.statusCode() == 200) { "the lost write must have applied: ${answer.statusCode()}" }
            val after = downAfter.get()
            if (after != null && txid.startsWith("$after:") && answer.statusCode() == 200 &&
                downAfter.compareAndSet(after, null)
            ) {
                down.set(true)
            }
            val bytes = if (losing) "{\"error\":\"lost applied response\"}".toByteArray() else answer.body()
            ex.sendResponseHeaders(
                if (losing) 422 else answer.statusCode(),
                if (bytes.isEmpty()) -1 else bytes.size.toLong(),
            )
            ex.responseBody.use { it.write(bytes) }
        }
        relay.start()
        val client = HogliteClient("http://127.0.0.1:${relay.address.port}", sleeper = {})
        val store = HogliteCatalogStore(client, HogliteConformanceEngine.TENANTS)
        try {
            val catalog = HogliteConformanceEngine.uniqueName("replay")
            store.createCatalog(catalog, "s3://hoglake-it/$catalog")
            body(Fixture(store, catalog, lose, downAfter, down, refuse))
        } finally {
            client.close()
            relay.stop(0)
        }
    }

    private val id = ColumnDef("id", ColType.LONG)
    private val widen = listOf(AlterOp.AddColumn(ColumnDef("c2", ColType.LONG)))

    @ParameterizedTest
    @ValueSource(
        strings = ["commit-keyed", "upload-claim", "upload-renew", "upload-abandon"],
    )
    fun `a write whose replay converges keeps its retry invitation and a replay lands the same state`(write: String) =
        fixture { f ->
            val c = f.catalog
            f.store.createNamespace(c, "ns")
            f.store.createTable(c, "ns", "t", listOf(id))
            f.append("a.parquet")
            val key = UUID.randomUUID()
            val owner = UUID.randomUUID()
            val upload = UUID.randomUUID()
            if (write != "upload-claim") f.store.uploads.claim(c, upload, owner, "s3://hoglake-it/$c/up", "data")
            val claimed = f.rows("SELECT path FROM uploads WHERE upload_id = ?", upload.toString()).map { it[0] }

            val prefix = if (write == "commit-keyed") "ch" else write
            val action: () -> Any =
                when (write) {
                    "commit-keyed" -> ({ f.append("b.parquet", key) })
                    "upload-claim" -> ({ f.store.uploads.claim(c, upload, owner, "s3://hoglake-it/$c/up", "data") })
                    "upload-renew" -> ({ f.store.uploads.renew(c, owner) })
                    else -> ({ f.store.uploads.abandon(c, owner, claimed.map { it as String }) })
                }
            // What the replay must leave as it found it: expires_at moves
            // with the clock on a renewal, so it is not part of the state.
            val state: () -> Any =
                {
                    listOf(
                        f.paths(),
                        f.rows("SELECT upload_id, owner, prefix, path, state FROM uploads ORDER BY upload_id"),
                    )
                }

            val unknown = f.losing(prefix) { action() }
            assertThat(unknown.replay).isEqualTo(WriteReplay.CONVERGES)
            assertThat(unknown.replaySafe).isTrue()
            assertThat(unknown.message).contains("idempotency_key")
            val applied = state()
            assertThatCode { action() }.describedAs("the blind replay of '$write'").doesNotThrowAnyException()
            assertThat(state()).describedAs("the state after the replay of '$write'").isEqualTo(applied)
        }

    /**
     * A write that sets a value without a guard on the one it replaces: a
     * replay alone would land the same state, but the replay the header
     * invites runs after whatever another writer did in between, and
     * undoes it or is refused by it.
     */
    @ParameterizedTest
    @ValueSource(strings = ["options-patch", "containment-mark", "containment-clear", "offset"])
    fun `an unconditional set is reported without a retry invitation because its replay undoes a later write`(
        write: String,
    ) = fixture { f ->
        val c = f.catalog
        f.store.createNamespace(c, "ns")
        val t = f.store.createTable(c, "ns", "t", listOf(id))
        val first = f.append("a.parquet").snapshotId
        val team = NamespaceContainment("team_id", 7)
        f.store.createNamespace(c, "team_7")
        if (write == "containment-clear") f.store.setNamespaceContainment(c, "team_7", team, "op")

        val prefix = if (write.startsWith("containment")) "ns-containment" else write
        val action: () -> Any =
            when (write) {
                "options-patch" -> ({ f.store.options.patch(c, PatchField.Absent, false, "test", "floor off") })
                "containment-mark" -> ({ f.store.setNamespaceContainment(c, "team_7", team, "op") })
                "containment-clear" -> ({ f.store.setNamespaceContainment(c, "team_7", null, "op") })
                else -> ({ f.store.commitOffset(c, "reader", t.tableUuid, first) })
            }
        val unknown = f.losing(prefix) { action() }
        assertThat(unknown.replay).isEqualTo(WriteReplay.UNCONDITIONAL)
        assertThat(unknown.replaySafe).isFalse()
        assertThat(unknown.message)
            .contains("Do not retry it blindly", "undo", unknown.txid)
            .doesNotContain("idempotency_key")
        assertThat(unknown.cause).isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)

        // Another writer lands in between, and this is what the replay the
        // header would have invited does to it.
        when (write) {
            "options-patch" -> {
                assertThat(f.store.options.get(c).consumerFloor).isFalse()
                f.store.options.patch(c, PatchField.Absent, true, "other", "floor on")
                action()
                assertThat(f.store.options.get(c).consumerFloor).describedAs("re-enabled in between").isFalse()
            }
            "containment-mark" -> {
                assertThat(f.store.liveContainment(c, "team_7")).isEqualTo(team)
                f.store.setNamespaceContainment(c, "team_7", null, "other")
                action()
                assertThat(f.store.liveContainment(c, "team_7")).describedAs("cleared in between").isEqualTo(team)
            }
            "containment-clear" -> {
                assertThat(f.store.liveContainment(c, "team_7")).isNull()
                f.store.setNamespaceContainment(c, "team_7", team, "other")
                action()
                assertThat(f.store.liveContainment(c, "team_7")).describedAs("re-marked in between").isNull()
            }
            else -> {
                assertThat(f.store.getOffset(c, "reader", t.tableUuid).committedSnapshot).isEqualTo(first)
                val second = f.append("b.parquet").snapshotId
                f.store.commitOffset(c, "reader", t.tableUuid, second)
                assertThatThrownBy { action() }.isInstanceOf(HoglakeException.OffsetRegression::class.java)
                assertThat(f.store.getOffset(c, "reader", t.tableUuid).committedSnapshot).isEqualTo(second)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            "commit-keyless", "create-catalog", "create-ns", "drop-ns", "create-table", "drop-table", "truncate",
            "alter", "view-create", "view-drop",
        ],
    )
    fun `a write whose replay would not converge is reported without a retry invitation`(ddl: String) =
        fixture { f ->
            val c = f.catalog
            val c2 = HogliteConformanceEngine.uniqueName("replay")
            f.store.createNamespace(c, "ns")
            val t = f.store.createTable(c, "ns", "t", listOf(id))
            if (ddl == "view-drop") f.store.views.create(c, "ns", "v", "SELECT 1", "trino", "test", "define")
            if (ddl == "drop-ns") f.store.createNamespace(c, "empty")

            val action: () -> Any =
                when (ddl) {
                    "commit-keyless" -> ({ f.append("b.parquet") })
                    "create-catalog" -> ({ f.store.createCatalog(c2, "s3://hoglake-it/$c2", "test", "create") })
                    "create-ns" -> ({ f.store.createNamespace(c, "fresh", "test", "create") })
                    "drop-ns" -> ({ f.store.dropNamespace(c, "empty", null, "test", "drop", false) })
                    "create-table" -> ({ f.store.createTable(c, "ns", "u", listOf(id), "test", "create") })
                    "drop-table" -> ({ f.store.dropTable(c, "ns", "t", t.tableUuid, "test", "drop") })
                    "truncate" -> ({ f.store.truncateTable(c, "ns", "t", t.tableUuid, "test", "clear") })
                    "alter" -> ({ f.store.alters.alterTable(c, "ns", "t", widen, t.tableUuid, null, "test", "widen") })
                    "view-create" -> ({ f.store.views.create(c, "ns", "v", "SELECT 1", "trino", "test", "define") })
                    else -> ({ f.store.views.drop(c, "ns", "v", "test", "drop") })
                }

            val unknown = f.losing(if (ddl == "commit-keyless") "ch" else ddl) { action() }
            assertThat(unknown.replaySafe).isFalse()
            assertThat(unknown.message).contains("Do not retry it blindly", unknown.txid)
            if (ddl == "commit-keyless") {
                // The route takes a key; the detail says to send one.
                assertThat(unknown.replay).isEqualTo(WriteReplay.KEYLESS_COMMIT)
                assertThat(unknown.message).contains("a second time", "send an idempotency_key")
            } else {
                assertThat(unknown.replay).isEqualTo(WriteReplay.DDL)
                assertThat(unknown.message).contains("already applied").doesNotContain("idempotency_key")
            }
            // The client's own report is kept underneath, for operators.
            assertThat(unknown.cause).isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            assertThat((unknown.cause as HoglakeException.CommitOutcomeUnknown).txid).isEqualTo(unknown.txid)

            // The write landed, and this is what a replay the header invited
            // would have met: a refusal that reads as the DDL's own failure;
            // for truncate, a second truncate that ends what a writer
            // committed in between; for a keyless commit, the same file
            // registered twice (duplicate paths are legal) and a 200.
            when (ddl) {
                "commit-keyless" -> {
                    val once = f.paths()
                    assertThat(once).containsExactly("s3://hoglake-it/$c/data/ns/t/b.parquet")
                    action()
                    assertThat(f.paths()).isEqualTo(once + once)
                }
                "create-catalog" ->
                    assertThatThrownBy { action() }.isInstanceOf(HoglakeException.AlreadyExists::class.java)
                "create-ns", "create-table", "view-create" ->
                    assertThatThrownBy { action() }.isInstanceOf(HoglakeException.AlreadyExists::class.java)
                "drop-ns", "drop-table", "view-drop" ->
                    assertThatThrownBy { action() }.isInstanceOf(HoglakeException.NotFound::class.java)
                "alter" ->
                    assertThatThrownBy { action() }
                        .isInstanceOf(HoglakeException.Validation::class.java)
                        .hasMessageContaining("already exists")
                "truncate" -> {
                    val between = f.append("between.parquet")
                    assertThat(f.store.listFiles(c, "ns", "t")).hasSize(1)
                    action()
                    assertThat(f.store.listFiles(c, "ns", "t")).isEmpty()
                    assertThat(f.store.listFiles(c, "ns", "t", snapshot = between.snapshotId)).hasSize(1)
                }
            }
        }

    /**
     * A DDL answers from its own transaction, as PostgreSQL's alter does:
     * every read after the commit fails here, and a DDL that read its
     * answer after the commit would fail with BackendUnavailable (503 with
     * Retry-After) for a write that applied.
     */
    @Test
    fun `a DDL answers from its own transaction when every read after it fails`() =
        fixture { f ->
            val c = f.catalog
            val c2 = HogliteConformanceEngine.uniqueName("replay")
            f.store.createNamespace(c, "ns")
            val t = f.store.createTable(c, "ns", "t", listOf(id))
            f.append("a.parquet")

            val created = f.readsFailingAfter("create-catalog") { f.store.createCatalog(c2, "s3://hoglake-it/$c2") }
            assertThat(created).isEqualTo(f.store.getCatalog(c2))
            assertThat(created.name).isEqualTo(c2)
            assertThat(created.headSnapshotId).isEqualTo(0L)

            val altered =
                f.readsFailingAfter("alter") {
                    f.store.alters.alterTable(c, "ns", "t", widen, t.tableUuid, null, "test", "widen")
                }
            // Exact for the alter's own snapshot: the file landed before it.
            assertThat(altered.snapshotId).isEqualTo(f.store.getCatalog(c).headSnapshotId)
            assertThat(listOf(altered.recordCount, altered.fileCount, altered.fileSizeBytes))
                .isEqualTo(listOf(10L, 1L, 100L))
            assertThat(altered.columns.map { it.def.name }).containsExactly("id", "c2")
        }

    /**
     * The catalog drop is a DDL like the others: the request names the
     * catalog, not the incarnation the fence protects. A lost fence answer
     * leaves the catalog fenced but present; another operator finishes the
     * drop and recreates the name, and the replay the header would have
     * invited passes the same emptiness guards against the empty
     * replacement and deletes it.
     */
    @Test
    fun `a catalog drop whose fence answer is lost is reported without a retry invitation`() =
        fixture { f ->
            val c2 = HogliteConformanceEngine.uniqueName("replay")
            f.store.createCatalog(c2, "s3://hoglake-it/$c2", "test", "create")
            val tenant = HogliteConformanceEngine.TENANTS.tenantFor(c2)

            fun dropping(): Long =
                engine.client
                    .query(tenant, "SELECT dropping FROM meta WHERE id = 1", emptyList())
                    .firstRow()!![0]
                    .asLong()
            assertThat(dropping()).isEqualTo(0L)

            val unknown = f.losing("drop-fence") { f.store.dropCatalog(c2, "test", "drop") }
            assertThat(unknown.replay).isEqualTo(WriteReplay.DDL)
            assertThat(unknown.replaySafe).isFalse()
            assertThat(unknown.message).contains("Do not retry it blindly", unknown.txid)
            // The read the DDL detail advises does not show the fence; the detail names what does.
            assertThat(unknown.message).contains("(it is being dropped), sending DELETE again finishes the drop")
            assertThat(unknown.cause).isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            // Fenced, not dropped: the release and delete never ran.
            assertThat(f.store.getCatalog(c2).name).isEqualTo(c2)
            assertThat(dropping()).isEqualTo(1L)

            f.store.dropCatalog(c2, "other", "finish")
            val replacement = f.store.createCatalog(c2, "s3://hoglake-it/$c2-new", "other", "recreate")
            assertThat(replacement.dataPath).isEqualTo("s3://hoglake-it/$c2-new")
            f.store.dropCatalog(c2, "test", "drop")
            assertThatThrownBy { f.store.getCatalog(c2) }
                .describedAs("the replacement")
                .isInstanceOf(HoglakeException.NotFound::class.java)
        }

    /**
     * A failure after the drop's fence applied (here every request after
     * it fails, so the release spends its retry budget) is the fence's
     * unknown outcome, not a `backend_unavailable` whose Retry-After
     * would invite the same replay.
     */
    @Test
    fun `a catalog drop that fails after its fence is the fence's unknown outcome`() =
        fixture { f ->
            val c2 = HogliteConformanceEngine.uniqueName("replay")
            f.store.createCatalog(c2, "s3://hoglake-it/$c2", "test", "create")

            val thrown =
                catchThrowable { f.readsFailingAfter("drop-fence") { f.store.dropCatalog(c2, "test", "drop") } }
            assertThat(thrown).isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            val unknown = thrown as HoglakeException.CommitOutcomeUnknown
            assertThat(unknown.txid).startsWith("drop-fence:")
            assertThat(unknown.replay).isEqualTo(WriteReplay.DDL)
            assertThat(unknown.message).contains("the catalog is fenced", "Do not retry it blindly")
            assertThat(unknown.cause).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            // The catalog is still there, fenced: an operator who read it drops it again.
            assertThat(f.store.getCatalog(c2).name).isEqualTo(c2)
            f.store.dropCatalog(c2, "test", "drop")
            assertThatThrownBy { f.store.getCatalog(c2) }.isInstanceOf(HoglakeException.NotFound::class.java)
        }

    /**
     * A final engine refusal after the drop's fence applied (a proxy's
     * 502 on the release, a 422 on the tenant delete) is the fence's
     * unknown outcome too, not a 500 or a 422 that reads as the drop's
     * own refusal of a catalog it left fenced.
     */
    @ParameterizedTest
    @ValueSource(strings = ["release-502", "delete-422"])
    fun `a catalog drop refused after its fence is the fence's unknown outcome`(refusal: String) =
        fixture { f ->
            val c2 = HogliteConformanceEngine.uniqueName("replay")
            f.store.createCatalog(c2, "s3://hoglake-it/$c2", "test", "create")
            val release = refusal == "release-502"
            val request = if (release) Regex("POST .*/release") else Regex("DELETE /v1/tenants/.*")
            val cause =
                if (release) {
                    HoglakeException.BackendUnavailable::class.java
                } else {
                    HoglakeException.Validation::class.java
                }

            val thrown = f.refusing(request, if (release) 502 else 422) { f.store.dropCatalog(c2, "test", "drop") }
            assertThat(thrown).isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            val unknown = thrown as HoglakeException.CommitOutcomeUnknown
            assertThat(unknown.txid).startsWith("drop-fence:")
            assertThat(unknown.replay).isEqualTo(WriteReplay.DDL)
            assertThat(unknown.message).contains("the catalog is fenced", "sending DELETE again finishes the drop")
            assertThat(unknown.cause).isInstanceOf(cause)
            // Fenced: a write is refused as being dropped, and DELETE again finishes the drop.
            assertThatThrownBy { f.store.createNamespace(c2, "late") }
                .isInstanceOf(HoglakeException.NotFound::class.java)
                .hasMessageContaining("being dropped")
            f.store.dropCatalog(c2, "test", "drop")
            assertThatThrownBy { f.store.getCatalog(c2) }.isInstanceOf(HoglakeException.NotFound::class.java)
        }
}
