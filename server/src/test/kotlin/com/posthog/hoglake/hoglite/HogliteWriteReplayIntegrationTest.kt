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
import java.util.concurrent.atomic.AtomicReference

/**
 * Which hoglite writes may invite a replay of their unknown outcome
 * (#682), on the live engine. A relay in front of hoglited applies ONE
 * chosen write and then loses its answer, so the store sees exactly what
 * an owner that failed after durability shows it. The write that
 * converges on replay keeps `replaySafe` (and so its Retry-After); the
 * keyless commit and the DDL, whose replay would apply twice or be
 * refused, do not, and the replay itself is run here to show what the
 * invitation would have met. The catalog drop is the DDL that converges,
 * and has its own case.
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
    ) {
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
        val upstream = HttpClient.newHttpClient()
        val relay = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        relay.createContext("/") { ex ->
            val raw = ex.requestBody.readBytes()
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
            body(Fixture(store, catalog, lose))
        } finally {
            client.close()
            relay.stop(0)
        }
    }

    private val id = ColumnDef("id", ColType.LONG)
    private val widen = listOf(AlterOp.AddColumn(ColumnDef("c2", ColType.LONG)))

    @ParameterizedTest
    @ValueSource(
        strings = [
            "commit-keyed", "offset", "options-patch", "containment-mark", "containment-clear",
            "upload-claim", "upload-renew", "upload-abandon",
        ],
    )
    fun `a write whose replay converges keeps its retry invitation and a replay lands the same state`(write: String) =
        fixture { f ->
            val c = f.catalog
            f.store.createNamespace(c, "ns")
            val t = f.store.createTable(c, "ns", "t", listOf(id))
            val head = f.append("a.parquet").snapshotId
            val key = UUID.randomUUID()
            val owner = UUID.randomUUID()
            val upload = UUID.randomUUID()
            val team = NamespaceContainment("team_id", 7)
            f.store.createNamespace(c, "team_7")
            if (write == "containment-clear") f.store.setNamespaceContainment(c, "team_7", team, "op")
            if (write != "upload-claim") f.store.uploads.claim(c, upload, owner, "s3://hoglake-it/$c/up", "data")
            val claimed = f.rows("SELECT path FROM uploads WHERE upload_id = ?", upload.toString()).map { it[0] }

            val prefix =
                when (write) {
                    "commit-keyed" -> "ch"
                    "offset" -> "offset"
                    "options-patch" -> "options-patch"
                    "containment-mark", "containment-clear" -> "ns-containment"
                    else -> write
                }
            val action: () -> Any =
                when (write) {
                    "commit-keyed" -> ({ f.append("b.parquet", key) })
                    "offset" -> ({ f.store.commitOffset(c, "reader", t.tableUuid, head) })
                    "options-patch" -> ({ f.store.options.patch(c, PatchField.Set(3600L), true, "test", "lower") })
                    "containment-mark" -> ({ f.store.setNamespaceContainment(c, "team_7", team, "op") })
                    "containment-clear" -> ({ f.store.setNamespaceContainment(c, "team_7", null, "op") })
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
                        runCatching { f.store.getOffset(c, "reader", t.tableUuid).committedSnapshot }.getOrNull(),
                        f.store.options.get(c),
                        f.store.liveContainment(c, "team_7"),
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
     * The one DDL whose replay converges, so it keeps the default and its
     * Retry-After: the drop's fence transaction opts out of the fence, so
     * a lost answer leaves the catalog fenced but present (nothing after
     * the fence ran), and a replay passes the same emptiness guards, sets
     * `dropping = 1` again and finishes the release and delete.
     */
    @Test
    fun `a catalog drop whose fence answer is lost keeps its retry invitation and a replay finishes the drop`() =
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
            assertThat(unknown.replay).isEqualTo(WriteReplay.CONVERGES)
            assertThat(unknown.replaySafe).isTrue()
            assertThat(unknown.message).doesNotContain("Do not retry it blindly")
            // Fenced, not dropped: the release and delete never ran.
            assertThat(f.store.getCatalog(c2).name).isEqualTo(c2)
            assertThat(dropping()).isEqualTo(1L)

            val dropped = f.store.dropCatalog(c2, "test", "drop")
            assertThat(dropped.name).isEqualTo(c2)
            assertThatThrownBy { f.store.getCatalog(c2) }.isInstanceOf(HoglakeException.NotFound::class.java)
        }
}
