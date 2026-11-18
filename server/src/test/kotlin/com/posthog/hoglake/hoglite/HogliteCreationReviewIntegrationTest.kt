package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.service.TableCreationDefinition
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/** Regression cases for review 5802232251, against real guarded transactions. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteCreationReviewIntegrationTest {
    private var started = false
    private val engine by lazy { HogliteConformanceEngine().also { started = true } }
    private val store get() = engine.store

    @AfterAll
    fun close() {
        if (started) engine.close()
    }

    private fun catalog(): String =
        HogliteConformanceEngine.uniqueName("review").also {
            store.createCatalog(it, "s3://hoglake-it/$it")
            store.createNamespace(it, "ns")
        }

    private val definition = TableCreationDefinition("ns", "t", listOf(ColumnDef("a", ColType.INT)))
    private val containment = NamespaceContainment("team_id", 42)

    @Test
    fun `prepare and publish preserve a contained namespace prefix`() {
        val cat = catalog()
        store.setNamespaceContainment(cat, "ns", containment, null)
        val op = UUID.randomUUID()
        val prepared = store.tableCreations.prepare(cat, op, definition)
        assertThat(prepared.writePath).isEqualTo("s3://hoglake-it/$cat/data/ns/${prepared.tableUuid}/")
        assertThat(
            store.tableCreations.publish(cat, op, listOf(FileRegistration(prepared.writePath + "a", 1, 100, 20))).state,
        )
            .isEqualTo("committed")
    }

    @Test
    fun `containment enabled after prepare refuses publication without side effects`() {
        val cat = catalog()
        val op = UUID.randomUUID()
        val prepared = store.tableCreations.prepare(cat, op, definition)
        store.setNamespaceContainment(cat, "ns", containment, null)
        val publications = AtomicInteger()
        throughProxy({ path, body ->
            if (path.endsWith("/tx") && "INSERT INTO tables" in body) publications.incrementAndGet()
        }) { racing ->
            assertThatThrownBy {
                racing.tableCreations.publish(cat, op, listOf(FileRegistration(prepared.writePath + "a", 1, 100, 20)))
            }.isExactlyInstanceOf(HoglakeException.ContainmentViolation::class.java)
        }
        assertThat(publications.get()).isZero()
        assertThat(store.tableCreations.status(cat, op).state).isEqualTo("prepared")
        assertThat(store.listTables(cat, "ns")).isEmpty()
    }

    private fun throughProxy(
        before: (String, String) -> Unit,
        after: (String, String) -> Unit = { _, _ -> },
        run: (HogliteCatalogStore) -> Unit,
    ) {
        val http = HttpClient.newHttpClient()
        val proxy = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        proxy.createContext("/") { ex ->
            try {
                val body = ex.requestBody.readBytes()
                before(ex.requestURI.path, body.decodeToString())
                val response =
                    http.send(
                        HttpRequest.newBuilder(URI.create(engine.url() + ex.requestURI))
                            .header("Content-Type", "application/json")
                            .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                        HttpResponse.BodyHandlers.ofByteArray(),
                    )
                after(ex.requestURI.path, body.decodeToString())
                ex.sendResponseHeaders(response.statusCode(), response.body().size.toLong())
                ex.responseBody.use { it.write(response.body()) }
            } finally {
                ex.close()
            }
        }
        proxy.start()
        try {
            HogliteClient("http://127.0.0.1:${proxy.address.port}", sleeper = {}).use {
                run(HogliteCatalogStore(it, HogliteConformanceEngine.TENANTS))
            }
        } finally {
            proxy.stop(0)
        }
    }

    @Test
    fun `containment enabled after publish preflight trips the transaction guard`() {
        val cat = catalog()
        val op = UUID.randomUUID()
        val prepared = store.tableCreations.prepare(cat, op, definition)
        val intercepted = AtomicInteger()
        throughProxy({ path, body ->
            if (path.endsWith("/tx") && "INSERT INTO tables" in body && intercepted.incrementAndGet() == 1) {
                store.setNamespaceContainment(cat, "ns", containment, null)
            }
        }) { racing ->
            assertThatThrownBy {
                racing.tableCreations.publish(cat, op, listOf(FileRegistration(prepared.writePath + "a", 1, 100, 20)))
            }.isExactlyInstanceOf(HoglakeException.ContainmentViolation::class.java)
        }
        assertThat(intercepted.get()).isEqualTo(1)
        assertThat(store.listTables(cat, "ns")).isEmpty()
        assertThat(store.tableCreations.status(cat, op).state).isEqualTo("prepared")
    }

    @Test
    fun `publish rejects unsafe path suffixes`() {
        val cat = catalog()
        val op = UUID.randomUUID()
        val prepared = store.tableCreations.prepare(cat, op, definition)
        val before = store.getCatalog(cat).headSnapshotId
        for (suffix in listOf("../other/a", "./a", "%2e%2e/a", "a%2fb", "a\\b", "a b", "a//b", "a/")) {
            assertThatThrownBy {
                store.tableCreations.publish(cat, op, listOf(FileRegistration(prepared.writePath + suffix, 1, 100, 20)))
            }.describedAs(suffix).isExactlyInstanceOf(HoglakeException.Validation::class.java)
            assertThat(store.tableCreations.status(cat, op).state).isEqualTo("prepared")
            assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(before)
        }
        assertThat(
            store.tableCreations.publish(
                cat,
                op,
                listOf(FileRegistration(prepared.writePath + "ok", 1, 100, 20)),
            ).state,
        )
            .isEqualTo("committed")
    }
}
