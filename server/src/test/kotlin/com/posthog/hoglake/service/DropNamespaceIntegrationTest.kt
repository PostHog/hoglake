package com.posthog.hoglake.service

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.SnapshotChange
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.persistence.SnapshotRepo
import com.posthog.hoglake.testing.PgTestSupport
import net.logstash.logback.encoder.LogstashEncoder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * dropNamespace, at the service level: the snapshot-change row it records,
 * the commit-lock serialization of a drop against a concurrent create into
 * the same namespace, and the audit line it emits. The wire-level status
 * codes live in ApiIntegrationTest; these assert what the wire cannot see.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DropNamespaceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val catalogs = CatalogService(jdbi)
    private val json = ObjectMapper()

    // ---- audit capture (AuditIntegrationTest's harness, per-test) --------

    private class CapturingAppender(private val encoder: LogstashEncoder) :
        AppenderBase<ILoggingEvent>() {
        val lines = CopyOnWriteArrayList<String>()

        override fun append(event: ILoggingEvent) {
            lines += String(encoder.encode(event))
        }
    }

    private lateinit var capture: CapturingAppender

    @BeforeEach
    fun attachAppender() {
        val ctx = LoggerFactory.getILoggerFactory() as LoggerContext
        val encoder =
            LogstashEncoder().apply {
                context = ctx
                setIncludeMdc(false)
                start()
            }
        capture =
            CapturingAppender(encoder).apply {
                context = ctx
                start()
            }
        (LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger).addAppender(capture)
    }

    @AfterEach
    fun detachAppender() {
        (LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger).detachAppender(capture)
        capture.stop()
    }

    @org.junit.jupiter.api.AfterAll
    fun tearDown() = db.close()

    private fun auditLines(): List<com.fasterxml.jackson.databind.JsonNode> = capture.lines.map { json.readTree(it) }

    // ---- tests ------------------------------------------------------------

    @Test
    fun `drop records a namespace_dropped change row keyed by the namespace id`() {
        val cat = "nsdrop-changefeed"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        val namespaceId = catalogs.getNamespace(cat, "ns").namespaceId

        val result = catalogs.dropNamespace(cat, "ns")

        // The drop snapshot's change feed is exactly one namespace_dropped
        // row, carrying the namespace's id (not a table's, not a catalog's).
        val catalogId = catalogs.getCatalog(cat).catalogId
        val changes =
            jdbi.withHandleUnchecked { h ->
                SnapshotRepo.changesFor(h, catalogId, result.snapshotId, result.snapshotId)
            }
        assertThat(changes[result.snapshotId])
            .containsExactly(SnapshotChange(ChangeKind.NAMESPACE_DROPPED, namespaceId))
    }

    @Test
    fun `drop audits one namespace_drop line with the committed snapshot`() {
        val cat = "nsdrop-audit"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")

        val result = catalogs.dropNamespace(cat, "ns")

        val drops =
            auditLines().filter { it["action"]?.asText() == "namespace_drop" && it["catalog"]?.asText() == cat }
        assertThat(drops).hasSize(1)
        val line = drops.single()
        assertThat(line["outcome"].asText()).isEqualTo("ok")
        assertThat(line["object"].asText()).isEqualTo("ns")
        assertThat(line["detail"].asText()).isEqualTo("snapshot=${result.snapshotId}")
    }

    @Test
    fun `a refused drop audits the refusal, not a success`() {
        val cat = "nsdrop-audit-refuse"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG, nullable = false)))

        assertThatThrownBy { catalogs.dropNamespace(cat, "ns") }
            .isInstanceOf(HoglakeException.NamespaceNotEmpty::class.java)

        val drops =
            auditLines().filter { it["action"]?.asText() == "namespace_drop" && it["catalog"]?.asText() == cat }
        // Exactly one line, and it names the failure — never an 'ok' for a
        // namespace that is still live.
        assertThat(drops).hasSize(1)
        assertThat(drops.single()["outcome"].asText()).isNotEqualTo("ok")
        // And the namespace really is still live.
        assertThat(catalogs.listNamespaces(cat).map { it.name }).containsExactly("ns")
    }

    @Test
    fun `drop serializes against a concurrent create into the same namespace - exactly one wins`() {
        val cat = "nsdrop-race"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")

        // One thread drops, one creates a table into the same namespace.
        // Both take the per-catalog commit lock, so they cannot interleave:
        // either the create lands first (drop then sees a live table and
        // 409s) or the drop lands first (create then finds no live namespace
        // and 404s). What must NOT happen is a table created into a dropped
        // namespace (silent orphan) or both succeeding (drop claims empty
        // while a table exists).
        val pool = Executors.newFixedThreadPool(2)
        val start = CountDownLatch(1)
        val createOk = AtomicInteger(0)
        val dropOk = AtomicInteger(0)
        val createConflict = AtomicInteger(0)
        val dropConflict = AtomicInteger(0)
        val unexpected = CopyOnWriteArrayList<Throwable>()

        val create =
            pool.submit {
                start.await()
                try {
                    catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
                    createOk.incrementAndGet()
                } catch (e: HoglakeException.NotFound) {
                    createConflict.incrementAndGet() // drop won: namespace gone
                } catch (e: Throwable) {
                    unexpected += e
                }
            }
        val drop =
            pool.submit {
                start.await()
                try {
                    catalogs.dropNamespace(cat, "ns")
                    dropOk.incrementAndGet()
                } catch (e: HoglakeException.NamespaceNotEmpty) {
                    dropConflict.incrementAndGet() // create won: not empty
                } catch (e: Throwable) {
                    unexpected += e
                }
            }
        start.countDown()
        create.get(30, TimeUnit.SECONDS)
        drop.get(30, TimeUnit.SECONDS)
        pool.shutdown()

        assertThat(unexpected).isEmpty()
        // Exactly one outcome pair is possible:
        //  - create wins (createOk=1, dropConflict=1), or
        //  - drop wins (dropOk=1, createConflict=1).
        // No interleaving, no both-succeed, no both-fail.
        assertThat(createOk.get() + createConflict.get()).isEqualTo(1)
        assertThat(dropOk.get() + dropConflict.get()).isEqualTo(1)
        assertThat(createOk.get() + dropOk.get()).isEqualTo(1)

        // The end state is consistent whichever way the coin landed: either
        // a live namespace with the table, or no namespace at all.
        if (dropOk.get() == 1) {
            assertThat(catalogs.listNamespaces(cat)).isEmpty()
        } else {
            assertThat(catalogs.listNamespaces(cat).map { it.name }).containsExactly("ns")
            assertThat(catalogs.listTables(cat, "ns").map { it.name }).containsExactly("t")
        }
    }
}
