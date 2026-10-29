package com.posthog.hoglake.compaction

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/**
 * The interim stop-gap: compaction does not plan a table with the
 * object-prefix layout, because its output path is the table root, which
 * the layout refuses, and nothing checks an output's stats against the
 * prefix value. Planning only, so the store is a dead endpoint: a plan
 * that reached a rewrite would fail the group, which `failedGroups`
 * would show.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionObjectPrefixSkipIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val counter = AtomicInteger(0)

    private val deadStore =
        ObjectStore(
            endpoint = "http://127.0.0.1:9",
            region = "us-east-1",
            accessKey = "unused",
            secretKey = "unused",
            pathStyle = true,
        )

    private val cfg = CompactionConfig(targetBytes = 1000, minInputFiles = 2, maxGroupsPerRun = 10)
    private val svc = CompactionService(db.jdbi, deadStore, cfg)

    @AfterAll
    fun tearDown() {
        deadStore.close()
        db.close()
    }

    private fun le8(v: Long): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array()

    /** One table with four small conforming team_id=4 files, the option on or off. */
    private fun tableWithSmallFiles(option: Boolean): String {
        val cat = "prefix-compact-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("team_id", ColType.LONG)))
        val ops =
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))) +
                if (option) listOf(AlterOp.SetObjectPrefixField("team_id")) else emptyList()
        alter.alterTable(cat, "ns", "t", ops)
        val files =
            (1..4).map {
                FileRegistration(
                    "s3://bucket/$cat/data/ns/t/team_id=4/${UUID.randomUUID()}.parquet",
                    10,
                    300,
                    20,
                    listOf(ColumnStats(1, 10, 0, null, null, le8(4), le8(4))),
                    listOf("4"),
                )
            }
        commits.commit(cat, CommitRequest(readSnapshot = db.head(cat), appends = listOf(TableAppend("ns", "t", files))))
        return cat
    }

    @Test
    fun `a table with the option plans nothing, and the sweep counts the skip`() {
        val off = tableWithSmallFiles(option = false)
        assertThat(svc.planTable(off, "ns", "t").groups)
            .describedAs("the same files on a table without the option are a group")
            .hasSize(1)

        val on = tableWithSmallFiles(option = true)
        assertThat(svc.planTable(on, "ns", "t").groups).isEmpty()

        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        try {
            val result = svc.runOnce(on, cfg, MaintenanceTrigger.LOOP)
            assertThat(result.groupsCompacted).isZero()
            assertThat(result.failedGroups).isZero()
            assertThat(registry.scrape())
                .contains(
                    "hoglake_compaction_skipped_total{catalog=\"$on\",reason=\"object_prefix_layout\"} 1.0",
                )
        } finally {
            Metrics.clear()
        }
    }
}
