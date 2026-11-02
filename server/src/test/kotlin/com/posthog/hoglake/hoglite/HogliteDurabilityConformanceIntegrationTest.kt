package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID

/**
 * Recovery arm of the caller contract. PostgreSQL crash injection is explicitly
 * excluded here: PgTestSupport owns a shared container, not a restartable one.
 * See HogliteConformance.md; this is NOT the stronger read-durability-basis test.
 */
@Tag("integration")
class HogliteDurabilityConformanceIntegrationTest {
    @Test
    fun `acknowledged snapshots survive abrupt owner loss and recovery without local state`() {
        HogliteConformanceEngine().use { engine ->
            val cat = "recovery" + UUID.randomUUID().toString().replace("-", "")
            engine.store.createCatalog(cat, "s3://hoglake-it/$cat")
            engine.store.createNamespace(cat, "ns")
            engine.store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
            val keys = (1..3).map { UUID.randomUUID() }
            val snapshots =
                keys.mapIndexed { i, key ->
                    engine.store.commit(
                        cat,
                        CommitRequest(
                            idempotencyKey = key,
                            appends =
                                listOf(
                                    TableAppend(
                                        "ns",
                                        "t",
                                        listOf(
                                            FileRegistration("s3://hoglake-it/$cat/data/$i.parquet", 10, 100, 50),
                                        ),
                                    ),
                                ),
                        ),
                    ).snapshotId
                }
            val answers = snapshots.map { engine.store.planScan(cat, "ns", "t", snapshot = it) }
            engine.crashAndRecover()
            // Lease takeover is intentionally asynchronous; no assumed fixed lease duration.
            await().atMost(Duration.ofSeconds(60)).ignoreExceptions().untilAsserted {
                assertThat(engine.store.getCatalog(cat).headSnapshotId).isEqualTo(snapshots.last())
                snapshots.forEachIndexed { i, snapshot ->
                    assertThat(engine.store.planScan(cat, "ns", "t", snapshot = snapshot)).isEqualTo(answers[i])
                    assertThat(engine.store.receipt(cat, keys[i]).snapshotId).isEqualTo(snapshot)
                }
            }
        }
    }
}
