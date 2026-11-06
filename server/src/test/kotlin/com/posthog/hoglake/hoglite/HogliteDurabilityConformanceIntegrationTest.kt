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
 * Recovery: acknowledged commits survive the loss of the owner and its
 * local directory, because recovery reads durable S3 state.
 */
@Tag("integration")
class HogliteDurabilityConformanceIntegrationTest {
    @Test
    fun `acknowledged snapshots survive abrupt owner loss and recovery without local state`() {
        HogliteConformanceEngine().use { engine ->
            val cat = HogliteConformanceEngine.uniqueName("recovery")
            engine.store.createCatalog(cat, "s3://hoglake-it/$cat", null, null)
            engine.store.createNamespace(cat, "ns", null, null)
            engine.store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)), null, null)
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
                                        listOf(FileRegistration("s3://hoglake-it/$cat/data/$i.parquet", 10, 100, 50)),
                                    ),
                                ),
                        ),
                        null,
                    ).snapshotId
                }

            fun scan(snapshot: Long) = engine.store.planScan(cat, "ns", "t", snapshot, null, null, false)
            val answers = snapshots.map { scan(it) }
            engine.crashAndRecover()
            // Lease takeover is asynchronous; the test assumes no fixed lease duration.
            await().atMost(Duration.ofSeconds(60)).ignoreExceptions().untilAsserted {
                assertThat(engine.store.getCatalog(cat).headSnapshotId).isEqualTo(snapshots.last())
                snapshots.forEachIndexed { i, snapshot ->
                    assertThat(scan(snapshot)).isEqualTo(answers[i])
                    assertThat(engine.store.receipt(cat, keys[i]).snapshotId).isEqualTo(snapshot)
                }
            }
        }
    }
}
