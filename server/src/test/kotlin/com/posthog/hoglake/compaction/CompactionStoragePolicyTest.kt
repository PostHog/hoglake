package com.posthog.hoglake.compaction

import com.posthog.hoglake.Config
import com.posthog.hoglake.compaction.CompactionService.GroupOutcome
import com.posthog.hoglake.compaction.CompactionWork.Bucket
import com.posthog.hoglake.compaction.CompactionWork.CandidateFetch
import com.posthog.hoglake.compaction.CompactionWork.PlanWithContext
import com.posthog.hoglake.compaction.CompactionWork.PlannedWork
import com.posthog.hoglake.compaction.CompactionWork.TableContext
import com.posthog.hoglake.compaction.CompactionWork.WorkItem
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.persistence.PartialResult
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import java.util.UUID

class CompactionStoragePolicyTest {
    private val cfg =
        CompactionConfig(targetBytes = 1_000, minInputFiles = 2, maxInputFiles = 2, maxFanIn = 2, maxGroupsPerRun = 1)

    private open class Storage : CompactionStorage {
        val failure = HoglakeException.NotFound("lost catalog")
        var brokenPlan = false
        var fetches = 0
        var useClaims = false
        var releases = 0
        val context =
            TableContext(
                catalogId = 1,
                catalog = "cat",
                dataPath = "s3://bucket/cat",
                namespace = "ns",
                table = "t",
                tableId = 1,
                columns = emptyList(),
                sortFields = emptyList(),
            )

        override fun <T> withPlanningSession(body: (CompactionStorage.PlanningSession) -> T): T =
            body(
                object : CompactionStorage.PlanningSession {
                    override fun fetch(
                        catalog: String,
                        namespace: String,
                        table: String,
                        cfg: CompactionConfig,
                    ): CandidateFetch {
                        fetches++
                        if (brokenPlan && table == "a") throw failure
                        val files =
                            (1L..2L).map {
                                CompactionCandidate(it, "s3://bucket/in$it", 1, 1, null, it, false, null, 1)
                            }
                        return CandidateFetch(
                            context,
                            cfg.targetBytes,
                            mapOf(Bucket(null, null) to files),
                            2,
                            1,
                            1,
                            false,
                        )
                    }

                    override fun liveClaimedFileIds(ctx: TableContext): Set<Long> = emptySet()

                    override fun reserve(
                        planned: PlanWithContext,
                        cfg: CompactionConfig,
                        room: Int,
                        claimant: UUID,
                    ): PlannedWork =
                        PlannedWork(
                            planned.plan,
                            planned.plan.groups.take(room).map {
                                WorkItem(planned.ctx, it, claimant.takeIf { useClaims })
                            },
                        )
                },
            )

        override fun liveTables(catalog: String): List<Pair<String, String>> = listOf("ns" to "a", "ns" to "b")

        override fun purgeExpired(catalog: String) {}

        override fun refreshClaim(
            ctx: TableContext,
            claimKey: String,
            bucketKeys: List<String>,
            claimant: UUID,
            cfg: CompactionConfig,
        ): Boolean = true

        override fun releaseClaim(
            ctx: TableContext,
            claimKey: String,
            claimant: UUID,
        ) {
            releases++
        }

        override fun settleClaim(
            ctx: TableContext,
            claimKey: String,
            claimant: UUID,
            cfg: CompactionConfig,
            committed: Boolean,
            inputFileIds: List<Long>?,
        ) {
        }

        override fun stageOutputPath(
            catalogId: Long,
            outputPath: String,
        ): Long = 1

        override fun publish(
            ctx: TableContext,
            group: CompactionGroup,
            outputPath: String,
            outputBytes: Long,
            footerSize: Long,
            stats: List<ColumnStats>?,
            splitOffsets: List<Long>?,
            survivors: Long,
            rowIdStart: Long,
            stagingId: Long,
        ): GroupOutcome =
            error(
                "the rewrite hook stops before publication",
            )

        override fun recorded(
            catalog: String,
            trigger: MaintenanceTrigger,
            body: () -> CompactionResult,
        ): CompactionResult = body()

        override fun isStatementLockTimeout(error: Throwable): Boolean = false
    }

    private fun objects() = ObjectStore(Config(s3AccessKey = "test", s3SecretKey = "test", s3Region = "us-east-1"))

    @Test
    fun `default policies retain group isolation for a typed backend failure`() {
        val storage = Storage()
        assertThat(storage.isTerminalFailure(storage.failure)).isFalse()
        assertThat(storage.isRecoverablePlanningFailure(storage.failure)).isFalse()
        objects().use { objects ->
            val executor = CompactionExecutor(storage, objects)
            executor.beforeRewrite = { throw storage.failure }
            val result = executor.runOnce("cat", cfg, MaintenanceTrigger.MANUAL)
            assertThat(result.failedGroups).isEqualTo(1)
            assertThat(storage.fetches).isEqualTo(1)
        }
    }

    @Test
    fun `default policy propagates the original planning exception without decoration`() {
        val storage = Storage().apply { brokenPlan = true }
        objects().use { objects ->
            val error =
                catchThrowable { CompactionExecutor(storage, objects).runOnce("cat", cfg, MaintenanceTrigger.MANUAL) }
            assertThat(error).isSameAs(storage.failure)
            assertThat(error).isNotInstanceOf(PartialResult::class.java)
            assertThat(storage.fetches).isEqualTo(1)
        }
    }

    @Test
    fun `an opt-in terminal failure carries the partial result without becoming a failed group`() {
        val storage =
            object : Storage() {
                override fun isTerminalFailure(error: Throwable): Boolean = error === failure
            }
        objects().use { objects ->
            val executor = CompactionExecutor(storage, objects)
            executor.beforeRewrite = { throw storage.failure }
            val error = catchThrowable { executor.runOnce("cat", cfg, MaintenanceTrigger.MANUAL) }
            assertThat(error.cause).isSameAs(storage.failure)
            val partial = (error as PartialResult).partial as CompactionResult
            assertThat(partial.failedGroups + partial.groupsCompacted).isZero()
            assertThat(storage.fetches).isEqualTo(1)
        }
    }

    @Test
    fun `a terminal claim refresh releases the reservation before it stops the sweep`() {
        val storage =
            object : Storage() {
                override fun isTerminalFailure(error: Throwable): Boolean = error === failure

                override fun refreshClaim(
                    ctx: TableContext,
                    claimKey: String,
                    bucketKeys: List<String>,
                    claimant: UUID,
                    cfg: CompactionConfig,
                ): Boolean = throw failure
            }.apply { useClaims = true }
        objects().use { objects ->
            val failure =
                catchThrowable { CompactionExecutor(storage, objects).runOnce("cat", cfg, MaintenanceTrigger.MANUAL) }
            assertThat(failure.cause).isSameAs(storage.failure)
            assertThat(storage.releases).isEqualTo(1)
            assertThat((failure as PartialResult).partial).isInstanceOf(CompactionResult::class.java)
        }
    }

    @Test
    fun `an opt-in planning failure spends no attempt and permits the next table`() {
        val storage =
            object : Storage() {
                override fun isRecoverablePlanningFailure(error: Throwable): Boolean = error === failure
            }.apply { brokenPlan = true }
        objects().use { objects ->
            val executor = CompactionExecutor(storage, objects)
            executor.beforeRewrite = { throw UnconvertibleSchemaException("cannot project") }
            val result = executor.runOnce("cat", cfg, MaintenanceTrigger.MANUAL)
            assertThat(result.failedGroups).isEqualTo(1)
            assertThat(result.unconvertibleSchema).isEqualTo(1)
            assertThat(storage.fetches).isEqualTo(2)
        }
    }
}
