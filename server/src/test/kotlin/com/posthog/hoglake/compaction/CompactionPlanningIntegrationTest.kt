package com.posthog.hoglake.compaction

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.MaintenanceBacklog
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.MaintenanceStatusService
import com.posthog.hoglake.service.MaintenanceSummarySampler
import com.posthog.hoglake.service.PartitionStatsService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * Compaction PLANNING is metadata-only: no object-store contact
 * happens here (the store below points at a dead endpoint). Covers
 * candidate selection (live, below target, DV-bearing), bucketing by
 * (spec_id, partition_values, size TIER), the tier floors (aggregate
 * bytes must reach the next tier or the bucket is skipped), the
 * minimal row-id-ordered prefix (never more files than the floor
 * needs), and the max_input_files fan-in cap.
 *
 * The ladder for these tests: T=4, target=1000 -> 1/4/16/63/250/1000.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionPlanningIntegrationTest {
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

    private val cfg = CompactionConfig(targetBytes = 1000, tierTarget = 4, maxGroupsPerRun = 10)
    private val svc = CompactionService(db.jdbi, deadStore, cfg)

    @AfterAll
    fun tearDown() {
        deadStore.close()
        db.close()
    }

    private fun fixture(columns: List<ColumnDef> = listOf(ColumnDef("id", ColType.LONG))): String {
        val cat = "plan-cat-${counter.incrementAndGet()}"
        db.jdbi.useHandleUnchecked { h ->
            // Raw insert: this suite tests compaction planning, not catalog validation.
            // The shared "s3://bucket" data_path (files at s3://bucket/x/)
            // would trip the creation-time shape/overlap rules.
            val id =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES (?, ?) RETURNING catalog_id",
                ).bind(0, cat).bind(1, "s3://bucket").mapTo(Long::class.java).one()
            h.execute(
                "INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) VALUES (?, 0, 0)",
                id,
            )
        }
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", columns)
        return cat
    }

    private fun append(
        cat: String,
        vararg files: FileRegistration,
    ) = commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", files.toList()))))

    private fun file(
        name: String,
        bytes: Long,
        records: Long = 10,
        values: List<String?>? = null,
    ) = FileRegistration(
        path = "s3://bucket/x/$name.parquet",
        recordCount = records,
        fileSizeBytes = bytes,
        partitionValues = values,
    )

    @Test
    fun `small live files group by minimal prefix to the next tier, files at or over target are excluded`() {
        val cat = fixture()
        append(cat, file("a", 600), file("b", 600), file("big", 1000), file("c", 600))
        val plan = svc.planTable(cat, "ns", "t", cfg)
        // All three small files are in [250,1000); a+b reaches 1000,
        // so c is NOT taken: the group
        // is the minimal prefix, and c waits for the next run.
        assertThat(plan.groups).hasSize(1)
        assertThat(plan.groups.single().files.map { it.path })
            .containsExactly("s3://bucket/x/a.parquet", "s3://bucket/x/b.parquet")
        assertThat(plan.groups.single().totalBytes).isEqualTo(1200)
    }

    @Test
    fun `a run repeats minimal prefixes in each tier until the remainder is short`() {
        val cat = fixture()
        append(cat, file("a", 600), file("b", 600), file("c", 600), file("d", 600), file("e", 600))
        val plan = svc.planTable(cat, "ns", "t", cfg)
        // Two complete pairs reach 1000; e alone cannot.
        assertThat(plan.groups).hasSize(2)
        assertThat(plan.groups[0].files.map { it.path })
            .containsExactly("s3://bucket/x/a.parquet", "s3://bucket/x/b.parquet")
        assertThat(plan.groups[1].files.map { it.path })
            .containsExactly("s3://bucket/x/c.parquet", "s3://bucket/x/d.parquet")
    }

    @Test
    fun `a bucket that cannot reach its tier floor is skipped, exactly reaching it merges`() {
        val short = fixture()
        append(short, file("a", 100), file("b", 100))
        // Files in [63,250) must reach 250; 200 < 250: waits for more appends.
        assertThat(svc.planTable(short, "ns", "t", cfg).groups).isEmpty()

        val exact = fixture()
        append(exact, file("a", 100), file("b", 150))
        // 250 = the floor exactly: the merge is on (floors are inclusive).
        assertThat(svc.planTable(exact, "ns", "t", cfg).groups).hasSize(1)
    }

    @Test
    fun `eight files at the lower bound promote and changing T changes the ladder`() {
        val cat = fixture()
        append(cat, *(0..7).map { file("f$it", 128) }.toTypedArray())
        val eight = svc.planTable(cat, "ns", "t", cfg.copy(targetBytes = 1024, tierTarget = 8))
        assertThat(eight.groups.single().files).hasSize(8)
        val four = svc.planTable(cat, "ns", "t", cfg.copy(targetBytes = 1024, tierTarget = 4))
        assertThat(four.groups).hasSize(4)
        assertThat(four.groups.map { it.files.size }).containsOnly(2)
    }

    @Test
    fun `groups never mix tiers`() {
        val cat = fixture()
        // One 100B file that can't reach 250 alone, one 600B file that
        // can't reach 1000 alone: NO cross-tier merge
        // rescue — both sit until their own tier can form a group.
        append(cat, file("small", 100), file("mid", 600))
        assertThat(svc.planTable(cat, "ns", "t", cfg).groups).isEmpty()

        val cat2 = fixture()
        append(
            cat2,
            file("s1", 100),
            file("s2", 100),
            file("s3", 100),
            file("m1", 600),
            file("m2", 600),
        )
        val groups = svc.planTable(cat2, "ns", "t", cfg).groups
        assertThat(groups).hasSize(2)
        // Lower tier first, each group homogeneous.
        assertThat(groups[0].files.map { it.path })
            .containsExactly(
                "s3://bucket/x/s1.parquet",
                "s3://bucket/x/s2.parquet",
                "s3://bucket/x/s3.parquet",
            )
        assertThat(groups[1].files.map { it.path })
            .containsExactly("s3://bucket/x/m1.parquet", "s3://bucket/x/m2.parquet")
    }

    @Test
    fun `a file with a live deletion vector is a candidate carrying its planned vector`() {
        val cat = fixture()
        val snap = append(cat, file("a", 100), file("b", 100), file("c", 100)).snapshotId
        val fileIds =
            db.jdbi.withHandle<List<Long>, Exception> { h ->
                h.createQuery(
                    """
                    SELECT f.data_file_id FROM hog_data_file f
                    JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                    WHERE c.name = :cat ORDER BY f.data_file_id
                    """,
                )
                    .bind("cat", cat)
                    .mapTo(Long::class.java).list()
            }
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = snap,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(
                                DeleteFileRegistration(
                                    dataFileId = fileIds[1],
                                    path = "s3://bucket/x/b.dv",
                                    deleteCount = 1,
                                    fileSizeBytes = 10,
                                ),
                            ),
                        ),
                    ),
            ),
        )
        val plan = svc.planTable(cat, "ns", "t", cfg)
        // DV-bearing files compact too: the plan captures the live vector's
        // identity so execution applies it and commit detects supersession.
        assertThat(plan.groups).hasSize(1)
        val group = plan.groups.single()
        assertThat(group.files.map { it.path })
            .containsExactly("s3://bucket/x/a.parquet", "s3://bucket/x/b.parquet", "s3://bucket/x/c.parquet")
        assertThat(group.files.map { it.dv?.path })
            .containsExactly(null, "s3://bucket/x/b.dv", null)
        val plannedDv = group.files[1].dv!!
        assertThat(plannedDv.deleteCount).isEqualTo(1)
        // Survivors = gross minus the planned deletes.
        assertThat(group.totalRecords).isEqualTo(30)
        assertThat(group.survivingRecords).isEqualTo(29)
    }

    @Test
    fun `groups never mix partition values or spec vintages`() {
        val cat = fixture(listOf(ColumnDef("id", ColType.LONG), ColumnDef("bucket", ColType.STRING)))
        // Two unpartitioned files, then a spec, then files across two
        // partitions (150B each: every 2-file bucket clears the 250 floor).
        append(cat, file("u1", 150), file("u2", 150))
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY)))),
        )
        append(
            cat,
            file("p1a", 150, values = listOf("p1")),
            file("p1b", 150, values = listOf("p1")),
            file("p2a", 150, values = listOf("p2")),
            file("p2b", 150, values = listOf("p2")),
        )
        val plan = svc.planTable(cat, "ns", "t", cfg)
        assertThat(plan.groups).hasSize(3)
        val byValues = plan.groups.associateBy { it.partitionValues }
        assertThat(byValues[null]!!.files.map { it.path })
            .containsExactly("s3://bucket/x/u1.parquet", "s3://bucket/x/u2.parquet")
        assertThat(byValues[listOf<String?>("p1")]!!.files.map { it.path })
            .containsExactly("s3://bucket/x/p1a.parquet", "s3://bucket/x/p1b.parquet")
        assertThat(byValues[listOf<String?>("p2")]!!.files.map { it.path })
            .containsExactly("s3://bucket/x/p2a.parquet", "s3://bucket/x/p2b.parquet")
        assertThat(byValues[listOf<String?>("p1")]!!.specId).isEqualTo(1)
    }

    @Test
    fun `candidates are ordered by row_id_start within a group`() {
        val cat = fixture()
        append(cat, file("a", 100, records = 5), file("b", 100, records = 7), file("c", 100, records = 3))
        val plan = svc.planTable(cat, "ns", "t", cfg)
        assertThat(plan.groups.single().files.map { it.rowIdStart }).containsExactly(0L, 5L, 12L)
    }

    @Test
    fun `schema and candidate reads share a snapshot across concurrent alter and append`() {
        val cat = fixture()
        append(cat, file("before-alter", 150)) // below the 250 quota alone
        val changed = java.util.concurrent.atomic.AtomicBoolean(false)
        val instrumented = com.posthog.hoglake.Database.jdbi(db.dataSource)
        instrumented.setSqlLogger(
            object : org.jdbi.v3.core.statement.SqlLogger {
                override fun logAfterExecution(context: org.jdbi.v3.core.statement.StatementContext) {
                    if (context.renderedSql.contains("FROM hog_column") && changed.compareAndSet(false, true)) {
                        // A second connection commits NEW-schema data after the
                        // planner has read its columns, before it reads files.
                        alter.alterTable(
                            cat,
                            "ns",
                            "t",
                            listOf(AlterOp.AddColumn(ColumnDef("new_value", ColType.STRING))),
                        )
                        append(cat, file("after-alter", 150))
                    }
                }
            },
        )
        val planner = CompactionService(instrumented, deadStore, cfg)
        assertThat(planner.planTable(cat, "ns", "t").groups).isEmpty()
        assertThat(changed.get()).isTrue()
        assertThat(planner.planTable(cat, "ns", "t").groups.single().files).hasSize(2)
    }

    @Test
    fun `planner and both debt endpoints agree across vintages null partitions and remainders`() {
        val cat = fixture(listOf(ColumnDef("id", ColType.LONG), ColumnDef("bucket", ColType.STRING)))
        append(cat, *(0..8).map { file("old$it", 1) }.toTypedArray())
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY)))),
        )
        append(cat, *(0..16).map { file("a$it", 128, values = listOf("a")) }.toTypedArray())
        append(cat, *(0..6).map { file("b$it", 16, values = listOf("b")) }.toTypedArray())
        append(cat, *(0..7).map { file("null$it", 16, values = listOf(null)) }.toTypedArray())
        val policy = cfg.copy(targetBytes = 1024, tierTarget = 8)
        val planned = svc.planTable(cat, "ns", "t", policy).groups.sumOf { it.files.size.toLong() }
        assertThat(planned).isEqualTo(32) // 8 old + 16 a + 0 b + 8 null
        val sampler = MaintenanceSummarySampler(db.jdbi, 1024, 8, 3600)
        while (sampler.runOnce()) { /* drain bounded checkpoints in this test */ }
        val stats = PartitionStatsService(db.jdbi, 1024, tierTarget = 8).partitionStats(cat, null, null, 50)
        assertThat(stats.partitions.sumOf { it.debtScore }).isEqualTo(planned)
        assertThat(stats.partitions.sumOf { it.smallFileCount }).isEqualTo(41)
        val maintenance = MaintenanceStatusService(db.jdbi, 0, 0, 0, 0, 1024, tierTarget = 8)

        fun count(status: com.posthog.hoglake.model.MaintenanceStatus): Long? =
            (
                status.tasks.single {
                    it.task == MaintenanceTask.COMPACTION
                }.backlog as MaintenanceBacklog.CompactionBacklog
            ).smallFiles
        assertThat(count(maintenance.status(cat))).isEqualTo(planned)
        assertThat(count(maintenance.instanceStatus().catalogs.single { it.catalog == cat })).isEqualTo(planned)
        // Budget is not debt: reducing this run's allowance doesn't hide backlog.
        assertThat(
            svc.planTable(cat, "ns", "t", policy.copy(maxGroupsPerRun = 1)).groups.sumOf { it.files.size.toLong() },
        )
            .isEqualTo(planned)
    }
}
