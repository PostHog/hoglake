package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.compaction.CompactionGrouping
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * The two measures V22 adds to the sampler's per-bucket accumulator:
 * `record_count` (summed) and `newest_begin_snapshot` (max).
 *
 * They ride the scan the sampler already does — the file cursor's page
 * carries `record_count` and `begin_snapshot` in the same row it reads
 * `file_size_bytes` from — so the only things that can go wrong are
 * arithmetic and RESUMPTION, and resumption is the interesting one: the
 * accumulator lives in the tier ROW between pages, so a generation that
 * straddles the deploy reads back a row that has never held either
 * measure. The columns' defaults (0 and NULL) are the
 * backward-compatible start, and the second test is what says so.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MaintenanceSummaryRowMeasuresIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val counter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    private fun sampler() =
        MaintenanceSummarySampler(db.jdbi, 1000, 2, CompactionGrouping.DEFAULT_MAX_INPUT_FILES, 3600)

    private fun due() =
        db.jdbi.useHandleUnchecked { it.execute("UPDATE hog_maintenance_summary SET next_batch_at = now()") }

    private fun fixture(): String {
        val cat = "rowmeas-${counter.incrementAndGet()}"
        db.jdbi.useHandleUnchecked { h ->
            val id =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES (?, ?) RETURNING catalog_id",
                ).bind(0, cat).bind(1, "s3://bucket").mapTo(Long::class.java).one()
            h.execute("INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) VALUES (?, 0, 0)", id)
        }
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(ColumnDef("id", ColType.LONG), ColumnDef("team", ColType.STRING)),
        )
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY)))),
        )
        return cat
    }

    private fun append(
        cat: String,
        vararg files: FileRegistration,
        // readSnapshot = head: an append carrying partition_values
        // requires one (invariant 12), and the fixture's DDL is done.
    ) = commits.commit(
        cat,
        CommitRequest(readSnapshot = db.head(cat), appends = listOf(TableAppend("ns", "t", files.toList()))),
    ).snapshotId

    private fun file(
        name: String,
        records: Long,
        team: String = "a",
        bytes: Long = 100,
    ) = FileRegistration(
        path = "s3://bucket/x/$name.parquet",
        recordCount = records,
        fileSizeBytes = bytes,
        partitionValues = listOf(team),
    )

    /** The published generation's rows, by partition value. */
    private fun measures(cat: String): Map<String?, Pair<Long, Long?>> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT p.partition_values[1] AS v, p.record_count, p.newest_begin_snapshot
                FROM hog_maintenance_summary_tier p
                JOIN hog_maintenance_summary s ON s.catalog_id = p.catalog_id
                  AND s.published_generation = p.generation
                JOIN hog_catalog c ON c.catalog_id = p.catalog_id
                WHERE c.name = :cat
                """,
            ).bind("cat", cat).map { rs, _ ->
                rs.getString("v") to
                    (
                        rs.getLong("record_count") to
                            rs.getObject("newest_begin_snapshot")?.let { (it as Number).toLong() }
                    )
            }.list().toMap()
        }

    /** record_count on the in-flight bucket, whatever generation it is in. */
    private fun rowsSeen(cat: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT COALESCE(sum(p.record_count), 0) FROM hog_maintenance_summary_tier p
                JOIN hog_catalog c ON c.catalog_id = p.catalog_id
                WHERE c.name = :cat
                """,
            ).bind("cat", cat).mapTo(Long::class.java).one()
        }

    @Test
    fun `two files in one partition sum their rows and keep the later begin snapshot`() {
        val cat = fixture()
        val first = append(cat, file("a1", records = 7))
        val second = append(cat, file("a2", records = 5))
        assertThat(second).isGreaterThan(first)
        due()
        val s = sampler()
        while (s.runOnce()) { /* run the generation to completion */ }

        val (rows, newest) = measures(cat).getValue("a")
        assertThat(rows).describedAs("record_count is the SUM over the bucket's files").isEqualTo(12)
        assertThat(newest)
            .describedAs("newest_begin_snapshot is the LATER of the two commits, not the first seen")
            .isEqualTo(second)
    }

    @Test
    fun `newest_begin_snapshot is a MAX, not the last file the scan saw`() {
        val cat = fixture()
        // THE SCAN IS SIZE-ASCENDING, not commit-ordered
        // (`(table_id, file_size_bytes, data_file_id)` — the order
        // compaction bin-packs in). So a fixture whose files are all
        // the same size degenerates to commit order and passes under
        // last-wins, which is what every earlier fixture here did.
        //
        // Here the NEWER file is the SMALLER one, so the scan visits it
        // FIRST and the older file last. `pool.newest = f.begin` would
        // report the older snapshot.
        val older = append(cat, file("big-old", records = 1, bytes = 5_000))
        val newer = append(cat, file("small-new", records = 1, bytes = 50))
        assertThat(newer).isGreaterThan(older)
        due()
        val s = sampler()
        while (s.runOnce()) { /* run the generation to completion */ }

        val (_, newest) = measures(cat).getValue("a")
        assertThat(newest)
            .describedAs("the bucket's newest write, not the last file the size-ordered scan saw")
            .isEqualTo(newer)
    }

    @Test
    fun `a bucket resumed from a pre-V22 row restarts the two measures at 0 and NULL`() {
        val cat = fixture()
        // Six files in one partition, walked one page at a time so the
        // accumulator has to survive the tier row between pages.
        val snapshots = (1..6).map { append(cat, file("a$it", records = it.toLong())) }
        due()
        val s = sampler()
        // Tick until the file phase has taken ONE page of two files:
        // the first ticks are catalog discovery and the generation
        // begin, which write no bucket. Bounded so a change of shape
        // fails the assertion below rather than spinning.
        var ticks = 0
        while (rowsSeen(cat) == 0L && ticks++ < 10) s.runOnce(batchSize = 2)
        assertThat(rowsSeen(cat))
            .describedAs("the first page's two files (1 + 2 rows) must be accumulated before the rewrite")
            .isEqualTo(3)
        // Nothing is PUBLISHED yet — the generation is in flight — so
        // rewrite the in-flight bucket to the shape a pre-V22 build
        // would have left it in.
        val rewritten =
            db.jdbi.withHandleUnchecked { h ->
                h.createUpdate(
                    """
                    UPDATE hog_maintenance_summary_tier p
                    SET record_count = 0, newest_begin_snapshot = NULL
                    FROM hog_catalog c
                    WHERE c.catalog_id = p.catalog_id AND c.name = :cat
                    """,
                ).bind("cat", cat).execute()
            }
        assertThat(rewritten).describedAs("the partial bucket must exist to be rewritten").isEqualTo(1)
        assertThat(measures(cat)).describedAs("still nothing published").isEmpty()

        // THE OTHER HALF OF THE SIMULATION, and it goes through the
        // real mechanism rather than round it. A generation a pre-V22
        // replica wrote is one it also BEGAN, so its checkpoint has no
        // `measures` key — and `Scan.measures` defaults to false, so
        // the resumed scan publishes without stamping
        // `measures_generation`. Dropping the key from the stored JSON
        // is exactly the state a pre-V22 build leaves, and it exercises
        // the default instead of asserting it.
        db.jdbi.useHandleUnchecked { h ->
            val stripped =
                h.createUpdate(
                    """
                    UPDATE hog_maintenance_summary s SET scan_state = s.scan_state - 'measures'
                    FROM hog_catalog c WHERE c.catalog_id = s.catalog_id AND c.name = :cat
                      AND jsonb_exists(s.scan_state, 'measures')
                    """,
                ).bind("cat", cat).execute()
            assertThat(stripped)
                .describedAs("the in-flight checkpoint must carry `measures` for this to mean anything")
                .isEqualTo(1)
        }

        while (s.runOnce(batchSize = 2)) { /* finish the generation */ }
        val (rows, newest) = measures(cat).getValue("a")
        // MECHANICALLY, the stored accumulation is an UNDERCOUNT: the
        // first page's two files (1 + 2 rows) were erased with the
        // rewrite and the resumed scan accumulates on top of the column
        // defaults. 18 where the truth is 21. That is the shape a
        // generation straddling the deploy leaves behind, and the point
        // of the next assertion is that it is never PUBLISHED as fact.
        assertThat(rows).isEqualTo(3L + 4 + 5 + 6)
        assertThat(newest)
            .describedAs("non-null beside an undercount — which is why the row cannot be the gate")
            .isEqualTo(snapshots.last())

        // THE API REFUSES TO REPORT EITHER. The publish left
        // `measures_generation` behind `published_generation` because
        // the scan it published carried `measures = false`, and that
        // gate holds however the buckets themselves end up looking.
        val listing =
            PartitionListingService(
                db.jdbi,
                smallFileThresholdBytes = 1000,
                minInputFiles = 2,
                maxInputFiles = CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
            ).listPartitions(cat, "ns", "t")
        val group = listing.partitions.single()
        assertThat(group.recordCount)
            .describedAs("18 is not the truth and must not be published as it")
            .isNull()
        assertThat(group.lastWrittenSnapshot).isNull()
        // The measures that were always there are unaffected.
        assertThat(group.fileCount).isEqualTo(6)
        // Every other measure is untouched by the rewrite: the whole
        // six files are still counted.
        val files =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT p.file_count FROM hog_maintenance_summary_tier p
                    JOIN hog_maintenance_summary s ON s.catalog_id = p.catalog_id
                      AND s.published_generation = p.generation
                    JOIN hog_catalog c ON c.catalog_id = p.catalog_id
                    WHERE c.name = :cat
                    """,
                ).bind("cat", cat).mapTo(Long::class.java).one()
            }
        assertThat(files).isEqualTo(6)
    }
}
