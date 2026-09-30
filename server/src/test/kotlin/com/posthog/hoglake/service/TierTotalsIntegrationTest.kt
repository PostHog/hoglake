package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.compaction.CompactionGrouping
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.publishMaintenanceSample
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * The two ways the sampler's own state can make a head read's totals a
 * LIE, and neither is visible from the API's happy path (#232).
 *
 * `CatalogService.getTable` sums `hog_maintenance_summary_tier` rows for
 * one table. That table holds EVERY generation's buckets, not just the
 * published one — the sampler accumulates the generation it is currently
 * scanning into it, page by page, and garbage-collects older ones
 * afterwards. So:
 *
 *  1. joining on the LIVE `generation` rather than `published_generation`
 *     would serve a HALF-ACCUMULATED scan as fact. Undetectable by any
 *     test that runs the sampler to completion, because then the two are
 *     equal — which is every other test of this feature.
 *  2. a generation whose `record_count` accumulation started from the
 *     V22 column defaults partway through (the deploy straddle V22's
 *     header describes) is UNDERCOUNTED, and
 *     `measures_generation != published_generation` is the only thing
 *     that says so.
 *
 * Both are asserted through the real service read, with the sampler
 * driven by hand.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TierTotalsIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val counter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    /** A catalog with one table and [files] appended files, one row each. */
    private fun fixture(files: Int): String {
        val cat = "tier-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        repeat(files) { i -> append(cat, "f$i") }
        return cat
    }

    private fun append(
        cat: String,
        name: String,
        records: Long = 10,
    ) = commits.commit(
        cat,
        CommitRequest(
            appends =
                listOf(
                    TableAppend(
                        "ns",
                        "t",
                        listOf(FileRegistration("s3://$cat/data/$name.parquet", records, 100)),
                    ),
                ),
        ),
    ).snapshotId

    private fun totals(cat: String) = catalogs.getTable(cat, "ns", "t")

    private fun generations(cat: String): Pair<Long, Long> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT s.generation, s.published_generation
                  FROM hog_maintenance_summary s
                  JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                 WHERE c.name = :cat
                """,
            ).bind("cat", cat)
                .map { rs, _ -> rs.getLong("generation") to rs.getLong("published_generation") }
                .one()
        }

    @Test
    fun `a head read serves the PUBLISHED generation, never the one being scanned`() {
        // Six files, so a one-row-per-tick scan cannot finish in one
        // tick and the second generation is guaranteed to be in flight.
        val cat = fixture(files = 6)
        publishMaintenanceSample(db.jdbi)
        val published = totals(cat)
        assertThat(published.fileCount).isEqualTo(6)
        assertThat(published.recordCount).isEqualTo(60)

        // More files land, and a SECOND generation starts accumulating
        // them — one row at a time, so it stops mid-scan with buckets in
        // `hog_maintenance_summary_tier` that no publish has blessed.
        repeat(4) { i -> append(cat, "later$i") }
        db.jdbi.useHandleUnchecked { it.execute("UPDATE hog_maintenance_summary SET next_batch_at = now()") }
        val sampler =
            MaintenanceSummarySampler(
                db.jdbi,
                512L * 1024 * 1024,
                CompactionGrouping.DEFAULT_MIN_INPUT_FILES,
                CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
                3600,
            )
        // TICK UNTIL A GENERATION IS IN FLIGHT, one row of work at a
        // time, and stop the moment it is. A tick is not always a page
        // of the scan: with no checkpoint the sampler first garbage-
        // collects one batch of a stale generation's buckets and returns
        // without beginning anything, so "one tick" does not mean "one
        // page" and a fixed number of ticks would be a guess.
        var ticks = 0
        while (generations(cat).let { (live, published) -> live <= published }) {
            check(sampler.runOnce(batchSize = 1)) { "nothing was due after $ticks ticks" }
            check(++ticks < 1_000) { "no generation began in $ticks ticks" }
        }

        val (live, stillPublished) = generations(cat)
        assertThat(live)
            .describedAs(
                "the fixture's premise: a generation is IN FLIGHT and is not the published one. " +
                    "Without this the next assertion cannot fail however the join is written",
            )
            .isGreaterThan(stillPublished)

        val duringScan = totals(cat)
        assertThat(duringScan.fileCount)
            .describedAs(
                "a head read must answer from the PUBLISHED generation. Joining the tier rows " +
                    "on the live `generation` instead would serve however much of the new scan " +
                    "happens to have landed — a number between 1 and 10 that changes every " +
                    "time the sampler ticks, presented as the table's totals",
            )
            .isEqualTo(6)
        assertThat(duringScan.recordCount).isEqualTo(60)
        assertThat(duringScan.totalsSnapshotId)
            .describedAs("and it is still dated by the published generation's snapshot")
            .isEqualTo(published.totalsSnapshotId)

        // Finishing the scan publishes it, and only then do the numbers move.
        publishMaintenanceSample(db.jdbi)
        assertThat(totals(cat).fileCount).isEqualTo(10)
    }

    @Test
    fun `an unmeasured published generation withholds all three totals`() {
        val cat = fixture(files = 2)
        publishMaintenanceSample(db.jdbi)
        assertThat(totals(cat).recordCount).isEqualTo(20)

        // V22's straddle, forged: the published generation's buckets were
        // accumulated by a sampler that did not know `record_count`, so
        // the rows carry the column default for part of the scan and the
        // sum is UNDERCOUNTED. `measures_generation` is the only witness.
        db.jdbi.useHandleUnchecked {
            it.execute(
                "UPDATE hog_maintenance_summary SET measures_generation = published_generation - 1",
            )
        }

        val unmeasured = totals(cat)
        assertThat(unmeasured.recordCount)
            .describedAs("an undercounted row total must not be served as fact")
            .isNull()
        // ALL THREE, not just record_count — file_count and
        // file_size_bytes predate V22 and are trustworthy, but the three
        // travel as one fact on the wire and `record_count` is the one a
        // reader looks at first. Two of three with no way to say which
        // is missing is worse than saying nothing.
        assertThat(unmeasured.fileCount).isNull()
        assertThat(unmeasured.fileSizeBytes).isNull()
        assertThat(unmeasured.totalsSnapshotId)
            .describedAs("nothing is dated, because nothing is reported")
            .isNull()

        // And it recovers on its own the moment a measured generation
        // publishes — the self-healing V22's header describes.
        db.jdbi.useHandleUnchecked {
            it.execute("UPDATE hog_maintenance_summary SET measures_generation = published_generation")
        }
        assertThat(totals(cat).recordCount).isEqualTo(20)
    }

    @Test
    fun `a catalog the sampler has never touched reports no totals`() {
        // No summary row at all — the sampler's warm-up state, and the
        // reason the per-table statement's summary join may be inner:
        // no row means no answer, which is the same thing a row with no
        // published sample means.
        val cat = fixture(files = 1)
        assertThat(
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_maintenance_summary s JOIN hog_catalog c " +
                        "ON c.catalog_id = s.catalog_id WHERE c.name = :cat",
                ).bind("cat", cat).mapTo(Long::class.java).one()
            },
        ).describedAs("the sampler discovers catalogs on its own tick; nothing has ticked").isZero()

        val info = totals(cat)
        assertThat(info.recordCount).isNull()
        assertThat(info.fileCount).isNull()
        assertThat(info.fileSizeBytes).isNull()
        assertThat(info.totalsSnapshotId).isNull()
        assertThat(info.totalsAsOf).isNull()
        assertThat(info.readSnapshotId)
            .describedAs("the read still says what snapshot it resolved at")
            .isGreaterThan(0)
    }
}
