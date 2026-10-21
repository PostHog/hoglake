package com.posthog.hoglake.service

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * The maintenance sampler when EXPIRY OVERTAKES ITS SCAN — the
 * gigahog-prod-us failure of 2026-10-16 to 10-21.
 *
 * A scan used to discard itself and restart at head whenever
 * `earliest_snapshot_id` passed its captured snapshot. Once a scan takes
 * longer than retention that is every scan: millpond's took 58 min
 * against a 3,600 s retention and millrace's took longer than 600 s, so
 * neither catalog published a generation for five days, and the
 * compaction planner, which picks buckets only from the published
 * generation, went blind to every partition written since.
 *
 * Now the scan RE-ANCHORS (see `MaintenanceSummarySampler`'s KDoc): it
 * moves its visibility snapshot to head and continues from its cursor.
 * These tests drive expiry by setting the floor directly, as
 * `CompactionDebtIntegrationTest` does — `ExpiryService` would also purge
 * the ended rows, which is what makes the old snapshot undecidable, but
 * the sampler's decision reads only the floor.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MaintenanceSummaryReanchorIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private var seq = 0

    /** Every file is under it, so every live file is in the small-file measure. */
    private val target = 1L shl 20

    private companion object {
        const val FILES = 20
        const val BATCH = 5

        /** A tick cap, so a regression is a failed assertion and not a hung suite. */
        const val TICK_CAP = 200
    }

    @AfterAll
    fun tearDown() = db.close()

    private fun sampler() = MaintenanceSummarySampler(db.jdbi, target, 3, 64, refreshSeconds = 3_600)

    /**
     * One table of [FILES] files with DISTINCT ascending sizes, so the
     * scan's size-order cursor after N pages is exactly the N x [BATCH]
     * smallest — the test can then say which side of the cursor a file is
     * on. `record_count = n`, so the ROW total names the exact set of
     * files counted: with a constant count, the wrong set {1..15, 16,
     * 18, 20} and the right one {1..15, 19, 20, 21} have the same file
     * count and byte sum. Returns (catalog name, catalog id, head).
     */
    private fun seed(): Triple<String, Long, Long> {
        val name = "reanchor-${seq++}"
        val cat = catalogs.createCatalog(name, "s3://bucket/$name")
        catalogs.createNamespace(name, "ns")
        catalogs.createTable(name, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val head = head(cat.catalogId)
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, table_id, data_file_id, row_id_start, begin_snapshot,
                    path, record_count, file_size_bytes, stats_state)
                SELECT :cat, 1, n, (n - 1) * 100, :head, 's3://bucket/f' || n, n, 1000 + n, 'provided'
                FROM generate_series(1, :n) n
                """,
            ).bind("cat", cat.catalogId).bind("head", head).bind("n", FILES).execute()
        }
        return Triple(name, cat.catalogId, head)
    }

    private fun head(catalogId: Long): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE catalog_id = :c")
                .bind("c", catalogId).mapTo(Long::class.java).one()
        }

    /** A new head snapshot (DDL is the cheapest commit), with expiry's floor moved onto it. */
    private fun expireToNewHead(name: String): Long {
        catalogs.createNamespace(name, "advance-${seq++}")
        return db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "UPDATE hog_catalog SET earliest_snapshot_id = last_snapshot_id WHERE name = :n " +
                    "RETURNING earliest_snapshot_id",
            ).bind("n", name).mapTo(Long::class.java).one()
        }
    }

    private fun published(catalogId: Long): MaintenanceSummarySampler.Published? =
        db.jdbi.withHandleUnchecked { h -> MaintenanceSummarySampler.read(h, listOf(catalogId))[catalogId] }

    private data class Totals(val files: Long?, val bytes: Long?, val rows: Long?, val snapshot: Long?)

    private fun totals(catalogId: Long): Totals =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT live_files, live_bytes, live_rows, published_snapshot FROM hog_maintenance_summary " +
                    "WHERE catalog_id = :c",
            ).bind("c", catalogId).map { rs, _ ->
                Totals(
                    rs.getObject("live_files") as Long?,
                    rs.getObject("live_bytes") as Long?,
                    rs.getObject("live_rows") as Long?,
                    rs.getObject("published_snapshot") as Long?,
                )
            }.one()
        }

    private fun scanStateKeys(catalogId: Long): List<String> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT jsonb_object_keys(scan_state) FROM hog_maintenance_summary WHERE catalog_id = :c",
            ).bind("c", catalogId).mapTo(String::class.java).list()
        }

    private fun scanState(catalogId: Long): Map<String, Any?>? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT scan_state->>'visibleAt' AS visible_at, scan_state->>'reanchors' AS reanchors, " +
                    "scan_state->>'snapshot' AS snapshot, scan_state IS NOT NULL AS present " +
                    "FROM hog_maintenance_summary WHERE catalog_id = :c",
            ).bind("c", catalogId).map { rs, _ ->
                if (!rs.getBoolean("present")) {
                    null
                } else {
                    mapOf(
                        "visibleAt" to rs.getString("visible_at")?.toLong(),
                        "reanchors" to rs.getString("reanchors")?.toInt(),
                        "snapshot" to rs.getString("snapshot")?.toLong(),
                    )
                }
            }.one()
        }

    @Test
    fun `a scan that expiry overtakes on EVERY tick still publishes`() {
        val (name, catalogId, start) = seed()
        val sampler = sampler()

        // Retention shorter than the scan, the production shape: every
        // tick after the first finds the floor past whatever the scan is
        // evaluating at.
        var ticks = 0
        while (published(catalogId) == null && ticks < TICK_CAP) {
            if (ticks > 0) expireToNewHead(name)
            sampler.runOnce(BATCH)
            ticks++
        }

        // MUTATION: put back the discard — `checkpoint(h, catalogId,
        // generation, null)` when `earliest > scan.snapshot` — and this
        // reds at the tick cap with nothing published: every tick
        // restarts at a head the next tick's expiry overtakes again.
        // That is millpond-prod-us and millrace-prod-us from 2026-10-16.
        val got = published(catalogId)
        assertThat(got)
            .describedAs("the generation must publish although expiry overtook it on every tick (%d ticks)", ticks)
            .isNotNull()
        got!!
        // FILES / BATCH pages, the first at the captured snapshot and
        // every later file-phase page after a re-anchor; the page that
        // comes back short (or reaches the upper bound) ends the phase.
        assertThat(got.sample.reanchors)
            // `reanchors` counts EVENTS — ticks that found the floor past
            // the snapshot being evaluated — not pages: a tick that then
            // only skips a dropped table's key range counts too. This
            // fixture has no dropped table, so here the two coincide.
            .describedAs("one re-anchor per file-phase tick after the first, none in the removal phase")
            .isEqualTo(FILES / BATCH - 1)
        // The sample's snapshot is the scan's START, never a re-anchor:
        // coverage and the planner's new-bucket bound both read it.
        assertThat(got.sample.snapshotId).isEqualTo(start)
        assertThat(totals(catalogId).snapshot).isEqualTo(start)
        // Nothing ended, so a window observation and a point-in-time one
        // agree: every file, once.
        assertThat(got.sample.smallFiles).isEqualTo(FILES.toLong())
        assertThat(totals(catalogId).files).isEqualTo(FILES.toLong())
        assertThat(totals(catalogId).rows).isEqualTo((1..FILES).sum().toLong())
    }

    @Test
    fun `a re-anchor excludes files that ended before it and counts each row exactly once`() {
        val (name, catalogId, start) = seed()
        val sampler = sampler()

        // Page 1 at the captured snapshot: files 1..5, the five smallest.
        assertThat(sampler.runOnce(BATCH)).isTrue()
        assertThat(scanState(catalogId)!!["visibleAt"]).isNull()
        // OMITTED, not null: `->>` reads SQL NULL for a missing key and
        // a JSON null alike, so the straddle claim (an older build parses
        // this checkpoint) is asserted on the KEYS.
        // MUTATION: drop NON_NULL on `Scan.visibleAt` or NON_DEFAULT on
        // `Scan.reanchors` and this reds.
        assertThat(scanStateKeys(catalogId)).doesNotContain("visibleAt", "reanchors")

        // Between pages, one commit's worth of change on BOTH sides of
        // the cursor, then expiry past the captured snapshot:
        //  - files 16..18 END (above the cursor: not yet paged);
        //  - file 2 ENDS (below the cursor: already paged, at the old
        //    snapshot, where it was live);
        //  - file 21 is APPENDED with a size above the cursor, and file
        //    22 with a size below it.
        catalogs.createNamespace(name, "commit")
        val commit = head(catalogId)
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_data_file SET end_snapshot = ? WHERE catalog_id = ? AND data_file_id IN (2, 16, 17, 18)",
                commit,
                catalogId,
            )
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, table_id, data_file_id, row_id_start, begin_snapshot,
                    path, record_count, file_size_bytes, stats_state)
                VALUES (?, 1, 21, 2100, ?, 's3://bucket/f21', 21, 1015, 'provided'),
                       (?, 1, 22, 2200, ?, 's3://bucket/f22', 22, 1000, 'provided')
                """,
                catalogId,
                commit,
                catalogId,
                commit,
            )
        }
        val floor = expireToNewHead(name)

        // The next tick re-anchors before its page and records it in the
        // checkpoint.
        assertThat(sampler.runOnce(BATCH)).isTrue()
        val state = scanState(catalogId)!!
        assertThat(state["visibleAt"]).isEqualTo(floor)
        assertThat(state["reanchors"]).isEqualTo(1)
        assertThat(scanStateKeys(catalogId)).contains("visibleAt", "reanchors")
        assertThat(state["snapshot"])
            .describedAs("the captured snapshot does not move")
            .isEqualTo(start)

        var ticks = 2
        while (sampler.runOnce(BATCH)) check(++ticks < TICK_CAP)

        val got = published(catalogId)!!
        // MUTATION: compare the floor against `scan.snapshot` instead of
        // the snapshot being evaluated at, and every later file-phase
        // tick re-anchors again: this reads 3, not 1.
        assertThat(got.sample.reanchors).isEqualTo(1)
        assertThat(got.sample.snapshotId).isEqualTo(start)

        // 20 seeded:
        //  - 16, 17, 18 ended before the re-anchor, above the cursor:
        //    EXCLUDED, the point of evaluating later pages at head;
        //  - 2 ended too, but was paged at the captured snapshot where it
        //    was live: COUNTED, the window semantics the KDoc states;
        //  - 21 appended above the cursor: COUNTED; 22 appended below it:
        //    NOT SEEN (the planner's new-bucket read is what finds it).
        // MUTATION: bind `scan.snapshot` rather than `scan.evaluatedAt`
        // into the page query and 16..18 are counted: 21 files, not 18,
        // and the row total moves by 51.
        val expected = (1..20).filter { it !in setOf(16, 17, 18) } + 21
        assertThat(totals(catalogId).files).isEqualTo(expected.size.toLong())
        assertThat(totals(catalogId).bytes).isEqualTo(expected.sumOf { if (it == 21) 1015L else 1000L + it })
        assertThat(totals(catalogId).rows).isEqualTo(expected.sum().toLong())
        assertThat(got.sample.smallFiles).isEqualTo(expected.size.toLong())
    }

    @Test
    fun `a policy change still restarts the scan rather than re-anchoring it`() {
        val (_, catalogId, _) = seed()
        assertThat(sampler().runOnce(BATCH)).isTrue()
        assertThat(scanState(catalogId)).isNotNull()
        // A different target changes what small_count and selected MEAN.
        // MUTATION: fold the policy check into the re-anchor branch and
        // the checkpoint survives with its old accumulators.
        assertThat(MaintenanceSummarySampler(db.jdbi, target * 2, 3, 64, 3_600).runOnce(BATCH)).isTrue()
        assertThat(scanState(catalogId)).describedAs("the checkpoint is discarded").isNull()
        // Leave nothing due: the other tests' first tick must be theirs.
        val other = MaintenanceSummarySampler(db.jdbi, target * 2, 3, 64, 3_600)
        while (other.runOnce(BATCH)) Unit
    }

    @Test
    fun `a sample that never re-anchored is written in the shape an older build parses`() {
        val (_, catalogId, _) = seed()
        val sampler = sampler()
        while (sampler.runOnce(BATCH)) Unit
        assertThat(published(catalogId)!!.sample.reanchors).isZero()
        // A build BEFORE this change parses the sample strictly, so a key
        // it does not know drops its whole sample there. A
        // rollback past this change must keep every catalog that never
        // re-anchored readable.
        // MUTATION: drop the `NON_DEFAULT` include on `Sample.reanchors`
        // and this reds: the key is written as 0 on every publish.
        val keys =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT jsonb_object_keys(sample) FROM hog_maintenance_summary WHERE catalog_id = :c",
                ).bind("c", catalogId).mapTo(String::class.java).list()
            }
        assertThat(keys).doesNotContain("reanchors")
    }

    @Test
    fun `a sample carrying a key this build does not know is still read`() {
        val (_, catalogId, _) = seed()
        val sampler = sampler()
        while (sampler.runOnce(BATCH)) Unit
        // What a NEWER build's publish looks like to this one.
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_maintenance_summary SET sample = sample || '{\"addedLater\": 7}'::jsonb " +
                    "WHERE catalog_id = ?",
                catalogId,
            )
        }
        // MUTATION: parse with the strict mapper in `sampleFrom` and this
        // reads null — the status page, the head table GET's totals and
        // the partition listing all go absent for the catalog until this
        // build republishes it.
        assertThat(published(catalogId)).isNotNull()
        assertThat(published(catalogId)!!.sample.smallFiles).isEqualTo(FILES.toLong())
    }
}
