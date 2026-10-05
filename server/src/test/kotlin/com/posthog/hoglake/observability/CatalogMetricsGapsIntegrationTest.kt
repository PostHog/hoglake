package com.posthog.hoglake.observability

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.persistence.DatabaseHealthRepo
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.publishMaintenanceSample
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.within
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * The catalog-metrics gauges that close the DuckLake-dashboard gaps:
 * live file count, the per-table series off the published summary
 * generation, deletion vectors, snapshot/retirement lifecycle, relation
 * sizes and the maintenance ledger — and the sampler's group isolation.
 *
 * THE FIXTURE carries one instance of every row each predicate exists to
 * exclude, so dropping a predicate changes a number asserted here:
 *  - `gone`, a DROPPED table that still has live data files and a live
 *    DV, and whose tier rows are still in the published generation (the
 *    drop lands AFTER the publish, which is the production order: a drop
 *    is O(columns) and leaves the generation alone);
 *  - `gone2`, a dropped table whose only file row is ENDED, so it is not
 *    pending retirement — and the NEWEST drop, so a probe cap of 1 sees
 *    only it;
 *  - an ENDED data-file row on a live table, and a SUPERSEDED (ended) DV;
 *  - `late`, a live table created after the generation's snapshot, which
 *    the generation does not cover;
 *  - `oldname`, renamed to `newname` after the publish (an ended version
 *    row beside the live one);
 *  - an IN-FLIGHT generation (published + 1) holding a 999-file bucket for
 *    `big`, as a scan in progress would;
 *  - a second bucket row for `little`'s one partition in the published
 *    generation, as a target change mid-generation produces (the bucket
 *    key hashes the quota);
 *  - a floor moved past a still-present older snapshot, with the two
 *    oldest snapshots backdated by an hour and half an hour;
 *  - `gaps-unsampled`, a catalog with a table and files and no published
 *    generation; `gaps-empty`, a catalog with nothing.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogMetricsGapsIntegrationTest {
    private companion object {
        const val CAT = "gaps"
        const val NEIGHBOUR = "gaps-empty"
        const val UNSAMPLED = "gaps-unsampled"

        /** Compaction target for the sample: 100-byte files are small, 5,000-byte ones are not. */
        const val TARGET = 1_000L

        /** Live data files on live tables: big 4 + little 2 + late 1 + newname 1. */
        const val LIVE_FILES = 8.0

        val TABLE_FAMILIES =
            listOf(
                "hoglake_table_files",
                "hoglake_table_small_files",
                "hoglake_table_bytes",
                "hoglake_table_small_bytes",
                "hoglake_table_rows",
                "hoglake_table_delete_files",
                "hoglake_table_partitions",
                "hoglake_table_largest_partition_files",
            )
    }

    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val registry = SimpleMeterRegistry()
    private val metrics by lazy { CatalogMetrics(db.jdbi, registry) }
    private var catalogId = 0L

    /** Epoch seconds of the two compaction runs and the expiry run seeded into the ledger. */
    private var compactionOkAt = 0.0
    private var compactionFailedAt = 0.0
    private var expiryOkAt = 0.0

    @AfterAll
    fun tearDown() = db.close()

    private fun file(
        cat: String,
        name: String,
        bytes: Long,
        records: Long = 10,
        partition: String? = null,
    ) = FileRegistration(
        path = "s3://$cat/data/$name.parquet",
        recordCount = records,
        fileSizeBytes = bytes,
        footerSize = 20,
        // Inline (empty) stats, not deferred: a DV may not target a pending file.
        columnStats = emptyList(),
        partitionValues = partition?.let { listOf(it) },
    )

    private fun file(
        name: String,
        bytes: Long,
        records: Long = 10,
        partition: String? = null,
    ) = file(CAT, name, bytes, records, partition)

    private fun head(cat: String = CAT): Long = catalogs.getCatalog(cat).headSnapshotId

    private fun append(
        table: String,
        vararg files: FileRegistration,
        cat: String = CAT,
    ) = commits.commit(
        cat,
        CommitRequest(readSnapshot = head(cat), appends = listOf(TableAppend("ns", table, files.toList()))),
    )

    private fun dv(
        table: String,
        dataPath: String,
        count: Long,
        bytes: Long,
    ) {
        val dataFileId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT data_file_id FROM hog_data_file WHERE catalog_id = ? AND path = ?")
                    .bind(0, catalogId)
                    .bind(1, "s3://$CAT/data/$dataPath.parquet")
                    .mapTo(Long::class.java)
                    .one()
            }
        val dv = DeleteFileRegistration(dataFileId, "s3://$CAT/dv/$dataPath-$count.puffin", count, bytes)
        commits.commit(
            CAT,
            CommitRequest(readSnapshot = head(), deletes = listOf(TableDeletes("ns", table, listOf(dv)))),
        )
    }

    private fun tableId(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT table_id FROM hog_table_version WHERE catalog_id = ? AND name = ? LIMIT 1")
                .bind(0, catalogId)
                .bind(1, name)
                .mapTo(Long::class.java)
                .one()
        }

    @BeforeAll
    fun seed() {
        catalogs.createCatalog(CAT, "s3://$CAT")
        catalogs.createCatalog(NEIGHBOUR, "s3://$NEIGHBOUR")
        catalogs.createNamespace(CAT, "ns")
        catalogId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = ?")
                    .bind(0, CAT)
                    .mapTo(Long::class.java)
                    .one()
            }
        val cols = listOf(ColumnDef("p", ColType.STRING), ColumnDef("id", ColType.LONG))

        // `big`: partitioned on p. Partition a = 2 small + 1 large file,
        // partition b = 1 small file.
        catalogs.createTable(CAT, "ns", "big", cols)
        alter.alterTable(
            CAT,
            "ns",
            "big",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
        )
        append(
            "big",
            file("big-a1", 100, partition = "a"),
            file("big-a2", 100, partition = "a"),
            file("big-a3", 5_000, partition = "a"),
            file("big-b1", 100, partition = "b"),
        )

        // `little`: unpartitioned, two small files, one DV (superseded
        // once, so an ENDED DV row exists beside the live one).
        catalogs.createTable(CAT, "ns", "little", cols)
        append("little", file("little-1", 100, records = 7), file("little-2", 100, records = 7))
        dv("little", "little-1", 1, 50)
        dv("little", "little-1", 2, 60)

        // `oldname`: one file; renamed to `newname` after the publish.
        catalogs.createTable(CAT, "ns", "oldname", cols)
        append("oldname", file("renamed-1", 100))

        // `gone`: files and a live DV, dropped after the publish below.
        catalogs.createTable(CAT, "ns", "gone", cols)
        append("gone", *(1..5).map { file("gone-$it", 100) }.toTypedArray())
        dv("gone", "gone-1", 1, 999)

        // `gone2`: one file, ended below, dropped after the publish.
        catalogs.createTable(CAT, "ns", "gone2", cols)
        append("gone2", file("gone2-1", 100))

        publishMaintenanceSample(db.jdbi, TARGET)

        catalogs.dropTable(CAT, "ns", "gone")
        catalogs.dropTable(CAT, "ns", "gone2")
        alter.alterTable(CAT, "ns", "oldname", listOf(AlterOp.RenameTable("newname")))

        // `late`: created and filled after the generation's snapshot.
        catalogs.createTable(CAT, "ns", "late", cols)
        append("late", file("late-1", 100))

        // `gaps-unsampled`: tables and files, but created after the
        // sampler ran, so it has no published generation.
        catalogs.createCatalog(UNSAMPLED, "s3://$UNSAMPLED")
        catalogs.createNamespace(UNSAMPLED, "ns")
        catalogs.createTable(UNSAMPLED, "ns", "u", cols)
        append("u", file(UNSAMPLED, "u-1", 100), cat = UNSAMPLED)

        val headNow = head()
        val bigId = tableId("big")
        val littleId = tableId("little")
        db.jdbi.useHandleUnchecked { h ->
            // An ENDED file row on a live table (what compaction leaves
            // until expiry purges it), and gone2's only row ended.
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 900000, ?, 1, ?, 's3://gaps/data/ended.parquet', 1000, 77777, 900000)
                """,
                catalogId,
                bigId,
                headNow,
            )
            h.execute(
                "UPDATE hog_data_file SET end_snapshot = ? " +
                    "WHERE catalog_id = ? AND path = 's3://gaps/data/gone2-1.parquet'",
                headNow,
                catalogId,
            )

            // An IN-FLIGHT generation: the sampler has moved to published
            // + 1 and accumulated a bucket for `big` that no publish has
            // blessed. Reading it would report 999 files.
            val published =
                h.createQuery("SELECT published_generation FROM hog_maintenance_summary WHERE catalog_id = ?")
                    .bind(0, catalogId)
                    .mapTo(Long::class.java)
                    .one()
            h.execute(
                "UPDATE hog_maintenance_summary SET generation = ? WHERE catalog_id = ?",
                published + 1,
                catalogId,
            )
            h.execute(
                """
                INSERT INTO hog_maintenance_summary_tier
                    (catalog_id, generation, bucket_key, table_id, quota, remaining, pending,
                     file_count, small_count, total_bytes, small_bytes, dv_count, record_count)
                VALUES (?, ?, 'in-flight', ?, 1, 1, 0, 999, 999, 999, 999, 0, 999)
                """,
                catalogId,
                published + 1,
                bigId,
            )
            // A SECOND bucket row for `little`'s one partition in the
            // published generation, as a compaction-target change mid-scan
            // leaves behind (the bucket key hashes the quota): one more
            // small file of 7 rows. Still one partition.
            h.execute(
                """
                INSERT INTO hog_maintenance_summary_tier
                    (catalog_id, generation, bucket_key, table_id, spec_id, partition_values, quota, remaining,
                     pending, file_count, small_count, total_bytes, small_bytes, dv_count, record_count)
                VALUES (?, ?, 'little-other-quota', ?, NULL, NULL, 7, 1, 0, 1, 1, 100, 100, 0, 7)
                """,
                catalogId,
                published,
                littleId,
            )

            // The floor moved past a snapshot that is still present: the
            // two oldest snapshots backdated by an hour and half an hour,
            // the floor at the second. The age is the second's.
            val oldest =
                h.createQuery(
                    "SELECT snapshot_id FROM hog_snapshot WHERE catalog_id = ? ORDER BY snapshot_id LIMIT 2",
                ).bind(0, catalogId).mapTo(Long::class.java).list()
            h.execute(
                "UPDATE hog_snapshot SET snapshot_time = now() - interval '1 hour' " +
                    "WHERE catalog_id = ? AND snapshot_id = ?",
                catalogId,
                oldest[0],
            )
            h.execute(
                "UPDATE hog_snapshot SET snapshot_time = now() - interval '30 minutes' " +
                    "WHERE catalog_id = ? AND snapshot_id = ?",
                catalogId,
                oldest[1],
            )
            h.execute("UPDATE hog_catalog SET earliest_snapshot_id = ? WHERE catalog_id = ?", oldest[1], catalogId)

            // The maintenance ledger: compaction ok then failed, expiry ok.
            val now =
                h.createQuery("SELECT extract(epoch FROM date_trunc('second', now()))")
                    .mapTo(Double::class.java)
                    .one()
            compactionOkAt = now - 600
            compactionFailedAt = now - 60
            expiryOkAt = now - 30
            run(h, "compaction", "ok", compactionOkAt, 2)
            run(h, "compaction", "failed", compactionFailedAt, 5)
            run(h, "expiry", "ok", expiryOkAt, 3)
            h.execute("ANALYZE")
        }
    }

    private fun run(
        h: org.jdbi.v3.core.Handle,
        task: String,
        status: String,
        finishedAt: Double,
        durationSeconds: Int,
    ) = h.execute(
        """
        INSERT INTO hog_maintenance_run (catalog_id, task, run_trigger, started_at, finished_at, status, error)
        VALUES (?, ?, 'loop', to_timestamp(?) - make_interval(secs => ?), to_timestamp(?), ?, ?)
        """,
        catalogId,
        task,
        finishedAt,
        durationSeconds,
        finishedAt,
        status,
        if (status == "failed") "boom" else null,
    )

    private fun value(
        reg: SimpleMeterRegistry,
        name: String,
        vararg tags: String,
    ): Double? = reg.find(name).tags(*tags).gauge()?.value()

    private fun value(
        name: String,
        vararg tags: String,
    ): Double? = value(registry, name, *tags)

    /** `namespace.table` for every series of [name] on [catalog]. */
    private fun tableSeries(
        reg: SimpleMeterRegistry,
        name: String,
        catalog: String = CAT,
    ): Set<String> =
        reg.find(name).tag("catalog", catalog).gauges()
            .map { "${it.id.getTag("namespace")}.${it.id.getTag("table")}" }
            .toSet()

    private fun table(name: String) = arrayOf("catalog", CAT, "namespace", "ns", "table", name)

    private fun withRenamed(
        relation: String,
        block: () -> Unit,
    ) {
        db.jdbi.useHandleUnchecked { it.execute("ALTER TABLE $relation RENAME TO ${relation}_hidden") }
        try {
            block()
        } finally {
            db.jdbi.useHandleUnchecked { it.execute("ALTER TABLE ${relation}_hidden RENAME TO $relation") }
        }
    }

    @Test
    fun `live files ride the manifest pass and exclude dropped tables and ended rows`() {
        metrics.sampleOnce()
        // NOT gone's 5 (dropped, still live rows), NOT the ended row on big.
        assertThat(value("hoglake_live_files", "catalog", CAT)).isEqualTo(LIVE_FILES)
        assertThat(value("hoglake_live_files", "catalog", NEIGHBOUR)).isEqualTo(0.0)
    }

    @Test
    fun `per-table series come from the published generation, live covered tables only`() {
        metrics.sampleOnce()
        // MUTATION: join the tier on the summary's LIVE `generation`
        // instead of `published_generation` and big reads 999 files.
        val big = table("big")
        assertThat(value("hoglake_table_files", *big)).isEqualTo(4.0)
        assertThat(value("hoglake_table_small_files", *big)).isEqualTo(3.0)
        assertThat(value("hoglake_table_bytes", *big)).isEqualTo(5_300.0)
        assertThat(value("hoglake_table_small_bytes", *big)).isEqualTo(300.0)
        assertThat(value("hoglake_table_rows", *big)).isEqualTo(40.0)
        assertThat(value("hoglake_table_delete_files", *big)).isEqualTo(0.0)
        assertThat(value("hoglake_table_partitions", *big)).isEqualTo(2.0)
        assertThat(value("hoglake_table_largest_partition_files", *big)).isEqualTo(3.0)

        // little's one partition has TWO bucket rows (2 files + 1 file).
        // MUTATION: count bucket rows instead of grouping by (spec,
        // partition values) first and partitions reads 2, largest 2.
        val little = table("little")
        assertThat(value("hoglake_table_files", *little)).isEqualTo(3.0)
        assertThat(value("hoglake_table_small_files", *little)).isEqualTo(3.0)
        assertThat(value("hoglake_table_bytes", *little)).isEqualTo(300.0)
        assertThat(value("hoglake_table_small_bytes", *little)).isEqualTo(300.0)
        assertThat(value("hoglake_table_rows", *little)).isEqualTo(21.0)
        assertThat(value("hoglake_table_delete_files", *little)).isEqualTo(1.0)
        assertThat(value("hoglake_table_partitions", *little)).isEqualTo(1.0)
        assertThat(value("hoglake_table_largest_partition_files", *little)).isEqualTo(3.0)

        assertThat(value("hoglake_table_files", *table("newname"))).isEqualTo(1.0)

        // MUTATION: drop BOTH `t.dropped_snapshot IS NULL` and the
        // live-version join's `v.end_snapshot IS NULL` and ns.gone /
        // ns.gone2 appear; either alone is covered by the other, because
        // a drop ends the live version row. Drop `v.end_snapshot IS NULL`
        // alone and the renamed table ALSO publishes under ns.oldname.
        // Drop the coverage clause and ns.late appears at 0.
        for (family in TABLE_FAMILIES) {
            assertThat(tableSeries(registry, family))
                .describedAs(family)
                .containsExactlyInAnyOrder("ns.big", "ns.little", "ns.newname")
        }
        assertThat(value("hoglake_table_series_truncated", "catalog", CAT)).isEqualTo(0.0)
        assertThat(value("hoglake_table_series_truncated", "catalog", NEIGHBOUR)).isEqualTo(0.0)
    }

    @Test
    fun `a catalog with no published generation publishes no table series`() {
        metrics.sampleOnce()
        for (family in TABLE_FAMILIES) {
            assertThat(tableSeries(registry, family, UNSAMPLED)).describedAs(family).isEmpty()
        }
        assertThat(value("hoglake_table_series_truncated", "catalog", UNSAMPLED)).isNull()
        // The core group still covers it.
        assertThat(value("hoglake_live_files", "catalog", UNSAMPLED)).isEqualTo(1.0)
    }

    @Test
    fun `table rows are withheld while the generation's row measure is incomplete`() {
        db.jdbi.useHandleUnchecked {
            it.execute("UPDATE hog_maintenance_summary SET measures_generation = -1 WHERE catalog_id = ?", catalogId)
        }
        try {
            val reg = SimpleMeterRegistry()
            CatalogMetrics(db.jdbi, reg).sampleOnce()
            // MUTATION: drop the `measured` filter and rows appear.
            assertThat(tableSeries(reg, "hoglake_table_rows")).isEmpty()
            assertThat(value(reg, "hoglake_table_files", *table("big"))).isEqualTo(4.0)
        } finally {
            db.jdbi.useHandleUnchecked {
                it.execute(
                    "UPDATE hog_maintenance_summary SET measures_generation = published_generation " +
                        "WHERE catalog_id = ?",
                    catalogId,
                )
            }
        }
    }

    @Test
    fun `the per-catalog table cap keeps the largest tables and says how many it cut`() {
        val capped = SimpleMeterRegistry()
        CatalogMetrics(db.jdbi, capped, maxTableSeries = 1).sampleOnce()
        // MUTATION: drop `rank <= :cap` and the others come back; order
        // the window ascending and ns.newname replaces ns.big.
        assertThat(tableSeries(capped, "hoglake_table_files")).containsExactly("ns.big")
        assertThat(tableSeries(capped, "hoglake_table_rows")).containsExactly("ns.big")
        assertThat(value(capped, "hoglake_table_series_truncated", "catalog", CAT)).isEqualTo(2.0)
        assertThat(value(capped, "hoglake_table_series_truncated", "catalog", NEIGHBOUR)).isEqualTo(0.0)
    }

    @Test
    fun `delete files count live DVs on live tables only`() {
        metrics.sampleOnce()
        // little's live DV (60 bytes). NOT its superseded 50-byte DV, NOT
        // gone's 999-byte DV on a dropped table.
        assertThat(value("hoglake_live_delete_files", "catalog", CAT)).isEqualTo(1.0)
        assertThat(value("hoglake_live_delete_bytes", "catalog", CAT)).isEqualTo(60.0)
        assertThat(value("hoglake_live_delete_files", "catalog", NEIGHBOUR)).isEqualTo(0.0)
        assertThat(value("hoglake_live_delete_bytes", "catalog", NEIGHBOUR)).isEqualTo(0.0)
    }

    @Test
    fun `earliest snapshot age is the floor snapshot's, not the oldest row's or the head's`() {
        metrics.sampleOnce()
        val floorAge =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT extract(epoch FROM now() - s.snapshot_time)
                      FROM hog_snapshot s JOIN hog_catalog c
                        ON c.catalog_id = s.catalog_id AND s.snapshot_id = c.earliest_snapshot_id
                     WHERE c.catalog_id = ?
                    """,
                ).bind(0, catalogId).mapTo(Double::class.java).one()
            }
        assertThat(floorAge).isCloseTo(1_800.0, within(60.0))
        // MUTATION: ORDER BY snapshot_id DESC reads the head (~0 s); drop
        // the `>= earliest_snapshot_id` clause and it reads the hour-old
        // snapshot below the floor.
        assertThat(value("hoglake_earliest_snapshot_age_seconds", "catalog", CAT)).isCloseTo(floorAge, within(5.0))
        // The catalogs listing's instant comes from the same probe.
        val oldest = metrics.latestByCatalog.getValue(CAT).oldestSnapshotTime!!
        assertThat(java.time.Duration.between(oldest, java.time.Instant.now()).seconds.toDouble())
            .isCloseTo(floorAge, within(5.0))
    }

    @Test
    fun `dropped tables pending retirement is a lower bound over the newest drops`() {
        metrics.sampleOnce()
        // gone has live rows; gone2's only row is ended; the others are
        // not dropped.
        assertThat(value("hoglake_dropped_tables_pending_retirement", "catalog", CAT)).isEqualTo(1.0)
        assertThat(value("hoglake_dropped_tables_pending_retirement", "catalog", NEIGHBOUR)).isEqualTo(0.0)

        // The cap and its ORDER: with one probe only the NEWEST drop
        // (gone2, drained) is looked at, so the older pending `gone` is
        // not counted — the documented lower bound. MUTATION: drop the
        // LIMIT, or order ascending, and this reads 1.
        val one = SimpleMeterRegistry()
        CatalogMetrics(db.jdbi, one, droppedTableProbeCap = 1).sampleOnce()
        assertThat(value(one, "hoglake_dropped_tables_pending_retirement", "catalog", CAT)).isEqualTo(0.0)
        val two = SimpleMeterRegistry()
        CatalogMetrics(db.jdbi, two, droppedTableProbeCap = 2).sampleOnce()
        assertThat(value(two, "hoglake_dropped_tables_pending_retirement", "catalog", CAT)).isEqualTo(1.0)
    }

    @Test
    fun `relation sizes and tuple estimates match the database health read`() {
        metrics.sampleOnce()
        val health = db.jdbi.withHandleUnchecked { h -> DatabaseHealthRepo.tables(h) }.associateBy { it.name }
        val dataFile = health.getValue("hog_data_file")
        assertThat(value("hoglake_relation_bytes", "relation", "hog_data_file", "kind", "heap"))
            .isEqualTo(dataFile.tableBytes.toDouble())
            .isGreaterThan(0.0)
        assertThat(value("hoglake_relation_bytes", "relation", "hog_data_file", "kind", "index"))
            .isEqualTo(dataFile.indexBytes.toDouble())
            .isGreaterThan(0.0)
        assertThat(value("hoglake_relation_bytes", "relation", "hog_data_file", "kind", "toast"))
            .isEqualTo(dataFile.toastBytes.toDouble())
        assertThat(value("hoglake_relation_live_tuples", "relation", "hog_data_file")).isNotNull()
        assertThat(value("hoglake_relation_dead_tuples", "relation", "hog_data_file")).isNotNull()
        // Every hog_* relation, and only those.
        assertThat(registry.find("hoglake_relation_live_tuples").gauges().map { it.id.getTag("relation") }.toSet())
            .isEqualTo(health.keys)
    }

    @Test
    fun `maintenance ledger gauges carry the latest run per status and the latest duration`() {
        metrics.sampleOnce()
        val compaction = arrayOf("catalog", CAT, "task", "compaction")
        assertThat(value("hoglake_maintenance_last_run_epoch", *compaction, "status", "ok")).isEqualTo(compactionOkAt)
        assertThat(value("hoglake_maintenance_last_run_epoch", *compaction, "status", "failed"))
            .isEqualTo(compactionFailedAt)
        assertThat(value("hoglake_maintenance_last_run_duration_seconds", *compaction)).isEqualTo(5.0)

        val expiry = arrayOf("catalog", CAT, "task", "expiry")
        assertThat(value("hoglake_maintenance_last_run_epoch", *expiry, "status", "ok")).isEqualTo(expiryOkAt)
        assertThat(value("hoglake_maintenance_last_run_epoch", *expiry, "status", "failed")).isNull()
        assertThat(value("hoglake_maintenance_last_run_duration_seconds", *expiry)).isEqualTo(3.0)
        // A task that never ran has no series rather than a zero epoch.
        assertThat(value("hoglake_maintenance_last_run_duration_seconds", "catalog", CAT, "task", "cleanup")).isNull()
        // MUTATION: drop `k.catalog_id = c.catalog_id` and the neighbour
        // inherits gaps' ledger.
        assertThat(registry.find("hoglake_maintenance_last_run_epoch").tag("catalog", NEIGHBOUR).gauges()).isEmpty()
        assertThat(registry.find("hoglake_maintenance_last_run_duration_seconds").tag("catalog", NEIGHBOUR).gauges())
            .isEmpty()

        // The window is the bound: with lookback 1 only compaction's
        // newest row (failed) is read, so a FRESH process has no ok.
        // MUTATION: drop the LIMIT and ok comes back.
        val narrow = SimpleMeterRegistry()
        CatalogMetrics(db.jdbi, narrow, maintenanceRunLookback = 1).sampleOnce()
        assertThat(value(narrow, "hoglake_maintenance_last_run_epoch", *compaction, "status", "ok")).isNull()
        assertThat(value(narrow, "hoglake_maintenance_last_run_epoch", *compaction, "status", "failed"))
            .isEqualTo(compactionFailedAt)
    }

    @Test
    fun `a status that scrolls out of the window is republished from memory`() {
        val now = System.currentTimeMillis() / 1000.0
        val okAt = Math.floor(now) - 500
        db.jdbi.useHandleUnchecked { run(it, "retirement", "ok", okAt, 1) }
        val reg = SimpleMeterRegistry()
        val m = CatalogMetrics(db.jdbi, reg, maintenanceRunLookback = 2)
        val retirement = arrayOf("catalog", CAT, "task", "retirement")
        m.sampleOnce()
        assertThat(value(reg, "hoglake_maintenance_last_run_epoch", *retirement, "status", "ok")).isEqualTo(okAt)

        // Two failures push the success out of a 2-row window: a staleness
        // alert on status="ok" must keep its series (and keep ageing),
        // not lose it. MUTATION: publish only this window's rows and the
        // ok series disappears.
        db.jdbi.useHandleUnchecked {
            run(it, "retirement", "failed", okAt + 100, 1)
            run(it, "retirement", "failed", okAt + 200, 1)
        }
        m.sampleOnce()
        assertThat(value(reg, "hoglake_maintenance_last_run_epoch", *retirement, "status", "ok")).isEqualTo(okAt)
        assertThat(value(reg, "hoglake_maintenance_last_run_epoch", *retirement, "status", "failed"))
            .isEqualTo(okAt + 200)
    }

    @Test
    fun `a deleted catalog's remembered ledger epochs are forgotten`() {
        catalogs.createCatalog("gaps-doomed", "s3://gaps-doomed")
        val now = Math.floor(System.currentTimeMillis() / 1000.0)
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_maintenance_run (catalog_id, task, run_trigger, started_at, finished_at, status)
                SELECT catalog_id, 'expiry', 'loop', to_timestamp(?), to_timestamp(?), 'ok'
                  FROM hog_catalog WHERE name = 'gaps-doomed'
                """,
                now - 10,
                now - 9,
            )
        }
        val reg = SimpleMeterRegistry()
        val m = CatalogMetrics(db.jdbi, reg)
        m.sampleOnce()
        assertThat(value(reg, "hoglake_maintenance_last_run_epoch", "catalog", "gaps-doomed", "status", "ok"))
            .isEqualTo(now - 9)

        // The catalog goes (its ledger rows cascade with it). MUTATION:
        // keep every remembered key regardless of the catalog list and the
        // series outlives the catalog forever.
        db.jdbi.useHandleUnchecked { it.execute("DELETE FROM hog_catalog WHERE name = 'gaps-doomed'") }
        m.sampleOnce()
        assertThat(reg.find("hoglake_maintenance_last_run_epoch").tag("catalog", "gaps-doomed").gauges()).isEmpty()
        // The survivors are untouched.
        assertThat(value(reg, "hoglake_maintenance_last_run_epoch", "catalog", CAT, "task", "expiry", "status", "ok"))
            .isEqualTo(expiryOkAt)
    }

    @Test
    fun `one failing extended group does not stop the others or the liveness epoch`() {
        val reg = SimpleMeterRegistry()
        val m = CatalogMetrics(db.jdbi, reg)
        // Every group's failure counter exists at zero before anything fails.
        assertThat(reg.find("hoglake_metrics_group_failures_total").counters().map { it.id.getTag("group") })
            .containsExactlyInAnyOrderElementsOf(CatalogMetrics.GROUPS)
        assertThat(reg.find("hoglake_metrics_group_failures_total").counters().map { it.count() }).containsOnly(0.0)
        withRenamed("hog_maintenance_run") {
            assertThrows(Exception::class.java) { m.sampleOnce() }
        }
        assertThat(reg.get("hoglake_metrics_sample_errors_total").counter().count()).isEqualTo(1.0)
        // Exactly the broken group is counted, once.
        val failures =
            reg.find("hoglake_metrics_group_failures_total").counters().associate {
                it.id.getTag("group") to it.count()
            }
        assertThat(failures).containsEntry("maintenance_runs", 1.0)
        assertThat(failures.filterKeys { it != "maintenance_runs" }.values).containsOnly(0.0)
        // The core group succeeded, so the liveness epoch advanced.
        // MUTATION: move the epoch set back after the failure check and
        // this reads 0.
        assertThat(reg.get("hoglake_metrics_last_sample_epoch").gauge().value()).isGreaterThan(0.0)
        assertThat(value(reg, "hoglake_head_snapshot_id", "catalog", CAT)).isGreaterThan(0.0)
        assertThat(value(reg, "hoglake_live_files", "catalog", CAT)).isEqualTo(LIVE_FILES)
        // Every other group still published.
        assertThat(value(reg, "hoglake_table_files", *table("big"))).isEqualTo(4.0)
        assertThat(value(reg, "hoglake_live_delete_files", "catalog", CAT)).isEqualTo(1.0)
        assertThat(value(reg, "hoglake_dropped_tables_pending_retirement", "catalog", CAT)).isEqualTo(1.0)
        assertThat(reg.find("hoglake_relation_bytes").gauges()).isNotEmpty()
        assertThat(reg.find("hoglake_maintenance_last_run_duration_seconds").gauges()).isEmpty()
    }

    @Test
    fun `a failing core group leaves the epoch alone while extended groups still publish`() {
        val reg = SimpleMeterRegistry()
        val m = CatalogMetrics(db.jdbi, reg)
        withRenamed("hog_consumer_offset") {
            assertThrows(Exception::class.java) { m.sampleOnce() }
        }
        assertThat(reg.get("hoglake_metrics_group_failures_total").tag("group", "catalogs").counter().count())
            .isEqualTo(1.0)
        assertThat(reg.get("hoglake_metrics_sample_errors_total").counter().count()).isEqualTo(1.0)
        // MUTATION: set the epoch after the group block regardless of its
        // outcome and this reads non-zero.
        assertThat(reg.get("hoglake_metrics_last_sample_epoch").gauge().value()).isEqualTo(0.0)
        assertThat(value(reg, "hoglake_live_files", "catalog", CAT)).isNull()
        assertThat(value(reg, "hoglake_table_files", *table("big"))).isEqualTo(4.0)
    }

    @Test
    fun `without extended groups only the core gauges publish`() {
        val reg = SimpleMeterRegistry()
        CatalogMetrics(db.jdbi, reg, extendedGroups = false).sampleOnce()
        assertThat(value(reg, "hoglake_live_files", "catalog", CAT)).isEqualTo(LIVE_FILES)
        assertThat(reg.get("hoglake_metrics_last_sample_epoch").gauge().value()).isGreaterThan(0.0)
        // MUTATION: run the extended groups unconditionally and these appear.
        for (
        family in TABLE_FAMILIES +
            listOf(
                "hoglake_table_series_truncated",
                "hoglake_live_delete_files",
                "hoglake_earliest_snapshot_age_seconds",
                "hoglake_dropped_tables_pending_retirement",
                "hoglake_relation_bytes",
                "hoglake_maintenance_last_run_epoch",
            )
        ) {
            assertThat(reg.find(family).gauges()).describedAs(family).isEmpty()
        }
    }
}
