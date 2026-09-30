package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.compaction.CompactionGrouping
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.PartitionListing
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.atomic.AtomicInteger

/**
 * `PartitionListingService`: a table's partitions as the maintenance
 * sampler measured them.
 *
 * Metadata-only — no object store anywhere. The threshold is a
 * test-sized 1000 bytes with a two-file group minimum, so these small
 * fixtures form groups at all; production wires
 * `Config.compactionTargetBytes`, the same knob compaction plans with.
 *
 * EVERY SEMANTIC CASE GOES THROUGH THE REAL SAMPLER. Seeding
 * `hog_maintenance_summary_tier` by hand would assert the shape this
 * test wrote rather than the shape the sampler writes, which is the
 * mistake that would hide a missed column in the INSERT. The two places
 * that DO seed by hand are the ones the sampler cannot produce: a row
 * from a build that predates V22 (the whole point of the null
 * `record_count`), and the 5,000-group latency fixture, where 5,000
 * real commits would measure the commit path rather than the listing.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PartitionListingServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val counter = AtomicInteger(0)

    private val listings =
        PartitionListingService(
            db.jdbi,
            smallFileThresholdBytes = TARGET,
            minInputFiles = 2,
            maxInputFiles = CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
        )

    @AfterAll
    fun tearDown() = db.close()

    /** Run the sampler to completion, then list — the published sample is the input. */
    private fun list(
        cat: String,
        table: String = "t",
        sort: PartitionListingService.SortColumn = PartitionListingService.SortColumn.PARTITION,
        desc: Boolean? = null,
        limit: Int = 100,
        offset: Int = 0,
        filters: List<PartitionListingService.Filter> = emptyList(),
    ): PartitionListing {
        sample()
        return listings.listPartitions(cat, "ns", table, sort, desc, limit, offset, filters)
    }

    private fun sample() {
        // Every catalog, because `runOnce` claims whichever is due and
        // a half-sampled neighbour would keep the loop spinning. The
        // cost grows with the class, not with any one assertion.
        db.jdbi.useHandleUnchecked { it.execute("UPDATE hog_maintenance_summary SET next_batch_at = now()") }
        val sampler =
            MaintenanceSummarySampler(db.jdbi, TARGET, 2, CompactionGrouping.DEFAULT_MAX_INPUT_FILES, 3600)
        while (sampler.runOnce()) { /* materialize a complete sample before asserting on it */ }
    }

    private fun fixture(
        columns: List<ColumnDef> =
            listOf(
                ColumnDef("id", ColType.LONG),
                ColumnDef("team", ColType.STRING),
                ColumnDef("ts", ColType.TIMESTAMP),
            ),
        table: String = "t",
    ): String {
        val cat = "plist-cat-${counter.incrementAndGet()}"
        db.jdbi.useHandleUnchecked { h ->
            // Raw insert: this suite tests the listing, not catalog
            // validation. The shared "s3://bucket" data_path would trip
            // the creation-time shape/overlap rules.
            val id =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES (?, ?) RETURNING catalog_id",
                ).bind(0, cat).bind(1, "s3://bucket").mapTo(Long::class.java).one()
            h.execute("INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) VALUES (?, 0, 0)", id)
        }
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", table, columns)
        return cat
    }

    private fun addTable(
        cat: String,
        table: String,
    ) = catalogs.createTable(
        cat,
        "ns",
        table,
        listOf(
            ColumnDef("id", ColType.LONG),
            ColumnDef("team", ColType.STRING),
            ColumnDef("ts", ColType.TIMESTAMP),
        ),
    )

    private fun append(
        cat: String,
        table: String,
        vararg files: FileRegistration,
        // readSnapshot = head: an append carrying partition_values
        // requires one (invariant 12), and the fixture has just finished
        // whatever DDL it meant to do, so head is its honest answer.
    ) = commits.commit(
        cat,
        CommitRequest(
            readSnapshot = db.head(cat),
            appends = listOf(TableAppend("ns", table, files.toList())),
        ),
    )

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

    /**
     * A live deletion vector over one data file, so `dv_count` differs
     * between groups. Registered through the commit path, which is what
     * the sampler's per-file EXISTS probe reads.
     */
    private fun deleteVectorOn(
        cat: String,
        path: String,
    ) {
        val fileId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT f.data_file_id FROM hog_data_file f
                    JOIN hog_catalog c ON c.catalog_id = f.catalog_id
                    WHERE c.name = :cat AND f.path = :path
                    """,
                ).bind("cat", cat).bind("path", path).mapTo(Long::class.java).one()
            }
        val head =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE name = :cat")
                    .bind("cat", cat).mapTo(Long::class.java).one()
            }
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = head,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(
                                DeleteFileRegistration(
                                    dataFileId = fileId,
                                    path = "$path.dv",
                                    deleteCount = 1,
                                    fileSizeBytes = 16,
                                ),
                            ),
                        ),
                    ),
            ),
        )
    }

    /** `team` (identity, field 2). */
    private fun partitionByTeam(
        cat: String,
        table: String = "t",
    ) = alter.alterTable(
        cat,
        "ns",
        table,
        listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY)))),
    )

    /** `ts` under a day transform (field 3) — the decoding case. */
    private fun partitionByDay(
        cat: String,
        table: String = "t",
    ) = alter.alterTable(
        cat,
        "ns",
        table,
        listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(3, Transform.DAY)))),
    )

    // ---- scoping and grouping ------------------------------------------------

    @Test
    fun `the listing covers one table, across spec vintages, including a null value`() {
        val cat = fixture()
        addTable(cat, "other")
        // Pre-spec vintage on t: one unpartitioned group.
        append(cat, "t", file("u1", 100), file("u2", 100))
        partitionByTeam(cat)
        append(
            cat,
            "t",
            file("p1a", 100, values = listOf("p1")),
            file("p1b", 100, values = listOf("p1")),
            // A NULL partition value is its own group, not a missing one.
            file("na", 100, values = listOf(null)),
        )
        // The other table's files must never appear in t's listing.
        append(cat, "other", file("o1", 100), file("o2", 100))

        val got = list(cat)
        assertThat(got.sampledAt).isNotNull()
        assertThat(got.sampledSnapshotId).isNotNull()
        assertThat(got.total).isEqualTo(3)
        assertThat(got.partitions).hasSize(3)
        // Default sort is the decoded tuple ascending, nulls first, and
        // the unpartitioned group's EMPTY tuple sorts ahead of both.
        assertThat(got.partitions.map { p -> p.values.map { it.decoded } })
            .containsExactly(emptyList(), listOf(null), listOf("p1"))
        assertThat(got.partitions[0].specId).isNull()
        assertThat(got.partitions[1].values.single().field).isEqualTo("team")
        assertThat(got.partitions[1].values.single().raw).isNull()
        assertThat(got.partitions[2].values.single().raw).isEqualTo("p1")
        // The spec-less vintage is not the current spec's.
        assertThat(got.staleSpecGroups).isEqualTo(1)
        val specField = got.spec!!.fields.single()
        assertThat(specField.field).isEqualTo("team")
        assertThat(specField.transform).isEqualTo("identity")

        // Scoping, from the other side: `other` sees only its own group.
        val other = listings.listPartitions(cat, "ns", "other")
        assertThat(other.total).isEqualTo(1)
        assertThat(other.partitions.single().fileCount).isEqualTo(2)
    }

    @Test
    fun `measures and avg bytes come from the sample`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(
            cat,
            "t",
            // Two files of EQUAL size: the planner's dominance split
            // closes a group before a file that outweighs everything
            // held, so 100 + 300 would be two groups of one and no debt
            // at all. This is the shape that is actionable.
            file("a1", 100, records = 7, values = listOf("a")),
            file("a2", 100, records = 5, values = listOf("a")),
            file("b1", 1500, records = 9, values = listOf("b")),
        )
        val got = list(cat)
        val a = got.partitions.single { it.values.single().decoded == "a" }
        assertThat(a.fileCount).isEqualTo(2)
        assertThat(a.smallFileCount).isEqualTo(2)
        assertThat(a.totalBytes).isEqualTo(200)
        assertThat(a.smallFileBytes).isEqualTo(200)
        assertThat(a.avgFileBytes).isEqualTo(100)
        assertThat(a.debtScore).isEqualTo(2)
        assertThat(a.recordCount).isEqualTo(12)
        assertThat(a.lastWrittenSnapshot).isEqualTo(got.sampledSnapshotId)

        val b = got.partitions.single { it.values.single().decoded == "b" }
        // At or over the target: counted, never a candidate.
        assertThat(b.smallFileCount).isEqualTo(0)
        assertThat(b.debtScore).isEqualTo(0)
        assertThat(b.avgFileBytes).isEqualTo(1500)
        assertThat(b.recordCount).isEqualTo(9)
    }

    @Test
    fun `avg bytes is zero rather than a division by zero when a bucket holds no files`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(cat, "t", file("a1", 100, values = listOf("a")))
        sample()
        // A bucket with no files is not a shape the sampler produces (a
        // bucket exists because a file put it there), so it is seeded —
        // the point is that the arithmetic degrades rather than throws.
        seedTierRow(cat, specId = 1, values = listOf("empty"), fileCount = 0, totalBytes = 0)
        val got = listings.listPartitions(cat, "ns", "t")
        assertThat(got.partitions.single { it.values.single().raw == "empty" }.avgFileBytes).isEqualTo(0)
    }

    // ---- sorting -------------------------------------------------------------

    @Test
    fun `every sort column orders both ways, and measures default to descending`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(
            cat,
            "t",
            // a: 1 file, 900 bytes, 1 row
            file("a1", 900, records = 1, values = listOf("a")),
            // b: 3 files, 300 bytes, 30 rows
            file("b1", 100, records = 10, values = listOf("b")),
            file("b2", 100, records = 10, values = listOf("b")),
            file("b3", 100, records = 10, values = listOf("b")),
            // c: 2 files, 200 bytes, 4 rows
            file("c1", 100, records = 2, values = listOf("c")),
            file("c2", 100, records = 2, values = listOf("c")),
        )
        sample()

        fun keys(
            sort: PartitionListingService.SortColumn,
            desc: Boolean? = null,
        ) = listings.listPartitions(cat, "ns", "t", sort, desc)
            .partitions.map { it.values.single().decoded }

        assertThat(keys(PartitionListingService.SortColumn.PARTITION)).containsExactly("a", "b", "c")
        assertThat(keys(PartitionListingService.SortColumn.PARTITION, desc = true))
            .containsExactly("c", "b", "a")
        // A measure with no explicit order is DESCENDING by default.
        assertThat(keys(PartitionListingService.SortColumn.FILES)).containsExactly("b", "c", "a")
        assertThat(keys(PartitionListingService.SortColumn.FILES, desc = false))
            .containsExactly("a", "c", "b")
        assertThat(keys(PartitionListingService.SortColumn.SMALL_FILES)).containsExactly("b", "c", "a")
        assertThat(keys(PartitionListingService.SortColumn.SMALL_FILES, desc = false))
            .containsExactly("a", "c", "b")
        assertThat(keys(PartitionListingService.SortColumn.DEBT)).containsExactly("b", "c", "a")
        assertThat(keys(PartitionListingService.SortColumn.DEBT, desc = false))
            .containsExactly("a", "c", "b")
        assertThat(keys(PartitionListingService.SortColumn.TOTAL_SIZE)).containsExactly("a", "b", "c")
        assertThat(keys(PartitionListingService.SortColumn.TOTAL_SIZE, desc = false))
            .containsExactly("c", "b", "a")
        assertThat(keys(PartitionListingService.SortColumn.AVG_SIZE)).containsExactly("a", "b", "c")
        assertThat(keys(PartitionListingService.SortColumn.AVG_SIZE, desc = false))
            .containsExactly("b", "c", "a")
        // No deletion vectors anywhere: every group ties at 0, so the
        // decoded tuple is the whole order — which is what makes paging
        // stable rather than arbitrary.
        assertThat(keys(PartitionListingService.SortColumn.DVS)).containsExactly("a", "b", "c")
        assertThat(keys(PartitionListingService.SortColumn.ROWS)).containsExactly("b", "c", "a")
        assertThat(keys(PartitionListingService.SortColumn.ROWS, desc = false))
            .containsExactly("a", "c", "b")
        // All three were written in one commit, so last_written ties and
        // falls through to the tuple in both directions.
        assertThat(keys(PartitionListingService.SortColumn.LAST_WRITTEN)).containsExactly("a", "b", "c")
    }

    @Test
    fun `an unmeasured generation sorts by the tuple and reports every row null`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(
            cat,
            "t",
            file("a1", 100, records = 5, values = listOf("a")),
            file("b1", 100, records = 50, values = listOf("b")),
        )
        sample()
        // The gate is the GENERATION, so `rows` is either measured for
        // every group or for none — there is no mixed page to sort
        // nulls to the back of. (The comparator still puts nulls last;
        // that arm is defensive, kept for a measure that could become
        // per-row nullable, and unreachable through this path today.)
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_maintenance_summary s SET measures_generation = -1
                FROM hog_catalog c WHERE c.catalog_id = s.catalog_id AND c.name = :cat
                """,
            ).bind("cat", cat).execute()
        }
        for (desc in listOf(true, false)) {
            val got =
                listings.listPartitions(
                    cat,
                    "ns",
                    "t",
                    PartitionListingService.SortColumn.ROWS,
                    desc = desc,
                )
            assertThat(got.partitions.map { it.recordCount }).containsOnlyNulls()
            assertThat(got.partitions.map { it.lastWrittenSnapshot }).containsOnlyNulls()
            // Every key ties at null, so the decoded tuple is the whole
            // order — which is what keeps paging stable rather than
            // arbitrary.
            assertThat(got.partitions.map { it.values.single().decoded })
                .containsExactly("a", "b")
        }
    }

    @Test
    fun `a measured generation populates both new measures`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(
            cat,
            "t",
            file("a1", 100, records = 5, values = listOf("a")),
            file("b1", 100, records = 50, values = listOf("b")),
        )
        sample()
        val got =
            listings.listPartitions(
                cat,
                "ns",
                "t",
                PartitionListingService.SortColumn.ROWS,
                desc = true,
            )
        assertThat(got.partitions.map { it.values.single().decoded }).containsExactly("b", "a")
        assertThat(got.partitions.map { it.recordCount }).containsExactly(50, 5)
        assertThat(got.partitions.map { it.lastWrittenSnapshot }).doesNotContainNull()
    }

    // ---- filtering -----------------------------------------------------------

    @Test
    fun `a decoded prefix filter matches the value the console shows`() {
        val cat = fixture()
        partitionByDay(cat)
        append(
            cat,
            "t",
            // 20713 = 2026-09-17, 20714 = 2026-09-18, 20744 = 2026-10-18.
            file("d1", 100, values = listOf("20713")),
            file("d2", 100, values = listOf("20714")),
            file("d3", 100, values = listOf("20744")),
            file("dn", 100, values = listOf(null)),
        )
        sample()

        fun filtered(text: String) =
            listings.listPartitions(
                cat,
                "ns",
                "t",
                filters = listOf(PartitionListingService.Filter(0, text)),
            )

        // A MONTH prefix over a DAY transform: the whole point of
        // decoding server-side. "2026-09" matches no stored value.
        val september = filtered("2026-09")
        assertThat(september.total).isEqualTo(2)
        assertThat(september.partitions.map { it.values.single().decoded })
            .containsExactly("2026-09-17", "2026-09-18")
        assertThat(september.partitions.map { it.values.single().raw })
            .containsExactly("20713", "20714")

        assertThat(filtered("2026-10").total).isEqualTo(1)
        assertThat(filtered("2026").total).isEqualTo(3)
        // Case-insensitive, and no match is an empty page rather than a 404.
        assertThat(filtered("2027").total).isEqualTo(0)
        assertThat(filtered("2027").partitions).isEmpty()

        // The EMPTY filter addresses the null value, which no prefix could.
        val nulls = filtered("")
        assertThat(nulls.total).isEqualTo(1)
        assertThat(nulls.partitions.single().values.single().raw).isNull()

        // The stale-spec count is over the FILTERED set, so it agrees
        // with `total` rather than with a set the caller cannot see.
        assertThat(september.staleSpecGroups).isEqualTo(0)
    }

    @Test
    fun `a case-insensitive prefix matches an identity value`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(
            cat,
            "t",
            file("a", 100, values = listOf("Acme")),
            file("b", 100, values = listOf("acorn")),
            file("c", 100, values = listOf("beta")),
        )
        sample()
        val got =
            listings.listPartitions(
                cat,
                "ns",
                "t",
                filters = listOf(PartitionListingService.Filter(0, "AC")),
            )
        assertThat(got.partitions.map { it.values.single().raw }).containsExactly("Acme", "acorn")
    }

    @Test
    fun `a filter on a key the table does not partition by is a validation error`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(cat, "t", file("a", 100, values = listOf("a")))
        sample()
        assertThatThrownBy {
            listings.listPartitions(
                cat,
                "ns",
                "t",
                filters = listOf(PartitionListingService.Filter(1, "x")),
            )
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("partition key_index 1 is out of range")
    }

    // ---- paging --------------------------------------------------------------

    @Test
    fun `paging walks the ordered set and total counts the whole match`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(
            cat,
            "t",
            *(1..5).map { file("p$it", 100, values = listOf("p%02d".format(it))) }.toTypedArray(),
        )
        sample()

        val first = listings.listPartitions(cat, "ns", "t", limit = 2)
        assertThat(first.total).isEqualTo(5)
        assertThat(first.partitions.map { it.values.single().raw }).containsExactly("p01", "p02")
        val second = listings.listPartitions(cat, "ns", "t", limit = 2, offset = 2)
        assertThat(second.total).isEqualTo(5)
        assertThat(second.partitions.map { it.values.single().raw }).containsExactly("p03", "p04")
        val last = listings.listPartitions(cat, "ns", "t", limit = 2, offset = 4)
        assertThat(last.partitions.map { it.values.single().raw }).containsExactly("p05")
        // Past the end is an empty page, not an error — `total` still says 5.
        val past = listings.listPartitions(cat, "ns", "t", limit = 2, offset = 10)
        assertThat(past.partitions).isEmpty()
        assertThat(past.total).isEqualTo(5)
    }

    // ---- the warm-up and not-found states ------------------------------------

    @Test
    fun `a table whose catalog has no published sample is 200 with a null sampled_at`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(cat, "t", file("a", 100, values = listOf("a")))
        // No sampler run: nothing published.
        val got = listings.listPartitions(cat, "ns", "t")
        assertThat(got.sampledAt).isNull()
        assertThat(got.sampledSnapshotId).isNull()
        assertThat(got.total).isEqualTo(0)
        assertThat(got.partitions).isEmpty()
        // The SPEC is still reported — it comes from the catalog, not
        // from the sample, and the console needs it for its filters.
        assertThat(got.spec!!.fields.single().field).isEqualTo("team")
    }

    @Test
    fun `a sample computed under a different policy is treated as absent`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(cat, "t", file("a", 100, values = listOf("a")))
        sample()
        assertThat(listings.listPartitions(cat, "ns", "t").partitions).isNotEmpty()
        // A listing wired to a different compaction target cannot use
        // this sample's debt_score, so it reports the warm-up state.
        val other =
            PartitionListingService(
                db.jdbi,
                smallFileThresholdBytes = TARGET * 2,
                minInputFiles = 2,
                maxInputFiles = CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
            )
        val got = other.listPartitions(cat, "ns", "t")
        assertThat(got.sampledAt).isNull()
        assertThat(got.partitions).isEmpty()
    }

    @Test
    fun `unknown catalog, namespace and table are all 404, and a dropped table is too`() {
        val cat = fixture()
        assertThatThrownBy { listings.listPartitions("no-such-catalog", "ns", "t") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { listings.listPartitions(cat, "nope", "t") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { listings.listPartitions(cat, "ns", "nope") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        catalogs.dropTable(cat, "ns", "t", null)
        assertThatThrownBy { listings.listPartitions(cat, "ns", "t") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `an unpartitioned table is one group with no values and no spec`() {
        val cat = fixture()
        append(cat, "t", file("u1", 100), file("u2", 100))
        val got = list(cat)
        assertThat(got.spec).isNull()
        assertThat(got.total).isEqualTo(1)
        assertThat(got.partitions.single().values).isEmpty()
        assertThat(got.partitions.single().specId).isNull()
        // With no spec there is nothing to be stale against.
        assertThat(got.staleSpecGroups).isEqualTo(0)
    }

    // ---- the published generation --------------------------------------------

    @Test
    fun `only the PUBLISHED generation is read`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(cat, "t", file("a1", 100, records = 4, values = listOf("a")))
        sample()
        val first = listings.listPartitions(cat, "ns", "t")
        assertThat(first.partitions.single().fileCount).isEqualTo(1)

        // A second generation, with a second file in the same bucket.
        append(cat, "t", file("a2", 100, records = 4, values = listOf("a")))
        sample()
        val second = listings.listPartitions(cat, "ns", "t")

        // Two generations now exist for this catalog; the sampler keeps
        // the published one plus whatever it has not garbage-collected.
        val generations =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT count(DISTINCT p.generation) FROM hog_maintenance_summary_tier p
                    JOIN hog_catalog c ON c.catalog_id = p.catalog_id WHERE c.name = :cat
                    """,
                ).bind("cat", cat).mapTo(Int::class.java).one()
            }
        assertThat(generations)
            .describedAs("the fixture must actually hold two generations, or this pins nothing")
            .isGreaterThanOrEqualTo(2)
        // One group, two files — NOT two groups and not four files.
        // Dropping the published_generation join double-counts here.
        assertThat(second.total).isEqualTo(1)
        assertThat(second.partitions.single().fileCount).isEqualTo(2)
        assertThat(second.partitions.single().recordCount).isEqualTo(8)
    }

    @Test
    fun `the measures survive the NEXT generation being in flight`() {
        // THE STEADY STATE, and the one every other fixture misses.
        // `sample()` drains to a publish and stops, so the last
        // generation begun is also the one published — a state
        // production is in for about one minute in thirty. The rest of
        // the time a new scan is in flight, and a marker that named the
        // generation being SCANNED would blank both measures for all of
        // it.
        val cat = fixture()
        partitionByTeam(cat)
        append(cat, "t", file("a1", 100, records = 11, values = listOf("a")))
        sample()
        assertThat(listings.listPartitions(cat, "ns", "t").partitions.single().recordCount)
            .isEqualTo(11)

        // Make the catalog due and take exactly ONE tick, so the next
        // generation has begun and cannot have finished.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_maintenance_summary s SET next_batch_at = now()
                FROM hog_catalog c WHERE c.catalog_id = s.catalog_id AND c.name = :cat
                """,
            ).bind("cat", cat).execute()
        }
        MaintenanceSummarySampler(db.jdbi, TARGET, 2, CompactionGrouping.DEFAULT_MAX_INPUT_FILES, 3600)
            .runOnce(batchSize = 1)
        val inFlight =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT s.generation > s.published_generation
                    FROM hog_maintenance_summary s
                    JOIN hog_catalog c ON c.catalog_id = s.catalog_id WHERE c.name = :cat
                    """,
                ).bind("cat", cat).mapTo(Boolean::class.javaObjectType).one()
            }
        assertThat(inFlight)
            .describedAs("a later generation must actually be in flight, or this pins nothing")
            .isTrue()

        val got = listings.listPartitions(cat, "ns", "t").partitions.single()
        assertThat(got.recordCount)
            .describedAs("the PUBLISHED generation measured this; a scan in flight is not news")
            .isEqualTo(11)
        assertThat(got.lastWrittenSnapshot).isNotNull()
    }

    @Test
    fun `a generation no V22 sampler began reports both new measures as null`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(cat, "t", file("a1", 100, records = 9, values = listOf("a")))
        sample()
        assertThat(listings.listPartitions(cat, "ns", "t").partitions.single().recordCount)
            .isEqualTo(9)

        // THE EXPLICIT "NEVER V22" CASE, set by hand on purpose: with
        // the marker stamped at publish, the real mechanism now
        // produces the in-flight state (pinned in the test above) but
        // not this one, which needs a pre-V22 build to have published. Its buckets may carry a
        // partial sum beside a non-null newest, so NEITHER measure can
        // be reported — the gate is the generation, not the row.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_maintenance_summary s SET measures_generation = -1
                FROM hog_catalog c WHERE c.catalog_id = s.catalog_id AND c.name = :cat
                """,
            ).bind("cat", cat).execute()
        }
        val got = listings.listPartitions(cat, "ns", "t").partitions.single()
        assertThat(got.recordCount).isNull()
        assertThat(got.lastWrittenSnapshot).isNull()
        // Everything the sampler always measured is untouched.
        assertThat(got.fileCount).isEqualTo(1)
        assertThat(got.totalBytes).isEqualTo(100)
    }

    // ---- decoded, not raw ----------------------------------------------------

    @Test
    fun `sort=partition orders by the DECODED value, not the stored one`() {
        val cat = fixture()
        partitionByDay(cat)
        // Raw "9" < "10" < "100" chronologically, but LEXICALLY
        // "10" < "100" < "9". Sorting the stored strings would put
        // 1970-01-10 and 1970-04-11 ahead of 1970-01-10's predecessor.
        append(
            cat,
            "t",
            file("d9", 100, values = listOf("9")),
            file("d10", 100, values = listOf("10")),
            file("d100", 100, values = listOf("100")),
        )
        sample()
        val asc =
            listings.listPartitions(cat, "ns", "t", PartitionListingService.SortColumn.PARTITION)
        assertThat(asc.partitions.map { it.values.single().decoded })
            .containsExactly("1970-01-10", "1970-01-11", "1970-04-11")
        assertThat(asc.partitions.map { it.values.single().raw })
            .describedAs("raw order would be 10, 100, 9")
            .containsExactly("9", "10", "100")
        val desc =
            listings.listPartitions(
                cat,
                "ns",
                "t",
                PartitionListingService.SortColumn.PARTITION,
                desc = true,
            )
        assertThat(desc.partitions.map { it.values.single().raw }).containsExactly("100", "10", "9")
    }

    @Test
    fun `the filter is a PREFIX, not a contains`() {
        val cat = fixture()
        partitionByDay(cat)
        append(
            cat,
            "t",
            // 20713 = 2026-09-17, 20744 = 2026-10-18.
            file("d1", 100, values = listOf("20713")),
            file("d2", 100, values = listOf("20744")),
        )
        sample()

        fun total(text: String) =
            listings.listPartitions(
                cat,
                "ns",
                "t",
                filters = listOf(PartitionListingService.Filter(0, text)),
            ).total

        // "09-17" appears INSIDE "2026-09-17" and at the start of
        // nothing: a `contains` would return 1 here.
        assertThat(total("09-17")).isEqualTo(0)
        assertThat(total("10-18")).isEqualTo(0)
        // ...and the prefix of the same value still matches.
        assertThat(total("2026-09-17")).isEqualTo(1)
        assertThat(total("2026-")).isEqualTo(2)
    }

    @Test
    fun `an empty filter matches a null VALUE, never a missing key`() {
        val cat = fixture()
        // The pre-spec vintage has no keys at all; the spec'd groups
        // have one, one of them null.
        append(cat, "t", file("u1", 100))
        partitionByTeam(cat)
        append(
            cat,
            "t",
            file("p1", 100, values = listOf("p1")),
            file("na", 100, values = listOf(null)),
        )
        sample()
        val got =
            listings.listPartitions(
                cat,
                "ns",
                "t",
                filters = listOf(PartitionListingService.Filter(0, "")),
            )
        // ONE group: the null value. The unpartitioned vintage has no
        // key 0 to be null, and "absent" is not "null".
        assertThat(got.total).isEqualTo(1)
        assertThat(got.partitions.single().values).hasSize(1)
        assertThat(got.partitions.single().values.single().raw).isNull()
        assertThat(got.partitions.single().specId).isNotNull()
    }

    // ---- the two sort columns the first pass left tied ------------------------

    @Test
    fun `dvs and last_written sort by their own measure, descending by default`() {
        val cat = fixture()
        partitionByTeam(cat)
        // b gets a deletion vector and the later write; a gets neither.
        append(cat, "t", file("a1", 100, values = listOf("a")))
        val bSnapshot = append(cat, "t", file("b1", 100, values = listOf("b"))).snapshotId
        deleteVectorOn(cat, "s3://bucket/x/b1.parquet")
        sample()

        fun keys(
            sort: PartitionListingService.SortColumn,
            desc: Boolean? = null,
        ) = listings.listPartitions(cat, "ns", "t", sort, desc)
            .partitions.map { it.values.single().decoded }

        assertThat(listings.listPartitions(cat, "ns", "t").partitions.map { it.dvCount })
            .describedAs("the fixture must actually differ in dv_count, or this pins nothing")
            .containsExactly(0, 1)
        assertThat(keys(PartitionListingService.SortColumn.DVS)).containsExactly("b", "a")
        assertThat(keys(PartitionListingService.SortColumn.DVS, desc = false))
            .containsExactly("a", "b")

        val listed = listings.listPartitions(cat, "ns", "t").partitions
        assertThat(listed.map { it.lastWrittenSnapshot })
            .describedAs("the two groups must differ in last_written, or this pins nothing")
            .doesNotHaveDuplicates()
        assertThat(listed.last().lastWrittenSnapshot).isEqualTo(bSnapshot)
        assertThat(keys(PartitionListingService.SortColumn.LAST_WRITTEN)).containsExactly("b", "a")
        assertThat(keys(PartitionListingService.SortColumn.LAST_WRITTEN, desc = false))
            .containsExactly("a", "b")
    }

    @Test
    fun `stale_spec_groups counts every vintage that is not head's`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(cat, "t", file("v1", 100, values = listOf("x")))
        // A SECOND numbered spec, so the stale one is spec 1 rather
        // than the null-spec vintage the first test uses.
        partitionByDay(cat)
        append(cat, "t", file("v2", 100, values = listOf("20713")))
        sample()
        val got = listings.listPartitions(cat, "ns", "t")
        assertThat(got.total).isEqualTo(2)
        // Ordered by the decoded tuple, so "2026-09-17" precedes "x".
        assertThat(got.partitions.map { it.specId }).containsExactly(2L, 1L)
        assertThat(got.staleSpecGroups)
            .describedAs("spec 1 is stale; spec 2 is head's")
            .isEqualTo(1)
        // And the stale vintage is decoded against ITS OWN spec: an
        // identity team value, not a day ordinal.
        assertThat(got.partitions.first { it.specId == 1L }.values.single().decoded).isEqualTo("x")
        assertThat(got.partitions.first { it.specId == 2L }.values.single().decoded)
            .isEqualTo("2026-09-17")
    }

    // ---- parameter validation -------------------------------------------------

    @Test
    fun `limit and offset are validated, and limit is capped rather than honoured`() {
        val cat = fixture()
        partitionByTeam(cat)
        append(
            cat,
            "t",
            *(1..3).map { file("p$it", 100, values = listOf("p$it")) }.toTypedArray(),
        )
        sample()
        assertThatThrownBy { listings.listPartitions(cat, "ns", "t", limit = 0) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("limit must be positive")
        assertThatThrownBy { listings.listPartitions(cat, "ns", "t", offset = -1) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("offset must be >= 0")
        // Over MAX_LIMIT: served, capped, not refused.
        assertThat(
            listings.listPartitions(
                cat,
                "ns",
                "t",
                limit = PartitionListingService.MAX_LIMIT * 5,
            ).partitions,
        ).hasSize(3)
    }

    // ---- the cap -------------------------------------------------------------

    @Test
    fun `a table over the group cap is refused, and a filter does not rescue it`() {
        val cat = fixture()
        partitionByDay(cat)
        append(cat, "t", file("seed", 100, values = listOf("20000")))
        sample()
        // AT THE REAL CONSTANT, not an injected one. The failure an
        // injected cap would miss is the interesting one: the read's
        // LIMIT drifting from the cap it enforces, so the guard fires
        // on a different population than the one it protects. One
        // generate_series of 50,001 rows costs a few hundred ms, which
        // is what the 5,000-group fixture already pays for 25,000.
        // One past the cap, plus the seed group above: 50,002, so the
        // bounded read is strictly short of the population.
        seedGroups(cat, "t", (PartitionListingService.MAX_GROUPS + 1).toInt())

        // THE `LIMIT` ITSELF, not just the refusal. The cap is
        // structural — at most MAX_GROUPS + 1 rows may reach the JVM —
        // and the refusal alone does not pin that: with the LIMIT
        // deleted the service would read all 50,002 groups, count them
        // and throw the same message, which is the unbounded
        // materialisation the cap exists to prevent. So the statement
        // is executed directly and its row count asserted.
        val read =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(PartitionListingService.GROUP_ROWS_SQL)
                    .bind("catalogId", catalogId(cat))
                    .bind("tableId", tableId(cat, "t"))
                    .mapTo(Long::class.java)
                    .list()
                    .size
            }
        assertThat(read)
            .describedAs("the read must be bounded at the cap plus one, whatever the table holds")
            .isEqualTo((PartitionListingService.MAX_GROUPS + 1).toInt())

        assertThatThrownBy { listings.listPartitions(cat, "ns", "t") }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("more than ${PartitionListingService.MAX_GROUPS}")
            .hasMessageContaining("stats/partitions")
            // The advice must NOT be to filter: filters are applied
            // after the read and cannot lower the row count.
            .hasMessageContaining("'filter' does not help")

        // ...and following that advice anyway changes nothing.
        assertThatThrownBy {
            listings.listPartitions(
                cat,
                "ns",
                "t",
                filters = listOf(PartitionListingService.Filter(0, "2024")),
            )
        }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("more than ${PartitionListingService.MAX_GROUPS}")
    }

    // ---- the bound the design rests on ---------------------------------------

    @Test
    fun `a 5,000-group table lists inside the stated budget`() {
        val cat = fixture()
        partitionByDay(cat)
        append(cat, "t", file("seed", 100, values = listOf("20000")))
        sample()
        // Four more tables' worth of groups in the same generation, so
        // the measurement includes the work of NOT reading them — the
        // whole reason V22 adds hog_maintenance_summary_tier_table.
        for (i in 2..5) addTable(cat, "t$i")
        seedGroups(cat, "t", GROUPS)
        for (i in 2..5) seedGroups(cat, "t$i", GROUPS)
        db.jdbi.useHandleUnchecked { it.execute("ANALYZE hog_maintenance_summary_tier") }

        // Warm the JIT and the buffers; the budget is for a served
        // request, not for the first one after a cold start.
        repeat(3) { listings.listPartitions(cat, "ns", "t", limit = 100) }
        val started = System.nanoTime()
        val got = listings.listPartitions(cat, "ns", "t", limit = 100)
        val elapsedMs = (System.nanoTime() - started) / 1_000_000.0
        assertThat(got.total).isEqualTo(GROUPS + 1)
        assertThat(got.partitions).hasSize(100)
        assertThat(elapsedMs)
            .describedAs(
                "listing %d groups took %.1f ms; the design's bound is a few thousand " +
                    "groups per table (partitions x spec versions) materialised and sorted " +
                    "in memory, and 200 ms is where that stops being true",
                GROUPS,
                elapsedMs,
            )
            .isLessThan(BUDGET_MS)
    }

    // ---- seeding the shapes the sampler cannot produce ------------------------

    /**
     * One tier row in the catalog's PUBLISHED generation.
     *
     * [preV22] writes the columns a pre-V22 sampler left at their
     * defaults — `record_count` 0 and a NULL `newest_begin_snapshot` —
     * which is the state the listing must report as "not sampled"
     * rather than as zero rows.
     */
    private fun seedTierRow(
        cat: String,
        specId: Long?,
        values: List<String?>,
        fileCount: Long,
        totalBytes: Long,
        preV22: Boolean = false,
        table: String = "t",
    ) = db.jdbi.useHandleUnchecked { h ->
        h.createUpdate(
            """
            INSERT INTO hog_maintenance_summary_tier
                (catalog_id, generation, bucket_key, table_id, spec_id, partition_values,
                 quota, remaining, pending, selected, pending_max_bytes,
                 file_count, small_count, total_bytes, small_bytes, dv_count,
                 record_count, newest_begin_snapshot)
            SELECT c.catalog_id, s.published_generation, :key, t.table_id, :spec, :vals,
                   :target, :target, 0, 0, 0,
                   :files, :files, :bytes, :bytes, 0,
                   :rows, :newest
            FROM hog_catalog c
            JOIN hog_maintenance_summary s ON s.catalog_id = c.catalog_id
            JOIN hog_namespace ns ON ns.catalog_id = c.catalog_id AND ns.name = 'ns'
            JOIN hog_table_version tv ON tv.catalog_id = c.catalog_id
              AND tv.namespace_id = ns.namespace_id AND tv.name = :table AND tv.end_snapshot IS NULL
            JOIN hog_table t ON t.catalog_id = c.catalog_id AND t.table_id = tv.table_id
            WHERE c.name = :cat
            """,
        )
            .bind("cat", cat)
            .bind("table", table)
            .bind("key", "seeded-${counter.incrementAndGet()}")
            .bindBySqlType("spec", specId, java.sql.Types.BIGINT)
            .bindArray("vals", String::class.java, *values.toTypedArray())
            .bind("target", TARGET)
            .bind("files", fileCount)
            .bind("bytes", totalBytes)
            .bind("rows", if (preV22) 0L else fileCount * 10)
            .bindBySqlType("newest", if (preV22) null else 1L, java.sql.Types.BIGINT)
            .execute()
    }

    private fun catalogId(cat: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = :cat")
                .bind("cat", cat).mapTo(Long::class.java).one()
        }

    private fun tableId(
        cat: String,
        table: String,
    ): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT tv.table_id FROM hog_table_version tv
                JOIN hog_catalog c ON c.catalog_id = tv.catalog_id
                JOIN hog_namespace ns ON ns.catalog_id = tv.catalog_id
                  AND ns.namespace_id = tv.namespace_id AND ns.name = 'ns'
                WHERE c.name = :cat AND tv.name = :table AND tv.end_snapshot IS NULL
                """,
            ).bind("cat", cat).bind("table", table).mapTo(Long::class.java).one()
        }

    /** [n] day-partitioned groups for one table, in one statement. */
    private fun seedGroups(
        cat: String,
        table: String,
        n: Int,
    ) = db.jdbi.useHandleUnchecked { h ->
        h.createUpdate(
            """
            INSERT INTO hog_maintenance_summary_tier
                (catalog_id, generation, bucket_key, table_id, spec_id, partition_values,
                 quota, remaining, pending, selected, pending_max_bytes,
                 file_count, small_count, total_bytes, small_bytes, dv_count,
                 record_count, newest_begin_snapshot)
            SELECT c.catalog_id, s.published_generation,
                   :table || '-' || g::text, t.table_id, 1, ARRAY[(20000 + g)::text],
                   :target, :target, 0, g % 7, 0,
                   3 + g % 5, 2, 1000 + g, 900, g % 3,
                   100 + g, 1 + g
            FROM generate_series(1, :n) g
            CROSS JOIN hog_catalog c
            JOIN hog_maintenance_summary s ON s.catalog_id = c.catalog_id
            JOIN hog_namespace ns ON ns.catalog_id = c.catalog_id AND ns.name = 'ns'
            JOIN hog_table_version tv ON tv.catalog_id = c.catalog_id
              AND tv.namespace_id = ns.namespace_id AND tv.name = :table AND tv.end_snapshot IS NULL
            JOIN hog_table t ON t.catalog_id = c.catalog_id AND t.table_id = tv.table_id
            WHERE c.name = :cat
            """,
        )
            .bind("cat", cat)
            .bind("table", table)
            .bind("target", TARGET)
            .bind("n", n)
            .execute()
    }

    private companion object {
        const val TARGET = 1000L

        /**
         * The listing's stated bound, exercised: partitions x spec
         * versions for one table. A decade of daily partitions under
         * three spec revisions is about 11,000; 5,000 is the shape a
         * real day-partitioned table reaches, and the one the KDoc
         * prices.
         */
        const val GROUPS = 5_000

        /**
         * The listing's latency budget. MEASURED at 12.5 ms for this
         * fixture (5,000 groups for the table, 25,000 in the
         * generation, PG 18 in Testcontainers, warm, serial), so the
         * assertion has 16x of headroom — it is a regression gate on
         * the access path, not a tight timing check that flakes on a
         * loaded CI box. A plan that lost the V22 index would read five
         * times the rows and a plan that lost the table filter would
         * read them all.
         */
        const val BUDGET_MS = 200.0
    }
}
