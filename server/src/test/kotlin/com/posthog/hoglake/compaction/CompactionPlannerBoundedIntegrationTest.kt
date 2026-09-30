package com.posthog.hoglake.compaction

import com.posthog.hoglake.Database
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.MaintenanceSummarySampler
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
 * The compaction planner's TRANSACTION SHAPE and its BOUNDED candidate
 * read — the two halves of the 2026-09-30 gigahog-prod-us failure.
 *
 * What happened: `planSnapshot` wrapped the candidate read, the in-JVM
 * bin packing and the claim read in ONE transaction.
 * `ingest.events_raw` had ~9.9M candidate files over ~2,800 buckets, the
 * packing took minutes, and a connection sitting idle inside an open
 * transaction is killed by `idle_in_transaction_session_timeout` (30 s,
 * `Database.SESSION_INIT_SQL`) — so every sweep died on the statement
 * AFTER the packing with `FATAL: terminating connection due to
 * idle-in-transaction timeout`. The failure rate went from 1 run in 16
 * to 100% as the candidate set grew.
 *
 * So there are two properties here and they are independent:
 *
 *  - **nothing is held across the packing**, which the injected pause
 *    below tests directly against a session carrying the same bound
 *    production runs (`PgTestSupport.IDLE_IN_TRANSACTION_GUARD`, 2 s so
 *    it fires inside a test's patience);
 *  - **the read is bounded**, by buckets from the published sample and
 *    by a hard cap either way.
 *
 * Planning is metadata-only, so the store points at a dead port: any
 * object-store contact would fail loudly rather than pass quietly.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionPlannerBoundedIntegrationTest {
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

    /**
     * Small numbers, so the bounds are reachable with a few dozen files
     * rather than a few hundred thousand.
     *
     * `candidateBudget = min(headroom x maxGroupsPerRun x maxFanIn,
     * maxCandidates) = min(1 x 2 x 4, 100) = 8`, and `maxCandidates` is
     * held ABOVE that so the two bounds are distinguishable: the
     * bucket-scoped tests reach the first and the fallback tests reach
     * the second.
     */
    private val cfg =
        CompactionConfig(
            targetBytes = 1000,
            minInputFiles = 2,
            maxInputFiles = 4,
            maxFanIn = 4,
            maxGroupsPerRun = 2,
            candidateHeadroom = 1,
            maxCandidates = 100,
        )

    /** The same, with a fallback cap low enough to be reached. */
    private val cappedCfg = cfg.copy(maxCandidates = 6)

    @AfterAll
    fun tearDown() {
        deadStore.close()
        db.close()
    }

    private fun fixture(partitioned: Boolean): String {
        val cat = "bounded-${counter.incrementAndGet()}"
        db.jdbi.useHandleUnchecked { h ->
            // Raw, like the other planner fixtures: the shared
            // "s3://bucket" data_path would trip the creation-time
            // shape/overlap rules.
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
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(ColumnDef("id", ColType.LONG), ColumnDef("day", ColType.STRING)),
        )
        if (partitioned) {
            alter.alterTable(
                cat,
                "ns",
                "t",
                listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY)))),
            )
        }
        return cat
    }

    private fun append(
        cat: String,
        files: List<FileRegistration>,
    ) = commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", files))))

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

    /** Publish one generation over [cat], so the planner has buckets. */
    private fun publishSample(cat: String) {
        val sampler =
            MaintenanceSummarySampler(db.jdbi, cfg.targetBytes, cfg.minInputFiles, cfg.maxInputFiles, 3600)
        var steps = 0
        while (sampler.runOnce(10_000)) check(++steps < 1_000)
        check(
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT published_generation FROM hog_maintenance_summary s " +
                        "JOIN hog_catalog c ON c.catalog_id = s.catalog_id WHERE c.name = :n",
                ).bind("n", cat).mapTo(Long::class.java).one()
            } > 0,
        ) { "the sampler published nothing for $cat" }
    }

    private fun service() = CompactionService(db.jdbi, deadStore, cfg)

    /** The live column forest, for `sortedRowCeiling` arithmetic. */
    private fun columnsOf(cat: String): List<com.posthog.hoglake.model.Column> =
        db.jdbi.withHandleUnchecked { h ->
            val ids =
                h.createQuery(
                    "SELECT c.catalog_id, c.table_id, k.last_snapshot_id FROM hog_column c " +
                        "JOIN hog_catalog k ON k.catalog_id = c.catalog_id WHERE k.name = :n LIMIT 1",
                ).bind("n", cat)
                    .map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }
                    .one()
            com.posthog.hoglake.persistence.TableRepo.columnsAt(h, ids.first, ids.second, ids.third)
        }

    // ---- (1) nothing is held across the packing --------------------------------

    @Test
    fun `a plan whose packing takes longer than the session's idle bound still succeeds`() {
        // THE REGRESSION TEST FOR THE OUTAGE, and it is a test about a
        // TRANSACTION BOUNDARY rather than about a result.
        //
        // The pause goes where the bin packing runs. Under the old shape
        // that was inside the planning transaction, so the connection
        // was idle-in-transaction for the whole pause and Postgres killed
        // it — the next statement (the claim read) then failed with
        // `FATAL: terminating connection due to idle-in-transaction
        // timeout` and the whole catalog sweep aborted. Under this shape
        // the candidate read's transaction is already committed when the
        // pause starts and the claim read opens its own, so there is no
        // open transaction to be idle inside.
        //
        // 3 s against the fixture's 2 s bound. The production numbers are
        // minutes against 30 s; what matters is the RATIO, and the
        // property being pinned is "holds nothing", which is true at
        // every duration or at none.
        val cat = fixture(partitioned = true)
        append(cat, (0..5).map { file("p$it", 100, values = listOf("a")) })
        publishSample(cat)

        val svc = service()
        svc.beforePacking = { Thread.sleep(3_000) }
        try {
            val plan = svc.planTable(cat, "ns", "t", cfg)
            assertThat(plan.groups)
                .describedAs("the plan must survive a slow packing phase, not merely not crash")
                .isNotEmpty()
            assertThat(plan.planMs)
                .describedAs("and the pause really was inside the measured plan")
                .isGreaterThanOrEqualTo(3_000)
        } finally {
            svc.beforePacking = {}
        }
    }

    @Test
    fun `the session bound this suite runs under is real`() {
        // The control for the test above. Without this, a planner that
        // held a transaction across the packing would still pass if the
        // fixture's sessions had no idle bound at all — which is exactly
        // the state the whole suite was in before
        // `PgTestSupport.IDLE_IN_TRANSACTION_GUARD`, and the reason no
        // test could fail on the outage.
        val bound =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SHOW idle_in_transaction_session_timeout").mapTo(String::class.java).one()
            }
        assertThat(bound).describedAs("every connection in this fixture carries the guard").isEqualTo("2s")
        // And it FIRES: a transaction held idle past it is killed.
        // Asserted rather than assumed, because the setting being
        // present and the setting being enforced are different claims
        // and only one of them is the guard.
        val killed =
            runCatching {
                db.jdbi.useHandleUnchecked { h ->
                    h.begin()
                    h.createQuery("SELECT 1").mapTo(Int::class.java).one()
                    Thread.sleep(3_000)
                    h.createQuery("SELECT 1").mapTo(Int::class.java).one()
                }
            }.exceptionOrNull()
        assertThat(killed)
            .describedAs("a transaction held idle for 3 s under a 2 s bound must be killed")
            .isNotNull()
    }

    // ---- (2) the read is bounded ----------------------------------------------

    @Test
    fun `the bucket-scoped fetch stops at the candidate budget and says how much it saw`() {
        // Four buckets of four files each — sixteen candidates against a
        // candidate budget of headroom(1) x groups(2) x files(4) = 8.
        val cat = fixture(partitioned = true)
        for (day in listOf("a", "b", "c", "d")) {
            append(cat, (0..3).map { file("$day$it", 100, values = listOf(day)) })
        }
        publishSample(cat)

        val plan = service().planTable(cat, "ns", "t", cfg)
        assertThat(plan.candidatesFetched)
            .describedAs("bounded by headroom x maxGroupsPerRun x maxInputFiles")
            .isLessThanOrEqualTo(cfg.candidateBudget.toLong())
            .isEqualTo(8)
        assertThat(plan.bucketsAvailable)
            .describedAs("every one of the four buckets has enough small files to group")
            .isEqualTo(4)
        assertThat(plan.bucketsConsidered)
            .describedAs("two buckets of four files exhaust a budget of eight")
            .isEqualTo(2)
        assertThat(plan.candidatesTruncated)
            .describedAs("the table has debt this plan did not look at, and the ledger says so")
            .isEqualTo(1)
        // And the work is real: every group is inside one bucket.
        assertThat(plan.groups).isNotEmpty()
        for (group in plan.groups) {
            assertThat(group.partitionValues).hasSize(1)
        }
    }

    @Test
    fun `an unpartitioned table falls back to a capped fetch and records the truncation`() {
        // No tuple to probe with, so the planner takes the size-ordered
        // prefix. `maxCandidates` is 6 here against ten live files.
        val cat = fixture(partitioned = false)
        append(cat, (0..9).map { file("u$it", 100) })

        // BEFORE any sample is published: the catalog has no bucket list
        // at all, which is the other route into the fallback and the one
        // a fresh install is in.
        val cold = service().planTable(cat, "ns", "t", cappedCfg)
        assertThat(cold.candidatesFetched)
            .describedAs("capped at HOGLAKE_COMPACTION_MAX_CANDIDATES")
            .isEqualTo(cappedCfg.maxCandidates.toLong())
        assertThat(cold.candidatesTruncated).isEqualTo(1)
        assertThat(cold.bucketsAvailable)
            .describedAs("no published generation, so no buckets were offered")
            .isZero()
        assertThat(cold.groups).describedAs("and it still plans work").isNotEmpty()

        // AFTER a sample exists the table has exactly one bucket — the
        // tuple-less one — and it is fetched through the size-ordered
        // prefix as well, now bounded by the candidate budget.
        publishSample(cat)
        val warm = service().planTable(cat, "ns", "t", cappedCfg)
        assertThat(warm.bucketsAvailable).describedAs("one bucket: the tuple-less one").isEqualTo(1)
        assertThat(warm.bucketsConsidered).isEqualTo(1)
        assertThat(warm.candidatesFetched).isLessThanOrEqualTo(cappedCfg.candidateBudget.toLong())
        assertThat(warm.groups).isNotEmpty()
        for (group in warm.groups) {
            assertThat(group.partitionValues).describedAs("no partition values at all").isNull()
        }
    }

    @Test
    fun `a table partitioned mid-life still compacts the vintage written before the spec`() {
        // The case a bucket-first planner can silently lose: the
        // tuple-less bucket sits BESIDE the partitioned ones, and it has
        // no tuple to probe V23's index with. Skipping it would leave
        // those files uncompactable forever, with nothing saying so.
        val cat = fixture(partitioned = false)
        append(cat, (0..3).map { file("pre$it", 10) })
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY)))),
        )
        append(cat, (0..3).map { file("post$it", 100, values = listOf("a")) })
        publishSample(cat)

        val plan = service().planTable(cat, "ns", "t", cfg)
        assertThat(plan.bucketsAvailable).describedAs("the vintage and the partition").isEqualTo(2)
        val paths = plan.groups.flatMap { it.files }.map { it.path }
        assertThat(paths)
            .describedAs("the pre-spec vintage is planned, not orphaned")
            .contains("s3://bucket/x/pre0.parquet")
        // And the two never share a group: they are different buckets.
        for (group in plan.groups) {
            val kinds = group.files.map { it.path.contains("/pre") }.distinct()
            assertThat(kinds).describedAs("a group never spans buckets").hasSize(1)
        }
    }

    @Test
    fun `ten successive sweeps rotate across the buckets instead of re-reading the first`() {
        // THE ANTI-STARVATION PROPERTY, and the one a strict order
        // cannot have. On gigahog-prod-us the first version's ordering
        // was a fixed point: one bucket won every sweep for a day,
        // `buckets_considered = 1` against `buckets_available ~ 2,800`,
        // and the 2,790 stray buckets holding the 9.9M-file backlog were
        // never looked at. Ten sweeps touched one bucket, ten times.
        //
        // Six buckets of four files each against a candidate budget of
        // eight: two buckets a sweep. Ten sweeps must therefore cover
        // every bucket, several times over, and never repeat the same
        // pair.
        val cat = fixture(partitioned = true)
        val days = (0 until 6).map { "d$it" }
        for (day in days) {
            append(cat, (0..3).map { file("$day-$it", 100, values = listOf(day)) })
        }
        publishSample(cat)

        val svc = service()
        val touched = mutableListOf<Set<String>>()
        repeat(10) {
            val plan = svc.planTable(cat, "ns", "t", cfg)
            touched += plan.groups.mapNotNull { it.partitionValues?.firstOrNull() }.toSet()
        }

        // 1. TINY BUCKETS FROM SWEEP ONE. Every bucket here holds the
        //    same tiny files, so the ordering's first key ties and the
        //    point is simply that work starts immediately.
        assertThat(touched.first()).describedAs("sweep 1 planned real buckets").isNotEmpty()
        // 2. IT ROTATES: the set a sweep touches moves. Without the
        //    cursor every sweep would return the same buckets in the
        //    same order, because nothing about the sample changes
        //    between them (no group is executed here).
        assertThat(touched.distinct())
            .describedAs("ten sweeps must not all plan the same buckets: %s", touched)
            .hasSizeGreaterThan(1)
        // 3. AND IT COVERS. Every bucket the sample offers is reached
        //    within `ceil(available / considered)` sweeps — three here —
        //    which is the property that replaces "the order is fair".
        val coveredInThree = touched.take(3).flatten().toSet()
        assertThat(coveredInThree)
            .describedAs("three sweeps of two buckets must cover all six: %s", touched.take(3))
            .containsExactlyInAnyOrderElementsOf(days)
    }

    @Test
    fun `a published generation with no groupable bucket reads nothing at all`() {
        // THE MIDDLE STATE, which the first version of this change
        // conflated with "no sample at all" and answered by running the
        // whole-table statement — the one statement here that still
        // carries the per-row tuple aggregate — every sweep, forever,
        // to produce zero groups.
        //
        // One file per bucket: real debt on the debt page, and nothing
        // the packer would ever group (a group of one file is a copy).
        val cat = fixture(partitioned = true)
        for (day in 0 until 5) {
            append(cat, listOf(file("lonely$day", 100, values = listOf("d$day"))))
        }
        publishSample(cat)

        val plan = service().planTable(cat, "ns", "t", cfg)
        assertThat(plan.groups).describedAs("nothing is groupable").isEmpty()
        assertThat(plan.bucketsAvailable)
            .describedAs("and the sample offers no qualifying bucket either")
            .isZero()
        assertThat(plan.candidatesFetched)
            .describedAs(
                "so the planner must read NOTHING — not 50,000 rows of whole-table fallback to " +
                    "discover the same answer on every sweep",
            )
            .isZero()
        assertThat(plan.bucketsConsidered).isZero()
        assertThat(plan.candidatesTruncated).describedAs("nothing was truncated; there was nothing").isZero()
    }

    @Test
    fun `a bucket groupable only under the SCALED minimum is still fetched`() {
        // The floor on the sampled buckets is `selected >= 2`, not
        // `selected >= minInputFiles`, and this is why.
        // `CompactionGrouping`'s minimum SCALES: a group needs
        // `max(2, min(minInputFiles, target / its largest file))` files.
        // So three files that each fill a third of the target are a
        // legal group at `need = 2`, while `small_count` and `selected`
        // are both 3 — and a floor of `minInputFiles` = 5 would have
        // excluded the bucket from the fetch forever while the
        // compaction-debt page went on reporting its debt.
        val policy = cfg.copy(minInputFiles = 5, maxInputFiles = 8, maxFanIn = 8)
        val cat = fixture(partitioned = true)
        // 400 bytes each against a 1,000-byte target: need =
        // max(2, min(5, 1000/400 = 2)) = 2.
        append(cat, (0..2).map { file("big$it", 400, values = listOf("d")) })
        publishSample(cat)

        val plan = service().planTable(cat, "ns", "t", policy)
        assertThat(plan.bucketsAvailable)
            .describedAs("the bucket must survive the sampled-bucket floor")
            .isEqualTo(1)
        assertThat(plan.groups)
            .describedAs("and it must produce the group the scaled minimum allows")
            .isNotEmpty()
        assertThat(plan.groups.first().files).hasSizeGreaterThanOrEqualTo(2)
    }

    @Test
    fun `the number of statements a plan issues is bounded, not only the rows`() {
        // The doctrine bounds the WORK per run, not only what it
        // returns. A sample that credits many buckets a large
        // compaction has since emptied would otherwise make one plan
        // issue one round trip per bucket and never reach the row cap.
        //
        // Ten buckets whose files are all end-snapshotted after the
        // generation was published: every probe returns nothing, so the
        // row budget is never spent and only the STATEMENT bound can
        // stop the loop.
        val policy = cfg.copy(minInputFiles = 4)
        val cat = fixture(partitioned = true)
        for (day in 0 until 10) {
            append(cat, (0..3).map { file("g$day-$it", 100, values = listOf("g$day")) })
        }
        publishSample(cat)
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_data_file SET end_snapshot = 99
                 WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :n)
                """,
            ).bind("n", cat).execute()
        }

        val plan = service().planTable(cat, "ns", "t", policy)
        assertThat(plan.candidatesFetched).describedAs("every bucket is empty now").isZero()
        assertThat(plan.bucketsAvailable).describedAs("the stale sample still credits ten").isEqualTo(10)
        // candidateBudget / minInputFiles = 8 / 4 = 2.
        assertThat(plan.bucketsConsidered)
            .describedAs("the statement count is capped at candidateBudget / minInputFiles")
            .isLessThanOrEqualTo((policy.candidateBudget / policy.minInputFiles).toLong())
        assertThat(plan.candidatesTruncated)
            .describedAs("and the ledger says the plan stopped short")
            .isEqualTo(1)
    }

    @Test
    fun `a sorted table never fetches a file whose own rows exceed the ceiling`() {
        // Dropping the density derate made every file under the RAW
        // target a candidate again, so a sorted file whose own surviving
        // rows exceed the row ceiling would be fetched, packed, emitted
        // as a one-file group, refused, counted and named in a WARN —
        // spending a slot of the candidate budget to learn something
        // `record_count` already said. The candidate filter carries
        // `record_count < ceiling` for exactly that, and it is exact
        // rather than an estimate.
        val ceiling = 100L
        val policy =
            cfg.copy(
                minInputFiles = 2,
                sortedHeapBytes = CompactionConfig.SORTED_HEAP_BYTES_PER_NODE * 3 * ceiling,
            )
        val cat = fixture(partitioned = true)
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(
                AlterOp.SetSortOrder(
                    listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST)),
                ),
            ),
        )
        // Two files the ceiling admits, one it cannot.
        append(
            cat,
            listOf(
                file("fits-a", 100, records = 40, values = listOf("d")),
                file("fits-b", 100, records = 40, values = listOf("d")),
                file("huge", 100, records = 5_000, values = listOf("d")),
            ),
        )
        publishSample(cat)

        val plan = service().planTable(cat, "ns", "t", policy)
        assertThat(policy.sortedRowCeiling(columnsOf(cat)))
            .describedAs("the fixture's ceiling")
            .isEqualTo(ceiling)
        assertThat(plan.candidatesFetched)
            .describedAs("the over-ceiling file must not be fetched at all")
            .isEqualTo(2)
        assertThat(plan.heapRefusedGroups)
            .describedAs("so it costs no refusal, no WARN and no budget slot")
            .isZero()
        assertThat(plan.groups.single().files.map { it.recordCount })
            .describedAs("and the two that fit still group")
            .containsExactly(40L, 40L)
    }

    @Test
    fun `a bucket that emptied since the sample yields no group and no error`() {
        // The sample is allowed to be stale, and nothing the planner does
        // depends on it being right: it decides WHERE TO LOOK, and the
        // files come from live rows. Here the whole bucket is compacted
        // away (its files end-snapshotted) after the generation was
        // published, so the probe finds nothing.
        val cat = fixture(partitioned = true)
        append(cat, (0..3).map { file("gone$it", 100, values = listOf("gone")) })
        append(cat, (0..3).map { file("stay$it", 100, values = listOf("stay")) })
        publishSample(cat)

        db.jdbi.useHandleUnchecked { h ->
            // End-snapshot the "gone" bucket's files, exactly as a
            // compaction commit would, without touching the sample.
            h.createUpdate(
                """
                UPDATE hog_data_file SET end_snapshot = 99
                 WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :n)
                   AND path LIKE 's3://bucket/x/gone%'
                """,
            ).bind("n", cat).execute()
        }

        val plan = service().planTable(cat, "ns", "t", cfg)
        assertThat(plan.bucketsAvailable)
            .describedAs("the STALE sample still credits both buckets with debt")
            .isEqualTo(2)
        assertThat(plan.groups.flatMap { it.files }.map { it.path })
            .describedAs("but the fetch is by live rows, so the emptied bucket contributes nothing")
            .noneMatch { it.contains("/gone") }
        assertThat(plan.groups)
            .describedAs("and the bucket that still has files is planned")
            .isNotEmpty()
    }

    @Test
    fun `the candidate read holds no transaction, which is the invariant behind all of it`() {
        // The property stated DIRECTLY rather than through a timeout: at
        // the moment the packing runs, the planner's own backend is not
        // inside a transaction.
        //
        // # Why this is asserted on a RAW JDBC connection
        //
        // The first version of this test called `db.jdbi` from inside
        // the `beforePacking` hook and counted `pg_stat_activity` rows
        // in state `idle in transaction`. That test PASSED under the
        // mutation it exists to catch, and the reason is worth keeping:
        // the nested call on the planner's own thread is served the
        // PLANNER'S OWN CONNECTION, so with the transaction held the
        // probe ran *inside* it — that backend's state is `active`, not
        // `idle in transaction`, and the count was 0 either way.
        //
        // So the probe opens its own connection, outside the pool the
        // planner draws from, and asks about the planner's backend BY
        // PID. A pid is the only handle that cannot be confused with
        // the prober's own.
        val cat = fixture(partitioned = true)
        append(cat, (0..5).map { file("h$it", 100, values = listOf("a")) })
        publishSample(cat)

        val svc = service()
        // The planner's backend pid, captured on its own connection
        // inside phase (a) — which is the only place it is knowable.
        var plannerPid = -1
        var stateDuringPacking: String? = null
        var xactDuringPacking: Boolean? = null
        svc.beforeFetch = { h ->
            plannerPid = h.createQuery("SELECT pg_backend_pid()").mapTo(Int::class.java).one()
        }
        svc.beforePacking = {
            java.sql.DriverManager.getConnection(db.jdbcUrl, PgTestSupport.USER, PgTestSupport.PASSWORD)
                .use { probe ->
                    probe.prepareStatement(
                        "SELECT state, xact_start IS NOT NULL FROM pg_stat_activity WHERE pid = ?",
                    ).use { st ->
                        st.setInt(1, plannerPid)
                        st.executeQuery().use { rs ->
                            check(rs.next()) { "the planner's backend $plannerPid vanished" }
                            stateDuringPacking = rs.getString(1)
                            xactDuringPacking = rs.getBoolean(2)
                        }
                    }
                }
        }
        try {
            svc.planTable(cat, "ns", "t", cfg)
        } finally {
            svc.beforePacking = {}
            svc.beforeFetch = {}
        }

        assertThat(plannerPid).describedAs("the planner's backend was identified").isGreaterThan(0)
        // `idle`, not `idle in transaction`: the candidate read's
        // transaction is committed before the packing starts, so the
        // connection is parked with nothing open. THIS is the whole
        // failure of 2026-09-30 — a connection idle inside a
        // transaction while the JVM packed for minutes — and
        // `idle_in_transaction_session_timeout` is only what punishes
        // it.
        assertThat(stateDuringPacking)
            .describedAs("the planner's backend during packing (pid %d)", plannerPid)
            .isEqualTo("idle")
        assertThat(xactDuringPacking)
            .describedAs("and `xact_start` must be null: no transaction is open at all")
            .isFalse()
    }

    @Test
    fun `the production session bound is the one the guard imitates`() {
        // The guard is 2 s and production is 30 s, and the two must stay
        // the same SETTING or the test above is pinning a knob nothing
        // uses. Parsed out of Database.SESSION_INIT_SQL rather than
        // restated, which is the rule the other two derived bounds
        // follow.
        assertThat(Database.SESSION_INIT_SQL)
            .describedAs("the planner's failure was this setting firing; the guard must be it")
            .contains("idle_in_transaction_session_timeout")
        assertThat(PgTestSupport.IDLE_IN_TRANSACTION_GUARD)
            .contains("idle_in_transaction_session_timeout")
    }
}
