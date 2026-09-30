package com.posthog.hoglake.service

import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * Commit-receipt retention (#240, V24): what the cleanup sweep purges,
 * what it refuses to purge, and what bounds it per run.
 *
 * NO OBJECT STORE IS NEEDED and none is reachable — the removal queue is
 * empty in every case here, so the drain does no S3 work and the store
 * points at the discard port. What is under test is the statement the
 * drain runs AFTER its queue work, and the fence around it.
 *
 * THE ONE THING THESE CASES CANNOT SHOW is the consequence of getting the
 * retention wrong, so it is worth stating where the assertions come from:
 * a receipt purged while a client can still replay its request makes that
 * replay a SECOND publication of the same files — a successful commit, a
 * valid snapshot, duplicated rows, and no counter anywhere disagreeing.
 * That is why the cutoff has a per-catalog floor and why the floor is
 * asserted directly rather than inferred from a row count.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CleanupReceiptPurgeIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi

    /** Never contacted successfully: 127.0.0.1:9 is the discard port. */
    private val deadRemovals =
        RemovalStore(
            endpoint = "http://127.0.0.1:9",
            region = "us-east-1",
            accessKey = "unused",
            secretKey = "unused",
            pathStyle = true,
        )

    @AfterAll
    fun tearDown() {
        deadRemovals.close()
        db.close()
    }

    private companion object {
        const val DAY = 24L * 60 * 60

        /** The knob's default, stated so a case that relies on it says so. */
        const val WEEK = 7L * DAY

        /**
         * Body size for a legacy receipt, in bytes.
         *
         * 150 KiB, against production's measured ~160 KiB per receipt
         * (#240: 58.1 GiB of TOAST over 366,740 rows). What matters is
         * that it is far past the ~2 KB TOAST chunk size, so the row
         * really does carry ~80 chunks and the DELETE really does pay
         * for them.
         */
        const val BODY_BYTES = 150 * 1024
    }

    private fun seedCatalog(
        name: String,
        snapshotRetentionSeconds: Long? = null,
    ): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                INSERT INTO hog_catalog (name, data_path, snapshot_retention_seconds)
                VALUES (:name, 's3://b', :retention)
                RETURNING catalog_id
                """,
            ).bind("name", name).bind("retention", snapshotRetentionSeconds)
                .mapTo(Long::class.java).one()
        }

    /** [count] receipts for [catalogId], all dated [ageSeconds] ago. */
    private fun seedReceipts(
        catalogId: Long,
        count: Int,
        ageSeconds: Long,
    ) = jdbi.useHandleUnchecked { h ->
        h.createUpdate(
            """
            INSERT INTO hog_commit_receipt
                (catalog_id, idempotency_key, fingerprint, snapshot_id, schema_version, created_at)
            SELECT :c, gen_random_uuid(), sha256(g::text::bytea), g, 1,
                   now() - make_interval(secs => :age)
            FROM generate_series(1, :n) g
            """,
        ).bind("c", catalogId).bind("n", count).bind("age", ageSeconds).execute()
    }

    /**
     * [count] PRE-V24 receipts: a `request` body of ~[BODY_BYTES] and no
     * digest, which is the only kind of row the purge will meet for the
     * first retention window after the V24 deploy.
     *
     * The body is what makes these rows expensive, and the cost is not in
     * the heap tuple: at 150 KiB the payload is ~82 rows in the TOAST
     * relation, and `heap_delete` deletes those SYNCHRONOUSLY inside the
     * page's transaction. A fixture without bodies measures the
     * post-backlog regime and says nothing about the drain this change
     * exists to perform.
     */
    private fun seedLegacyReceipts(
        catalogId: Long,
        count: Int,
        ageSeconds: Long,
    ) = jdbi.useHandleUnchecked { h ->
        h.execute("ALTER TABLE hog_commit_receipt ALTER COLUMN request SET STORAGE EXTERNAL")
        h.createUpdate(
            """
            INSERT INTO hog_commit_receipt
                (catalog_id, idempotency_key, request, snapshot_id, schema_version, created_at)
            SELECT :c, gen_random_uuid(),
                   jsonb_build_object('appends', repeat('x', :body)),
                   g, 1, now() - make_interval(secs => :age)
            FROM generate_series(1, :n) g
            """,
        ).bind("c", catalogId).bind("n", count).bind("age", ageSeconds).bind("body", BODY_BYTES).execute()
    }

    /** Bytes in the receipt table's TOAST relation — the thing a legacy page pays for. */
    private fun toastBytes(): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT pg_total_relation_size(reltoastrelid) FROM pg_class " +
                    "WHERE relname = 'hog_commit_receipt'",
            ).mapTo(Long::class.java).one()
        }

    private fun receiptCount(catalogId: Long): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT count(*) FROM hog_commit_receipt WHERE catalog_id = :c")
                .bind("c", catalogId).mapTo(Long::class.java).one()
        }

    private fun service(
        receiptRetentionSeconds: Long = WEEK,
        receiptPurgePage: Int = CleanupService.RECEIPT_PURGE_PAGE,
        receiptPurgeBudgetMs: Long = CleanupService.RECEIPT_PURGE_BUDGET_MS,
    ) = CleanupService(
        jdbi,
        deadRemovals,
        receiptRetentionSeconds = receiptRetentionSeconds,
        receiptPurgePage = receiptPurgePage,
        receiptPurgeBudgetMs = receiptPurgeBudgetMs,
    )

    // ---- what goes and what stays ------------------------------------------

    @Test
    fun `only receipts past the retention are purged`() {
        val catalogId = seedCatalog("receipt-cutoff")
        seedReceipts(catalogId, count = 40, ageSeconds = 8 * DAY)
        seedReceipts(catalogId, count = 25, ageSeconds = 6 * DAY)
        // One second on the young side of the cutoff, which is the row a
        // `<=`/`<` mistake takes.
        seedReceipts(catalogId, count = 1, ageSeconds = WEEK - 1)

        val result = service().runOnce("receipt-cutoff", batchSize = 10)

        // MUTATION: flip the purge's `created_at <` to `<=` or drop the
        // predicate and this reds on the survivors.
        assertThat(result.receiptsPurged)
            .describedAs("the 40 rows older than a week, and nothing else")
            .isEqualTo(40)
        assertThat(receiptCount(catalogId)).isEqualTo(26)
        val oldestLeftSeconds =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT max(EXTRACT(EPOCH FROM now() - created_at)) " +
                        "FROM hog_commit_receipt WHERE catalog_id = :c",
                ).bind("c", catalogId).mapTo(Double::class.javaObjectType).one()
            }
        assertThat(oldestLeftSeconds)
            .describedAs("nothing older than the cutoff survived")
            .isLessThan(WEEK.toDouble())
    }

    @Test
    fun `a catalog's snapshot retention raises the cutoff, and never lowers it`() {
        // 10 days of snapshot retention means pyhoglake may legitimately
        // hold a prepared payload for ~5 days and replay it, so the
        // derived floor is 2 x 10 = 20 days and a 15-day-old receipt must
        // survive the 7-day knob. A 21-day-old one is past even the floor.
        val guarded = seedCatalog("receipt-floor", snapshotRetentionSeconds = 10 * DAY)
        seedReceipts(guarded, count = 10, ageSeconds = 15 * DAY)
        seedReceipts(guarded, count = 3, ageSeconds = 21 * DAY)

        val result = service().runOnce("receipt-floor", batchSize = 10)

        // MUTATION: drop the `maxOf(receiptRetentionSeconds, floor)` and
        // all 13 go instead of 3.
        assertThat(result.receiptsPurged)
            .describedAs("only what is past 2 x the catalog's 10-day snapshot retention")
            .isEqualTo(3)
        assertThat(receiptCount(guarded)).isEqualTo(10)

        // The mirror case: a SHORT snapshot retention must not shorten the
        // instance knob. gigahog-prod-us runs 3,600 s, so its derived
        // floor is 7,200 s — two hours — and a 3-day-old receipt must
        // survive the 7-day knob regardless.
        val busy = seedCatalog("receipt-busy", snapshotRetentionSeconds = 3_600)
        seedReceipts(busy, count = 5, ageSeconds = 3 * DAY)
        val busyRun = service().runOnce("receipt-busy", batchSize = 10)
        assertThat(busyRun.receiptsPurged)
            .describedAs("the floor raises the cutoff; it can never lower it")
            .isZero()
        assertThat(receiptCount(busy)).isEqualTo(5)
    }

    @Test
    fun `the derived floor is capped, so a long-retention catalog still purges`() {
        // 90-day snapshot retention would derive a 180-day floor and the
        // catalog would effectively never purge — silently, since the
        // purge logs nothing when it finds nothing. The cap is
        // RECEIPT_FLOOR_CEILING_SECONDS (30 days), so a 31-day-old
        // receipt goes and a 29-day-old one does not.
        //
        // MUTATION: remove the `coerceAtMost` and this reds on both
        // counts. It is also the only arithmetic here that could
        // overflow: `2 x` an operator-supplied bigint.
        val catalogId = seedCatalog("receipt-capped", snapshotRetentionSeconds = 90 * DAY)
        seedReceipts(catalogId, count = 6, ageSeconds = 31 * DAY)
        seedReceipts(catalogId, count = 4, ageSeconds = 29 * DAY)
        val run = service().runOnce("receipt-capped", batchSize = 10)
        assertThat(run.receiptsPurged).isEqualTo(6)
        assertThat(receiptCount(catalogId)).isEqualTo(4)
    }

    @Test
    fun `retention off keeps every receipt, which is V7's behaviour`() {
        val catalogId = seedCatalog("receipt-off")
        seedReceipts(catalogId, count = 12, ageSeconds = 400 * DAY)
        // MUTATION: remove the `receiptRetentionSeconds <= 0` early return
        // and this reds — a 0 retention would purge everything, which is
        // the opposite of what 0 means on every other retention knob here.
        val off = service(receiptRetentionSeconds = 0).runOnce("receipt-off", batchSize = 10)
        assertThat(off.receiptsPurged).isZero()
        assertThat(receiptCount(catalogId)).isEqualTo(12)
    }

    /**
     * NULL snapshot retention: the floor is THE KNOB, and idempotency is
     * unprotected for anything held past it.
     *
     * The derivation the floor comes from has no term here, and not
     * because the window is short — because it is UNBOUNDED. A catalog
     * with retention off never expires a snapshot, so pyhoglake's
     * prepared-payload cache has no age limit to beat (`client.py`:
     * `float("inf") if seconds is None`), and no derived floor can cover
     * an infinite window. So the knob stands as the floor: a payload
     * replayed more than `HOGLAKE_RECEIPT_RETENTION_SECONDS` after it was
     * prepared re-publishes its files, which is the accepted risk the
     * knob's KDoc states (and which is hedgerow's situation on EVERY
     * catalog, retention or not).
     *
     * Every real catalog has snapshot retention set, so this is an edge
     * rather than a regime — but it is the edge with the longest client
     * window, which is why it is asserted rather than left to the reader.
     */
    @Test
    fun `a catalog with snapshot retention disabled falls back to the knob as its floor`() {
        val catalogId = seedCatalog("receipt-null-retention", snapshotRetentionSeconds = null)
        seedReceipts(catalogId, count = 7, ageSeconds = 9 * DAY)
        seedReceipts(catalogId, count = 2, ageSeconds = 5 * DAY)
        val run = service().runOnce("receipt-null-retention", batchSize = 10)
        // Past the 7-day knob: gone. Inside it: kept. MUTATION: make the
        // NULL branch 0 (or anything below the knob) and the `maxOf`
        // hides it; make it unbounded and nothing is ever purged here.
        assertThat(run.receiptsPurged).isEqualTo(7)
        assertThat(receiptCount(catalogId)).isEqualTo(2)
    }

    // ---- what bounds it ----------------------------------------------------

    @Test
    fun `the purge walks in pages and stops on its wall budget`() {
        val catalogId = seedCatalog("receipt-pages")
        seedReceipts(catalogId, count = 250, ageSeconds = 30 * DAY)

        // A budget of 0 stops the walk BEFORE its first page: the bound is
        // checked at the top of the loop, so a run with no budget does no
        // work at all rather than one page's worth.
        //
        // MUTATION: move the deadline check to the bottom of the loop and
        // this reds at 100.
        val noBudget =
            service(receiptPurgePage = 100, receiptPurgeBudgetMs = 0)
                .runOnce("receipt-pages", batchSize = 10)
        assertThat(noBudget.receiptsPurged)
            .describedAs("a budget stop is a budget stop even before the first page")
            .isZero()
        assertThat(receiptCount(catalogId)).isEqualTo(250)

        // With a budget, the walk continues until a page comes up short —
        // the eligible rows are a dense prefix of the index range, so
        // `purged < page` is the end of them and needs no cursor.
        val run = service(receiptPurgePage = 100).runOnce("receipt-pages", batchSize = 10)
        assertThat(run.receiptsPurged).isEqualTo(250)
        assertThat(receiptCount(catalogId)).isZero()
    }

    @Test
    fun `the purge is per catalog and leaves its neighbours alone`() {
        // The drained-ledger purge is deliberately GLOBAL (its cutoff is a
        // per-process constant and its key is global). This one is not: the
        // cutoff's floor is derived per catalog, so a sweep for A must not
        // judge B's receipts by A's floor.
        val a = seedCatalog("receipt-a")
        val b = seedCatalog("receipt-b", snapshotRetentionSeconds = 30 * DAY)
        seedReceipts(a, count = 9, ageSeconds = 10 * DAY)
        seedReceipts(b, count = 9, ageSeconds = 10 * DAY)

        val sweptA = service().runOnce("receipt-a", batchSize = 10)
        assertThat(sweptA.receiptsPurged).isEqualTo(9)
        assertThat(receiptCount(a)).isZero()
        // MUTATION: drop `catalog_id = :catalogId` from the page and B's
        // rows go with A's, under A's floor.
        assertThat(receiptCount(b))
            .describedAs("B's 60-day floor is B's, whoever is being swept")
            .isEqualTo(9)
    }

    @Test
    fun `a purge that throws is a zero on the wire and never fails the drain`() {
        val catalogId = seedCatalog("receipt-fenced")
        seedReceipts(catalogId, count = 5, ageSeconds = 30 * DAY)
        // A ROW THAT CANNOT BE DELETED, built as a foreign key rather
        // than a trigger: one receipt is referenced with ON DELETE
        // RESTRICT, so the page's DELETE raises a foreign-key violation
        // and the whole page transaction rolls back. Retention is hygiene:
        // it is allowed to fail, it is not allowed to misreport the drain
        // it followed.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                CREATE TABLE receipt_pin (
                    catalog_id bigint NOT NULL,
                    idempotency_key uuid NOT NULL,
                    FOREIGN KEY (catalog_id, idempotency_key)
                        REFERENCES hog_commit_receipt (catalog_id, idempotency_key) ON DELETE RESTRICT
                )
                """.trimIndent(),
            )
            h.createUpdate(
                "INSERT INTO receipt_pin SELECT catalog_id, idempotency_key FROM hog_commit_receipt " +
                    "WHERE catalog_id = :c LIMIT 1",
            ).bind("c", catalogId).execute()
        }
        try {
            // MUTATION: remove the try/catch around purgeCommitReceipts
            // and this throws instead of returning a result.
            val result = service().runOnce("receipt-fenced", batchSize = 10)
            assertThat(result.receiptsPurged).isZero()
            // AND THE FAILURE IS COUNTED, which is the half that makes the
            // zero above readable. Without it this run is indistinguishable
            // from an idle one on every surface, and the state it describes
            // — a page that can never finish — is the one the 58 GiB legacy
            // backlog would fail in, forever, silently.
            //
            // MUTATION: return ReceiptPurge() instead of
            // ReceiptPurge(failures = 1) from either fence and this reds.
            assertThat(result.receiptsPurgeFailures)
                .describedAs("a failing purge must not read as an idle one")
                .isEqualTo(1)
            assertThat(result.removed).isZero()
            assertThat(receiptCount(catalogId)).isEqualTo(5)
            // The drain's own ledger row still says `ok`: the run happened
            // and did what it could.
            val status =
                jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "SELECT status FROM hog_maintenance_run WHERE catalog_id = :c " +
                            "AND task = 'cleanup' ORDER BY run_id DESC LIMIT 1",
                    ).bind("c", catalogId).mapTo(String::class.java).one()
                }
            assertThat(status)
                .describedAs("a hygiene failure must not become the drain's verdict")
                .isEqualTo("ok")
        } finally {
            jdbi.useHandleUnchecked { h -> h.execute("DROP TABLE receipt_pin") }
        }
    }

    // ---- what the ledger records -------------------------------------------

    @Test
    fun `the count lands on the run ledger's cleanup row`() {
        val catalogId = seedCatalog("receipt-ledger")
        seedReceipts(catalogId, count = 4, ageSeconds = 30 * DAY)
        service().runOnce("receipt-ledger", batchSize = 10)
        val resultJson =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT CAST(result AS text) FROM hog_maintenance_run WHERE catalog_id = :c " +
                        "AND task = 'cleanup' ORDER BY run_id DESC LIMIT 1",
                ).bind("c", catalogId).mapTo(String::class.java).one()
            }
        // Snake_case, like every other counter in the ledger: the row is
        // the API's own response body.
        assertThat(resultJson).contains(""""receipts_purged": 4""")
    }

    @Test
    fun `the purge rides every run, and a run with nothing eligible reports zero`() {
        val catalogId = seedCatalog("receipt-idle")
        seedReceipts(catalogId, count = 3, ageSeconds = 60)
        val result = service().runOnce("receipt-idle", batchSize = 10)
        assertThat(result.receiptsPurged).isZero()
        // NON_DEFAULT on the stored model: a zero is OMITTED from the
        // ledger row, and the read path fills it back in. Asserted here
        // because the alternative — a row asserting 0 — is what makes a
        // counter that postdates a row indistinguishable from one that
        // counted nothing.
        val resultJson =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT CAST(result AS text) FROM hog_maintenance_run WHERE catalog_id = :c " +
                        "AND task = 'cleanup' ORDER BY run_id DESC LIMIT 1",
                ).bind("c", catalogId).mapTo(String::class.java).one()
            }
        assertThat(resultJson).doesNotContain("receipts_purged")
        assertThat(receiptCount(catalogId)).isEqualTo(3)
    }

    // ---- the per-page cost, recorded ---------------------------------------

    /**
     * THE REGIME THAT MATTERS FOR SEVEN DAYS, and the one the first
     * version of this test could not see: legacy receipts with bodies.
     *
     * The case below measures post-V24 rows, which store nothing out of
     * line. These carry [BODY_BYTES] each, so a page of them is
     * `page x ~80` TOAST chunk deletes on top of its heap deletes, all
     * inside one transaction. Both figures are printed, because the ratio
     * between them is what `CleanupService.RECEIPT_PURGE_PAGE` is sized
     * from — and because a reviewer reading a per-row figure for this
     * purge needs to know WHICH rows: the first version of this suite
     * measured only the cheap ones and concluded the legacy backlog was
     * two seconds of work.
     */
    @Test
    fun `a page of legacy receipts pays for its TOAST chunks, and stays inside the statement bound`() {
        val catalogId = seedCatalog("receipt-legacy-cost")
        val rows = CleanupService.RECEIPT_PURGE_PAGE * 3
        seedLegacyReceipts(catalogId, count = rows, ageSeconds = 30 * DAY)
        jdbi.useHandleUnchecked { it.execute("VACUUM (ANALYZE) hog_commit_receipt") }
        val toastBefore = toastBytes()
        assertThat(toastBefore)
            .describedAs("the fixture is only the legacy regime if the bodies really toasted")
            .isGreaterThan(rows.toLong() * BODY_BYTES / 2)

        val started = System.nanoTime()
        val run = service(receiptPurgeBudgetMs = 120_000).runOnce("receipt-legacy-cost", batchSize = 10)
        val elapsedMs = (System.nanoTime() - started) / 1_000_000.0
        assertThat(run.receiptsPurged).isEqualTo(rows.toLong())
        assertThat(run.receiptsPurgeFailures)
            .describedAs("no page may hit the %s statement bound at this page size", "5s")
            .isZero()
        val perPageMs = elapsedMs * CleanupService.RECEIPT_PURGE_PAGE / rows
        println(
            "receipt purge, LEGACY rows (${BODY_BYTES / 1024} KiB bodies, " +
                "${toastBefore / 1024 / 1024} MiB of TOAST): $rows rows in " +
                "${"%.0f".format(elapsedMs)} ms (${"%.0f".format(elapsedMs * 1000 / rows)} us/row, " +
                "${"%.0f".format(perPageMs)} ms per ${CleanupService.RECEIPT_PURGE_PAGE}-row page)",
        )
        // A page must stay well inside the 5 s statement bound, which is
        // the whole reason the page size came down from 1,000. The margin
        // is deliberately large: this fixture's TOAST relation is
        // megabytes and production's is 58 GiB with a cold cache, so the
        // fixture is the OPTIMISTIC end and the bound has to hold with
        // room for the difference.
        assertThat(perPageMs)
            .describedAs("a page of legacy receipts must be nowhere near the 5 s statement bound")
            .isLessThan(1_000.0)
    }

    /**
     * The post-V24 regime: the per-row cost of a page of receipts that
     * store nothing out of line, which is every receipt written from V24
     * onward and therefore the steady state.
     *
     * Not a threshold assertion on wall clock — that is a property of the
     * machine — but a bound loose enough to catch a page that has lost its
     * index and become O(the catalog's receipts). The measured figure is
     * printed, and the arithmetic it feeds is in
     * `CleanupService.RECEIPT_PURGE_PAGE`'s KDoc.
     */
    @Test
    fun `a full page of receipts is deleted in bounded time`() {
        val catalogId = seedCatalog("receipt-cost")
        val rows = 20_000
        seedReceipts(catalogId, count = rows, ageSeconds = 30 * DAY)
        jdbi.useHandleUnchecked { it.execute("VACUUM (ANALYZE) hog_commit_receipt") }
        val started = System.nanoTime()
        val purged = service(receiptPurgeBudgetMs = 60_000).runOnce("receipt-cost", batchSize = 10).receiptsPurged
        val elapsedMs = (System.nanoTime() - started) / 1_000_000.0
        assertThat(purged).isEqualTo(rows.toLong())
        val perRowMicros = elapsedMs * 1_000 / rows
        println(
            "receipt purge: $rows rows in ${"%.0f".format(elapsedMs)} ms " +
                "(${"%.1f".format(perRowMicros)} us/row, ${rows / CleanupService.RECEIPT_PURGE_PAGE} pages)",
        )
        // 200 us/row is ~20x the honest figure and still bounds the shape
        // this test exists to catch: without V24's index every page is a
        // scan of the catalog's whole receipt history, which at 20,000
        // rows is quadratic and blows this out by orders of magnitude.
        assertThat(perRowMicros)
            .describedAs("a page must be a descent, not a scan of the catalog's receipts")
            .isLessThan(200.0)
    }

    // ---- the drain still drains ---------------------------------------------

    @Test
    fun `an unknown catalog is still a NotFound, purge or no purge`() {
        // The catalog lookup moved (it reads the whole row now, for the
        // snapshot retention the floor needs). Its failure mode must not
        // have moved with it.
        org.assertj.core.api.Assertions.assertThatThrownBy {
            service().runOnce("no-such-catalog-${UUID.randomUUID()}", batchSize = 10)
        }.isInstanceOf(com.posthog.hoglake.model.HoglakeException.NotFound::class.java)
    }
}
