package com.posthog.hoglake.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.Database
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.ReindexResult
import com.posthog.hoglake.model.ReindexSkip
import com.posthog.hoglake.observability.IndexBloatGauges
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.testing.PgTestSupport
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

/**
 * The reindex task (#268) against a real Postgres: real bloat, a real
 * REINDEX INDEX CONCURRENTLY, a real failed one and its `_ccnew`
 * leftover, and every guard driven by the state it names rather than by a
 * flag.
 *
 * A FRESH DATABASE PER CASE. The subject is database-wide state — index
 * sizes, invalid indexes, advisory locks, Flyway's history — so cases
 * sharing a database would be ordering-dependent by construction.
 *
 * Every database is `productionSession = true` so the pooled connections
 * carry production's 60 s `statement_timeout`: the restore after the
 * rebuild's raised bound is asserted against THAT value, not against the
 * server default a test pool would otherwise have.
 */
@Tag("integration")
class ReindexServiceIntegrationTest {
    private val json = ObjectMapper()
    private val opened = mutableListOf<PgTestSupport.TestDb>()

    @AfterEach
    fun tearDown() {
        opened.forEach { it.close() }
        Metrics.clear()
        IndexBloatGauges.clear()
    }

    private fun db(): PgTestSupport.TestDb =
        PgTestSupport.freshDatabase(productionSession = true, holdsTransactions = true).also { opened += it }

    // ---- fixture -----------------------------------------------------------

    private fun catalog(
        db: PgTestSupport.TestDb,
        name: String,
        floor: Long = 100,
    ): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "INSERT INTO hog_catalog (name, data_path, earliest_snapshot_id, last_snapshot_id) " +
                    "VALUES (:n, 's3://reindex/' || :n, :f, 1000) RETURNING catalog_id",
            ).bind("n", name).bind("f", floor).mapTo(Long::class.java).one()
        }

    /**
     * Real bloat on `hog_data_file`'s indexes: [rows] file rows inserted,
     * all but one in [keepEvery] deleted SCATTERED (so no leaf page empties
     * and btree vacuum can reclaim nothing), then VACUUM ANALYZE so
     * `relpages`, `reltuples` and `pg_stats` describe the result. The path
     * index is the largest (an md5 path per row), at ~20 MiB for 200k rows.
     */
    private fun bloat(
        db: PgTestSupport.TestDb,
        catalogId: Long,
        rows: Int = 200_000,
        keepEvery: Int = 20,
    ) = db.jdbi.useHandleUnchecked { h ->
        h.execute("INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)", catalogId)
        h.createUpdate(
            """
            INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                       path, record_count, file_size_bytes, row_id_start)
            SELECT :c, g, 1, 1,
                   's3://reindex/live/' || md5(g::text) || '/part-' || g || '.parquet',
                   100, 4096, g::bigint * 100
              FROM generate_series(1, :n) g
            """,
        ).bind("c", catalogId).bind("n", rows).execute()
        h.createUpdate("DELETE FROM hog_data_file WHERE catalog_id = :c AND data_file_id % :k <> 0")
            .bind("c", catalogId)
            .bind("k", keepEvery)
            .execute()
        h.execute("VACUUM (ANALYZE) hog_data_file")
    }

    private fun estimates(db: PgTestSupport.TestDb) =
        db.jdbi.withHandleUnchecked { h -> ReindexService(db.jdbi).estimate(h) }

    private fun indexValid(
        db: PgTestSupport.TestDb,
        index: String,
    ): Boolean? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = :n",
            ).bind("n", index).mapTo(Boolean::class.javaObjectType).findOne().orElse(null)
        }

    private fun invalidIndexes(db: PgTestSupport.TestDb): List<String> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid " +
                    "WHERE NOT i.indisvalid ORDER BY 1",
            ).mapTo(String::class.java).list()
        }

    private data class LedgerRow(val catalog: String, val status: String, val error: String?, val result: JsonNode?)

    private fun ledger(db: PgTestSupport.TestDb): List<LedgerRow> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT c.name, r.status, r.error, CAST(r.result AS text) AS result
                  FROM hog_maintenance_run r JOIN hog_catalog c ON c.catalog_id = r.catalog_id
                 WHERE r.task = 'reindex'
                 ORDER BY r.run_id
                """,
            ).map { rs, _ ->
                LedgerRow(
                    rs.getString("name"),
                    rs.getString("status"),
                    rs.getString("error"),
                    rs.getString("result")?.let { json.readTree(it) },
                )
            }.list()
        }

    /**
     * An open transaction holding ROW EXCLUSIVE on `hog_data_file`, which
     * is what any commit holds: REINDEX CONCURRENTLY's first wait
     * ("waiting for writers before build") is for exactly such a
     * transaction, AFTER it has committed the `_ccnew` catalog entry. So
     * a REINDEX started while this is open parks with its leftover
     * already on disk — the production failure, produced by the
     * production mechanism.
     */
    private fun <T> withOpenWriter(
        db: PgTestSupport.TestDb,
        body: () -> T,
    ): T {
        val writer: Handle = db.jdbi.open()
        try {
            writer.begin()
            writer.execute("LOCK TABLE hog_data_file IN ROW EXCLUSIVE MODE")
            return body()
        } finally {
            writer.rollback()
            writer.close()
        }
    }

    private companion object {
        /** Indexes whose keys end in a unique column, so btree deduplication never applies. */
        val FRESH =
            listOf(
                "hog_file_column_stats_pkey",
                "hog_data_file_pkey",
                "hog_data_file_path",
                "hog_data_file_maintenance_scan",
                "hog_data_file_maintenance_size_scan",
            )
    }

    private fun sessionStatementTimeouts(db: PgTestSupport.TestDb): Set<String> =
        // Every pooled connection, by borrowing them all at once: the one
        // the run used must have been put back with production's bound.
        (1..8).map { db.jdbi.open() }.let { handles ->
            try {
                handles.map { it.createQuery("SHOW statement_timeout").mapTo(String::class.java).one() }.toSet()
            } finally {
                handles.forEach { it.close() }
            }
        }

    // ---- (a) real bloat, a real rebuild ------------------------------------

    @Test
    fun `real bloat crosses the threshold, the largest index is rebuilt, and the run is recorded per catalog`() {
        val db = db()
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        val id = catalog(db, "rx-a")
        catalog(db, "rx-a-neighbour")

        // CONTROL FIRST: a freshly migrated, freshly analyzed database is
        // not bloated. An estimate that reads every index as 3x would make
        // the assertions below pass for the wrong reason.
        db.jdbi.useHandleUnchecked { h -> h.execute("ANALYZE") }
        assertThat(estimates(db).filter { ReindexService.overThreshold(it) }).isEmpty()
        assertThat(DatabaseHealthService(db.jdbi).report().findings.map { it.code }).doesNotContain("index_bloat")

        bloat(db, id)
        val before = estimates(db)
        val path = before.single { it.index == "hog_data_file_path" }
        println("[#268] bloated hog_data_file_path: $path ratio=${path.ratio} excess=${path.excessBytes}")
        assertThat(path.expectedBytes).describedAs("every key column is analyzed").isNotNull()
        assertThat(
            path.ratio,
        ).describedAs("19 rows in 20 deleted, scattered").isGreaterThan(ReindexService.RATIO_THRESHOLD)
        assertThat(ReindexService.overThreshold(path)).isTrue()

        // The health page carries the same estimate on its index rows and
        // the finding, from the same statement.
        val health = DatabaseHealthService(db.jdbi).report()
        val row = health.indexes.single { it.name == "hog_data_file_path" }
        assertThat(row.estimatedBloatBytes).isEqualTo(path.excessBytes)
        assertThat(row.estimatedBloatRatio).isEqualTo(path.ratio)
        assertThat(health.findings.map { it.code }).contains("index_bloat")

        // The health page's service owns no gauge (an API pod's shape):
        // computing the estimate published nothing. MUTATION: publish
        // regardless of `publishGauge` and this reds.
        assertThat(registry.scrape()).doesNotContain("hoglake_index_bloat_bytes")

        val started = System.nanoTime()
        val result = ReindexService(db.jdbi, publishGauge = true).runOnce(MaintenanceTrigger.LOOP)
        println("[#268] run took ${(System.nanoTime() - started) / 1_000_000} ms: $result")

        assertThat(result.checked).isEqualTo(before.size.toLong())
        assertThat(result.overThreshold).isPositive()
        assertThat(result.skippedReason).isNull()
        assertThat(result.table).isEqualTo("hog_data_file")
        assertThat(result.index)
            .describedAs("the largest excess goes first")
            .isEqualTo(ReindexService.pickVictim(before.filter { ReindexService.overThreshold(it) })!!.index)
        assertThat(result.beforeBytes!!).isGreaterThan(result.afterBytes!!)
        assertThat(result.expectedBytes).isEqualTo(path.expectedBytes)
        assertThat(result.excluded).isZero()
        assertThat(result.postStepError).isNull()
        assertThat(result.durationMs).isNotNull()
        assertThat(indexValid(db, result.index!!)).describedAs("the rebuilt index is the valid one").isTrue()
        assertThat(invalidIndexes(db)).describedAs("a successful REINDEX leaves no copy behind").isEmpty()

        // The rebuilt index is no longer over threshold — the estimate
        // agrees with a fresh build, which is the other half of the control.
        val rebuilt = estimates(db).single { it.index == result.index }
        assertThat(ReindexService.overThreshold(rebuilt)).isFalse()

        // ONE ROW PER CATALOG, identical: the run is instance-wide.
        val rows = ledger(db)
        assertThat(rows.map { it.catalog }).containsExactlyInAnyOrder("rx-a", "rx-a-neighbour")
        assertThat(rows).allSatisfy { row ->
            assertThat(row.status).isEqualTo("ok")
            assertThat(row.result!!["index"].asText()).isEqualTo(result.index)
            assertThat(row.result["before_bytes"].asLong()).isGreaterThan(row.result["after_bytes"].asLong())
            assertThat(row.result["invalid_dropped"].asLong()).isZero()
            assertThat(row.result["expected_bytes"].asLong()).isEqualTo(path.expectedBytes)
            assertThat(row.result.has("skipped_reason")).describedAs("null is absent on the wire").isFalse()
        }

        // The raised bound went back: every pooled connection carries the
        // session's 60 s, not the rebuild's hour.
        assertThat(sessionStatementTimeouts(db)).containsExactly("1min")

        // The gauge was published, and republished after the rebuild.
        val scrape = registry.scrape()
        assertThat(scrape).contains("hoglake_index_bloat_bytes{index=\"hog_data_file_path\",table=\"hog_data_file\"}")
    }

    @Test
    fun `an index with a key column pg_stats has never seen gets no estimate rather than a huge one`() {
        val db = db()
        bloat(db, catalog(db, "rx-nostats"), rows = 60_000)
        // An expression column's statistics exist only once the table is
        // analyzed AFTER the index exists, so this index has none.
        // Joining pg_stats INNER (as ioguix does) would count its width as
        // zero, call a fresh build tiny, and report the whole index as
        // bloat. MUTATION: make `complete` always true and this reds.
        db.jdbi.useHandleUnchecked { h ->
            h.execute("CREATE INDEX hog_data_file_upper_path ON hog_data_file (upper(path))")
        }
        val e = estimates(db).single { it.index == "hog_data_file_upper_path" }
        assertThat(e.expectedBytes).isNull()
        assertThat(ReindexService.overThreshold(e, minRatioIndexBytes = 0)).isFalse()

        // And once analyzed, the expression's own pg_stats row is found
        // (the index-relation arm of the attribute join) and it is a
        // fresh, unbloated build.
        db.jdbi.useHandleUnchecked { h -> h.execute("ANALYZE hog_data_file") }
        val analyzed = estimates(db).single { it.index == "hog_data_file_upper_path" }
        assertThat(analyzed.expectedBytes).isNotNull()
        assertThat(analyzed.ratio!!).isLessThan(ReindexService.RATIO_THRESHOLD)
    }

    // ---- the estimator against ground truth ---------------------------------

    /**
     * Fresh builds of the indexes that do NOT deduplicate (every key ends
     * in a unique column), so a correct estimate reads ~1.0. This is what
     * pins the arithmetic itself — the fillfactor, the page and line-pointer
     * terms, MAXALIGN — rather than its outcome on a bloated fixture, where
     * a 26x ratio survives any of those being wrong.
     */
    @Test
    fun `a fresh build of every non-deduplicating index reads as 0_95 to 1_10`() {
        val db = db()
        val id = catalog(db, "rx-fresh")
        db.jdbi.useHandleUnchecked { h ->
            h.execute("INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)", id)
            h.execute(
                "INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, path, " +
                    "record_count, file_size_bytes, row_id_start) " +
                    "SELECT ?, g, 1, 1, 's3://reindex/live/' || md5(g::text) || '/part-' || g || '.parquet', " +
                    "100, 4096 + g % 977, g::bigint * 100 FROM generate_series(1, 100000) g",
                id,
            )
            h.execute(
                "INSERT INTO hog_file_column_stats (catalog_id, data_file_id, field_id, value_count, null_count) " +
                    "SELECT ?, g, f, 100, 0 FROM generate_series(1, 100000) g, generate_series(1, 5) f",
                id,
            )
            for (index in FRESH) h.execute("REINDEX INDEX $index")
            h.execute("VACUUM (ANALYZE) hog_data_file")
            h.execute("VACUUM (ANALYZE) hog_file_column_stats")
        }
        val byName = estimates(db).associateBy { it.index }
        for (index in FRESH) {
            val e = byName.getValue(index)
            println("[#268] fresh $index: size=${e.sizeBytes} expected=${e.expectedBytes} ratio=${e.ratio}")
            // MUTATIONS: drop the fillfactor term (expected shrinks ~10%,
            // ratio ~1.11), the 4-byte line pointer, or the MAXALIGN of the
            // data width, and an index here leaves the band.
            assertThat(e.ratio).describedAs(index).isBetween(0.95, 1.10)
        }
    }

    /**
     * A NULLABLE key column: the null bitmap makes the estimate's tuple
     * header 12 bytes (ioguix's term). On a fresh build that OVER-states
     * the expected size — a tuple with a null is 12 header (aligned 16) +
     * 16 data, one without is 8 + 24, both 32, while the estimate pads the
     * averaged header and data separately to 16 + 24 = 40 — so the ratio
     * reads ~0.82 and bloat on such an index is UNDER-reported. Pinned as
     * measured, the conservative side; a header of 8 would read ~1.0. The
     * trailing unique column keeps btree deduplication (which merges equal
     * keys, NULLs included) out of the measurement.
     */
    @Test
    fun `a nullable key column is estimated with the null-bitmap header`() {
        val db = db()
        val id = catalog(db, "rx-null")
        db.jdbi.useHandleUnchecked { h ->
            h.execute("INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)", id)
            h.execute(
                "INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, " +
                    "path, record_count, file_size_bytes, row_id_start) " +
                    "SELECT ?, g, 1, 1, CASE WHEN g % 2 = 0 THEN 2 + g ELSE NULL END, 's3://x/' || g, 100, 4096, " +
                    "g::bigint * 100 FROM generate_series(1, 100000) g",
                id,
            )
            h.execute(
                "CREATE INDEX hog_data_file_test_nullable ON hog_data_file (catalog_id, end_snapshot, data_file_id)",
            )
            h.execute("VACUUM (ANALYZE) hog_data_file")
        }
        val e = estimates(db).single { it.index == "hog_data_file_test_nullable" }
        println("[#268] fresh nullable: size=${e.sizeBytes} expected=${e.expectedBytes} ratio=${e.ratio}")
        // MUTATION: header 12 -> 8 and the ratio rises to ~1.0, out of band.
        assertThat(e.ratio).isBetween(0.75, 0.90)
    }

    @Test
    fun `an index never vacuumed (reltuples -1) gets no estimate`() {
        val db = db()
        bloat(db, catalog(db, "rx-reltuples"), rows = 60_000)
        // PG 14+ marks a never-vacuumed relation with reltuples = -1. Set
        // directly on a bloated index, so the arm is the only thing between
        // it and a negative row count read as a one-page "ideal" size.
        // MUTATION: drop `idx.reltuples >= 0` and this reds.
        db.jdbi.useHandleUnchecked { h ->
            h.execute("UPDATE pg_class SET reltuples = -1 WHERE relname = 'hog_data_file_path'")
        }
        val e = estimates(db).single { it.index == "hog_data_file_path" }
        assertThat(e.expectedBytes).isNull()
        assertThat(ReindexService.overThreshold(e, 0)).isFalse()
    }

    // ---- (b) a failed rebuild, its leftover, and the retry -----------------

    /**
     * [withOpenWriter], but the writer commits by itself after [holdMs] on
     * its own thread — so a build cancelled at its statement bound can
     * then drop its own copy, which waits out the same writer.
     */
    private fun writerFor(
        db: PgTestSupport.TestDb,
        holdMs: Long,
    ): CompletableFuture<Void> {
        val writer: Handle = db.jdbi.open()
        writer.begin()
        writer.execute("LOCK TABLE hog_data_file IN ROW EXCLUSIVE MODE")
        return CompletableFuture.runAsync {
            Thread.sleep(holdMs)
            writer.rollback()
            writer.close()
        }
    }

    @Test
    fun `a failed REINDEX drops its own copy at once, records the attempt, and is not retried next day`() {
        val db = db()
        val id = catalog(db, "rx-b")
        bloat(db, id, rows = 60_000)
        // A VALID index with a leftover-shaped name is somebody's, not a
        // corpse; it must survive every drop. MUTATION: drop
        // `NOT i.indisvalid` from LEFTOVERS_SQL and this index goes too.
        db.jdbi.useHandleUnchecked { h ->
            h.execute("CREATE INDEX hog_data_file_mine_ccnew ON hog_data_file (record_count)")
        }
        val service = ReindexService(db.jdbi, statementTimeout = Duration.ofSeconds(2), minRatioIndexBytes = 0)

        // The writer outlives the REINDEX's 2 s bound and is gone a second
        // later, inside the drop's own bound.
        val writer = writerFor(db, holdMs = 3_000)
        val failure =
            assertThatThrownBy { service.runOnce() }
                .isInstanceOf(ReindexService.ReindexFailed::class.java)
                .hasMessageContaining("statement timeout")
                .actual()
        writer.get(30, TimeUnit.SECONDS)
        val attempted = (failure as ReindexService.ReindexFailed).partial
        assertThat(attempted.index).isNotNull()
        assertThat(attempted.expectedBytes).isNotNull()
        assertThat(attempted.afterBytes).isNull()
        // MUTATION: drop the catch-path drop and the copy survives until
        // tomorrow — a ready `_ccnew` every insert keeps maintaining.
        assertThat(attempted.invalidDropped).isEqualTo(1)
        assertThat(invalidIndexes(db)).describedAs("the copy went with the failure").isEmpty()
        assertThat(indexValid(db, "hog_data_file_mine_ccnew")).isTrue()

        val failedRow = ledger(db).single()
        assertThat(failedRow.status).isEqualTo("failed")
        assertThat(failedRow.error).contains("statement timeout")
        assertThat(failedRow.result!!["index"].asText()).isEqualTo(attempted.index)
        assertThat(failedRow.result["expected_bytes"].asLong()).isEqualTo(attempted.expectedBytes)
        assertThat(failedRow.result.has("after_bytes")).isFalse()
        // The failure path restores the bound too.
        assertThat(sessionStatementTimeouts(db)).containsExactly("1min")

        // A leftover that did NOT go at once (an operator's cancelled
        // build) is dropped before the next run.
        val operator = db.jdbi.open()
        try {
            operator.execute("SET statement_timeout = '1s'")
            withOpenWriter(db) {
                assertThatThrownBy { operator.execute("REINDEX INDEX CONCURRENTLY hog_data_file_pkey") }
                    .hasMessageContaining("statement timeout")
            }
            operator.execute("RESET statement_timeout")
        } finally {
            operator.close()
        }
        assertThat(invalidIndexes(db)).containsExactly("hog_data_file_pkey_ccnew")

        // Next day: the leftover goes first, and the index whose last
        // attempt failed is NOT picked again — the next candidate is.
        // MUTATION: drop the last-attempt exclusion and this reds.
        val next = service.runOnce()
        assertThat(next.invalidDropped).isEqualTo(1)
        assertThat(next.index).isNotNull().isNotEqualTo(attempted.index)
        assertThat(next.afterBytes).isNotNull()
        assertThat(invalidIndexes(db)).isEmpty()
        assertThat(indexValid(db, "hog_data_file_mine_ccnew")).isTrue()
        assertThat(ledger(db).map { it.status }).containsExactly("failed", "ok")
    }

    /** A failed attempt row for [index], as `recordedAllCatalogs` writes one. */
    private fun failedAttempt(
        db: PgTestSupport.TestDb,
        catalogId: Long,
        index: String,
    ) = db.jdbi.useHandleUnchecked { h ->
        h.execute(
            "INSERT INTO hog_maintenance_run (catalog_id, task, run_trigger, started_at, finished_at, " +
                "status, error, result) VALUES (?, 'reindex', 'loop', now(), now(), 'failed', " +
                "'canceling statement due to statement timeout', CAST(? AS jsonb))",
            catalogId,
            """{"checked": 1, "over_threshold": 1, "invalid_dropped": 0, "index": "$index",
               "table": "hog_data_file", "expected_bytes": 1, "before_bytes": 2}""",
        )
    }

    @Test
    fun `last_attempt_failed when every candidate failed its newest attempt, and a skip row does not clear it`() {
        val db = db()
        val id = catalog(db, "rx-lastfail")
        bloat(db, id, rows = 60_000)
        val service = ReindexService(db.jdbi, minRatioIndexBytes = 0)
        val ranked =
            db.jdbi.withHandleUnchecked {
                    h ->
                ReindexService.rank(service.estimate(h).filter { ReindexService.overThreshold(it, 0) })
            }
        assertThat(ranked).isNotEmpty()
        ranked.forEach { failedAttempt(db, id, it.index) }

        val skipped = service.runOnce()
        assertThat(skipped.skippedReason).isEqualTo(ReindexSkip.LAST_ATTEMPT_FAILED.wire)
        assertThat(
            skipped.index,
        ).describedAs("names the one that would have gone first").isEqualTo(ranked.first().index)
        assertThat(skipped.expectedBytes).isEqualTo(ranked.first().expectedBytes)
        assertThat(skipped.beforeBytes).isNull()

        // That ok row names the index but is not an ATTEMPT. MUTATION: drop
        // the `before_bytes IS NOT NULL` filter and the skip row reads as
        // the newest attempt — "ok" — and the failed index is retried.
        assertThat(service.runOnce().skippedReason).isEqualTo(ReindexSkip.LAST_ATTEMPT_FAILED.wire)

        // An OK newest attempt clears it.
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_maintenance_run (catalog_id, task, run_trigger, started_at, finished_at, " +
                    "status, result) VALUES (?, 'reindex', 'loop', now(), now(), 'ok', CAST(? AS jsonb))",
                id,
                """{"checked": 1, "over_threshold": 1, "invalid_dropped": 0,
                   "index": "${ranked.first().index}", "before_bytes": 2, "after_bytes": 1}""",
            )
        }
        assertThat(service.runOnce().index).isEqualTo(ranked.first().index)
    }

    @Test
    fun `too_large names the index and its expected size, and a smaller candidate still goes`() {
        val db = db()
        val id = catalog(db, "rx-large")
        bloat(db, id, rows = 60_000)
        val probe = ReindexService(db.jdbi, minRatioIndexBytes = 0)
        val ranked =
            db.jdbi.withHandleUnchecked { h ->
                ReindexService.rank(probe.estimate(h).filter { ReindexService.overThreshold(it, 0) })
            }
        assertThat(ranked.size).isGreaterThan(1)

        // A cap under every candidate: nothing is rebuilt, and the row says
        // which index is waiting for a human and how big it will be.
        // MUTATION: drop the cap from the victim filter and this rebuilds.
        val capped = ReindexService(db.jdbi, minRatioIndexBytes = 0, maxRebuildExpectedBytes = 1).runOnce()
        assertThat(capped.skippedReason).isEqualTo(ReindexSkip.TOO_LARGE.wire)
        assertThat(capped.index).isEqualTo(ranked.first().index)
        assertThat(capped.expectedBytes).isEqualTo(ranked.first().expectedBytes)
        assertThat(capped.beforeBytes).isNull()
        assertThat(ledger(db).single().result!!["expected_bytes"].asLong()).isEqualTo(ranked.first().expectedBytes)

        // A cap between the first and a smaller one: the first is passed
        // over, a candidate under the cap is rebuilt.
        val cap = ranked.first().expectedBytes!! - 1
        val under = ranked.first { it.expectedBytes!! <= cap }
        val result = ReindexService(db.jdbi, minRatioIndexBytes = 0, maxRebuildExpectedBytes = cap).runOnce()
        assertThat(result.index).isEqualTo(under.index)
        assertThat(result.skippedReason).isNull()
        // The passed-over index still shows up in the ledger, as a count.
        // MUTATION: drop `excluded` from the rebuild row and this reds.
        assertThat(result.excluded).isEqualTo(ranked.count { it.expectedBytes!! > cap }.toLong())
        assertThat(ledger(db).last().result!!["excluded"].asLong()).isEqualTo(result.excluded)
    }

    @Test
    @Timeout(120)
    fun `a rebuild killed mid-flight keeps its index on the failed row though every cleanup also fails`() {
        val db = db()
        bloat(db, catalog(db, "rx-killed"), rows = 60_000)
        val service = ReindexService(db.jdbi, minRatioIndexBytes = 0)
        val outcome =
            withOpenWriter(db) {
                val run = CompletableFuture.supplyAsync { runCatching { service.runOnce() } }
                val deadline = Instant.now().plusSeconds(30)
                var pid: Int? = null
                while (pid == null && Instant.now().isBefore(deadline)) {
                    pid =
                        db.jdbi.withHandleUnchecked { h ->
                            h.createQuery(
                                "SELECT pid FROM pg_stat_activity WHERE datname = current_database() " +
                                    "AND query LIKE 'REINDEX INDEX CONCURRENTLY%'",
                            ).mapTo(Int::class.javaObjectType).findOne().orElse(null)
                        }
                    if (pid == null) Thread.sleep(50)
                }
                assertThat(pid).describedAs("the rebuild never started").isNotNull()
                // What a DBA's pg_terminate_backend, an RDS failover or a
                // broken socket does: the session is gone, so the catch-path
                // drop, the statement-bound restore and the lock release all
                // throw on the same dead connection.
                db.jdbi.useHandleUnchecked { h -> h.execute("SELECT pg_terminate_backend(?)", pid) }
                run.get(60, TimeUnit.SECONDS)
            }
        // MUTATION: rethrow a cleanup failure instead of attaching it (the
        // old `finally { throw }`) and the funnel records THAT exception —
        // no ReindexFailed, no partial, no index on the row.
        val e = outcome.exceptionOrNull()
        assertThat(e).isInstanceOf(ReindexService.ReindexFailed::class.java)
        assertThat(e!!.suppressed).describedAs("the cleanups' failures ride along").isNotEmpty()
        val row = ledger(db).single()
        assertThat(row.status).isEqualTo("failed")
        assertThat(row.result!!["index"].asText()).isEqualTo((e as ReindexService.ReindexFailed).partial.index)
        assertThat(row.result.has("before_bytes")).isTrue()
    }

    @Test
    fun `a rebuild that succeeded stays ok when reading it back fails`() {
        val db = db()
        bloat(db, catalog(db, "rx-post"), rows = 60_000)
        // The session dies between the REINDEX and the read-back: the
        // post-rebuild estimate, the size, the bound's restore and the lock
        // release all fail on it. The rebuild itself is done.
        val service =
            ReindexService(
                db.jdbi,
                minRatioIndexBytes = 0,
                afterRebuild = { h -> runCatching { h.execute("SELECT pg_terminate_backend(pg_backend_pid())") } },
            )
        // MUTATION: let a post-step failure propagate (drop its catch) and
        // this run throws, recording an anonymous failed row that would
        // also exclude the index tomorrow.
        val result = service.runOnce()
        assertThat(result.index).isNotNull()
        assertThat(result.durationMs).isNotNull()
        assertThat(result.afterBytes).isNull()
        assertThat(result.postStepError).isNotBlank()
        assertThat(indexValid(db, result.index!!)).isTrue()
        val row = ledger(db).single()
        assertThat(row.status).isEqualTo("ok")
        assertThat(row.error).isNull()
        assertThat(row.result!!["post_step_error"].asText()).isNotBlank()
        // A completed rebuild is not a failed attempt: a fresh run does not
        // call the index last_attempt_failed.
        assertThat(ReindexService(db.jdbi, minRatioIndexBytes = 0).runOnce().skippedReason).isNull()
    }

    // ---- (c) the guards ----------------------------------------------------

    /** A bloated database, so a guard that fails to fire is visible as a rebuild. */
    private fun bloatedDb(): Pair<PgTestSupport.TestDb, Long> {
        val db = db()
        val id = catalog(db, "rx-guard")
        bloat(db, id, rows = 60_000)
        return db to id
    }

    /** The guards' service: the ratio floor off, so the 60k-row fixture's ~6 MiB index qualifies. */
    private fun guarded(db: PgTestSupport.TestDb) = ReindexService(db.jdbi, minRatioIndexBytes = 0)

    private fun assertSkipped(
        result: ReindexResult,
        reason: ReindexSkip,
    ) {
        assertThat(result.skippedReason).isEqualTo(reason.wire)
        assertThat(result.index).describedAs("a skipped run rebuilds nothing").isNull()
        assertThat(result.overThreshold).describedAs("the estimate still ran").isPositive()
    }

    @Test
    fun `the unguarded control rebuilds, so each guard below is what stops it`() {
        val (db, _) = bloatedDb()
        val result = guarded(db).runOnce()
        assertThat(result.skippedReason).isNull()
        assertThat(result.index).isNotNull()
    }

    @Test
    fun `retirement_pending when a dropped table's rows are eligible for retirement`() {
        val (db, id) = bloatedDb()
        // Dropped at 50, floor at 100: eligible, and it still has live rows.
        db.jdbi.useHandleUnchecked { h ->
            h.execute("UPDATE hog_table SET dropped_snapshot = 50 WHERE catalog_id = ? AND table_id = 1", id)
        }
        assertSkipped(guarded(db).runOnce(), ReindexSkip.RETIREMENT_PENDING)

        // ABOVE the floor the rows are not retirable yet, so retirement is
        // not pending and the rebuild goes ahead. MUTATION: drop the
        // floor term from the guard (ask "is anything dropped") and this
        // reds.
        db.jdbi.useHandleUnchecked { h ->
            h.execute("UPDATE hog_table SET dropped_snapshot = 500 WHERE catalog_id = ? AND table_id = 1", id)
        }
        assertThat(guarded(db).runOnce().index).isNotNull()
    }

    @Test
    fun `purge_pending on a mass delete or an unknown backlog, not on routine lag`() {
        val (db, id) = bloatedDb()
        val other = catalog(db, "rx-guard-other")
        val cap = ExpiryService.PURGE_REMAINING_CAP

        fun expiryRow(
            catalogId: Long,
            payload: String,
        ) = db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_maintenance_run (catalog_id, task, run_trigger, started_at, finished_at, " +
                    "status, result) VALUES (?, 'expiry', 'loop', now(), now(), 'ok', CAST(? AS jsonb))",
                catalogId,
                payload,
            )
        }
        expiryRow(id, """{"snapshots_expired": 0, "purge_remaining": 0}""")

        // Routine lag: a purge a few pages behind deletes at a rate page
        // reuse absorbs. MUTATION: `>= :cap` back to `> 0` and this reds.
        expiryRow(other, """{"snapshots_expired": 0, "purge_truncated": true, "purge_remaining": 7}""")
        assertThat(guarded(db).runOnce().skippedReason).isNull()

        // At the cap: behind by more than any sweep catches up.
        // MUTATION: `>= :cap` to `> :cap` and this reds.
        expiryRow(other, """{"snapshots_expired": 0, "purge_truncated": true, "purge_remaining": $cap}""")
        assertSkipped(guarded(db).runOnce(), ReindexSkip.PURGE_PENDING)

        // Truncated with the count UNKNOWN (absent: it timed out). NULL
        // compares as not-true, so only the truncated arm catches it.
        // MUTATION: drop that arm and this reds.
        expiryRow(other, """{"snapshots_expired": 0, "purge_truncated": true}""")
        assertSkipped(guarded(db).runOnce(), ReindexSkip.PURGE_PENDING)

        // The LATEST run is what counts: once that catalog's purge drained,
        // older rows are history. MUTATION: drop the
        // `ORDER BY run_id DESC LIMIT 1` and this reds.
        expiryRow(other, """{"snapshots_expired": 0, "purge_remaining": 0}""")
        assertThat(guarded(db).runOnce().index).isNotNull()
    }

    @Test
    fun `retirement and purge only skip a rebuild that would happen, so a quiet day stays quiet`() {
        val db = db()
        val id = catalog(db, "rx-quiet-guard")
        db.jdbi.useHandleUnchecked { h ->
            h.execute("ANALYZE")
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot, dropped_snapshot) VALUES (?, 1, 0, 50)",
                id,
            )
            h.execute(
                "INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, path, " +
                    "record_count, file_size_bytes, row_id_start) VALUES (?, 1, 1, 1, 's3://x/1', 1, 1, 0)",
                id,
            )
        }
        // Retirement IS pending, but nothing is over threshold: the row
        // must carry no skip, or every day of a week-long retirement
        // would be a loud row saying it skipped nothing. MUTATION: move
        // the retirement/purge guard ahead of the victim check and this
        // reds.
        val result = ReindexService(db.jdbi).runOnce()
        assertThat(result.overThreshold).isZero()
        assertThat(result.skippedReason).isNull()
    }

    @Test
    fun `migration_pending while a replica holds the migration lock`() {
        val (db, _) = bloatedDb()
        // Session advisory locks outlive a pooled handle's close, so every
        // lock taken here is released explicitly before its handle goes
        // back to the pool.
        val replica = db.jdbi.open()
        try {
            replica.createQuery("SELECT pg_advisory_lock(${Database.MIGRATION_LOCK_KEY})").mapToMap().one()
            assertSkipped(guarded(db).runOnce(), ReindexSkip.MIGRATION_PENDING)
        } finally {
            replica.createQuery("SELECT pg_advisory_unlock(${Database.MIGRATION_LOCK_KEY})").mapToMap().one()
            replica.close()
        }
        // A DIFFERENT advisory key is not the migration lock. MUTATION:
        // drop the classid/objid terms and any advisory lock reads as a
        // migration — the commit lock any writer holds would block every
        // rebuild.
        val other = db.jdbi.open()
        try {
            other.createQuery("SELECT pg_advisory_lock(${Database.MIGRATION_LOCK_KEY + 1})").mapToMap().one()
            assertThat(guarded(db).runOnce().index).isNotNull()
        } finally {
            other.createQuery("SELECT pg_advisory_unlock(${Database.MIGRATION_LOCK_KEY + 1})").mapToMap().one()
            other.close()
        }
    }

    @Test
    fun `migration_pending while Flyway's history holds a failed migration`() {
        val (db, _) = bloatedDb()
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO flyway_schema_history (installed_rank, version, description, type, script, " +
                    "checksum, installed_by, execution_time, success) " +
                    "VALUES (9999, '9999', 'half applied', 'SQL', 'V9999__x.sql', 0, 'test', 0, false)",
            )
        }
        assertSkipped(guarded(db).runOnce(), ReindexSkip.MIGRATION_PENDING)
    }

    @Test
    fun `reindex_lock_held closes the day while another session holds the reindex lock`() {
        val (db, _) = bloatedDb()
        var now = Instant.parse("2026-10-05T03:00:30Z")
        val other = db.jdbi.open()
        try {
            // The orphaned backend of a pod killed mid-rebuild, or a second
            // pod: this task's own lock, held by another session.
            assertThat(Locks.tryAcquireReindexLock(other)).isTrue()
            val skipped = ReindexService(db.jdbi, minRatioIndexBytes = 0, clock = { now }).pollOnce()!!
            // MUTATION: record REINDEX_IN_PROGRESS here and this reds.
            assertSkipped(skipped, ReindexSkip.REINDEX_LOCK_HELD)
        } finally {
            Locks.releaseReindexLock(other)
            other.close()
        }
        // The orphan has ended. Its rebuild WAS today's: a fresh instance
        // polling inside the retry window must not run a second one.
        // MUTATION: add REINDEX_LOCK_HELD to RETRYABLE_SKIPS and this reds.
        now = Instant.parse("2026-10-05T03:10:00Z")
        assertThat(ReindexService(db.jdbi, minRatioIndexBytes = 0, clock = { now }).pollOnce()).isNull()
        // The skipped run released nothing it did not take: a direct run
        // proceeds.
        assertThat(guarded(db).runOnce().index).isNotNull()
    }

    // BOUNDED, because the mutation this case exists for (dropping
    // leftovers before this guard) does not fail: it DEADLOCKS across the
    // test thread — the drop waits on the operator's REINDEX, which waits
    // on the writer this thread holds — and would sit out the hour-long
    // statement bound.
    @Test
    @Timeout(120)
    fun `reindex_in_progress while anything else is running a REINDEX, whose _ccnew must survive`() {
        val (db, _) = bloatedDb()
        // Created BEFORE the writer opens: its migrations build indexes
        // concurrently, and a concurrent build waits out older snapshots in
        // EVERY database of the cluster, the open writer's included.
        val neighbour = db()
        val operator = db.jdbi.open()
        try {
            val reindex =
                withOpenWriter(db) {
                    // An operator's REINDEX, parked behind the open writer
                    // with its `_ccnew` already committed and INVALID — the
                    // one invalid index the leftover drop must NOT touch.
                    val pending =
                        CompletableFuture.runAsync {
                            operator.execute("REINDEX INDEX CONCURRENTLY hog_data_file_path")
                        }
                    // The progress row appears before the `_ccnew` is
                    // committed, so wait for both: the build parked at
                    // "waiting for writers", copy on disk.
                    val deadline = Instant.now().plusSeconds(30)
                    while (Instant.now().isBefore(deadline) &&
                        !(inProgress(db) && "hog_data_file_path_ccnew" in invalidIndexes(db))
                    ) {
                        Thread.sleep(50)
                    }
                    assertThat(inProgress(db)).isTrue()
                    assertThat(invalidIndexes(db)).contains("hog_data_file_path_ccnew")

                    val result = guarded(db).runOnce()
                    assertSkipped(result, ReindexSkip.REINDEX_IN_PROGRESS)
                    // MUTATION: move the leftover drop ahead of this guard
                    // and the operator's build loses its index under it.
                    assertThat(result.invalidDropped).isZero()
                    assertThat(invalidIndexes(db)).contains("hog_data_file_path_ccnew")

                    // The boot-time log line (Database.migrate) names it.
                    val builds = db.dataSource.connection.use { Database.inFlightIndexBuilds(it) }
                    assertThat(builds).singleElement().asString().contains("hog_data_file")
                    // ...and migrate() actually logs it, before Flyway (a
                    // no-op migration here: the chain is applied).
                    // MUTATION: drop the logInFlightIndexBuilds call from
                    // migrate() and this reds.
                    val appender = ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>()
                    val logger =
                        org.slf4j.LoggerFactory.getLogger(Database::class.java.name) as ch.qos.logback.classic.Logger
                    appender.start()
                    logger.addAppender(appender)
                    try {
                        Database.migrate(db.dataSource)
                    } finally {
                        logger.detachAppender(appender)
                    }
                    assertThat(
                        appender.list.filter {
                            it.level == ch.qos.logback.classic.Level.WARN &&
                                it.formattedMessage.contains("an index build is in flight while migrating")
                        },
                    ).singleElement().extracting { it.formattedMessage }.asString().contains("hog_data_file_path")

                    // A DIFFERENT database in the same cluster sees no build:
                    // `pg_stat_progress_create_index` is cluster-wide.
                    // MUTATION: drop the datid filter from either statement
                    // and the matching assertion reds.
                    assertThat(inProgress(neighbour)).isFalse()
                    assertThat(neighbour.dataSource.connection.use { Database.inFlightIndexBuilds(it) }).isEmpty()
                    pending
                }
            // The writer is gone; the operator's REINDEX completes.
            reindex.get(60, TimeUnit.SECONDS)
        } finally {
            operator.close()
        }
        assertThat(indexValid(db, "hog_data_file_path")).isTrue()
        assertThat(invalidIndexes(db)).isEmpty()
    }

    private fun inProgress(db: PgTestSupport.TestDb): Boolean =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(ReindexService.INDEX_BUILD_IN_PROGRESS_SQL).mapTo(Boolean::class.javaObjectType).one()
        }

    // ---- (d) a no-op run ---------------------------------------------------

    @Test
    fun `a run with nothing over threshold still writes a ledger row, with only the counters`() {
        val db = db()
        catalog(db, "rx-quiet")
        db.jdbi.useHandleUnchecked { h -> h.execute("ANALYZE") }

        val result = ReindexService(db.jdbi).runOnce(MaintenanceTrigger.LOOP)
        assertThat(result.overThreshold).isZero()
        assertThat(result.index).isNull()
        assertThat(result.skippedReason).isNull()
        assertThat(result.checked).isPositive()

        val row = ledger(db).single()
        assertThat(row.status).isEqualTo("ok")
        // Exactly the three counters: the shape the console classifies as
        // quiet (webui isQuietRun).
        assertThat(row.result!!.fieldNames().asSequence().toSet())
            .containsExactlyInAnyOrder("checked", "over_threshold", "invalid_dropped")
    }

    // ---- the daily gate, through the ledger --------------------------------

    @Test
    fun `a run whose ledger row could not be written still runs once a day in this process`() {
        val db = db()
        catalog(db, "rx-unrecorded")
        // A ledger that refuses the row: recording is best-effort and
        // swallows the failure, so the gate cannot see the run.
        db.jdbi.useHandleUnchecked { h ->
            h.execute("ALTER TABLE hog_maintenance_run DROP CONSTRAINT hog_maintenance_run_task_check")
            h.execute(
                "ALTER TABLE hog_maintenance_run ADD CONSTRAINT hog_maintenance_run_task_check " +
                    "CHECK (task <> 'reindex')",
            )
        }
        var now = Instant.parse("2026-10-05T03:05:00Z")
        val service = ReindexService(db.jdbi, clock = { now })
        assertThat(service.pollOnce()).isNotNull()
        assertThat(ledger(db)).describedAs("the write was refused").isEmpty()
        // MUTATION: drop the in-process backstop and this second poll
        // runs (and would rebuild) again.
        now = Instant.parse("2026-10-05T03:10:00Z")
        assertThat(service.pollOnce()).isNull()
        now = Instant.parse("2026-10-06T03:00:00Z")
        assertThat(service.pollOnce()).describedAs("and tomorrow is a new day").isNotNull()
    }

    @Test
    fun `a run that stepped aside for a migration is retried on the next poll, until 06 00`() {
        val db = db()
        val id = catalog(db, "rx-retry")
        bloat(db, id, rows = 60_000)
        var now = Instant.parse("2026-10-05T03:00:30Z")
        val service = ReindexService(db.jdbi, minRatioIndexBytes = 0, clock = { now })
        val replica = db.jdbi.open()
        try {
            replica.createQuery("SELECT pg_advisory_lock(${Database.MIGRATION_LOCK_KEY})").mapToMap().one()
            assertThat(service.pollOnce()!!.skippedReason).isEqualTo(ReindexSkip.MIGRATION_PENDING.wire)
        } finally {
            replica.createQuery("SELECT pg_advisory_unlock(${Database.MIGRATION_LOCK_KEY})").mapToMap().one()
            replica.close()
        }
        // The boot's migration is done; the next poll runs the day's
        // rebuild. MUTATION: count a retryable skip as the day's run and
        // this reds — from the LEDGER, so a fresh instance (a restarted
        // pod) is asked too.
        now = Instant.parse("2026-10-05T03:05:00Z")
        val retried = ReindexService(db.jdbi, minRatioIndexBytes = 0, clock = { now }).pollOnce()
        assertThat(retried).isNotNull()
        assertThat(retried!!.index).isNotNull()
        now = Instant.parse("2026-10-05T03:10:00Z")
        assertThat(ReindexService(db.jdbi, minRatioIndexBytes = 0, clock = { now }).pollOnce()).isNull()

        // Past 06:00 a retryable skip closes the day.
        now = Instant.parse("2026-10-06T05:59:00Z")
        val late = db.jdbi.open()
        try {
            late.createQuery("SELECT pg_advisory_lock(${Database.MIGRATION_LOCK_KEY})").mapToMap().one()
            assertThat(service.pollOnce()!!.skippedReason).isEqualTo(ReindexSkip.MIGRATION_PENDING.wire)
        } finally {
            late.createQuery("SELECT pg_advisory_unlock(${Database.MIGRATION_LOCK_KEY})").mapToMap().one()
            late.close()
        }
        now = Instant.parse("2026-10-06T06:00:00Z")
        assertThat(ReindexService(db.jdbi, minRatioIndexBytes = 0, clock = { now }).pollOnce()).isNull()
    }

    @Test
    fun `the loop runs once a day at 03 00 UTC, survives a restart, and catches up a missed window`() {
        val db = db()
        db.jdbi.useHandleUnchecked { h -> h.execute("ANALYZE") }
        var now = Instant.parse("2026-10-05T02:55:00Z")

        fun service() = ReindexService(db.jdbi, clock = { now })

        // No catalog: nothing could record the run, so nothing runs.
        now = Instant.parse("2026-10-05T03:05:00Z")
        assertThat(service().pollOnce()).isNull()

        catalog(db, "rx-gate-a")
        catalog(db, "rx-gate-b")
        now = Instant.parse("2026-10-05T02:55:00Z")
        assertThat(service().pollOnce()).describedAs("before the window").isNull()
        assertThat(ledger(db)).isEmpty()

        now = Instant.parse("2026-10-05T03:05:00Z")
        assertThat(service().pollOnce()).isNotNull()
        assertThat(ledger(db)).hasSize(2)

        // A restart (a new instance) the same day: the ledger says done.
        now = Instant.parse("2026-10-05T07:00:00Z")
        assertThat(service().pollOnce()).isNull()

        // The pod was down at 03:00 on the 6th and back at 14:00: due.
        now = Instant.parse("2026-10-06T14:00:00Z")
        assertThat(service().pollOnce()).isNotNull()
        now = Instant.parse("2026-10-06T14:05:00Z")
        assertThat(service().pollOnce()).isNull()
        assertThat(ledger(db)).hasSize(4)
    }
}
