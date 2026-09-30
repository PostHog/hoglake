package com.posthog.hoglake.service

import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.Awaitility.await
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.useTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import java.time.Duration

/**
 * Cleanup drain semantics against a real MinIO: physical deletion is
 * always liveness-checked (a queue entry is a suggestion, never an
 * authorization), missing objects drain as success, still-referenced
 * paths are skipped with the object AND queue row surviving, and
 * sub-batches settle independently. Ends with the full lifecycle:
 * expire -> cleanup -> the queued objects are gone from S3.
 *
 * AND THE CLAIM (V21), which is the half that has no visible outcome of
 * its own: the drain is a claimed work queue that takes NO advisory lock
 * anywhere, so the cases below pin the lease and the fences directly —
 * that no run takes a lock at all, that workers partition the queue,
 * that a lapsed claim is reclaimed and a fresh one is not, and that a
 * worker whose claim moved under it cannot write the ledger. Every one
 * of those is invisible in `removed`/`missing`, which is why they are
 * asserted on the statement stream and on the row's claim columns rather
 * than on counters.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CleanupServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val svc by lazy { CleanupService(jdbi, removals) }

    /**
     * A drain with no staging grace, for the tests that seed a
     * `compaction_staging` ticket directly. The grace
     * (HOGLAKE_CLEANUP_STAGING_GRACE_SECONDS, 1 h) exists to keep the
     * drain off a ticket whose compaction group may still be uploading;
     * a test that seeds the ticket itself has no such group, and zeroing
     * the knob is how the fixture says so rather than sleeping an hour.
     */
    private val noGrace by lazy { CleanupService(jdbi, removals, stagingGraceSeconds = 0) }

    @AfterAll
    fun tearDown() = db.close()

    private companion object {
        /** The claim statement's `worker` binding, as the SqlLogger renders it. */
        val WORKER_BINDING = Regex("worker:([^,}]+)")

        const val BUCKET = "hoglake-cleanup"

        /** A second bucket, so a sub-batch can span two and prove the per-bucket chunking. */
        const val SECOND_BUCKET = "hoglake-cleanup-other"

        val minio: MinIOContainer by lazy {
            TestImages.minio().also { it.start() }
        }

        /** Read/put side (bucket bootstrap + object seeding). */
        val objects: ObjectStore by lazy {
            ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ).also { it.createBucket(BUCKET) }
        }

        /** Delete side under test. */
        val removals: RemovalStore by lazy {
            RemovalStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            )
        }
    }

    // ---- seeding -----------------------------------------------------------

    private fun seedCatalog(name: String): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "INSERT INTO hog_catalog (name, data_path) VALUES (:name, 's3://$BUCKET/') RETURNING catalog_id",
            )
                .bind("name", name)
                .mapTo(Long::class.java)
                .one()
        }

    private fun queue(
        catalogId: Long,
        path: String,
        kind: String = "data",
    ): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                VALUES (:catalogId, :path, :kind, 'snapshot_expiry')
                RETURNING removal_id
                """,
            )
                .bind("catalogId", catalogId)
                .bind("path", path)
                .bind("kind", kind)
                .mapTo(Long::class.java)
                .one()
        }

    /** A compaction staging ticket: the one reason with its own drain rules. */
    private fun stagingTicket(
        catalogId: Long,
        path: String,
    ): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                VALUES (:catalogId, :path, 'data', 'compaction_staging')
                RETURNING removal_id
                """,
            )
                .bind("catalogId", catalogId)
                .bind("path", path)
                .mapTo(Long::class.java)
                .one()
        }

    private fun putObject(path: String) = objects.put(path, "bytes".toByteArray())

    /**
     * Stamp a claim on a row, [ageSeconds] old — how another worker's
     * claim looks to this one, and the only way to make a LAPSED one
     * without waiting out a 900 s lease. The drain's own claim is a real
     * one; this is a fixture for the claims it has to reason about.
     */
    private fun claimRow(
        removalId: Long,
        worker: String,
        ageSeconds: Long = 0,
    ) = jdbi.useHandleUnchecked { h ->
        h.execute(
            "UPDATE hog_file_removal SET claimed_by = ?, " +
                "claimed_at = now() - make_interval(secs => ?) WHERE removal_id = ?",
            worker,
            ageSeconds.toDouble(),
            removalId,
        )
    }

    /** How many DISTINCT workers claimed in a recorded statement stream. */
    private fun workerCount(issued: List<String>): Int =
        issued
            .filter { it.contains("SET claimed_at = now()") }
            .mapNotNull { line -> WORKER_BINDING.find(line)?.groupValues?.get(1) }
            .distinct()
            .size

    /** The distinct worker ids that CLAIMED in a recorded statement stream. */
    private fun claimants(
        issued: List<String>,
        prefix: String,
    ): List<String> =
        issued
            .filter { it.contains("SET claimed_at = now()") }
            .flatMap { line -> Regex(Regex.escape(prefix) + "#\\d+").findAll(line).map { it.value } }
            .distinct()
            .sorted()

    /**
     * Every statement a drain issued, in order, as its rendered SQL and
     * its BINDING — the binding because the worker id rides a `?` and
     * which worker claimed what is half of what the concurrency cases
     * assert. The service is built on a Jdbi of its own so the logger
     * sees only this drain.
     */
    private fun recordingJdbi(
        issued: MutableList<String>,
        dataSource: javax.sql.DataSource = db.dataSource,
    ): org.jdbi.v3.core.Jdbi =
        com.posthog.hoglake.Database.jdbi(dataSource).also { j ->
            j.setSqlLogger(
                object : org.jdbi.v3.core.statement.SqlLogger {
                    override fun logAfterExecution(context: org.jdbi.v3.core.statement.StatementContext) {
                        issued += "${context.renderedSql} || bound=${context.binding}"
                    }
                },
            )
        }

    /** Paths still awaiting drain (soft-deleted ledger rows excluded). */
    private fun queuedPaths(catalogId: Long): List<String> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT path FROM hog_file_removal " +
                    "WHERE catalog_id = ? AND drained_at IS NULL ORDER BY removal_id",
            )
                .bind(0, catalogId).mapTo(String::class.java).list()
        }

    private data class LedgerRow(
        val path: String,
        val attempts: Int,
        val lastAttemptAt: java.time.OffsetDateTime?,
        val drainedAt: java.time.OffsetDateTime?,
        val drainedOutcome: String?,
        /** The claim (V21): who holds the row, and since when. */
        val claimedAt: java.time.OffsetDateTime?,
        val claimedBy: String?,
    )

    /** Every ledger row (drained or not), in queue order. */
    private fun ledgerRows(catalogId: Long): List<LedgerRow> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT path, attempts, last_attempt_at, drained_at, drained_outcome,
                       claimed_at, claimed_by
                FROM hog_file_removal WHERE catalog_id = ? ORDER BY removal_id
                """,
            )
                .bind(0, catalogId)
                .map { rs, _ ->
                    LedgerRow(
                        path = rs.getString("path"),
                        attempts = rs.getInt("attempts"),
                        lastAttemptAt = rs.getObject("last_attempt_at", java.time.OffsetDateTime::class.java),
                        drainedAt = rs.getObject("drained_at", java.time.OffsetDateTime::class.java),
                        drainedOutcome = rs.getString("drained_outcome"),
                        claimedAt = rs.getObject("claimed_at", java.time.OffsetDateTime::class.java),
                        claimedBy = rs.getString("claimed_by"),
                    )
                }
                .list()
        }

    // ---- tests -------------------------------------------------------------

    @Test
    fun `upload claims protect active PUTs and abandoned late PUTs remain reclaimable`() {
        val catalogId = seedCatalog("cl-uploads")
        val uploads = UploadService(jdbi)
        val owner = java.util.UUID.randomUUID()
        val claim = uploads.claim("cl-uploads", java.util.UUID.randomUUID(), owner, "s3://$BUCKET/cl-uploads", "data")
        putObject(claim.path)
        queue(catalogId, claim.path)
        assertThat(svc.runOnce("cl-uploads", 100).stillReferenced).isEqualTo(1)
        assertThat(uploads.abandon("cl-uploads", owner, listOf(claim.path))).isEqualTo(1)
        assertThat(svc.runOnce("cl-uploads", 100).removed).isEqualTo(1)
        // An already-running PUT can finish after the first DELETE. A later explicit
        // sweep revisits the permanent fence rather than assuming the first DELETE was final.
        putObject(claim.path)
        assertThat(uploads.scheduleExpired("cl-uploads")).isEqualTo(1)
        assertThat(svc.runOnce("cl-uploads", 100).removed).isEqualTo(1)
        assertThat(uploads.renew("cl-uploads", owner)).isZero()
    }

    @Test
    fun `two undrained rows over one path both settle deleted`() {
        // The other half of V16's non-unique argument. That migration
        // declines to make `(catalog_id, path) WHERE drained_at IS NULL`
        // unique because expiry can legitimately queue one path twice
        // (nothing makes a file path unique, and no writer carries
        // `ON CONFLICT`), and V16's own test proves the pair is
        // REACHABLE. This is what makes it HARMLESS, which is the claim
        // the migration actually rests on: the queue is a suggestion and
        // never an authorization, so the drain treats the second row as
        // an ordinary entry whose object is already gone.
        //
        // One sweep, one batch, so both rows are in the same sub-batch
        // and the second is settled by the first's DELETE — the
        // narrowest version of the race.
        val catalogId = seedCatalog("cl-dup")
        val path = "s3://$BUCKET/cl-dup/shared.parquet"
        putObject(path)
        val first = queue(catalogId, path)
        val second = queue(catalogId, path)

        val result = svc.runOnce("cl-dup", batchSize = 100)

        // Both rows settle 'deleted', where a per-object drain settled
        // the first 'deleted' and the second 'absent'. One batched
        // delete covers both, and it has no HEAD to tell a key it
        // removed from one that was never there — removing a missing key
        // IS the end state the queue asked for. What V16's argument
        // needs is unchanged, and it is what is asserted: two undrained
        // rows over one path are legitimate, both settle, neither is a
        // violation, and the object goes exactly once.
        assertThat(result.removed).describedAs("both rows settle over one delete").isEqualTo(2)
        assertThat(result.missing).describedAs("a batched delete never reports a miss").isZero()
        assertThat(result.stillReferenced)
            .describedAs("a duplicate is not an invariant violation")
            .isZero()
        assertThat(queuedPaths(catalogId)).describedAs("both rows settle").isEmpty()
        assertThat(removals.exists(path)).describedAs("the object is gone").isFalse()
        val rows = ledgerRows(catalogId)
        assertThat(rows.map { it.drainedOutcome })
            .describedAs("ledger, in queue order (removal_id %d then %d)", first, second)
            .containsExactly("deleted", "deleted")
        assertThat(rows).allSatisfy { assertThat(it.drainedAt).isNotNull() }
    }

    @Test
    fun `the files-removed metric counts objects, not ledger rows`() {
        // hoglake_files_removed_total is documented as physical S3
        // deletes, and `removed` stopped being that the moment the
        // deletes were batched: two undrained rows over one path are
        // legitimate state (nothing makes a file path unique — V16's
        // non-unique argument) and ONE batched delete settles both. Fed
        // from `removed`, the metric would report a duplicate queue row
        // as storage reclaimed twice.
        val catalogId = seedCatalog("cl-metric")
        val shared = "s3://$BUCKET/cl-metric/shared.parquet"
        val other = "s3://$BUCKET/cl-metric/other.parquet"
        putObject(shared)
        putObject(other)
        queue(catalogId, shared)
        queue(catalogId, shared)
        queue(catalogId, other)

        val registry = io.micrometer.core.instrument.simple.SimpleMeterRegistry()
        com.posthog.hoglake.observability.Metrics.bind(registry)
        try {
            val result = svc.runOnce("cl-metric", batchSize = 100)
            assertThat(result.removed).describedAs("three ledger rows settle").isEqualTo(3)
            assertThat(result.objectsRemoved).describedAs("two objects went").isEqualTo(2)
            assertThat(
                registry.get("hoglake_files_removed_total").tag("catalog", "cl-metric").counter().count(),
            )
                .describedAs("the metric is physical deletes; a duplicate row is not reclaimed storage")
                .isEqualTo(2.0)
        } finally {
            com.posthog.hoglake.observability.Metrics.clear()
            registry.close()
        }
    }

    @Test
    fun `happy drain deletes objects and empties the queue`() {
        val catalogId = seedCatalog("cl-happy")
        val paths = (1..3).map { "s3://$BUCKET/cl-happy/f$it.parquet" }
        paths.forEach {
            putObject(it)
            queue(catalogId, it)
        }

        val result = svc.runOnce("cl-happy", batchSize = 100)
        assertThat(result.removed).isEqualTo(3)
        assertThat(result.missing).isEqualTo(0)
        assertThat(result.stillReferenced).isEqualTo(0)
        assertThat(queuedPaths(catalogId)).isEmpty()
        paths.forEach { assertThat(removals.exists(it)).isFalse() }

        // Soft-delete: the rows SURVIVE as the ledger, marked drained.
        val ledger = ledgerRows(catalogId)
        assertThat(ledger).hasSize(3)
        for (row in ledger) {
            assertThat(row.drainedAt).isNotNull()
            assertThat(row.drainedOutcome).isEqualTo("deleted")
            assertThat(row.lastAttemptAt).isNotNull()
            assertThat(row.attempts).isEqualTo(0) // settled first touch: no failed attempts
        }
    }

    @Test
    fun `a sub-batch holding absent keys settles them deleted without failing`() {
        // DeleteObjects does not report a key that was not there, and
        // removing a missing key IS the end state the queue asked for,
        // so those rows settle alongside their siblings rather than
        // failing the sub-batch. The 'absent' outcome survives for
        // staging tickets (next test) and for rows settled before
        // batching landed.
        val catalogId = seedCatalog("cl-missing")
        val real = "s3://$BUCKET/cl-missing/real.parquet"
        putObject(real)
        queue(catalogId, real)
        queue(catalogId, "s3://$BUCKET/cl-missing/never-existed.parquet")
        queue(catalogId, "s3://$BUCKET/cl-missing/also-never.parquet")

        val result = svc.runOnce("cl-missing", batchSize = 100)
        assertThat(result.removed).isEqualTo(3)
        assertThat(result.missing).isZero()
        assertThat(result.stillReferenced).isZero()
        assertThat(queuedPaths(catalogId)).isEmpty()
        assertThat(ledgerRows(catalogId).map { it.drainedOutcome }).containsOnly("deleted")
        assertThat(removals.exists(real)).isFalse()
    }

    @Test
    fun `a staging ticket keeps the HEAD, so absent still reaches the ledger`() {
        // The carve-out, and why it exists: /verify's staging_tickets
        // check reads 'absent' (a staged path drained 'absent' that IS a
        // live file row is the staged-output race resolved the wrong
        // way), and a DeleteObjects response cannot produce that value.
        // So compaction_staging rows keep the HEAD + DELETE pair the
        // rest of the queue gave up — one row per compaction group, so
        // the two round trips stay affordable.
        val catalogId = seedCatalog("cl-staging-absent")
        val gone = "s3://$BUCKET/cl-staging-absent/never-uploaded.parquet"
        val landed = "s3://$BUCKET/cl-staging-absent/uploaded.parquet"
        putObject(landed)
        stagingTicket(catalogId, gone)
        stagingTicket(catalogId, landed)

        val result = noGrace.runOnce("cl-staging-absent", batchSize = 100)
        assertThat(result.removed).describedAs("the object that landed").isEqualTo(1)
        assertThat(result.missing).describedAs("the object that never did").isEqualTo(1)
        assertThat(ledgerRows(catalogId).map { it.path to it.drainedOutcome })
            .containsExactlyInAnyOrder(gone to "absent", landed to "deleted")
        assertThat(removals.exists(landed)).isFalse()
    }

    @Test
    fun `a fresh staging ticket is left alone until its grace expires`() {
        // The leak this closes: the ticket is inserted BEFORE the
        // rewrite starts, so on an empty queue a drain can settle it
        // while the group is still uploading — the group then aborts and
        // re-stages, and in between the object exists with no ticket
        // naming it. Rows of every other reason are untouched by the
        // grace, which is the half a `reason <> ...` mutation loses.
        val catalogId = seedCatalog("cl-staging-grace")
        val fresh = "s3://$BUCKET/cl-staging-grace/fresh.parquet"
        val ordinary = "s3://$BUCKET/cl-staging-grace/ordinary.parquet"
        putObject(fresh)
        putObject(ordinary)
        stagingTicket(catalogId, fresh)
        queue(catalogId, ordinary)

        val graced = CleanupService(jdbi, removals, stagingGraceSeconds = 3600)
        val result = graced.runOnce("cl-staging-grace", batchSize = 100)
        assertThat(result.removed).describedAs("only the ordinary row").isEqualTo(1)
        assertThat(queuedPaths(catalogId)).containsExactly(fresh)
        assertThat(removals.exists(fresh)).describedAs("the staged object survives").isTrue()

        // Age it past the grace and the same drain reclaims it.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_file_removal SET scheduled_at = now() - interval '2 hours' " +
                    "WHERE catalog_id = ? AND path = ?",
                catalogId,
                fresh,
            )
        }
        assertThat(graced.runOnce("cl-staging-grace", batchSize = 100).removed).isEqualTo(1)
        assertThat(removals.exists(fresh)).isFalse()
        assertThat(queuedPaths(catalogId)).isEmpty()
    }

    @Test
    fun `a ticket a compaction commit settled between the claim and the check is settled_elsewhere`() {
        // THE ONE RACE THE CLAIM CANNOT PREVENT, and it is a commit
        // doing its job. CompactionService's group commit settles its own
        // staging ticket 'registered' in the transaction that registers
        // the output path. It refuses to do that while an UNEXPIRED claim
        // holds the row (its re-read is `FOR UPDATE` and reads
        // claimed_at), so this can only happen to a claim whose lease
        // lapsed — a worker that was killed, or one that spent longer on
        // S3 than HOGLAKE_CLEANUP_CLAIM_LEASE_SECONDS. The fixture's
        // "commit" is that group: it registers the path and settles the
        // ticket exactly as `commitGroup` does, claim cleared and all.
        //
        // What must NOT follow is an alert. The reference check sees a
        // live file row for the path and calls it referenced, and all
        // four of these used to follow: an ERROR log, a
        // `cleanup_violation` audit event, a `still_referenced` count
        // that pages someone, and an attempts/last_attempt_at bump on a
        // settled row. The row is simply not this worker's any more, and
        // the FENCE on the attempts bump is the only thing that can tell
        // the two apart — which is why its RETURNING is what the
        // violation count is taken from.
        //
        // ORDER-INDEPENDENT BY CONSTRUCTION: the row under test is a
        // staging ticket placed behind a FULL staging sub-batch, so it is
        // in the second staging sub-batch whatever order the reasons are
        // worked in, and the commit fires from the first sub-batch's
        // first probe. Nothing here rests on bulk running before
        // staging.
        val catalogId = seedCatalog("cl-settled-elsewhere")
        val filler =
            (1..CleanupService.STAGING_SUB_BATCH).map {
                "s3://$BUCKET/cl-settled-elsewhere/filler$it.parquet"
            }
        filler.forEach {
            putObject(it)
            stagingTicket(catalogId, it)
        }
        val path = "s3://$BUCKET/cl-settled-elsewhere/staged.parquet"
        putObject(path)
        val removalId = stagingTicket(catalogId, path)

        // A second transaction plays the compaction commit: register the
        // path and settle the ticket together, once, from BEFORE the
        // reference check of the sub-batch that holds the row — the one
        // window a claim cannot close, because the claim has already
        // committed and the check has not yet read. (Firing it any earlier
        // no longer reaches the drain at all: the claim itself refuses a
        // settled row, which is why this is driven off the check's own
        // statement rather than off an object-store call.)
        val committed = java.util.concurrent.atomic.AtomicBoolean()
        val racing = com.posthog.hoglake.Database.jdbi(db.dataSource)
        racing.setSqlLogger(
            object : org.jdbi.v3.core.statement.SqlLogger {
                override fun logBeforeExecution(context: org.jdbi.v3.core.statement.StatementContext) {
                    if (!context.renderedSql.contains("SELECT path FROM hog_data_file")) return
                    if (!context.binding.toString().contains(path)) return
                    if (!committed.compareAndSet(false, true)) return
                    jdbi.useTransactionUnchecked { h ->
                        h.execute(
                            "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) " +
                                "VALUES (?, 1, 0)",
                            catalogId,
                        )
                        h.execute(
                            """
                            INSERT INTO hog_data_file (catalog_id, data_file_id, table_id,
                                begin_snapshot, path, record_count, file_size_bytes,
                                row_id_start)
                            VALUES (?, 1, 1, 1, ?, 10, 100, 0)
                            """,
                            catalogId,
                            path,
                        )
                        // commitGroup's settle, verbatim.
                        h.execute(
                            "UPDATE hog_file_removal SET drained_at = now(), " +
                                "drained_outcome = 'registered', last_attempt_at = now() " +
                                "WHERE removal_id = ?",
                            removalId,
                        )
                    }
                }
            },
        )

        val before = ledgerRows(catalogId).single { it.path == path }
        val result =
            withAuditCapture { capture ->
                val r =
                    CleanupService(racing, removals, stagingGraceSeconds = 0)
                        .runOnce("cl-settled-elsewhere", batchSize = 100)
                assertThat(capture.lines())
                    .describedAs("a committing compaction group is not an invariant violation")
                    .noneSatisfy { assertThat(it).contains("action=cleanup_violation") }
                r
            }

        assertThat(committed.get()).describedAs("the race must have been driven").isTrue()
        assertThat(result.stillReferenced)
            .describedAs("the settled ticket must not be counted as a violation")
            .isZero()
        assertThat(result.settledElsewhere).isEqualTo(1)
        assertThat(result.removed)
            .describedAs("every other ticket still drains")
            .isEqualTo(filler.size.toLong())
        val after = ledgerRows(catalogId).single { it.path == path }
        assertThat(after.drainedOutcome)
            .describedAs("the outcome the commit recorded stands")
            .isEqualTo("registered")
        assertThat(after.attempts).describedAs("no attempt was made on a settled row").isZero()
        assertThat(after.lastAttemptAt)
            .describedAs(
                "last_attempt_at is the COMMIT's own stamp (removal_id %d, before=%s): what must " +
                    "not have happened is a touch by this drain, which `attempts == 0` above is",
                removalId,
                before.lastAttemptAt,
            )
            .isNotNull()
        assertThat(removals.exists(path))
            .describedAs("the registered object is untouched")
            .isTrue()
    }

    @Test
    fun `a registered that lands after the deletes is refused AND reported as a violation`() {
        // The window the reference check cannot cover: a settle that
        // lands AFTER it, while this sub-batch is talking to S3. Both
        // ledger writes carry `drained_at IS NULL AND claimed_by =
        // :worker`, and unfenced each would write over another writer's
        // record — the ledger is the only record there is that the path
        // became a live catalog file.
        //
        // UNREACHABLE AS OF THIS CHANGE, and asserted anyway because it
        // is the only detector: `commitGroup` refuses any ticket cleanup
        // has TOUCHED (claimed or attempted), so a group cannot register a
        // path a drain is working. If that ever stops being true, the
        // shape is a live file row pointing at a deleted object, which no
        // other check in the system can see — every `staging_tickets` arm
        // passes once the file row exists. So the drain reads back the
        // rows its settle did not match and CLASSIFIES them: a
        // `'registered'` over a path THIS sub-batch deleted is an ERROR, a
        // `cleanup_violation` audit event and a counted invariant
        // violation, not ordinary contention.
        //
        // TWO tickets, because the row can leave a sub-batch by either
        // door: `settled` settles (SETTLE_SQL's fence) and `failed`
        // throws after the 'registered' lands, so it goes to the
        // attempts bump (BUMP_ATTEMPTS_SQL's fence) instead.
        val catalogId = seedCatalog("cl-settle-backstop")
        val settled = "s3://$BUCKET/cl-settle-backstop/settled.parquet"
        val failed = "s3://$BUCKET/cl-settle-backstop/failed.parquet"
        putObject(settled)
        putObject(failed)
        val settledId = stagingTicket(catalogId, settled)
        val failedId = stagingTicket(catalogId, failed)

        // TWO SHAPES OF OTHER WRITER, because the settle's two fence
        // terms catch different ones and a fixture that only produced
        // compaction's shape would leave `drained_at IS NULL` asserted by
        // nothing. `clearClaim = true` is `commitGroup` exactly (it clears
        // the claim it just judged expired), and `claimed_by = :worker` is
        // what refuses this drain afterwards. `clearClaim = false` is any
        // writer that settles the row and leaves the claim alone — an
        // operator's manual UPDATE, or a future writer that forgets — and
        // there `drained_at IS NULL` is the ONLY term that can refuse it.
        fun register(
            removalId: Long,
            clearClaim: Boolean,
        ) = jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_file_removal SET drained_at = now(), drained_outcome = 'registered'" +
                    (if (clearClaim) ", claimed_at = NULL, claimed_by = NULL " else " ") +
                    "WHERE removal_id = ? AND drained_at IS NULL",
                removalId,
            )
        }

        val racer =
            object : RemovalStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun deleteIfExists(pathUri: String): Outcome {
                    // After the reference check, before the ledger write.
                    if (pathUri == failed) {
                        register(failedId, clearClaim = true)
                        throw IllegalStateException("object store failed after the other writer settled")
                    }
                    val outcome = super.deleteIfExists(pathUri)
                    register(settledId, clearClaim = false)
                    return outcome
                }
            }
        val before = ledgerRows(catalogId).associateBy { it.path }

        val result =
            withAuditCapture { capture ->
                val r =
                    CleanupService(jdbi, racer, stagingGraceSeconds = 0)
                        .runOnce("cl-settle-backstop", batchSize = 100)
                assertThat(capture.lines())
                    .describedAs("the deleted-then-registered path must be named in the audit stream")
                    .anySatisfy {
                        assertThat(it)
                            .contains("action=cleanup_violation")
                            .contains("outcome=invariant_violation")
                            .contains("object=$settled")
                    }
                assertThat(capture.lines())
                    .describedAs("and the run's own event must carry the violation outcome")
                    .anySatisfy {
                        assertThat(it).contains("action=cleanup").contains("outcome=invariant_violation")
                    }
                r
            }

        assertThat(result.removed).describedAs("a row settled elsewhere is not counted").isZero()
        assertThat(result.settledElsewhere)
            .describedAs("both doors out of the sub-batch report the row as another writer's")
            .isEqualTo(2)
        assertThat(result.stillReferenced)
            .describedAs(
                "the row whose OBJECT this sub-batch deleted and whose ledger row came back " +
                    "'registered' is an invariant violation — a live file row for a deleted " +
                    "object — and it is the only signature that state has",
            )
            .isEqualTo(1)
        assertThat(result.objectsRemoved)
            .describedAs("the object did go, and that is counted separately from the ledger row")
            .isEqualTo(1)
        val after = ledgerRows(catalogId).associateBy { it.path }
        assertThat(after.values.map { it.drainedOutcome })
            .describedAs("the outcome that was already recorded stands, on both rows")
            .containsOnly("registered")
        assertThat(after.getValue(failed).attempts)
            .describedAs("a failed delete must not bump attempts on a row another writer settled")
            .isZero()
        assertThat(after.getValue(failed).lastAttemptAt)
            .describedAs("last_attempt_at must not move on a settled row")
            .isEqualTo(before.getValue(failed).lastAttemptAt)
    }

    @Test
    fun `a per-key delete error leaves only its own row queued`() {
        // S3 reports per-KEY failures inside an otherwise successful
        // DeleteObjects response, and no bucket can be asked to produce
        // one on demand — hence the store override, which is why
        // RemovalStore.deleteBatch is open (ObjectStore's reason
        // exactly). The contract is the one a per-object delete failure
        // always had: the failed row stays queued with attempts + 1,
        // its siblings settle, nothing wedges.
        val catalogId = seedCatalog("cl-perkey")
        val doomed = "s3://$BUCKET/cl-perkey/doomed.parquet"
        val fine = (1..3).map { "s3://$BUCKET/cl-perkey/fine$it.parquet" }
        (fine + doomed).forEach {
            putObject(it)
            queue(catalogId, it)
        }
        val flaky =
            object : RemovalStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun deleteBatch(paths: Collection<String>): Map<String, String> =
                    super.deleteBatch(paths.filter { it != doomed }) +
                        mapOf(doomed to "AccessDenied: simulated per-key failure")
            }

        val result = CleanupService(jdbi, flaky).runOnce("cl-perkey", batchSize = 100)

        assertThat(result.removed).isEqualTo(3)
        assertThat(result.missing).isZero()
        assertThat(queuedPaths(catalogId)).containsExactly(doomed)
        assertThat(removals.exists(doomed)).describedAs("its object is untouched").isTrue()
        fine.forEach { assertThat(removals.exists(it)).isFalse() }
        val row = ledgerRows(catalogId).single { it.path == doomed }
        assertThat(row.attempts).isEqualTo(1)
        assertThat(row.lastAttemptAt).isNotNull()
        assertThat(row.drainedAt).isNull()
    }

    @Test
    fun `a sub-batch issues one delete request per bucket chunk`() {
        // The headline property, and nothing else in this class pins it:
        // revert deleteBatch to a per-key loop and every other test here
        // stays green, because the OUTCOMES are identical and only the
        // round trips differ. Those round trips are the lock hold, which
        // is the change.
        //
        // 1,500 keys in one bucket + 10 in another, in ONE sub-batch:
        // two requests for the first bucket (S3 caps a request at 1,000
        // keys) and one for the second. The objects are deliberately not
        // uploaded — DeleteObjects does not care, and this test is about
        // request count, not bytes.
        val catalogId = seedCatalog("cl-chunks")
        objects.createBucket(SECOND_BUCKET)
        val many = (1..1_500).map { "s3://$BUCKET/cl-chunks/f$it.parquet" }
        val few = (1..10).map { "s3://$SECOND_BUCKET/cl-chunks/g$it.parquet" }
        jdbi.useHandleUnchecked { h ->
            val batch =
                h.prepareBatch(
                    "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason) " +
                        "VALUES (:c, :p, 'data', 'snapshot_expiry')",
                )
            (many + few).forEach { batch.bind("c", catalogId).bind("p", it).add() }
            batch.execute()
        }

        val requests = java.util.concurrent.CopyOnWriteArrayList<Pair<String, Int>>()
        val counting =
            object : RemovalStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun deleteChunk(
                    bucket: String,
                    chunk: List<Pair<String, String>>,
                ): Map<String, String> {
                    requests += bucket to chunk.size
                    return super.deleteChunk(bucket, chunk)
                }
            }

        val result =
            CleanupService(jdbi, counting, subBatchSize = 2_000)
                .runOnce("cl-chunks", batchSize = 2_000)

        assertThat(result.removed).isEqualTo(1_510)
        assertThat(requests)
            .describedAs("one request per bucket, per %d-key chunk", RemovalStore.MAX_KEYS_PER_DELETE)
            .containsExactlyInAnyOrder(BUCKET to 1_000, BUCKET to 500, SECOND_BUCKET to 10)
    }

    @Test
    fun `staging tickets settle in their own small sub-batches`() {
        // A compaction_staging row costs a HEAD and a DELETE (no batched
        // form reports 'absent'), so ONE claim of the default 1,000 could
        // hold 1,000 probe pairs — ~128 s at the measured 64 ms per round
        // trip. It is no longer a lock hold; it is a block of work whose
        // settle is all-or-nothing, and the grace CLUSTERS the rows,
        // because tickets become eligible in the order their groups ran.
        // So they settle 25 at a time, which is what this pins.
        //
        // The SUB-BATCH is the transaction, and it is observed from
        // outside it: a sub-batch's settles are invisible to another
        // connection until it commits, so the count of settled rows read
        // on a separate connection is constant within one and steps at
        // every boundary. That is the probe counter's reset.
        val catalogId = seedCatalog("cl-staging-holds")
        val tickets = (1..60).map { "s3://$BUCKET/cl-staging-holds/s$it.parquet" }
        tickets.forEach {
            putObject(it)
            stagingTicket(catalogId, it)
        }
        var lastSettled = -1L
        var probesThisHold = 0
        val holdSizes = mutableListOf<Int>()
        val counting =
            object : RemovalStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun deleteIfExists(pathUri: String): Outcome {
                    val settled =
                        jdbi.withHandleUnchecked { h ->
                            h.createQuery(
                                "SELECT count(*) FROM hog_file_removal " +
                                    "WHERE catalog_id = ? AND drained_at IS NOT NULL",
                            ).bind(0, catalogId).mapTo(Long::class.java).one()
                        }
                    if (settled != lastSettled) {
                        lastSettled = settled
                        probesThisHold = 0
                        holdSizes += 0
                    }
                    probesThisHold++
                    holdSizes[holdSizes.lastIndex] = probesThisHold
                    return super.deleteIfExists(pathUri)
                }
            }

        val result =
            CleanupService(jdbi, counting, subBatchSize = 1_000, stagingGraceSeconds = 0)
                .runOnce("cl-staging-holds", batchSize = 1_000)

        assertThat(result.removed).isEqualTo(60)
        assertThat(holdSizes)
            .describedAs(
                "60 tickets at %d per sub-batch: two FULL sub-batches and a remainder, and the " +
                    "full ones must be exactly the sub-batch — `<= 25` would also pass for a " +
                    "drain that probed one row per transaction",
                CleanupService.STAGING_SUB_BATCH,
            )
            .containsExactly(CleanupService.STAGING_SUB_BATCH, CleanupService.STAGING_SUB_BATCH, 10)
    }

    @Test
    fun `still-referenced path is never deleted - object and queue row survive`() {
        val catalogId = seedCatalog("cl-live")
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                catalogId,
            )
            // A HISTORICAL data-file row (end-snapshotted) still counts as a reference.
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    end_snapshot, path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 1, 1, 1, 2, 's3://$BUCKET/cl-live/df.parquet', 10, 100, 0)
                """,
                catalogId,
            )
            h.execute(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                    begin_snapshot, path, delete_count, file_size_bytes)
                VALUES (?, 10, 1, 1, 1, 's3://$BUCKET/cl-live/dv.puffin', 1, 10)
                """,
                catalogId,
            )
        }
        val dataPath = "s3://$BUCKET/cl-live/df.parquet"
        val dvPath = "s3://$BUCKET/cl-live/dv.puffin"
        putObject(dataPath)
        putObject(dvPath)
        queue(catalogId, dataPath, kind = "data")
        queue(catalogId, dvPath, kind = "delete")

        val result = svc.runOnce("cl-live", batchSize = 100)
        assertThat(result.removed).isEqualTo(0)
        assertThat(result.missing).isEqualTo(0)
        assertThat(result.stillReferenced).isEqualTo(2)
        // Nothing deleted, entries left for a later run (the reference may go away).
        assertThat(removals.exists(dataPath)).isTrue()
        assertThat(removals.exists(dvPath)).isTrue()
        assertThat(queuedPaths(catalogId)).containsExactly(dataPath, dvPath)
        // Each skip recorded an attempt; a second run records another.
        // And the CLAIM IS RELEASED: a row this run will not settle must
        // be visible to the next one and to an operator reading the
        // queue, not held for a lease it has no use for. (It is also what
        // makes the second run below bump attempts at all — a row still
        // claimed by the first would be skipped until the lease lapsed.)
        for (row in ledgerRows(catalogId)) {
            assertThat(row.attempts).isEqualTo(1)
            assertThat(row.lastAttemptAt).isNotNull()
            assertThat(row.drainedAt).isNull()
            assertThat(row.claimedAt).describedAs("the claim is released with the bump").isNull()
            assertThat(row.claimedBy).isNull()
        }
        svc.runOnce("cl-live", batchSize = 100)
        assertThat(ledgerRows(catalogId).map { it.attempts }).containsOnly(2)
        assertThat(ledgerRows(catalogId)).allSatisfy {
            assertThat(it.claimedBy).describedAs("released again by the second run").isNull()
        }
    }

    @Test
    fun `purge removes only drained rows older than the ledger retention`() {
        val catalogId = seedCatalog("cl-purge")
        // Three ledger states: an OLD drained row (past retention), a fresh
        // drained row, and an undrained entry (whose object is absent, so
        // this run drains it as 'absent').
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, drained_at, drained_outcome) " +
                    "VALUES (?, 's3://$BUCKET/cl-purge/old-drained', 'data', 'snapshot_expiry', " +
                    "now() - interval '2 hours', 'deleted')",
                catalogId,
            )
            h.execute(
                "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, drained_at, drained_outcome) " +
                    "VALUES (?, 's3://$BUCKET/cl-purge/fresh-drained', 'data', 'snapshot_expiry', " +
                    "now(), 'absent')",
                catalogId,
            )
        }
        queue(catalogId, "s3://$BUCKET/cl-purge/pending")

        // Retention of one hour: only the 2-hours-old drained row purges.
        val shortRetention = CleanupService(jdbi, removals, ledgerRetentionSeconds = 3600)
        val result = shortRetention.runOnce("cl-purge", batchSize = 100)
        // The pending entry settles 'deleted': its object was never
        // there, and a batched delete cannot tell that from a removal.
        assertThat(result.removed).isEqualTo(1)

        val paths = ledgerRows(catalogId).map { it.path }
        assertThat(paths).containsExactlyInAnyOrder(
            "s3://$BUCKET/cl-purge/fresh-drained",
            "s3://$BUCKET/cl-purge/pending",
        )
    }

    @Test
    fun `sub-batches commit independently - a skip in one never undoes another`() {
        val catalogId = seedCatalog("cl-subbatch")
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                catalogId,
            )
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 1, 1, 1, 's3://$BUCKET/cl-subbatch/c.parquet', 10, 100, 0)
                """,
                catalogId,
            )
        }
        // Sub-batches of 2 over [a, b, c(referenced), d, e]: [a,b] drains,
        // [c,d] drains d only, [e] drains.
        val paths = listOf("a", "b", "c", "d", "e").map { "s3://$BUCKET/cl-subbatch/$it.parquet" }
        paths.forEach {
            putObject(it)
            queue(catalogId, it)
        }

        val result =
            CleanupService(jdbi, removals, subBatchSize = 2)
                .runOnce("cl-subbatch", batchSize = 100)
        assertThat(result.removed).isEqualTo(4)
        assertThat(result.stillReferenced).isEqualTo(1)
        assertThat(queuedPaths(catalogId)).containsExactly("s3://$BUCKET/cl-subbatch/c.parquet")
        assertThat(removals.exists("s3://$BUCKET/cl-subbatch/c.parquet")).isTrue()
        for (p in paths - "s3://$BUCKET/cl-subbatch/c.parquet") {
            assertThat(removals.exists(p)).isFalse()
        }
    }

    @Test
    fun `an undeletable entry stays queued without wedging the rest`() {
        val catalogId = seedCatalog("cl-badpath")
        // Not an s3:// URI: the per-object delete throws, the row stays.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason) " +
                    "VALUES (?, 'file:///not-s3', 'data', 'table_drop_gc')",
                catalogId,
            )
        }
        val good = "s3://$BUCKET/cl-badpath/good.parquet"
        putObject(good)
        queue(catalogId, good)

        val result = svc.runOnce("cl-badpath", batchSize = 100)
        assertThat(result.removed).isEqualTo(1)
        assertThat(result.missing).isEqualTo(0)
        assertThat(result.stillReferenced).isEqualTo(0)
        assertThat(removals.exists(good)).isFalse()
        assertThat(queuedPaths(catalogId)).containsExactly("file:///not-s3")
        // The transient failure recorded an attempt on the surviving entry.
        val badRow = ledgerRows(catalogId).single { it.path == "file:///not-s3" }
        assertThat(badRow.attempts).isEqualTo(1)
        assertThat(badRow.drainedAt).isNull()
    }

    @Test
    fun `batch size bounds one run`() {
        val catalogId = seedCatalog("cl-batch")
        val paths = (1..3).map { "s3://$BUCKET/cl-batch/f$it.parquet" }
        paths.forEach {
            putObject(it)
            queue(catalogId, it)
        }

        val result = svc.runOnce("cl-batch", batchSize = 2)
        assertThat(result.removed).isEqualTo(2)
        assertThat(queuedPaths(catalogId)).containsExactly(paths[2]) // lowest removal_id first

        val rest = svc.runOnce("cl-batch", batchSize = 2)
        assertThat(rest.removed).isEqualTo(1)
        assertThat(queuedPaths(catalogId)).isEmpty()
    }

    @Test
    fun `a full run takes no advisory lock of any kind`() {
        // THE HEADLINE PROPERTY, and the only test that can see it.
        // Every other case here asserts OUTCOMES, and a drain that takes
        // the per-catalog commit lock produces identical outcomes — it
        // just pays its reference check, its DeleteObjects call and its
        // settle with the lock every commit on the catalog queues on
        // (~19 s per 1,000-row sub-batch measured on gigahog-prod-us,
        // which is why cleanup is off in production).
        //
        // The run is deliberately a MIXED one — bulk rows, a staging
        // ticket past its grace, a still-referenced row, and the ledger
        // purge — so every statement-issuing path in the class is
        // exercised inside the recording. A `pg_advisory_xact_lock`
        // anywhere in that stream fails this.
        val catalogId = seedCatalog("cl-no-lock")
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                catalogId,
            )
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 1, 1, 1, 's3://$BUCKET/cl-no-lock/live.parquet', 10, 100, 0)
                """,
                catalogId,
            )
        }
        val live = "s3://$BUCKET/cl-no-lock/live.parquet"
        val bulk = (1..3).map { "s3://$BUCKET/cl-no-lock/f$it.parquet" }
        val ticket = "s3://$BUCKET/cl-no-lock/staged.parquet"
        (bulk + live).forEach {
            putObject(it)
            queue(catalogId, it)
        }
        putObject(ticket)
        stagingTicket(catalogId, ticket)

        val issued = java.util.concurrent.CopyOnWriteArrayList<String>()
        val result =
            CleanupService(recordingJdbi(issued), removals, stagingGraceSeconds = 0)
                .runOnce("cl-no-lock", batchSize = 100)

        assertThat(result.removed).describedAs("the run did real work").isEqualTo(4)
        assertThat(result.stillReferenced).isEqualTo(1)
        assertThat(issued)
            .describedAs("the run must issue statements at all, or this asserts nothing")
            .isNotEmpty()
        assertThat(issued)
            .describedAs(
                "cleanup takes NO advisory lock — not the per-catalog commit lock, not a " +
                    "single-flight lock of its own:%n%s",
                issued.joinToString("\n---\n"),
            )
            .noneSatisfy { assertThat(it).contains("pg_advisory") }
        // And the claim is what replaced it: one UPDATE ... FOR UPDATE
        // SKIP LOCKED, whose own transaction ends before the first
        // object-store call. A mutation that dropped SKIP LOCKED would
        // turn concurrent workers back into a queue with nothing else
        // here noticing.
        assertThat(issued.filter { it.contains("FOR UPDATE SKIP LOCKED") })
            .describedAs("the claim partitions the queue instead of serializing on a lock")
            .isNotEmpty()
    }

    // ---- the claim (V21) ---------------------------------------------------

    @Test
    fun `concurrent workers partition the queue and settle every row exactly once`() {
        // WHAT SKIP LOCKED BUYS, and it is the reason there is no
        // single-flight advisory lock: two workers cannot claim the same
        // row, so they need no coordination at all. A row settled twice
        // would show up as `removed` exceeding the queue (the counter is
        // per settle, not per row), and a row NOT settled would show up
        // as a leftover in the queue — so the two assertions together are
        // "exactly once, and all of them".
        //
        // THE OVERLAP IS FORCED, not hoped for. Without the latch the
        // first worker can finish all eight claims before the executor
        // schedules the second, and the test would pass without ever
        // having run two workers at once (review C10's finding on the
        // predecessor of this test). Each worker's first delete counts
        // the latch down and then waits for the others, so all four hold
        // a claim simultaneously by construction.
        val catalogId = seedCatalog("cl-workers")
        val paths = (1..40).map { "s3://$BUCKET/cl-workers/f$it.parquet" }
        paths.forEach {
            putObject(it)
            queue(catalogId, it)
        }
        val workers = 4
        val arrived = java.util.concurrent.CountDownLatch(workers)
        val everyoneArrived = java.util.concurrent.atomic.AtomicBoolean()
        val waited = java.util.concurrent.ConcurrentHashMap.newKeySet<Long>()
        val rendezvous =
            object : RemovalStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun deleteBatch(paths: Collection<String>): Map<String, String> {
                    if (waited.add(Thread.currentThread().threadId())) {
                        arrived.countDown()
                        if (arrived.await(30, java.util.concurrent.TimeUnit.SECONDS)) {
                            everyoneArrived.set(true)
                        }
                    }
                    return super.deleteBatch(paths)
                }
            }

        val issued = java.util.concurrent.CopyOnWriteArrayList<String>()
        val result =
            CleanupService(
                recordingJdbi(issued),
                rendezvous,
                subBatchSize = 5,
                workers = workers,
                workerIdPrefix = "wk",
            ).runOnce("cl-workers", batchSize = 100)

        assertThat(everyoneArrived.get())
            .describedAs("all %d workers must have held a claim at the same time", workers)
            .isTrue()
        assertThat(result.removed)
            .describedAs("one settle per row: a row settled twice would count twice")
            .isEqualTo(paths.size.toLong())
        assertThat(result.settledElsewhere)
            .describedAs("no worker's settle was refused, because no two claimed one row")
            .isZero()
        assertThat(result.stillReferenced).isZero()
        assertThat(queuedPaths(catalogId)).describedAs("nothing was left behind").isEmpty()
        paths.forEach { assertThat(removals.exists(it)).isFalse() }
        assertThat(ledgerRows(catalogId)).allSatisfy {
            assertThat(it.drainedOutcome).isEqualTo("deleted")
            assertThat(it.attempts).describedAs("no row was touched twice").isZero()
            assertThat(it.claimedBy).describedAs("a settled row carries no claimant").isNull()
            assertThat(it.claimedAt).isNull()
        }
        // The queue really was PARTITIONED — several worker ids claimed —
        // rather than one worker draining it while three idled.
        val claimants =
            issued
                .filter { it.contains("SET claimed_at = now()") }
                .flatMap { Regex("""wk#\d+""").findAll(it).map { m -> m.value } }
                .distinct()
        assertThat(claimants)
            .describedAs("every worker claims under its OWN id:%n%s", claimants)
            .hasSize(workers)
    }

    @Test
    fun `two concurrent runs on one catalog drain it once between them`() {
        // The same property one level up: two `runOnce` calls (two
        // replicas, or a manual POST landing on top of the loop) partition
        // the queue instead of fighting over it. Neither reports a refused
        // settle, and between them they drain the queue exactly once.
        val catalogId = seedCatalog("cl-two-runs")
        val paths = (1..20).map { "s3://$BUCKET/cl-two-runs/f$it.parquet" }
        paths.forEach {
            putObject(it)
            queue(catalogId, it)
        }
        val arrived = java.util.concurrent.CountDownLatch(2)
        val bothArrived = java.util.concurrent.atomic.AtomicBoolean()
        val waited = java.util.concurrent.ConcurrentHashMap.newKeySet<Long>()
        val rendezvous =
            object : RemovalStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun deleteBatch(paths: Collection<String>): Map<String, String> {
                    if (waited.add(Thread.currentThread().threadId())) {
                        arrived.countDown()
                        if (arrived.await(30, java.util.concurrent.TimeUnit.SECONDS)) {
                            bothArrived.set(true)
                        }
                    }
                    return super.deleteBatch(paths)
                }
            }
        // Two SERVICES, so the two runs carry different worker ids exactly
        // as two pods would; a shared instance would still be two claims,
        // but it would not pin that the fence is per worker.
        val a = CleanupService(jdbi, rendezvous, subBatchSize = 5, workerIdPrefix = "runA")
        val b = CleanupService(jdbi, rendezvous, subBatchSize = 5, workerIdPrefix = "runB")
        val pool = java.util.concurrent.Executors.newFixedThreadPool(2)
        val results =
            try {
                listOf(
                    pool.submit<com.posthog.hoglake.model.CleanupResult> { a.runOnce("cl-two-runs", 100) },
                    pool.submit<com.posthog.hoglake.model.CleanupResult> { b.runOnce("cl-two-runs", 100) },
                ).map { it.get(60, java.util.concurrent.TimeUnit.SECONDS) }
            } finally {
                pool.shutdownNow()
            }

        assertThat(bothArrived.get()).describedAs("the two runs must have overlapped").isTrue()
        assertThat(results.sumOf { it.removed })
            .describedAs("every row settled, and none twice: %s", results)
            .isEqualTo(paths.size.toLong())
        assertThat(results).allSatisfy {
            assertThat(it.settledElsewhere).describedAs("no refused settle: %s", it).isZero()
            assertThat(it.stillReferenced).isZero()
            assertThat(it.removed).describedAs("both runs did work: %s", results).isPositive()
        }
        assertThat(queuedPaths(catalogId)).isEmpty()
        paths.forEach { assertThat(removals.exists(it)).isFalse() }
    }

    @Test
    fun `the batch is a budget PER WORKER, so workers multiply the run's rows`() {
        // THE THROUGHPUT ARITHMETIC, as code. The budget used to be ONE
        // counter for the run, handed out in sub-batch chunks, so
        // `HOGLAKE_CLEANUP_WORKERS` above `batch / subBatch` was dead
        // weight: four workers at the production batch and sub-batch left
        // two of them with nothing to claim, while the knob's own KDoc
        // promised N times the rate. Per worker, a run asks for
        // `workers x batch` rows, which is what the rows/hour in Config's
        // KDoc is derived from.
        //
        // The queue is deliberately TWICE what the run may take, so the
        // assertion is the budget and not the queue's size: every worker
        // gets full claims and stops on its own budget.
        val catalogId = seedCatalog("cl-per-worker-budget")
        val paths = (1..40).map { "s3://$BUCKET/cl-per-worker-budget/f$it.parquet" }
        paths.forEach {
            putObject(it)
            queue(catalogId, it)
        }

        val result =
            CleanupService(jdbi, removals, subBatchSize = 5, workers = 4, workerIdPrefix = "budget")
                .runOnce("cl-per-worker-budget", batchSize = 5)

        assertThat(result.removed)
            .describedAs("4 workers x a 5-row budget each, not one 5-row budget between them")
            .isEqualTo(20)
        assertThat(queuedPaths(catalogId))
            .describedAs("and the run stops there rather than draining the queue")
            .hasSize(20)
        assertThat(result.settledElsewhere).isZero()
        // A single worker at the same budget takes a quarter of it, which
        // is the other half of the arithmetic.
        val single =
            CleanupService(jdbi, removals, subBatchSize = 5, workers = 1, workerIdPrefix = "solo")
                .runOnce("cl-per-worker-budget", batchSize = 5)
        assertThat(single.removed).isEqualTo(5)
        assertThat(queuedPaths(catalogId)).hasSize(15)
    }

    @Test
    fun `a lapsed claim is reclaimed and a fresh one is left strictly alone`() {
        // THE LEASE, in both directions, and both halves are the same
        // predicate: `claimed_at IS NULL OR claimed_at < now() - lease`.
        // Drop it and a live worker's rows are stolen while it is
        // deleting them; invert it and a dead worker's rows are never
        // reclaimed at all. The fixture stamps the two claims itself,
        // which is the only way to have a 900 s lease lapse inside a
        // test.
        val catalogId = seedCatalog("cl-lease")
        val lapsed = "s3://$BUCKET/cl-lease/lapsed.parquet"
        val fresh = "s3://$BUCKET/cl-lease/fresh.parquet"
        putObject(lapsed)
        putObject(fresh)
        val lapsedId = queue(catalogId, lapsed)
        val freshId = queue(catalogId, fresh)
        claimRow(lapsedId, "dead-worker", ageSeconds = 2 * CleanupService.CLAIM_LEASE_SECONDS)
        claimRow(freshId, "live-worker", ageSeconds = 0)
        val before = ledgerRows(catalogId).single { it.path == fresh }

        val result = CleanupService(jdbi, removals).runOnce("cl-lease", batchSize = 100)

        assertThat(result.removed).describedAs("the dead worker's row, and only it").isEqualTo(1)
        assertThat(result.settledElsewhere).isZero()
        assertThat(removals.exists(lapsed)).isFalse()
        assertThat(ledgerRows(catalogId).single { it.path == lapsed }.drainedOutcome)
            .isEqualTo("deleted")

        val after = ledgerRows(catalogId).single { it.path == fresh }
        assertThat(removals.exists(fresh))
            .describedAs("a live worker is presumed to be deleting its own object")
            .isTrue()
        assertThat(after.drainedAt).describedAs("and its row is not settled by us").isNull()
        assertThat(after.attempts).describedAs("nor touched: no attempt was made on it").isZero()
        assertThat(after.claimedBy).describedAs("the claim stands, unchanged").isEqualTo("live-worker")
        assertThat(after.claimedAt).isEqualTo(before.claimedAt)
        assertThat(queuedPaths(catalogId)).containsExactly(fresh)

        // Age that claim past the lease and the very same drain takes it.
        claimRow(freshId, "live-worker", ageSeconds = 2 * CleanupService.CLAIM_LEASE_SECONDS)
        assertThat(CleanupService(jdbi, removals).runOnce("cl-lease", batchSize = 100).removed)
            .isEqualTo(1)
        assertThat(removals.exists(fresh)).isFalse()
        assertThat(queuedPaths(catalogId)).isEmpty()
    }

    @Test
    fun `a worker whose claim was taken from it cannot settle the row`() {
        // THE OTHER HALF OF THE LEASE. A worker that spends longer on S3
        // than the lease loses the row to whoever reclaims it — and must
        // not then stamp its own outcome over that worker's. `claimed_by =
        // :worker` on the settle is the fence; without it the ledger would
        // say this drain settled a row it no longer held, which is the
        // same class of lie as overwriting a compaction group's
        // 'registered'.
        //
        // The theft happens from inside the delete, i.e. after the claim
        // and before the settle, which is exactly the window a lapsed
        // lease opens.
        val catalogId = seedCatalog("cl-stolen")
        val path = "s3://$BUCKET/cl-stolen/f.parquet"
        putObject(path)
        val removalId = queue(catalogId, path)
        val thief =
            object : RemovalStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun deleteBatch(paths: Collection<String>): Map<String, String> {
                    val failures = super.deleteBatch(paths)
                    claimRow(removalId, "another-worker", ageSeconds = 0)
                    return failures
                }
            }

        val result = CleanupService(jdbi, thief).runOnce("cl-stolen", batchSize = 100)

        assertThat(result.removed).describedAs("the settle was refused, so nothing is counted").isZero()
        assertThat(result.settledElsewhere)
            .describedAs("and the row is reported as somebody else's")
            .isEqualTo(1)
        assertThat(result.objectsRemoved)
            .describedAs(
                "the object did go — that is counted separately, and it is why the re-drain " +
                    "settles it rather than failing",
            )
            .isEqualTo(1)
        assertThat(result.stillReferenced).isZero()
        val row = ledgerRows(catalogId).single()
        assertThat(row.drainedAt).describedAs("undrained: the new claimant settles it").isNull()
        assertThat(row.claimedBy).isEqualTo("another-worker")
        assertThat(row.attempts).describedAs("a refused settle is not an attempt bump either").isZero()
        assertThat(removals.exists(path)).isFalse()
    }

    @Test
    fun `a worker killed after its deletes leaves the rows claimed until the lease expires`() {
        // THE CRASH THE LEASE EXISTS FOR. The claim commits before the
        // first delete and the settle is a later transaction, so a worker
        // that dies in between leaves rows undrained, claimed, and with
        // their objects already gone. Nothing may touch them until the
        // lease lapses — and when it does, the re-drain must settle them
        // without an error, because `DeleteObjects` reports a key that was
        // never there exactly like one it removed.
        //
        // (A bulk row settles 'deleted' on that re-drain, not 'absent':
        // only the `compaction_staging` path keeps the HEAD that can tell
        // the two apart — see the staging-ticket case above, which is
        // where 'absent' is pinned.)
        val catalogId = seedCatalog("cl-crash")
        val paths = (1..3).map { "s3://$BUCKET/cl-crash/f$it.parquet" }
        paths.forEach {
            putObject(it)
            queue(catalogId, it)
        }
        val killed =
            object : RemovalStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun deleteBatch(paths: Collection<String>): Map<String, String> {
                    super.deleteBatch(paths)
                    throw IllegalStateException("the pod went away between the delete and the settle")
                }
            }

        assertThatThrownBy {
            CleanupService(jdbi, killed, workerIdPrefix = "doomed").runOnce("cl-crash", batchSize = 100)
        }
            .isInstanceOf(IllegalStateException::class.java)

        paths.forEach { assertThat(removals.exists(it)).describedAs("%s", it).isFalse() }
        assertThat(ledgerRows(catalogId)).allSatisfy {
            assertThat(it.drainedAt).describedAs("nothing settled").isNull()
            assertThat(it.claimedBy).describedAs("the claim outlives the worker").startsWith("doomed")
            assertThat(it.attempts).isZero()
        }

        // A run inside the lease must not touch them: the dead worker is
        // indistinguishable from a slow one until the lease says otherwise.
        val tooSoon = CleanupService(jdbi, removals).runOnce("cl-crash", batchSize = 100)
        assertThat(tooSoon.removed).isZero()
        assertThat(tooSoon.missing).isZero()
        assertThat(ledgerRows(catalogId)).allSatisfy {
            assertThat(it.drainedAt).isNull()
            assertThat(it.claimedBy).startsWith("doomed")
        }

        // Past the lease, a healthy drain reclaims and settles them, with
        // no error and no failed delete over the already-absent keys.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_file_removal SET claimed_at = now() - make_interval(secs => ?) " +
                    "WHERE catalog_id = ?",
                (2 * CleanupService.CLAIM_LEASE_SECONDS).toDouble(),
                catalogId,
            )
        }
        val reclaimed = CleanupService(jdbi, removals).runOnce("cl-crash", batchSize = 100)
        assertThat(reclaimed.removed).isEqualTo(paths.size.toLong())
        assertThat(reclaimed.settledElsewhere).isZero()
        assertThat(reclaimed.stillReferenced).isZero()
        assertThat(queuedPaths(catalogId)).isEmpty()
        assertThat(ledgerRows(catalogId)).allSatisfy {
            assertThat(it.drainedOutcome).isEqualTo("deleted")
            assertThat(it.attempts).describedAs("a re-drain of a gone object is not a failure").isZero()
            assertThat(it.claimedBy).isNull()
        }
    }

    @Test
    fun `a worker whose claim was taken cannot bump attempts or clear the new claim`() {
        // THE BUMP'S FENCE, and the half the theft test cannot reach. That
        // test steals the claim on the SUCCESS path, so the row goes to
        // SETTLE_SQL; this one steals it on the SKIP path, where the row
        // goes to BUMP_ATTEMPTS_SQL instead. The bump does not merely
        // record an attempt — it RELEASES the claim — so unfenced it would
        // clear the live claim of the worker that reclaimed the row and
        // hand the same path to a third claimer while the second is
        // deleting it. That is the failure the lease exists to prevent,
        // reached through the release rather than the settle.
        //
        // The seam is the reference check: by the time it answers, the
        // claim has moved.
        val catalogId = seedCatalog("cl-stolen-skip")
        val path = "s3://$BUCKET/cl-stolen-skip/live.parquet"
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                catalogId,
            )
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 1, 1, 1, ?, 10, 100, 0)
                """,
                catalogId,
                path,
            )
        }
        putObject(path)
        val removalId = queue(catalogId, path)

        // Steal the claim from inside the reference check: the statement
        // is the drain's own, and the theft lands after the claim and
        // before the bump.
        val stolen = java.util.concurrent.atomic.AtomicBoolean()
        val racing = com.posthog.hoglake.Database.jdbi(db.dataSource)
        racing.setSqlLogger(
            object : org.jdbi.v3.core.statement.SqlLogger {
                override fun logAfterExecution(context: org.jdbi.v3.core.statement.StatementContext) {
                    if (!context.renderedSql.contains("SELECT path FROM hog_data_file")) return
                    if (!stolen.compareAndSet(false, true)) return
                    claimRow(removalId, "another-worker", ageSeconds = 0)
                }
            },
        )

        val result =
            withAuditCapture { capture ->
                val r =
                    CleanupService(racing, removals, workerIdPrefix = "loser")
                        .runOnce("cl-stolen-skip", batchSize = 100)
                assertThat(capture.lines())
                    .describedAs("a row this worker no longer holds is not its violation to report")
                    .noneSatisfy { assertThat(it).contains("action=cleanup_violation") }
                r
            }

        assertThat(stolen.get()).describedAs("the theft must have been driven").isTrue()
        assertThat(result.stillReferenced)
            .describedAs("the fence refused the bump, so there is no violation to count")
            .isZero()
        assertThat(result.settledElsewhere)
            .describedAs("the row is reported as somebody else's instead")
            .isEqualTo(1)
        assertThat(result.removed).isZero()
        val row = ledgerRows(catalogId).single()
        assertThat(row.attempts).describedAs("the fence refused the bump").isZero()
        assertThat(row.claimedBy)
            .describedAs("and the bump must NOT clear the claim of the worker that took the row")
            .isEqualTo("another-worker")
        assertThat(row.claimedAt)
            .describedAs("nor its timestamp — the new lease still runs")
            .isNotNull()
        assertThat(removals.exists(path))
            .describedAs("the referenced object is untouched throughout")
            .isTrue()
    }

    @Test
    fun `a claim SKIPS the rows another claim holds instead of waiting for them`() {
        // THE PREDICATE THE WHOLE "no single-flight lock" ARGUMENT RESTS
        // ON, and EXPLAIN cannot see it: the plan of `FOR UPDATE` and of
        // `FOR UPDATE SKIP LOCKED` are byte-identical (`LockRows` in both),
        // because the wait policy is not a plan property. So the skip is
        // driven: a second connection holds one candidate row, and a
        // `lock_timeout` on the claiming connection turns "waits" into an
        // observable failure. Without SKIP LOCKED this statement raises
        // 55P03 instead of returning the rows it could lock.
        val catalogId = seedCatalog("cl-skip-locked")
        val ids = (1..4).map { queue(catalogId, "s3://$BUCKET/cl-skip-locked/f$it.parquet") }
        val held = ids.first()

        val holder = jdbi.open()
        try {
            holder.begin()
            holder.createQuery("SELECT removal_id FROM hog_file_removal WHERE removal_id = ? FOR UPDATE")
                .bind(0, held).mapTo(Long::class.javaObjectType).one()

            val taken =
                jdbi.withHandleUnchecked { h ->
                    // SET LOCAL inside a transaction, not SET: this handle
                    // goes back to a pool of eight that the rest of the
                    // class borrows from, and a session-scoped
                    // `lock_timeout` would ride it into every later test —
                    // the `is_local` hazard, in a test rather than in
                    // production code.
                    h.begin()
                    try {
                        h.execute("SET LOCAL lock_timeout = '2s'")
                        h.createQuery(CleanupService.CLAIM_CANDIDATE_SQL)
                            .bind("catalogId", catalogId)
                            .bind("limit", 3)
                            .bind("leaseSeconds", CleanupService.CLAIM_LEASE_SECONDS.toDouble())
                            .mapTo(Long::class.javaObjectType)
                            .list()
                    } finally {
                        h.rollback()
                    }
                }
            assertThat(taken)
                .describedAs(
                    "the held row is SKIPPED, not waited for — which is why concurrent workers " +
                        "need no coordination and no single-flight lock",
                )
                .doesNotContain(held)
            assertThat(taken)
                .describedAs("and the skip does not consume the LIMIT: three other rows came back")
                .hasSize(3)
                .containsExactlyElementsOf(ids.drop(1))
        } finally {
            if (holder.isInTransaction) holder.rollback()
            holder.close()
        }
    }

    @Test
    fun `a partly deleted sub-batch leaves the rest claimed, and the re-drain settles it whole`() {
        // The crash the previous test could not produce: a bulk sub-batch
        // is one `DeleteObjects` call PER BUCKET CHUNK, so a throw out of
        // the SECOND chunk leaves some objects gone, some present, and
        // every row claimed and unsettled. What has to hold is that the
        // re-drain after the lease does not care which is which —
        // `DeleteObjects` reports a key that was never there exactly like
        // one it removed, so the whole sub-batch settles 'deleted' with no
        // failure and no attempts bump.
        val catalogId = seedCatalog("cl-crash-partial")
        objects.createBucket(SECOND_BUCKET)
        val first = (1..3).map { "s3://$BUCKET/cl-crash-partial/f$it.parquet" }
        val second = (1..3).map { "s3://$SECOND_BUCKET/cl-crash-partial/g$it.parquet" }
        (first + second).forEach {
            putObject(it)
            queue(catalogId, it)
        }
        val chunks = java.util.concurrent.atomic.AtomicInteger()
        val killed =
            object : RemovalStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun deleteChunk(
                    bucket: String,
                    chunk: List<Pair<String, String>>,
                ): Map<String, String> {
                    if (chunks.incrementAndGet() > 1) {
                        throw IllegalStateException("the pod went away between two chunks")
                    }
                    return super.deleteChunk(bucket, chunk)
                }
            }

        assertThatThrownBy {
            CleanupService(jdbi, killed, workerIdPrefix = "doomed").runOnce("cl-crash-partial", 100)
        }
            .isInstanceOf(IllegalStateException::class.java)

        val gone = (first + second).filter { !removals.exists(it) }
        assertThat(gone)
            .describedAs("exactly one bucket's chunk went; the other never was attempted")
            .hasSize(3)
        assertThat(ledgerRows(catalogId)).allSatisfy {
            assertThat(it.drainedAt).describedAs("nothing settled").isNull()
            assertThat(it.claimedBy).describedAs("every row is still the dead worker's").startsWith("doomed")
            assertThat(it.attempts).isZero()
        }

        // Past the lease a healthy drain settles the whole sub-batch,
        // including the three keys that are already gone.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_file_removal SET claimed_at = now() - make_interval(secs => ?) " +
                    "WHERE catalog_id = ?",
                (2 * CleanupService.CLAIM_LEASE_SECONDS).toDouble(),
                catalogId,
            )
        }
        val reclaimed = CleanupService(jdbi, removals).runOnce("cl-crash-partial", 100)
        assertThat(reclaimed.removed).isEqualTo(6)
        assertThat(reclaimed.settledElsewhere).isZero()
        assertThat(queuedPaths(catalogId)).isEmpty()
        assertThat(ledgerRows(catalogId)).allSatisfy {
            assertThat(it.drainedOutcome).isEqualTo("deleted")
            assertThat(it.attempts).describedAs("a re-drain over a mixed batch is not a failure").isZero()
        }
        (first + second).forEach { assertThat(removals.exists(it)).isFalse() }
    }

    @Test
    fun `a worker's failure reaches the run as itself, at one worker and at four`() {
        // THE TWO WORKER COUNTS FAIL DIFFERENTLY unless the wrapper is
        // unwrapped: `workers == 1` runs inline and throws the store's
        // exception, `workers > 1` goes through `invokeAll(...).get()` and
        // would throw an ExecutionException wrapping it. The run ledger's
        // `error`, the audit event's detail and the manual POST's status
        // all read that exception, so which one an operator sees must not
        // depend on a knob.
        val catalogId = seedCatalog("cl-worker-failure")
        (1..8).forEach {
            val p = "s3://$BUCKET/cl-worker-failure/f$it.parquet"
            putObject(p)
            queue(catalogId, p)
        }
        val exploding =
            object : RemovalStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun deleteBatch(paths: Collection<String>): Map<String, String> =
                    throw IllegalStateException("object store is on fire")
            }

        for (workers in listOf(1, 4)) {
            assertThatThrownBy {
                CleanupService(jdbi, exploding, subBatchSize = 2, workers = workers)
                    .runOnce("cl-worker-failure", batchSize = 2)
            }
                .describedAs("at %d worker(s) the cause must arrive as itself", workers)
                .isInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("object store is on fire")
        }
    }

    @Test
    fun `the ledger purge walks PAST an un-purgeable window, across catalogs`() {
        // THE SHAPE THAT CAUSED THE 2026-09-28 OUTAGE, removed — and the
        // second-order version of it, removed too. The purge used to be
        // one `DELETE ... WHERE catalog_id = :c AND drained_at < cutoff`,
        // which no index on this table can serve (both are partial on
        // `drained_at IS NULL`, the complement of what it deletes), so it
        // was a sequential scan with an unbounded row count against a
        // ledger heading for ~137M rows.
        //
        // It now pages the PRIMARY KEY with no `catalog_id` — a per-catalog
        // window would bound the rows DELETED without bounding the rows
        // READ — and that global window is why a page with nothing to purge
        // is SKIPPED rather than treated as the end of the walk: stopping
        // there would park EVERY catalog's retention behind ONE catalog's
        // un-purgeable rows (a tenant whose drain is wedged holds a block
        // of undrained ids, and nothing in the deployment would purge
        // again). What bounds the skipping is the wall budget.
        //
        // ITS OWN DATABASE, because the walk's stopping point is a
        // property of the whole table and sharing this class's database
        // would make the assertion depend on every other test's rows.
        PgTestSupport.freshDatabase().use { own ->
            fun catalog(name: String): Long =
                own.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "INSERT INTO hog_catalog (name, data_path) VALUES (:n, 's3://$BUCKET/') " +
                            "RETURNING catalog_id",
                    ).bind("n", name).mapTo(Long::class.java).one()
                }

            val a = catalog("cl-purge-a")
            val b = catalog("cl-purge-b")

            fun insert(
                catalogId: Long,
                name: String,
                drained: Boolean,
            ) = own.jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, " +
                        "drained_at, drained_outcome) VALUES (:c, :p, 'data', 'snapshot_expiry', " +
                        "CASE WHEN :drained THEN now() - interval '40 days' END, " +
                        "CASE WHEN :drained THEN 'deleted' END)",
                )
                    .bind("c", catalogId)
                    .bind("p", "s3://$BUCKET/purge/$name")
                    .bind("drained", drained)
                    .execute()
            }

            fun remaining(): List<String> =
                own.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT path FROM hog_file_removal ORDER BY removal_id")
                        .mapTo(String::class.java)
                        .list()
                        .map { it.substringAfterLast('/') }
                }

            // Catalog A's expired rows, then a wedged tenant's block of
            // UNDRAINED rows in the middle of the key, then catalog B's
            // expired rows above them — and one of B's that is NOT expired.
            (1..2).forEach { insert(a, "a-old$it", drained = true) }
            (1..3).forEach { insert(b, "b-wedged$it", drained = false) }
            (1..2).forEach { insert(b, "b-old$it", drained = true) }
            own.jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, " +
                        "drained_at, drained_outcome) VALUES (:c, :p, 'data', 'snapshot_expiry', " +
                        "now(), 'deleted')",
                ).bind("c", b).bind("p", "s3://$BUCKET/purge/b-fresh").execute()
            }

            // A page of ONE, so every un-purgeable row is a page that
            // purges nothing: the walk has to skip three of them in a row
            // to reach what is above.
            CleanupService(own.jdbi, removals, ledgerPurgePage = 1)
                .runOnce("cl-purge-a", batchSize = 100)

            assertThat(remaining())
                .describedAs(
                    "the wedged rows survive (undrained), the FRESH drained row survives " +
                        "(inside retention), and everything expired below AND ABOVE the wedge is " +
                        "purged — the walk skipped the un-purgeable window instead of parking on it",
                )
                .containsExactly("b-wedged1", "b-wedged2", "b-wedged3", "b-fresh")

            // AND THE SCOPE IS THE INSTANCE, not the catalog the run was
            // for: this run was `cl-purge-a`, and catalog B's expired rows
            // went with A's. That is the design (a per-catalog page bounds
            // deletions and not work) and it is what the endpoint's
            // OpenAPI description now tells an operator.
            val survivorsByCatalog =
                own.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "SELECT c.name, count(*) FROM hog_file_removal r " +
                            "JOIN hog_catalog c USING (catalog_id) GROUP BY c.name ORDER BY c.name",
                    ).map { rs, _ -> rs.getString(1) to rs.getLong(2) }.list()
                }
            assertThat(survivorsByCatalog)
                .describedAs("a run for catalog A purged catalog B's expired ledger rows too")
                .containsExactly("cl-purge-b" to 4L)
        }
    }

    @Test
    fun `a wedge wider than the cap stops the walk, a narrower one is walked past`() {
        // THE CAP IS ABOUT DAY ONE, not about wedges. The cursor restarts
        // at the bottom of the primary key every run, and on a queue that
        // is behind, the bottom is the UNDRAINED backlog — 2.6M rows on
        // gigahog-prod-us, ~2,600 pages with nothing to purge. Uncapped,
        // the walk spends its whole wall budget on them EVERY run and
        // never reaches a purgeable row; capped, it gives up after
        // LEDGER_PURGE_EMPTY_PAGES and says so.
        //
        // What the cap must NOT cost is the wedge-skip the walk exists
        // for, so both sides are asserted against the same fixture shape:
        // a block of un-purgeable rows with purgeable rows above it, once
        // wider than the cap and once narrower.
        fun run(
            wedge: Int,
            label: String,
        ): List<String> {
            var remaining = listOf<String>()
            PgTestSupport.freshDatabase().use { own ->
                val catalogId =
                    own.jdbi.withHandleUnchecked { h ->
                        h.createQuery(
                            "INSERT INTO hog_catalog (name, data_path) VALUES (:n, 's3://$BUCKET/') " +
                                "RETURNING catalog_id",
                        ).bind("n", label).mapTo(Long::class.java).one()
                    }
                own.jdbi.useHandleUnchecked { h ->
                    // One purgeable row at the bottom so the walk starts,
                    // then the wedge, then two purgeable rows above it.
                    h.createUpdate(
                        """
                        INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason,
                                                      drained_at, drained_outcome)
                        SELECT :c, 's3://$BUCKET/$label/below', 'data', 'snapshot_expiry',
                               now() - interval '40 days', 'deleted'
                        """,
                    ).bind("c", catalogId).execute()
                    h.createUpdate(
                        """
                        INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                        SELECT :c, 's3://$BUCKET/$label/wedge' || g, 'data', 'snapshot_expiry'
                        FROM generate_series(1, :n) g
                        """,
                    ).bind("c", catalogId).bind("n", wedge).execute()
                    h.createUpdate(
                        """
                        INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason,
                                                      drained_at, drained_outcome)
                        SELECT :c, 's3://$BUCKET/$label/above' || g, 'data', 'snapshot_expiry',
                               now() - interval '40 days', 'deleted'
                        FROM generate_series(1, 2) g
                        """,
                    ).bind("c", catalogId).execute()
                }
                // A page of one, so one un-purgeable row is one page with
                // nothing to purge and the cap is reached at exactly
                // LEDGER_PURGE_EMPTY_PAGES of them.
                CleanupService(own.jdbi, removals, ledgerPurgePage = 1)
                    .runOnce(label, batchSize = 100)
                // THE ORDER THIS FIXTURE DEPENDS ON, asserted rather than
                // assumed: the drain settles the wedge rows (their objects
                // were never uploaded) and the purge runs AFTER it, so the
                // wedge was un-purgeable *while the walk passed it* — it is
                // drained now, but freshly, and inside retention.
                assertThat(
                    own.jdbi.withHandleUnchecked { h ->
                        h.createQuery(
                            "SELECT count(*) FROM hog_file_removal WHERE path LIKE '%/wedge%' " +
                                "AND drained_at > now() - interval '1 hour'",
                        ).mapTo(Int::class.java).one()
                    },
                )
                    .describedAs("the drain settled the wedge, and the purge ran after it")
                    .isEqualTo(wedge)
                remaining =
                    own.jdbi.withHandleUnchecked { h ->
                        h.createQuery(
                            "SELECT path FROM hog_file_removal WHERE drained_at < " +
                                "now() - interval '30 days' ORDER BY removal_id",
                        ).mapTo(String::class.java).list().map { it.substringAfterLast('/') }
                    }
            }
            return remaining
        }

        // WIDER than the cap: the walk stops inside the wedge, so the
        // expired rows above it survive this run.
        assertThat(run(CleanupService.LEDGER_PURGE_EMPTY_PAGES + 1, "cl-wedge-wide"))
            .describedAs(
                "a wedge wider than the %d-page cap ends the walk; the rows above it are the " +
                    "next run's work",
                CleanupService.LEDGER_PURGE_EMPTY_PAGES,
            )
            .containsExactly("above1", "above2")

        // NARROWER than the cap: the walk skips it and purges what is
        // above, which is the property the cap must not cost.
        assertThat(run(CleanupService.LEDGER_PURGE_EMPTY_PAGES - 1, "cl-wedge-narrow"))
            .describedAs("a wedge inside the cap is walked past and everything expired goes")
            .isEmpty()
    }

    @Test
    fun `a purge that examined pages and deleted nothing reports itself at INFO`() {
        // The state an operator has to be able to see. A walk that gave up
        // on the cap and one that ran out of budget both look exactly like
        // a purge with nothing to do — in the database and, at debug, in
        // the log. This is the line that tells them apart, and the stop
        // reason is in it.
        PgTestSupport.freshDatabase().use { own ->
            val catalogId =
                own.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "INSERT INTO hog_catalog (name, data_path) VALUES ('cl-purge-info', " +
                            "'s3://$BUCKET/') RETURNING catalog_id",
                    ).mapTo(Long::class.java).one()
                }
            own.jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    """
                    INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                    SELECT :c, 's3://$BUCKET/cl-purge-info/wedge' || g, 'data', 'snapshot_expiry'
                    FROM generate_series(1, :n) g
                    """,
                )
                    .bind("c", catalogId)
                    .bind("n", CleanupService.LEDGER_PURGE_EMPTY_PAGES + 2)
                    .execute()
            }

            val logged = java.util.concurrent.CopyOnWriteArrayList<String>()
            withInfoCapture(logged) {
                CleanupService(own.jdbi, removals, ledgerPurgePage = 1)
                    .runOnce("cl-purge-info", batchSize = 100)
            }
            assertThat(logged)
                .describedAs("the stop reason is reported, and it names the cap:%n%s", logged)
                .anySatisfy {
                    assertThat(it)
                        .contains("purged 0 drained ledger rows")
                        .contains("${CleanupService.LEDGER_PURGE_EMPTY_PAGES} consecutive pages")
                }
        }
    }

    @Test
    fun `the purge stops on its wall budget rather than walking a whole ledger in one run`() {
        // THE SECOND STOP CONDITION, which the page walk needs and nothing
        // else asserts: a ledger with more eligible rows than one run
        // should spend on is PACED, not walked to the end. Driven with a
        // budget of zero — the honest way to make a wall clock observable
        // without sleeping — so the loop stops before its first page and
        // the rows survive; the same service with the default budget
        // clears them.
        PgTestSupport.freshDatabase().use { own ->
            val catalogId =
                own.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "INSERT INTO hog_catalog (name, data_path) " +
                            "VALUES ('cl-purge-budget', 's3://$BUCKET/') RETURNING catalog_id",
                    ).mapTo(Long::class.java).one()
                }
            own.jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    """
                    INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason,
                                                  drained_at, drained_outcome)
                    SELECT :c, 's3://$BUCKET/cl-purge-budget/' || g, 'data', 'snapshot_expiry',
                           now() - interval '40 days', 'deleted'
                    FROM generate_series(1, 20) g
                    """,
                ).bind("c", catalogId).execute()
            }

            fun remaining(): Int =
                own.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT count(*) FROM hog_file_removal").mapTo(Int::class.java).one()
                }

            // AND IT SAYS SO. A budget that is already spent before the
            // first page IS a budget stop, and reporting it as one is the
            // difference between "paced" and "silently did nothing" —
            // which is the indistinguishability that made the 2026-09-28
            // misdiagnosis slow. The earlier form only reported a stop
            // after at least one page, so this exact case was invisible.
            val logged = java.util.concurrent.CopyOnWriteArrayList<String>()
            withDebugCapture(logged) {
                CleanupService(own.jdbi, removals, ledgerPurgePage = 1, ledgerPurgeBudgetMs = 0)
                    .runOnce("cl-purge-budget", batchSize = 100)
            }
            assertThat(remaining())
                .describedAs("a spent budget stops the walk before it starts, and nothing is lost")
                .isEqualTo(20)
            assertThat(logged)
                .describedAs("the stop is reported, not silent:%n%s", logged)
                .anySatisfy { assertThat(it).contains("stopped on the 0ms budget") }

            CleanupService(own.jdbi, removals, ledgerPurgePage = 1)
                .runOnce("cl-purge-budget", batchSize = 100)
            assertThat(remaining())
                .describedAs("and the next run, with a real budget, clears them")
                .isZero()
        }
    }

    @Test
    fun `the manual path on a loop-off pod runs one worker, whatever the knob says`() {
        // THE CLAMP THE POOL REFUSAL RESTS ON. `POST /v1/maintenance/cleanup`
        // runs this drain on whichever pod serves it, including pods whose
        // loop is disabled — and `Config` prices a loop-off pod at exactly
        // ONE worker, because pricing four would refuse every API replica
        // in the fleet. Without the clamp, `HOGLAKE_CLEANUP_WORKERS=4` with
        // the loop off boots and then takes four pooled connections the
        // moment somebody curls the endpoint: the failure the refusal says
        // it prevents, through a configuration it approved.
        //
        // The worker count is visible in the claims' `claimed_by` binding,
        // which is `<prefix>#<index>` — so "one worker" is one distinct id.
        val catalogId = seedCatalog("cl-clamp")
        val paths = (1..12).map { "s3://$BUCKET/cl-clamp/f$it.parquet" }
        paths.forEach {
            putObject(it)
            queue(catalogId, it)
        }

        val offIssued = java.util.concurrent.CopyOnWriteArrayList<String>()
        CleanupService(
            recordingJdbi(offIssued),
            removals,
            subBatchSize = 2,
            workers = 4,
            workerIdPrefix = "loopoff",
            loopEnabled = false,
        ).runOnce("cl-clamp", batchSize = 2)
        assertThat(claimants(offIssued, "loopoff"))
            .describedAs("a loop-off pod runs the one worker its Config priced:%n%s", offIssued)
            .containsExactly("loopoff#0")

        // With the loop ON the same four workers are what the refusal
        // priced, and all four run.
        val onIssued = java.util.concurrent.CopyOnWriteArrayList<String>()
        CleanupService(
            recordingJdbi(onIssued),
            removals,
            subBatchSize = 2,
            workers = 4,
            workerIdPrefix = "loopon",
            loopEnabled = true,
        ).runOnce("cl-clamp", batchSize = 2)
        assertThat(claimants(onIssued, "loopon"))
            .describedAs("with the loop on, the configured count is the count that runs")
            .hasSize(4)
    }

    @Test
    fun `the production wiring clamps the manual path when the loop is off`() {
        // THE WIRING ITSELF, which nothing pinned. The clamp is what makes
        // `Config`'s pool refusal true — a loop-off pod draws the ONE
        // connection the refusal priced, not `HOGLAKE_CLEANUP_WORKERS` of
        // them — and until this test the predicate behind it
        // (`cleanupIntervalMs > 0`) was spelled out at the `App.kt` call
        // site, where `loopEnabled`'s default of `true` meant a later edit
        // could write `loopEnabled = true` and leave the whole suite green
        // while the refusal's argument stopped holding in the shipped
        // binary. The derivation now lives in the service's own `Config`
        // constructor, which is what App uses and what this builds.
        val catalogId = seedCatalog("cl-wiring")
        (1..12).forEach {
            val p = "s3://$BUCKET/cl-wiring/f$it.parquet"
            putObject(p)
            queue(catalogId, p)
        }

        fun cfg(intervalMs: Long) =
            com.posthog.hoglake.Config(
                cleanupIntervalMs = intervalMs,
                cleanupWorkers = 4,
                cleanupSubBatchSize = 2,
                cleanupStagingGraceSeconds = 0,
            )

        val offIssued = java.util.concurrent.CopyOnWriteArrayList<String>()
        CleanupService(recordingJdbi(offIssued), removals, cfg(intervalMs = 0))
            .runOnce("cl-wiring", batchSize = 2)
        assertThat(workerCount(offIssued))
            .describedAs(
                "a Config with the loop OFF must produce a service that runs ONE worker, " +
                    "whatever HOGLAKE_CLEANUP_WORKERS says:%n%s",
                offIssued,
            )
            .isEqualTo(1)

        val onIssued = java.util.concurrent.CopyOnWriteArrayList<String>()
        CleanupService(recordingJdbi(onIssued), removals, cfg(intervalMs = 1_800_000))
            .runOnce("cl-wiring", batchSize = 2)
        assertThat(workerCount(onIssued))
            .describedAs("and with the loop ON, the four workers the refusal priced")
            .isEqualTo(4)
    }

    @Test
    fun `an arm that comes back empty is not asked again for the rest of the run`() {
        // EFFICIENCY, PINNED. The two arms share a worker's iterations, so
        // an arm whose queue is empty used to be re-claimed once per
        // iteration for as long as the OTHER arm kept finding work — up to
        // `batchSize / subBatchSize` wasted round trips per worker per run
        // (80 at the production defaults). Zeroing the empty arm's budget
        // on its first empty claim is the fix, and the statement stream is
        // the only place it is visible: the outcomes are identical either
        // way.
        val catalogId = seedCatalog("cl-empty-arm")
        val paths = (1..60).map { "s3://$BUCKET/cl-empty-arm/f$it.parquet" }
        paths.forEach {
            putObject(it)
            queue(catalogId, it)
        }

        val issued = java.util.concurrent.CopyOnWriteArrayList<String>()
        val result =
            CleanupService(recordingJdbi(issued), removals, subBatchSize = 10, stagingGraceSeconds = 0)
                .runOnce("cl-empty-arm", batchSize = 60)

        assertThat(result.removed).describedAs("the bulk arm did six sub-batches of work").isEqualTo(60)
        val claims = issued.filter { it.contains("SET claimed_at = now()") }
        assertThat(claims.count { it.contains("reason <> 'compaction_staging'") })
            .describedAs("six bulk claims of ten:%n%s", claims.joinToString("\n---\n"))
            .isEqualTo(6)
        assertThat(claims.count { it.contains("reason = 'compaction_staging'") })
            .describedAs(
                "and the staging arm is asked ONCE — it came back empty, so the remaining five " +
                    "iterations must not ask it again:%n%s",
                claims.joinToString("\n---\n"),
            )
            .isEqualTo(1)
    }

    @Test
    fun `a sweep over two catalogs walks the retention purge ONCE, not once per catalog`() {
        // THE HOIST, pinned. The purge walks the removal table's primary
        // key with no `catalog_id`, so it is instance-wide work: called
        // per catalog it would pay N wall budgets to re-walk the same
        // prefix of the key, which on a catalog-rich deployment is the
        // whole budget spent N times on the same pages. The outcomes are
        // identical either way — the rows that go, go — so the statement
        // stream is the only place the saving is visible.
        //
        // Its own database, so the sweep sees exactly two catalogs.
        PgTestSupport.freshDatabase().use { own ->
            fun catalog(name: String): Long =
                own.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "INSERT INTO hog_catalog (name, data_path) VALUES (:n, 's3://$BUCKET/') " +
                            "RETURNING catalog_id",
                    ).bind("n", name).mapTo(Long::class.java).one()
                }
            val a = catalog("sweep-a")
            val b = catalog("sweep-b")
            // One expired ledger row per catalog, so the walk has work and
            // the page count is small and predictable.
            own.jdbi.useHandleUnchecked { h ->
                for (c in listOf(a, b)) {
                    h.createUpdate(
                        "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, " +
                            "drained_at, drained_outcome) VALUES (:c, :p, 'data', " +
                            "'snapshot_expiry', now() - interval '40 days', 'deleted')",
                    ).bind("c", c).bind("p", "s3://$BUCKET/sweep/$c").execute()
                }
            }

            val issued = java.util.concurrent.CopyOnWriteArrayList<String>()
            val results =
                CleanupService(
                    recordingJdbi(issued, own.dataSource),
                    removals,
                    ledgerPurgePage = 1_000,
                ).runOnceAllCatalogs(batchSize = 100)

            assertThat(results.map { it.first })
                .describedAs("both catalogs were drained")
                .containsExactlyInAnyOrder("sweep-a", "sweep-b")
            // The walk over a two-row ledger is ONE page (both rows purge,
            // the window is short, so it stops) — and exactly one walk, not
            // one per catalog.
            val pages = issued.filter { it.contains("WITH page AS") }
            assertThat(pages)
                .describedAs(
                    "one purge walk for the whole sweep, not one per catalog:%n%s",
                    pages.joinToString("\n---\n"),
                )
                .hasSize(1)
            assertThat(
                own.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT count(*) FROM hog_file_removal").mapTo(Long::class.java).one()
                },
            )
                .describedAs("and it purged both catalogs' expired rows, which is the point of one walk")
                .isZero()
        }
    }

    @Test
    fun `a claim never mixes the two reasons, and a staging claim is capped at STAGING_SUB_BATCH`() {
        // THE REASON-AWARE SPLIT, pinned behaviourally. The arithmetic for
        // it lives in RemovalStoreBoundsTest, but an assertion over
        // CONSTANTS cannot stop the two statements being merged back into
        // the one whose worst case is 22x the lease — a merged claim would
        // take 1,000 tickets under a single 900 s lease and still produce
        // identical outcomes everywhere else in this class. What tells
        // them apart is the STATEMENT STREAM: one claim per reason, and a
        // staging claim that stops at 25 however large the sub-batch is.
        val catalogId = seedCatalog("cl-arms")
        (1..30).forEach {
            val p = "s3://$BUCKET/cl-arms/bulk$it.parquet"
            putObject(p)
            queue(catalogId, p)
        }
        (1..30).forEach {
            val p = "s3://$BUCKET/cl-arms/ticket$it.parquet"
            putObject(p)
            stagingTicket(catalogId, p)
        }

        // batch 30 per arm, sub-batch 1,000: the bulk arm takes its 30 in
        // ONE claim (a merged claim would take all 60 in that one), and
        // the staging arm needs TWO because its own statement stops at 25.
        val issued = java.util.concurrent.CopyOnWriteArrayList<String>()
        val result =
            CleanupService(
                recordingJdbi(issued),
                removals,
                subBatchSize = 1_000,
                stagingGraceSeconds = 0,
            ).runOnce("cl-arms", batchSize = 30)

        assertThat(result.removed).describedAs("the whole queue drains").isEqualTo(60)
        val claims = issued.filter { it.contains("SET claimed_at = now()") }
        assertThat(claims)
            .describedAs("the run must claim at all:%n%s", claims.joinToString("\n---\n"))
            .isNotEmpty()
        assertThat(claims)
            .describedAs("every claim asks for ONE reason, because the lease bounds a CLAIM")
            .allSatisfy {
                assertThat(it).satisfiesAnyOf(
                    { s -> assertThat(s).contains("reason <> 'compaction_staging'") },
                    { s -> assertThat(s).contains("reason = 'compaction_staging'") },
                )
            }
        // THE LOAD-BEARING COUNT: 30 tickets at 25 per claim is two
        // staging claims. A merged claim would issue ONE statement for all
        // 60 rows and red here.
        assertThat(claims.count { it.contains("reason = 'compaction_staging'") })
            .describedAs(
                "%d tickets at %d per claim is two staging claims:%n%s",
                30,
                CleanupService.STAGING_SUB_BATCH,
                claims.joinToString("\n---\n"),
            )
            .isEqualTo(2)
        assertThat(claims.count { it.contains("reason <> 'compaction_staging'") })
            .describedAs("and the bulk arm takes its 30 in one claim of up to %d", 1_000)
            .isEqualTo(1)
    }

    @Test
    fun `a bulk backlog bigger than the batch does not starve the staging tickets`() {
        // THE STARVATION THE PER-ARM BUDGET REMOVES. With one budget
        // shared by both arms and bulk going first, a catalog whose bulk
        // queue exceeds the batch gave staging exactly one claim — 25 rows
        // per worker per run — and with `batchSize <= subBatchSize` it gave
        // them NOTHING, because the bulk arm spent the budget in the first
        // iteration. On gigahog-prod-us that is ~9.4k orphaned tickets at
        // 50/h: eight days, with /verify's `staging_tickets.leaked` arm
        // (a 6 h bound) firing for every one of them the whole time.
        //
        // The fixture is that exact shape: a bulk queue larger than the
        // batch, and more than one staging claim's worth of tickets behind
        // it. Both budgets are spent in the same run.
        val catalogId = seedCatalog("cl-starve")
        val bulk = (1..40).map { "s3://$BUCKET/cl-starve/bulk$it.parquet" }
        val tickets = (1..60).map { "s3://$BUCKET/cl-starve/ticket$it.parquet" }
        bulk.forEach {
            putObject(it)
            queue(catalogId, it)
        }
        tickets.forEach {
            putObject(it)
            stagingTicket(catalogId, it)
        }

        // batch 30 < the 40-row bulk queue, sub-batch 10, grace 0: under a
        // shared budget the bulk arm would take 30 and staging would get
        // at most 25 — and on the second iteration, nothing.
        val result =
            CleanupService(jdbi, removals, subBatchSize = 10, stagingGraceSeconds = 0)
                .runOnce("cl-starve", batchSize = 30)

        assertThat(result.removed)
            .describedAs("30 of each reason, not 30 between them")
            .isEqualTo(60)
        val drained = ledgerRows(catalogId).filter { it.drainedAt != null }.map { it.path }
        assertThat(drained.count { it.contains("/ticket") })
            .describedAs("the staging budget is the tickets' own, so the bulk backlog cannot spend it")
            .isEqualTo(30)
        assertThat(drained.count { it.contains("/bulk") }).isEqualTo(30)
        assertThat(queuedPaths(catalogId))
            .describedAs("and the remainder of both is the next run's")
            .hasSize(40)
    }

    @Test
    fun `invalid inputs - unknown catalog and non-positive batch`() {
        assertThatThrownBy { svc.runOnce("cl-nope", 100) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { svc.runOnce("cl-nope", 0) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `background loop drains all catalogs and a non-positive interval is a no-op`() {
        com.posthog.hoglake.BackgroundLoops().use { it.register("cleanup", 0) { svc.runOnceAllCatalogs(100) } }
        com.posthog.hoglake.BackgroundLoops().use { it.register("cleanup", -1) { svc.runOnceAllCatalogs(100) } }

        val idA = seedCatalog("cl-loop-a")
        val idB = seedCatalog("cl-loop-b")
        val pathA = "s3://$BUCKET/cl-loop-a/f.parquet"
        val pathB = "s3://$BUCKET/cl-loop-b/f.parquet"
        putObject(pathA)
        queue(idA, pathA)
        putObject(pathB)
        queue(idB, pathB)

        com.posthog.hoglake.BackgroundLoops().use { loops ->
            loops.register("cleanup", 50) { svc.runOnceAllCatalogs(100) }
            await().atMost(Duration.ofSeconds(30)).untilAsserted {
                assertThat(queuedPaths(idA)).isEmpty()
                assertThat(queuedPaths(idB)).isEmpty()
            }
        }
        assertThat(removals.exists(pathA)).isFalse()
        assertThat(removals.exists(pathB)).isFalse()
    }

    /** Captures audit-logger events; assertions read the structured kv args. */
    private class AuditCapture : ch.qos.logback.core.AppenderBase<ch.qos.logback.classic.spi.ILoggingEvent>() {
        val events = java.util.concurrent.CopyOnWriteArrayList<ch.qos.logback.classic.spi.ILoggingEvent>()

        override fun append(event: ch.qos.logback.classic.spi.ILoggingEvent) {
            events += event
        }

        fun lines(): List<String> = events.map { e -> e.argumentArray.orEmpty().joinToString(" ") { it.toString() } }
    }

    /**
     * Capture the cleanup service's own app-log lines (not the audit
     * stream) for the duration of [block], at INFO.
     *
     * Needed for the purge's stop reason, which has no other channel: a
     * walk that gave up on the empty-page cap and one that ran out of
     * budget both look exactly like a purge with nothing to do, in the
     * database and in the counters.
     */
    private fun withInfoCapture(
        sink: MutableList<String>,
        block: () -> Unit,
    ) = withCapture(sink, ch.qos.logback.classic.Level.INFO, block)

    /** The same at DEBUG, for the lines an operator only gets on request. */
    private fun withDebugCapture(
        sink: MutableList<String>,
        block: () -> Unit,
    ) = withCapture(sink, ch.qos.logback.classic.Level.DEBUG, block)

    private fun withCapture(
        sink: MutableList<String>,
        level: ch.qos.logback.classic.Level,
        block: () -> Unit,
    ) {
        val logger =
            org.slf4j.LoggerFactory.getLogger(CleanupService::class.java)
                as ch.qos.logback.classic.Logger
        val ctx = org.slf4j.LoggerFactory.getILoggerFactory() as ch.qos.logback.classic.LoggerContext
        val appender =
            object : ch.qos.logback.core.AppenderBase<ch.qos.logback.classic.spi.ILoggingEvent>() {
                override fun append(event: ch.qos.logback.classic.spi.ILoggingEvent) {
                    sink += event.formattedMessage
                }
            }
        appender.context = ctx
        appender.start()
        val previous = logger.level
        logger.level = level
        logger.addAppender(appender)
        try {
            block()
        } finally {
            logger.detachAppender(appender)
            appender.stop()
            logger.level = previous
        }
    }

    private fun <T> withAuditCapture(block: (AuditCapture) -> T): T {
        val ctx = org.slf4j.LoggerFactory.getILoggerFactory() as ch.qos.logback.classic.LoggerContext
        val capture = AuditCapture().apply { context = ctx }
        capture.start()
        val logger =
            org.slf4j.LoggerFactory.getLogger(com.posthog.hoglake.observability.Audit.LOGGER_NAME)
                as ch.qos.logback.classic.Logger
        logger.addAppender(capture)
        try {
            return block(capture)
        } finally {
            logger.detachAppender(capture)
            capture.stop()
        }
    }

    @Test
    fun `audit trail - per-path file_deleted and cleanup_violation events, silence when idle`() {
        val catalogId = seedCatalog("cl-audit")
        // One referenced path (violation) + two deletable ones.
        jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                catalogId,
            )
            h.execute(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 1, 1, 1, 's3://$BUCKET/cl-audit/live.parquet', 10, 100, 0)
                """,
                catalogId,
            )
        }
        val live = "s3://$BUCKET/cl-audit/live.parquet"
        val dead1 = "s3://$BUCKET/cl-audit/dead1.parquet"
        val dead2 = "s3://$BUCKET/cl-audit/dead2.parquet"
        for (p in listOf(live, dead1, dead2)) {
            putObject(p)
            queue(catalogId, p)
        }

        withAuditCapture { capture ->
            val result = svc.runOnce("cl-audit", batchSize = 100)
            assertThat(result.removed).isEqualTo(2)
            assertThat(result.stillReferenced).isEqualTo(1)
            val lines = capture.lines()
            // One file_deleted event per physically removed object, keyed
            // by path.
            for (p in listOf(dead1, dead2)) {
                assertThat(lines)
                    .describedAs("file_deleted event for %s", p)
                    .anySatisfy {
                        assertThat(it).contains("action=file_deleted").contains("object=$p")
                    }
            }
            // The invariant violation names its path too.
            assertThat(lines).anySatisfy {
                assertThat(it)
                    .contains("action=cleanup_violation")
                    .contains("outcome=invariant_violation")
                    .contains("object=$live")
            }
            // Summary event flags the violation.
            assertThat(lines).anySatisfy {
                assertThat(it).contains("action=cleanup").contains("outcome=invariant_violation")
            }
        }

        // Zero-work drain (only the still-referenced entry remains — and it
        // still counts as work): drain a catalog with an EMPTY queue and
        // expect audit silence.
        seedCatalog("cl-audit-idle")
        withAuditCapture { capture ->
            val idle = svc.runOnce("cl-audit-idle", batchSize = 100)
            assertThat(idle).isEqualTo(com.posthog.hoglake.model.CleanupResult(0, 0, 0))
            assertThat(capture.lines())
                .describedAs("zero-work drain stays out of the audit stream")
                .noneSatisfy { assertThat(it).contains("action=cleanup") }
        }
    }

    @Test
    fun `end to end - expiry queues unreachable files and cleanup removes them from S3`() {
        // Catalog with old snapshots 0..4 (head 4), retention 60s.
        val catalogId =
            jdbi.withHandleUnchecked { h ->
                val id =
                    h.createQuery(
                        """
                INSERT INTO hog_catalog
                    (name, data_path, last_snapshot_id, snapshot_retention_seconds)
                VALUES ('cl-e2e', 's3://$BUCKET/', 4, 60)
                RETURNING catalog_id
                """,
                    ).mapTo(Long::class.java).one()
                for (s in 0..4) {
                    h.execute(
                        """
                    INSERT INTO hog_snapshot (catalog_id, snapshot_id, snapshot_time, schema_version)
                    VALUES (?, ?, now() - make_interval(secs => 3600), 0)
                    """,
                        id,
                        s,
                    )
                }
                h.execute("INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)", id)
                // Rewritten at snapshot 2 -> unreachable once the floor passes it.
                h.execute(
                    """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    end_snapshot, path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 1, 1, 1, 2, 's3://$BUCKET/cl-e2e/old.parquet', 10, 100, 0)
                """,
                    id,
                )
                // Its live replacement survives everything.
                h.execute(
                    """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                    path, record_count, file_size_bytes, row_id_start)
                VALUES (?, 2, 1, 2, 's3://$BUCKET/cl-e2e/live.parquet', 10, 100, 10)
                """,
                    id,
                )
                id
            }
        val oldPath = "s3://$BUCKET/cl-e2e/old.parquet"
        val livePath = "s3://$BUCKET/cl-e2e/live.parquet"
        putObject(oldPath)
        putObject(livePath)

        val expiry = ExpiryService(jdbi).runOnce("cl-e2e", batchSize = 100)
        assertThat(expiry.newEarliestSnapshotId).isEqualTo(4)
        assertThat(expiry.snapshotsExpired).isEqualTo(4)
        assertThat(expiry.dataFilesQueued).isEqualTo(1)
        assertThat(queuedPaths(catalogId)).containsExactly(oldPath)

        val cleanup = svc.runOnce("cl-e2e", batchSize = 100)
        assertThat(cleanup.removed).isEqualTo(1)
        assertThat(cleanup.missing).isEqualTo(0)
        assertThat(cleanup.stillReferenced).isEqualTo(0)
        assertThat(queuedPaths(catalogId)).isEmpty()
        assertThat(removals.exists(oldPath)).isFalse() // physically gone
        assertThat(removals.exists(livePath)).isTrue() // live data untouched
    }
}
