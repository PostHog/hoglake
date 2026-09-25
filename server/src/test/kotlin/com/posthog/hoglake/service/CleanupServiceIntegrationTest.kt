package com.posthog.hoglake.service

import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.CleanupResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.persistence.Locks
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
 * sub-batches commit independently. Ends with the full lifecycle:
 * expire -> cleanup -> the queued objects are gone from S3.
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
    )

    /** Every ledger row (drained or not), in queue order. */
    private fun ledgerRows(catalogId: Long): List<LedgerRow> =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT path, attempts, last_attempt_at, drained_at, drained_outcome
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
    fun `a ticket a compaction commit settled between select and lock is skipped, not alerted`() {
        // The realistic interleaving, and it is a commit doing its job.
        // CompactionService's group commit settles its own staging
        // ticket 'registered' in the transaction that registers the
        // output path. The drain's batch select runs OUTSIDE the commit
        // lock, so that settle can land between the select and the
        // sub-batch that holds the row — at which point the path IS a
        // live file.
        //
        // Before the under-lock re-check, all four of these followed: an
        // ERROR log, a `cleanup_violation` audit event, a
        // `still_referenced` count that pages someone, and an
        // attempts/last_attempt_at bump on a settled row. The row is
        // simply not this drain's any more.
        //
        // ORDER-INDEPENDENT BY CONSTRUCTION: the row under test is a
        // staging ticket placed behind a FULL staging sub-batch, so it
        // is in the second staging hold whatever order the reasons
        // drain in, and the commit fires from the first hold's first
        // probe. Nothing here rests on bulk running before staging.
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
        // path and settle the ticket together, once, from inside the
        // FIRST sub-batch's object-store work — which is after this
        // run's batch select and before the second sub-batch takes its
        // lock.
        val committed = java.util.concurrent.atomic.AtomicBoolean()
        val committer =
            object : RemovalStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun deleteIfExists(
                    pathUri: String,
                    mayIssueCall: () -> Boolean,
                ): Outcome {
                    if (committed.compareAndSet(false, true)) {
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
                            h.execute(
                                "UPDATE hog_file_removal SET drained_at = now(), " +
                                    "drained_outcome = 'registered' WHERE removal_id = ?",
                                removalId,
                            )
                        }
                    }
                    return super.deleteIfExists(pathUri, mayIssueCall)
                }
            }

        val before = ledgerRows(catalogId).single { it.path == path }
        val result =
            withAuditCapture { capture ->
                val r =
                    CleanupService(jdbi, committer, stagingGraceSeconds = 0)
                        .runOnce("cl-settled-elsewhere", batchSize = 100)
                assertThat(capture.lines())
                    .describedAs("a committing compaction group is not an invariant violation")
                    .noneSatisfy { assertThat(it).contains("action=cleanup_violation") }
                r
            }

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
            .describedAs("last_attempt_at must not move on a row this drain never touched")
            .isEqualTo(before.lastAttemptAt)
        assertThat(removals.exists(path))
            .describedAs("the registered object is untouched")
            .isTrue()
    }

    @Test
    fun `both ledger writes backstop a registered that lands inside the sub-batch's own hold`() {
        // The remainder the under-lock re-check cannot cover. That
        // re-check is the first statement under the lock, so it catches
        // a settle that landed BEFORE it; a settle that lands after it,
        // inside this sub-batch's own hold, reaches the two statements
        // that write the ledger. Narrower than the realistic race (a
        // commit would have to take the advisory lock this hold is
        // holding), which is why `drained_at IS NULL` on SETTLE_SQL and
        // on BUMP_ATTEMPTS_SQL is the backstop and not the catcher —
        // but unfenced, each writes over another writer's record, and
        // the ledger is the only record there is.
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

        fun register(removalId: Long) =
            jdbi.useHandleUnchecked { h ->
                h.execute(
                    "UPDATE hog_file_removal SET drained_at = now(), drained_outcome = 'registered' " +
                        "WHERE removal_id = ? AND drained_at IS NULL",
                    removalId,
                )
            }

        val racer =
            object : RemovalStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun deleteIfExists(
                    pathUri: String,
                    mayIssueCall: () -> Boolean,
                ): Outcome {
                    // Inside the hold: after the re-check, before the
                    // ledger write.
                    if (pathUri == failed) {
                        register(failedId)
                        throw IllegalStateException("object store failed after the other writer settled")
                    }
                    val outcome = super.deleteIfExists(pathUri, mayIssueCall)
                    register(settledId)
                    return outcome
                }
            }
        val before = ledgerRows(catalogId).associateBy { it.path }

        val result =
            CleanupService(jdbi, racer, stagingGraceSeconds = 0)
                .runOnce("cl-settle-backstop", batchSize = 100)

        assertThat(result.removed).describedAs("a row settled elsewhere is not counted").isZero()
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
                override fun deleteBatch(
                    paths: Collection<String>,
                    mayIssueCall: () -> Boolean,
                ): BatchOutcome {
                    val outcome = super.deleteBatch(paths.filter { it != doomed }, mayIssueCall)
                    return outcome.copy(
                        failures =
                            outcome.failures + mapOf(doomed to "AccessDenied: simulated per-key failure"),
                    )
                }
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
    fun `staging tickets drain in their own small sub-batches`() {
        // The lock-hold bound, as code. A compaction_staging row costs a
        // HEAD and a DELETE (no batched form reports 'absent'), so a
        // 1,000-row sub-batch of them would be 1,000 probe pairs in ONE
        // hold — ~128 s at the measured 64 ms per round trip, 40x the
        // hold this change removed. The grace clusters them too, because
        // tickets become eligible in the order their groups ran.
        //
        // The hold IS the transaction, and it is observed from outside
        // it: a sub-batch's settles are invisible to another connection
        // until it commits, so the count of settled rows read on a
        // separate connection is constant within a hold and steps at
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
                override fun deleteIfExists(
                    pathUri: String,
                    mayIssueCall: () -> Boolean,
                ): Outcome {
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
                    return super.deleteIfExists(pathUri, mayIssueCall)
                }
            }

        val result =
            CleanupService(jdbi, counting, subBatchSize = 1_000, stagingGraceSeconds = 0)
                .runOnce("cl-staging-holds", batchSize = 1_000)

        assertThat(result.removed).isEqualTo(60)
        assertThat(holdSizes)
            .describedAs(
                "60 tickets at %d per hold: two FULL holds and a remainder, and the full ones " +
                    "must be exactly the sub-batch — `<= 25` would also pass for a drain that " +
                    "probed one row per hold",
                CleanupService.STAGING_SUB_BATCH,
            )
            .containsExactly(CleanupService.STAGING_SUB_BATCH, CleanupService.STAGING_SUB_BATCH, 10)
    }

    @Test
    fun `a hold stops issuing calls once its budget is spent, and leaves the rest untouched`() {
        // The bound that a call COUNT cannot state. Each call may take a
        // whole RemovalStore.apiCallTimeout, so 25 probe pairs could
        // reach 500 s — long past the idle_in_transaction_session_timeout
        // every pooled connection is opened with. Past that Postgres
        // kills the backend: the sub-batch rolls back WITH ITS OBJECTS
        // ALREADY DELETED and its ledger rows unsettled, and the next
        // run drains the same rows and does it again, forever.
        //
        // Driven with an injected clock rather than sleeps. A deadline
        // test that sleeps is a deadline test that is flaky, and at a
        // 20 s budget it is also a 20 s test.
        val catalogId = seedCatalog("cl-hold-budget")
        val tickets = (1..6).map { "s3://$BUCKET/cl-hold-budget/s$it.parquet" }
        tickets.forEach {
            putObject(it)
            stagingTicket(catalogId, it)
        }
        // The clock advances only when an object-store call is issued,
        // so the deadline is a function of calls made and nothing else.
        val now = java.util.concurrent.atomic.AtomicLong(0)
        val perCall = Duration.ofSeconds(4)
        val slow =
            object : RemovalStore(minio.s3URL, "us-east-1", minio.userName, minio.password, true) {
                override fun deleteIfExists(
                    pathUri: String,
                    mayIssueCall: () -> Boolean,
                ): Outcome =
                    super.deleteIfExists(pathUri) {
                        mayIssueCall().also { if (it) now.addAndGet(perCall.toNanos()) }
                    }
            }
        // Budget 20 s, call bound 10 s: a call may start while at most
        // 10 s is spent. At 4 s per call that is HEAD+DELETE for rows
        // one and two (8 s spent), then row three's HEAD at 8 s, its
        // DELETE at 12 s — past the line — so the drain stops mid-row
        // and row three is untouched along with the rest.
        val drain =
            CleanupService(
                jdbi,
                slow,
                stagingGraceSeconds = 0,
                nanoTime = { now.get() },
            )

        val first = drain.runOnce("cl-hold-budget", batchSize = 100)

        assertThat(first.removed)
            .describedAs("the deadline stopped the hold well short of the six queued rows")
            .isLessThan(tickets.size.toLong())
            .isGreaterThan(0)
        val undrained = queuedPaths(catalogId)
        assertThat(undrained).describedAs("the remainder is still queued").isNotEmpty()
        assertThat(ledgerRows(catalogId).filter { it.path in undrained })
            .describedAs(
                "a row the deadline never reached was not ATTEMPTED: no attempts bump, no " +
                    "last_attempt_at, and its object is untouched",
            )
            .allSatisfy {
                assertThat(it.attempts).isZero()
                assertThat(it.lastAttemptAt).isNull()
            }
        undrained.forEach { assertThat(removals.exists(it)).isTrue() }

        // And they are simply the next run's work: a fresh drain (fresh
        // budget) takes them.
        now.set(0)
        var drained = first.removed + first.missing
        repeat(6) {
            now.set(0)
            drained += drain.runOnce("cl-hold-budget", batchSize = 100).let { r -> r.removed + r.missing }
        }
        assertThat(drained).isEqualTo(tickets.size.toLong())
        assertThat(queuedPaths(catalogId)).isEmpty()
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
        for (row in ledgerRows(catalogId)) {
            assertThat(row.attempts).isEqualTo(1)
            assertThat(row.lastAttemptAt).isNotNull()
            assertThat(row.drainedAt).isNull()
        }
        svc.runOnce("cl-live", batchSize = 100)
        assertThat(ledgerRows(catalogId).map { it.attempts }).containsOnly(2)
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
    fun `drain sub-batches take the per-catalog commit lock`() {
        // Pinned regression (bug hunt #2, locking half): the reference
        // check and the physical delete must be serialized against the
        // commit tail via the SAME advisory lock every commit takes.
        // Without it, an in-flight commit past its own removal-queue
        // check could insert a hog_data_file row for a queued path that
        // referencedPaths (READ COMMITTED) cannot see yet — the drain
        // would delete the object under the about-to-commit live row.
        // With the lock, the drain waits for the commit to finish (and
        // then sees its rows), or the commit waits for the sub-batch.
        // This test asserts the lock is actually taken: while a fake
        // "commit" holds it, the drain makes no progress.
        val catalogId = seedCatalog("cl-lock")
        val path = "s3://$BUCKET/cl-lock/f.parquet"
        putObject(path)
        queue(catalogId, path)

        val holder = jdbi.open()
        val executor = java.util.concurrent.Executors.newSingleThreadExecutor()
        try {
            holder.begin()
            Locks.acquireCatalogCommitLock(holder, catalogId)

            val drain = executor.submit<CleanupResult> { svc.runOnce("cl-lock", batchSize = 100) }
            Thread.sleep(500)
            // Blocked behind the "commit": nothing settled, object intact.
            assertThat(drain.isDone).isFalse()
            assertThat(queuedPaths(catalogId)).containsExactly(path)
            assertThat(removals.exists(path)).isTrue()

            holder.rollback() // the "commit" finishes; the drain proceeds
            val result = drain.get(30, java.util.concurrent.TimeUnit.SECONDS)
            assertThat(result.removed).isEqualTo(1)
            assertThat(queuedPaths(catalogId)).isEmpty()
            assertThat(removals.exists(path)).isFalse()
        } finally {
            if (holder.isInTransaction) holder.rollback()
            holder.close()
            executor.shutdownNow()
        }
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
