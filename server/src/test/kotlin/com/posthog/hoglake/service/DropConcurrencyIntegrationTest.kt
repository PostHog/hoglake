package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * What the O(columns) drop did NOT change, asserted rather than
 * assumed (#193).
 *
 * A change that removes two statements from a transaction is exactly
 * the kind that quietly removes a guarantee with them. These are the
 * two guarantees the drop still has to carry, and neither has a
 * mutation claimed against it: they are properties this change had to
 * PRESERVE, and the point of pinning them is that the next person
 * touching the drop finds them written down.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DropConcurrencyIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val catalogs = CatalogService(jdbi)
    private val commits = CommitService(jdbi)

    @AfterAll
    fun tearDown() = db.close()

    private fun seed(catalog: String): Pair<Long, String> {
        catalogs.createCatalog(catalog, "s3://bucket/$catalog")
        catalogs.createNamespace(catalog, "ns")
        catalogs.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG, nullable = false)))
        val path = "s3://bucket/$catalog/f.parquet"
        commits.commit(
            catalog,
            CommitRequest(appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(path, 10, 100))))),
        )
        return catalogs.getCatalog(catalog).catalogId to path
    }

    @Test
    fun `a drop still serializes behind the per-catalog commit lock`() {
        // The drop no longer writes file rows, but it still ALLOCATES A
        // SNAPSHOT, and invariant 1 says snapshot ids are assigned
        // inside the per-catalog advisory-lock commit tail. Dropping
        // the file-row writes must not have turned the drop into
        // something that skips the lock: a drop that raced a commit
        // could give the drop a lower snapshot id than a commit that
        // preceded it, and the change log would be out of order.
        val catalog = "drop-lock"
        val (catalogId, _) = seed(catalog)

        val holding = CountDownLatch(1)
        val release = CountDownLatch(1)
        val dropFinishedAt = AtomicLong(0)
        val holder =
            Thread({
                jdbi.useHandleUnchecked { h ->
                    h.begin()
                    // The REAL key, from the production helper: a copy
                    // of this SQL that computed a different key would
                    // make this test pass while proving nothing.
                    Locks.acquireCatalogCommitLock(h, catalogId)
                    holding.countDown()
                    release.await(60, TimeUnit.SECONDS)
                    h.rollback()
                }
            }, "lock-holder")

        var dropSnapshot = 0L
        val dropper =
            Thread({
                dropSnapshot = catalogs.dropTable(catalog, "ns", "t").snapshotId
                dropFinishedAt.set(System.nanoTime())
            }, "dropper")
        try {
            holder.start()
            assertThat(holding.await(60, TimeUnit.SECONDS)).isTrue()
            dropper.start()
            // The drop is now milliseconds of work, so if it were NOT
            // serialized it would finish immediately. Half a second is
            // two orders of magnitude more than it needs.
            dropper.join(500)
            assertThat(dropper.isAlive)
                .describedAs("the drop must queue behind the commit lock, however cheap it has become")
                .isTrue()
        } finally {
            release.countDown()
        }
        holder.join(60_000)
        dropper.join(60_000)
        assertThat(dropSnapshot).isGreaterThan(0)
        assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(dropSnapshot)
    }

    @Test
    fun `a dropped table's paths still fence the cleanup drain and an upload claim`() {
        // THE FIRST CARVE-OUT OF THE INVARIANT, and the one that keeps
        // the whole design safe. "No code path may treat a dropped
        // table's file rows as reachable data" has an exception for
        // PATH-KEYED liveness: `CleanupService.referencedPaths` and
        // `UploadService`'s probes ask "does ANY row name this path",
        // live or not, and they MUST keep seeing a dropped table's rows
        // — that is what stops the drain from deleting the objects
        // before retirement has queued them.
        //
        // Before #193 a dropped table's rows were end-snapshotted but
        // still present, so this held by accident. It still holds, and
        // for the same reason (the check is not filtered on
        // `end_snapshot`, and V17's indexes are deliberately not
        // partial) — but the population it now protects is much larger
        // and the consequence of getting it wrong is deleting a live
        // table's worth of objects.
        val catalog = "drop-fence"
        val (catalogId, path) = seed(catalog)
        val claimPath = "s3://bucket/$catalog/claimed.parquet"
        val owner = UUID.randomUUID()
        jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind, state) " +
                    "VALUES (:c, gen_random_uuid(), :o, 's3://bucket', :p, 'data', 'active')",
            ).bind("c", catalogId).bind("o", owner).bind("p", claimPath).execute()
        }

        catalogs.dropTable(catalog, "ns", "t")

        // The reference check's own question, asked the way the drain
        // asks it. Kept as the drain's three-leg UNION rather than a
        // single-table probe, because what is being asserted is the
        // ANSWER the drain would get.
        val referenced =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT path FROM hog_data_file
                    WHERE catalog_id = :catalogId AND path = ANY(:paths)
                    UNION
                    SELECT path FROM hog_delete_file
                    WHERE catalog_id = :catalogId AND path = ANY(:paths)
                    UNION
                    SELECT path FROM hog_upload
                    WHERE catalog_id = :catalogId AND path = ANY(:paths) AND state = 'active'
                    """,
                ).bind("catalogId", catalogId)
                    .bindArray("paths", String::class.java, listOf(path, claimPath))
                    .mapTo(String::class.java).list().toSet()
            }
        assertThat(referenced)
            .describedAs(
                "a dropped table's object is still referenced until retirement queues it, and an " +
                    "active upload claim is unaffected by a drop of the table it targets",
            )
            .containsExactlyInAnyOrder(path, claimPath)

        // The claim is untouched: a drop settles no upload claim, and
        // it never did. The claim's own state machine (its sweep, its
        // owner fence) is what resolves it.
        val claimState =
            jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT state FROM hog_upload WHERE catalog_id = :c AND path = :p")
                    .bind("c", catalogId).bind("p", claimPath).mapTo(String::class.java).one()
            }
        assertThat(claimState).isEqualTo("active")

        // ...and once retirement HAS queued the path, the row is gone
        // and the fence is the queue entry instead — which is the
        // handover the whole ordering exists to make atomic.
        jdbi.useHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_catalog SET earliest_snapshot_id = last_snapshot_id WHERE catalog_id = :c")
                .bind("c", catalogId).execute()
        }
        RetirementService(jdbi, batchSize = 10, pauseMs = 0).runOnce(catalog)
        val afterRetirement =
            jdbi.withHandleUnchecked { h ->
                val stillAFile =
                    h.createQuery("SELECT count(*) FROM hog_data_file WHERE catalog_id = :c AND path = :p")
                        .bind("c", catalogId).bind("p", path).mapTo(Long::class.java).one()
                val queuedUndrained =
                    h.createQuery(
                        "SELECT count(*) FROM hog_file_removal WHERE catalog_id = :c AND path = :p " +
                            "AND drained_at IS NULL",
                    ).bind("c", catalogId).bind("p", path).mapTo(Long::class.java).one()
                stillAFile to queuedUndrained
            }
        assertThat(afterRetirement.first).isZero()
        assertThat(afterRetirement.second)
            .describedAs("the path must never be unreferenced AND unqueued, not even for an instant")
            .isEqualTo(1)
    }
}
