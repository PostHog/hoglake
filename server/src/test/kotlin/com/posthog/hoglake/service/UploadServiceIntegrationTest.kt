package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UploadServiceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val uploads = UploadService(db.jdbi)
    private val commits = CommitService(db.jdbi)

    @AfterAll
    fun close() = db.close()

    private fun catalog(): String {
        val name = "upload-" + UUID.randomUUID().toString().replace("-", "")
        catalogs.createCatalog(name, "s3://bucket/$name")
        catalogs.createNamespace(name, "test")
        catalogs.createTable(name, "test", "target", listOf(ColumnDef("id", ColType.LONG)))
        return name
    }

    private fun claim(
        catalog: String,
        owner: UUID,
        kind: String = "data",
    ) = uploads.claim(catalog, UUID.randomUUID(), owner, catalogs.getCatalog(catalog).dataPath, kind)

    private fun request(
        catalog: String,
        owner: UUID,
        claim: UploadClaim,
    ) = CommitRequest(
        readSnapshot = catalogs.getCatalog(catalog).headSnapshotId,
        appends =
            listOf(
                TableAppend(
                    "test",
                    "target",
                    listOf(FileRegistration(claim.path, 1, 100, 20)),
                    catalogs.getTable(catalog, "test", "target").tableUuid,
                ),
            ),
        idempotencyKey = owner,
    )

    private fun expire(claim: UploadClaim) =
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_upload SET expires_at = now() - interval '1 second' WHERE upload_id = :id")
                .bind("id", claim.uploadId).execute()
        }

    @Test
    fun `retained registered objects can be referenced again but never revived after expiry`() {
        val catalog = catalog()
        val owner = UUID.randomUUID()
        val claim = claim(catalog, owner)
        commits.commit(catalog, request(catalog, owner, claim))
        commits.commit(catalog, request(catalog, UUID.randomUUID(), claim))
        assertThat(catalogs.getTable(catalog, "test", "target").recordCount).isEqualTo(2)
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("DELETE FROM hog_data_file WHERE path = :path").bind("path", claim.path).execute()
        }
        assertThatThrownBy { commits.commit(catalog, request(catalog, UUID.randomUUID(), claim)) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
    }

    @Test
    fun `expiry and publication race has exactly one winner`() {
        val pool = java.util.concurrent.Executors.newFixedThreadPool(2)
        try {
            repeat(10) {
                val catalog = catalog()
                val owner = UUID.randomUUID()
                val claim = claim(catalog, owner)
                val request = request(catalog, owner, claim)
                expire(claim)
                val start = java.util.concurrent.CountDownLatch(1)
                val publication =
                    pool.submit<Boolean> {
                        start.await()
                        try {
                            commits.commit(catalog, request)
                            true
                        } catch (_: HoglakeException.CommitConflict) {
                            false
                        }
                    }
                val cleanup =
                    pool.submit<Int> {
                        start.await()
                        uploads.scheduleExpired(catalog)
                    }
                start.countDown()
                val committed = publication.get(10, java.util.concurrent.TimeUnit.SECONDS)
                val queued = cleanup.get(10, java.util.concurrent.TimeUnit.SECONDS)
                assertThat(queued).isEqualTo(if (committed) 0 else 1)
                assertThat(uploads.claim(catalog, claim.uploadId, owner, claim.prefix, "data").state)
                    .isEqualTo(if (committed) "registered" else "abandoned")
            }
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `claim replay preserves path and renewal cannot revive a fence`() {
        val catalog = catalog()
        val owner = UUID.randomUUID()
        val claim = claim(catalog, owner)
        assertThat(uploads.claim(catalog, claim.uploadId, owner, claim.prefix, "data")).isEqualTo(claim)
        assertThatThrownBy { uploads.claim(catalog, claim.uploadId, UUID.randomUUID(), claim.prefix, "data") }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThatThrownBy { uploads.claim(catalog, claim.uploadId, owner, claim.prefix, "delete") }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThatThrownBy { uploads.claim(catalog, UUID.randomUUID(), owner, claim.prefix + "/../victim", "data") }
            .isInstanceOf(HoglakeException.Validation::class.java)
        expire(claim)
        assertThat(uploads.renew(catalog, owner)).isEqualTo(1)
        assertThat(uploads.scheduleExpired(catalog)).isZero()
        expire(claim)
        assertThat(uploads.scheduleExpired(catalog)).isEqualTo(1)
        assertThat(uploads.renew(catalog, owner)).isZero()
    }

    @Test
    fun `publication and unknown-response replay survive cleanup and table drop`() {
        val catalog = catalog()
        val owner = UUID.randomUUID()
        val claim = claim(catalog, owner)
        val request = request(catalog, owner, claim)
        val committed = commits.commit(catalog, request)
        expire(claim)
        assertThat(uploads.abandon(catalog, owner, listOf(claim.path))).isZero()
        assertThat(uploads.scheduleExpired(catalog)).isZero()
        catalogs.dropTable(catalog, "test", "target")
        assertThat(commits.commit(catalog, request)).isEqualTo(committed)
        assertThat(uploads.claim(catalog, claim.uploadId, owner, claim.prefix, "data").state).isEqualTo("registered")
    }

    @Test
    fun `expired upload remains fenced after removal ledger purges and rescheduling is bounded`() {
        val catalog = catalog()
        val owner = UUID.randomUUID()
        val claim = claim(catalog, owner)
        val request = request(catalog, owner, claim)
        val head = catalogs.getCatalog(catalog).headSnapshotId
        expire(claim)
        assertThat(uploads.scheduleExpired(catalog)).isEqualTo(1)
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("DELETE FROM hog_file_removal WHERE path = :path").bind("path", claim.path).execute()
        }
        assertThatThrownBy {
            commits.commit(
                catalog,
                request,
            )
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(head)
        // A tombstone RESTS for the drained-ledger retention between
        // offers: sweeping every abandoned row on every call re-queued
        // paths whose previous removal row was still in the ledger, and
        // made the sweep's cost grow with the catalog's upload history.
        assertThat(uploads.scheduleExpired(catalog)).isZero()
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_upload SET last_scheduled_at = now() - interval '60 days' WHERE upload_id = :id",
            ).bind("id", claim.uploadId).execute()
        }
        assertThat(uploads.scheduleExpired(catalog)).isEqualTo(1)
        val queued =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_file_removal WHERE path = :path")
                    .bind("path", claim.path).mapTo(Long::class.java).one()
            }
        assertThat(queued).isEqualTo(1)
    }

    // ---- the commit lock is not a claim's business --------------------------
    //
    // A writer claims one upload per OUTPUT FILE, so taking the per-catalog
    // commit lock (unbounded, with no admission timeout) once per claim
    // taxed the catalog's whole write throughput for nothing.

    @Test
    fun `claim renew and abort do not wait on the catalog commit lock`() {
        val catalog = catalog()
        val owner = UUID.randomUUID()
        val existing = claim(catalog, owner)
        val catalogId = catalogs.getCatalog(catalog).catalogId
        val holder = db.jdbi.open()
        try {
            holder.begin()
            com.posthog.hoglake.persistence.Locks.acquireCatalogCommitLock(holder, catalogId)
            // Each of these used to queue behind the held lock forever. The
            // timeout is the assertion: it fails the test rather than hanging.
            val pool = java.util.concurrent.Executors.newSingleThreadExecutor()
            try {
                fun within(block: () -> Unit) = pool.submit(block).get(15, java.util.concurrent.TimeUnit.SECONDS)
                within { assertThat(claim(catalog, owner).state).isEqualTo("active") }
                within { assertThat(uploads.renew(catalog, owner)).isEqualTo(2) }
                within { assertThat(uploads.abandon(catalog, owner, listOf(existing.path))).isEqualTo(1) }
                within { assertThat(uploads.scheduleExpired(catalog)).isEqualTo(1) }
            } finally {
                pool.shutdownNow()
            }
        } finally {
            holder.rollback()
            holder.close()
        }
    }

    @Test
    fun `a path a file row still claims is never queued for removal`() {
        val catalog = catalog()
        val owner = UUID.randomUUID()
        val claim = claim(catalog, owner)
        commits.commit(catalog, request(catalog, owner, claim))
        // A registered claim is not a candidate at all, so force the state a
        // pre-fix race could leave behind: a tombstone over a LIVE path.
        // Queueing it would show up in every cleanup drain as a
        // still_referenced invariant violation — an alert, forever.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_upload SET state = 'abandoned' WHERE upload_id = :id")
                .bind("id", claim.uploadId).execute()
        }
        assertThat(uploads.scheduleExpired(catalog)).isEqualTo(1)
        val queued =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_file_removal WHERE path = :path")
                    .bind("path", claim.path).mapTo(Long::class.java).one()
            }
        assertThat(queued).isZero()
    }

    @Test
    fun `a renewal that beats the sweep keeps its claim and its path`() {
        val catalog = catalog()
        val owner = UUID.randomUUID()
        val claim = claim(catalog, owner)
        // The candidate predicate now matches: an expired active claim.
        expire(claim)
        // ... and then the writer renews, which renewUploads promises wins
        // against expiry. Before the fence re-checked the candidate
        // predicate (it only excluded 'registered'), the sweep abandoned
        // the renewed claim and queued its path, and the writer's eventual
        // register 409'd. The commit lock used to serialize these two.
        assertThat(uploads.renew(catalog, owner)).isEqualTo(1)

        assertThat(uploads.scheduleExpired(catalog)).isZero()
        assertThat(uploads.claim(catalog, claim.uploadId, owner, claim.prefix, "data").state)
            .isEqualTo("active")
        val queued =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_file_removal WHERE path = :path")
                    .bind("path", claim.path).mapTo(Long::class.java).one()
            }
        assertThat(queued).isZero()
        // The claim is still publishable, which is the point.
        commits.commit(catalog, request(catalog, owner, claim))
    }

    @Test
    fun `a renewal landing between candidate selection and the fence still wins`() {
        val catalog = catalog()
        val owner = UUID.randomUUID()
        val claim = claim(catalog, owner)
        expire(claim)
        val holder = db.jdbi.open()
        val pool = java.util.concurrent.Executors.newSingleThreadExecutor()
        try {
            // Pin the claim row so the sweep's fence has to queue behind
            // this transaction. The candidate query takes no lock, so the
            // sweep has ALREADY chosen this claim by then — which is the
            // window the commit lock used to close.
            holder.begin()
            holder.createQuery("SELECT state FROM hog_upload WHERE upload_id = :id FOR UPDATE")
                .bind("id", claim.uploadId).mapTo(String::class.java).one()
            val sweep = pool.submit<Int> { uploads.scheduleExpired(catalog) }
            assertThatThrownBy { sweep.get(2, java.util.concurrent.TimeUnit.SECONDS) }
                .isInstanceOf(java.util.concurrent.TimeoutException::class.java)

            // The renewal commits inside that window. This is the statement
            // renew() issues; running it here is the only way to place it
            // between the sweep's two steps deterministically.
            holder.createUpdate(
                """
                UPDATE hog_upload SET expires_at = now() + interval '24 hours'
                WHERE upload_id = :id AND state = 'active'
                """,
            ).bind("id", claim.uploadId).execute()
            holder.commit()

            // READ COMMITTED re-evaluates the fence's WHERE after the wait.
            // It re-checks the CANDIDATE predicate, not just 'registered',
            // so the renewed claim no longer qualifies and is left alone.
            assertThat(sweep.get(10, java.util.concurrent.TimeUnit.SECONDS)).isZero()
            assertThat(uploads.claim(catalog, claim.uploadId, owner, claim.prefix, "data").state)
                .isEqualTo("active")
            val queued =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT count(*) FROM hog_file_removal WHERE path = :path")
                        .bind("path", claim.path).mapTo(Long::class.java).one()
                }
            assertThat(queued).isZero()
            commits.commit(catalog, request(catalog, owner, claim))
        } finally {
            pool.shutdownNow()
            // Rollback BEFORE close: closing a handle with an open
            // transaction throws a JDBI TransactionException, which on the
            // failure path replaces the assertion error that caused it.
            if (holder.isInTransaction) holder.rollback()
            holder.close()
        }
    }

    @Test
    fun `register takes the claim row lock so a concurrent fence cannot be lost`() {
        val catalog = catalog()
        val owner = UUID.randomUUID()
        val claim = claim(catalog, owner)
        val request = request(catalog, owner, claim)
        val holder = db.jdbi.open()
        val pool = java.util.concurrent.Executors.newSingleThreadExecutor()
        try {
            holder.begin()
            // Stand in for a claim row another transaction is settling.
            holder.createQuery("SELECT state FROM hog_upload WHERE upload_id = :id FOR UPDATE")
                .bind("id", claim.uploadId).mapTo(String::class.java).one()

            val publication =
                pool.submit<Boolean> {
                    try {
                        commits.commit(catalog, request)
                        true
                    } catch (_: HoglakeException.CommitConflict) {
                        false
                    }
                }
            // register's OWN `FOR UPDATE` must queue behind the holder.
            // Without it, register reads 'active' immediately, publishes,
            // and its final UPDATE — keyed on upload_id alone — silently
            // revives the row the fence below is about to write.
            assertThatThrownBy { publication.get(2, java.util.concurrent.TimeUnit.SECONDS) }
                .isInstanceOf(java.util.concurrent.TimeoutException::class.java)

            holder.createUpdate("UPDATE hog_upload SET state = 'abandoned' WHERE upload_id = :id")
                .bind("id", claim.uploadId).execute()
            holder.commit()

            assertThat(publication.get(10, java.util.concurrent.TimeUnit.SECONDS)).isFalse()
            assertThat(uploads.claim(catalog, claim.uploadId, owner, claim.prefix, "data").state)
                .isEqualTo("abandoned")
        } finally {
            pool.shutdownNow()
            // Rollback BEFORE close: closing a handle with an open
            // transaction throws a JDBI TransactionException, which on the
            // failure path replaces the assertion error that caused it.
            if (holder.isInTransaction) holder.rollback()
            holder.close()
        }
    }

    @Test
    fun `a deletion-vector path a delete file row still claims is never queued`() {
        val catalog = catalog()
        val owner = UUID.randomUUID()
        val data = claim(catalog, owner)
        commits.commit(catalog, request(catalog, owner, data))
        val dataFileId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT data_file_id FROM hog_data_file WHERE path = :path")
                    .bind("path", data.path).mapTo(Long::class.java).one()
            }
        // A distinct owner: the commit path authenticates an upload claim
        // against the request's idempotency_key, and `owner` is already
        // spent on the append above.
        val vectorOwner = UUID.randomUUID()
        val vector = claim(catalog, vectorOwner, "delete")
        commits.commit(
            catalog,
            CommitRequest(
                readSnapshot = catalogs.getCatalog(catalog).headSnapshotId,
                deletes =
                    listOf(
                        com.posthog.hoglake.model.TableDeletes(
                            "test",
                            "target",
                            listOf(com.posthog.hoglake.model.DeleteFileRegistration(dataFileId, vector.path, 1, 20)),
                            catalogs.getTable(catalog, "test", "target").tableUuid,
                        ),
                    ),
                idempotencyKey = vectorOwner,
            ),
        )
        // Same forced state as the data-file case: a tombstone over a path
        // a LIVE hog_delete_file row still references.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_upload SET state = 'abandoned' WHERE upload_id = :id")
                .bind("id", vector.uploadId).execute()
        }
        assertThat(uploads.scheduleExpired(catalog)).isEqualTo(1)
        val queued =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_file_removal WHERE path = :path")
                    .bind("path", vector.path).mapTo(Long::class.java).one()
            }
        assertThat(queued).isZero()
    }

    @Test
    fun `the sweep settles each candidate in its own transaction`() {
        val catalog = catalog()
        val owner = UUID.randomUUID()
        val first = claim(catalog, owner)
        val second = claim(catalog, owner)
        listOf(first, second).forEach(::expire)
        // Force the order the sweep will visit them in, so "the other one"
        // is deterministic rather than whichever uuid sorts first.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_upload SET last_scheduled_at = now() - interval '60 days' WHERE upload_id = :id")
                .bind("id", second.uploadId).execute()
        }
        val holder = db.jdbi.open()
        val pool = java.util.concurrent.Executors.newSingleThreadExecutor()
        try {
            // Pin the SECOND candidate. If the sweep ran as one
            // transaction over the whole batch, the first candidate's
            // fence and removal row would be invisible to everyone else
            // until this holder let go — and a commit registering the
            // first path would be queued behind an unrelated claim while
            // holding the catalog commit lock, which is the convoy that
            // taking the lock off this path was meant to remove.
            holder.begin()
            holder.createQuery("SELECT state FROM hog_upload WHERE upload_id = :id FOR UPDATE")
                .bind("id", second.uploadId).mapTo(String::class.java).one()
            val sweep = pool.submit<Int> { uploads.scheduleExpired(catalog) }
            assertThatThrownBy { sweep.get(2, java.util.concurrent.TimeUnit.SECONDS) }
                .isInstanceOf(java.util.concurrent.TimeoutException::class.java)

            // A THIRD connection, while the sweep is still blocked.
            db.jdbi.withHandleUnchecked { h ->
                assertThat(
                    h.createQuery("SELECT state FROM hog_upload WHERE upload_id = :id")
                        .bind("id", first.uploadId).mapTo(String::class.java).one(),
                ).describedAs("first candidate already committed").isEqualTo("abandoned")
                assertThat(
                    h.createQuery("SELECT count(*) FROM hog_file_removal WHERE path = :path")
                        .bind("path", first.path).mapTo(Long::class.java).one(),
                ).describedAs("and its removal row is visible").isEqualTo(1)
            }

            holder.rollback()
            assertThat(sweep.get(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(2)
        } finally {
            pool.shutdownNow()
            // Rollback BEFORE close: closing a handle with an open
            // transaction throws a JDBI TransactionException, which on the
            // failure path replaces the assertion error that caused it.
            if (holder.isInTransaction) holder.rollback()
            holder.close()
        }
    }

    @Test
    fun `selection is bounded by limit and rotates by last scheduling time`() {
        val catalog = catalog()
        val owner = UUID.randomUUID()
        val claims = (1..3).map { claim(catalog, owner) }
        claims.forEach(::expire)
        // Distinct last_scheduled_at values, so "NULLS FIRST, upload_id" has
        // something to order by other than the random uuids.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                "UPDATE hog_upload SET last_scheduled_at = now() - interval '61 days' WHERE upload_id = :id",
            ).bind("id", claims[1].uploadId).execute()
            h.createUpdate(
                "UPDATE hog_upload SET last_scheduled_at = now() - interval '60 days' WHERE upload_id = :id",
            ).bind("id", claims[2].uploadId).execute()
        }

        // NULLS FIRST: the untouched claim goes first, then the oldest
        // last_scheduled_at. limit stops the batch after two.
        assertThat(uploads.scheduleExpired(catalog, limit = 2)).isEqualTo(2)

        fun queued(path: String) =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_file_removal WHERE path = :path")
                    .bind("path", path).mapTo(Long::class.java).one()
            }
        assertThat(queued(claims[0].path)).describedAs("never scheduled -> first").isEqualTo(1)
        assertThat(queued(claims[1].path)).describedAs("oldest last_scheduled_at -> second").isEqualTo(1)
        assertThat(queued(claims[2].path)).describedAs("beyond the limit -> untouched this call").isZero()

        // The remainder is picked up next time: the two just fenced now
        // carry a fresh last_scheduled_at and are resting.
        assertThat(uploads.scheduleExpired(catalog, limit = 2)).isEqualTo(1)
        assertThat(queued(claims[2].path)).isEqualTo(1)
        assertThat(uploads.scheduleExpired(catalog, limit = 2)).isZero()
    }

    @Test
    fun `a publication racing the sweep never leaves a live path queued`() {
        val pool = java.util.concurrent.Executors.newFixedThreadPool(2)
        try {
            repeat(10) {
                val catalog = catalog()
                val owner = UUID.randomUUID()
                val claim = claim(catalog, owner)
                val request = request(catalog, owner, claim)
                expire(claim)
                val start = java.util.concurrent.CountDownLatch(1)
                val publication =
                    pool.submit<Boolean> {
                        start.await()
                        try {
                            commits.commit(catalog, request)
                            true
                        } catch (_: HoglakeException.CommitConflict) {
                            false
                        }
                    }
                val sweep =
                    pool.submit<Int> {
                        start.await()
                        uploads.scheduleExpired(catalog)
                    }
                start.countDown()
                publication.get(10, java.util.concurrent.TimeUnit.SECONDS)
                sweep.get(10, java.util.concurrent.TimeUnit.SECONDS)
                // The invariant, whichever side won: no undrained removal row
                // for a path a file row references.
                val violating =
                    db.jdbi.withHandleUnchecked { h ->
                        h.createQuery(
                            """
                            SELECT count(*) FROM hog_file_removal r
                            JOIN hog_catalog c ON c.catalog_id = r.catalog_id
                            WHERE c.name = :cat AND r.drained_at IS NULL
                              AND EXISTS (SELECT 1 FROM hog_data_file f
                                          WHERE f.catalog_id = r.catalog_id AND f.path = r.path)
                            """,
                        ).bind("cat", catalog).mapTo(Long::class.java).one()
                    }
                assertThat(violating).isZero()
            }
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `wrong owner kind and stale publication cannot consume ownership`() {
        val catalog = catalog()
        val owner = UUID.randomUUID()
        val data = claim(catalog, owner)
        assertThatThrownBy { commits.commit(catalog, request(catalog, UUID.randomUUID(), data)) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
        val delete = claim(catalog, owner, "delete")
        assertThatThrownBy { commits.commit(catalog, request(catalog, owner, delete)) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
        val stale = request(catalog, owner, data)
        AlterService(db.jdbi).alterTable(
            catalog,
            "test",
            "target",
            listOf(com.posthog.hoglake.model.AlterOp.SetTableComment("new")),
        )
        assertThatThrownBy { commits.commit(catalog, stale) }.isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(uploads.claim(catalog, data.uploadId, owner, data.prefix, "data").state).isEqualTo("active")
        commits.commit(catalog, request(catalog, owner, data))
    }

    @Test
    fun `atomic creation registers its upload owner with its publication receipt`() {
        val catalog = catalog()
        val creations = TableCreationService(db.jdbi, catalogs, commits)
        val owner = UUID.randomUUID()
        val operation =
            creations.prepare(
                catalog,
                owner,
                TableCreationDefinition("test", "created", listOf(ColumnDef("id", ColType.LONG))),
            )
        val claim = uploads.claim(catalog, UUID.randomUUID(), owner, operation.writePath, "data")
        val files = listOf(FileRegistration(claim.path, 1, 100, 20))
        val committed = creations.publish(catalog, owner, files)
        assertThat(creations.publish(catalog, owner, files)).isEqualTo(committed)
        assertThat(uploads.abandon(catalog, owner, listOf(claim.path))).isZero()
        assertThat(uploads.scheduleExpired(catalog)).isZero()
    }
}
