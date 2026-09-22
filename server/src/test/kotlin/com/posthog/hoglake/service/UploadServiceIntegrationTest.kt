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
        assertThat(uploads.scheduleExpired(catalog)).isEqualTo(1)
        assertThat(uploads.scheduleExpired(catalog)).isEqualTo(1)
        val queued =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_file_removal WHERE path = :path")
                    .bind("path", claim.path).mapTo(Long::class.java).one()
            }
        assertThat(queued).isEqualTo(1)
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
