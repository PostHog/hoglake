package com.posthog.hoglake.commit

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.UploadService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TransactionCommitIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val uploads = UploadService(db.jdbi)

    @AfterAll
    fun close() = db.close()

    private fun request(): Pair<String, CommitRequest> {
        val catalog = "transaction-" + UUID.randomUUID()
        val prefix = "s3://bucket/$catalog"
        catalogs.createCatalog(catalog, prefix)
        catalogs.createNamespace(catalog, "test")
        val appends = mutableListOf<TableAppend>()
        val deletes = mutableListOf<TableDeletes>()
        val owner = UUID.randomUUID()
        for (table in listOf("a", "b")) {
            catalogs.createTable(catalog, "test", table, listOf(ColumnDef("id", ColType.LONG)))
            val uuid = catalogs.getTable(catalog, "test", table).tableUuid
            val data = uploads.claim(catalog, UUID.randomUUID(), owner, prefix, "data")
            val vector = uploads.claim(catalog, UUID.randomUUID(), owner, prefix, "delete")
            appends += TableAppend("test", table, listOf(FileRegistration(data.path, 10, 100, 20)), uuid)
            val deletion = DeleteFileRegistration(0, vector.path, 3, 20, data.path)
            deletes += TableDeletes("test", table, listOf(deletion), uuid)
        }
        return catalog to
            CommitRequest(
                readSnapshot = catalogs.getCatalog(catalog).headSnapshotId,
                appends = appends,
                deletes = deletes,
                idempotencyKey = owner,
                requireUnchangedTables = true,
                allowPendingDeletes = true,
            )
    }

    @Test
    fun `two tables and staged deletes publish atomically and replay once`() {
        val (catalog, request) = request()
        val result = commits.commit(catalog, request)
        assertThat(result.snapshotId).isEqualTo(request.readSnapshot!! + 1)
        assertThat(commits.commit(catalog, request)).isEqualTo(result)
        assertThat(commits.receipt(catalog, request.idempotencyKey!!)).isEqualTo(result)
        for (table in listOf("a", "b")) {
            assertThat(catalogs.getTable(catalog, "test", table).recordCount).isEqualTo(10)
        }
        db.jdbi.withHandleUnchecked { h ->
            assertThat(
                h.createQuery("SELECT count(*) FROM hog_upload WHERE owner = :owner AND state = 'registered'")
                    .bind("owner", request.idempotencyKey).mapTo(Long::class.java).one(),
            ).isEqualTo(4)
        }
        db.jdbi.withHandleUnchecked { h ->
            val deleted =
                h.createQuery(
                    """
                SELECT sum(d.delete_count) FROM hog_delete_file d JOIN hog_catalog c USING (catalog_id)
                WHERE c.name = :catalog AND d.end_snapshot IS NULL
                """,
                ).bind("catalog", catalog).mapTo(Long::class.java).one()
            assertThat(deleted).isEqualTo(6)
        }
        val changed =
            request.copy(
                deletes =
                    request.deletes.map {
                        it.copy(
                            files =
                                it.files.map {
                                        f ->
                                    f.copy(deleteCount = 4)
                                },
                        )
                    },
            )
        assertThatThrownBy {
            commits.commit(
                catalog,
                changed,
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `invalid private reference rolls back every table allocator and upload settlement`() {
        val (catalog, request) = request()
        val broken =
            request.copy(
                deletes =
                    request.deletes.map {
                        it.copy(
                            files =
                                it.files.map {
                                        f ->
                                    f.copy(dataFilePath = "s3://bucket/missing")
                                },
                        )
                    },
            )
        assertThatThrownBy { commits.commit(catalog, broken) }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(request.readSnapshot)
        assertThat(catalogs.getTable(catalog, "test", "a").recordCount).isZero()
        db.jdbi.withHandleUnchecked { h ->
            assertThat(
                h.createQuery("SELECT count(*) FROM hog_upload WHERE owner = :owner AND state = 'active'")
                    .bind("owner", request.idempotencyKey).mapTo(Long::class.java).one(),
            ).isEqualTo(4)
        }
        commits.commit(catalog, request)
    }

    @Test
    fun `ordinary endpoints reject private references and aliases cannot select ambiguous appends`() {
        val (catalog, request) = request()
        assertThatThrownBy { commits.commit(catalog, request.copy(allowPendingDeletes = false)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        val duplicate = request.copy(appends = request.appends.map { it.copy(files = it.files + it.files) })
        assertThatThrownBy { commits.commit(catalog, duplicate) }.isInstanceOf(HoglakeException.Validation::class.java)
        val wrongTable =
            request.copy(
                deletes = listOf(request.deletes.first().copy(files = request.deletes.last().files)),
            )
        assertThatThrownBy { commits.commit(catalog, wrongTable) }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(request.readSnapshot)
    }

    @Test
    fun `concurrent change to either target prevents all publication`() {
        val (catalog, request) = request()
        val append = request.appends.first()
        commits.commit(
            catalog,
            CommitRequest(
                appends =
                    listOf(
                        append.copy(files = listOf(FileRegistration("s3://bucket/$catalog/concurrent", 1, 100, 20))),
                    ),
            ),
        )
        val head = catalogs.getCatalog(catalog).headSnapshotId
        assertThatThrownBy {
            commits.commit(
                catalog,
                request,
            )
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(head)
        assertThat(catalogs.getTable(catalog, "test", "b").recordCount).isZero()
    }
}
