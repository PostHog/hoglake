package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.UUID

@Tag("integration")
class AtomicChangefeedIntegrationTest {
    @Test
    fun `creation snapshot exposes both change kinds and initial files exactly once`() {
        PgTestSupport.freshDatabase().use { db ->
            val catalogs = CatalogService(db.jdbi)
            val creations = TableCreationService(db.jdbi, catalogs, CommitService(db.jdbi))
            catalogs.createCatalog("feed", "s3://bucket/feed")
            catalogs.createNamespace("feed", "test")
            val before = catalogs.getCatalog("feed").headSnapshotId
            val prepared =
                creations.prepare(
                    "feed",
                    UUID.randomUUID(),
                    TableCreationDefinition("test", "target", listOf(ColumnDef("id", ColType.LONG))),
                )
            val files = listOf(FileRegistration(prepared.writePath + "part.parquet", 5, 100, 20))
            val committed = creations.publish("feed", prepared.operationId, files)
            val page = catalogs.listSnapshots("feed", before, 10).first
            assertThat(page).hasSize(1)
            assertThat(page.single().changes.map { it.kind }).containsExactlyInAnyOrder(
                ChangeKind.TABLE_CREATED,
                ChangeKind.TABLE_INSERTED_INTO,
            )
            assertThat(page.single().changes.map { it.objectId }.toSet()).hasSize(1)
            val changes = catalogs.changes("feed", "test", "target", before)
            assertThat(changes.tableUuid).isEqualTo(prepared.tableUuid)
            assertThat(changes.files.map { it.path }).containsExactly(files.single().path)
            assertThat(changes.files.single().rowIdStart).isZero()
            assertThat(changes.files.single().recordCount).isEqualTo(5)
            creations.publish("feed", prepared.operationId, files)
            assertThat(catalogs.listSnapshots("feed", before, 10).first).hasSize(1)
            assertThat(catalogs.changes("feed", "test", "target", committed.snapshotId!!).files).isEmpty()
        }
    }
}
