package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.UUID

@Tag("integration")
class TableCreationRowLockIntegrationTest {
    @Test
    fun `status and abort bound their wait for an operation row`() {
        PgTestSupport.freshDatabase().use { db ->
            val catalogs = CatalogService(db.jdbi)
            val creations = TableCreationService(db.jdbi, catalogs, CommitService(db.jdbi), 100)
            catalogs.createCatalog("row-lock", "s3://bucket/row-lock")
            catalogs.createNamespace("row-lock", "test")
            val prepared =
                creations.prepare(
                    "row-lock",
                    UUID.randomUUID(),
                    TableCreationDefinition("test", "target", listOf(ColumnDef("id", ColType.LONG))),
                )
            db.jdbi.open().use { holder ->
                holder.begin()
                holder.createQuery("SELECT operation_id FROM hog_table_creation WHERE operation_id = :id FOR UPDATE")
                    .bind("id", prepared.operationId).mapTo(UUID::class.java).one()
                try {
                    assertThatThrownBy { creations.status("row-lock", prepared.operationId) }
                        .isInstanceOf(HoglakeException.CommitQueueTimeout::class.java)
                    assertThatThrownBy { creations.abort("row-lock", prepared.operationId) }
                        .isInstanceOf(HoglakeException.CommitQueueTimeout::class.java)
                } finally {
                    holder.rollback()
                }
            }
            assertThat(creations.status("row-lock", prepared.operationId).state).isEqualTo("prepared")
        }
    }
}
