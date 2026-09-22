package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.flywaydb.core.api.MigrationVersion
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("integration")
class V13NumericCatalogNamesMigrationIntegrationTest {
    @Test
    fun `upgrade preserves existing catalog and admits UUID names`(): Unit =
        PgTestSupport.freshDatabaseRaw("").use { db ->
            Database.flywayConfig(db.dataSource).target(MigrationVersion.fromVersion("12")).load().migrate()
            val service = CatalogService(db.jdbi)
            service.createCatalog("existing", "s3://bucket/existing/")
            service.createNamespace("existing", "main")
            val existing = service.getCatalog("existing")
            val uuid = "01234567-89ab-4cde-8fab-0123456789ab"
            assertThatThrownBy { service.createCatalog(uuid, "s3://bucket/uuid/") }
                .isInstanceOf(HoglakeException.Validation::class.java)

            Database.flywayConfig(db.dataSource).load().migrate()

            assertThat(service.getCatalog("existing")).isEqualTo(existing)
            val created = service.createCatalog(uuid, "s3://bucket/uuid/")
            assertThat(service.getCatalog(uuid)).isEqualTo(created)
            assertThatThrownBy { service.createCatalog("_invalid", "s3://bucket/invalid/") }
                .isInstanceOf(HoglakeException.Validation::class.java)
        }
}
