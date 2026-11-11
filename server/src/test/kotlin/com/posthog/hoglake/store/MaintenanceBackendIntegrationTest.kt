package com.posthog.hoglake.store

import com.posthog.hoglake.Config
import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.compaction.CompactionService
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.MaintenanceRunStatus
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.MaintenanceStatusService
import com.posthog.hoglake.service.OptionsService
import com.posthog.hoglake.service.PatchField
import com.posthog.hoglake.service.RemovalStore
import com.posthog.hoglake.service.RetirementService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

/** Exercise every maintenance accessor and inherited default against PostgreSQL. */
@Tag("integration")
class MaintenanceBackendIntegrationTest {
    @Test
    fun `accessors retain PostgreSQL instances and default manual run metadata`() {
        PgTestSupport.freshDatabase().use { db ->
            ObjectStore("http://localhost:1", "us-east-1", "unused", "unused", true).use { objects ->
                RemovalStore("http://localhost:1", "us-east-1", "unused", "unused", true).use { removals ->
                    val catalog = "maintenance-seam"
                    CatalogService(db.jdbi).createCatalog(catalog, "s3://bucket/seam")
                    val selector = CatalogStoreSelector(PostgresCatalogStore({ db.jdbi }, Config()))
                    val options = OptionsService(db.jdbi)
                    val expiry = ExpiryService(db.jdbi)
                    val cleanup = CleanupService(db.jdbi, removals)
                    val compaction =
                        CompactionService(db.jdbi, objects, CompactionConfig(targetBytes = 1024, maxGroupsPerRun = 1))
                    val retirement = RetirementService(db.jdbi)
                    val hydrator = Hydrator(db.jdbi, objects)
                    val status =
                        MaintenanceStatusService(
                            db.jdbi,
                            hydratorIntervalMs = 0,
                            expiryIntervalMs = 0,
                            cleanupIntervalMs = 0,
                            compactionIntervalMs = 0,
                            retirementIntervalMs = 0,
                            reindexIntervalMs = 0,
                            smallFileThresholdBytes = 1024,
                        )
                    testApplication {
                        application {
                            routing {
                                get("/maintenance-seam") {
                                    assertThat(selector.options(call, catalog, options)).isSameAs(options)
                                    assertThat(selector.expiry(call, catalog, expiry)).isSameAs(expiry)
                                    assertThat(selector.cleanup(call, catalog, cleanup)).isSameAs(cleanup)
                                    assertThat(selector.compaction(call, catalog, compaction)).isSameAs(compaction)
                                    assertThat(selector.retirement(call, catalog, retirement)).isSameAs(retirement)
                                    assertThat(selector.hydratorMaintenance(call, catalog, hydrator)).isSameAs(hydrator)
                                    assertThat(selector.maintenanceStatus(call, catalog, status)).isSameAs(status)

                                    val opts = selector.options(call, catalog) { options }
                                    assertThat(
                                        opts.patch(catalog, PatchField.Absent, null),
                                    ).isEqualTo(opts.get(catalog))
                                    selector.expiry(call, catalog) { expiry }.runOnce(catalog, 1)
                                    selector.cleanup(call, catalog) { cleanup }.runOnce(catalog, 1)
                                    selector.compaction(call, catalog) { compaction }.runOnce(catalog)
                                    selector.retirement(call, catalog) { retirement }.runOnce(catalog)
                                    assertThat(
                                        selector.hydratorMaintenance(call, catalog) { hydrator }
                                            .rehydrateFailed(catalog).requeued,
                                    ).isZero()
                                    val selectedStatus = selector.maintenanceStatus(call, catalog) { status }
                                    assertThat(selectedStatus.status(catalog).catalog).isEqualTo(catalog)
                                    val runs = selectedStatus.runs(catalog, null, null, 50).runs
                                    assertThat(runs.map { it.task }).containsExactlyInAnyOrder(
                                        MaintenanceTask.EXPIRY,
                                        MaintenanceTask.CLEANUP,
                                        MaintenanceTask.COMPACTION,
                                        MaintenanceTask.RETIREMENT,
                                        MaintenanceTask.HYDRATOR,
                                    )
                                    assertThat(runs.map { it.trigger }).containsOnly(MaintenanceTrigger.MANUAL)
                                    assertThat(runs.map { it.status }).containsOnly(MaintenanceRunStatus.OK)
                                    call.respondText("ok")
                                }
                            }
                        }
                        assertThat(client.get("/maintenance-seam").bodyAsText()).isEqualTo("ok")
                    }
                }
            }
        }
    }
}
