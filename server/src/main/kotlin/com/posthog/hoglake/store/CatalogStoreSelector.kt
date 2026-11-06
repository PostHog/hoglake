package com.posthog.hoglake.store

import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.Routing
import com.posthog.hoglake.auth.hoglakeAuth
import com.posthog.hoglake.hoglite.HogliteCatalogStore
import com.posthog.hoglake.hoglite.HogliteClient
import com.posthog.hoglake.hoglite.HogliteTenantResolver
import com.posthog.hoglake.model.HoglakeException
import io.ktor.server.application.ApplicationCall
import io.ktor.util.AttributeKey
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/** Select a catalog backend from verified placement, never from request parameters or claim endpoints. */
class CatalogStoreSelector(
    private val postgres: CatalogStore?,
    private val cfg: Config? = null,
) {
    private val clients = ConcurrentHashMap<UUID, HogliteClient>()
    private val requestStore = AttributeKey<HogliteCatalogStore>("HoglakeCatalogStore")

    /** Return one request-scoped hoglite store, or the process's PostgreSQL singleton. */
    fun forCatalog(
        call: ApplicationCall,
        catalog: String,
    ): CatalogStore {
        val routing = call.hoglakeAuth.callerOrNull?.routing
        return if (routing != null && routing.catalog == catalog) routed(call, routing) else fallback(catalog)
    }

    /** A hoglite listing must carry the token's single catalog as an explicit filter. */
    fun forListing(call: ApplicationCall): Listing {
        val caller = call.hoglakeAuth.callerOrNull
        val routing = caller?.routing
        return if (routing != null) {
            Listing(routed(call, routing), setOf(routing.catalog))
        } else {
            Listing(fallback(""), caller?.visibleCatalogs())
        }
    }

    /** Maintenance is PostgreSQL-only. Resolve placement before any service or body work. */
    fun requirePostgres(
        call: ApplicationCall,
        catalog: String,
    ) {
        if (forCatalog(call, catalog) is HogliteCatalogStore) {
            throw HoglakeException.NotSupported("this operation is not supported on a hoglite backend")
        }
    }

    /** Readiness also checks every configured hoglite backend, including clients not yet used by a request. */
    fun reachable(): Boolean = cfg?.hogliteBackends?.keys?.all { client(it).reachable() } ?: true

    private fun fallback(catalog: String): CatalogStore =
        postgres?.takeIf { cfg == null || cfg.defaultBackend == "postgres" }
            ?: throw HoglakeException.NotFound("catalog '$catalog'")

    private fun routed(
        call: ApplicationCall,
        routing: Routing,
    ): CatalogStore =
        when (routing.backend) {
            BackendKind.POSTGRES -> postgres ?: throw HoglakeException.NotFound("catalog '${routing.catalog}'")
            BackendKind.HOGLITE ->
                call.attributes.computeIfAbsent(requestStore) {
                    HogliteCatalogStore(
                        client(routing.backendId),
                        HogliteTenantResolver { catalog ->
                            if (catalog == routing.catalog) {
                                checkNotNull(routing.tenant)
                            } else {
                                throw HoglakeException.NotFound("catalog '$catalog'")
                            }
                        },
                        refuseBlindPartitionedAppends = checkNotNull(cfg).refuseBlindPartitionedAppends,
                        ledgerRetentionSeconds = cfg.removalLedgerRetentionSeconds,
                        smallFileThresholdBytes = cfg.compactionTargetBytes,
                        minInputFiles = cfg.compactionMinInputFiles,
                        maxInputFiles = cfg.compactionMaxInputFiles,
                    )
                }
        }

    private fun client(id: UUID): HogliteClient =
        clients.computeIfAbsent(id) {
            HogliteClient.fromConfig(checkNotNull(cfg).hogliteBackends.getValue(it))
        }

    data class Listing(val store: CatalogStore, val catalogs: Set<String>?)
}
