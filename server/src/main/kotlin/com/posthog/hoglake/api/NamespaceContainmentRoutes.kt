package com.posthog.hoglake.api

import com.posthog.hoglake.auth.actorOrRefuse
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.service.CatalogService
import io.ktor.server.application.Application
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.delete
import io.ktor.server.routing.put
import io.ktor.server.routing.route
import io.ktor.server.routing.routing

/**
 * PUT and DELETE .../namespaces/{namespace}/containment (V33). Only the
 * PostgreSQL backend stores namespace containment, so these routes are not
 * part of the CatalogStore seam. They are installed separately and call
 * CatalogService, as the alter, upload and view routes call their services.
 */
fun Application.installNamespaceContainmentRoutes(catalogs: CatalogService) {
    routing {
        route("/v1/catalogs/{catalog}/namespaces") {
            // Containment (V33): operator-only in AuthPolicy.
            put("/{namespace}/containment") {
                val req = call.receive<NamespaceContainmentDto>()
                call.respond(
                    catalogs.setNamespaceContainment(
                        call.catalog(),
                        call.namespace(),
                        NamespaceContainment(req.field, req.value),
                        call.actorOrRefuse(null),
                    ).toDto(),
                )
            }
            delete("/{namespace}/containment") {
                call.respond(
                    catalogs.setNamespaceContainment(
                        call.catalog(),
                        call.namespace(),
                        null,
                        call.actorOrRefuse(null),
                    ).toDto(),
                )
            }
        }
    }
}
