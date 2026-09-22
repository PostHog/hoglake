package com.posthog.hoglake.api

import com.posthog.hoglake.service.UploadService
import io.ktor.server.application.Application
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import java.util.UUID

data class ClaimUploadDto(val owner: UUID, val prefix: String, val fileKind: String)

data class UploadOwnerDto(val owner: UUID)

data class AbandonUploadsDto(val owner: UUID, val paths: List<String>)

fun Application.installUploadRoutes(uploads: UploadService) {
    routing {
        route("/v1/catalogs/{catalog}/uploads") {
            put("/{upload}") {
                val req = call.receive<ClaimUploadDto>()
                val id =
                    try {
                        UUID.fromString(call.parameters["upload"])
                    } catch (_: IllegalArgumentException) {
                        throw BadRequestException("invalid upload UUID")
                    }
                call.respond(uploads.claim(call.parameters["catalog"]!!, id, req.owner, req.prefix, req.fileKind))
            }
            post("/renew") {
                val req = call.receive<UploadOwnerDto>()
                call.respond(mapOf("renewed" to uploads.renew(call.parameters["catalog"]!!, req.owner)))
            }
            post("/abandon") {
                val req = call.receive<AbandonUploadsDto>()
                call.respond(mapOf("abandoned" to uploads.abandon(call.parameters["catalog"]!!, req.owner, req.paths)))
            }
            post("/schedule-expired") {
                val limit =
                    call.request.queryParameters["limit"]?.let {
                        it.toIntOrNull() ?: throw BadRequestException("invalid limit")
                    } ?: 1000
                call.respond(mapOf("scheduled" to uploads.scheduleExpired(call.parameters["catalog"]!!, limit)))
            }
        }
    }
}
