package com.posthog.hoglake.api

import com.posthog.hoglake.service.ReplacementTarget
import com.posthog.hoglake.service.TableCreation
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationService
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import java.time.Instant
import java.util.UUID

data class PrepareTableCreationDto(
    val namespace: String,
    val name: String,
    val columns: List<ColumnDefDto>,
    val replacement: ReplacementTarget? = null,
    val partitionFields: List<AlterPartitionFieldDto> = emptyList(),
)

data class PublishTableCreationDto(val files: List<FileRegistrationDto>)

data class TableCreationDto(
    val operationId: UUID,
    val tableUuid: UUID,
    val namespace: String,
    val name: String,
    val columns: List<ColumnDto>,
    val writePath: String,
    val state: String,
    val expiresAt: Instant,
    val snapshotId: Long?,
    val schemaVersion: Long?,
    val reason: String?,
)

private fun TableCreation.toDto() =
    TableCreationDto(
        operationId, tableUuid, definition.namespace, definition.name,
        columns.map {
            it.toDto()
        },
        writePath, state, expiresAt, snapshotId, schemaVersion, reason,
    )

fun Application.installTableCreationRoutes(creations: TableCreationService) {
    routing {
        route("/v1/catalogs/{catalog}/table-creations/{operation}") {
            put { call.prepareCreation(creations, partitioned = false) }
            put("/partitioned") { call.prepareCreation(creations, partitioned = true) }
            get { call.respond(creations.status(call.creationCatalog(), call.creationOperation()).toDto()) }
            post("/commit") {
                val request = call.receive<PublishTableCreationDto>()
                call.respond(
                    creations.publish(
                        call.creationCatalog(),
                        call.creationOperation(),
                        request.files.map {
                            it.toModel()
                        },
                    ).toDto(),
                )
            }
            post("/abort") { call.respond(creations.abort(call.creationCatalog(), call.creationOperation()).toDto()) }
        }
    }
}

private suspend fun ApplicationCall.prepareCreation(
    creations: TableCreationService,
    partitioned: Boolean,
) {
    val request = receive<PrepareTableCreationDto>()
    if (request.partitionFields.isNotEmpty() != partitioned) {
        throw com.posthog.hoglake.model.HoglakeException.Validation(
            "partition fields require the partitioned preparation endpoint",
        )
    }
    respond(
        creations.prepare(
            creationCatalog(),
            creationOperation(),
            TableCreationDefinition(
                request.namespace,
                request.name,
                request.columns.map {
                    it.toModel()
                },
                request.replacement,
                request.partitionFields.map { it.toModel() },
            ),
        ).toDto(),
    )
}

private fun ApplicationCall.creationCatalog(): String =
    parameters["catalog"] ?: throw BadRequestException("missing catalog")

private fun ApplicationCall.creationOperation(): UUID =
    try {
        UUID.fromString(parameters["operation"])
    } catch (_: IllegalArgumentException) {
        throw BadRequestException("invalid operation UUID")
    }
