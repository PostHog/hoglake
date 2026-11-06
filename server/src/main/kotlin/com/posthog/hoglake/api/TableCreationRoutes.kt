package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.actorOrRefuse
import com.posthog.hoglake.auth.hoglakeAuth
import com.posthog.hoglake.auth.requireScopedNamespaces
import com.posthog.hoglake.service.ReplacementTarget
import com.posthog.hoglake.service.TableCreation
import com.posthog.hoglake.service.TableCreationBackend
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.store.CatalogStoreSelector
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
    val sortFields: List<AlterSortFieldDto> = emptyList(),
    val comment: String? = null,
    val properties: Map<String, String> = emptyMap(),
    @JsonDeserialize(using = OperationTextDeserializer::class)
    val author: String? = null,
    @JsonDeserialize(using = OperationTextDeserializer::class)
    val message: String? = null,
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

fun Application.installTableCreationRoutes(
    creations: TableCreationBackend,
    selector: CatalogStoreSelector? = null,
) {
    fun ApplicationCall.creationBackend(): TableCreationBackend =
        selector?.tableCreations(this, creationCatalog(), creations) ?: creations

    routing {
        route("/v1/catalogs/{catalog}/table-creations/{operation}") {
            put { call.prepareCreation(call.creationBackend(), partitioned = false) }
            put("/partitioned") { call.prepareCreation(call.creationBackend(), partitioned = true) }
            put("/sorted") { call.prepareCreation(call.creationBackend(), partitioned = false, sorted = true) }
            put("/metadata") { call.prepareCreation(call.creationBackend(), partitioned = false, metadata = true) }
            get {
                val creations = call.creationBackend()
                call.requireScopedCreation(creations)
                call.respond(creations.status(call.creationCatalog(), call.creationOperation()).toDto())
            }
            post("/commit") {
                val creations = call.creationBackend()
                call.requireScopedCreation(creations)
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
            post("/commit/uploads") {
                val creations = call.creationBackend()
                call.requireScopedCreation(creations)
                val request = call.receive<PublishTableCreationDto>()
                call.respond(
                    creations.publish(
                        call.creationCatalog(),
                        call.creationOperation(),
                        request.files.map { it.toModel() },
                    ).toDto(),
                )
            }
            post("/abort") {
                val creations = call.creationBackend()
                call.requireScopedCreation(creations)
                call.respond(creations.abort(call.creationCatalog(), call.creationOperation()).toDto())
            }
        }
    }
}

private suspend fun ApplicationCall.prepareCreation(
    creations: TableCreationBackend,
    partitioned: Boolean,
    sorted: Boolean = false,
    metadata: Boolean = false,
) {
    val request = receive<PrepareTableCreationDto>()
    requireScopedNamespaces(creationCatalog(), listOf(request.namespace))

    fun hasComment(columns: List<ColumnDefDto>): Boolean =
        columns.any {
            it.comment != null || hasComment(it.children ?: emptyList())
        }
    if (!metadata && (request.comment != null || request.properties.isNotEmpty() || hasComment(request.columns))) {
        throw com.posthog.hoglake.model.HoglakeException.Validation(
            "metadata requires its dedicated preparation endpoint",
        )
    }
    if (!metadata &&
        ((!sorted && request.partitionFields.isNotEmpty() != partitioned) || request.sortFields.isNotEmpty() != sorted)
    ) {
        throw com.posthog.hoglake.model.HoglakeException.Validation(
            "partition or sort fields require their dedicated preparation endpoint",
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
                request.sortFields.map { it.toModel() },
                request.comment,
                request.properties,
                actorOrRefuse(request.author),
                request.message,
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

/**
 * A scoped caller may read, publish or abort only a creation prepared for a
 * namespace it may use. The namespace is the one recorded at prepare, which
 * publish re-checks against the live namespace's id, so a creation cannot be
 * pointed elsewhere after the check. A creation that does not exist answers
 * 404 as it would for anyone.
 */
private fun ApplicationCall.requireScopedCreation(creations: TableCreationBackend) {
    if (hoglakeAuth.callerOrNull?.kind != Caller.Kind.SCOPED) return
    val creation = creations.status(creationCatalog(), creationOperation())
    requireScopedNamespaces(creationCatalog(), listOf(creation.definition.namespace))
}
