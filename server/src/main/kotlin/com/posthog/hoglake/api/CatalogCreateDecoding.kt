package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.posthog.hoglake.DatabaseBackend
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.receive

/** Legacy PostgreSQL never accepted selection fields, including explicit nulls. */
internal data class LegacyPostgresCreateCatalogRequestDto(
    val name: String,
    val dataPath: String,
    @JsonDeserialize(using = OperationTextDeserializer::class)
    val author: String? = null,
    @JsonDeserialize(using = OperationTextDeserializer::class)
    val message: String? = null,
) {
    fun toPublicRequest() = CreateCatalogRequestDto(name, dataPath, author, message)
}

/** Only configured mode selects the decoder; no backend/directory access precedes receive or actor checks. */
internal suspend fun ApplicationCall.receiveCatalogCreate(database: DatabaseBackend?): CreateCatalogRequestDto =
    if (database == DatabaseBackend.POSTGRES) {
        receive<LegacyPostgresCreateCatalogRequestDto>().toPublicRequest()
    } else {
        receive<CreateCatalogRequestDto>()
    }
