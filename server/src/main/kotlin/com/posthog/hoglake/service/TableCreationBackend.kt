package com.posthog.hoglake.service

import com.posthog.hoglake.model.FileRegistration
import java.util.UUID

/**
 * The atomic table-creation seam (prepare -> publish|abort, with
 * durable receipts). The Postgres implementation is
 * [TableCreationService]; the hoglite backend's is
 * [com.posthog.hoglake.hoglite.HogliteTableCreations].
 */
interface TableCreationBackend {
    fun prepare(
        catalog: String,
        operationId: UUID,
        definition: TableCreationDefinition,
    ): TableCreation

    fun status(
        catalog: String,
        operationId: UUID,
    ): TableCreation

    fun abort(
        catalog: String,
        operationId: UUID,
    ): TableCreation

    fun publish(
        catalog: String,
        operationId: UUID,
        files: List<FileRegistration>,
    ): TableCreation
}
