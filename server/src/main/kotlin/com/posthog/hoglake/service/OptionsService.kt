package com.posthog.hoglake.service

import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.CatalogOptions
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.persistence.CatalogRepo
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked

/**
 * A PATCH-style field: distinguishes "not mentioned" (leave unchanged)
 * from "explicitly set" — where the set value may itself be null (for
 * snapshot_retention_seconds, null = expiry disabled).
 */
sealed interface PatchField<out T> {
    /** Field absent from the request: leave the stored value unchanged. */
    data object Absent : PatchField<Nothing>

    /** Field present; [value] null means "set to NULL". */
    data class Set<T>(val value: T?) : PatchField<T>
}

/**
 * The options payload of a catalog row. The row itself comes from
 * CatalogRepo's single hog_catalog mapper (the LifecycleCatalog
 * duplicate that predated the retention columns is gone — the
 * mapper-coverage gate now pins the one mapping to the live schema).
 */
internal fun CatalogInfo.options() =
    CatalogOptions(
        snapshotRetentionSeconds = snapshotRetentionSeconds,
        consumerFloor = consumerFloor,
        earliestSnapshotId = earliestSnapshotId,
    )

/**
 * The catalog options surface (GET/PATCH /v1/catalogs/{catalog}/options):
 * retention as a catalog property the service enforces continuously
 * (README.md §6), never a cron bolted on outside it.
 *
 * PATCH semantics: a field absent from the request is unchanged; an
 * explicit null snapshot_retention_seconds disables expiry; a value
 * must be positive (mirrors the schema CHECK) or the patch is rejected
 * with Validation before touching the row.
 */
class OptionsService(private val jdbi: Jdbi) : OptionsBackend {
    override fun get(catalog: String): CatalogOptions =
        jdbi.withHandleUnchecked { h -> CatalogRepo.require(h, catalog).options() }

    override fun patch(
        catalog: String,
        snapshotRetentionSeconds: PatchField<Long>,
        consumerFloor: Boolean?,
        author: String?,
        message: String?,
    ): CatalogOptions =
        Audit.audited(
            "options_patch",
            catalog,
            null,
            actor = { author },
            message = { message },
            detail = { opts ->
                "snapshot_retention_seconds=${opts.snapshotRetentionSeconds} " +
                    "consumer_floor=${opts.consumerFloor}"
            },
        ) {
            if (snapshotRetentionSeconds is PatchField.Set) {
                val v = snapshotRetentionSeconds.value
                if (v != null && v <= 0) {
                    throw HoglakeException.Validation(
                        "snapshot_retention_seconds must be positive (got $v); " +
                            "null disables expiry",
                    )
                }
            }
            jdbi.inTransactionUnchecked { h ->
                val cat = CatalogRepo.requireForUpdate(h, catalog)
                if (snapshotRetentionSeconds is PatchField.Set) {
                    val next = snapshotRetentionSeconds.value
                    val current = cat.snapshotRetentionSeconds
                    if (next != null && (current == null || next < current)) {
                        requireOperationMetadata(author, message)
                    }
                    h.createUpdate(
                        """
                    UPDATE hog_catalog SET snapshot_retention_seconds = :v
                    WHERE catalog_id = :catalogId
                    """,
                    )
                        .apply {
                            val v = snapshotRetentionSeconds.value
                            if (v == null) bindNull("v", java.sql.Types.BIGINT) else bind("v", v)
                        }
                        .bind("catalogId", cat.catalogId)
                        .execute()
                }
                if (consumerFloor != null) {
                    h.createUpdate(
                        "UPDATE hog_catalog SET consumer_floor = :v WHERE catalog_id = :catalogId",
                    )
                        .bind("v", consumerFloor)
                        .bind("catalogId", cat.catalogId)
                        .execute()
                }
                CatalogRepo.require(h, catalog).options()
            }
        }
}
