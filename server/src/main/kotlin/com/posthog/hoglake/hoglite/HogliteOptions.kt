package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.CatalogOptions
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.service.OptionsBackend
import com.posthog.hoglake.service.PatchField
import com.posthog.hoglake.service.requireOperationMetadata

/**
 * The catalog options surface on hoglite (OptionsService's port): the
 * retention and consumer-floor knobs live on the tenant's meta row, the
 * guarded patch is one transaction.
 *
 * The guard that survives the port verbatim: LOWERING retention (or
 * setting it where none was) is the operation that can strand a reader's
 * pinned snapshot, so it requires operator attribution; raising it never
 * can. OptionsService.patch's rule, kept exactly.
 */
internal class HogliteOptions(
    private val store: HogliteCatalogStore,
) : OptionsBackend {
    override fun get(catalog: String): CatalogOptions {
        val m = store.meta(catalog)
        return CatalogOptions(
            snapshotRetentionSeconds = m.retentionSeconds,
            consumerFloor = m.consumerFloor,
            earliestSnapshotId = m.earliestSnapshotId,
        )
    }

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
            val current = store.meta(catalog)
            if (snapshotRetentionSeconds is PatchField.Set) {
                val next = snapshotRetentionSeconds.value
                if (next != null && (current.retentionSeconds == null || next < current.retentionSeconds)) {
                    requireOperationMetadata(author, message)
                }
            }
            val tx = HogliteTx()
            if (snapshotRetentionSeconds is PatchField.Set) {
                tx.add(
                    "UPDATE meta SET snapshot_retention_seconds = ? WHERE id = 1",
                    snapshotRetentionSeconds.value,
                )
            }
            if (consumerFloor != null) {
                tx.add(
                    "UPDATE meta SET consumer_floor = ? WHERE id = 1",
                    if (consumerFloor) 1L else 0L,
                )
            }
            if (tx.size > 0) {
                store.runTx(catalog, HogliteCatalogStore.freshTxid("options-patch"), null, tx)
            }
            get(catalog)
        }
}
