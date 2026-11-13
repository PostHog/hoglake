package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.RehydrateResult
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.service.HydratorMaintenanceBackend

/** Requeue failed live files. Footer reads and background hydration are not part of this backend. */
internal class HogliteHydrator(
    private val store: HogliteCatalogStore,
) : HydratorMaintenanceBackend {
    override fun rehydrateFailed(
        catalog: String,
        namespace: String?,
        table: String?,
    ): RehydrateResult =
        store.recorded(catalog, MaintenanceTask.HYDRATOR, MaintenanceTrigger.MANUAL) {
            Audit.audited(
                "rehydrate",
                catalog,
                table?.let { "$namespace.$it" },
                detail = { "requeued=${it.requeued}" },
            ) {
                if ((namespace == null) != (table == null)) {
                    throw HoglakeException.Validation(
                        "namespace and table must be supplied together (or neither, for the whole catalog)",
                    )
                }
                store.meta(catalog)
                val tableId =
                    if (namespace != null && table != null) {
                        store.liveTable(
                            catalog,
                            namespace,
                            table,
                        ).tableId
                    } else {
                        null
                    }
                val tx = HogliteTx()
                tx.add(
                    "UPDATE files SET stats_state = 'pending' " +
                        "WHERE stats_state = 'failed' AND end_snapshot IS NULL " +
                        (if (tableId != null) "AND table_id = ? " else "") +
                        "AND EXISTS (SELECT 1 FROM tables t WHERE t.id = files.table_id " +
                        "AND t.dropped_snapshot IS NULL)",
                    *(if (tableId != null) arrayOf(tableId) else emptyArray()),
                )
                val count = tx.add("SELECT changes()")
                val requeued =
                    store.runTx(
                        catalog,
                        HogliteCatalogStore.freshTxid("rehydrate"),
                        null,
                        tx,
                    )[count].firstRow()!![0].asLong()
                RehydrateResult(requeued)
            }
        }
}
