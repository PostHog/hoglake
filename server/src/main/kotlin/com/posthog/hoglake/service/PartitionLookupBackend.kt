package com.posthog.hoglake.service

import com.posthog.hoglake.model.PartitionLookup

/**
 * The exact partition inspection seam (`POST .../partitions/lookup`). The
 * Postgres implementation is [PartitionLookupService]; the hoglite
 * backend's is [com.posthog.hoglake.hoglite.HoglitePartitionLookup]. The
 * request contract — validation, cursor scope, candidate bound, the
 * per-request deadline and the admission limit — is the one documented on
 * [PartitionLookupService]; both backends answer it with the same shapes.
 */
interface PartitionLookupBackend {
    fun lookup(
        catalog: String,
        namespace: String,
        table: String,
        specId: Long,
        values: List<String?>,
        snapshot: Long? = null,
        limit: Int? = null,
        cursor: String? = null,
    ): PartitionLookup
}
