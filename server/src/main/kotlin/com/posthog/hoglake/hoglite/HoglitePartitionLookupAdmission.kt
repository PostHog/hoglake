package com.posthog.hoglake.hoglite

import com.posthog.hoglake.service.PartitionLookupAdmission

/** Backend-scoped lookup capacity, injected into request-scoped stores by their owner. */
class HoglitePartitionLookupAdmission(maxConcurrentRequests: Int = 2) {
    init {
        require(maxConcurrentRequests > 0)
    }

    private val admission = PartitionLookupAdmission(maxConcurrentRequests)

    internal fun acquire(
        tenant: String,
        deadline: Long,
    ): Boolean = admission.acquire(tenant, deadline)

    internal fun release(tenant: String) = admission.release(tenant)
}
