package com.posthog.hoglake.service

import com.posthog.hoglake.model.PartitionListing
import com.posthog.hoglake.model.PartitionStatsReport

interface PartitionStatsBackend {
    fun partitionStats(
        catalog: String,
        namespace: String?,
        table: String?,
        limit: Int,
    ): PartitionStatsReport
}

interface PartitionListingBackend {
    fun listPartitions(
        catalog: String,
        namespace: String,
        table: String,
        sort: PartitionListingService.SortColumn = PartitionListingService.SortColumn.PARTITION,
        desc: Boolean? = null,
        limit: Int = PartitionListingService.DEFAULT_LIMIT,
        offset: Int = 0,
        filters: List<PartitionListingService.Filter> = emptyList(),
    ): PartitionListing
}
