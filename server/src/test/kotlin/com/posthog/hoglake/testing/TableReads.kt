package com.posthog.hoglake.testing

import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.service.CatalogService

/**
 * `getTable` at the catalog's CURRENT HEAD, with EXACT totals.
 *
 * WHY THIS EXISTS. A plain `getTable(c, ns, t)` — no snapshot, no
 * timestamp — serves `record_count` / `file_count` / `file_size_bytes`
 * from the MAINTENANCE SAMPLER's published generation, and returns NULL
 * for all three until a generation covers the table (#232). That is
 * right for the endpoint: the aggregate it replaced was ~10M rows on
 * production's busiest table, three times per writer flush. It is
 * useless to a test asserting what a commit just did, where the
 * question is "how many rows does the manifest hold RIGHT NOW" — and no
 * test fixture runs the sampler loop, so a bare head read in a test
 * reports nothing at all.
 *
 * Naming head explicitly puts the read on the time-travel path, which
 * still aggregates the manifest, so the numbers are a fact rather than a
 * sample and no sampler has to be driven first.
 *
 * WHAT IT IS NOT FOR: any test whose subject is the sample itself — the
 * absence before a generation covers the table, the freshness fields,
 * `totals=false`. Those read the endpoint the way a client does and
 * drive the sampler with [publishMaintenanceSample]; see
 * `api/TableTotalsApiTest` and `service/TierTotalsIntegrationTest`.
 */
fun CatalogService.tableWithExactTotals(
    catalog: String,
    namespace: String,
    table: String,
): TableInfo =
    getTable(
        catalog,
        namespace,
        table,
        snapshot = getCatalog(catalog).headSnapshotId,
    )
