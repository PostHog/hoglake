package com.posthog.hoglake.compaction

/**
 * THE REPLACED BUCKET PROBE. Not asserted about as code: no code path
 * runs it.
 *
 * One correlated `EXISTS` per key, filtering a manifest-driven scan
 * whose `LIMIT` has no inner `ORDER BY`. That leaves Postgres free to
 * drive from `hog_data_file` and apply the arms as filters, which walks
 * the table's rows until the `LIMIT` fills: O(table) for any bucket
 * whose files are fewer than the cap or late in the walk's order. The
 * replacement (`CompactionService.candidateSql`) drives from an id-set
 * InitPlan that PG 18 always costs as a few primary-key descents.
 * Retyped once, here, rather than in each test that uses it.
 *
 * Two uses. Its PLANS are recorded (printed) by the plan tests, never
 * asserted: how Postgres plans a statement no code path runs guards
 * nothing, and a point release that plans it better must not fail the
 * suite. Its RESULTS are the "old" column of
 * `CandidateSqlEquivalenceIntegrationTest`, which asserts that the
 * replacement returns what this returned wherever this was right.
 *
 * The SQL is the replaced statement's exactly, including its shapes for
 * NULL partition values (`value IS NULL`) and for no arms (the inner
 * `ORDER BY`), and with the spec predicate OUTSIDE the scan cap — the
 * pre-existing under-fetch `CompactionService.bucketCandidates` records.
 */
internal object ReplacedCandidateSql {
    /**
     * The replaced statement with an arm per key index in [keys]:
     * `value IS NULL` for the keys in [nullArms], `value = :vN` otherwise.
     */
    fun sql(
        keys: List<Int>,
        nullArms: Set<Int> = emptySet(),
    ): String {
        val arms =
            keys.joinToString("\n") { k ->
                val valuePredicate = if (k in nullArms) "pv$k.value IS NULL" else "pv$k.value = :v$k"
                """
                   AND EXISTS (SELECT 1 FROM hog_file_partition_value pv$k
                                WHERE pv$k.catalog_id = f0.catalog_id
                                  AND pv$k.data_file_id = f0.data_file_id
                                  AND pv$k.key_index = :k$k
                                  AND $valuePredicate)
                """.trimEnd()
            }
        val innerOrder =
            if (keys.isEmpty()) "\n                     ORDER BY f0.file_size_bytes, f0.data_file_id" else ""
        return """
            SELECT f.data_file_id, f.path, f.record_count, f.file_size_bytes,
                   f.footer_size, f.row_id_start, f.explicit_row_ids,
                   f.begin_snapshot, f.spec_id,
                   dv.delete_file_id AS dv_id, dv.path AS dv_path, dv.delete_count AS dv_count
              FROM (
                    SELECT f0.catalog_id, f0.data_file_id, f0.path, f0.record_count,
                           f0.file_size_bytes, f0.footer_size, f0.row_id_start,
                           f0.explicit_row_ids, f0.begin_snapshot, f0.spec_id
                      FROM hog_data_file f0
                     WHERE f0.catalog_id = :catalogId AND f0.table_id = :tableId
                       AND f0.end_snapshot IS NULL
                       AND f0.file_size_bytes < :targetBytes$arms$innerOrder
                     LIMIT :scanLimit
                   ) f
              LEFT JOIN hog_delete_file dv
                ON dv.catalog_id = f.catalog_id
               AND dv.data_file_id = f.data_file_id
               AND dv.end_snapshot IS NULL
             WHERE f.spec_id IS NOT DISTINCT FROM :specId
             ORDER BY f.file_size_bytes, f.data_file_id
             LIMIT :limit
            """
    }
}
