package com.posthog.hoglake.compaction

/**
 * THE REPLACED BUCKET PROBE, kept so plan tests can MEASURE the new
 * statement against it on the same rows. Not asserted about as code:
 * no code path runs it.
 *
 * One correlated `EXISTS` per key, filtering a manifest-driven scan
 * whose `LIMIT` has no inner `ORDER BY`. That leaves Postgres free to
 * drive from `hog_data_file` and apply the arms as filters, which walks
 * the table's rows until the `LIMIT` fills: O(table) for any bucket
 * whose files are fewer than the cap or late in the walk's order. The
 * replacement (`CompactionService.candidateSql`) removes that plan from
 * the planner's choices. Retyped once, here, rather than in each test
 * that measures it.
 */
internal object ReplacedCandidateSql {
    /** The replaced statement with arms on key indexes `0 until keys`, all `value = :vN`. */
    fun sql(keys: Int): String {
        val arms =
            (0 until keys).joinToString("\n") { k ->
                """
                   AND EXISTS (SELECT 1 FROM hog_file_partition_value pv$k
                                WHERE pv$k.catalog_id = f0.catalog_id
                                  AND pv$k.data_file_id = f0.data_file_id
                                  AND pv$k.key_index = :k$k
                                  AND pv$k.value = :v$k)
                """.trimEnd()
            }
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
                       AND f0.file_size_bytes < :targetBytes$arms
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
