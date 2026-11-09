package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.PartitionSpec
import com.posthog.hoglake.model.Transform
import org.jdbi.v3.core.Handle

/**
 * hog_partition_spec / hog_partition_field reads. Spec visibility
 * follows the standard versioned-row rule (visible at S iff
 * begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot));
 * at most one spec row is visible per table per snapshot by
 * construction (AlterService end-snapshots the old spec when a new one
 * is set). Fields are immutable per (table_id, spec_id) and ordered by
 * key_index.
 */
object SpecRepo {
    /**
     * The table's partition spec visible at [snapshot], or null when the
     * table is unpartitioned there. A zero-field spec never exists
     * (SetPartitionSpec([]) just retires the old spec), so a returned
     * spec always has >= 1 field.
     */
    fun specAt(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ): PartitionSpec? {
        val specId =
            h.createQuery(
                """
            SELECT spec_id FROM hog_partition_spec
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND begin_snapshot <= :snapshot
              AND (end_snapshot IS NULL OR :snapshot < end_snapshot)
            """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .mapTo(Long::class.javaObjectType)
                .findOne()
                .orElse(null) ?: return null
        return PartitionSpec(specId, fields(h, catalogId, tableId, specId))
    }

    /**
     * Every spec row of the table, live or retired, by spec_id, each with
     * its fields by key_index: the specs a file of the table can name.
     * Partition values are stored by key_index and read by position, so a
     * spec whose key_indexes are not exactly 0..n-1 (every writer writes
     * them so), or that has no field rows, comes back with no fields, for
     * the caller to refuse ([com.posthog.hoglake.store.ReadScope]).
     */
    fun boundSpecs(
        h: Handle,
        catalogId: Long,
        tableId: Long,
    ): List<PartitionSpec> =
        h.createQuery(
            """
            SELECT s.spec_id, f.key_index, f.source_field_id, f.transform, f.transform_param
            FROM hog_partition_spec s
            LEFT JOIN hog_partition_field f
              ON f.catalog_id = s.catalog_id AND f.table_id = s.table_id AND f.spec_id = s.spec_id
            WHERE s.catalog_id = :catalogId AND s.table_id = :tableId
            ORDER BY s.spec_id
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .map { rs, _ ->
                val field =
                    rs.getObject("source_field_id")?.let {
                        rs.getInt("key_index") to
                            PartitionFieldDef(
                                sourceFieldId = (it as Number).toLong(),
                                transform = Transform.fromWire(rs.getString("transform")),
                                transformParam = rs.getObject("transform_param")?.let { p -> (p as Number).toInt() },
                            )
                    }
                rs.getLong("spec_id") to field
            }
            .list()
            .groupBy({ it.first }, { it.second })
            .map { (specId, rows) ->
                // Placed by their stored key_index, never by row order.
                val byKey = rows.filterNotNull().toMap()
                val dense = byKey.keys == (0 until byKey.size).toSet()
                PartitionSpec(specId, if (dense) (0 until byKey.size).map(byKey::getValue) else emptyList())
            }

    /**
     * The fields of the table's spec [specId], by key_index, whether the
     * spec is live or retired (retired specs are never cleared): what a
     * file written under it names by `spec_id`.
     */
    fun fields(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        specId: Long,
    ): List<PartitionFieldDef> =
        h.createQuery(
            """
            SELECT source_field_id, transform, transform_param
            FROM hog_partition_field
            WHERE catalog_id = :catalogId AND table_id = :tableId AND spec_id = :specId
            ORDER BY key_index
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("specId", specId)
            .map { rs, _ ->
                PartitionFieldDef(
                    sourceFieldId = rs.getLong("source_field_id"),
                    transform = Transform.fromWire(rs.getString("transform")),
                    transformParam = rs.getObject("transform_param")?.let { (it as Number).toInt() },
                )
            }
            .list()
}
