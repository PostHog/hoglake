package com.posthog.hoglake.store

import com.posthog.hoglake.auth.PredicateGrant
import com.posthog.hoglake.model.ObjectPrefix
import com.posthog.hoglake.model.PartitionSpec
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.LayoutRepo
import com.posthog.hoglake.persistence.SpecRepo
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.statement.SqlStatement

/**
 * The files one mount read may see (#291 §6-§7, `ReadScope`): the files
 * of one table, visible at the statement's snapshot, whose identity partition value on the mount's field
 * is in [values], and whose stats prove it (`stats_state = 'provided' AND
 * NOT prefix_quarantined`, [ObjectPrefix]). Every statement a mount
 * serves ANDs [sql] in, before limits, stats and totals, so nothing a
 * mount returns comes from a file outside it.
 *
 * [values] are the caller's `prd` values that grant read, intersected with
 * the mount's; `*` means the mount's (#147 D5, D6). A token never widens
 * the mount, and an empty set reads no file: [sql] is then `FALSE`, never
 * an absent filter.
 *
 * EVERY BOUND SPEC IS MATCHED AT ITS OWN KEY. A file is matched through
 * its own `spec_id`, then that spec's one identity field on the mount's
 * field and its key_index (a key's position is not an identity). Specs
 * that hold the field at the same key_index share one arm, so a table
 * whose specs all lead with it (the events table) has one.
 * `hog_file_partition_value` has no `table_id` or `spec_id`, so an arm's
 * InitPlan collects every file of the catalog with a matching value at
 * that key_index, another table's or a day ordinal equal to a team id
 * included; the arm's spec ids and the shared `table_id` drop them. Ids
 * are allocated per catalog, so both `catalog_id` terms are what keep
 * another catalog's ids out. Null values, unpartitioned files and other
 * transforms match no arm.
 *
 * THE PLAN is `CompactionService.candidateSql`'s, custom and generic
 * alike (the manifest side is costed from a Param array's default size,
 * not the values): each arm's ids come from
 * `hog_file_partition_value_lookup` as an InitPlan, and the manifest is
 * reached from them on its primary key. Its work is what the arms
 * collect, catalog-wide, not the table: ReadScopePlanIntegrationTest at
 * 41M files read 26,189 buffers for a team whose arm collected 2,050 ids
 * (12.8 per collected id), and 27,542 for a reordered spec whose key-1 arm
 * collected a whole day (42,640 ids for 1,988 returned). The InitPlan's
 * array is built whole before the scan starts, about 32 bytes per id
 * (2M ids for a team with 5% of a 41M-file catalog). Nothing in the
 * statement bounds it: its size is the granted values' files across the
 * catalog, up to Postgres's 1 GB array limit; a LIMIT does not bound it,
 * and a deadline bounds the time, not the memory. With two or more arms the
 * manifest is read through a BitmapOr whose heap filter re-tests each row
 * against its arm's array, linearly (a Param array is never hashed), so
 * that shape costs rows x ids; only specs that put the field at different
 * keys produce it, and the events table's do not.
 *
 * THE TABLE'S LAYOUT IS THE MOUNT'S FIELD at the read snapshot, or the
 * read is refused ([MountIntegrityError]). The option is set once, on an
 * empty table, and never unset, so every file visible at that snapshot
 * was committed under it (its path checked), and every spec since keeps
 * the identity field; the stats proof is the `provided AND NOT
 * prefix_quarantined` term.
 *
 * A MISSING SPEC cannot occur for those files: they were committed after
 * #479, from which expiry never deletes a spec row, and the option itself
 * (#481) came later. A file bound to an absent spec row would match no
 * arm and stay out (fail-closed); finding one per read would scan the
 * whole table. A spec row without fields or with key_indexes other than
 * 0..n-1 ([SpecRepo.boundSpecs]), or no spec with the identity field, can
 * only be corruption, and is refused.
 */
class ReadScope private constructor(
    val catalogId: Long,
    val tableId: Long,
    /** Canonical decimals, distinct, in ascending numeric order. */
    val values: List<String>,
    val arms: List<Arm>,
) {
    /** The files of the specs [specIds] match on their partition value at [keyIndex]. */
    data class Arm(
        val keyIndex: Int,
        val specIds: List<Long>,
    )

    /** No value is granted, so [sql] is `FALSE` and binds nothing. */
    val isEmpty: Boolean get() = values.isEmpty()

    /**
     * The predicate on `hog_data_file` [alias], which is always a literal
     * table alias at the call site. Bind it with [bind]. It does not test
     * visibility: the statement does that at its own snapshot.
     */
    fun sql(alias: String): String {
        if (isEmpty) return "FALSE"
        val arms =
            arms.indices.joinToString(" OR ") { i ->
                "($alias.spec_id = ANY(:rs_specs_$i) AND $alias.data_file_id = ANY(ARRAY(" +
                    "SELECT rs_pv.data_file_id FROM hog_file_partition_value rs_pv " +
                    "WHERE rs_pv.catalog_id = :rs_catalog AND rs_pv.key_index = :rs_key_$i " +
                    "AND rs_pv.value = ANY(:rs_values))))"
            }
        return "($alias.catalog_id = :rs_catalog AND $alias.table_id = :rs_table " +
            "AND $alias.stats_state = 'provided' AND NOT $alias.prefix_quarantined AND ($arms))"
    }

    /** Binds [sql]'s parameters, all named `rs_*`, on [statement]. */
    fun <S : SqlStatement<S>> bind(statement: S): S {
        if (isEmpty) return statement
        statement.bind("rs_catalog", catalogId)
            .bind("rs_table", tableId)
            .bindArray("rs_values", String::class.java, values)
        arms.forEachIndexed { i, arm ->
            statement.bindArray("rs_specs_$i", Long::class.javaObjectType, arm.specIds).bind("rs_key_$i", arm.keyIndex)
        }
        return statement
    }

    companion object {
        /**
         * The scope of table [tableId] read at [snapshot] through a mount
         * on field [sourceFieldId] holding [mountValues], for a caller
         * whose grant on the mount's predicate field is [grant].
         */
        fun load(
            h: Handle,
            catalogId: Long,
            tableId: Long,
            snapshot: Long,
            sourceFieldId: Long,
            mountValues: Collection<Long>,
            grant: PredicateGrant?,
        ): ReadScope =
            compile(
                catalogId,
                tableId,
                sourceFieldId,
                LayoutRepo.layoutAt(h, catalogId, tableId, snapshot),
                SpecRepo.boundSpecs(h, catalogId, tableId),
                mountValues,
                grant,
            )

        /**
         * [layoutFieldId] is the table's object-prefix field at the read
         * snapshot, and [specs] every spec row of the table, live or
         * retired. A null [grant] grants nothing.
         */
        internal fun compile(
            catalogId: Long,
            tableId: Long,
            sourceFieldId: Long,
            layoutFieldId: Long?,
            specs: List<PartitionSpec>,
            mountValues: Collection<Long>,
            grant: PredicateGrant?,
        ): ReadScope {
            if (layoutFieldId != sourceFieldId) {
                integrity("table $tableId's object-prefix field is ${layoutFieldId ?: "unset"}, not $sourceFieldId")
            }
            specs.firstOrNull { it.fields.isEmpty() }?.let {
                integrity("spec ${it.specId} of table $tableId has no readable fields")
            }
            mountValues.firstOrNull { it < 0 }?.let { integrity("mount value $it is not a canonical decimal") }
            val arms =
                specs.mapNotNull { spec -> ObjectPrefix.keyIndex(spec.fields, sourceFieldId)?.to(spec.specId) }
                    .groupBy({ it.first }, { it.second })
                    .map { (key, specIds) -> Arm(key, specIds.sorted()) }
                    .sortedBy { it.keyIndex }
            if (arms.isEmpty()) integrity("no spec of table $tableId has an identity field on $sourceFieldId")
            // Every permission a value can hold (read or write, #147 D2a) reads it.
            val granted: Collection<Long> =
                when (grant) {
                    null -> emptySet()
                    is PredicateGrant.All -> mountValues
                    is PredicateGrant.Values -> grant.values.keys
                }
            val values = granted.toSet().intersect(mountValues.toSet()).sorted().map(Long::toString)
            return ReadScope(catalogId, tableId, values, arms)
        }

        private fun integrity(detail: String): Nothing {
            Metrics.mountIntegrityError()
            throw MountIntegrityError(detail)
        }
    }
}

/**
 * A mount's table cannot be scoped reliably, so the mount refuses the
 * read. Not a client error: the catch-all answers 500 and logs it, and
 * `hoglake_mount_integrity_errors_total` counts it.
 */
class MountIntegrityError(
    detail: String,
) : IllegalStateException(detail)
