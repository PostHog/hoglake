package com.posthog.hoglake.commit

import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.ObjectPrefix
import com.posthog.hoglake.model.ObjectPrefixLayout
import com.posthog.hoglake.service.Identifiers

/**
 * The object-prefix layout's commit rules ([ObjectPrefix]), as pure
 * functions over a table's resolved layout: the ONE copy both catalog
 * backends run. CommitService calls them from its tail (under the commit
 * lock, after the structural checks); the hoglite commit port calls them
 * from its pre-flight, which its in-transaction schema fence pins to the
 * layout it read. The rules, their order, and the 422's text are
 * CommitService.checkObjectPrefix's — see its KDoc for the why.
 */
internal object ObjectPrefixRules {
    /**
     * Why each of [files] breaks the layout, one entry per offending path
     * (empty = all good). [files] are SANITIZED registrations (StatsSanity
     * drops a bound it cannot decode, and a dropped bound proves nothing)
     * whose partition-value arity was already validated; [keyIndex] is the
     * live spec's single identity key on the prefix field.
     */
    fun appendViolations(
        files: List<FileRegistration>,
        layout: ObjectPrefixLayout,
        keyIndex: Int,
        namespace: String,
        table: String,
        dataPath: String,
    ): List<String> {
        val violations = ArrayList<String>()
        for (file in files) {
            // validateFiles already refused a missing list and a wrong arity.
            val raw = file.partitionValues!![keyIndex]
            val value = ObjectPrefix.parseValue(raw, layout.colType)
            if (value == null) {
                violations += "${file.path} (partition value ${raw?.let { "'${Identifiers.cap(it)}'" } ?: "null"} " +
                    "is not a canonical ${layout.colType.wire} value: digits only, no sign or leading zero)"
                continue
            }
            val prefix = ObjectPrefix.prefix(dataPath, namespace, table, layout, raw!!)
            if (!ObjectPrefix.isUnder(file.path, prefix)) {
                violations += "${file.path} (expected under $prefix)"
                continue
            }
            // Deferred stats: the hydrator checks the footer.
            val stats = file.columnStats ?: continue
            val verdict =
                ObjectPrefix.checkFieldStats(layout, value, stats.firstOrNull { it.fieldId == layout.fieldId })
            if (verdict != ObjectPrefix.Verdict.OK) {
                violations += "${file.path} (its column stats for '${layout.fieldName}' do not prove every row " +
                    "is $raw: ${describe(verdict)})"
            }
        }
        return violations
    }

    /**
     * Why one DV registration breaks its table's layout, or null when it
     * does not: its target has no canonical value for the field ([raw]:
     * the target's identity partition value for it — fail closed), or the
     * DV is not under the target's prefix.
     */
    fun dvViolation(
        reg: DeleteFileRegistration,
        raw: String?,
        layout: ObjectPrefixLayout,
        namespace: String,
        table: String,
        dataPath: String,
    ): String? {
        if (ObjectPrefix.parseValue(raw, layout.colType) == null) {
            return "${reg.path} (its target data_file_id ${reg.dataFileId} has no canonical " +
                "'${layout.fieldName}' value, got ${raw?.let { "'${Identifiers.cap(it)}'" } ?: "none"})"
        }
        val prefix = ObjectPrefix.prefix(dataPath, namespace, table, layout, raw!!)
        return if (ObjectPrefix.isUnder(reg.path, prefix)) {
            null
        } else {
            "${reg.path} (expected under $prefix, its target data_file_id ${reg.dataFileId}'s prefix)"
        }
    }

    fun describe(verdict: ObjectPrefix.Verdict): String =
        when (verdict) {
            ObjectPrefix.Verdict.OK -> "ok"
            ObjectPrefix.Verdict.MISSING_STATS -> "no stats row for the field"
            ObjectPrefix.Verdict.NULL_BOUNDS ->
                "a bound is missing (absent, or dropped as undecodable for the column's type)"
            ObjectPrefix.Verdict.NULLS_PRESENT -> "null_count is not 0"
            ObjectPrefix.Verdict.BOUND_MISMATCH -> "the lower and upper bounds are not both the value"
        }

    /**
     * The 422 for [violations] on [namespace].[table]: the first five, the
     * count of the rest, and the layout a writer has to follow.
     */
    fun violation(
        namespace: String,
        table: String,
        dataPath: String,
        layout: ObjectPrefixLayout,
        violations: List<String>,
    ): HoglakeException.ObjectPrefixViolation {
        val qualified = "$namespace.$table"
        val expected = ObjectPrefix.tableRoot(dataPath, namespace, table) + "${layout.fieldName}=<value>/"
        return HoglakeException.ObjectPrefixViolation(
            "${violations.size} file(s) registered to $qualified break its object-prefix layout: " +
                violations.take(5).joinToString("; ") +
                (if (violations.size > 5) " (+${violations.size - 5} more)" else "") +
                ". Every data file and deletion vector of $qualified lives under $expected, <value> being the " +
                "data file's identity partition value for '${layout.fieldName}' (a DV's: its target's), with " +
                "column stats proving every row holds it; re-read the table and re-prepare, replaying this " +
                "request cannot succeed",
            table = qualified,
        )
    }
}
