package com.posthog.hoglake.model

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File

/**
 * [ChangeKind.TABLE_SCOPED] — the set that decides which change rows
 * count toward a table's `snapshot_count` / `earliest_snapshot_id`.
 *
 * `hog_snapshot_change.object_id` is one column over three disjoint id
 * spaces, so a kind in the wrong set makes a table's history count a
 * namespace's or a view's.
 *
 * What this test CAN and CANNOT do is the whole design of it. It cannot
 * know which id space a new kind's `object_id` belongs to — nothing in
 * the schema records that, and a name is not evidence. The first
 * version pretended otherwise: it filtered the schema's kinds by
 * `startsWith("table_")` and compared that against an enum set built by
 * filtering on `startsWith("TABLE_")`, which is the same rule on both
 * sides. `TABLE_NAMESPACE_MOVED` would have passed it while silently
 * joining the set.
 *
 * So the assertion is COVERAGE, not correctness: the two explicit sets
 * must PARTITION the vocabulary in schema.sql — every kind classified,
 * none twice, none invented. A new kind reds until someone writes it
 * into one of them, and writing it into one is the decision this test
 * exists to force rather than to make.
 */
class TableSummaryVocabularyTest {
    private val schema = File("schema.sql").readText()

    /** The kinds the CHECK constraint on hog_snapshot_change allows. */
    private fun schemaKinds(): List<String> {
        val start = schema.indexOf("CREATE TABLE hog_snapshot_change")
        assertThat(start).describedAs("hog_snapshot_change in schema.sql").isNotEqualTo(-1)
        val check =
            schema.substring(start)
                .substringAfter("kind        text   NOT NULL CHECK (kind IN (")
                .substringBefore("))")
        return Regex("'([a-z_]+)'").findAll(check).map { it.groupValues[1] }.toList()
    }

    @Test
    fun `the schema's change vocabulary is the enum's, with no extras either way`() {
        assertThat(schemaKinds())
            .containsExactlyInAnyOrderElementsOf(ChangeKind.entries.map { it.wire })
    }

    @Test
    fun `every change kind is classified as table-scoped or not, exactly once`() {
        val table = ChangeKind.TABLE_SCOPED
        val other = ChangeKind.NON_TABLE_SCOPED

        // Disjoint: a kind in both would be counted and excluded at once.
        assertThat(table.intersect(other))
            .describedAs("a kind classified both ways")
            .isEmpty()

        // Exhaustive, against the SCHEMA's list rather than the enum's,
        // so a kind added to the database and the enum but to neither
        // set reds here instead of silently defaulting to "not a table".
        assertThat((table + other).map { it.wire })
            .describedAs(
                "every kind in schema.sql must be classified in ChangeKind.TABLE_SCOPED or " +
                    "NON_TABLE_SCOPED — a new kind's object_id is a table id or it is not, and " +
                    "only the author of the kind knows which",
            )
            .containsExactlyInAnyOrderElementsOf(schemaKinds())
    }

    @Test
    fun `TABLE_SCOPED holds the six kinds whose object_id is a table id`() {
        // The literal set, written out. This is the one assertion in the
        // file that a reviewer has to check against the schema by
        // reading it — deliberately, because it is the one fact no
        // derivation can supply. It is spelled with wire strings so the
        // failure message names what the QUERY binds.
        assertThat(ChangeKind.TABLE_SCOPED.map { it.wire })
            .containsExactlyInAnyOrder(
                "table_created",
                "table_dropped",
                "table_altered",
                "table_inserted_into",
                "table_deleted_from",
                "table_compacted",
            )
    }
}
