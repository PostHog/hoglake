package com.posthog.hoglake.hoglite

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.service.VariantShredding
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.PostgresCatalogStore
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import java.math.BigInteger
import java.util.UUID

/**
 * `set_variant_shredding` behind the CatalogStore seam, on both backends
 * with one set of assertions: the declaration a top-level variant column
 * carries is replaced or cleared under the column's field id, visible on
 * the next read and time-travelled away at the snapshot before; a writer
 * whose read_snapshot predates a CHANGE meets the DDL refusal every alter
 * produces; and re-declaring what the column already declares writes
 * nothing and mints no snapshot, so it refuses nobody. Every refusal is
 * asserted with the state it leaves: head unmoved, declaration as it was.
 *
 * In this package so the hoglite-live job (`hoglite.*IntegrationTest`)
 * runs it against a live hoglited (HogliteConformanceEngine); without a
 * binary the hoglite half skips and the Postgres half still runs.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteAlterVariantShreddingIntegrationTest {
    enum class Backend { POSTGRES, HOGLITE }

    private var pg: PgTestSupport.TestDb? = null
    private var live: HogliteConformanceEngine? = null
    private lateinit var store: CatalogStore
    private lateinit var alters: AlterBackend
    private lateinit var selected: Backend

    /**
     * Every column-version row the store holds for [fieldId] of `ns.t`,
     * live or ended: the fixture is the catalog's only table, so the
     * catalog bounds it. A re-declaration that wrote an identical row
     * would be invisible to `Column` equality; it is not to this count.
     */
    private fun columnRows(
        cat: String,
        fieldId: Long,
    ): Long =
        when (selected) {
            Backend.POSTGRES ->
                pg!!.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        """
                        SELECT COUNT(*) FROM hog_column c JOIN hog_catalog k USING (catalog_id)
                        WHERE k.name = :cat AND c.field_id = :field
                        """,
                    ).bind("cat", cat).bind("field", fieldId).mapTo(Long::class.java).one()
                }
            Backend.HOGLITE ->
                // Bound to the catalog's tenant the way every store call is.
                live!!.store.catalogOperation(cat) {
                    live!!.store.queryCatalog(cat, "SELECT COUNT(*) FROM columns WHERE field_id = ?", fieldId)
                        .firstRow()!![0].asLong()
                }
        }

    private fun select(backend: Backend) {
        selected = backend
        when (backend) {
            Backend.POSTGRES -> {
                val db = pg ?: PgTestSupport.freshDatabase().also { pg = it }
                store = PostgresCatalogStore(CatalogService(db.jdbi), CommitService(db.jdbi), ScanService(db.jdbi))
                alters = AlterService(db.jdbi)
            }
            Backend.HOGLITE -> {
                val engine = live ?: HogliteConformanceEngine().also { live = it }
                store = engine.store
                alters = HogliteAlters(engine.store)
            }
        }
    }

    @AfterAll
    fun close() {
        pg?.close()
        live?.close()
    }

    /**
     * `ns.t`: id long (field 1), properties variant declared [first] (2),
     * raw variant undeclared (3), r struct{x variant} (4, 5), blank
     * variant declared `{"shredding": null}` (6).
     */
    private fun fixture(): String {
        val cat = "shred" + UUID.randomUUID().toString().replace("-", "")
        store.createCatalog(cat, "s3://hoglake-it/$cat")
        store.createNamespace(cat, "ns")
        store.createTable(
            cat,
            "ns",
            "t",
            listOf(
                ColumnDef("id", ColType.LONG),
                ColumnDef("properties", ColType.VARIANT, typeParams = params(first)),
                ColumnDef("raw", ColType.VARIANT),
                ColumnDef("r", ColType.STRUCT, children = listOf(ColumnDef("x", ColType.VARIANT))),
                ColumnDef("blank", ColType.VARIANT, typeParams = mapOf(VariantShredding.KEY to null)),
            ),
        )
        return cat
    }

    private fun head(cat: String) = store.getCatalog(cat).headSnapshotId

    private fun columns(
        cat: String,
        snapshot: Long? = null,
    ): List<Column> = store.getTable(cat, "ns", "t", snapshot = snapshot, totals = false).columns

    private fun declared(
        cat: String,
        name: String = "properties",
        snapshot: Long? = null,
    ): Map<String, Any?>? = columns(cat, snapshot).single { it.def.name == name }.def.typeParams

    private fun alter(
        cat: String,
        vararg ops: AlterOp,
        read: Long? = null,
    ) = alters.alterTable(cat, "ns", "t", ops.toList(), readSnapshot = read)

    private fun append(
        cat: String,
        read: Long,
    ) = store.commit(
        cat,
        CommitRequest(
            readSnapshot = read,
            idempotencyKey = UUID.randomUUID(),
            appends =
                listOf(
                    TableAppend(
                        "ns",
                        "t",
                        listOf(FileRegistration("s3://hoglake-it/$cat/${UUID.randomUUID()}", 10, 100, 50)),
                    ),
                ),
        ),
    )

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a new declaration replaces the old under the same field id, and the old reads at the snapshot before`(
        backend: Backend,
    ) {
        select(backend)
        val cat = fixture()
        val before = head(cat)
        val fieldId = columns(cat).single { it.def.name == "properties" }.fieldId
        assertThat(declared(cat)).isEqualTo(params(first))

        val info = alter(cat, AlterOp.SetVariantShredding("properties", second))

        assertThat(info.snapshotId).isEqualTo(before + 1)
        assertThat(info.readSnapshotId).isEqualTo(before + 1)
        val altered = info.columns.single { it.def.name == "properties" }
        assertThat(altered.fieldId).isEqualTo(fieldId)
        assertThat(altered.def.typeParams).isEqualTo(params(second))
        // The next read is the new declaration; the one at the snapshot
        // before is still the old, under the same field id.
        assertThat(head(cat)).isEqualTo(before + 1)
        assertThat(columns(cat).single { it.def.name == "properties" }.fieldId).isEqualTo(fieldId)
        assertThat(declared(cat)).isEqualTo(params(second))
        assertThat(declared(cat, snapshot = before)).isEqualTo(params(first))
        // The declaration round-trips as it was set: numbers, order, nesting.
        assertThat(declared(cat)!!["shredding"]).isEqualTo(second)
        // The other columns are untouched.
        assertThat(declared(cat, "raw")).isNull()
        assertThat(columns(cat).map { it.def.name }).containsExactly("id", "properties", "raw", "r", "blank")
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a null declaration clears, and an undeclared column can be declared`(backend: Backend) {
        select(backend)
        val cat = fixture()

        val cleared = alter(cat, AlterOp.SetVariantShredding("properties", null))
        assertThat(cleared.columns.single { it.def.name == "properties" }.def.typeParams).isNull()
        assertThat(declared(cat)).describedAs("cleared: no type_params at all").isNull()

        alter(cat, AlterOp.SetVariantShredding("raw", second))
        assertThat(declared(cat, "raw")).isEqualTo(params(second))
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `re-declaring what the column declares writes nothing, mints no snapshot and refuses no writer`(
        backend: Backend,
    ) {
        select(backend)
        val cat = fixture()
        val before = head(cat)
        val rowsBefore = columns(cat)
        // Store-level: every version row of every column, so a rewritten
        // identical row (which Column equality cannot see) would show.
        val storedBefore = rowsBefore.associate { it.def.name to columnRows(cat, it.fieldId) }
        assertThat(storedBefore.values).allMatch { it == 1L }

        // The same declaration, spelt differently: another key order, an
        // integer as a BigInteger (the mapper's spelling of a large literal).
        val respelt =
            mapOf(
                "fields" to
                    listOf(
                        mapOf("type" to "string", "name" to "\$browser"),
                        mapOf(
                            "scale" to BigInteger.valueOf(2),
                            "precision" to 18L,
                            "type" to "decimal8",
                            "name" to "price",
                        ),
                    ),
                "type" to "object",
            )
        for ((label, op) in listOf(
            "the declaration as stored" to AlterOp.SetVariantShredding("properties", first),
            "the declaration respelt" to AlterOp.SetVariantShredding("properties", respelt),
            "clearing a column with no type_params" to AlterOp.SetVariantShredding("raw", null),
            "clearing a column declared {shredding: null}" to AlterOp.SetVariantShredding("blank", null),
        )) {
            val info = alter(cat, op)
            assertThat(info.snapshotId).describedAs("%s: answers the unmoved head", label).isEqualTo(before)
            assertThat(info.readSnapshotId).describedAs(label).isEqualTo(before)
            assertThat(info.columns.sortedBy { it.ordinal }).describedAs(label).isEqualTo(rowsBefore)
            assertThat(head(cat)).describedAs("%s: no snapshot minted", label).isEqualTo(before)
            assertThat(columns(cat)).describedAs("%s: no column row written", label).isEqualTo(rowsBefore)
            assertThat(rowsBefore.associate { it.def.name to columnRows(cat, it.fieldId) })
                .describedAs("%s: no column-version row written, identical or not", label)
                .isEqualTo(storedBefore)
            if (backend == Backend.POSTGRES) {
                // A no-op receipt does not run the exact aggregate under
                // the commit lock: it answers as a head GET does, with the
                // sampler's totals and their freshness, or none at all.
                // (hoglite has no sampler; its GET and this receipt both
                // aggregate, and it holds no lock while they do.)
                assertThat(info.fileCount == null || info.totalsSnapshotId != null)
                    .describedAs("%s: totals are sampled or absent, never an exact aggregate: %s", label, info)
                    .isTrue()
                assertThat(info.totalsAsOf == null).isEqualTo(info.totalsSnapshotId == null)
            }
        }
        // Two no-ops in one request are still one.
        assertThat(
            alter(
                cat,
                AlterOp.SetVariantShredding("properties", first),
                AlterOp.SetVariantShredding("raw", null),
            ).snapshotId,
        ).isEqualTo(before)
        assertThat(head(cat)).isEqualTo(before)

        // Nothing was DDL: a writer that read before all of it lands.
        val landed = append(cat, read = before)
        assertThat(landed.snapshotId).isEqualTo(before + 1)
        // A request guarded by that older read is not refused either.
        assertThat(alter(cat, AlterOp.SetVariantShredding("properties", first), read = before).snapshotId)
            .isEqualTo(before + 1)

        // ...whereas a CHANGING alter's receipt is exact for its own
        // snapshot, with no freshness: one file landed above.
        val changed = alter(cat, AlterOp.SetVariantShredding("raw", second))
        assertThat(changed.snapshotId).isEqualTo(before + 2)
        assertThat(changed.fileCount).isEqualTo(1L)
        assertThat(changed.totalsSnapshotId).isNull()
        assertThat(changed.totalsAsOf).isNull()
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `an equal declaration beside another op writes nothing while the other op commits`(backend: Backend) {
        select(backend)
        val cat = fixture()
        val before = head(cat)
        val propertiesBefore = columns(cat).single { it.def.name == "properties" }
        val rawBefore = columns(cat).single { it.def.name == "raw" }
        assertThat(columnRows(cat, propertiesBefore.fieldId)).isEqualTo(1L)
        assertThat(columnRows(cat, rawBefore.fieldId)).isEqualTo(1L)

        val info =
            alter(
                cat,
                AlterOp.SetVariantShredding("properties", first),
                AlterOp.SetColumnComment("raw", "untyped"),
                AlterOp.SetVariantShredding("raw", null),
            )

        assertThat(info.snapshotId).isEqualTo(before + 1)
        assertThat(head(cat)).isEqualTo(before + 1)
        assertThat(columns(cat).single { it.def.name == "raw" }.def.comment).isEqualTo("untyped")
        // `properties` was not rewritten: its row is the one from before,
        // still visible, and nothing begins at the new snapshot for it.
        assertThat(columns(cat).single { it.def.name == "properties" }).isEqualTo(propertiesBefore)
        // Store-level, since an identical rewritten row would pass the
        // equality above: `properties` still has its one version row;
        // `raw`, whose comment changed, has the ended one and the new one.
        assertThat(columnRows(cat, propertiesBefore.fieldId)).isEqualTo(1L)
        assertThat(columnRows(cat, rawBefore.fieldId)).isEqualTo(2L)
        assertThat(declared(cat, "raw")).isNull()
        // And the snapshot IS DDL, for the comment: a stale writer is refused.
        assertThatThrownBy { append(cat, read = before) }
            .isExactlyInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `a writer whose read_snapshot predates a change gets ddl_since_read_snapshot and lands on a re-read`(
        backend: Backend,
    ) {
        select(backend)
        val cat = fixture()
        val read = head(cat)
        alter(cat, AlterOp.SetVariantShredding("properties", second))
        val afterAlter = head(cat)

        val error = catchThrowable { append(cat, read = read) }
        assertThat(error).isExactlyInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
        error as HoglakeException.DdlSinceReadSnapshot
        assertThat(error.tables).containsExactly("ns.t")
        assertThat(error.readSnapshot).isEqualTo(read)
        assertThat(head(cat)).describedAs("a refused commit mints nothing").isEqualTo(afterAlter)

        // The same file against a fresh read registers: the declaration
        // is a target, not a rule on files.
        val result = append(cat, read = afterAlter)
        assertThat(result.snapshotId).isEqualTo(afterAlter + 1)

        // The alter itself is guarded the same way: one issued on a
        // read_snapshot that predates another alter is refused, even
        // when what it asks for would be a no-op.
        assertThatThrownBy { alter(cat, AlterOp.SetVariantShredding("properties", second), read = read) }
            .isExactlyInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
        assertThatThrownBy { alter(cat, AlterOp.SetVariantShredding("properties", first), read = read) }
            .isExactlyInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
        assertThat(declared(cat)).isEqualTo(params(second))
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(Backend::class)
    fun `the op applies to a column an earlier op in the same request added`(backend: Backend) {
        select(backend)
        val cat = fixture()
        val before = head(cat)
        val info =
            alter(
                cat,
                AlterOp.AddColumn(ColumnDef("later", ColType.VARIANT, typeParams = params(first))),
                AlterOp.SetVariantShredding("later", second),
            )
        assertThat(info.snapshotId).isEqualTo(before + 1)
        assertThat(declared(cat, "later")).isEqualTo(params(second))
        // One snapshot, one live row for the new column: the row the
        // first op created was replaced, not ended, at the same snapshot.
        assertThat(columns(cat).count { it.def.name == "later" }).isEqualTo(1)
        assertThat(columns(cat, snapshot = before).map { it.def.name }).doesNotContain("later")
    }

    @ParameterizedTest(name = "{0} / {1}")
    @MethodSource("refusals")
    fun `a declaration no column definition would accept is refused, and the table is as it was`(
        backend: Backend,
        label: String,
        op: AlterOp.SetVariantShredding,
        detail: String,
    ) {
        select(backend)
        val cat = fixture()
        val before = head(cat)
        val columnsBefore = columns(cat)

        assertThatThrownBy { alter(cat, op) }
            .describedAs(label)
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessage(detail)

        assertThat(head(cat)).describedAs("%s: no snapshot minted", label).isEqualTo(before)
        assertThat(columns(cat)).describedAs("%s: columns unchanged", label).isEqualTo(columnsBefore)
        assertThat(declared(cat)).isEqualTo(params(first))
        // Nor does a refused op in a multi-op request leave the earlier
        // ops' work behind: the whole request is one transaction.
        assertThatThrownBy { alter(cat, AlterOp.SetVariantShredding("raw", second), op) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
        assertThat(head(cat)).isEqualTo(before)
        assertThat(declared(cat, "raw")).isNull()
        // A no-op beside it does not make the request a no-op: the bad op
        // is still refused, as a request that would commit refuses it.
        assertThatThrownBy { alter(cat, AlterOp.SetVariantShredding("properties", first), op) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessage(detail)
    }

    companion object {
        private val first: Map<String, Any?> =
            mapOf(
                "type" to "object",
                "fields" to
                    listOf(
                        mapOf("name" to "\$browser", "type" to "string"),
                        mapOf("name" to "price", "type" to "decimal8", "precision" to 18, "scale" to 2),
                    ),
            )

        private val second: Map<String, Any?> =
            mapOf(
                "type" to "object",
                "fields" to
                    listOf(
                        mapOf("name" to "\$browser", "type" to "string"),
                        mapOf("name" to "\$os", "type" to "string"),
                        mapOf("name" to "tags", "type" to "array", "element" to mapOf("type" to "string")),
                        mapOf("name" to "payload", "type" to "variant"),
                    ),
            )

        private fun params(declaration: Map<String, Any?>) = mapOf("shredding" to declaration)

        private fun objectOf(fields: List<Map<String, Any?>>) = mapOf("type" to "object", "fields" to fields)

        /** An object nested [levels] deep (the root counting as one). */
        private fun nested(levels: Int): Map<String, Any?> {
            var node: Map<String, Any?> = mapOf("type" to "string")
            repeat(levels) { node = objectOf(listOf(mapOf("name" to "a") + node)) }
            return node
        }

        @JvmStatic
        fun refusals(): List<Arguments> =
            Backend.entries.flatMap { backend ->
                listOf(
                    Arguments.of(
                        backend,
                        "a column of another type",
                        AlterOp.SetVariantShredding("id", mapOf("type" to "string")),
                        "column 'id' is 'long', and only a variant column can declare type_params.shredding",
                    ),
                    Arguments.of(
                        backend,
                        "clearing a column of another type",
                        AlterOp.SetVariantShredding("id", null),
                        "column 'id' is 'long', and only a variant column can declare type_params.shredding",
                    ),
                    Arguments.of(
                        backend,
                        "a nested variant",
                        AlterOp.SetVariantShredding("r.x", mapOf("type" to "string")),
                        "variant column 'r.x' is nested, and only a top-level variant column can declare " +
                            "type_params.shredding",
                    ),
                    Arguments.of(
                        backend,
                        "clearing a nested variant",
                        AlterOp.SetVariantShredding("r.x", null),
                        "variant column 'r.x' is nested, and only a top-level variant column can declare " +
                            "type_params.shredding",
                    ),
                    Arguments.of(
                        backend,
                        "a struct",
                        AlterOp.SetVariantShredding("r", mapOf("type" to "string")),
                        "column 'r' is 'struct', and only a variant column can declare type_params.shredding",
                    ),
                    Arguments.of(
                        backend,
                        "an unknown column",
                        AlterOp.SetVariantShredding("ghost", mapOf("type" to "string")),
                        "column 'ghost' does not exist",
                    ),
                    Arguments.of(
                        backend,
                        "an unknown type",
                        AlterOp.SetVariantShredding("properties", mapOf("type" to "text")),
                        "variant column 'properties' has an invalid type_params.shredding: " +
                            "$ has an unknown type 'text'",
                    ),
                    Arguments.of(
                        backend,
                        "not an object",
                        AlterOp.SetVariantShredding("properties", "string"),
                        "variant column 'properties' has an invalid type_params.shredding: $ is not a JSON object",
                    ),
                    Arguments.of(
                        backend,
                        "a decimal its width does not hold",
                        AlterOp.SetVariantShredding(
                            "properties",
                            objectOf(
                                listOf(mapOf("name" to "price", "type" to "decimal4", "precision" to 18, "scale" to 2)),
                            ),
                        ),
                        "variant column 'properties' has an invalid type_params.shredding: " +
                            "$.price has precision 18 and scale 2, which decimal4 does not hold",
                    ),
                    Arguments.of(
                        backend,
                        "fields that differ only by case",
                        AlterOp.SetVariantShredding(
                            "properties",
                            objectOf(
                                listOf(
                                    mapOf("name" to "plan", "type" to "string"),
                                    mapOf("name" to "Plan", "type" to "string"),
                                ),
                            ),
                        ),
                        "variant column 'properties' has an invalid type_params.shredding: " +
                            "$ has fields that differ only by case: 'plan' and 'Plan'",
                    ),
                    Arguments.of(
                        backend,
                        "more than ${VariantShredding.MAX_FIELDS} fields",
                        AlterOp.SetVariantShredding(
                            "properties",
                            objectOf(
                                (0..VariantShredding.MAX_FIELDS).map { mapOf("name" to "f$it", "type" to "string") },
                            ),
                        ),
                        "variant column 'properties' has an invalid type_params.shredding: " +
                            "$ has more than ${VariantShredding.MAX_FIELDS} fields and arrays",
                    ),
                    Arguments.of(
                        backend,
                        "a path of names longer than ${VariantShredding.MAX_PATH_NAME_BYTES} bytes",
                        AlterOp.SetVariantShredding(
                            "properties",
                            objectOf(
                                listOf(
                                    mapOf(
                                        "name" to "a".repeat(VariantShredding.MAX_PATH_NAME_BYTES + 1),
                                        "type" to "string",
                                    ),
                                ),
                            ),
                        ),
                        "variant column 'properties' has an invalid type_params.shredding: " +
                            "$ has a field whose name, with the names above it, is longer than " +
                            "${VariantShredding.MAX_PATH_NAME_BYTES} bytes",
                    ),
                    Arguments.of(
                        backend,
                        "objects nested more than ${VariantShredding.MAX_DEPTH} deep",
                        AlterOp.SetVariantShredding("properties", nested(VariantShredding.MAX_DEPTH + 1)),
                        "variant column 'properties' has an invalid type_params.shredding: " +
                            "$${".a".repeat(VariantShredding.MAX_DEPTH)} is nested more than " +
                            "${VariantShredding.MAX_DEPTH} levels deep",
                    ),
                )
            }
    }
}
