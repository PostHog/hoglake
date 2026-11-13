package com.posthog.hoglake.compaction

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.wireObjectMapper
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.MessageTypeParser
import org.apache.parquet.schema.Type
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import shaded.parquet.org.apache.thrift.TBase
import shaded.parquet.org.apache.thrift.TEnum
import shaded.parquet.org.apache.thrift.TFieldIdEnum
import java.nio.file.Files
import java.nio.file.Path

/**
 * [VariantLayout] against pyhoglake's `Plan.expected_elements`, through
 * the vector file both sides read
 * (pyhoglake/tests/vectors/variant_layout_vectors.json, regenerated and
 * compared whole by pyhoglake/tests/test_variant_layout_vectors.py): each
 * declaration's group, written to Thrift SchemaElements by parquet-java's
 * own footer converter, must be the file's elements, field id by field
 * id. The one licence is the legacy converted type, with a DECIMAL's scale
 * and precision beside it, which parquet-java and pyarrow spell
 * differently (TIME_MICROS) and [VariantLayout.layoutMismatch] ignores.
 */
class VariantLayoutTest {
    @Test
    fun `every vector's layout is the elements pyhoglake derives`() {
        val root =
            wireObjectMapper().readValue<Map<String, Any?>>(
                Files.readAllBytes(VariantLayoutVectorFile.resolve()),
            )
        assertThat(root["format"]).isEqualTo("hoglake-variant-layout-vectors")
        assertThat(root["version"]).isEqualTo(1)
        val vectors = root["vectors"] as List<*>
        assertThat(vectors)
            .describedAs("vector count (pinned in both languages; see VariantLayoutVectorFile.EXPECTED_COUNT)")
            .hasSize(VariantLayoutVectorFile.EXPECTED_COUNT)
        val failures = mutableListOf<String>()
        for (entry in vectors) {
            val vector = entry as Map<*, *>
            val nullable = vector["nullable"] as Boolean
            val group =
                VariantLayout.groupType(
                    column(vector["declaration"], nullable),
                    if (nullable) Type.Repetition.OPTIONAL else Type.Repetition.REQUIRED,
                )
            val got = footerElements(MessageType("m", group)).drop(1).map(::thrift)
            val want = (vector["elements"] as List<*>).map { normalize((it as Map<*, *>)["element"]) }
            if (got.map(::withoutLegacy) != want.map(::withoutLegacy)) {
                failures += "'${vector["id"]}':\n  want $want\n  got  $got"
            }
        }
        assertThat(failures).describedAs("layouts that differ from pyhoglake's").isEmpty()
    }

    @Test
    fun `the derived layout conforms to itself and to its own wider spellings`() {
        val group = VariantLayout.groupType(column(DECLARATION), Type.Repetition.OPTIONAL)
        assertThat(VariantLayout.layoutMismatch(group, group)).isNull()
        // A renamed column's file, a NOT NULL column's REQUIRED group, and
        // the TIME_MICROS that Trino and parquet-java write beside a time
        // leaf: none changes a value the copy moves.
        val wider =
            parse(
                """
                required group old_name (VARIANT(1)) = 9 {
                  required binary metadata;
                  optional binary value;
                  optional group typed_value {
                    required group t { optional binary value; optional int64 typed_value (TIME(MICROS,false)); }
                    required group tags {
                      optional binary value;
                      optional group typed_value (LIST) {
                        repeated group list {
                          required group element { optional binary value; optional binary typed_value (STRING); }
                        }
                      }
                    }
                  }
                }
                """,
            )
        assertThat(VariantLayout.layoutMismatch(wider, group)).isNull()
    }

    @Test
    fun `every difference the copy cannot carry is refused, naming where`() {
        val group = VariantLayout.groupType(column(DECLARATION), Type.Repetition.OPTIONAL)
        val base = render(group)
        val refused =
            mapOf(
                // Fields reordered: the positional copy would swap them.
                "reordered" to
                    base.replace(TIME_FIELD, "@").replace(TAGS_FIELD, TIME_FIELD).replace("@", TAGS_FIELD),
                // An OPTIONAL field group: a missing field becomes a null group.
                "optional field group" to base.replace("required group t {", "optional group t {"),
                "extra leaf" to base.replace("binary metadata;", "binary metadata; optional binary x;"),
                "missing value" to base.replace("required group t { optional binary value;", "required group t {"),
                // DuckDB's spelling of an unsigned integer.
                "unsigned leaf" to base.replace(TIME_LEAF, "int64 typed_value (INTEGER(64,false))"),
                "another physical type" to base.replace(TIME_LEAF, "int32 typed_value"),
                // The next five each differ in ONE way, so no earlier clause
                // refuses them first. The copy reads a leaf by the input's
                // physical type and pairs children by position, so a
                // trailing child too many or too few passes every name.
                "physical type only" to base.replace(T_VALUE, "required group t { optional int64 value;"),
                "a missing trailing field" to base.replace(" $TAGS_FIELD", ""),
                "an extra trailing field" to
                    base.replace(TAGS_FIELD, "$TAGS_FIELD ${TIME_FIELD.replace("group t", "group z")}"),
                "a group where a leaf is declared" to
                    base.replace(T_VALUE, "required group t { optional group value { optional binary x; }"),
                "a leaf where a group is declared" to base.replace(TAGS_FIELD, "required binary tags;"),
                "a renamed leaf" to base.replace("required group t {", "required group u {"),
                "no VARIANT annotation" to base.replace(" (VARIANT(1))", ""),
                "an unshredded file under a declaration" to
                    "optional group v (VARIANT(1)) { required binary metadata; required binary value; }",
            )
        for ((case, schema) in refused) {
            assertThat(schema).describedAs(case).isNotEqualTo(base)
            assertThat(VariantLayout.layoutMismatch(parse(schema), group)).describedAs(case).isNotNull()
        }
        assertThat(VariantLayout.layoutMismatch(parse(refused.getValue("optional field group")), group))
            .startsWith("v.typed_value.t is")
        // decimal16 is FIXED at the fewest bytes its precision needs, 9 for
        // 20 digits; a wider FIXED holds the same values in other bytes.
        val decimal = mapOf("type" to "decimal16", "precision" to 20, "scale" to 4)
        val d16 = VariantLayout.groupType(column(decimal), Type.Repetition.OPTIONAL)
        assertThat(render(d16)).contains("fixed_len_byte_array(9) typed_value (DECIMAL(20,4))")
        val wider = parse(render(d16).replace("fixed_len_byte_array(9)", "fixed_len_byte_array(10)"))
        assertThat(VariantLayout.layoutMismatch(wider, d16)).contains("fixed_len_byte_array(10)")
    }

    @Test
    fun `a file's subtree canonicalizes to the layout the output writes, and equal layouts to equal groups`() {
        val group = VariantLayout.groupType(column(DECLARATION), Type.Repetition.OPTIONAL)
        // The declared layout is its own canonical form.
        assertThat(VariantLayout.canonical(group, column(DECLARATION))).isEqualTo(group)
        // A file's wider spelling — renamed, REQUIRED, legacy TIME_MICROS,
        // ids below the top — canonicalizes to the declared group exactly:
        // the relation layoutMismatch passes is equality here.
        val wider =
            parse(
                """
                required group old_name (VARIANT(1)) = 9 {
                  required binary metadata = 10;
                  optional binary value = 11;
                  optional group typed_value = 12 {
                    required group t { optional binary value; optional int64 typed_value (TIME(MICROS,false)); }
                    required group tags {
                      optional binary value;
                      optional group typed_value (LIST) {
                        repeated group list {
                          required group element { optional binary value; optional binary typed_value (STRING); }
                        }
                      }
                    }
                  }
                }
                """,
            )
        assertThat(VariantLayout.layoutMismatch(wider, group)).isNull()
        assertThat(VariantLayout.canonical(wider, column(DECLARATION))).isEqualTo(group)
        // Another layout is another group, and fingerprints follow.
        val other = parse("optional group v (VARIANT(1)) { required binary metadata; required binary value; }")
        val canonicalOther = VariantLayout.canonical(other, column(DECLARATION))!!
        assertThat(canonicalOther).isNotEqualTo(group)
        assertThat(canonicalOther).isEqualTo(VariantLayout.groupType(column(null), Type.Repetition.OPTIONAL))
        assertThat(VariantLayout.fingerprint(mapOf(7L to canonicalOther)))
            .isNotEqualTo(VariantLayout.fingerprint(mapOf(7L to group)))
            .isEqualTo(
                VariantLayout.fingerprint(mapOf(7L to VariantLayout.groupType(column(null), Type.Repetition.OPTIONAL))),
            )
        // The canonical form is what the output writes a NOT NULL column as, too.
        assertThat(VariantLayout.withRepetition(group, Type.Repetition.REQUIRED))
            .isEqualTo(VariantLayout.groupType(column(DECLARATION, nullable = false), Type.Repetition.REQUIRED))
        assertThat(VariantLayout.withRepetition(group, Type.Repetition.OPTIONAL)).isSameAs(group)
    }

    @Test
    fun `what is no VARIANT group has no canonical layout`() {
        val unshredded = "required binary metadata; required binary value;"
        val variant = "optional group v (VARIANT(1)) = 7"
        for ((case, field) in listOf(
            "a leaf" to "optional binary v = 7;",
            "a repeated group" to "repeated group v (VARIANT(1)) = 7 { $unshredded }",
            "no VARIANT annotation" to "optional group v = 7 { $unshredded }",
            "no metadata at all" to "$variant { required binary value; required binary meta; }",
            "optional metadata" to "$variant { optional binary metadata; required binary value; }",
            "a group named metadata" to
                "$variant { required group metadata { required binary m; } required binary value; }",
        )) {
            val type = MessageTypeParser.parseMessageType("message m { $field }").getType(0)
            assertThat(VariantLayout.canonical(type, column(DECLARATION))).describedAs(case).isNull()
            assertThat(VariantLayout.layoutOf(type, column(DECLARATION)))
                .describedAs(case)
                .isInstanceOfSatisfying(VariantLayout.Layout.Refused::class.java) {
                    assertThat(it.reason).contains("not one VARIANT group")
                }
        }
    }

    @Test
    fun `metadata is found by name, and the source order of the children is kept`() {
        // Spark writes `value` before `metadata`; the specification fixes no
        // order. The canonical form keeps the file's order, since the copy
        // is positional and the output is written in this very layout.
        val valueFirst = parse("optional group v (VARIANT(1)) = 7 { required binary value; required binary metadata; }")
        val canonical = VariantLayout.canonical(valueFirst, column(DECLARATION))!!
        assertThat(canonical.fields.map { it.name }).containsExactly("value", "metadata")
        assertThat(render(canonical))
            .isEqualTo("optional group v (VARIANT(1)) = 7 { required binary value; required binary metadata; }")
        assertThat(VariantLayout.layoutMismatch(valueFirst, canonical)).isNull()
        // Another layout than metadata-first, so the two never share a part.
        assertThat(canonical).isNotEqualTo(VariantLayout.groupType(column(null), Type.Repetition.OPTIONAL))
    }

    @Test
    fun `a subtree the positional copy cannot carry whole is refused, naming the node`() {
        // identityStep copies a REPEATED node only as the repeated group of
        // a LIST (one child) or a MAP (two); any other repetition is read at
        // its first value and the rest are lost — with the row count right,
        // so the output would commit. Refused before it can be a layout.
        val variant = "optional group v (VARIANT(1)) = 7"
        val refused =
            mapOf(
                "a repeated value" to "$variant { required binary metadata; repeated binary value; }",
                "a repeated typed_value leaf" to
                    "$variant { required binary metadata; optional binary value; repeated int64 typed_value; }",
                "a repeated field group" to
                    "$variant { required binary metadata; optional binary value; optional group typed_value { " +
                    "repeated group t { optional binary value; optional binary typed_value; } } }",
                "a repeated leaf inside a field group" to
                    "$variant { required binary metadata; optional binary value; optional group typed_value { " +
                    "required group t { optional binary value; repeated binary typed_value; } } }",
                "a LIST with no repeated layer" to
                    "$variant { required binary metadata; optional binary value; optional group typed_value (LIST) { " +
                    "required group element { optional binary value; } } }",
                "a LIST whose layer has two children" to
                    "$variant { required binary metadata; optional binary value; optional group typed_value (LIST) { " +
                    "repeated group list { required group element { optional binary value; } optional binary x; } } }",
                "a MAP whose key is optional" to
                    "$variant { required binary metadata; optional group m (MAP) { repeated group key_value { " +
                    "optional binary key; optional binary value; } } }",
                "a repeated leaf inside a list element" to
                    "$variant { required binary metadata; optional binary value; optional group typed_value (LIST) { " +
                    "repeated group list { required group element { repeated binary value; } } } }",
            )
        for ((case, schema) in refused) {
            val layout = VariantLayout.layoutOf(parse(schema), column(DECLARATION))
            assertThat(layout).describedAs(case).isInstanceOf(VariantLayout.Layout.Refused::class.java)
            assertThat((layout as VariantLayout.Layout.Refused).reason).describedAs(case).contains("v.")
            assertThat(VariantLayout.canonical(parse(schema), column(DECLARATION))).describedAs(case).isNull()
        }
        assertThat(
            (
                VariantLayout.layoutOf(
                    parse(refused.getValue("a repeated value")),
                    column(DECLARATION),
                ) as VariantLayout.Layout.Refused
            ).reason,
        )
            .contains("`v.value` is REPEATED outside a LIST or MAP layer")
        // The shapes the copy DOES carry: the declared layout's list, a map.
        assertThat(
            VariantLayout.layoutOf(
                VariantLayout.groupType(column(DECLARATION), Type.Repetition.OPTIONAL),
                column(DECLARATION),
            ),
        )
            .isInstanceOf(VariantLayout.Layout.Of::class.java)
        val withMap =
            parse(
                "$variant { required binary metadata; optional binary value; optional group m (MAP) { " +
                    "repeated group key_value { required binary key; optional binary value; } } }",
            )
        assertThat(
            VariantLayout.layoutOf(withMap, column(DECLARATION)),
        ).isInstanceOf(VariantLayout.Layout.Of::class.java)
    }

    @Test
    fun `a refusal names the two differing nodes on one line, never their subtrees`() {
        // The grammar's widest object: its subtree printed is ~120 KB, in a
        // warning logged for the group on every sweep.
        val widest =
            mapOf("type" to "object", "fields" to (1..1000).map { mapOf("name" to "f$it", "type" to "string") })
        val group = VariantLayout.groupType(column(widest), Type.Repetition.OPTIONAL)
        val unshredded = parse("optional group v (VARIANT(1)) { required binary metadata; required binary value; }")
        assertThat(VariantLayout.layoutMismatch(unshredded, group)).isEqualTo(
            "v is `optional group v (VARIANT(1)) of 2 fields`, " +
                "where the declaration lays out `optional group v (VARIANT(1)) of 3 fields`",
        )
    }

    @Test
    fun `no declaration and a root variant are the unshredded layout, and a bad declaration is a schema skip`() {
        for (declaration in listOf(null, mapOf("type" to "variant"))) {
            assertThat(render(VariantLayout.groupType(column(declaration), Type.Repetition.OPTIONAL)))
                .isEqualTo("optional group v (VARIANT(1)) = 7 { required binary metadata; required binary value; }")
        }
        assertThat(VariantLayout.shape(column(null))).isEqualTo(3 to false)
        assertThat(VariantLayout.shape(column(DECLARATION))).isEqualTo(1 + 1 + 2 + 1 + 3 + 1 + 1 + 1 + 3 to true)
        val bad = column(mapOf("type" to "object", "fields" to emptyList<Any>()))
        assertThatThrownBy { VariantLayout.groupType(bad, Type.Repetition.OPTIONAL) }
            .isInstanceOf(UnconvertibleSchemaException::class.java)
            .hasMessageContaining("has no fields")
        assertThat(VariantLayout.shape(bad)).isNull()
    }

    /** [schema] as the Thrift SchemaElements parquet-java writes into a footer. */
    private fun footerElements(schema: MessageType) =
        ParquetMetadataConverter()
            .toParquetMetadata(1, ParquetMetadata(FileMetaData(schema, emptyMap(), "test"), emptyList()))
            .schema

    /** A Thrift struct as the vector file spells one: field id -> value, enums by value, numbers as Long. */
    private fun thrift(value: Any?): Any? =
        when (value) {
            is TBase<*, *> -> {
                @Suppress("UNCHECKED_CAST")
                val struct = value as TBase<*, TFieldIdEnum>
                (1..32).mapNotNull { id -> struct.fieldForId(id)?.takeIf { struct.isSet(it) } }
                    .associate { it.thriftFieldId.toString() to thrift(struct.getFieldValue(it)) }
            }
            is TEnum -> value.value.toLong()
            else -> normalize(value)
        }

    private fun normalize(value: Any?): Any? =
        when (value) {
            is Map<*, *> -> value.entries.associate { (k, v) -> k.toString() to normalize(v) }
            is Number -> value.toLong()
            else -> value
        }

    @Suppress("UNCHECKED_CAST")
    private fun withoutLegacy(element: Any?): Map<String, Any?> = (element as Map<String, Any?>) - setOf("6", "7", "8")

    private fun column(
        declaration: Any?,
        nullable: Boolean = true,
    ) = Column(
        7,
        0,
        ColumnDef("v", ColType.VARIANT, declaration?.let { mapOf("shredding" to it) }, nullable = nullable),
    )

    private fun parse(group: String): GroupType =
        MessageTypeParser.parseMessageType(
            "message m { $group }",
        ).getType(0).asGroupType()

    private fun render(group: GroupType): String =
        MessageType(
            "m",
            group,
        ).toString().trim().removePrefix("message m {").removeSuffix("}").trim().replace(Regex("\\s+"), " ")

    private companion object {
        val DECLARATION =
            mapOf(
                "type" to "object",
                "fields" to
                    listOf(
                        mapOf("name" to "t", "type" to "time"),
                        mapOf("name" to "tags", "type" to "array", "element" to mapOf("type" to "string")),
                    ),
            )
        const val TIME_LEAF = "int64 typed_value (TIME(MICROS,false))"
        const val T_VALUE = "required group t { optional binary value;"
        const val TIME_FIELD = "required group t { optional binary value; optional $TIME_LEAF; }"
        val TAGS_FIELD =
            "required group tags { optional binary value; optional group typed_value (LIST) { repeated group list { " +
                "required group element { optional binary value; optional binary typed_value (STRING); } } } }"
    }
}

/**
 * Locating the shared vector file as [com.posthog.hoglake.service.VariantShreddingVectorFile]
 * does: relative to `server/` first, then walking up from user.dir, and a
 * missing file FAILS, naming every path tried.
 */
internal object VariantLayoutVectorFile {
    /**
     * Pinned exactly, so a vector lost to a bad merge fails instead of
     * shrinking coverage in silence. Update DELIBERATELY, together with the
     * identical pin in pyhoglake/tests/test_variant_layout_vectors.py.
     */
    const val EXPECTED_COUNT = 49

    private const val REPO_RELATIVE = "pyhoglake/tests/vectors/variant_layout_vectors.json"

    fun resolve(): Path {
        val fromCwd = Path.of("..").resolve(REPO_RELATIVE).normalize()
        if (Files.exists(fromCwd)) return fromCwd
        val tried = mutableListOf("cwd-relative: ${fromCwd.toAbsolutePath()}")
        var dir: Path? = Path.of(System.getProperty("user.dir")).toAbsolutePath()
        while (dir != null) {
            val candidate = dir.resolve(REPO_RELATIVE)
            if (Files.exists(candidate)) return candidate
            tried += "walk-up: $candidate"
            dir = dir.parent
        }
        throw AssertionError(
            "layout vector file not found, so the Python/Kotlin layout parity gate cannot run. Tried:\n" +
                tried.joinToString("\n") { "  $it" },
        )
    }
}
