package com.posthog.hoglake.model

import com.posthog.hoglake.service.Identifiers
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.nio.file.Files
import java.nio.file.Path

/**
 * The type vocabulary's governing rules, pinned.
 *
 * The vocabulary lives in FIVE places that must agree — the [ColType]
 * enum, the `hog_column.col_type` CHECK in V4__scalar_types.sql, the
 * same CHECK inline in schema.sql, the OpenAPI ColumnDef enum, and the
 * webui's COLUMN_TYPES. The first four are asserted equal here from
 * their actual files, because "someone added a type to the enum and not
 * the migration" is a 500 at insert time, not a compile error. (The
 * webui list is TypeScript and is checked by nested.test.tsx,
 * which pins COLUMN_TYPES against this same vocabulary.)
 *
 * The promotion matrix is pinned twice: once literally, and once
 * against the RULE it is supposed to obey — a promotion is legal only
 * if the induced Iceberg schema evolution is. The second assertion is
 * the one that catches a future "just add uint32 -> uint64, DuckLake
 * allows it" change.
 */
class ScalarTypeParityTest {
    // ---- vocabulary agreement across artifacts ---------------------------

    @Nested
    inner class VocabularyAgreement {
        private val wireNames = ColType.entries.map { it.wire }

        @Test
        fun `wire names round-trip through fromWire for every member`() {
            for (t in ColType.entries) {
                assertThat(ColType.fromWire(t.wire))
                    .describedAs("fromWire(%s)", t.wire)
                    .isEqualTo(t)
            }
            // The one name that is not its enum name lowercased.
            assertThat(ColType.UUID_T.wire).isEqualTo("uuid")
        }

        @Test
        fun `the ten new DuckLake scalars are present`() {
            assertThat(wireNames).contains(
                "int8", "int16", "uint8", "uint16", "uint32", "uint64",
                "timestamp_s", "timestamp_ms", "timestamp_ns", "json",
            )
        }

        @Test
        fun `the latest migration's CHECK lists exactly the enum, in order`() {
            // The LAST migration that recreates the CHECK is the one
            // that must agree with the enum — the chain is append-only,
            // so V4's and V8's lists are history and V9's is the live
            // vocabulary. Adding a type means adding a migration, and
            // this assertion is what makes forgetting one a red test
            // rather than a 500 at insert.
            val sql = read("src/main/resources/db/migration/V9__nested_types.sql")
            assertThat(checkMembers(sql))
                .describedAs("V9__nested_types.sql col_type CHECK")
                .isEqualTo(wireNames)
        }

        @Test
        fun `the three container types are present and classified`() {
            assertThat(wireNames).containsSubsequence("list", "struct", "map")
            for (t in listOf(ColType.LIST, ColType.STRUCT, ColType.MAP)) {
                assertThat(t.isNested).describedAs("%s.isNested", t.wire).isTrue()
            }
            assertThat(ColType.entries.filter { it.isNested })
                .containsExactly(ColType.LIST, ColType.STRUCT, ColType.MAP)
            // Appended, never inserted: the fuzz seed corpus encodes
            // ColType.ordinal as its first byte, so inserting a member
            // silently re-points every committed seed at another type.
            assertThat(ColType.entries.takeLast(3))
                .describedAs("containers are the LAST three members")
                .containsExactly(ColType.LIST, ColType.STRUCT, ColType.MAP)
        }

        @Test
        fun `container child arity is fixed for list and map, open for struct`() {
            assertThat(ColType.LIST.requiredChildCount).isEqualTo(1)
            assertThat(ColType.MAP.requiredChildCount).isEqualTo(2)
            assertThat(ColType.STRUCT.requiredChildCount).isNull()
            assertThat(ColType.syntheticChildNames(ColType.LIST)).containsExactly("element")
            assertThat(ColType.syntheticChildNames(ColType.MAP)).containsExactly("key", "value")
            // struct children keep the user's names, so there is nothing
            // synthetic to impose.
            assertThat(ColType.syntheticChildNames(ColType.STRUCT)).isNull()
            for (t in ColType.entries.filter { !it.isNested }) {
                assertThat(t.requiredChildCount).describedAs("%s", t.wire).isNull()
                assertThat(ColType.syntheticChildNames(t)).describedAs("%s", t.wire).isNull()
            }
        }

        @Test
        fun `schema_sql's inline CHECK lists exactly the enum, in the same order`() {
            // Same order, not just the same set: the schema-equivalence
            // gate compares pg_get_constraintdef, which preserves it.
            assertThat(checkMembers(read("schema.sql")))
                .describedAs("schema.sql col_type CHECK")
                .isEqualTo(wireNames)
        }

        @Test
        fun `the OpenAPI ColumnDef enum lists exactly the enum, in order`() {
            val spec = read("src/main/resources/openapi/hoglake.yaml")
            val columnDef = spec.substringAfter("\n    ColumnDef:")
            val listed =
                Regex("""enum:\s*\[([^\]]*)]""", RegexOption.DOT_MATCHES_ALL)
                    .find(columnDef)
                    ?.groupValues
                    ?.get(1)
                    ?: error("ColumnDef.type enum not found in the OpenAPI spec")
            assertThat(listed.split(",").map { it.trim() }).isEqualTo(wireNames)
        }

        @Test
        fun `the OpenAPI ColumnDef name schema carries the reserved prefix rule`() {
            // The spec is the contract a generated client is built
            // from. While it described the identifier regex as the
            // complete policy, a generated client happily submitted
            // `_hog_row_id` and met a 422 the spec never mentioned.
            val spec = read("src/main/resources/openapi/hoglake.yaml")
            val nameSchema = spec.substringAfter("\n    ColumnDef:").substringBefore("\n        type:\n")
            // In its SIBLING position, where a generator reading
            // `schema.pattern` finds it — the same place every other
            // identifier in the spec declares one. Nesting it inside an
            // allOf hid it from codegen entirely, which is why the
            // assertion pins the LINE, not just the string.
            val patternLine = "          pattern: \"" + Identifiers.PATTERN + "\""
            assertThat(nameSchema.lines())
                .describedAs("the base identifier pattern is a direct property of name")
                .contains(patternLine)
            assertThat(nameSchema)
                .describedAs("and not buried in a composition keyword generators ignore")
                .doesNotContain("allOf")
            assertThat(nameSchema)
                .describedAs("and the reserved prefix is machine-readable, not just prose")
                .contains("not: { pattern: \"^${Identifiers.RESERVED_COLUMN_PREFIX}\" }")
            assertThat(nameSchema)
                .describedAs("and named, so a client author can find the refusal")
                .contains(Identifiers.RESERVED_COLUMN_PREFIX)
        }

        @Test
        fun `no refused name is smuggled into the vocabulary`() {
            assertThat(wireNames).doesNotContainAnyElementsOf(ColType.REFUSALS.keys)
        }

        /** The quoted members of the first `col_type IN (...)` list in [sql]. */
        private fun checkMembers(sql: String): List<String> {
            val list = sql.substringAfter("col_type IN (").substringBefore(")")
            return Regex("'([a-z0-9_]+)'").findAll(list).map { it.groupValues[1] }.toList()
        }

        private fun read(relative: String): String = Files.readString(Path.of(relative))
    }

    // ---- the facade mapping ----------------------------------------------

    @Nested
    inner class FacadeMapping {
        @Test
        fun `every type maps to the Iceberg type iceberg-federation md section 2 records`() {
            val expected =
                mapOf(
                    ColType.BOOLEAN to IcebergType.BOOLEAN,
                    ColType.INT8 to IcebergType.INT,
                    ColType.INT16 to IcebergType.INT,
                    ColType.UINT8 to IcebergType.INT,
                    ColType.UINT16 to IcebergType.INT,
                    ColType.INT to IcebergType.INT,
                    ColType.UINT32 to IcebergType.LONG,
                    ColType.LONG to IcebergType.LONG,
                    ColType.UINT64 to IcebergType.DECIMAL,
                    ColType.FLOAT to IcebergType.FLOAT,
                    ColType.DOUBLE to IcebergType.DOUBLE,
                    ColType.DECIMAL to IcebergType.DECIMAL,
                    ColType.DATE to IcebergType.DATE,
                    ColType.TIME to IcebergType.TIME,
                    ColType.TIMESTAMP_S to IcebergType.TIMESTAMP,
                    ColType.TIMESTAMP_MS to IcebergType.TIMESTAMP,
                    ColType.TIMESTAMP to IcebergType.TIMESTAMP,
                    ColType.TIMESTAMP_NS to IcebergType.TIMESTAMP_NS,
                    ColType.TIMESTAMPTZ to IcebergType.TIMESTAMPTZ,
                    ColType.STRING to IcebergType.STRING,
                    ColType.JSON to IcebergType.STRING,
                    ColType.UUID_T to IcebergType.UUID,
                    ColType.BINARY to IcebergType.BINARY,
                    ColType.VARIANT to IcebergType.VARIANT,
                    // Native, one for one (docs/iceberg-federation.md §2.8).
                    ColType.LIST to IcebergType.LIST,
                    ColType.STRUCT to IcebergType.STRUCT,
                    ColType.MAP to IcebergType.MAP,
                )
            assertThat(expected.keys).containsExactlyInAnyOrderElementsOf(ColType.entries)
            for ((type, iceberg) in expected) {
                assertThat(type.icebergType).describedAs(type.wire).isEqualTo(iceberg)
            }
        }

        @Test
        fun `isScalar splits the Iceberg types exactly where isNested splits ours`() {
            // The two predicates must agree, because every bounds and
            // promotion decision keys on the MAPPED type: an Iceberg
            // container reachable from a scalar ColType (or the reverse)
            // would put a single-value encoding where there is none.
            for (t in ColType.entries) {
                assertThat(t.icebergType.isScalar)
                    .describedAs("%s -> %s", t.wire, t.icebergType.wire)
                    .isEqualTo(!t.isNested)
            }
        }
    }

    // ---- promotions -------------------------------------------------------

    @Nested
    inner class Promotions {
        /**
         * DuckLake's documented promotion table, transcribed from
         * https://ducklake.select/docs/stable/duckdb/usage/schema_evolution
         * ("Only type promotions are supported. Type promotions must be
         * lossless"), in hoglake wire names: DuckLake's int32/int64 are
         * our int/long, its float32/float64 our float/double.
         *
         * Stated here as a LITERAL, independently of the production
         * table, so the test can compute the intersection itself rather
         * than restate the answer. Update it only against the docs.
         */
        private val duckLake: Map<ColType, Set<ColType>> =
            mapOf(
                ColType.INT8 to setOf(ColType.INT16, ColType.INT, ColType.LONG),
                ColType.INT16 to setOf(ColType.INT, ColType.LONG),
                ColType.INT to setOf(ColType.LONG),
                ColType.UINT8 to setOf(ColType.UINT16, ColType.UINT32, ColType.UINT64),
                ColType.UINT16 to setOf(ColType.UINT32, ColType.UINT64),
                ColType.UINT32 to setOf(ColType.UINT64),
                ColType.FLOAT to setOf(ColType.DOUBLE),
            )

        /**
         * Iceberg schema-evolution legality of the induced facade change,
         * as a predicate over [icebergType] rather than a list: same
         * mapped type, or one of Iceberg's own widenings. (Decimal
         * precision widening is legal too, but no hoglake promotion
         * produces it — precision is a type_param, not a type.)
         */
        private fun icebergLegal(
            from: ColType,
            to: ColType,
        ): Boolean {
            val a = from.icebergType
            val b = to.icebergType
            return a == b ||
                (a == IcebergType.INT && b == IcebergType.LONG) ||
                (a == IcebergType.FLOAT && b == IcebergType.DOUBLE)
        }

        @Test
        fun `PROMOTIONS is exactly DuckLake's table intersected with Iceberg legality`() {
            // The whole matrix, derived from the two sources above rather
            // than copied from the implementation — so a hand-edited entry
            // (in either direction) fails instead of being restated.
            for (from in ColType.entries) {
                val expected =
                    (duckLake[from] ?: emptySet())
                        .filter { icebergLegal(from, it) }
                        .toSet()
                val actual = ColType.entries.filter { from.canPromoteTo(it) }.toSet()
                assertThat(actual)
                    .describedAs(
                        "promotions from %s: DuckLake offers %s, Iceberg legality keeps %s",
                        from.wire,
                        (duckLake[from] ?: emptySet()).map { it.wire }.sorted(),
                        expected.map { it.wire }.sorted(),
                    )
                    .isEqualTo(expected)
            }
        }

        @Test
        fun `each half of the intersection actually excludes something`() {
            // Guards the test above from passing because one of its two
            // filters is a no-op. If either list ever stops biting, the
            // "intersection" claim is decoration.
            val duckLakeRejects =
                ColType.entries.flatMap { from ->
                    ColType.entries.filter { to ->
                        from != to && icebergLegal(from, to) && to !in (duckLake[from] ?: emptySet())
                    }.map { from to it }
                }
            assertThat(duckLakeRejects)
                .describedAs("Iceberg-legal pairs DuckLake does not offer")
                .isNotEmpty()

            val icebergRejects =
                duckLake.entries.flatMap { (from, tos) ->
                    tos.filter { !icebergLegal(from, it) }.map { from to it }
                }
            assertThat(icebergRejects.map { "${it.first.wire}->${it.second.wire}" })
                .describedAs("DuckLake promotions Iceberg legality removes")
                .containsExactlyInAnyOrder("uint8->uint64", "uint16->uint64", "uint32->uint64")
        }

        @Test
        fun `the named consequences of the intersection`() {
            // Spelled out because each one surprised somebody.

            // ADDED by following DuckLake: uint8/uint16 reach uint32,
            // which is int -> long in Iceberg terms.
            assertThat(ColType.UINT8.canPromoteTo(ColType.UINT32)).isTrue()
            assertThat(ColType.UINT16.canPromoteTo(ColType.UINT32)).isTrue()

            // REMOVED by following DuckLake, despite being value-preserving:
            // DuckLake offers no unsigned -> signed rung at all, and no
            // timestamp rungs at all. Accepting these would mean a hoglake
            // catalog takes DDL a DuckLake client rejects.
            assertThat(ColType.UINT8.canPromoteTo(ColType.INT)).isFalse()
            assertThat(ColType.UINT16.canPromoteTo(ColType.LONG)).isFalse()
            assertThat(ColType.UINT32.canPromoteTo(ColType.LONG)).isFalse()
            assertThat(ColType.TIMESTAMP_S.canPromoteTo(ColType.TIMESTAMP_MS)).isFalse()
            assertThat(ColType.TIMESTAMP_MS.canPromoteTo(ColType.TIMESTAMP)).isFalse()

            // REMOVED by Iceberg legality, despite DuckLake offering them:
            // uint64 maps to decimal(20,0), and int/long -> decimal is not
            // an Iceberg evolution.
            assertThat(ColType.UINT8.canPromoteTo(ColType.UINT64)).isFalse()
            assertThat(ColType.UINT16.canPromoteTo(ColType.UINT64)).isFalse()
            assertThat(ColType.UINT32.canPromoteTo(ColType.UINT64)).isFalse()

            // Never offered by either: json/string share a mapped type but
            // not a validity claim, and timestamp_ns is its own Iceberg type.
            assertThat(ColType.JSON.canPromoteTo(ColType.STRING)).isFalse()
            assertThat(ColType.STRING.canPromoteTo(ColType.JSON)).isFalse()
            assertThat(ColType.TIMESTAMP.canPromoteTo(ColType.TIMESTAMP_NS)).isFalse()
            // uint64 is terminal.
            assertThat(ColType.entries.none { ColType.UINT64.canPromoteTo(it) }).isTrue()
        }

        @Test
        fun `promotion is irreflexive and never narrows`() {
            for (t in ColType.entries) {
                assertThat(t.canPromoteTo(t)).describedAs("%s -> itself", t.wire).isFalse()
            }
            for (a in ColType.entries) {
                for (b in ColType.entries) {
                    if (a.canPromoteTo(b)) {
                        assertThat(b.canPromoteTo(a))
                            .describedAs("%s -> %s and back", a.wire, b.wire)
                            .isFalse()
                    }
                }
            }
        }
    }

    // ---- named refusals ---------------------------------------------------

    @Nested
    inner class Refusals {
        @Test
        fun `the refused set is exactly the twelve permanently unsupported names`() {
            assertThat(ColType.REFUSALS.keys).containsExactlyInAnyOrder(
                "int128", "uint128", "timetz", "interval",
                "point", "linestring", "polygon", "multipoint",
                "multilinestring", "multipolygon", "linestring_z",
                "geometrycollection",
            )
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("com.posthog.hoglake.model.ScalarTypeParityTest#refusedNames")
        fun `a refused name fails with a reason, never the unknown-type message`(name: String) {
            assertThatThrownBy { ColType.parseWire(name) { "unknown column type '$name'" } }
                .isInstanceOf(HoglakeException.Validation::class.java)
                .satisfies({ e ->
                    val message = e.message ?: ""
                    // Names the type...
                    assertThat(message).contains("'$name'")
                    // ...says it is not supported...
                    assertThat(message).contains("is not supported")
                    // ...and gives a REASON, not just a verdict.
                    assertThat(message).containsAnyOf(
                        "decimal digits",
                        "no Iceberg mapping",
                        "geometry",
                    )
                    // Never the typo answer.
                    assertThat(message).doesNotContain("unknown column type")
                })
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("com.posthog.hoglake.model.ScalarTypeParityTest#refusedNames")
        fun `a refused name is refused in any case, like every other type name`(name: String) {
            // fromWire uppercases before valueOf, so "INT" and "Int" are
            // accepted types. A refusal lookup that only matched lowercase
            // therefore answered "unknown column type 'INT128'" — sending
            // the caller hunting for a spelling of a type that will never
            // exist, which is exactly what REFUSALS is for.
            for (spelling in listOf(name.uppercase(), name.replaceFirstChar { it.uppercase() })) {
                assertThatThrownBy { ColType.parseWire(spelling) { "unknown column type '$spelling'" } }
                    .describedAs("parseWire(%s)", spelling)
                    .isInstanceOf(HoglakeException.Validation::class.java)
                    .satisfies({ e ->
                        assertThat(e.message).doesNotContain("unknown column type")
                        assertThat(e.message).contains("is not supported")
                    })
            }
        }

        @Test
        fun `case-insensitivity is inherited from fromWire, not invented here`() {
            // The premise the test above rests on: if this ever stops
            // holding, the refusal lowercasing becomes dead weight rather
            // than a fix, and someone should notice.
            assertThat(ColType.fromWire("INT")).isEqualTo(ColType.INT)
            assertThat(ColType.fromWire("TimeStamp_Ns")).isEqualTo(ColType.TIMESTAMP_NS)
        }

        @Test
        fun `a genuinely unknown name still gets the unknown-type message`() {
            assertThatThrownBy { ColType.parseWire("int7") { "unknown column type 'int7'" } }
                .isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessage("unknown column type 'int7'")
        }

        @Test
        fun `fromWire stays strict and untyped for internal callers`() {
            // The persistence reader uses fromWire; it must keep throwing
            // IllegalArgumentException, not a wire-shaped Validation.
            assertThatThrownBy { ColType.fromWire("interval") }
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }

    companion object {
        @JvmStatic
        fun refusedNames(): List<String> = ColType.REFUSALS.keys.sorted()
    }
}
