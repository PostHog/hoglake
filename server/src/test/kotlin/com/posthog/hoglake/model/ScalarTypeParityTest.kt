package com.posthog.hoglake.model

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
 * webui list is TypeScript and is covered by its own suite.)
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
        fun `the V4 migration's CHECK lists exactly the enum, in order`() {
            val sql = read("src/main/resources/db/migration/V4__scalar_types.sql")
            assertThat(checkMembers(sql))
                .describedAs("V4__scalar_types.sql col_type CHECK")
                .isEqualTo(wireNames)
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
                )
            assertThat(expected.keys).containsExactlyInAnyOrderElementsOf(ColType.entries)
            for ((type, iceberg) in expected) {
                assertThat(type.icebergType).describedAs(type.wire).isEqualTo(iceberg)
            }
        }
    }

    // ---- promotions -------------------------------------------------------

    @Nested
    inner class Promotions {
        @Test
        fun `the allowed matrix is exactly this`() {
            val expected =
                mapOf(
                    ColType.INT8 to setOf(ColType.INT16, ColType.INT, ColType.LONG),
                    ColType.INT16 to setOf(ColType.INT, ColType.LONG),
                    ColType.INT to setOf(ColType.LONG),
                    ColType.UINT8 to setOf(ColType.UINT16, ColType.INT, ColType.LONG),
                    ColType.UINT16 to setOf(ColType.INT, ColType.LONG),
                    ColType.UINT32 to setOf(ColType.LONG),
                    ColType.FLOAT to setOf(ColType.DOUBLE),
                    ColType.TIMESTAMP_S to setOf(ColType.TIMESTAMP_MS, ColType.TIMESTAMP),
                    ColType.TIMESTAMP_MS to setOf(ColType.TIMESTAMP),
                )
            for (from in ColType.entries) {
                val allowed = ColType.entries.filter { from.canPromoteTo(it) }.toSet()
                assertThat(allowed)
                    .describedAs("promotions from %s", from.wire)
                    .isEqualTo(expected[from] ?: emptySet<ColType>())
            }
        }

        @Test
        fun `every allowed promotion is a legal Iceberg schema evolution`() {
            // THE rule. Iceberg permits: same type, int -> long,
            // float -> double, and decimal precision widening (which no
            // hoglake promotion produces, since decimal precision is a
            // type_param rather than a type). Anything else would make the
            // lake unreadable through the facade the moment it is applied.
            for (from in ColType.entries) {
                for (to in ColType.entries.filter { from.canPromoteTo(it) }) {
                    val a = from.icebergType
                    val b = to.icebergType
                    val legal =
                        a == b ||
                            (a == IcebergType.INT && b == IcebergType.LONG) ||
                            (a == IcebergType.FLOAT && b == IcebergType.DOUBLE)
                    assertThat(legal)
                        .describedAs(
                            "%s -> %s induces the Iceberg evolution %s -> %s, which is not legal",
                            from.wire,
                            to.wire,
                            a.wire,
                            b.wire,
                        )
                        .isTrue()
                }
            }
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

        @Test
        fun `the deliberate refusals stay refused`() {
            // uint32 -> uint64 would be long -> decimal(20,0) in Iceberg:
            // DuckLake allows the widening, hoglake cannot.
            assertThat(ColType.UINT32.canPromoteTo(ColType.UINT64)).isFalse()
            // timestamp -> timestamp_ns changes the mapped type AND the
            // stored bound unit under every existing stats row.
            assertThat(ColType.TIMESTAMP.canPromoteTo(ColType.TIMESTAMP_NS)).isFalse()
            assertThat(ColType.TIMESTAMP_MS.canPromoteTo(ColType.TIMESTAMP_NS)).isFalse()
            assertThat(ColType.TIMESTAMP_S.canPromoteTo(ColType.TIMESTAMP_NS)).isFalse()
            // json and string share a mapped type but not a claim.
            assertThat(ColType.JSON.canPromoteTo(ColType.STRING)).isFalse()
            assertThat(ColType.STRING.canPromoteTo(ColType.JSON)).isFalse()
            // uint64 is terminal: nothing in the vocabulary contains it.
            assertThat(ColType.entries.none { ColType.UINT64.canPromoteTo(it) }).isTrue()
            // Signed sources never reach the unsigned types (int8 holds -128).
            assertThat(ColType.INT8.canPromoteTo(ColType.UINT16)).isFalse()
            assertThat(ColType.INT.canPromoteTo(ColType.UINT64)).isFalse()
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
