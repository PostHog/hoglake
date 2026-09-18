package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.model.MAX_COLUMN_NESTING_DEPTH
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.MethodSource
import java.util.concurrent.atomic.AtomicInteger

/**
 * list, struct and map over the HTTP surface: created, read back with
 * their assigned field ids, altered, and — mostly — REFUSED by name.
 *
 * The refusals are the bulk of this file on purpose. `list`, `struct`
 * and `map` used to fall through to the generic unknown-type error, and
 * the whole hazard of making them real is that a half-specified one
 * (`{"type":"list"}` with no children, a map with three children, a
 * nullable key) reaches code that assumes the shape and 500s. Every such
 * shape is pinned here with the words a caller would need to fix it —
 * and pinned in MIXED CASE too, because the type name is
 * case-insensitive and a refusal that only fires on the lowercase
 * spelling is a refusal with a hole in it.
 *
 * Everything runs at the wire with raw JSON: the recursion crosses a
 * Jackson boundary in both directions, and a `children` array that
 * parses but does not serialize back is still a broken type.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NestedColumnApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()
    private val counter = AtomicInteger(0)

    private val catalog = "nested-types"
    private val tablesUrl = "/v1/catalogs/$catalog/namespaces/ns/tables"

    @BeforeAll
    fun seed() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "$catalog", "data_path": "s3://b/$catalog"}""")
            client.postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""")
        }

    @AfterAll
    fun tearDown() = db.close()

    // ---- the shapes that work ---------------------------------------------

    @Test
    fun `each nested shape creates, reads back, and numbers its fields depth-first`() =
        api { client ->
            val name = table()
            val created =
                client.postJson(
                    tablesUrl,
                    """{"name": "$name", "columns": [
                        {"name": "id", "type": "long"},
                        {"name": "tags", "type": "list", "children": [
                            {"name": "element", "type": "string"}]},
                        {"name": "addr", "type": "struct", "children": [
                            {"name": "city", "type": "string"},
                            {"name": "zip", "type": "int"}]},
                        {"name": "props", "type": "map", "children": [
                            {"name": "key", "type": "string", "nullable": false},
                            {"name": "value", "type": "long"}]},
                        {"name": "deep", "type": "struct", "children": [
                            {"name": "runs", "type": "list", "children": [
                                {"name": "element", "type": "struct", "children": [
                                    {"name": "score", "type": "double"}]}]}]}]}""",
                )
            assertThat(created.status).isEqualTo(HttpStatusCode.Created)

            // Read back through GET, not just the create response: the two
            // serialize from different places (the service's in-memory tree
            // vs. the assembled hog_column rows), and only the second proves
            // parent_field_id actually persisted the shape.
            val table = body(client.get("$tablesUrl/$name"))
            val ids = mutableMapOf<String, Long>()

            fun walk(
                node: JsonNode,
                prefix: String,
            ) {
                val path = if (prefix.isEmpty()) node["name"].asText() else "$prefix.${node["name"].asText()}"
                ids[path] = node["field_id"].asLong()
                node["children"]?.forEach { walk(it, path) }
            }
            table["columns"].forEach { walk(it, "") }

            // Depth-first: a parent before its children, a subtree before
            // its next sibling. That is Iceberg's own assignment order and
            // it keeps a subtree's ids contiguous.
            assertThat(ids).isEqualTo(
                mapOf(
                    "id" to 1L,
                    "tags" to 2L,
                    "tags.element" to 3L,
                    "addr" to 4L,
                    "addr.city" to 5L,
                    "addr.zip" to 6L,
                    "props" to 7L,
                    "props.key" to 8L,
                    "props.value" to 9L,
                    "deep" to 10L,
                    "deep.runs" to 11L,
                    "deep.runs.element" to 12L,
                    "deep.runs.element.score" to 13L,
                ),
            )

            // Ordinals order SIBLINGS: both structs' first field is 0.
            val addr = table["columns"].first { it["name"].asText() == "addr" }
            assertThat(addr["children"].map { it["ordinal"].asInt() }).containsExactly(0, 1)
            assertThat(addr["ordinal"].asInt()).isEqualTo(2)

            // A scalar carries no `children` key at all, so a pre-phase-2
            // client sees exactly the shape it always saw.
            val id = table["columns"].first { it["name"].asText() == "id" }
            assertThat(id.has("children")).isFalse()
        }

    @Test
    fun `the container type names are case-insensitive, like every other type name`() =
        api { client ->
            val name = table()
            val created =
                client.postJson(
                    tablesUrl,
                    """{"name": "$name", "columns": [
                        {"name": "a", "type": "LIST", "children": [
                            {"name": "element", "type": "INT"}]},
                        {"name": "b", "type": "Struct", "children": [
                            {"name": "x", "type": "Long"}]},
                        {"name": "c", "type": "MaP", "children": [
                            {"name": "key", "type": "string", "nullable": false},
                            {"name": "value", "type": "int"}]}]}""",
                )
            assertThat(created.status).isEqualTo(HttpStatusCode.Created)
            // ...and they come back in the canonical lowercase spelling.
            assertThat(body(created)["columns"].map { it["type"].asText() })
                .containsExactly("list", "struct", "map")
        }

    @Test
    fun `the depth cap is a boundary, not a guess`() =
        api { client ->
            // At the cap, accepted; one deeper, refused by name. A cap
            // tested only from the far side would pass with an off-by-one
            // that rejects legal schemas.
            val ok = client.postJson(tablesUrl, nestedTable(table(), MAX_COLUMN_NESTING_DEPTH))
            assertThat(ok.status).isEqualTo(HttpStatusCode.Created)

            val tooDeep = client.postJson(tablesUrl, nestedTable(table(), MAX_COLUMN_NESTING_DEPTH + 1))
            assertValidation(tooDeep) { detail ->
                assertThat(detail).contains("nesting depth ${MAX_COLUMN_NESTING_DEPTH + 1}")
                assertThat(detail).contains("maximum $MAX_COLUMN_NESTING_DEPTH")
            }
        }

    /** `{"name": t, "columns": [...]}` for one struct chain of [depth] levels. */
    private fun nestedTable(
        name: String,
        depth: Int,
    ): String {
        var body = """{"name": "leaf", "type": "int"}"""
        repeat(depth - 1) { body = """{"name": "s$it", "type": "struct", "children": [$body]}""" }
        return """{"name": "$name", "columns": [$body]}"""
    }

    @Test
    fun `a map key may itself be a container, and round-trips`() =
        api { client ->
            // Iceberg permits non-scalar map keys, so hoglake does too — the
            // alternative would be refusing a shape the facade would have
            // been happy to serve. Pinned because "we accept it" was true
            // before any test said so, which is how a shape stops being
            // accepted by accident.
            val name = table()
            val response =
                client.postJson(
                    tablesUrl,
                    """{"name": "$name", "columns": [
                    {"name": "mk", "type": "map", "children": [
                        {"name": "key", "type": "struct", "nullable": false, "children": [
                            {"name": "x", "type": "long"}]},
                        {"name": "value", "type": "string"}]},
                    {"name": "ml", "type": "map", "children": [
                        {"name": "key", "type": "list", "nullable": false, "children": [
                            {"name": "element", "type": "int"}]},
                        {"name": "value", "type": "long"}]}]}""",
                )
            assertThat(response.status)
                .describedAs("body was: %s", response.bodyAsText())
                .isEqualTo(HttpStatusCode.Created)

            val table = body(client.get("$tablesUrl/$name"))
            val mk = table["columns"].first { it["name"].asText() == "mk" }
            assertThat(mk["children"].map { it["name"].asText() }).containsExactly("key", "value")
            assertThat(mk["children"][0]["type"].asText()).isEqualTo("struct")
            assertThat(mk["children"][0]["children"].map { it["name"].asText() }).containsExactly("x")
            // Depth-first ids hold through a container key: 1 mk, 2 key,
            // 3 key.x, 4 value, 5 ml, 6 key, 7 element, 8 value.
            assertThat(mk["children"][0]["children"][0]["field_id"].asLong()).isEqualTo(3L)
            val ml = table["columns"].first { it["name"].asText() == "ml" }
            assertThat(ml["children"][0]["type"].asText()).isEqualTo("list")
            assertThat(ml["children"][0]["children"][0]["field_id"].asLong()).isEqualTo(7L)

            // A container key is still REQUIRED, and a nullable one is still
            // the named refusal — the rule is about the key slot, not about
            // the key's type.
            val nullableKey =
                client.postJson(
                    tablesUrl,
                    """{"name": "${table()}", "columns": [
                    {"name": "m", "type": "map", "children": [
                        {"name": "key", "type": "struct", "children": [
                            {"name": "x", "type": "long"}]},
                        {"name": "value", "type": "int"}]}]}""",
                )
            assertValidation(nullableKey) { assertThat(it).contains("must be required (nullable=false)") }
        }

    @Test
    fun `the depth cap is measured from the GRAFT POINT on alter, not from zero`() =
        api { client ->
            // Create-time depth is covered above; ALTER is where the cap is
            // easy to get wrong, because the subtree being added is shallow
            // and only its POSITION makes it illegal. A cap that measured
            // the added subtree alone would wave a 4-deep graft onto a
            // 5-deep struct straight past.
            val name = table()
            // s4 > s3 > s2 > s1 > s0 > leaf: six levels.
            var chain = """{"name": "leaf", "type": "int"}"""
            repeat(5) { chain = """{"name": "s$it", "type": "struct", "children": [$chain]}""" }
            assertThat(client.postJson(tablesUrl, """{"name": "$name", "columns": [$chain]}""").status)
                .isEqualTo(HttpStatusCode.Created)
            val alterUrl = "$tablesUrl/$name/alter"
            val parent = "s4.s3.s2.s1.s0" // depth 5

            // 5 + 3 = 8, exactly the cap.
            val ok =
                client.postJson(
                    alterUrl,
                    """{"ops": [{"op": "add_column", "parent": "$parent",
                    "column": {"name": "g3", "type": "struct", "children": [
                        {"name": "g2", "type": "struct", "children": [
                            {"name": "g1", "type": "int"}]}]}}]}""",
                )
            assertThat(ok.status).describedAs("body was: %s", ok.bodyAsText()).isEqualTo(HttpStatusCode.OK)

            // 5 + 4 = 9, one past it.
            val tooDeep =
                client.postJson(
                    alterUrl,
                    """{"ops": [{"op": "add_column", "parent": "$parent",
                    "column": {"name": "h4", "type": "struct", "children": [
                        {"name": "h3", "type": "struct", "children": [
                            {"name": "h2", "type": "struct", "children": [
                                {"name": "h1", "type": "int"}]}]}]}}]}""",
                )
            assertValidation(tooDeep) { detail ->
                assertThat(detail).contains("nesting depth ${MAX_COLUMN_NESTING_DEPTH + 1}")
                assertThat(detail).contains("maximum $MAX_COLUMN_NESTING_DEPTH")
            }
            // Refused BEFORE any id was allocated: the legal graft above is
            // still the last thing in the schema.
            val after = body(client.get("$tablesUrl/$name"))
            assertThat(after.toString()).doesNotContain("h4")
        }

    // ---- the named refusals -----------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("com.posthog.hoglake.api.NestedColumnApiTest#malformedShapes")
    fun `a malformed container is a named 422, never a 500`(
        label: String,
        columns: String,
        fragment: String,
    ) = api { client ->
        val response = client.postJson(tablesUrl, """{"name": "${table()}", "columns": [$columns]}""")
        assertValidation(response) { detail ->
            assertThat(detail).describedAs(label).contains(fragment)
            // Never the typo answer: the caller's type name was fine, its
            // SHAPE was not, and sending them off to check the spelling
            // is the one unhelpful thing this could do.
            assertThat(detail).doesNotContain("unknown column type")
        }
    }

    @ParameterizedTest(name = "{0} in mixed case")
    @MethodSource("com.posthog.hoglake.api.NestedColumnApiTest#malformedShapes")
    fun `a malformed container is refused in any case spelling`(
        label: String,
        columns: String,
        fragment: String,
    ) = api { client ->
        // The refusals key off ColType, which is case-insensitive. A guard
        // written against the lowercase literal would let "LIST" with no
        // children straight through to whatever assumes the shape.
        val upper =
            columns
                .replace("\"list\"", "\"LIST\"")
                .replace("\"struct\"", "\"Struct\"")
                .replace("\"map\"", "\"MAP\"")
        val response = client.postJson(tablesUrl, """{"name": "${table()}", "columns": [$upper]}""")
        assertValidation(response) { detail ->
            assertThat(detail).describedAs(label).contains(fragment)
            assertThat(detail).doesNotContain("unknown column type")
        }
    }

    @Test
    fun `a duplicate name among a struct's fields is refused, naming the struct`() =
        api { client ->
            val response =
                client.postJson(
                    tablesUrl,
                    """{"name": "${table()}", "columns": [
                        {"name": "s", "type": "struct", "children": [
                            {"name": "a", "type": "int"},
                            {"name": "a", "type": "long"}]}]}""",
                )
            assertValidation(response) { detail ->
                assertThat(detail).contains("duplicate column names")
                assertThat(detail).contains("'s'")
            }
        }

    @Test
    fun `two structs may hold the same field name, because names are per parent`() =
        api { client ->
            // The control for the test above: duplicate detection is
            // per-sibling-group, not table-wide, and a global check would
            // make the commonest nested schema in the world illegal.
            val response =
                client.postJson(
                    tablesUrl,
                    """{"name": "${table()}", "columns": [
                        {"name": "a", "type": "struct", "children": [{"name": "id", "type": "long"}]},
                        {"name": "b", "type": "struct", "children": [{"name": "id", "type": "long"}]}]}""",
                )
            assertThat(response.status).isEqualTo(HttpStatusCode.Created)
        }

    // ---- alter ------------------------------------------------------------

    @Test
    fun `the struct-field alter matrix works by dotted path`() =
        api { client ->
            val name = table()
            client.postJson(
                tablesUrl,
                """{"name": "$name", "columns": [
                    {"name": "id", "type": "long"},
                    {"name": "addr", "type": "struct", "children": [
                        {"name": "city", "type": "string"},
                        {"name": "zip", "type": "int"}]}]}""",
            )
            val alterUrl = "$tablesUrl/$name/alter"

            // ADD a field to an existing struct: new field id, appended ordinal.
            val added =
                client.postJson(
                    alterUrl,
                    """{"ops": [{"op": "add_column", "parent": "addr",
                        "column": {"name": "country", "type": "string"}}]}""",
                )
            assertThat(added.status).isEqualTo(HttpStatusCode.OK)
            var addr = body(added)["columns"].first { it["name"].asText() == "addr" }
            assertThat(addr["children"].map { it["name"].asText() })
                .containsExactly("city", "zip", "country")
            assertThat(addr["children"].last()["ordinal"].asInt()).isEqualTo(2)
            assertThat(addr["children"].last()["field_id"].asLong()).isEqualTo(5L)

            // RENAME a struct field.
            val renamed =
                client.postJson(
                    alterUrl,
                    """{"ops": [{"op": "rename_column", "from": "addr.zip", "to": "postcode"}]}""",
                )
            assertThat(renamed.status).isEqualTo(HttpStatusCode.OK)

            // PROMOTE a struct LEAF by the ordinary scalar matrix — it is
            // field-id-keyed, so living inside a struct changes nothing.
            val promoted =
                client.postJson(
                    alterUrl,
                    """{"ops": [{"op": "promote_column", "name": "addr.postcode", "to": "long"}]}""",
                )
            assertThat(promoted.status).isEqualTo(HttpStatusCode.OK)

            // DROP a struct field.
            val dropped =
                client.postJson(
                    alterUrl,
                    """{"ops": [{"op": "drop_column", "name": "addr.country"}]}""",
                )
            assertThat(dropped.status).isEqualTo(HttpStatusCode.OK)

            addr = body(client.get("$tablesUrl/$name"))["columns"].first { it["name"].asText() == "addr" }
            assertThat(addr["children"].map { "${it["name"].asText()}:${it["type"].asText()}" })
                .containsExactly("city:string", "postcode:long")
            // The renamed/promoted field KEPT the id it was created with
            // (addr=2, city=3, zip=4): that is what makes the change a
            // schema evolution rather than a new column.
            assertThat(addr["children"].last()["field_id"].asLong()).isEqualTo(4L)
        }

    @Test
    fun `dropping a struct takes its whole subtree with it`() =
        api { client ->
            val name = table()
            client.postJson(
                tablesUrl,
                """{"name": "$name", "columns": [
                    {"name": "id", "type": "long"},
                    {"name": "s", "type": "struct", "children": [
                        {"name": "inner", "type": "struct", "children": [
                            {"name": "x", "type": "int"}]}]}]}""",
            )
            val dropped =
                client.postJson("$tablesUrl/$name/alter", """{"ops": [{"op": "drop_column", "name": "s"}]}""")
            assertThat(dropped.status).isEqualTo(HttpStatusCode.OK)
            val table = body(client.get("$tablesUrl/$name"))
            assertThat(table["columns"].map { it["name"].asText() }).containsExactly("id")
            // The descendants are gone from the catalog, not orphaned into
            // top-level columns — which is what an assemble() that re-rooted
            // unknown parents would have produced.
            assertThat(
                db.jdbi.withHandle<Long, Exception> { h ->
                    h.createQuery(
                        """
                        SELECT count(*) FROM hog_column c
                        JOIN hog_table_version tv
                          ON tv.catalog_id = c.catalog_id AND tv.table_id = c.table_id
                        WHERE c.end_snapshot IS NULL AND tv.end_snapshot IS NULL
                          AND tv.name = :table AND c.name IN ('s', 'inner', 'x')
                        """,
                    ).bind("table", name).mapTo(Long::class.java).one()
                },
            ).describedAs("every row of the dropped subtree is retired").isZero()
        }

    @ParameterizedTest(name = "{0}")
    @CsvSource(
        delimiter = '|',
        value = [
            // Adding into a list or a map: there is nothing to add.
            """add into a list|{"op":"add_column","parent":"tags","column":{"name":"x","type":"int"}}|internals""",
            """add into a map|{"op":"add_column","parent":"props","column":{"name":"x","type":"int"}}|internals""",
            // The synthetic children are not addressable at all.
            """drop a list element|{"op": "drop_column", "name": "tags.element"}|list/map internals""",
            """drop a map key|{"op": "drop_column", "name": "props.key"}|list/map internals""",
            """rename a map value|{"op": "rename_column", "from": "props.value", "to": "v"}|list/map internals""",
            """promote a list element|{"op":"promote_column","name":"tags.element","to":"long"}|list/map internals""",
            // A scalar is not a path prefix.
            """address through a scalar|{"op": "drop_column", "name": "id.nope"}|not a struct""",
            // Containers are not promotable in either direction.
            """promote a struct|{"op": "promote_column", "name": "addr", "to": "long"}|not promotable""",
            """promote into a struct|{"op": "promote_column", "name": "id", "to": "struct"}|not promotable""",
        ],
    )
    fun `list and map internals are refused by name`(
        label: String,
        op: String,
        fragment: String,
    ) = api { client ->
        val response = client.postJson("$tablesUrl/${nestedFixture(client)}/alter", """{"ops": [$op]}""")
        assertValidation(response) { detail ->
            assertThat(detail).describedAs(label).contains(fragment)
        }
    }

    @Test
    fun `an ambiguous column path is refused rather than resolved to the first match`() =
        api { client ->
            // The DDL path refuses duplicate sibling names, but the
            // DATABASE only enforces unique (parent, ordinal) — not
            // (parent, name). A catalog that acquired two siblings called
            // the same thing (a hand-edited row, a restored dump) would
            // otherwise have alter ops act on whichever came first,
            // silently, forever.
            val name = table()
            client.postJson(
                tablesUrl,
                """{"name": "$name", "columns": [
                    {"name": "id", "type": "long"},
                    {"name": "s", "type": "struct", "children": [
                        {"name": "a", "type": "int"},
                        {"name": "b", "type": "int"}]}]}""",
            )
            // Forge the inconsistency the DDL cannot produce.
            db.jdbi.useHandle<Exception> { h ->
                h.createUpdate(
                    """
                    UPDATE hog_column SET name = 'a'
                    WHERE end_snapshot IS NULL AND name = 'b'
                      AND table_id = (
                        SELECT table_id FROM hog_table_version
                        WHERE name = :t AND end_snapshot IS NULL)
                    """,
                ).bind("t", name).execute()
            }
            val response =
                client.postJson("$tablesUrl/$name/alter", """{"ops": [{"op": "drop_column", "name": "s.a"}]}""")
            assertValidation(response) { detail ->
                assertThat(detail).contains("ambiguous")
                assertThat(detail).contains("2 live columns are named 'a'")
            }
        }

    @Test
    fun `dropping the last field of a struct is refused`() =
        api { client ->
            val name = table()
            client.postJson(
                tablesUrl,
                """{"name": "$name", "columns": [
                    {"name": "id", "type": "long"},
                    {"name": "s", "type": "struct", "children": [{"name": "only", "type": "int"}]}]}""",
            )
            val response =
                client.postJson("$tablesUrl/$name/alter", """{"ops": [{"op": "drop_column", "name": "s.only"}]}""")
            assertValidation(response) { detail ->
                assertThat(detail).contains("last field of struct 's'")
            }
        }

    @ParameterizedTest(name = "{0}")
    @CsvSource(
        delimiter = '|',
        value = [
            """drop_column|{"op":"drop_column","parent":"addr","name":"zip"}""",
            """rename_column|{"op":"rename_column","parent":"addr","from":"addr.zip","to":"pc"}""",
            """promote_column|{"op":"promote_column","parent":"addr","name":"addr.zip","to":"long"}""",
            """rename_table|{"op":"rename_table","parent":"addr","new_name":"t9"}""",
        ],
    )
    fun `parent supplied to an op that does not take it is refused, naming the op`(
        op: String,
        body: String,
    ) = api { client ->
        // Silently dropping it reads as "supported, and it did nothing".
        // A caller who writes `{"op":"drop_column","parent":"addr",
        // "name":"zip"}` means `addr.zip` and would otherwise get a 200
        // and the WRONG column dropped — the top-level `zip`, if one
        // exists, or a 422 about a column that does exist nested.
        val response = client.postJson("$tablesUrl/${nestedFixture(client)}/alter", """{"ops": [$body]}""")
        assertValidation(response) { detail ->
            assertThat(detail).contains("'$op' does not take 'parent'")
            assertThat(detail).contains("dotted path")
        }
    }

    @Test
    fun `add_column still takes parent, which is the control`() =
        api { client ->
            val name = nestedFixture(client)
            val response =
                client.postJson(
                    "$tablesUrl/$name/alter",
                    """{"ops": [{"op": "add_column", "parent": "addr",
                        "column": {"name": "country", "type": "string"}}]}""",
                )
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
        }

    // ---- partition and sort sources ---------------------------------------

    @Test
    fun `a struct leaf is a legal partition and sort source`() =
        api { client ->
            val name = nestedFixture(client)
            val response =
                client.postJson(
                    "$tablesUrl/$name/alter",
                    """{"ops": [
                        {"op": "set_partition_spec", "fields": [
                            {"source_field_id": 5, "transform": "identity"}]},
                        {"op": "set_sort_order", "sort_fields": [
                            {"source_field_id": 5, "direction": "asc", "null_order": "nulls_last"}]}]}""",
                )
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
        }

    @Test
    fun `a struct leaf of a bucketable scalar type is still bucketable`() =
        api { client ->
            // The phase-1 allowlist is untouched by nesting: bucketability
            // is a property of the LEAF's type, and `addr.city` is a string.
            val name = nestedFixture(client)
            val response =
                client.postJson(
                    "$tablesUrl/$name/alter",
                    """{"ops": [{"op": "set_partition_spec", "fields": [
                        {"source_field_id": 5, "transform": "bucket", "transform_param": 16}]}]}""",
                )
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
        }

    @ParameterizedTest(name = "{0}")
    @CsvSource(
        delimiter = '|',
        value = [
            "the struct itself|4|has no single value per row",
            "the list itself|2|has no single value per row",
            "a list element|3|a row has many such values",
            "a map key|8|a row has many such values",
        ],
    )
    fun `containers and repeated fields are refused as partition and sort sources`(
        label: String,
        fieldId: Int,
        fragment: String,
    ) = api { client ->
        val name = nestedFixture(client)
        val partition =
            client.postJson(
                "$tablesUrl/$name/alter",
                """{"ops": [{"op": "set_partition_spec", "fields": [
                    {"source_field_id": $fieldId, "transform": "identity"}]}]}""",
            )
        assertValidation(partition) { assertThat(it).describedAs(label).contains(fragment) }

        val sort =
            client.postJson(
                "$tablesUrl/$name/alter",
                """{"ops": [{"op": "set_sort_order", "sort_fields": [
                    {"source_field_id": $fieldId, "direction": "asc", "null_order": "nulls_last"}]}]}""",
            )
        assertValidation(sort) { assertThat(it).describedAs(label).contains(fragment) }
    }

    // ---- commit -----------------------------------------------------------

    @Test
    fun `column_stats addressed to a container field id are refused, naming the type`() =
        api { client ->
            // The server never sees the parquet file, so this is the only
            // place it can catch a client that thinks a list has bounds.
            val name = nestedFixture(client)
            val response =
                client.postJson(
                    "/v1/catalogs/$catalog/commit",
                    """{"appends": [{"namespace": "ns", "table": "$name", "files": [
                        {"path": "s3://b/$catalog/$name/f.parquet", "record_count": 1,
                         "file_size_bytes": 10, "column_stats": [
                            {"field_id": 2, "value_count": 1, "null_count": 0}]}]}]}""",
                )
            assertValidation(response) { detail ->
                assertThat(detail).contains("'list'")
                assertThat(detail).contains("per LEAF field")
            }
        }

    @Test
    fun `column_stats for a LEAF under a container are accepted`() =
        api { client ->
            // The control: the refusal above is about containers, not
            // about nested tables having become unstat-able.
            val name = nestedFixture(client)
            val response =
                client.postJson(
                    "/v1/catalogs/$catalog/commit",
                    """{"appends": [{"namespace": "ns", "table": "$name", "files": [
                        {"path": "s3://b/$catalog/$name/ok.parquet", "record_count": 1,
                         "file_size_bytes": 10, "column_stats": [
                            {"field_id": 3, "value_count": 1, "null_count": 0},
                            {"field_id": 5, "value_count": 1, "null_count": 0},
                            {"field_id": 8, "value_count": 1, "null_count": 0}]}]}]}""",
                )
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
        }

    // ---- harness ----------------------------------------------------------

    /**
     * A table with one of every nested shape, created once and reused:
     * field ids 1=id, 2=tags(list), 3=tags.element, 4=addr(struct),
     * 5=addr.city, 6=addr.zip, 7=props(map), 8=props.key, 9=props.value.
     */
    private suspend fun nestedFixture(client: HttpClient): String {
        val name = table()
        client.postJson(
            tablesUrl,
            """{"name": "$name", "columns": [
                {"name": "id", "type": "long"},
                {"name": "tags", "type": "list", "children": [
                    {"name": "element", "type": "int"}]},
                {"name": "addr", "type": "struct", "children": [
                    {"name": "city", "type": "string"},
                    {"name": "zip", "type": "int"}]},
                {"name": "props", "type": "map", "children": [
                    {"name": "key", "type": "string", "nullable": false},
                    {"name": "value", "type": "long"}]}]}""",
        )
        return name
    }

    private fun table(): String = "t${counter.incrementAndGet()}"

    private suspend fun assertValidation(
        response: HttpResponse,
        check: (String) -> Unit,
    ) {
        assertThat(response.status)
            .describedAs("body was: %s", response.bodyAsText())
            .isEqualTo(HttpStatusCode.UnprocessableEntity)
        val node = body(response)
        assertThat(node["error"].asText()).isEqualTo("validation")
        check(node["detail"].asText())
    }

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application { app.module(this) }
            block(client)
        }

    private suspend fun HttpClient.postJson(
        url: String,
        body: String,
    ): HttpResponse =
        post(url) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun body(response: HttpResponse): JsonNode = json.readTree(response.bodyAsText())

    companion object {
        /**
         * Every way a container can be malformed, with the words the
         * refusal must carry. The fragment is what a caller acts on: "got
         * 0" tells them to add a child, "must be named 'element'" tells
         * them which, "non-nullable" tells them why their key was
         * rejected.
         */
        @JvmStatic
        fun malformedShapes(): List<org.junit.jupiter.params.provider.Arguments> =
            listOf(
                args(
                    "a bare list",
                    """{"name": "c", "type": "list"}""",
                    "requires exactly one child (its 'element'); got 0",
                ),
                args(
                    "a list with two children",
                    """{"name": "c", "type": "list", "children": [
                        {"name": "element", "type": "int"}, {"name": "other", "type": "int"}]}""",
                    "requires exactly one child (its 'element'); got 2",
                ),
                args(
                    "a list whose child is misnamed",
                    """{"name": "c", "type": "list", "children": [{"name": "item", "type": "int"}]}""",
                    "must be named 'element'",
                ),
                args(
                    "a bare map",
                    """{"name": "c", "type": "map"}""",
                    "requires exactly two children ('key' then 'value'); got 0",
                ),
                args(
                    "a map with one child",
                    """{"name": "c", "type": "map", "children": [
                        {"name": "key", "type": "string", "nullable": false}]}""",
                    "requires exactly two children ('key' then 'value'); got 1",
                ),
                args(
                    "a map with three children",
                    """{"name": "c", "type": "map", "children": [
                        {"name": "key", "type": "string", "nullable": false},
                        {"name": "value", "type": "int"},
                        {"name": "extra", "type": "int"}]}""",
                    "requires exactly two children ('key' then 'value'); got 3",
                ),
                args(
                    "a map with a nullable key",
                    """{"name": "c", "type": "map", "children": [
                        {"name": "key", "type": "string"},
                        {"name": "value", "type": "int"}]}""",
                    "must be required (nullable=false)",
                ),
                args(
                    "a map whose children are in the wrong order",
                    """{"name": "c", "type": "map", "children": [
                        {"name": "value", "type": "int"},
                        {"name": "key", "type": "string", "nullable": false}]}""",
                    "must be named 'key'",
                ),
                args(
                    "a bare struct",
                    """{"name": "c", "type": "struct"}""",
                    "requires at least one child field",
                ),
                args(
                    "a struct with an empty child list",
                    """{"name": "c", "type": "struct", "children": []}""",
                    "requires at least one child field",
                ),
                args(
                    "a scalar with children",
                    """{"name": "c", "type": "int", "children": [{"name": "x", "type": "int"}]}""",
                    "a scalar type, and cannot have children",
                ),
                args(
                    // PRESENT, not non-empty. An explicit empty list was
                    // normalised to "omitted" and answered 201 — to a
                    // request the error message above promises a 422 for.
                    // Same mistake, same message.
                    "a scalar with an explicitly EMPTY children list",
                    """{"name": "c", "type": "int", "children": []}""",
                    "a scalar type, and cannot have children",
                ),
                args(
                    "a scalar with an empty children list nested inside a struct",
                    """{"name": "c", "type": "struct", "children": [
                        {"name": "inner", "type": "long", "children": []}]}""",
                    "column 'c.inner' is 'long', a scalar type, and cannot have children",
                ),
                args(
                    "a container carrying type_params",
                    """{"name": "c", "type": "list", "type_params": {"precision": 5, "scale": 2},
                        "children": [{"name": "element", "type": "int"}]}""",
                    "a nested container, and cannot have type_params",
                ),
                args(
                    // PRESENT, not non-empty — the same criterion the
                    // children check uses. `{}` was normalised to
                    // "omitted" and answered 201.
                    "a container carrying an EMPTY type_params",
                    """{"name": "c", "type": "list", "type_params": {},
                        "children": [{"name": "element", "type": "int"}]}""",
                    "a nested container, and cannot have type_params",
                ),
                args(
                    "a struct carrying type_params",
                    """{"name": "c", "type": "struct", "type_params": {"precision": 5},
                        "children": [{"name": "a", "type": "int"}]}""",
                    "a nested container, and cannot have type_params",
                ),
                args(
                    "a malformed container nested inside a good one",
                    """{"name": "c", "type": "struct", "children": [
                        {"name": "inner", "type": "list"}]}""",
                    "list column 'c.inner' requires exactly one child",
                ),
            )

        private fun args(
            label: String,
            columns: String,
            fragment: String,
        ) = org.junit.jupiter.params.provider.Arguments.of(label, columns, fragment)
    }
}
