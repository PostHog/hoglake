package com.posthog.hoglake.api

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.primaryConstructor

/**
 * `ApiError`'s fields and the typed commit 409s, against the spec and
 * against the ROUTES — both sides derived, neither restated.
 *
 * Two earlier shapes of this test were not gates. Round 2's version
 * hardcoded the six commit paths, so a seventh route would have passed
 * exactly the way `/commit/transaction` did when it was missed the first
 * time; and its field assertions constructed the exceptions and read
 * their own arguments back, which is the shape AGENT.md calls out — "one
 * that reconstructs its own expectation cannot fail at all" — and is why
 * the `ApiError.tables` prose could be wrong with a green suite.
 *
 * So: the path list comes from `Routes.kt` (every `post("/commit…")`),
 * the field list comes from the YAML, and the DTO is compared to the
 * spec through a REAL serialization. The only literals are the two error
 * codes and the `re-prepare` token, which are the contract itself.
 *
 * Unit test on purpose: it reads files and serializes objects, so it
 * runs under `-PunitOnly` where a Docker-less machine still catches the
 * drift.
 */
class CommitConflictSpecParityTest {
    private val spec: String = File("src/main/resources/openapi/hoglake.yaml").readText()
    private val routes: String = File("src/main/kotlin/com/posthog/hoglake/api/Routes.kt").readText()
    private val mapper = wireObjectMapper()

    /**
     * Every commit endpoint, read off the routing tree rather than
     * listed: `post("/commit…")` inside the catalog route is exactly the
     * set of handlers that call `commits.commit`, i.e. the set that can
     * reach `checkConflicts`. A seventh route fails this test the day it
     * is added, which is the whole point.
     */
    private fun commitPaths(): List<String> {
        val suffixes =
            Regex("""post\("(/commit[^"]*)"\)""").findAll(routes).map { it.groupValues[1] }.toList()
        assertThat(suffixes)
            .describedAs("Routes.kt must still declare its commit endpoints as post(\"/commit…\")")
            .isNotEmpty()
        return suffixes.map { "/catalogs/{catalog}$it" }
    }

    /** One schema block, up to the next sibling schema. */
    private fun schemaBlock(name: String): String {
        val header = "    $name:\n"
        val start = spec.indexOf(header)
        assertThat(start).describedAs("spec block %s", name).isNotEqualTo(-1)
        val rest = spec.substring(start + header.length)
        val end = Regex("\n    [A-Za-z]").find(rest)?.range?.first ?: rest.length
        return rest.substring(0, end)
    }

    /** One path item, up to the next sibling path. */
    private fun pathBlock(path: String): String {
        val header = "\n  $path:\n"
        val start = spec.indexOf(header)
        assertThat(start).describedAs("spec path %s", path).isNotEqualTo(-1)
        val rest = spec.substring(start + header.length)
        val end = Regex("\n  /").find(rest)?.range?.first ?: rest.length
        return rest.substring(0, end)
    }

    private fun specProperties(name: String): List<String> {
        val body = schemaBlock(name)
        val start = body.indexOf("      properties:\n")
        assertThat(start).describedAs("%s has a properties block", name).isNotEqualTo(-1)
        return body.substring(start)
            .lines()
            .drop(1)
            .takeWhile { it.isBlank() || it.startsWith("        ") }
            .mapNotNull { Regex("^        ([a-z_]+):").find(it)?.groupValues?.get(1) }
    }

    /** The prose of one property of one schema, up to the next property. */
    private fun propertyProse(
        schema: String,
        property: String,
    ): String {
        val body = schemaBlock(schema)
        val start = body.indexOf("        $property:")
        assertThat(start).describedAs("%s.%s", schema, property).isNotEqualTo(-1)
        val rest = body.substring(start)
        val end = Regex("\n        [a-z_]+:").find(rest)?.range?.first ?: rest.length
        return rest.substring(0, end)
    }

    private fun keysOf(dto: Any): List<String> =
        mapper.readTree(mapper.writeValueAsString(dto)).fieldNames().asSequence().toList()

    /**
     * The body the mapping REALLY produces, from the mapping itself.
     *
     * `errorBody` is production code (`ErrorMapping.kt`), so dropping a
     * field from one of its arms reds here. An earlier version of this
     * helper rebuilt the DTOs by hand, which made the test's own claim —
     * "read off the MAPPING, not off a restatement" — false, and left the
     * defect it was written to catch invisible from the other side.
     */
    private fun bodyFor(cause: HoglakeException): Map<String, Any?> {
        val (status, dto) = errorBody(cause)
        assertThat(status.value).describedAs("a typed refusal is a 409").isEqualTo(409)
        @Suppress("UNCHECKED_CAST")
        return mapper.convertValue(dto, Map::class.java) as Map<String, Any?>
    }

    @Test
    fun `every ApiErrorDto property is a declared spec property, and vice versa`() {
        val full =
            ApiErrorDto(
                error = "ddl_since_read_snapshot",
                detail = "why",
                tables = listOf("ns.t"),
                readSnapshot = 7,
                retry = "re-prepare",
            )
        assertThat(keysOf(full))
            .describedAs("the wire keys of a fully-populated ApiError must be the spec's properties")
            .containsExactlyInAnyOrderElementsOf(specProperties("ApiError"))
    }

    @Test
    fun `an ordinary error body is still exactly error and detail`() {
        // NON_NULL omission is what keeps every existing client — and
        // every other error in the taxonomy — byte-identical.
        assertThat(keysOf(ApiErrorDto("validation", "bad stats")))
            .containsExactly("error", "detail")
        assertThat(mapper.writeValueAsString(ApiErrorDto("not_found"))).isEqualTo("""{"error":"not_found"}""")
    }

    @Test
    fun `the retry enum is exactly the token the server sends`() {
        // The value is a wire token clients branch on; the spec's enum
        // and the constant ErrorMapping interpolates live in different
        // files and nothing else pins them together.
        assertThat(schemaBlock("ApiError")).contains("enum: [re-prepare]")
    }

    @Test
    fun `each ApiError field's prose names exactly the codes that really carry it`() {
        // Read off the MAPPING, not off a restatement: this is what
        // caught `tables` being documented as ddl-only while
        // table_recreated carries it too. A field's prose has to mention
        // every code whose body contains it, and no code whose does not.
        // Keyed by the CODE the mapping chose, not by a literal, so a
        // renamed code cannot leave this test asserting about a code
        // nothing produces.
        val bodies =
            listOf(
                bodyFor(HoglakeException.DdlSinceReadSnapshot("d", listOf("ns.t"), 7)),
                bodyFor(
                    HoglakeException.TableRecreated(
                        "d",
                        "ns.t",
                        java.util.UUID.randomUUID(),
                        java.util.UUID.randomUUID(),
                    ),
                ),
            ).associateBy { it["error"] as String }
        assertThat(bodies.keys)
            .containsExactlyInAnyOrder("ddl_since_read_snapshot", "table_recreated")
        // Only the PRESENCE-SCOPED fields — the ones whose description
        // opens "Present on …" — make a claim about which codes carry
        // them. `retry` is documented by meaning rather than by code, on
        // purpose, so it has no claim to check; the assertion below makes
        // sure a field cannot dodge the rule by quietly dropping the
        // word.
        val scoped = mutableListOf<String>()
        for (field in specProperties("ApiError") - setOf("error", "detail")) {
            val description = propertyProse("ApiError", field).substringAfter("description: >")
            if (!description.trim().startsWith("Present")) continue
            scoped += field
            // The claim is the first sentence, up to its colon; prose
            // after it is free, so a "(Not on X)" clarification may name
            // a code it is denying.
            val claim = description.substringBefore(":", "")
            val claimed = bodies.keys.filter { it in claim }.toSet()
            val carrying = bodies.filterValues { it[field] != null }.keys
            assertThat(claimed)
                .describedAs("ApiError.%s's prose must name exactly the codes whose body carries it", field)
                .isEqualTo(carrying)
        }
        assertThat(scoped)
            .describedAs("the presence-scoped ApiError fields must still document their scope")
            .contains("tables", "read_snapshot")
    }

    @Test
    fun `every HoglakeException subtype maps to its own wire code`() {
        // The cost of making CommitConflict `open`: the `when` in
        // errorBody is exhaustive over the sealed hierarchy WITHOUT an arm
        // for a subclass of one of its members, so a new
        // `class Foo : CommitConflict` compiles clean and silently answers
        // `commit_conflict`. Kotlin cannot catch that; this can.
        //
        // REFLECTION ALONE IS NOT TOTAL, and the companion test below is
        // why this one can still claim to be a gate. `sealedSubclasses`
        // gives only DIRECT permitted members, and `CommitConflict` is
        // `open` rather than sealed, so nothing can enumerate ITS
        // subclasses; `nestedClasses` catches the ones declared inside
        // HoglakeException, which is every one today, but a top-level
        // `class X : HoglakeException.CommitConflict` in this module would
        // compile and be invisible to both. So: walk both (union,
        // recursively through the sealed members) for the distinctness
        // check, and count the source DECLARATIONS separately to prove the
        // walk saw all of them.
        val types = exceptionTypes()
        assertThat(types).describedAs("the reflective walk must find the exception types").isNotEmpty
        val codeByType =
            types.associate { type ->
                val cause = instantiate(type)
                type.simpleName to (errorBody(cause).second.error)
            }
        assertThat(codeByType.values.toSet().size)
            .describedAs(
                "each exception type needs its OWN arm in errorBody; a duplicate code means a " +
                    "subtype fell through to a base arm. Got %s",
                codeByType,
            )
            .isEqualTo(codeByType.size)
    }

    /**
     * Every `HoglakeException` subtype reflection can reach: the sealed
     * members (recursively, for a member that is itself sealed) union the
     * classes nested inside `HoglakeException`.
     */
    private fun exceptionTypes(): Set<KClass<*>> {
        val found = mutableSetOf<KClass<*>>()

        fun walk(type: KClass<*>) {
            for (member in type.sealedSubclasses) {
                if (found.add(member)) walk(member)
            }
        }
        walk(HoglakeException::class)
        found += HoglakeException::class.nestedClasses.filter { it.isSubclassOf(HoglakeException::class) }
        return found
    }

    @Test
    fun `no exception subtype escapes the reflective walk`() {
        // What makes the distinctness check a real gate: every declaration
        // in the module that extends the hierarchy has to be one the walk
        // found. A top-level `class X : HoglakeException.CommitConflict(…)`
        // is the case reflection cannot see; here it is a declaration with
        // no matching type, and the counts diverge.
        val known = exceptionTypes().mapNotNull { it.simpleName }.toSet()
        val pattern = Regex(":\\s*(?:HoglakeException\\.)?(\\w+)\\(")
        val declared =
            File("src/main/kotlin")
                .walkTopDown()
                .filter { it.isFile && it.extension == "kt" }
                .flatMap { file -> pattern.findAll(file.readText()).map { it.groupValues[1] } }
                .filter { it == "HoglakeException" || it in known }
                .count()
        assertThat(declared)
            .describedAs(
                "declarations extending HoglakeException or one of its members in " +
                    "src/main/kotlin, vs the %s types the reflective walk found",
                known.size,
            )
            .isEqualTo(known.size)
    }

    /**
     * One instance of an exception type, built from its primary
     * constructor with a placeholder per parameter type.
     *
     * Deliberately NOT tolerant of an unknown parameter type: a new
     * exception carrying something this does not know fails loudly here,
     * which is the prompt to look at whether it needs its own wire code.
     */
    private fun instantiate(type: KClass<*>): HoglakeException {
        val ctor = type.primaryConstructor!!
        val args =
            ctor.parameters.map { parameter ->
                when (parameter.type.classifier) {
                    String::class -> "detail"
                    Long::class -> 1L
                    java.util.UUID::class -> java.util.UUID.randomUUID()
                    List::class -> listOf("ns.t")
                    else ->
                        error(
                            "add a placeholder for ${parameter.type} in instantiate(), and while " +
                                "you are there decide whether ${type.simpleName} needs its own " +
                                "wire code in errorBody",
                        )
                }
            }
        return ctor.call(*args.toTypedArray()) as HoglakeException
    }

    @Test
    fun `the derived path list sees every commit handler`() {
        // The one unsafe failure mode of deriving from a regex: a route
        // declared with a NON-LITERAL path (`post(COMMIT_PATH)`) is
        // invisible to it, and the test stays green with an undocumented
        // endpoint — the same failure class the derivation replaced, moved
        // up a level. Every commit handler calls `commits.commit` exactly
        // once, so counting those call sites pins "a route exists that the
        // regex did not see".
        val callSites = Regex(Regex.escape("commits.commit(")).findAll(routes).count()
        assertThat(callSites)
            .describedAs("commits.commit call sites in Routes.kt vs the derived commit paths")
            .isEqualTo(commitPaths().size)
    }

    @Test
    fun `every commit endpoint documents both non-retryable 409 codes and their recovery`() {
        val paths = commitPaths()
        assertThat(paths).describedAs("the routing tree's commit endpoints").hasSizeGreaterThanOrEqualTo(6)
        for (path in paths) {
            val body = pathBlock(path)
            for (code in listOf("ddl_since_read_snapshot", "table_recreated")) {
                assertThat(body).describedAs("%s must name %s", path, code).contains(code)
            }
            assertThat(body)
                .describedAs("%s must say the recovery is re-prepare", path)
                .contains("retry: re-prepare")
            assertThat(body)
                .describedAs("%s must reference ApiError from its 409 so the fields are documented", path)
                .contains("#/components/schemas/ApiError")
        }
    }

    @Test
    fun `no commit endpoint still claims every conflict is retryable`() {
        // The exact sentence two endpoints carried, which was the defect:
        // a client written from it loops on a refusal that can only be
        // refused again.
        for (path in commitPaths()) {
            assertThat(pathBlock(path))
                .describedAs("%s must not claim all conflicts are retryable", path)
                .doesNotContain("All are retryable after re-reading")
        }
    }

    @Test
    fun `every commit endpoint documents the below-floor 410`() {
        // Reachable on every one of them (CommitService refuses a
        // read_snapshot under the expiry floor) and documented on none
        // of them before this change.
        for (path in commitPaths()) {
            assertThat(pathBlock(path))
                .describedAs("%s must document the 410", path)
                .contains("\"410\":")
        }
    }

    @Test
    fun `the three DDL endpoints whose guard is typed document table_recreated`() {
        // SCOPE, stated because the list is hardcoded: these are the
        // routes whose handler reaches a TYPED guard (CatalogService's
        // drop and truncate, AlterService's alter). The three
        // `table-creations/…` endpoints also take expected_table_uuid but
        // their replacement guards in TableCreationService are still
        // untyped, so they correctly keep `commit_conflict` and are not
        // listed here. Typing those is a separate change, not an
        // oversight.
        //
        // Their 409 was a bare Conflict ref with no schema and no mention
        // of the code.
        for (
        path in
        listOf(
            "/catalogs/{catalog}/namespaces/{namespace}/tables/{table}",
            "/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/truncate",
            "/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/alter",
        )
        ) {
            val body = pathBlock(path)
            assertThat(body).describedAs("%s must name table_recreated", path).contains("table_recreated")
            assertThat(body)
                .describedAs("%s must reference ApiError from its 409", path)
                .contains("#/components/schemas/ApiError")
        }
    }

    @Test
    fun `the expected_table_uuid prose does not still promise commit_conflict`() {
        // Two of the most-consulted descriptions of the guard this change
        // re-typed said `commit_conflict` / `CommitConflict` long after
        // it stopped being true.
        val appendProse = propertyProse("TableAppend", "expected_table_uuid")
        assertThat(appendProse).contains("table_recreated")
        assertThat(appendProse).doesNotContain("commit_conflict")
    }
}
