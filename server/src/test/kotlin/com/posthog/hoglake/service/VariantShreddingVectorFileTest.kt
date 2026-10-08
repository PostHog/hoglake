package com.posthog.hoglake.service

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path

/**
 * The `type_params.shredding` grammar against the vector file pyhoglake
 * mirrors it with (pyhoglake/tests/vectors/variant_shredding_vectors.json,
 * read by pyhoglake/tests/test_variant_ddl.py): every declaration there is
 * accepted or refused here as the file says, and a refusal is this
 * validator's message word for word, path included. The Python client
 * refuses a declaration before the round trip with the same words, so
 * the file is what keeps the two from drifting: a rule changed here and
 * not there reds one side or the other.
 *
 * The file is read with the production wire mapper, so every number in a
 * declaration has the type a request gives it (`18.0` a Double, `2^64` a
 * BigInteger). Most of its expectations are not this validator's output
 * written back: they were transcribed from [VariantShreddingTest],
 * VariantShreddingApiTest, the fuzz seeds and the documented grammar, and
 * this test is what checks them. The case-folding vectors (how the JDK
 * lowercases GREEK CAPITAL SIGMA in its word, and the pairs of its newer
 * Unicode data) and the fault-order witnesses were recorded from this
 * validator instead, since no documented grammar decides them; for those
 * this test is a regression snapshot. A refusal marked `"client": "may_accept"`
 * is one the Python client leaves to this validator (a case collision that
 * turns on how GREEK CAPITAL SIGMA lowercases, or on a case pair newer than
 * the client's Unicode data); it is checked here exactly like any other,
 * and only a case collision of names that are not all ASCII may be marked.
 */
class VariantShreddingVectorFileTest {
    @Test
    fun `every vector is decided as the file says`() {
        val file = VariantShreddingVectorFile.resolve()
        val root = wireObjectMapper().readValue<Map<String, Any?>>(Files.readAllBytes(file))
        assertThat(root["format"]).isEqualTo("hoglake-variant-shredding-vectors")
        assertThat(root["version"]).isEqualTo(1)
        val column = root["column"] as String
        val vectors = root["vectors"] as List<*>
        assertThat(vectors)
            .describedAs("vector count (pinned in both languages; see VariantShreddingVectorFile.EXPECTED_COUNT)")
            .hasSize(VariantShreddingVectorFile.EXPECTED_COUNT)

        val failures = mutableListOf<String>()
        val ids = HashSet<String>()
        for ((index, entry) in vectors.withIndex()) {
            val vector = entry as Map<*, *>
            val id = vector["id"] as? String ?: error("vector[$index] has no id")
            check(ids.add(id)) { "duplicate vector id '$id'" }
            check(vector.containsKey("declaration")) { "vector '$id' has no declaration" }
            val declaration = vector["declaration"]
            check(vector["client"] == null || (vector["client"] == "may_accept" && vector["expect"] == "refuse")) {
                "vector '$id' has client '${vector["client"]}'; only a refusal may be marked may_accept"
            }
            // The marking switches off the client's half of the check, so it
            // is for the one rule the client may leave here: a case collision,
            // and one with a name that is not ASCII, whose case every client
            // decides as this validator does.
            if (vector["client"] != null) {
                val problem = vector["problem"] as? String ?: ""
                check(problem.startsWith(CASE_PROBLEM) && problem.any { it.code > 0x7F }) {
                    "vector '$id' is marked may_accept, but only a case collision of names that are " +
                        "not all ASCII may be: '$problem'"
                }
            }
            val expected =
                when (vector["expect"]) {
                    "accept" -> null
                    "refuse" -> {
                        val path = vector["path"] as? String ?: error("refusal '$id' has no path")
                        val problem = vector["problem"] as? String ?: error("refusal '$id' has no problem")
                        "variant column '$column' has an invalid type_params.shredding: $path $problem"
                    }
                    else -> error("vector '$id' expects '${vector["expect"]}'")
                }
            // Both entry points, as every DDL path reaches the rules through
            // the column check. A null declaration declares nothing and only
            // the column check takes one.
            val direct = if (declaration == null) null else refusal { VariantShredding.validate(declaration, column) }
            val viaColumn =
                refusal {
                    ColumnTrees.validate(
                        listOf(ColumnDef(column, ColType.VARIANT, mapOf(VariantShredding.KEY to declaration))),
                    )
                }
            if (direct != expected || viaColumn != expected) {
                failures += "'$id': expected ${expected ?: "accept"}, VariantShredding said ${direct ?: "accept"}, " +
                    "ColumnTrees said ${viaColumn ?: "accept"}"
            }
        }
        assertThat(failures).describedAs("vectors decided differently from the file").isEmpty()
    }

    @Test
    fun `the file covers every rule, both ways`() {
        val problems = vectors().mapNotNull { (it as Map<*, *>)["problem"] as String? }.toSet()
        // Every refusal the validator can make, read out of its source rather
        // than listed here, so a rule added to it without a vector reds this
        // test: each `refusal(path, "...")` template, its interpolations
        // standing for any text, must match some vector's problem in full.
        val templates = refusalTemplates()
        assertThat(templates).describedAs("refusal templates parsed out of VariantShredding.kt").hasSizeGreaterThan(15)
        for (template in templates) {
            val shape = Regex(template.split(INTERPOLATION).joinToString(".+") { Regex.escape(it) })
            val matched = problems.any { shape.matches(it) }
            assertThat(matched).describedAs("a refusal vector for \"%s\"", template).isTrue()
        }
        // And the values the parameterised templates take: every decimal
        // type's bound and both decimal parameters.
        val instances =
            listOf("has no integer precision", "has no integer scale") +
                listOf("decimal4", "decimal8", "decimal16").map { "which $it does not hold" }
        for (instance in instances) {
            val matched = problems.any { it.endsWith(instance) }
            assertThat(matched).describedAs("a refusal vector for '%s'", instance).isTrue()
        }
        assertThat(vectors().count { (it as Map<*, *>)["expect"] == "accept" }).isGreaterThan(20)
    }

    @Test
    fun `every request seed of the fuzz corpus is a vector`() {
        // The seeds the fuzz target replays are the file's too, verbatim, so
        // the Python client decides each of them as well. Checked here, a
        // change that adds a seed reds its own CI, which runs this suite.
        val byId = vectors().associateBy { (it as Map<*, *>)["id"] as String }
        val seeds =
            Files.list(SEEDS).use { files ->
                files.map { it.fileName.toString() }.filter { it.startsWith("request_") }.toList()
            }
        assertThat(seeds).contains("request_nul_name", "request_fields_and_arrays_over_limit")
        for (seed in seeds.sorted()) {
            val vector = byId["fuzz_$seed"] as Map<*, *>?
            assertThat(vector).describedAs("a vector fuzz_%s for the seed %s", seed, seed).isNotNull()
            // The seed is the declaration and one trailing byte that picks the
            // target's arbitrary mode.
            val bytes = Files.readAllBytes(SEEDS.resolve(seed))
            val declaration = wireObjectMapper().readValue<Any?>(bytes.copyOf(bytes.size - 1))
            assertThat(vector!!["declaration"]).describedAs("fuzz_%s", seed).isEqualTo(declaration)
        }
    }

    private fun vectors(): List<*> =
        wireObjectMapper().readValue<Map<String, Any?>>(Files.readAllBytes(VariantShreddingVectorFile.resolve()))
            .getValue("vectors") as List<*>

    /**
     * The problem templates of VariantShredding.kt: the string literal of
     * every `refusal(...)` call, with Kotlin's `" +` line splits joined.
     */
    private fun refusalTemplates(): List<String> {
        val source = Files.readString(SOURCE).replace(Regex("\"\\s*\\+\\s*\n\\s*\""), "")
        return Regex("""refusal\(\s*(?:path|"\$")\s*,\s*"((?:[^"\\]|\\.)*)"""")
            .findAll(source)
            .map { it.groupValues[1] }
            .toList()
    }

    private companion object {
        val SOURCE: Path = Path.of("src/main/kotlin/com/posthog/hoglake/service/VariantShredding.kt")
        val SEEDS: Path =
            Path.of("src/test/resources/com/posthog/hoglake/fuzz/VariantShreddingFuzzTestInputs")
                .resolve("declarationsFollowTheDocumentedRules")

        /** The problem of a case collision, the one refusal the client may leave here. */
        const val CASE_PROBLEM = "has fields that differ only by case: "

        /** A Kotlin string interpolation: `${...}` or `$name`. */
        val INTERPOLATION = Regex("""\$\{[^}]*\}|\$[A-Za-z_]\w*""")
    }

    private fun refusal(validate: () -> Unit): String? =
        try {
            validate()
            null
        } catch (e: HoglakeException.Validation) {
            e.message
        }
}

/**
 * Locating the shared vector file the way [com.posthog.hoglake.stats.QeBoundsVectorsTest]
 * locates its own: relative to `server/` first, then walking up from
 * user.dir, and a missing file FAILS, naming every path tried. A skip would
 * make a moved file indistinguishable from a passing parity gate.
 */
internal object VariantShreddingVectorFile {
    /**
     * Pinned exactly, so a vector lost to a bad merge fails instead of
     * shrinking coverage in silence. Update DELIBERATELY, together with the
     * identical pin in pyhoglake/tests/test_variant_ddl.py.
     */
    const val EXPECTED_COUNT = 205

    private const val REPO_RELATIVE = "pyhoglake/tests/vectors/variant_shredding_vectors.json"

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
            "shredding vector file not found, so the Python/Kotlin declaration parity gate cannot run. Tried:\n" +
                tried.joinToString("\n") { "  $it" },
        )
    }
}
