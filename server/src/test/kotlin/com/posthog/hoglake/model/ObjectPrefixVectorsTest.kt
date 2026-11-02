package com.posthog.hoglake.model

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.util.HexFormat

/**
 * The shared object-prefix vectors
 * (`pyhoglake/tests/vectors/object_prefix_vectors.json`) against
 * [ObjectPrefix]. pyhoglake regenerates the file and pins its count
 * (`test_object_prefix_vectors.py`), and holds pyhoglake's
 * `object_prefix`, which writers build these paths with, to the same
 * file, so the path the server checks and the path a writer builds
 * cannot drift apart.
 */
class ObjectPrefixVectorsTest {
    private val root: JsonNode = ObjectMapper().readTree(Files.readString(ObjectPrefixVectorFile.resolve()))

    @Test
    fun `the vector count is pinned`() {
        val iam = root["iam"]
        val count =
            root["prefix_cases"].size() + root["containment_cases"].size() + root["stats_cases"].size() +
                iam["values"].size() + iam["foreign_keys"].size()
        assertThat(count)
            .describedAs("vector count (see ObjectPrefixVectorFile.EXPECTED_COUNT)")
            .isEqualTo(ObjectPrefixVectorFile.EXPECTED_COUNT)
    }

    @Test
    fun `every prefix case builds the expected prefix or refuses the value`() {
        val failures = mutableListOf<String>()
        for ((i, v) in root["prefix_cases"].withIndex()) {
            val type = ColType.fromWire(v["col_type"].asText())
            val raw = v["value"].takeUnless { it.isNull }?.asText()
            val parsed = ObjectPrefix.parseValue(raw, type)
            if (v.has("error")) {
                if (parsed != null) failures += "prefix_cases[$i]: '$raw' on $type parsed as $parsed, not refused"
                continue
            }
            if (parsed == null) {
                failures += "prefix_cases[$i]: '$raw' on $type was refused"
                continue
            }
            if (parsed.toString() != raw) failures += "prefix_cases[$i]: '$raw' on $type parsed as $parsed"
            val layout = ObjectPrefixLayout(fieldId = 1, fieldName = v["field"].asText(), colType = type)
            val built =
                ObjectPrefix.prefix(
                    v["data_path"].asText(),
                    v["namespace"].asText(),
                    v["table"].asText(),
                    layout,
                    raw!!,
                )
            val expected = v["expected"].asText()
            if (built != expected) failures += "prefix_cases[$i]: built '$built', expected '$expected'"
        }
        assertThat(failures).isEmpty()
    }

    @Test
    fun `every containment case agrees with isUnder`() {
        val failures =
            root["containment_cases"].withIndex().mapNotNull { (i, v) ->
                val under = ObjectPrefix.isUnder(v["path"].asText(), v["prefix"].asText())
                val path = v["path"].asText()
                if (under == v["under"].asBoolean()) null else "containment_cases[$i]: $path under=$under"
            }
        assertThat(failures).isEmpty()
    }

    @Test
    fun `every stats case gets its verdict`() {
        val hex = HexFormat.of()
        val failures =
            root["stats_cases"].withIndex().mapNotNull { (i, v) ->
                val type = ColType.fromWire(v["col_type"].asText())
                val layout = ObjectPrefixLayout(fieldId = 7, fieldName = "team_id", colType = type)
                val stats =
                    v["stats"].takeUnless { it.isNull }?.let { s ->
                        ColumnStats(
                            fieldId = 7,
                            valueCount = 1,
                            nullCount = s["null_count"].asLong(),
                            nanCount = null,
                            sizeBytes = null,
                            lowerBound = s["lower_hex"].takeUnless { it.isNull }?.let { hex.parseHex(it.asText()) },
                            upperBound = s["upper_hex"].takeUnless { it.isNull }?.let { hex.parseHex(it.asText()) },
                        )
                    }
                val verdict = ObjectPrefix.checkFieldStats(layout, v["value"].asLong(), stats)
                val expected = v["verdict"].asText()
                if (verdict.name == expected) null else "stats_cases[$i]: got $verdict, expected $expected"
            }
        assertThat(failures).isEmpty()
    }
}

/**
 * Locating the shared vector file as [com.posthog.hoglake.stats.BoundsVectorFile]
 * does: relative to `server/` first, then walking up from user.dir, and a
 * missing file FAILS, naming every path tried.
 */
internal object ObjectPrefixVectorFile {
    /**
     * Pinned exactly, so a vector lost to a bad merge fails instead of
     * shrinking coverage in silence. Update DELIBERATELY, together with
     * EXPECTED_VECTOR_COUNT in pyhoglake/tests/test_object_prefix_vectors.py.
     * Counts every case, iam values and foreign keys included.
     */
    const val EXPECTED_COUNT = 69

    private const val REPO_RELATIVE = "pyhoglake/tests/vectors/object_prefix_vectors.json"

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
            "object-prefix vector file not found, so the layout parity gate cannot run. Tried:\n" +
                tried.joinToString("\n") { "  $it" },
        )
    }
}
