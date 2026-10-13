package com.posthog.hoglake.hydrator

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.wireObjectMapper
import org.apache.parquet.format.BsonType
import org.apache.parquet.format.ConvertedType
import org.apache.parquet.format.DateType
import org.apache.parquet.format.DecimalType
import org.apache.parquet.format.EnumType
import org.apache.parquet.format.FieldRepetitionType
import org.apache.parquet.format.Float16Type
import org.apache.parquet.format.GeographyType
import org.apache.parquet.format.GeometryType
import org.apache.parquet.format.IntType
import org.apache.parquet.format.JsonType
import org.apache.parquet.format.ListType
import org.apache.parquet.format.LogicalType
import org.apache.parquet.format.MapType
import org.apache.parquet.format.MicroSeconds
import org.apache.parquet.format.MilliSeconds
import org.apache.parquet.format.NanoSeconds
import org.apache.parquet.format.NullType
import org.apache.parquet.format.SchemaElement
import org.apache.parquet.format.StringType
import org.apache.parquet.format.TimeType
import org.apache.parquet.format.TimeUnit
import org.apache.parquet.format.TimestampType
import org.apache.parquet.format.UUIDType
import org.apache.parquet.format.Util
import org.apache.parquet.format.VariantType
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import org.apache.parquet.format.FileMetaData as ThriftFileMetaData
import org.apache.parquet.format.Type as ThriftType

/**
 * How this hydrator reads a schema element's annotation, against the vector
 * file pyhoglake refuses prepared files by
 * (pyhoglake/tests/vectors/schema_annotation_vectors.json, read by
 * pyhoglake/tests/test_variant_schema.py).
 *
 * pyarrow reads an element's logical type when it has one, and its converted
 * type only when it has not. parquet-java lets a converted type that spells
 * another type win over the logical one, and its schema builder throws on an
 * annotation that does not fit the element (a DATE on a BYTE_ARRAY, a DECIMAL
 * whose scale field contradicts its logical type), so a file pyarrow reads
 * is one this hydrator cannot parse, or reads as another type. pyhoglake
 * refuses every element whose verdict here is not `reads`, so a parquet-java
 * that reads any vector otherwise reds here, rather than as a file pyhoglake
 * publishes unread or refuses for nothing. Each vector's footer is built from
 * its fields with parquet-format's own Thrift classes, and parsed through
 * [FooterParse], the hydrator's entry point.
 */
class SchemaAnnotationVectorFileTest {
    @TempDir
    lateinit var dir: Path

    @Test
    fun `every vector is read as the file says`() {
        val bytes = Files.readAllBytes(SchemaAnnotationVectorFile.resolve())
        val root = wireObjectMapper().readValue<Map<String, Any?>>(bytes)
        assertThat(root["format"]).isEqualTo("hoglake-schema-annotation-vectors")
        assertThat(root["version"]).isEqualTo(1)
        val vectors = root["vectors"] as List<*>
        assertThat(vectors)
            .describedAs("vector count (pinned in both languages; see SchemaAnnotationVectorFile.EXPECTED_COUNT)")
            .hasSize(SchemaAnnotationVectorFile.EXPECTED_COUNT)
        val failures = mutableListOf<String>()
        val ids = HashSet<String>()
        for ((index, entry) in vectors.withIndex()) {
            val vector = entry as Map<*, *>
            val id = vector["id"] as? String ?: error("vector[$index] has no id")
            check(ids.add(id)) { "duplicate vector id '$id'" }
            val verdict = verdict(vector, index)
            if (verdict != vector["parquet_java"]) {
                failures += "$id: file says ${vector["parquet_java"]}, parquet-java $verdict"
            }
        }
        assertThat(failures).isEmpty()
    }

    /**
     * `refuses` when the footer does not parse; `reads_converted` when it
     * does, with a logical and a converted type, and the annotation read is
     * not the one the element has without its converted type (the logical
     * type alone, as pyarrow reads it); `reads` otherwise.
     */
    private fun verdict(
        vector: Map<*, *>,
        index: Int,
    ): String {
        val asIs = annotationOf(vector, index, withConverted = true) ?: return "refuses"
        if (vector["logical"] == null || vector["converted"] == null) return "reads"
        val logicalAlone = annotationOf(vector, index, withConverted = false)
        return if (logicalAlone == asIs) "reads" else "reads_converted"
    }

    /** The element's annotation as [FooterParse] reads it, or null for a footer it refuses. */
    private fun annotationOf(
        vector: Map<*, *>,
        index: Int,
        withConverted: Boolean,
    ): Read? {
        val schema = listOf(SchemaElement("schema").setNum_children(1)) + elements(vector, withConverted)
        val footer = ByteArrayOutputStream()
        Util.writeFileMetaData(ThriftFileMetaData(1, schema, 0, emptyList()), footer)
        val magic = "PAR1".toByteArray()
        val length = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footer.size()).array()
        val file = dir.resolve("$index-$withConverted.parquet")
        Files.write(file, magic + footer.toByteArray() + length + magic)
        return try {
            Read(FooterParse.parse(LocalInputFile(file)).fileMetaData.schema.getType("c").logicalTypeAnnotation)
        } catch (e: IOException) {
            null
        }
    }

    /** A null annotation read, told apart from a footer not read. */
    private data class Read(
        val annotation: LogicalTypeAnnotation?,
    )

    private fun elements(
        vector: Map<*, *>,
        withConverted: Boolean,
    ): List<SchemaElement> {
        val element = SchemaElement("c").setRepetition_type(FieldRepetitionType.OPTIONAL)
        val physical = vector["physical"] as String
        if (physical != "GROUP") {
            element.setType(ThriftType.valueOf(physical))
            (vector["length"] as Int?)?.let { element.setType_length(it) }
        } else {
            element.setNum_children(1)
        }
        val converted = vector["converted"] as String?
        if (withConverted && converted != null) element.setConverted_type(ConvertedType.valueOf(converted))
        (vector["scale"] as Int?)?.let { element.setScale(it) }
        (vector["precision"] as Int?)?.let { element.setPrecision(it) }
        (vector["logical"] as Map<*, *>?)?.let { element.setLogicalType(logicalType(it)) }
        val leaf =
            SchemaElement("x")
                .setType(ThriftType.BYTE_ARRAY)
                .setRepetition_type(FieldRepetitionType.OPTIONAL)
        return if (physical == "GROUP") listOf(element, leaf) else listOf(element)
    }

    private fun logicalType(spelled: Map<*, *>): LogicalType {
        val (name, raw) = spelled.entries.single()
        val fields = raw as Map<*, *>

        fun unit(): TimeUnit =
            when (fields["unit"]) {
                "MILLIS" -> TimeUnit.MILLIS(MilliSeconds())
                "MICROS" -> TimeUnit.MICROS(MicroSeconds())
                "NANOS" -> TimeUnit.NANOS(NanoSeconds())
                else -> error("unit ${fields["unit"]}")
            }
        return when (name) {
            "STRING" -> LogicalType.STRING(StringType())
            "MAP" -> LogicalType.MAP(MapType())
            "LIST" -> LogicalType.LIST(ListType())
            "ENUM" -> LogicalType.ENUM(EnumType())
            "DECIMAL" -> LogicalType.DECIMAL(DecimalType(fields["scale"] as Int, fields["precision"] as Int))
            "DATE" -> LogicalType.DATE(DateType())
            "TIME" -> LogicalType.TIME(TimeType(fields["isAdjustedToUTC"] as Boolean, unit()))
            "TIMESTAMP" -> LogicalType.TIMESTAMP(TimestampType(fields["isAdjustedToUTC"] as Boolean, unit()))
            "INTEGER" -> {
                val width = (fields["bitWidth"] as Int).toByte()
                LogicalType.INTEGER(IntType(width, fields["isSigned"] as Boolean))
            }
            "UNKNOWN" -> LogicalType.UNKNOWN(NullType())
            "JSON" -> LogicalType.JSON(JsonType())
            "BSON" -> LogicalType.BSON(BsonType())
            "UUID" -> LogicalType.UUID(UUIDType())
            "FLOAT16" -> LogicalType.FLOAT16(Float16Type())
            "VARIANT" ->
                LogicalType.VARIANT(
                    VariantType().setSpecification_version((fields["specification_version"] as Int).toByte()),
                )
            "GEOMETRY" -> LogicalType.GEOMETRY(GeometryType())
            "GEOGRAPHY" -> LogicalType.GEOGRAPHY(GeographyType())
            else -> error("logical type $name")
        }
    }
}

/**
 * Locating the shared vector file the way [com.posthog.hoglake.stats.QeBoundsVectorsTest]
 * locates its own: relative to `server/` first, then walking up from
 * user.dir, and a missing file FAILS, naming every path tried.
 */
internal object SchemaAnnotationVectorFile {
    /**
     * Pinned exactly, so a vector lost to a bad merge fails instead of
     * shrinking coverage in silence. Update DELIBERATELY, together with the
     * identical pin in pyhoglake/tests/test_variant_schema.py.
     */
    const val EXPECTED_COUNT = 1643

    private const val REPO_RELATIVE = "pyhoglake/tests/vectors/schema_annotation_vectors.json"

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
            "schema annotation vector file not found, so the pyhoglake/parquet-java annotation parity gate " +
                "cannot run. Tried:\n" + tried.joinToString("\n") { "  $it" },
        )
    }
}
