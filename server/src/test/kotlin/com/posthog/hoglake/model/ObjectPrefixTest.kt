package com.posthog.hoglake.model

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.stats.IcebergSingleValue
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.util.UUID

class ObjectPrefixTest {
    private val layout = ObjectPrefixLayout(fieldId = 3, fieldName = "team_id", colType = ColType.LONG)

    @Test
    fun `keyIndex finds the one identity field on the source`() {
        val fields =
            listOf(
                PartitionFieldDef(sourceFieldId = 9, transform = Transform.DAY),
                PartitionFieldDef(sourceFieldId = 3, transform = Transform.IDENTITY),
            )
        assertThat(ObjectPrefix.keyIndex(fields, 3)).isEqualTo(1)
        val identityOnBoth =
            listOf(
                PartitionFieldDef(sourceFieldId = 9, transform = Transform.IDENTITY),
                PartitionFieldDef(sourceFieldId = 3, transform = Transform.IDENTITY),
            )
        assertThat(ObjectPrefix.keyIndex(identityOnBoth, 3)).isEqualTo(1)
    }

    @Test
    fun `keyIndex is null without exactly one identity field on the source`() {
        val bucketOnly = listOf(PartitionFieldDef(sourceFieldId = 3, transform = Transform.BUCKET, transformParam = 8))
        val twice =
            listOf(
                PartitionFieldDef(sourceFieldId = 3, transform = Transform.IDENTITY),
                PartitionFieldDef(sourceFieldId = 3, transform = Transform.IDENTITY),
            )
        val otherSource = listOf(PartitionFieldDef(sourceFieldId = 9, transform = Transform.IDENTITY))
        assertThat(ObjectPrefix.keyIndex(emptyList(), 3)).isNull()
        assertThat(ObjectPrefix.keyIndex(otherSource, 3)).isNull()
        assertThat(ObjectPrefix.keyIndex(bucketOnly, 3)).isNull()
        assertThat(ObjectPrefix.keyIndex(twice, 3)).isNull()
    }

    @Test
    fun `the name grammar is the catalog identifier pattern`() {
        assertThat(ObjectPrefix.NAME.pattern).isEqualTo(Identifiers.PATTERN)
    }

    @Test
    fun `without a layout the data file path is compaction's current output path`() {
        val uuid = UUID.fromString("00000000-0000-0000-0000-000000000042")
        val dataPath = "s3://bucket/millrace/millrace-dev/"
        val legacy = "${dataPath.trimEnd('/')}/data/ingest/events/$uuid.parquet"
        assertThat(ObjectPrefix.dataFilePath(dataPath, "ingest", "events", layout = null, value = null, uuid = uuid))
            .isEqualTo(legacy)
    }

    @Test
    fun `with a layout the data file path sits under the value's prefix`() {
        val uuid = UUID.fromString("00000000-0000-0000-0000-000000000042")
        val path = ObjectPrefix.dataFilePath("s3://b/lake", "ingest", "events", layout, "4", uuid)
        assertThat(path).isEqualTo("s3://b/lake/data/ingest/events/team_id=4/$uuid.parquet")
        assertThat(
            ObjectPrefix.isUnder(path, ObjectPrefix.prefix("s3://b/lake", "ingest", "events", layout, "4")),
        ).isTrue()
        assertThat(
            ObjectPrefix.isUnder(path, ObjectPrefix.prefix("s3://b/lake", "ingest", "events", layout, "42")),
        ).isFalse()
    }

    @Test
    fun `a prefix refuses a non-canonical value or a name outside the identifier grammar`() {
        assertThatThrownBy { ObjectPrefix.prefix("s3://b/lake", "ingest", "events", layout, "042") }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { ObjectPrefix.prefix("s3://b/lake", "ingest", "ev/ents", layout, "4") }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy {
            ObjectPrefix.prefix("s3://b/lake", "ingest", "events", layout.copy(fieldName = "team=id"), "4")
        }.isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { ObjectPrefix.dataFilePath("s3://b/lake", "ingest", "events", layout, null) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    /**
     * The reader role grants `GetObject` on the prefix with the value
     * replaced by the `hoglake-team-id` principal tag, followed by `*`.
     * With the tag substituted, that Resource must be exactly the ARN of
     * the server's prefix (`arn:aws:s3:::` + the prefix without `s3://`)
     * plus `*`, and, read as an IAM glob, must not match the ARN of
     * another value's objects or of the table root.
     */
    @Test
    fun `the reader role's resource pattern grants exactly one value's prefix`() {
        val iam = ObjectMapper().readTree(Files.readString(ObjectPrefixVectorFile.resolve()))["iam"]
        val pattern = iam["resource_pattern"].asText()
        val dataPath = iam["data_path"].asText()
        val namespace = iam["namespace"].asText()
        val table = iam["table"].asText()
        val iamLayout =
            ObjectPrefixLayout(
                fieldId = 1,
                fieldName = iam["field"].asText(),
                colType = ColType.fromWire(iam["col_type"].asText()),
            )
        val tag = "\${aws:PrincipalTag/hoglake-team-id}"
        assertThat(pattern).contains(tag)
        for (value in iam["values"].map { it.asText() }) {
            val resource = pattern.replace(tag, value)
            val prefix = ObjectPrefix.prefix(dataPath, namespace, table, iamLayout, value)
            assertThat(resource).isEqualTo("arn:aws:s3:::" + prefix.removePrefix("s3://") + "*")
        }
        val grantFor4 = pattern.replace(tag, "4")
        val grantedKey = ObjectPrefix.prefix(dataPath, namespace, table, iamLayout, "4").removePrefix("s3://")
        assertThat(iamGlobMatches(grantFor4, "arn:aws:s3:::${grantedKey}a.parquet")).isTrue()
        for (key in iam["foreign_keys"].map { it.asText() }) {
            assertThat(iamGlobMatches(grantFor4, "arn:aws:s3:::$key")).describedAs(key).isFalse()
        }
    }

    @Test
    fun `expectedBound matches IcebergSingleValue for every allowed type`() {
        val max =
            mapOf(
                ColType.INT8 to 127L,
                ColType.INT16 to 32_767L,
                ColType.INT to Int.MAX_VALUE.toLong(),
                ColType.UINT8 to 255L,
                ColType.UINT16 to 65_535L,
                ColType.UINT32 to 4_294_967_295L,
                ColType.LONG to Long.MAX_VALUE,
            )
        assertThat(max.keys).isEqualTo(ObjectPrefix.ALLOWED_TYPES)
        for ((type, top) in max) {
            for (value in listOf(0L, 4L, top)) {
                val carrier: Any = if (type == ColType.UINT32 || type == ColType.LONG) value else value.toInt()
                assertThat(ObjectPrefix.expectedBound(type, value))
                    .describedAs("$type $value")
                    .isEqualTo(IcebergSingleValue.encode(type, carrier))
            }
        }
    }

    @Test
    fun `a value outside the field type's range never proves a file`() {
        val int8Layout = layout.copy(colType = ColType.INT8)
        val bound = ObjectPrefix.expectedBound(ColType.INT8, 4)
        val stats = ColumnStats(3, 1, 0, null, null, bound, bound)
        assertThat(ObjectPrefix.checkFieldStats(int8Layout, 300, stats)).isEqualTo(ObjectPrefix.Verdict.BOUND_MISMATCH)
        assertThat(ObjectPrefix.checkFieldStats(int8Layout, -1, stats)).isEqualTo(ObjectPrefix.Verdict.BOUND_MISMATCH)
        assertThatThrownBy { ObjectPrefix.expectedBound(ColType.INT8, 300) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `stats of another column never prove a file`() {
        val bound = ObjectPrefix.expectedBound(ColType.LONG, 4)
        val otherColumn =
            ColumnStats(
                fieldId = 99,
                valueCount = 1,
                nullCount = 0,
                nanCount = null,
                sizeBytes = null,
                lowerBound = bound,
                upperBound = bound,
            )
        assertThat(ObjectPrefix.checkFieldStats(layout, 4, otherColumn))
            .isEqualTo(ObjectPrefix.Verdict.MISSING_STATS)
        assertThat(ObjectPrefix.checkFieldStats(layout, 4, otherColumn.copy(fieldId = layout.fieldId)))
            .isEqualTo(ObjectPrefix.Verdict.OK)
    }

    /** IAM Resource matching: `*` matches any run of characters, `/` included; `?` one character. */
    private fun iamGlobMatches(
        pattern: String,
        arn: String,
    ): Boolean {
        val regex =
            pattern.split("*").joinToString(".*") { part ->
                part.split("?").joinToString(".") { Regex.escape(it) }
            }
        return Regex("^$regex$").matches(arn)
    }
}
