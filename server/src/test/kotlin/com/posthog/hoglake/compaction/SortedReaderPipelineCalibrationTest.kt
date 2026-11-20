package com.posthog.hoglake.compaction

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.concurrent.TimeUnit

/** Exercises real decoded readers under fixed production settings. */
class SortedReaderPipelineCalibrationTest {
    @TempDir
    lateinit var directory: Path

    @Test
    fun `a near-target dictionary row group shares positive reader excess and compacts`() {
        val source = child("generate-near", "1024m")
        assertThat(source["categoryDictionaries"].size()).isEqualTo(10)
        source["categoryDictionaries"].forEach { dictionary ->
            assertThat(dictionary["dictionaryEntries"].asInt()).isEqualTo(60_000)
            assertThat(dictionary["firstDataEncoding"].asText()).isIn("RLE_DICTIONARY", "PLAIN_DICTIONARY")
        }
        val result = child("rewrite-near", "2867m")
        assertThat(result["inputBytes"].asLong()).isLessThan(536_870_912L)
        assertThat(result["compressedTargetRatio"].asDouble()).isBetween(0.90, 0.98)
        assertThat(result["readerExcessBytes"].asLong()).isPositive()
        assertThat(result["rowsVerified"].asLong()).isEqualTo(2_000_000)
    }

    @Test
    fun `a sparse binary and string schema compacts in six runs at the defaults`() {
        child("generate-sparse", "1024m")
        val result = child("rewrite-sparse", "2867m")
        assertThat(result["rowsVerified"].asLong()).isEqualTo(200_000)
        assertThat(result["runsSpilled"].asLong()).isEqualTo(6)
        assertThat(result["sparseModulo"].asLong()).isEqualTo(1999)
        assertThat(result["optionalFields"].asLong()).isEqualTo(150)
        assertThat(result["nonNullOptionalValues"].asLong()).isEqualTo(15_006L)
        assertThat(result["payloadDigest"].asText()).isEqualTo(result["expectedPayloadDigest"].asText())
    }

    @Test
    fun `four hundred thousand event rows in one row group compact at the defaults`() {
        child("generate-event", "1024m")
        val result = child("rewrite-event", "2867m")
        assertThat(result["rowsVerified"].asLong()).isEqualTo(400_000)
        assertThat(result["inputRowGroups"].asLong()).isEqualTo(1)
        assertThat(result["payloadDigest"].asText()).isEqualTo(result["expectedPayloadDigest"].asText())
    }

    private fun child(
        mode: String,
        heap: String,
    ): JsonNode {
        val log = directory.resolve("$mode.log")
        val process =
            ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx$heap",
                "-XX:+UseG1GC",
                "-cp",
                System.getProperty("java.class.path"),
                SortedReaderPipelineCalibrationProbe::class.java.name,
                mode,
                directory.toString(),
            ).redirectErrorStream(true).redirectOutput(log.toFile()).start()
        val finished = process.waitFor(1200, TimeUnit.SECONDS)
        if (!finished) process.destroyForcibly().waitFor()
        val report = directory.resolve("$mode.json")
        val destination =
            Path.of(
                System.getenv("HOGLAKE_CALIBRATION_REPORT_DIR") ?: "build/reports/sorted-memory-calibration",
            ).resolve(directory.fileName.toString())
        Files.createDirectories(destination)
        Files.copy(log, destination.resolve("$mode.log"), StandardCopyOption.REPLACE_EXISTING)
        if (Files.exists(
                report,
            )
        ) {
            Files.copy(report, destination.resolve("$mode.json"), StandardCopyOption.REPLACE_EXISTING)
        }
        val text = Files.readString(log)
        assertThat(finished).describedAs(text).isTrue()
        assertThat(process.exitValue()).describedAs(text).isZero()
        assertThat(text).doesNotContain("OutOfMemoryError")
        assertThat(text).contains("READER_PIPELINE_OK $mode")
        assertThat(report).exists()
        return ObjectMapper().readTree(report.toFile())
    }
}
