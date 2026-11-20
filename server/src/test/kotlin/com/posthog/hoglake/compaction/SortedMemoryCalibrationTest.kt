package com.posthog.hoglake.compaction

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit

/** Fixes ordinary-input acceptance independently from the production sizing formulas. */
class SortedMemoryCalibrationTest {
    @TempDir
    lateinit var directory: Path

    @Test
    fun `the default event shape keeps its measured four-run admission`() {
        val config = CompactionConfig(targetBytes = 536_870_912L, maxGroupsPerRun = 1)
        val spill = config.sortSpill(SortedMemoryCalibrationProbe.columns)
        assertThat(config.sortedHeapBytes).isEqualTo(1_073_741_824L)
        assertThat(config.parallelGroups).isEqualTo(1)
        assertThat(config.targetBytes).isEqualTo(536_870_912L)
        assertThat(spill.chunkRows).isEqualTo(508_400L)
        assertThat(ExternalMergeSort.spillRuns(2_000_000, spill)).isEqualTo(4)
        // 16 MiB block, eleven 221,340-byte reader slots, and two 2,304-byte estimated heads.
        // Each slot adds packed scratch and fixed objects to the data/dictionary page targets.
        assertThat(ExternalMergeSort.predictedSpilledRunBytes(11, spill)).isEqualTo(19_216_564L)
        assertThat(ParquetRewriter.APPEND_MIN_ROW_GROUP_BYTES).isEqualTo(33_554_432L)
        assertThat(ExternalMergeSort.OUTPUT_BLOCK_BYTES).isEqualTo(134_217_728L)
    }

    @Test
    fun `two million honest event rows compact in exactly four spills`() {
        child("generate", "1024m")
        val ordinary = child("ordinary", "2867m")
        assertThat(ordinary["rowsVerified"].asLong()).isEqualTo(2_000_000)
        assertThat(ordinary["runsSpilled"].asInt()).isEqualTo(4)
        assertThat(ordinary["predictedRuns"].asLong()).isEqualTo(4)
        assertThat(ordinary["admissionReservationBytes"].asLong()).isEqualTo(76_866_256L)
        assertThat(ordinary["observedMergeBytes"].asLong())
            .isPositive()
            .isLessThanOrEqualTo(ordinary["admissionReservationBytes"].asLong())
        assertThat(ordinary["inputBytes"].asLong()).isBetween(32L shl 20, 192L shl 20)
        assertThat(ordinary["inputReadBytes"].asLong()).isPositive()
        assertThat(ordinary["inputPageHeaders"].asLong()).isPositive()
    }

    @Test
    fun `disjoint prior outputs append at the production floor without decoding`() {
        child("generate-append", "1024m")
        val appended = child("append", "2867m")
        assertThat(appended["rowGroupsAppended"].asLong()).isGreaterThanOrEqualTo(2)
        assertThat(appended["bytesAppended"].asLong()).isGreaterThanOrEqualTo(67_108_864L)
        assertThat(appended["decodedReaders"].asLong()).isZero()
        assertThat(appended["decodedRows"].asLong()).isZero()
        assertThat(appended["chunkBytesIdentical"].asBoolean()).isTrue()
    }

    private fun child(
        mode: String,
        heap: String,
    ): com.fasterxml.jackson.databind.JsonNode {
        val log = directory.resolve("$mode.log")
        val process =
            ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx$heap",
                "-XX:+UseG1GC",
                "-cp",
                System.getProperty("java.class.path"),
                SortedMemoryCalibrationProbe::class.java.name,
                mode,
                directory.toString(),
            ).redirectErrorStream(true).redirectOutput(log.toFile()).start()
        val finished = process.waitFor(900, TimeUnit.SECONDS)
        if (!finished) process.destroyForcibly().waitFor()
        val text = Files.readString(log)
        val destination =
            Path.of(System.getenv("HOGLAKE_CALIBRATION_REPORT_DIR") ?: "build/reports/sorted-memory-calibration")
                .resolve(directory.fileName.toString())
        Files.createDirectories(destination)
        Files.copy(log, destination.resolve("$mode.log"), java.nio.file.StandardCopyOption.REPLACE_EXISTING)
        val report = directory.resolve("$mode.json")
        if (Files.exists(
                report,
            )
        ) {
            Files.copy(report, destination.resolve("$mode.json"), java.nio.file.StandardCopyOption.REPLACE_EXISTING)
        }
        assertThat(finished).describedAs(text).isTrue()
        assertThat(process.exitValue()).describedAs(text).isZero()
        assertThat(text).doesNotContain("OutOfMemoryError")
        assertThat(text).contains("CALIBRATION_OK $mode")
        assertThat(report).exists()
        return ObjectMapper().readTree(report.toFile())
    }
}
