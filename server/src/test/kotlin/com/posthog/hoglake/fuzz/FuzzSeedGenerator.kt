package com.posthog.hoglake.fuzz

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.api.CommitRequestDto
import com.posthog.hoglake.compaction.PuffinTestFiles
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.ReplacementTarget
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationDefinitionCodec
import com.posthog.hoglake.wireObjectMapper
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import java.nio.file.Files
import java.nio.file.Path
import java.util.Random
import java.util.UUID

/**
 * One-shot generator for the committed fuzz seed corpus
 * (`./gradlew generateFuzzSeeds` — see build.gradle.kts). Writes into
 * src/test/resources/com/posthog/hoglake/fuzz/<Target>Inputs/<method>/,
 * jazzer-junit's inputs convention: the normal :test run replays every
 * file deterministically, and fuzzing mode uses them as the seed corpus.
 *
 * Sources:
 *  - the cross-language vector file (pyhoglake/tests/vectors/
 *    bounds_vectors.json, READ-ONLY here) for the codec targets — a
 *    fuzzer-found nasty value travels the other way: corpus first, then
 *    promotion into the vector file by the Python side (docs/fuzzing.md);
 *  - real parquet files written via parquet-java (the hydrator's own
 *    library) for the footer target;
 *  - spec-conformant puffin DV blobs (PuffinTestFiles) plus corrupted
 *    variants for the DV target;
 *  - representative wire bodies for the identifier and DTO targets.
 *
 * Regeneration is deterministic except for parquet writer metadata
 * (created_by timestamps do not exist in the footer, so outputs are
 * stable in practice); diffs after a rerun should be reviewed, not
 * assumed.
 */
object FuzzSeedGenerator {
    @JvmStatic
    fun main(args: Array<String>) {
        require(args.size == 2) { "usage: FuzzSeedGenerator <corpus-root> <bounds_vectors.json>" }
        val root = Path.of(args[0])
        val vectorFile = Path.of(args[1])

        seedDecodeTarget(root, vectorFile)
        seedBoundWireTarget(root, vectorFile)
        seedCompareTarget(root, vectorFile)
        seedFooterTarget(root)
        seedPuffinTarget(root)
        seedIdentifiersTarget(root)
        seedNestedAgreementTarget(root)
        seedDtoTarget(root)
        seedTableCreationCodecTarget(root)
        seedCommitReceiptTarget(root)

        println("fuzz seed corpus written under $root")
    }

    private fun dir(
        root: Path,
        target: String,
        method: String,
    ): Path = Files.createDirectories(root.resolve("${target}Inputs").resolve(method))

    private fun write(
        dir: Path,
        name: String,
        bytes: ByteArray,
    ) {
        Files.write(dir.resolve(name), bytes)
    }

    // ---- IcebergSingleValue decode: type byte + vector-file encodings ------

    private fun seedDecodeTarget(
        root: Path,
        vectorFile: Path,
    ) {
        val out = dir(root, "IcebergSingleValueDecodeFuzzTest", "decodeIsTotalAndRoundTrips")
        for ((i, v) in readVectors(vectorFile).withIndex()) {
            val type = ColType.fromWire(v.type)
            write(out, "vector_%02d_%s".format(i, v.type), byteArrayOf(type.ordinal.toByte()) + v.bytes)
        }
    }

    // ---- BoundWire render: type byte + scale byte + vector encodings -------

    /**
     * Same vectors as the decode target, one byte wider: byte 1 is the
     * decimal scale (BoundWireFuzzTest's input shape), taken from the
     * vector's type_params so decimal seeds start at their real scale.
     */
    private fun seedBoundWireTarget(
        root: Path,
        vectorFile: Path,
    ) {
        val out = dir(root, "BoundWireFuzzTest", "renderIsTotalOrRefusesTyped")
        for ((i, v) in readVectors(vectorFile).withIndex()) {
            val type = ColType.fromWire(v.type)
            write(
                out,
                "vector_%02d_%s".format(i, v.type),
                byteArrayOf(type.ordinal.toByte(), v.scale.toByte()) + v.bytes,
            )
        }
    }

    // ---- compareValues: raw vector bytes as FuzzedDataProvider fodder ------

    private fun seedCompareTarget(
        root: Path,
        vectorFile: Path,
    ) {
        val out = dir(root, "IcebergSingleValueCompareFuzzTest", "comparatorConsistency")
        val all = readVectors(vectorFile)
        // Concatenated encodings make decent FuzzedDataProvider material:
        // the provider slices ints/longs/bytes straight off them.
        for (chunk in all.chunked(8).withIndex()) {
            write(
                out,
                "vectors_%02d".format(chunk.index),
                chunk.value.fold(ByteArray(0)) { acc, v -> acc + v.bytes },
            )
        }
    }

    // ---- parquet footers ----------------------------------------------------

    private fun seedFooterTarget(root: Path) {
        val out = dir(root, "ParquetFooterFuzzTest", "footerParseFailsTypedAndStatsAreTotal")
        val tmp = Files.createTempDirectory("fuzz-seed-parquet")
        try {
            write(out, "footer_with_field_ids", parquetFile(tmp, "with_ids.parquet", withIds = true))
            write(out, "footer_without_field_ids", parquetFile(tmp, "no_ids.parquet", withIds = false))
            // A structurally valid file whose tail the fuzzer can mutate into
            // the truncated/oversized-footer-length class cheaply.
            val whole = parquetFile(tmp, "trunc.parquet", withIds = true)
            write(out, "footer_truncated", whole.copyOfRange(0, whole.size - 5))
        } finally {
            tmp.toFile().deleteRecursively()
        }
    }

    /** A tiny real parquet file covering the stats decode arms FooterStats exercises. */
    private fun parquetFile(
        tmp: Path,
        name: String,
        withIds: Boolean,
    ): ByteArray {
        fun <T : Types.Builder<T, out org.apache.parquet.schema.Type>> T.maybeId(id: Int): T =
            if (withIds) this.id(id) else this

        val schema: MessageType =
            Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT32).maybeId(2).named("c_int"))
                .addField(Types.required(PrimitiveTypeName.INT64).maybeId(3).named("c_long"))
                .addField(Types.optional(PrimitiveTypeName.DOUBLE).maybeId(5).named("c_double"))
                .addField(
                    Types.optional(PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.stringType()).maybeId(11).named("c_string"),
                )
                .named("fuzz_seed")
        val path = tmp.resolve(name)
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(path))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
            .use { w ->
                for (i in 0 until 3) {
                    val g = factory.newGroup()
                    g.add("c_int", i)
                    g.add("c_long", i * 1_000_000_007L)
                    if (i != 1) g.add("c_double", i * 2.5)
                    g.add("c_string", "row-$i")
                    w.write(g)
                }
            }
        return Files.readAllBytes(path)
    }

    // ---- puffin deletion vectors ---------------------------------------------

    private fun seedPuffinTarget(root: Path) {
        val out = dir(root, "PuffinDeletionVectorFuzzTest", "readRefusesLoudlyOrDecodesDeterministically")
        write(out, "dv_empty", PuffinTestFiles.deletionVector(emptyList()))
        write(out, "dv_small", PuffinTestFiles.deletionVector(listOf(0L, 1L, 2L, 41L)))
        write(
            out,
            "dv_high_buckets",
            PuffinTestFiles.deletionVector(listOf(7L, (1L shl 32) + 3, (5L shl 32) + 9)),
        )
        // Same valid file with one vector byte flipped: lands exactly on the
        // CRC-mismatch refusal, a boundary the fuzzer should start from.
        val corrupt = PuffinTestFiles.deletionVector(listOf(0L, 1L, 2L, 41L))
        corrupt[10] = (corrupt[10].toInt() xor 0x01).toByte()
        write(out, "dv_corrupt_crc", corrupt)
    }

    // ---- identifiers ----------------------------------------------------------

    private fun seedIdentifiersTarget(root: Path) {
        val out = dir(root, "IdentifiersFuzzTest", "identifierDecisionsAreStableAndTyped")
        val samples =
            mapOf(
                "valid_simple" to "events",
                "valid_boundary_128" to "_" + "a".repeat(127),
                "invalid_too_long" to "a".repeat(129),
                "invalid_leading_digit" to "0table",
                "invalid_slash" to "ns/table",
                "invalid_unicode" to "tåble🦔",
                "invalid_control" to "tab le\n",
                "request_id_shape" to "req-1.2_3",
            )
        for ((name, value) in samples) {
            write(out, name, value.toByteArray(Charsets.UTF_8))
        }
    }

    // ---- nested reader/rewriter agreement --------------------------------------

    /**
     * Seeds for the nested agreement target.
     *
     * Unlike the codec targets there is no vector file to draw from:
     * this target's input is pure ENTROPY, consumed by
     * [NestedFuzz]'s generator to choose a column tree, a schema
     * mutation and a data shape. So the corpus is a spread of blob
     * LENGTHS and byte patterns — the thing that actually varies which
     * branches the generator takes — rather than meaningful values.
     *
     * The short and degenerate blobs matter most: a FuzzedDataProvider
     * that runs out of bytes falls back to a fixed RNG, so an
     * eight-byte seed exercises a different half of the generator than a
     * four-kilobyte one. All-zero and all-0xFF pin the two ends of every
     * `consumeInt(lo, hi)` range at once.
     *
     * Campaign crashes are committed alongside these under their own
     * names; jazzer-junit replays every file in the directory.
     */
    private fun seedNestedAgreementTarget(root: Path) {
        val out = dir(root, "NestedAgreementFuzzTest", "readerAndRewriterAgree")
        write(out, "empty", ByteArray(0))
        write(out, "zeros_8", ByteArray(8))
        write(out, "zeros_256", ByteArray(256))
        write(out, "ones_64", ByteArray(64) { 0xFF.toByte() })
        write(out, "alternating_128", ByteArray(128) { if (it % 2 == 0) 0x00 else 0xFF.toByte() })
        write(out, "ascending_256", ByteArray(256) { it.toByte() })
        // A fixed seed, so a regeneration produces the same bytes and an
        // unexpected corpus diff means something really changed.
        val rng = java.util.Random(0x4E45_5354L)
        for (size in listOf(16, 64, 512, 4096)) {
            write(out, "random_$size", ByteArray(size).also { rng.nextBytes(it) })
        }
    }

    // ---- wire DTOs -------------------------------------------------------------

    private fun seedDtoTarget(root: Path) {
        val out = dir(root, "WireDtoParseFuzzTest", "wireParseFailsOnlyWithMappedExceptions")
        val samples =
            mapOf(
                "commit_full" to
                    """{"read_snapshot":7,"appends":[{"namespace":"ns","table":"t","files":[
                       {"path":"s3://b/f.parquet","record_count":10,"file_size_bytes":1024,
                        "footer_size":256,"column_stats":[{"field_id":1,"value_count":10,
                        "null_count":0,"lower_bound":"AAAAAA==","upper_bound":"/////w=="}],
                        "partition_values":["2026-01-01",null]}],
                       "expected_table_uuid":"123e4567-e89b-12d3-a456-426614174000"}],
                       "deletes":[],"author":"fuzz","message":"seed"}""",
                "commit_deletes" to
                    """{"deletes":[{"namespace":"ns","table":"t","files":[
                       {"data_file_id":5,"path":"s3://b/dv.puffin","delete_count":3,
                        "file_size_bytes":99}]}]}""",
                "commit_empty" to """{}""",
                // A footer-shipping registration's split_offsets: one the
                // contract accepts, and one it refuses (unsorted, and the
                // last entry at file_size_bytes).
                "commit_split_offsets" to
                    """{"appends":[{"namespace":"ns","table":"t","files":[
                       {"path":"s3://b/f.parquet","record_count":10,"file_size_bytes":1024,
                        "footer_size":256,"split_offsets":[4,300,700]}]}]}""",
                "commit_split_offsets_invalid" to
                    """{"appends":[{"namespace":"ns","table":"t","files":[
                       {"path":"s3://b/f.parquet","record_count":10,"file_size_bytes":1024,
                        "split_offsets":[700,300,1024]}]}]}""",
                "alter_all_ops" to
                    """{"ops":[
                       {"op":"add_column","column":{"name":"c1","type":"long"}},
                       {"op":"drop_column","name":"c2"},
                       {"op":"rename_column","from":"a","to":"b"},
                       {"op":"promote_column","name":"c3","to":"double"},
                       {"op":"rename_table","new_name":"t2"},
                       {"op":"set_partition_spec","fields":[
                          {"source_field_id":1,"transform":"day"}]},
                       {"op":"set_sort_order","sort_fields":[
                          {"source_field_id":1,"direction":"asc","null_order":"nulls_last"}]}]}""",
                "alter_unknown_op" to """{"ops":[{"op":"explode"}]}""",
                // int128, not uint64: uint64 is a real type now. This seed
                // must stay a name the parser REJECTS, and a permanently
                // refused one exercises the named-refusal branch too.
                "alter_bad_enum" to """{"ops":[{"op":"add_column","column":{"name":"c","type":"int128"}}]}""",
                "not_json" to "PAR1 ",
                // The two-phase table creation and the upload claims
                // (#156-#162): shapes the DTO target now parses AND
                // reserializes.
                "publish_table_creation" to
                    """{"files":[{"path":"s3://b/created.parquet","record_count":3,"file_size_bytes":300,
                       "footer_size":64,"column_stats":[{"field_id":1,"value_count":3,"null_count":0,
                       "lower_bound":"AAAAAA==","upper_bound":"/////w=="}]}]}""",
                "claim_upload" to
                    """{"owner":"123e4567-e89b-12d3-a456-426614174000","prefix":"data/",
                       "file_kind":"data"}""",
                "upload_owner" to """{"owner":"123e4567-e89b-12d3-a456-426614174000"}""",
                "abandon_uploads" to
                    """{"owner":"123e4567-e89b-12d3-a456-426614174000",
                       "paths":["s3://b/x.parquet","s3://b/y.parquet"]}""",
                // The guarded-DML fields on a commit body, in one seed.
                "commit_guarded_dml" to
                    """{"read_snapshot":9,"idempotency_key":"123e4567-e89b-12d3-a456-426614174008",
                       "appends":[{"namespace":"ns","table":"t",
                       "expected_table_uuid":"123e4567-e89b-12d3-a456-426614174009",
                       "files":[{"path":"s3://b/g.parquet","record_count":1,"file_size_bytes":10}]}],
                       "deletes":[{"namespace":"ns","table":"t",
                       "expected_table_uuid":"123e4567-e89b-12d3-a456-426614174009",
                       "files":[{"data_file_id":0,"data_file_path":"s3://b/g.parquet",
                       "path":"s3://b/g.dv","delete_count":1,"file_size_bytes":20}]}]}""",
            ) + scanStatsSamples()
        for ((name, value) in samples) {
            write(out, name, value.trimIndent().toByteArray(Charsets.UTF_8))
        }
    }

    /**
     * GET .../scan's `include` / `stats_fields` parsers
     * (api/Routes.kt), which the DTO target reaches by splitting its
     * input at the first NUL: everything before it is `include`,
     * everything after is `stats_fields`, and an input with no NUL is
     * an `include` alone.
     *
     * Before these seeds the block ran on every committed input and
     * `parseStatsFields` never executed once in PR CI — no seed
     * contained a NUL, so the second half was always empty. The names
     * say which half each one exercises.
     */
    private fun scanStatsSamples(): Map<String, String> {
        val nul = '\u0000'
        return mapOf(
            // include alone, no NUL: the accepted spelling, and the
            // shapes the parser has to separate from it.
            "scan_include_column_stats" to "column_stats",
            "scan_include_split_offsets" to "split_offsets",
            "scan_include_both" to "column_stats,split_offsets${nul}3",
            // split_offsets does not license stats_fields.
            "scan_include_split_offsets_orphan_fields" to "split_offsets${nul}3",
            "scan_include_unknown" to "column_stat",
            "scan_include_repeated" to "column_stats,column_stats",
            "scan_include_trailing_comma" to "column_stats,",
            "scan_include_over_cap" to (1..17).joinToString(",") { "v$it" },
            // An EMPTY include, which is a different refusal from an
            // unknown one. The NUL is the only way to write it: an empty
            // FILE is an absent parameter, not an empty value.
            "scan_include_empty" to "$nul",
            // The NUL-separated pair: both parsers and the
            // cross-parameter rule, in one input.
            "scan_stats_fields_pair" to "column_stats${nul}3,7,3",
            "scan_stats_fields_orphan" to "${nul}3,7",
            "scan_stats_fields_noninteger" to "column_stats${nul}3,abc,7",
            "scan_stats_fields_empty_entry" to "column_stats${nul}1,,2",
            "scan_stats_fields_int64" to "column_stats$nul-9223372036854775808,9223372036854775807",
            "scan_stats_fields_over_cap" to "column_stats$nul" + (1..10_001).joinToString(","),
        )
    }

    // ---- table creation definition codec ---------------------------------------

    /**
     * One seed per stored format version, 0 through 6.
     *
     * Versions 1-6 are produced BY the codec, so a seed can never claim
     * a shape the encoder does not actually write; version 0 is the
     * pre-versioned receipt (JVM enum names, `typeParams`), which no
     * encoder emits any more and which only a hand-written fixture can
     * cover. Each seed is the receipt and nothing else: the target reads
     * the whole input as the stored row AND as its generator tape, so
     * one readable file exercises both arms.
     */
    private fun seedTableCreationCodecTarget(root: Path) {
        val out = dir(root, "TableCreationDefinitionCodecFuzzTest", "decodeIsTypedAndEncodePicksTheLowestVersion")
        val scalar = ColumnDef("id", ColType.LONG, null, false)
        val nested =
            ColumnDef(
                "payload",
                ColType.STRUCT,
                null,
                true,
                listOf(
                    ColumnDef("inner", ColType.STRING),
                    ColumnDef("n", ColType.DECIMAL, mapOf("precision" to 9, "scale" to 2)),
                ),
            )
        val base = TableCreationDefinition("ns", "t", listOf(scalar))
        val byVersion =
            mapOf(
                1 to base,
                2 to base.copy(columns = listOf(scalar, nested)),
                3 to
                    base.copy(
                        replacement =
                            ReplacementTarget(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), 12L),
                    ),
                4 to base.copy(partitionFields = listOf(PartitionFieldDef(1L, Transform.BUCKET, 16))),
                5 to
                    base.copy(
                        partitionFields = listOf(PartitionFieldDef(1L, Transform.DAY)),
                        sortFields = listOf(SortFieldDef(1L, SortDirection.DESC, NullOrder.NULLS_FIRST)),
                    ),
                6 to
                    base.copy(
                        columns = listOf(scalar.copy(comment = "the id"), nested),
                        comment = "a table",
                        properties = mapOf("owner.team" to "data"),
                    ),
            )
        for ((version, definition) in byVersion) {
            val encoded = TableCreationDefinitionCodec.encode(definition)
            val written = ObjectMapper().readTree(encoded)["version"].asInt()
            require(written == version) {
                "seed for version $version encoded as $written; the seed names a version the codec does not write"
            }
            write(out, "definition_v$version", encoded.toByteArray(Charsets.UTF_8))
        }
        // Version 0: the pre-versioned shape, JVM enum spellings and
        // `typeParams`, including the UUID_T special case decode still
        // carries for it.
        write(out, "definition_v0", V0_RECEIPT.toByteArray(Charsets.UTF_8))
        // Receipts the decode arm must refuse by NAME rather than crash:
        // truncated JSON, a version this codec does not know, and a
        // field whose KIND is wrong where a coercing reader would have
        // invented a value.
        write(out, "corrupt_truncated", """{"version":2,"namespace":"ns","name":""".toByteArray(Charsets.UTF_8))
        write(
            out,
            "corrupt_future_version",
            """{"version":97,"namespace":"ns","name":"t","columns":[]}""".toByteArray(Charsets.UTF_8),
        )
        write(
            out,
            "corrupt_type_params",
            """{"version":2,"namespace":"ns","name":"t","columns":[{"name":"c","type":"long","type_params":5}]}"""
                .toByteArray(Charsets.UTF_8),
        )
    }

    /**
     * The pre-versioned receipt, on ONE line on purpose: a multi-line
     * raw string is reformatted by ktlint, which would silently change
     * the bytes of a committed seed without anyone touching the seed.
     */
    private const val V0_RECEIPT =
        """{"namespace":"ns","name":"t","columns":[{"name":"id","typeParams":null,"type":"LONG",""" +
            """"nullable":false},{"name":"u","typeParams":null,"type":"UUID_T","nullable":true}]}"""

    // ---- stored commit receipts ---------------------------------------------------

    /**
     * The guarded-DML request shapes, in the receipt target's layout:
     * eight bytes of permutation entropy, then the body.
     *
     * These are the bodies the endpoints added for guarded DML actually
     * receive — every one of them takes a `CommitRequestDto` and differs
     * only in which fields it insists on — so the seeds are named for
     * the guard they carry rather than for the route.
     */
    private fun seedCommitReceiptTarget(root: Path) {
        val out = dir(root, "CommitReceiptFuzzTest", "commitReceiptsAreStableUnderPermutation")
        val samples =
            mapOf(
                "guarded_idempotent" to
                    """{"read_snapshot":41,"idempotency_key":"123e4567-e89b-12d3-a456-426614174000",
                       "appends":[{"namespace":"ns","table":"t",
                       "expected_table_uuid":"123e4567-e89b-12d3-a456-426614174001",
                       "files":[{"path":"s3://b/a.parquet","record_count":10,"file_size_bytes":1024,
                       "footer_size":256,"column_stats":[{"field_id":2,"value_count":10,"null_count":1},
                       {"field_id":1,"value_count":10,"null_count":0}]}]}]}""",
                "require_unchanged_tables" to
                    """{"read_snapshot":7,"idempotency_key":"123e4567-e89b-12d3-a456-426614174002",
                       "appends":[{"namespace":"ns","table":"t",
                       "expected_table_uuid":"123e4567-e89b-12d3-a456-426614174003",
                       "files":[{"path":"s3://b/b.parquet","record_count":1,"file_size_bytes":10}]}],
                       "deletes":[{"namespace":"ns","table":"t",
                       "expected_table_uuid":"123e4567-e89b-12d3-a456-426614174003",
                       "files":[{"data_file_id":9,"path":"s3://b/9.dv","delete_count":2,"file_size_bytes":40}]}]}""",
                "allow_pending_deletes" to
                    """{"read_snapshot":3,"idempotency_key":"123e4567-e89b-12d3-a456-426614174004",
                       "appends":[{"namespace":"ns","table":"t",
                       "expected_table_uuid":"123e4567-e89b-12d3-a456-426614174005",
                       "files":[{"path":"s3://b/new.parquet","record_count":2,"file_size_bytes":100}]}],
                       "deletes":[{"namespace":"ns","table":"t",
                       "expected_table_uuid":"123e4567-e89b-12d3-a456-426614174005",
                       "files":[{"data_file_id":0,"data_file_path":"s3://b/new.parquet",
                       "path":"s3://b/new.dv","delete_count":1,"file_size_bytes":30}]}]}""",
                "empty_delete_group" to
                    """{"read_snapshot":5,"idempotency_key":"123e4567-e89b-12d3-a456-426614174006",
                       "deletes":[{"namespace":"ns","table":"t",
                       "expected_table_uuid":"123e4567-e89b-12d3-a456-426614174007","files":[]}]}""",
                // Two appends, two files each, stats out of field-id
                // order: the permutation oracle has something to permute.
                "permutable_appends" to
                    """{"read_snapshot":11,"appends":[
                       {"namespace":"ns","table":"a","files":[
                        {"path":"s3://b/1.parquet","record_count":3,"file_size_bytes":30,
                         "column_stats":[{"field_id":3,"value_count":3,"null_count":0},
                                         {"field_id":1,"value_count":3,"null_count":2}]},
                        {"path":"s3://b/2.parquet","record_count":4,"file_size_bytes":40}]},
                       {"namespace":"ns","table":"b","files":[
                        {"path":"s3://b/3.parquet","record_count":5,"file_size_bytes":50,
                         "partition_values":["2026-01-01",null]}]}]}""",
                // FOUR column stats, distinct ids, out of order. Two is
                // not enough to trust: a 2-element shuffle is the
                // identity half the time, and QE proved every seed here
                // still passed with the canonicalising sort deleted
                // because the identity is what the nonces happened to
                // produce. Replay-only PR CI cannot tell those apart, so
                // the seeds have to be chosen, not taken.
                "wide_column_stats" to
                    """{"read_snapshot":13,"appends":[{"namespace":"ns","table":"t","files":[
                       {"path":"s3://b/w.parquet","record_count":8,"file_size_bytes":800,
                        "column_stats":[{"field_id":4,"value_count":8,"null_count":0},
                                        {"field_id":1,"value_count":8,"null_count":3},
                                        {"field_id":9,"value_count":8,"null_count":1},
                                        {"field_id":2,"value_count":8,"null_count":0}]}]}]}""",
                "empty" to "{}",
            )
        var nonce = 1L
        for ((name, body) in samples) {
            val json = body.trimIndent()
            nonce = permutingNonce(name, json, nonce)
            write(out, name, noncePrefix(nonce) + json.toByteArray(Charsets.UTF_8))
            nonce += 0x0101_0101L
        }
    }

    private fun noncePrefix(nonce: Long): ByteArray =
        ByteArray(COMMIT_PERMUTATION_PREFIX_BYTES) { i -> ((nonce shr (8 * (7 - i))) and 0xFF).toByte() }

    /**
     * The first nonce at or after [start] whose permutation is
     * NON-IDENTITY at every level this body can exercise.
     *
     * The permutation itself comes from [permuteCommitRequest], the
     * function the target calls, so this cannot drift from what the
     * seed will actually do at replay time. A body with nothing to
     * permute (no two appends, no two files, no two column stats)
     * accepts the first nonce, which is correct: there is no level to
     * be identity at.
     */
    private fun permutingNonce(
        name: String,
        body: String,
        start: Long,
    ): Long {
        val mapper = wireObjectMapper()
        val request = mapper.readValue<CommitRequestDto>(body).toModel(allowEmptyDeletes = true)
        var nonce = start
        repeat(MAX_NONCE_SEARCH) {
            val permuted = permuteCommitRequest(request, Random(commitPermutationSeed(noncePrefix(nonce))))
            val identity = identityLevel(request, permuted)
            if (identity == null) return nonce
            nonce++
        }
        error("no nonce within $MAX_NONCE_SEARCH of $start permutes seed '$name' at every level")
    }

    /**
     * The first ordering that came back unchanged, or null if every
     * level with something to reorder was actually reordered.
     *
     * Appends are matched by namespace/table and files by path, which
     * is why the seeds give each one a distinct name: without that, a
     * reordered list and its original could not be paired up to compare.
     */
    private fun identityLevel(
        before: CommitRequest,
        after: CommitRequest,
    ): String? {
        fun key(append: TableAppend) = "${append.namespace}.${append.table}"
        if (before.appends.size >= 2 && after.appends.map(::key) == before.appends.map(::key)) return "appends"
        val originalAppends = before.appends.associateBy(::key)
        for (append in after.appends) {
            val original = originalAppends.getValue(key(append))
            if (original.files.size >= 2 && append.files.map { it.path } == original.files.map { it.path }) {
                return "files of ${key(append)}"
            }
            val originalFiles = original.files.associateBy { it.path }
            for (file in append.files) {
                val stats = file.columnStats ?: continue
                val originalStats = originalFiles.getValue(file.path).columnStats ?: continue
                if (originalStats.size >= 2 &&
                    originalStats.map { it.fieldId }.toSet().size == originalStats.size &&
                    stats.map { it.fieldId } == originalStats.map { it.fieldId }
                ) {
                    return "column_stats of ${file.path}"
                }
            }
        }
        return null
    }

    // ---- vector file -----------------------------------------------------------

    /** A runaway search is a bug in the seed, not a reason to keep trying. */
    private const val MAX_NONCE_SEARCH = 10_000

    private data class Vector(val type: String, val bytes: ByteArray, val scale: Int)

    private fun readVectors(vectorFile: Path): List<Vector> {
        val rootNode = ObjectMapper().readTree(vectorFile.toFile())
        val vectors = rootNode.get("vectors")
        require(vectors != null && vectors.isArray) { "unexpected vector file shape in $vectorFile" }
        return vectors.map { v ->
            Vector(
                type = v.get("type").asText(),
                bytes = unhex(v.get("hex").asText()),
                scale = v.get("type_params")?.get("scale")?.asInt() ?: 0,
            )
        }
    }

    private fun unhex(hex: String): ByteArray {
        require(hex.length % 2 == 0) { "odd hex length in vector file" }
        return ByteArray(hex.length / 2) { i ->
            ((Character.digit(hex[2 * i], 16) shl 4) or Character.digit(hex[2 * i + 1], 16)).toByte()
        }
    }
}
