package com.posthog.hoglake.hydrator

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.InputFile
import java.io.IOException

/** A parquet footer that parquet-java could not decode. */
class FooterParseException(message: String, cause: Throwable) : IOException(message, cause)

/**
 * The single entry point for decoding a writer-supplied parquet footer.
 *
 * parquet-java refuses a structurally-broken footer in three different
 * ways, and only the first is a contract: a typed [IOException]; a
 * deliberate bare RuntimeException ("is not a Parquet file"); or an
 * accidental runtime exception from an unguarded assumption about an
 * optional thrift field. The fuzzer has found three of that last kind so
 * far, all in the same decode pass and all from hostile-but-parseable
 * thrift:
 *
 *  - NPE in shaded thrift's `TCompactProtocol.readBinary`, wrapping a
 *    null buffer while skipping an unknown binary field;
 *  - NPE in `ParquetMetadataConverter.getPath` on a ColumnChunk whose
 *    `meta_data` is absent;
 *  - NoSuchElementException in `ParquetMetadataConverter.buildChildren`,
 *    walking a SchemaElement's `num_children` off the end of the list.
 *
 * Enumerating those is a losing game — each one found was masking the
 * next — so the rule here is by CATEGORY, not by class or by stack frame:
 *
 *  - an [IOException] passes through: it means the bytes could not be
 *    READ (the hydrator's region-miss signal rides on this, and the
 *    tail-read fallback depends on seeing it);
 *  - anything else escaping the decode means parquet-java could not make
 *    sense of the bytes, which is a corrupt footer whatever class it
 *    arrived as, and becomes a [FooterParseException].
 *
 * `Error` is deliberately not caught: an OOM or a StackOverflow is not a
 * statement about the file. Nothing but parquet-java runs inside the
 * guarded block, so no hoglake bug can hide behind this.
 */
object FooterParse {
    fun parse(input: InputFile): ParquetMetadata =
        try {
            ParquetFileReader.open(input).use { it.footer }
        } catch (e: IOException) {
            throw e
        } catch (e: RuntimeException) {
            // HotSpot's OmitStackTraceInFastThrow strips the frames AND the
            // message off a repeatedly-thrown implicit exception, so the
            // cause alone can carry nothing at all; name the file and the
            // class here so the refusal stays diagnosable either way.
            throw FooterParseException(
                "corrupt parquet footer in $input: parquet-java escaped with " +
                    "${e.javaClass.name}: ${e.message ?: "<no message>"}",
                e,
            )
        }
}
