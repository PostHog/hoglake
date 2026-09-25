package com.posthog.hoglake.testing

import org.apache.parquet.format.Util
import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Each row group's first-column-chunk start, computed from a parquet
 * file's RAW thrift footer (parquet-format's own `FileMetaData`, decoded
 * straight off the file's tail) — not from parquet-java's converted
 * `ParquetMetadata`, and not through the code under test — so a
 * split_offsets test compares the server's list against an independent
 * reading of the bytes.
 *
 * The rule is the thrift-level statement of `getStartingPos()`: the
 * chunk's `dictionary_page_offset` when it is set, positive and below
 * `data_page_offset`, else `data_page_offset`. [dictionaryFirst] counts
 * the row groups that took the dictionary arm, so a test can prove it
 * exercised that arm rather than assume it.
 */
data class ThriftRowGroupStarts(val offsets: List<Long>, val dictionaryFirst: Int) {
    companion object {
        fun of(bytes: ByteArray): ThriftRowGroupStarts {
            val footerLength = ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int
            val footerStart = bytes.size - 8 - footerLength
            val meta = Util.readFileMetaData(ByteArrayInputStream(bytes, footerStart, footerLength))
            var dictionaryFirst = 0
            val offsets =
                meta.row_groups.map { rg ->
                    val md = rg.columns[0].meta_data
                    val data = md.data_page_offset
                    if (md.isSetDictionary_page_offset && md.dictionary_page_offset > 0 &&
                        md.dictionary_page_offset < data
                    ) {
                        dictionaryFirst++
                        md.dictionary_page_offset
                    } else {
                        data
                    }
                }
            return ThriftRowGroupStarts(offsets, dictionaryFirst)
        }
    }
}
