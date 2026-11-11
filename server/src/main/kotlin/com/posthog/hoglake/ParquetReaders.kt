package com.posthog.hoglake

import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.conf.HadoopParquetConfiguration
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream

internal object ParquetReaders {
    private val configuration = HadoopParquetConfiguration()

    fun open(input: InputFile): ParquetFileReader =
        ParquetFileReader.open(input, ParquetReadOptions.builder(configuration).build())

    /**
     * A reader over [input] whose [footer] the caller has already read,
     * reading data through [stream]: parquet-java fetches no footer, so a
     * caller that opened the file once for its footer pays no second tail
     * read. The reader owns [stream] and closes it.
     */
    fun open(
        input: InputFile,
        footer: ParquetMetadata,
        stream: SeekableInputStream,
    ): ParquetFileReader = ParquetFileReader(input, footer, ParquetReadOptions.builder(configuration).build(), stream)
}
