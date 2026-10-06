package com.posthog.hoglake

import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.conf.HadoopParquetConfiguration
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.InputFile

internal object ParquetReaders {
    private val configuration = HadoopParquetConfiguration()

    fun open(input: InputFile): ParquetFileReader =
        ParquetFileReader.open(input, ParquetReadOptions.builder(configuration).build())
}
