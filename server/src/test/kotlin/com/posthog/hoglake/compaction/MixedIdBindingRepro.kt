package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.hydrator.FooterParse
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.io.SeekableInputStream
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import java.io.EOFException
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

/**
 * The mixed-id binding probe.
 *
 * A file where ONE field carries the catalog column's field id and a
 * DIFFERENT, id-less field carries its NAME. Field ids are the binding
 * contract, so the id must win — and it did until the two surfaces were
 * unified behind a single-pass `firstOrNull { bindsTo(..) }`, which is
 * first-match-wins by POSITION.
 *
 * Both surfaces then read the wrong column, identically, which is
 * exactly the shape an agreement oracle cannot see.
 */
object MixedIdBindingRepro {
    private const val ID_VALUE = 42L
    private const val NAME_VALUE = 999L

    @JvmStatic
    fun main(args: Array<String>) {
        val tmp = Files.createTempDirectory("hoglake-mixed-id")
        println(run(tmp))
    }

    /** A one-line verdict; `reader=42 rewriter=42` is correct. */
    fun run(tmp: Path): String {
        // Catalog: ONE column, field id 1, named "b".
        val catalog = listOf(CatalogColumn(1, "b", ColType.LONG, null))
        val live = listOf(Column(1, 0, ColumnDef("b", ColType.LONG)))

        // File: "b" FIRST and id-less, then "x" carrying field id 1.
        // Name says the first, id says the second. The id wins.
        val schema =
            MessageType(
                "m",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named("b"),
                    Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(1).named("x"),
                ),
            )
        val src = tmp.resolve("mixed.parquet")
        val factory = SimpleGroupFactory(schema)
        ExampleParquetWriter.builder(LocalOutputFile(src))
            .withType(schema)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
            .build()
            .use { w ->
                val g = factory.newGroup()
                g.add(0, NAME_VALUE)
                g.add(1, ID_VALUE)
                w.write(g)
            }

        // READER: which column's bounds land under field id 1?
        val footer = FooterParse.parse(bytesFile(src))
        val agg = FooterStats.aggregate(footer, catalog, src.toString()).singleOrNull()
        val readBound = agg?.lowerBound?.let { longLe(it) }

        // REWRITER: which column's VALUES land in output field id 1?
        val out = tmp.resolve("mixed-out.parquet")
        val written =
            try {
                ParquetRewriter.rewrite(listOf(ParquetRewriter.Input(src, 0L, null)), live, emptyList(), out)
                readFirstLong(out)
            } catch (t: Throwable) {
                "${t.javaClass.simpleName}: ${t.message?.take(90)}"
            }

        val correct = readBound == ID_VALUE && written == ID_VALUE
        return "${if (correct) "CORRECT" else "WRONG COLUMN"}  " +
            "reader bound=$readBound rewriter wrote=$written " +
            "(field id 1 holds $ID_VALUE; the id-less field named 'b' holds $NAME_VALUE)"
    }

    private fun readFirstLong(path: Path): Any? {
        val footer = FooterParse.parse(bytesFile(path))
        val schema = footer.fileMetaData.schema
        val reader = org.apache.parquet.hadoop.ParquetFileReader.open(LocalInputFile(path))
        return reader.use { r ->
            val columnIO = org.apache.parquet.io.ColumnIOFactory().getColumnIO(schema)
            val pages = r.readNextRowGroup() ?: return@use null
            val rr =
                columnIO.getRecordReader(
                    pages,
                    org.apache.parquet.example.data.simple.convert.GroupRecordConverter(schema),
                )
            val g = rr.read()
            val idx = schema.fields.indexOfFirst { it.id?.intValue() == 1 }
            if (idx < 0 || g.getFieldRepetitionCount(idx) == 0) null else g.getLong(idx, 0)
        }
    }

    private fun longLe(raw: ByteArray): Long = ByteBuffer.wrap(raw).order(java.nio.ByteOrder.LITTLE_ENDIAN).long

    private fun bytesFile(path: Path): InputFile {
        val bytes = Files.readAllBytes(path)
        return object : InputFile {
            override fun getLength(): Long = bytes.size.toLong()

            override fun newStream(): SeekableInputStream =
                object : org.apache.parquet.io.DelegatingSeekableInputStream(
                    java.io.ByteArrayInputStream(bytes),
                ) {
                    private var pos = 0L

                    override fun getPos(): Long = pos

                    override fun seek(newPos: Long) {
                        stream.reset()
                        stream.skip(newPos)
                        pos = newPos
                    }

                    override fun read(): Int {
                        val v = super.read()
                        if (v >= 0) pos++
                        return v
                    }

                    override fun read(
                        b: ByteArray,
                        off: Int,
                        len: Int,
                    ): Int {
                        val n = super.read(b, off, len)
                        if (n > 0) pos += n
                        return n
                    }

                    override fun readFully(b: ByteArray) {
                        var read = 0
                        while (read < b.size) {
                            val n = super.read(b, read, b.size - read)
                            if (n < 0) throw EOFException()
                            read += n
                        }
                        pos += b.size
                    }
                }
        }
    }
}
