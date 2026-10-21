package com.posthog.hoglake.compaction

import com.posthog.hoglake.compaction.ParquetRewriter.Input
import com.posthog.hoglake.compaction.ParquetRewriter.Row
import com.posthog.hoglake.compaction.ParquetRewriter.SortKeys
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.model.SortFieldDef
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.Type
import java.nio.ByteBuffer

/**
 * The sorted rewrite's PRE-PASS (hoglake#134, package C): is this input
 * file already in merge-key order? A file that is becomes a run read in
 * place and is never spilled; one that is not takes the spill path. See
 * [ParquetRewriter]'s class doc for where it sits.
 *
 * # What is checked
 *
 * Every PHYSICAL row, survivors or not: a deletion vector only removes
 * rows, so a file whose every row is in order has its survivors in order,
 * and checking the survivors would mean decoding the DV first for no
 * gain. Consecutive rows must be nondecreasing under the live spec's
 * directions and null orders, then by row id — positional for a client
 * file (so key order is the whole check: its row ids increase with the
 * position), the row-id carrier for an `explicit_row_ids` file, whose
 * ties must also be in row-id order because the merge orders ties by row
 * id and a run is read in file order.
 *
 * Keys are resolved in the INPUT's own schema by field id, struct leaves
 * included (the resolution [ParquetRewriter.sortKeyPath] does on the
 * output schema), and compared in the input's own physical domain with
 * the same extraction ([SortKeys]). That is equivalent to comparing in
 * the output domain because the only conversions the rewriter makes are
 * the promotions int32→int64 and float→double (and uint32→int64
 * zero-extension), which preserve order — NaN and -0.0 included — and
 * every other column is copied verbatim. A sort column the file does not
 * have is null in every row, which is sorted under either null order.
 *
 * Unverifiable, and therefore spilled: a file with any node lacking a
 * field id (those columns bind to the live schema by NAME, which this
 * check does not follow — an id-only lookup would see "missing, so null"
 * where the rewrite sees values), and a file carrying a sort field's id
 * somewhere a key cannot be read from (under a list or a map, or on a
 * container). Neither is an error; the spill path then reports any shape
 * the rewrite cannot take, exactly as it did before this pre-pass.
 *
 * # What is read
 *
 * The reader is asked for a PROJECTED schema holding only the key leaves
 * (and the carrier when needed), so `readNextRowGroup()` reads only those
 * column chunks; the first violation returns at once, and no further row
 * group is read. A file with no key column present and no carrier to
 * check is sorted without reading a row group at all.
 */
internal object SortednessCheck {
    /**
     * Whether every physical row of the file [reader] has open is in
     * merge-key order for [sortFields]. [input] supplies the label and
     * the `explicit_row_ids` registration; a file that contradicts its
     * registration is refused here exactly as the rewrite would refuse it
     * ([InvalidDataException]).
     */
    fun isSorted(
        input: Input,
        reader: ParquetFileReader,
        sortFields: List<SortFieldDef>,
    ): Boolean {
        val schema = reader.footer.fileMetaData.schema
        if (FooterStats.missingFieldIds(schema)) return false
        val present = ArrayList<SortFieldDef>()
        val paths = ArrayList<List<Int>>()
        for (field in sortFields) {
            val path =
                try {
                    ParquetRewriter.sortKeyPath(schema, field).path
                } catch (e: UnconvertibleSchemaException) {
                    // Not a readable key here. Absent everywhere is a null
                    // key; present where no key can be read is a shape the
                    // rewrite has to judge, so it takes the spill path.
                    if (carriesId(schema, field.sourceFieldId)) return false
                    continue
                }
            present += field
            paths += path
        }
        val carrier = ParquetRewriter.rowIdCarrier(schema, input)
        if (carrier != null) paths += listOf(carrier)
        if (paths.isEmpty()) return true

        val projection = MessageType(schema.name, project(schema, paths, emptyList()))
        val keys =
            try {
                SortKeys(projection, present)
            } catch (e: IllegalArgumentException) {
                // A physical type with no sort order (INT96): the rewrite
                // refuses it; this check has nothing to say about it.
                return false
            }
        val carrierIndex = if (carrier == null) null else projection.getFieldIndex(ParquetRewriter.ROW_ID_COLUMN)
        reader.setRequestedSchema(projection)
        val columnIO = ColumnIOFactory().getColumnIO(projection)
        var previous: Row? = null
        var position = 0L
        while (true) {
            val pages = reader.readNextRowGroup() ?: return true
            val records = columnIO.getRecordReader(pages, GroupRecordConverter(projection))
            // A Long count, not toIntExact: a row group past 2^31 rows is
            // legal parquet, and an ArithmeticException here would fail the
            // group every sweep rather than check it.
            var left = pages.rowCount
            while (left > 0) {
                left--
                val group = records.read()
                val rowId =
                    if (carrierIndex == null) {
                        position
                    } else if (group.getFieldRepetitionCount(carrierIndex) == 0) {
                        // A null carrier is invalid data the rewrite will
                        // refuse with its row number; not a run to trust.
                        return false
                    } else {
                        group.getLong(carrierIndex, 0)
                    }
                position++
                val row = Row(group, rowId, keys.extract(group, input.label))
                if (previous != null && keys.comparator.compare(previous!!, row) > 0) return false
                previous = row
            }
        }
    }

    /**
     * The fields of [group] pruned to the [paths] (index chains into it),
     * each kept at its own repetition, annotation and field id, and in
     * schema order — the shape `setRequestedSchema` expects.
     */
    private fun project(
        group: GroupType,
        paths: List<List<Int>>,
        prefix: List<Int>,
    ): List<Type> {
        val depth = prefix.size
        val here = paths.filter { it.size > depth }.map { it[depth] }.distinct().sorted()
        return here.map { index ->
            val type = group.getType(index)
            val deeper = paths.filter { it.size > depth + 1 && it[depth] == index }
            if (deeper.isEmpty()) {
                type
            } else {
                type.asGroupType().withNewFields(
                    project(type.asGroupType(), deeper, prefix + index),
                )
            }
        }
    }

    /** Whether any node anywhere under [group] carries [fieldId]. */
    private fun carriesId(
        group: GroupType,
        fieldId: Long,
    ): Boolean =
        group.fields.any { type ->
            type.id?.intValue()?.toLong() == fieldId || (!type.isPrimitive && carriesId(type.asGroupType(), fieldId))
        }
}

/**
 * An [InputFile] that counts the bytes its streams deliver: what the
 * pre-pass read, for `hoglake_compaction_sort_check_bytes_total`. Bytes
 * DELIVERED to the reader, not fetched — an `S3InputFile` may fetch up to
 * one readahead more per non-adjacent read.
 */
internal class ByteCountingInputFile(private val delegate: InputFile) : InputFile {
    @Volatile
    var bytesRead = 0L
        private set

    /**
     * Read calls that delivered bytes. parquet-java reads each run of
     * adjacent column chunks with one call, and `S3InputFile` serves a
     * call its buffer does not cover with one ranged GET, so for the
     * pre-pass this is about the GETs it cost.
     */
    @Volatile
    var reads = 0L
        private set

    override fun getLength(): Long = delegate.length

    override fun newStream(): SeekableInputStream = Counted(delegate.newStream())

    override fun toString(): String = delegate.toString()

    private inner class Counted(private val d: SeekableInputStream) : SeekableInputStream() {
        private fun count(n: Int) {
            if (n > 0) {
                bytesRead += n
                reads++
            }
        }

        override fun getPos(): Long = d.pos

        override fun seek(newPos: Long) = d.seek(newPos)

        override fun read(): Int = d.read().also { if (it >= 0) count(1) }

        override fun read(
            b: ByteArray,
            off: Int,
            len: Int,
        ): Int = d.read(b, off, len).also { count(it) }

        override fun readFully(bytes: ByteArray) {
            d.readFully(bytes)
            count(bytes.size)
        }

        override fun readFully(
            bytes: ByteArray,
            start: Int,
            len: Int,
        ) {
            d.readFully(bytes, start, len)
            count(len)
        }

        override fun read(buf: ByteBuffer): Int = d.read(buf).also { count(it) }

        override fun readFully(buf: ByteBuffer) {
            val before = buf.remaining()
            d.readFully(buf)
            count(before - buf.remaining())
        }

        override fun close() = d.close()
    }
}
