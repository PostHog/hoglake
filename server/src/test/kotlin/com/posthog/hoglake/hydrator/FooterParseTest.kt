package com.posthog.hoglake.hydrator

import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer

/**
 * Pinned regressions for the footer-parse contract, on the EXACT fuzzer
 * inputs that found each defect (fuzzing.md's promotion rule: a finding
 * becomes a corpus entry AND a pinned test, not just a corpus replay).
 *
 * Both crashing inputs make parquet-java escape with a raw
 * NullPointerException from a missing null check on an optional thrift
 * field. [FooterParse] must turn each into a [FooterParseException] so the
 * hydrator sees a refusal; the deliberate refusals must pass through
 * untouched.
 */
class FooterParseTest {
    private fun corpus(name: String): ByteArray =
        checkNotNull(
            javaClass.classLoader.getResourceAsStream(
                "com/posthog/hoglake/fuzz/ParquetFooterFuzzTestInputs/" +
                    "footerParseFailsTypedAndStatsAreTotal/$name",
            ),
        ) { "fuzz corpus entry $name is missing" }.use { it.readBytes() }

    @Test
    fun `the thrift readBinary input is refused, however parquet-java chooses to`() {
        // crash-3668a338… (nightly fuzz run 34670455393, #15). Under
        // parquet-hadoop 1.17.1 shaded thrift's TCompactProtocol.readBinary
        // wrapped a null buffer and this arrived as an NPE for FooterParse
        // to translate; 1.18.1 hits thrift's message-size limit first and
        // refuses it as an IOException itself.
        //
        // So the assertion is the CONTRACT, not the shape: this input is
        // refused with an IOException and nothing else. Pinning the NPE
        // made the test fail on an upstream FIX, which is the wrong thing
        // to be told — the fuzz target already reports any escape that is
        // not an IOException, so shape changes need no test edit.
        assertThatThrownBy { FooterParse.parse(Bytes(corpus(THRIFT_NPE_INPUT))) }
            .isInstanceOf(IOException::class.java)
    }

    @Test
    fun `column chunk without meta_data becomes a typed refusal`() {
        // crash-3d080bac…: ParquetMetadataConverter.getPath dereferences a
        // ColumnChunk whose meta_data is absent. Reachable only once stack
        // traces are preserved (-XX:-OmitStackTraceInFastThrow); before
        // that the thrift NPE masked it.
        assertThatThrownBy { FooterParse.parse(Bytes(corpus(META_DATA_NPE_INPUT))) }
            .isInstanceOf(FooterParseException::class.java)
            .hasCauseInstanceOf(NullPointerException::class.java)
            .hasMessageContaining("corrupt parquet footer")
    }

    @Test
    fun `schema children count past the end becomes a typed refusal`() {
        // crash-9a591495… : ParquetMetadataConverter.buildChildren walks a
        // SchemaElement's num_children off the end of the list. Not an NPE
        // — the reason FooterParse translates by category and not by class.
        assertThatThrownBy { FooterParse.parse(Bytes(corpus(CHILDREN_INPUT))) }
            .isInstanceOf(FooterParseException::class.java)
            .hasCauseInstanceOf(NoSuchElementException::class.java)
    }

    @Test
    fun `parquet's own bare refusal is translated but keeps its message`() {
        // "is not a Parquet file" is deliberate upstream, but it arrives as
        // a bare RuntimeException; callers get one category and the
        // original text.
        assertThatThrownBy { FooterParse.parse(Bytes(corpus("footer_truncated"))) }
            .isInstanceOf(FooterParseException::class.java)
            .hasMessageContaining("is not a Parquet file")
    }

    @Test
    fun `a good footer still parses`() {
        val footer = FooterParse.parse(Bytes(corpus("footer_with_field_ids")))
        assertThat(footer.fileMetaData.schema.fields).isNotEmpty()
    }

    /** Minimal in-memory [InputFile] over a corpus entry. */
    private class Bytes(private val bytes: ByteArray) : InputFile {
        override fun getLength(): Long = bytes.size.toLong()

        override fun newStream(): SeekableInputStream =
            object : SeekableInputStream() {
                private var pos = 0L

                override fun getPos(): Long = pos

                override fun seek(newPos: Long) {
                    pos = newPos
                }

                override fun read(): Int {
                    if (pos >= bytes.size) return -1
                    return (bytes[at(1)].toInt() and 0xFF).also { pos += 1 }
                }

                override fun read(
                    b: ByteArray,
                    off: Int,
                    len: Int,
                ): Int {
                    if (len == 0) return 0
                    if (pos >= bytes.size) return -1
                    val n = minOf(len.toLong(), bytes.size - pos).toInt()
                    System.arraycopy(bytes, at(n), b, off, n)
                    pos += n
                    return n
                }

                override fun readFully(b: ByteArray) = readFully(b, 0, b.size)

                override fun readFully(
                    b: ByteArray,
                    start: Int,
                    len: Int,
                ) {
                    System.arraycopy(bytes, at(len), b, start, len)
                    pos += len
                }

                override fun read(buf: ByteBuffer): Int {
                    val len = buf.remaining()
                    if (len == 0) return 0
                    if (pos >= bytes.size) return -1
                    val n = minOf(len.toLong(), bytes.size - pos).toInt()
                    buf.put(bytes, at(n), n)
                    pos += n
                    return n
                }

                override fun readFully(buf: ByteBuffer) {
                    val len = buf.remaining()
                    buf.put(bytes, at(len), len)
                    pos += len
                }

                private fun at(len: Int): Int {
                    if (pos < 0 || pos + len > bytes.size) {
                        throw EOFException("range [$pos, +$len) outside ${bytes.size} bytes")
                    }
                    return Math.toIntExact(pos)
                }
            }
    }

    private companion object {
        const val THRIFT_NPE_INPUT = "crash-3668a338699d3120815befa8ce99cc9df3f22dde"
        const val META_DATA_NPE_INPUT = "crash-3d080bacd1d81ea9f7ef93969e81ff31091491a5"
        const val CHILDREN_INPUT = "crash-9a5914956fb00dea1da98b8feaa7a9ed8c44990d"
    }
}
