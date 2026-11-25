package com.posthog.hoglake.hydrator

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.s3.model.S3Exception

/**
 * [FooterFetch]'s structural refusal over the whole-object cap names why the
 * tail did not serve: no usable `footer_size`, or a tail that did not parse.
 * Held to the registration (Hoglite), a `footer_size` over the footer cap is
 * not usable; without a footer cap (PostgreSQL) any `footer_size` reads its
 * tail, as before #675. Held to the registration, an object shorter than
 * its registration and a `file_size_bytes` under the footer suffix are
 * structural, not transient. Unit: no object store is reached beyond the stubs.
 */
class FooterFetchTest {
    /** Tail starts asked of [junk]'s open-ended tail read. */
    private val tails = mutableListOf<Long>()

    /** A store whose tail reads answer junk and which must not be read whole. */
    private val junk =
        object : ObjectStore(null, "us-east-1", "k", "s", true) {
            override fun getRange(
                pathUri: String,
                startInclusive: Long,
                length: Int,
            ): ByteArray = ByteArray(length)

            override fun getTail(
                pathUri: String,
                startInclusive: Long,
            ): ByteArray = ByteArray(16).also { tails += startInclusive }

            override fun getHead(
                pathUri: String,
                maxBytes: Int,
            ): ByteArray = error("the whole object must not be read")

            override fun get(pathUri: String): ByteArray = error("the whole object must not be read")
        }

    private val fetch = FooterFetch(maxWholeObjectBytes = 100, maxFooterBytes = 50)

    @Test
    fun `a usable footer_size whose tail does not parse is named as such`() {
        // MUTATION: drop the distinction and this says the file had no usable footer_size.
        assertThatThrownBy { fetch.read(FooterFetch.Target("s3://b/f.parquet", 1000, 40), junk) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("its tail read at footer_size=40 did not parse")
            .hasMessageNotContaining("no usable footer_size")
    }

    @Test
    fun `a footer_size over the footer cap is named unusable`() {
        assertThatThrownBy { fetch.read(FooterFetch.Target("s3://b/f.parquet", 1000, 51), junk) }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("no usable footer_size (footer_size=51, footer cap 50 bytes")
            .hasMessageContaining("HOGLAKE_HOGLITE_MAX_FOOTER_BYTES")
    }

    @Test
    fun `without a footer cap any footer_size reads its tail to the object's end`() {
        // PostgreSQL's read, unchanged by #675: a 1 GiB footer_size is still tried through the tail.
        // MUTATION: default the footer cap to 64 MiB and the tail is never read: "no usable footer_size".
        val gib = 1L shl 30
        assertThatThrownBy {
            FooterFetch(maxWholeObjectBytes = 100).read(FooterFetch.Target("s3://b/f.parquet", 4 * gib, gib), junk)
        }.isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("its tail read at footer_size=$gib did not parse")
        assertThat(tails).containsExactly(3 * gib - 8)
    }

    /** A store whose ranged reads answer 416, as S3 does for a start at or past the object's end. */
    private val short =
        object : ObjectStore(null, "us-east-1", "k", "s", true) {
            override fun getRange(
                pathUri: String,
                startInclusive: Long,
                length: Int,
            ): ByteArray = throw S3Exception.builder().statusCode(416).message("InvalidRange").build()

            override fun getTail(
                pathUri: String,
                startInclusive: Long,
            ): ByteArray = throw S3Exception.builder().statusCode(416).message("InvalidRange").build()

            override fun getHead(
                pathUri: String,
                maxBytes: Int,
            ): ByteArray = ByteArray(16)
        }

    @Test
    fun `held to the registration, a tail start past the object's end is structural`() {
        // MUTATION: classify the 416 as any other fetch failure and this is a TransientFetchException.
        assertThatThrownBy { fetch.read(FooterFetch.Target("s3://b/f.parquet", 1000, 40), short) }
            .isExactlyInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("shorter than its registered file_size_bytes 1000 (its tail at byte 952")
    }

    @Test
    fun `without a footer cap a 416 stays transient`() {
        // PostgreSQL's classification, unchanged by #675.
        assertThatThrownBy {
            FooterFetch(maxWholeObjectBytes = 100).read(FooterFetch.Target("s3://b/f.parquet", 1000, 40), short)
        }
            .isInstanceOf(TransientFetchException::class.java)
    }

    @Test
    fun `held to the registration, a shorter whole object is structural`() {
        // MUTATION: check only for a longer object and the 16 bytes reach the parse.
        assertThatThrownBy { fetch.read(FooterFetch.Target("s3://b/f.parquet", 20, null), short) }
            .isExactlyInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("shorter (16 bytes) than its registered file_size_bytes 20")
    }

    @Test
    fun `held to the registration, a file_size_bytes under the footer suffix is structural before any read`() {
        // MUTATION: drop the check and getHead's require() is classified a TransientFetchException.
        for (size in listOf(-1L, 7L)) {
            assertThatThrownBy { fetch.read(FooterFetch.Target("s3://b/f.parquet", size, null), junk) }
                .isExactlyInstanceOf(IllegalStateException::class.java)
                .hasMessageContaining("file_size_bytes $size is under the 8-byte parquet footer suffix")
        }
    }
}
