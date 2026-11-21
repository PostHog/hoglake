package com.posthog.hoglake.storage

import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.RemovalStore
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.net.URI
import java.time.Duration

/**
 * The shape of a catalog view: under every provider a view is bounded to
 * the catalog's data_path and refuses a path outside it before any S3
 * call (#668); under `broker` every view of a store sends through the
 * store's ONE client — an S3Client owns an HTTP connection pool, and a
 * process serving every tenant's catalogs must not grow one per catalog
 * — and under the unscoped providers the view forwards its IO to the
 * store's own methods, so a subclass that injects faults keeps
 * injecting. Nothing here touches the network: the endpoints and the
 * broker URL are a closed port and no request is sent — a view that
 * reached S3 would fail with a connect error, not the typed refusal.
 */
class StoreViewsTest {
    private fun broker() =
        BrokerProvider(
            brokerUrl = URI.create("http://127.0.0.1:9/storage"),
            tokenUrl = URI.create("http://127.0.0.1:9/oauth/token"),
            clientId = "hoglake",
            clientSecret = "secret",
            audience = "hoglake",
            refreshMargin = Duration.ofMinutes(5),
        )

    @Test
    fun `object store views share the base store's client`() {
        ObjectStore("http://127.0.0.1:9", "us-east-1", true, broker()).use { base ->
            val a = base.forCatalog("a", "s3://b/a", StorageAccess.READ)
            val b = base.forCatalog("b", "s3://b/b", StorageAccess.WRITE)
            val aAgain = base.forCatalog("a", "s3://b/a", StorageAccess.READ)

            assertThat(a).isNotSameAs(base)
            assertThat(aAgain).isSameAs(a)
            assertThat(a.client).isSameAs(base.client)
            assertThat(b.client).isSameAs(base.client)
            assertThat(a.scoped).isTrue()
            assertThatThrownBy { a.forCatalog("c", "s3://b/c", StorageAccess.READ) }
                .isInstanceOf(IllegalStateException::class.java)
            // Closing a view closes nothing: the base still owns the client.
            a.close()
            assertThat(base.forCatalog("c", "s3://b/c", StorageAccess.READ).client).isSameAs(base.client)
        }
    }

    @Test
    fun `removal store views share the base store's client`() {
        RemovalStore("http://127.0.0.1:9", "us-east-1", true, broker()).use { base ->
            val a = base.forCatalog("a", "s3://b/a")
            val b = base.forCatalog("b", "s3://b/b")
            assertThat(a).isNotSameAs(base)
            assertThat(base.forCatalog("a", "s3://b/a", StorageAccess.DELETE)).isSameAs(a)
            assertThat(a.client).isSameAs(base.client)
            assertThat(b.client).isSameAs(base.client)
            assertThat(base.forCatalog("a", "s3://b/a")).isSameAs(a)
        }
    }

    @Test
    fun `an unscoped provider's view is bounded and forwards its IO to the store`() {
        var served = 0
        val base =
            object : ObjectStore("http://127.0.0.1:9", "us-east-1", "k", "s", true) {
                override fun get(pathUri: String): ByteArray {
                    served++
                    return byteArrayOf(7)
                }
            }
        base.use {
            assertThat(base.scoped).isFalse()
            val view = base.forCatalog("a", "s3://b/a", StorageAccess.WRITE)
            assertThat(view).isNotSameAs(base)
            assertThat(view.client).isSameAs(base.client)
            assertThat(view.boundPrefix?.dataPath).isEqualTo("s3://b/a")
            assertThat(base.forCatalog("a", "s3://b/a", StorageAccess.WRITE)).isSameAs(view)
            // Inside the prefix: the base's own (overridden) method answers.
            assertThat(view.get("s3://b/a/x.parquet")).containsExactly(7)
            assertThat(served).isEqualTo(1)
            // Outside it: refused before any call, the override included.
            for (outside in listOf("s3://b/aa/x", "s3://b/x", "s3://c/a/x", "s3://b/a/../x", "s3://b/a/%2e%2e/x")) {
                assertThatThrownBy { view.get(outside) }
                    .describedAs(outside)
                    .isInstanceOf(PathOutsidePrefixException::class.java)
                    .hasMessageContaining("s3://b/a")
            }
            assertThatThrownBy {
                view.put(
                    "s3://b/x",
                    byteArrayOf(),
                )
            }.isInstanceOf(PathOutsidePrefixException::class.java)
            assertThatThrownBy { view.getTail("s3://b/x", 0) }.isInstanceOf(PathOutsidePrefixException::class.java)
            assertThatThrownBy { view.getRange("s3://b/x", 0, 1) }.isInstanceOf(PathOutsidePrefixException::class.java)
            assertThatThrownBy { view.startMultipartUpload("s3://b/x") }
                .isInstanceOf(PathOutsidePrefixException::class.java)
            assertThatThrownBy { view.uploadPart("s3://b/x", "u", 1, byteArrayOf(1), 1) }
                .isInstanceOf(PathOutsidePrefixException::class.java)
            assertThatThrownBy { view.completeMultipartUpload("s3://b/x", "u", listOf("e")) }
                .isInstanceOf(PathOutsidePrefixException::class.java)
            assertThatThrownBy { view.abortMultipartUpload("s3://b/x", "u") }
                .isInstanceOf(PathOutsidePrefixException::class.java)
            assertThat(served).isEqualTo(1)
        }
    }

    @Test
    fun `a broker view refuses an outside path before asking the broker for a session`() {
        ObjectStore("http://127.0.0.1:9", "us-east-1", true, broker()).use { base ->
            val view = base.forCatalog("a", "s3://b/a", StorageAccess.READ)
            // The broker is a closed port: a session fetch would be a connect
            // failure, not this refusal.
            assertThatThrownBy { view.get("s3://b/other/x") }.isInstanceOf(PathOutsidePrefixException::class.java)
        }
        RemovalStore("http://127.0.0.1:9", "us-east-1", true, broker()).use { base ->
            val view = base.forCatalog("a", "s3://b/a")
            assertThat(view.deleteBatch(listOf("s3://b/other/x")))
                .containsOnlyKeys("s3://b/other/x")
            assertThatThrownBy { view.deleteIfExists("s3://b/other/x") }
                .isInstanceOf(PathOutsidePrefixException::class.java)
        }
    }

    @Test
    fun `a removal view fails outside paths per key and hands the rest to the store`() {
        val chunks = mutableListOf<List<String>>()
        val base =
            object : RemovalStore("http://127.0.0.1:9", "us-east-1", null, null, true) {
                override fun deleteChunk(
                    bucket: String,
                    chunk: List<Pair<String, String>>,
                ): Map<String, String> {
                    chunks += chunk.map { it.second }
                    return emptyMap()
                }
            }
        val meters = SimpleMeterRegistry()
        Metrics.bind(meters)
        try {
            base.use {
                assertThat(base.scoped).isFalse()
                val view = base.forCatalog("a", "s3://b/a")
                assertThat(view).isNotSameAs(base)
                assertThat(view.boundPrefix?.dataPath).isEqualTo("s3://b/a")
                val failures =
                    view.deleteBatch(
                        listOf("s3://b/a/keep", "s3://b/victim/x", "s3://b/a/also", "s3://other/a/y", "not-a-uri"),
                    )
                // The two outside paths and the unparseable one (outside every
                // prefix) fail on their own; the admitted keys reached the
                // store's (overridden) delete in one chunk.
                assertThat(failures).containsOnlyKeys("s3://b/victim/x", "s3://other/a/y", "not-a-uri")
                assertThat(failures["not-a-uri"]).contains("not-a-uri")
                assertThat(failures["s3://b/victim/x"]).contains("outside the catalog's data_path 's3://b/a'")
                assertThat(chunks).containsExactly(listOf("s3://b/a/keep", "s3://b/a/also"))
                assertThat(
                    meters.find("hoglake_path_outside_data_path_total")
                        .tags("catalog", "a", "surface", "removal_store").counter()?.count(),
                ).isEqualTo(3.0)
                // Nothing admitted: no delete call at all.
                assertThat(view.deleteBatch(listOf("s3://b/victim/y"))).containsOnlyKeys("s3://b/victim/y")
                assertThat(chunks).hasSize(1)
            }
        } finally {
            Metrics.clear()
            meters.close()
        }
    }

    @Test
    fun `a bucket-root data_path admits every key of the bucket and nothing else`() {
        val prefix = StoragePrefix.of("s3://b")
        assertThat(prefix.bucket).isEqualTo("b")
        assertThat(prefix.admits("s3://b/x")).isTrue()
        assertThat(prefix.admits("s3://b/deep/x")).isTrue()
        assertThat(prefix.admits("s3://b")).isFalse()
        assertThat(prefix.admits("s3://b/")).isFalse()
        assertThat(prefix.admits("s3://bb/x")).isFalse()
        assertThat(prefix.admits("s3://c/b/x")).isFalse()
        // A trailing slash on the data_path changes nothing.
        assertThat(StoragePrefix.of("s3://b/a/").admits("s3://b/a/x")).isTrue()
        assertThat(StoragePrefix.of("s3://b/a/").admits("s3://b/a")).isFalse()
        assertThatThrownBy { StoragePrefix.of("/tmp/x") }.isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { StoragePrefix.of("s3:///x") }.isInstanceOf(IllegalArgumentException::class.java)
    }
}
