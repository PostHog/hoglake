package com.posthog.hoglake.storage

import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.service.RemovalStore
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.net.URI
import java.time.Duration

/**
 * The shape of a catalog view: under `broker` every view of a store
 * sends through the store's ONE client — an S3Client owns an HTTP
 * connection pool, and a process serving every tenant's catalogs must
 * not grow one per catalog — and under the unscoped providers the view
 * IS the store. Nothing here touches the network: the broker URL is a
 * closed port and no request is sent.
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
            assertThat(a.client).isSameAs(base.client)
            assertThat(b.client).isSameAs(base.client)
            assertThat(base.forCatalog("a", "s3://b/a")).isSameAs(a)
        }
    }

    @Test
    fun `an unscoped provider's view is the store itself`() {
        ObjectStore("http://127.0.0.1:9", "us-east-1", "k", "s", true).use { base ->
            assertThat(base.scoped).isFalse()
            assertThat(base.forCatalog("a", "s3://b/a", StorageAccess.WRITE)).isSameAs(base)
        }
        RemovalStore("http://127.0.0.1:9", "us-east-1", null, null, true).use { base ->
            assertThat(base.scoped).isFalse()
            assertThat(base.forCatalog("a", "s3://b/a")).isSameAs(base)
        }
    }
}
