package com.posthog.hoglake

import com.posthog.hoglake.storage.AmbientProvider
import com.posthog.hoglake.storage.BrokerProvider
import com.posthog.hoglake.storage.StaticProvider
import com.posthog.hoglake.storage.StorageCredentialProviders
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * `HOGLAKE_STORAGE_PROVIDER` resolution and the boot-time refusals
 * (docs/auth.md, "Storage credentials"). The default must be the
 * pre-knob rule exactly: an install that never heard of the knob
 * changes nothing.
 */
class StorageProviderConfigTest {
    @Test
    fun `unset is ambient without keys and static with both`() {
        assertThat(Config().effectiveStorageProvider).isEqualTo(Config.StorageProviderKind.AMBIENT)
        assertThat(Config(s3AccessKey = "k", s3SecretKey = "s").effectiveStorageProvider)
            .isEqualTo(Config.StorageProviderKind.STATIC)
        // One key alone is not a pair: the SDK chain, as before.
        assertThat(Config(s3AccessKey = "k").effectiveStorageProvider).isEqualTo(Config.StorageProviderKind.AMBIENT)
        assertThat(StorageCredentialProviders.fromConfig(Config())).isInstanceOf(AmbientProvider::class.java)
        assertThat(StorageCredentialProviders.fromConfig(Config(s3AccessKey = "k", s3SecretKey = "s")))
            .isInstanceOf(StaticProvider::class.java)
    }

    @Test
    fun `an unknown provider is refused by name`() {
        assertThatThrownBy { Config(storageProvider = "vault") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_STORAGE_PROVIDER")
            .hasMessageContaining("vault")
        assertThat(Config(storageProvider = " Ambient ").effectiveStorageProvider)
            .isEqualTo(Config.StorageProviderKind.AMBIENT)
    }

    @Test
    fun `explicit static needs both keys`() {
        assertThatThrownBy { Config(storageProvider = "static") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_STORAGE_PROVIDER=static")
        assertThatThrownBy { Config(storageProvider = "static", s3AccessKey = "k") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_S3_SECRET_KEY")
        assertThat(Config(storageProvider = "static", s3AccessKey = "k", s3SecretKey = "s").effectiveStorageProvider)
            .isEqualTo(Config.StorageProviderKind.STATIC)
    }

    @Test
    fun `explicit ambient refuses keys beside it`() {
        assertThatThrownBy { Config(storageProvider = "ambient", s3AccessKey = "k", s3SecretKey = "s") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_STORAGE_PROVIDER=ambient")
        assertThat(Config(storageProvider = "ambient").effectiveStorageProvider)
            .isEqualTo(Config.StorageProviderKind.AMBIENT)
    }

    @Test
    fun `broker needs every setting and refuses static keys`() {
        assertThatThrownBy { Config(storageProvider = "broker") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_STORAGE_BROKER_URL")
            .hasMessageContaining("HOGLAKE_STORAGE_BROKER_CLIENT_SECRET")
        assertThatThrownBy { broker(storageBrokerClientSecret = "")() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_STORAGE_PROVIDER=broker")
        for (leftover in listOf(
            broker(s3AccessKey = "k", s3SecretKey = "s"),
            broker(s3AccessKey = "k"),
            broker(s3SecretKey = "s"),
        )) {
            assertThatThrownBy { leftover() }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_S3_ACCESS_KEY")
        }
        assertThatThrownBy { broker(storageBrokerUrl = "ftp://broker/vend")() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_STORAGE_BROKER_URL must be an https URL")
        assertThatThrownBy { broker(storageBrokerTokenUrl = "not a url")() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_STORAGE_BROKER_TOKEN_URL")
        assertThatThrownBy { broker(storageBrokerRefreshMarginSeconds = -1)() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_STORAGE_BROKER_REFRESH_MARGIN_SECONDS")
        assertThatThrownBy { broker(storageBrokerAudience = " ")() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_STORAGE_BROKER_AUDIENCE")
    }

    @Test
    fun `broker urls are https, or http to the loopback host, and are trimmed`() {
        assertThatThrownBy { broker(storageBrokerUrl = "http://broker.example/storage")() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_STORAGE_BROKER_URL must be an https URL (http only for localhost)")
        assertThatThrownBy { broker(storageBrokerTokenUrl = "http://10.0.0.5/oauth/token")() }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_STORAGE_BROKER_TOKEN_URL")
        for (dev in listOf(
            "http://localhost:8081/storage",
            "http://127.0.0.1:8081/storage",
            "http://[::1]:8081/storage",
        )) {
            assertThat(
                broker(storageBrokerUrl = dev)().effectiveStorageProvider,
            ).isEqualTo(Config.StorageProviderKind.BROKER)
        }
        // Whitespace around a value passes the boot check AND builds.
        val padded =
            broker(
                storageBrokerUrl = "  https://broker.example/storage \n",
                storageBrokerTokenUrl = " https://issuer.example/oauth/token ",
            )()
        StorageCredentialProviders.fromConfig(padded).use { provider ->
            assertThat(provider.toString()).isEqualTo("broker(https://broker.example/storage)")
        }
    }

    @Test
    fun `a complete broker configuration builds the broker provider`() {
        val cfg = broker()()
        assertThat(cfg.effectiveStorageProvider).isEqualTo(Config.StorageProviderKind.BROKER)
        assertThat(cfg.storageBrokerAudience).isEqualTo("hoglake")
        assertThat(cfg.storageBrokerRefreshMarginSeconds).isEqualTo(300)
        StorageCredentialProviders.fromConfig(cfg).use { provider ->
            assertThat(provider).isInstanceOf(BrokerProvider::class.java)
            assertThat(provider.scoped).isTrue()
            // The secret is in no rendering of the provider.
            assertThat(provider.toString()).doesNotContain("hunter2")
        }
    }

    /** A broker config as a thunk, so a refusal is raised inside the assertion that expects it. */
    private fun broker(
        storageBrokerUrl: String = "https://broker.example/storage",
        storageBrokerTokenUrl: String = "https://issuer.example/oauth/token",
        storageBrokerClientId: String = "hoglake-prod",
        storageBrokerClientSecret: String = "hunter2",
        storageBrokerAudience: String = "hoglake",
        storageBrokerRefreshMarginSeconds: Long = 300,
        s3AccessKey: String = "",
        s3SecretKey: String = "",
    ): () -> Config =
        {
            Config(
                storageProvider = "broker",
                storageBrokerUrl = storageBrokerUrl,
                storageBrokerTokenUrl = storageBrokerTokenUrl,
                storageBrokerClientId = storageBrokerClientId,
                storageBrokerClientSecret = storageBrokerClientSecret,
                storageBrokerAudience = storageBrokerAudience,
                storageBrokerRefreshMarginSeconds = storageBrokerRefreshMarginSeconds,
                s3AccessKey = s3AccessKey,
                s3SecretKey = s3SecretKey,
            )
        }
}
