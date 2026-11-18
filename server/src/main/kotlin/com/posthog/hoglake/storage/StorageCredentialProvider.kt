package com.posthog.hoglake.storage

import com.posthog.hoglake.Config
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import java.net.URI
import java.time.Duration

/**
 * What a job asks the provider for, each level including the one
 * before: [READ] (the hydrator's footer fetches), [WRITE] (compaction's
 * reads plus its put and multipart uploads — and NO delete, so a leaked
 * or buggy compaction session cannot remove a tenant's referenced
 * files), [DELETE] (cleanup's drain of `hog_file_removal`, the one job
 * that deletes). A broker grants `s3:DeleteObject` on the prefix only
 * for `delete`.
 */
enum class StorageAccess(val wire: String) {
    READ("read"),
    WRITE("write"),
    DELETE("delete"),
}

/**
 * Where the server's OWN jobs get S3 credentials (docs/auth.md, "Storage
 * credentials"): the hydrator, compaction, cleanup, and the same work
 * when a `maintenance`-scoped caller triggers it through the manual
 * routes. Ordinary metadata requests never touch S3 and never call this.
 *
 * `HOGLAKE_STORAGE_PROVIDER` selects one per deployment:
 *
 * - `ambient`: the SDK's default chain, one shared provider.
 * - `static`: `HOGLAKE_S3_ACCESS_KEY` / `HOGLAKE_S3_SECRET_KEY`.
 * - `broker`: [BrokerProvider], one short-lived session per
 *   (catalog, access), fetched from an HTTP broker under the server's
 *   own client credential.
 *
 * Unset, it is `static` when both keys are set and `ambient` otherwise —
 * exactly what the server did before the knob existed.
 */
interface StorageCredentialProvider : AutoCloseable {
    /**
     * True when credentials differ per catalog, so an S3 client must be
     * built per (catalog, access) and the base client must refuse to do
     * IO. False for the two providers whose credentials reach every
     * catalog alike, where one client serves everything.
     */
    val scoped: Boolean

    /**
     * Credentials for [access] on [catalog]'s objects under [prefix] (its
     * `data_path`). The SDK consults the returned provider on EVERY
     * request, so a client built on it follows a refreshing session
     * without being rebuilt.
     */
    fun credentialsFor(
        catalog: String,
        prefix: String,
        access: StorageAccess,
    ): AwsCredentialsProvider

    /**
     * Credentials for a client built with no catalog in mind — the base
     * [com.posthog.hoglake.hydrator.ObjectStore] and
     * [com.posthog.hoglake.service.RemovalStore]. The unscoped providers
     * hand back their one shared provider; a scoped one hands back a
     * provider that REFUSES, because a request it answered would be a
     * request outside every grant the broker made.
     */
    fun unscoped(): AwsCredentialsProvider

    override fun close() {}
}

/** The SDK's default chain: a pod identity, an instance profile, or the environment. */
class AmbientProvider : StorageCredentialProvider {
    private val chain: AwsCredentialsProvider = DefaultCredentialsProvider.create()

    override val scoped: Boolean = false

    override fun credentialsFor(
        catalog: String,
        prefix: String,
        access: StorageAccess,
    ): AwsCredentialsProvider = chain

    override fun unscoped(): AwsCredentialsProvider = chain

    override fun toString(): String = "ambient"
}

/** One static key pair for everything: MinIO, local development, CI. */
class StaticProvider(
    accessKey: String,
    secretKey: String,
) : StorageCredentialProvider {
    private val keys: AwsCredentialsProvider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))

    override val scoped: Boolean = false

    override fun credentialsFor(
        catalog: String,
        prefix: String,
        access: StorageAccess,
    ): AwsCredentialsProvider = keys

    override fun unscoped(): AwsCredentialsProvider = keys

    override fun toString(): String = "static"
}

/**
 * A provider that answers every request with a refusal: what a scoped
 * provider hands a client built without a catalog. The message names the
 * knob, because reaching this means a code path did IO without first
 * taking a catalog view, and that is a bug to fix, not a credential to
 * find.
 */
internal object RefusingCredentials : AwsCredentialsProvider {
    override fun resolveCredentials(): AwsCredentials =
        throw StorageCredentialException(
            "HOGLAKE_STORAGE_PROVIDER=broker grants credentials per catalog; " +
                "this S3 client was built without one (an ObjectStore or RemovalStore used " +
                "without forCatalog)",
        )
}

/** The provider could not produce usable credentials; the job's IO fails with this. */
class StorageCredentialException(
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)

object StorageCredentialProviders {
    /**
     * The provider a `Config` selects. [Config] has already validated the
     * combination at boot (see its `init`), so this only builds.
     */
    fun fromConfig(cfg: Config): StorageCredentialProvider =
        when (cfg.effectiveStorageProvider) {
            Config.StorageProviderKind.AMBIENT -> AmbientProvider()
            Config.StorageProviderKind.STATIC -> StaticProvider(cfg.s3AccessKey, cfg.s3SecretKey)
            Config.StorageProviderKind.BROKER ->
                BrokerProvider(
                    brokerUrl =
                        BrokerUrls.require(
                            "HOGLAKE_STORAGE_BROKER_URL",
                            cfg.storageBrokerUrl,
                            cfg.storageBrokerAllowHttp,
                        ),
                    tokenUrl =
                        BrokerUrls.require(
                            "HOGLAKE_STORAGE_BROKER_TOKEN_URL",
                            cfg.storageBrokerTokenUrl,
                            cfg.storageBrokerAllowHttp,
                        ),
                    clientId = cfg.storageBrokerClientId,
                    clientSecret = cfg.storageBrokerClientSecret,
                    audience = cfg.storageBrokerAudience,
                    allowHttp = cfg.storageBrokerAllowHttp,
                    refreshMargin = Duration.ofSeconds(cfg.storageBrokerRefreshMarginSeconds),
                )
        }

    /**
     * The pre-knob rule, for the stores' key-pair constructors (tests
     * and bootstrap code): static when both keys are given, else ambient.
     */
    fun staticOrAmbient(
        accessKey: String?,
        secretKey: String?,
    ): StorageCredentialProvider =
        if (accessKey != null && secretKey != null) StaticProvider(accessKey, secretKey) else AmbientProvider()
}

/**
 * The one rule for the broker's two URLs, applied at boot (`Config`) and
 * again where the provider is built: `https`, because the client secret
 * and every vended key cross this hop; `http` only to the loopback host
 * a dev stack or an in-test server listens on — or to any host when the
 * operator has stated, with `HOGLAKE_STORAGE_BROKER_ALLOW_HTTP=true`,
 * that this hop runs inside a network they already trust (a broker's
 * in-cluster listener, reached by its Service name). That is an explicit
 * knob rather than a hostname heuristic: what counts as a trusted network
 * belongs in the values file, not in a pattern here. Surrounding
 * whitespace is trimmed, so a value that passed the boot check builds too.
 */
object BrokerUrls {
    private val loopback = setOf("localhost", "127.0.0.1", "[::1]", "::1")

    fun require(
        name: String,
        raw: String,
        allowHttp: Boolean = false,
    ): URI {
        val trimmed = raw.trim()
        val uri =
            runCatching { URI.create(trimmed) }.getOrElse {
                throw IllegalArgumentException("$name must be an https URL, got '$raw'")
            }
        val host = uri.host?.lowercase()
        val ok =
            when (uri.scheme) {
                "https" -> host != null
                "http" -> host != null && (allowHttp || host in loopback)
                else -> false
            }
        require(ok) {
            "$name must be an https URL (http only for localhost, or for any host with " +
                "HOGLAKE_STORAGE_BROKER_ALLOW_HTTP=true), got '$raw'"
        }
        return uri
    }
}
