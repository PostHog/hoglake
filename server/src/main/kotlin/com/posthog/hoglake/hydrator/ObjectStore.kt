package com.posthog.hoglake.hydrator

import com.posthog.hoglake.Config
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.storage.PathOutsidePrefixException
import com.posthog.hoglake.storage.StorageAccess
import com.posthog.hoglake.storage.StorageCredentialProvider
import com.posthog.hoglake.storage.StorageCredentialProviders
import com.posthog.hoglake.storage.StoragePrefix
import io.github.oshai.kotlinlogging.KotlinLogging
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.awscore.AwsRequest
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload
import software.amazon.awssdk.services.s3.model.CompletedPart
import software.amazon.awssdk.services.s3.model.CreateBucketRequest
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.UploadPartRequest
import java.net.URI
import java.util.concurrent.ConcurrentHashMap

/**
 * Thin S3/MinIO wrapper for the hydrator. Speaks `s3://bucket/key` URIs
 * (the absolute object-store paths stored in `hog_data_file.path`).
 *
 * `open`, with [get] and [put] open, for ONE reason: compaction's
 * failure-path tests need to fail at a chosen point in the fetch ->
 * stage -> upload -> commit sequence, and where the failure lands
 * relative to the staging ticket is the whole question (hoglake#118's
 * OOM-leak verification). A real S3 cannot be asked to throw an
 * OutOfMemoryError on the third call.
 */
open class ObjectStore private constructor(
    private val endpoint: String?,
    private val region: String,
    private val pathStyle: Boolean,
    /** Where this store's credentials come from; shared by every view it makes. */
    private val provider: StorageCredentialProvider,
    /**
     * Non-null on a catalog VIEW ([forCatalog]): the base store's client,
     * which the view shares rather than building its own. One client per
     * store, however many catalogs it serves: an S3Client owns an HTTP
     * connection pool, and a process serving every tenant's catalog must
     * not grow one pool per catalog.
     */
    shared: S3Client?,
    /**
     * Non-null on a catalog VIEW: the grant this view signs with, applied
     * PER REQUEST through the SDK's request override. Null on the base
     * store, whose client signs with [StorageCredentialProvider.unscoped]
     * — the shared keys under `ambient`/`static`, a refusal under `broker`.
     */
    private val credentials: AwsCredentialsProvider?,
    /**
     * Non-null on a catalog VIEW: the catalog and the prefix every path
     * this view touches must stay under, and (under an unscoped provider)
     * the base store its IO forwards to. Null on the base store, which
     * does catalog IO only through its views.
     */
    private val view: CatalogView?,
) : AutoCloseable {
    constructor(
        endpoint: String?,
        region: String,
        pathStyle: Boolean,
        provider: StorageCredentialProvider,
    ) : this(endpoint, region, pathStyle, provider, shared = null, credentials = null, view = null)

    /**
     * What makes a store a catalog view: the catalog, its [StoragePrefix],
     * and the base store to forward IO to under an unscoped provider. The
     * forwarding is what keeps a fault-injecting subclass of the base
     * store (compaction's and the hydrator's failure-path tests) in the
     * loop: the view checks the bound, then calls the base's own `open`
     * method, so the override runs. Under `broker` the view signs with
     * its own session and sends itself ([delegate] is null).
     */
    private class CatalogView(
        val catalog: String,
        val prefix: StoragePrefix,
        val delegate: ObjectStore?,
    ) {
        fun require(pathUri: String) = prefix.require(pathUri, catalog, SURFACE)
    }

    /**
     * The key-pair shape every test fixture builds: static keys when both
     * are given, otherwise the SDK chain — the rule the server applied
     * before `HOGLAKE_STORAGE_PROVIDER` existed.
     */
    constructor(
        endpoint: String?,
        region: String,
        accessKey: String?,
        secretKey: String?,
        pathStyle: Boolean,
    ) : this(
        endpoint = endpoint,
        region = region,
        pathStyle = pathStyle,
        provider = StorageCredentialProviders.staticOrAmbient(accessKey, secretKey),
    )

    constructor(
        config: Config,
        provider: StorageCredentialProvider = StorageCredentialProviders.fromConfig(config),
    ) : this(
        endpoint = config.s3Endpoint.ifBlank { null },
        region = config.s3Region,
        pathStyle = config.s3PathStyle,
        provider = provider,
    )

    /** True on the base store, which built [s3] and closes it; a view borrows it. */
    private val ownsClient: Boolean = shared == null

    private val s3: S3Client =
        shared
            ?: S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(provider.unscoped())
                .apply { if (endpoint != null) endpointOverride(URI.create(endpoint)) }
                .forcePathStyle(pathStyle)
                .build()

    /**
     * The per-request override a view stamps on every request it makes:
     * the catalog's own credentials, resolved by the SDK at send time, so
     * a refreshing session is followed without touching the client.
     */
    private val override: AwsRequestOverrideConfiguration? =
        credentials?.let { AwsRequestOverrideConfiguration.builder().credentialsProvider(it).build() }

    private fun <B : AwsRequest.Builder> B.scoped(): B = apply { override?.let { overrideConfiguration(it) } }

    /** The client this store sends with; a view's is its base store's. For tests. */
    internal val client: S3Client get() = s3

    /** True under a provider whose credentials differ per catalog ([forCatalog] is then mandatory before IO). */
    val scoped: Boolean get() = provider.scoped

    /** The catalog views this base store has built, one per (catalog, prefix, access). */
    private val views = ConcurrentHashMap<Triple<String, String, StorageAccess>, ObjectStore>()

    /**
     * The store to do [access] on [catalog]'s objects under [prefix] (its
     * `data_path`) with: ALWAYS a view bounded to that prefix, which
     * refuses a path outside it ([PathOutsidePrefixException]) before any
     * S3 call. Under an unscoped provider the view forwards its IO to
     * this very store — same client, same keys, and the base's own
     * (possibly overridden) methods, so a fault-injecting test store
     * keeps injecting. Under `broker` it signs each request with the
     * catalog's own session over THIS store's client. Views are cached
     * for the life of the base store; a session refreshes underneath
     * its view, so nothing is rebuilt.
     *
     * The bound is what stands between a catalog row naming another
     * catalog's object and a write under that prefix with the server's
     * identity (#668): the broker's grant enforces it in IAM, and this
     * enforces it in process for every provider.
     */
    fun forCatalog(
        catalog: String,
        prefix: String,
        access: StorageAccess,
    ): ObjectStore {
        check(view == null) { "forCatalog on a catalog view" }
        return views.computeIfAbsent(Triple(catalog, prefix, access)) {
            val bound = StoragePrefix.of(prefix)
            if (provider.scoped) {
                ObjectStore(
                    endpoint,
                    region,
                    pathStyle,
                    provider,
                    s3,
                    provider.credentialsFor(catalog, prefix, access),
                    CatalogView(catalog, bound, delegate = null),
                )
            } else {
                ObjectStore(
                    endpoint,
                    region,
                    pathStyle,
                    provider,
                    s3,
                    null,
                    CatalogView(catalog, bound, delegate = this),
                )
            }
        }
    }

    /** The prefix this view is bounded to; null on the base store. */
    val boundPrefix: StoragePrefix? get() = view?.prefix

    private val log = KotlinLogging.logger {}

    data class Location(val bucket: String, val key: String)

    /**
     * The location of [pathUri], after the view's bound check. Every IO
     * method starts here, so the check cannot be skipped by one of them.
     */
    private fun locate(pathUri: String): Location {
        view?.require(pathUri)
        return parse(pathUri)
    }

    /** Fetch the whole object at [pathUri] (`s3://bucket/key`). */
    open fun get(pathUri: String): ByteArray {
        val loc = locate(pathUri)
        view?.delegate?.let { return it.get(pathUri) }
        return s3.getObjectAsBytes(
            GetObjectRequest.builder().bucket(loc.bucket).key(loc.key).scoped().build(),
        ).asByteArray()
    }

    /** Ranged read of the object's tail, from byte [startInclusive] to the end. */
    fun getTail(
        pathUri: String,
        startInclusive: Long,
    ): ByteArray {
        require(startInclusive >= 0) { "negative range start $startInclusive for $pathUri" }
        locate(pathUri)
        view?.delegate?.let { return it.getTail(pathUri, startInclusive) }
        return getRange(pathUri, "bytes=$startInclusive-")
    }

    /**
     * Ranged read of [length] bytes from [startInclusive].
     *
     * `open` for the same reason [get] and [put] are: compaction's
     * failure-path tests inject faults at chosen points in the
     * fetch -> rewrite -> upload -> commit sequence, and streaming reads
     * move the fetch from one call per file to many.
     */
    open fun getRange(
        pathUri: String,
        startInclusive: Long,
        length: Int,
    ): ByteArray {
        require(startInclusive >= 0) { "negative range start $startInclusive for $pathUri" }
        require(length > 0) { "non-positive range length $length for $pathUri" }
        locate(pathUri)
        view?.delegate?.let { return it.getRange(pathUri, startInclusive, length) }
        val endInclusive = startInclusive + length - 1
        return getRange(pathUri, "bytes=$startInclusive-$endInclusive")
    }

    /** The raw ranged GET; its callers have located the path. */
    private fun getRange(
        pathUri: String,
        range: String,
    ): ByteArray {
        val loc = parse(pathUri)
        // Unsafe = "no defensive copy". Correct here: the response
        // object is discarded on the next line and nothing else can see
        // the array, and at one 8 MiB readahead fill per call the copy
        // asByteArray() makes is a second 8 MiB allocation for nothing.
        return s3.getObjectAsBytes(
            GetObjectRequest.builder()
                .bucket(loc.bucket)
                .key(loc.key)
                .range(range)
                .scoped()
                .build(),
        ).asByteArrayUnsafe()
    }

    open fun put(
        pathUri: String,
        bytes: ByteArray,
    ) {
        val loc = locate(pathUri)
        view?.delegate?.let { return it.put(pathUri, bytes) }
        s3.putObject(
            PutObjectRequest.builder().bucket(loc.bucket).key(loc.key).scoped().build(),
            RequestBody.fromBytes(bytes),
        )
    }

    /**
     * Multipart upload, which is S3's streaming write: there is no
     * append, so a writer that does not know its length up front buffers
     * one part at a time and completes at the end. Compaction's rewrite
     * is exactly that shape — parquet emits bytes forward and only knows
     * the total when it closes.
     *
     * These are `open` alongside [get], [getRange] and [put] so the
     * fault-injection tests can still fail at a chosen point in the
     * fetch -> rewrite -> upload -> commit sequence. The upload is now
     * several calls rather than one, which gives those tests more places
     * to cut, not fewer.
     *
     * Completion is ATOMIC: the object does not exist until
     * [completeMultipartUpload] returns, so the staging-ticket protocol
     * around compaction's output is unchanged from the single [put] it
     * replaces. An upload that is neither completed nor aborted leaves
     * parts that are invisible as objects and still billed — every
     * failure path owes an [abortMultipartUpload].
     */
    open fun startMultipartUpload(pathUri: String): String {
        val loc = locate(pathUri)
        view?.delegate?.let { return it.startMultipartUpload(pathUri) }
        return s3.createMultipartUpload(
            CreateMultipartUploadRequest.builder().bucket(loc.bucket).key(loc.key).scoped().build(),
        ).uploadId()
    }

    /**
     * Upload one part, returning its ETag. Part numbers start at 1. Every
     * part except the last must be at least 5 MiB — S3's rule, not ours.
     */
    open fun uploadPart(
        pathUri: String,
        uploadId: String,
        partNumber: Int,
        bytes: ByteArray,
        length: Int,
    ): String {
        require(partNumber >= 1) { "part numbers start at 1, got $partNumber for $pathUri" }
        val loc = locate(pathUri)
        view?.delegate?.let { return it.uploadPart(pathUri, uploadId, partNumber, bytes, length) }
        return s3.uploadPart(
            UploadPartRequest.builder()
                .bucket(loc.bucket).key(loc.key)
                .uploadId(uploadId).partNumber(partNumber)
                .scoped()
                .build(),
            RequestBody.fromInputStream(bytes.inputStream(0, length), length.toLong()),
        ).eTag()
    }

    /** Make the parts one object. [etags] is part 1..n in order. */
    open fun completeMultipartUpload(
        pathUri: String,
        uploadId: String,
        etags: List<String>,
    ) {
        require(etags.isNotEmpty()) { "cannot complete an upload with no parts: $pathUri" }
        val loc = locate(pathUri)
        view?.delegate?.let { return it.completeMultipartUpload(pathUri, uploadId, etags) }
        val parts =
            etags.mapIndexed { i, etag ->
                CompletedPart.builder().partNumber(i + 1).eTag(etag).build()
            }
        s3.completeMultipartUpload(
            CompleteMultipartUploadRequest.builder()
                .bucket(loc.bucket).key(loc.key).uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                .scoped()
                .build(),
        )
    }

    /**
     * Discard an upload and its parts. Best effort by design: this runs
     * on the failure path, where the original exception is the one worth
     * propagating.
     *
     * Best effort is not the same as silent. An abort that keeps failing
     * — IAM without `s3:AbortMultipartUpload` is the obvious way — leaks
     * parts that are billed and invisible, and nothing else in the
     * system can notice: parts are not objects, so neither
     * `hog_file_removal` nor the cleanup drain can see or reach them.
     * The only backstop is a bucket lifecycle rule with
     * AbortIncompleteMultipartUpload, which this repo does not configure
     * (see server/README.md). So the failure is logged and counted even
     * though it is not raised.
     */
    open fun abortMultipartUpload(
        pathUri: String,
        uploadId: String,
    ) {
        val loc = locate(pathUri)
        view?.delegate?.let { return it.abortMultipartUpload(pathUri, uploadId) }
        runCatching {
            s3.abortMultipartUpload(
                AbortMultipartUploadRequest.builder()
                    .bucket(loc.bucket).key(loc.key).uploadId(uploadId).scoped().build(),
            )
        }.onFailure { e ->
            Metrics.multipartAbortFailed()
            log.warn(e) {
                "failed to abort multipart upload $uploadId for $pathUri; its parts are " +
                    "billed and invisible until a bucket lifecycle rule reaps them"
            }
        }
    }

    /** Idempotent bucket creation (test/bootstrap convenience). */
    fun createBucket(bucket: String) {
        try {
            s3.createBucket(CreateBucketRequest.builder().bucket(bucket).build())
        } catch (_: BucketAlreadyOwnedByYouException) {
            // fine
        } catch (_: BucketAlreadyExistsException) {
            // fine
        }
    }

    /** A view holds nothing of its own; the base store closes the client and the provider. */
    override fun close() {
        if (!ownsClient) return
        views.clear()
        s3.close()
        provider.close()
    }

    companion object {
        private const val SCHEME = "s3://"

        /** The `surface` tag of the out-of-prefix metric for this store's views. */
        private const val SURFACE = "object_store"

        /** Parse `s3://bucket/key`; throws [IllegalArgumentException] on anything else. */
        fun parse(pathUri: String): Location {
            require(pathUri.startsWith(SCHEME)) { "not an s3:// URI: $pathUri" }
            val rest = pathUri.removePrefix(SCHEME)
            val slash = rest.indexOf('/')
            require(slash > 0) { "s3 URI has no key: $pathUri" }
            val bucket = rest.substring(0, slash)
            val key = rest.substring(slash + 1)
            require(key.isNotEmpty()) { "s3 URI has empty key: $pathUri" }
            return Location(bucket, key)
        }
    }
}
