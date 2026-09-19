package com.posthog.hoglake.hydrator

import com.posthog.hoglake.Config
import com.posthog.hoglake.observability.Metrics
import io.github.oshai.kotlinlogging.KotlinLogging
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
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
open class ObjectStore(
    endpoint: String?,
    region: String,
    accessKey: String?,
    secretKey: String?,
    pathStyle: Boolean,
) : AutoCloseable {
    constructor(config: Config) : this(
        endpoint = config.s3Endpoint.ifBlank { null },
        region = config.s3Region,
        accessKey = config.s3AccessKey.ifBlank { null },
        secretKey = config.s3SecretKey.ifBlank { null },
        pathStyle = config.s3PathStyle,
    )

    private val s3: S3Client =
        S3Client.builder()
            .region(Region.of(region))
            .apply {
                if (endpoint != null) endpointOverride(URI.create(endpoint))
                if (accessKey != null && secretKey != null) {
                    credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)),
                    )
                }
            }
            .forcePathStyle(pathStyle)
            .build()

    private val log = KotlinLogging.logger {}

    data class Location(val bucket: String, val key: String)

    /** Fetch the whole object at [pathUri] (`s3://bucket/key`). */
    open fun get(pathUri: String): ByteArray {
        val loc = parse(pathUri)
        return s3.getObjectAsBytes(
            GetObjectRequest.builder().bucket(loc.bucket).key(loc.key).build(),
        ).asByteArray()
    }

    /** Ranged read of the object's tail, from byte [startInclusive] to the end. */
    fun getTail(
        pathUri: String,
        startInclusive: Long,
    ): ByteArray {
        require(startInclusive >= 0) { "negative range start $startInclusive for $pathUri" }
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
        val endInclusive = startInclusive + length - 1
        return getRange(pathUri, "bytes=$startInclusive-$endInclusive")
    }

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
                .build(),
        ).asByteArrayUnsafe()
    }

    open fun put(
        pathUri: String,
        bytes: ByteArray,
    ) {
        val loc = parse(pathUri)
        s3.putObject(
            PutObjectRequest.builder().bucket(loc.bucket).key(loc.key).build(),
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
        val loc = parse(pathUri)
        return s3.createMultipartUpload(
            CreateMultipartUploadRequest.builder().bucket(loc.bucket).key(loc.key).build(),
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
        val loc = parse(pathUri)
        return s3.uploadPart(
            UploadPartRequest.builder()
                .bucket(loc.bucket).key(loc.key)
                .uploadId(uploadId).partNumber(partNumber)
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
        val loc = parse(pathUri)
        val parts =
            etags.mapIndexed { i, etag ->
                CompletedPart.builder().partNumber(i + 1).eTag(etag).build()
            }
        s3.completeMultipartUpload(
            CompleteMultipartUploadRequest.builder()
                .bucket(loc.bucket).key(loc.key).uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
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
        val loc = parse(pathUri)
        runCatching {
            s3.abortMultipartUpload(
                AbortMultipartUploadRequest.builder()
                    .bucket(loc.bucket).key(loc.key).uploadId(uploadId).build(),
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

    override fun close() = s3.close()

    companion object {
        private const val SCHEME = "s3://"

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
