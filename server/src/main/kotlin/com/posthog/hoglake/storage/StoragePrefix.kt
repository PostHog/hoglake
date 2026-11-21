package com.posthog.hoglake.storage

import com.posthog.hoglake.model.pathStaysUnder
import com.posthog.hoglake.observability.Metrics

/**
 * The object-store prefix a catalog's storage view is bounded to: its
 * `data_path`, `s3://<bucket>[/<prefix>]`. Every catalog view of
 * [com.posthog.hoglake.hydrator.ObjectStore] and
 * [com.posthog.hoglake.service.RemovalStore] carries one and refuses a
 * path outside it BEFORE any S3 call, whatever the credential provider:
 * under `ambient`/`static` the server's identity reaches every bucket,
 * so this check is the only thing between a catalog row that names
 * another catalog's object and a delete or write under that prefix
 * (#668).
 *
 * The test is [pathStaysUnder] (a literal prefix match, plus the dot
 * segment, whitespace, backslash and encoded-slash refusals that keep a
 * reader stack from re-addressing the key) over the data_path with one
 * trailing `/`, so `s3://b/demo` admits `s3://b/demo/x` and not
 * `s3://b/demo2/x`, plus the bucket compared on its own.
 */
class StoragePrefix private constructor(
    /** The data_path as given, verbatim. */
    val dataPath: String,
    /** The bucket the data_path names. */
    val bucket: String,
    private val root: String,
) {
    /** True when [pathUri] is an object under this prefix. */
    fun admits(pathUri: String): Boolean = bucketOf(pathUri) == bucket && pathStaysUnder(pathUri, root)

    /**
     * Refuse [pathUri] unless it is under this prefix. [catalog] and
     * [surface] (`object_store`, `removal_store`) label the metric, so an
     * operator can see which catalog's rows name foreign objects.
     */
    fun require(
        pathUri: String,
        catalog: String,
        surface: String,
    ) {
        if (admits(pathUri)) return
        Metrics.pathOutsideDataPath(catalog, surface)
        throw PathOutsidePrefixException(pathUri, dataPath)
    }

    override fun toString(): String = dataPath

    companion object {
        private const val SCHEME = "s3://"

        /**
         * The prefix for [dataPath]; a value that is not an
         * `s3://<bucket>[/<prefix>]` URI is an [IllegalArgumentException]
         * (every data_path hoglake stores passed CatalogService's
         * validation, so this only guards a caller that skipped it).
         */
        fun of(dataPath: String): StoragePrefix {
            val bucket =
                bucketOf(dataPath)
                    ?: throw IllegalArgumentException("data_path must be an s3://<bucket>[/<prefix>] URI: $dataPath")
            return StoragePrefix(dataPath, bucket, dataPath.trimEnd('/') + "/")
        }

        /** The bucket of an `s3://` URI, or null when [uri] is not one. */
        private fun bucketOf(uri: String): String? {
            if (!uri.startsWith(SCHEME)) return null
            return uri.removePrefix(SCHEME).substringBefore('/').takeIf { it.isNotEmpty() }
        }
    }
}

/** A path handed to a catalog's storage view that is not under the catalog's data_path. */
class PathOutsidePrefixException(
    val path: String,
    val prefix: String,
) : IllegalArgumentException("object path '$path' is outside the catalog's data_path '$prefix'")
