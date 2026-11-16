package com.posthog.hoglake.compaction

import java.security.MessageDigest

/** Stable identities for group and bucket reservations. */
internal object CompactionClaimKey {
    /**
     * The group's identity: spec id, partition values and sorted input
     * file ids, hashed.
     *
     * SORTED file ids, because the two replicas' planners may emit a
     * group's files in different orders if the candidate query's tie
     * break ever moves; the SET is the identity. Partition values are
     * length-prefixed rather than joined with a separator so a value
     * containing the separator cannot collide with two values that do
     * not, and a null value is distinguishable from an empty one.
     */
    fun groupKey(group: CompactionGroup): String =
        tupleDigest(group.specId, group.partitionValues) { feed ->
            group.files.map { it.dataFileId }.sorted().forEach { feed("f$it") }
        }

    /** The prefix that marks a bucket claim. */
    const val BUCKET_KEY_PREFIX: String = "bucket:"

    /**
     * A bucket claim's key: [BUCKET_KEY_PREFIX] and a hex SHA-256 over
     * the bucket's spec id and partition values, fed exactly as
     * [groupKey] feeds them (length-prefixed, null distinct from empty)
     * and with no file ids. The prefix is what keeps the two key spaces
     * disjoint — a group key is 64 hex characters and nothing else.
     */
    fun bucketKey(
        specId: Long?,
        partitionValues: List<String?>?,
    ): String {
        // The prefix IS the type tag that keeps the two kinds of row apart
        // (the overlap read, the pre-probe read, anything that counts
        // groups); an empty one would make every bucket claim look like a
        // group claim to a reader that filters on it.
        require(BUCKET_KEY_PREFIX.isNotEmpty()) { "BUCKET_KEY_PREFIX must be non-empty" }
        val key = BUCKET_KEY_PREFIX + tupleDigest(specId, partitionValues) { }
        check(key.startsWith(BUCKET_KEY_PREFIX) && key.length == BUCKET_KEY_PREFIX.length + 64)
        return key
    }

    private fun tupleDigest(
        specId: Long?,
        partitionValues: List<String?>?,
        rest: ((String) -> Unit) -> Unit,
    ): String {
        val digest = MessageDigest.getInstance("SHA-256")

        fun feed(s: String) {
            val bytes = s.toByteArray(Charsets.UTF_8)
            digest.update(bytes.size.toString().toByteArray(Charsets.UTF_8))
            digest.update(':'.code.toByte())
            digest.update(bytes)
        }
        feed("spec=${specId ?: "none"}")
        feed("values=${partitionValues?.size ?: -1}")
        partitionValues?.forEach { feed(if (it == null) "null:" else "v:$it") }
        rest(::feed)
        return digest.digest().joinToString("") { "%02x".format(it) }
    }
}
