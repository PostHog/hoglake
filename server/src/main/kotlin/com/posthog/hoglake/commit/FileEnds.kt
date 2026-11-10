package com.posthog.hoglake.commit

import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableFileEnds
import com.posthog.hoglake.service.requireOperationMetadata
import com.posthog.hoglake.wireObjectMapper

/**
 * The request-side rules of a commit that ends whole files
 * (`POST /commit/replacements/prepared`), shared by the PostgreSQL commit
 * and the hoglite one so both refuse the same shapes with the same words.
 * Everything here decides on the REQUEST alone; what depends on table
 * state (the incarnation, the conflict window, the live spec) is decided
 * by each backend inside its own commit.
 */
internal object FileEnds {
    /** A bound on one request's tuples, so a payload cannot size the matching statement. */
    const val MAX_PARTITION_TUPLES: Int = 10_000

    private val mapper = wireObjectMapper()

    /** The 422s for a request whose `file_ends` is set; a no-op for every other request. */
    fun validate(request: CommitRequest) {
        val ends = request.fileEnds ?: return
        if (ends.isEmpty()) throw HoglakeException.Validation("file_ends must not be empty")
        if (request.idempotencyKey == null) throw HoglakeException.Validation("idempotency_key is required")
        if (request.readSnapshot == null) throw HoglakeException.Validation("read_snapshot is required")
        if (request.deletes.isNotEmpty() || request.requireUnchangedTables || request.allowPendingDeletes) {
            throw HoglakeException.Validation(
                "file_ends cannot be combined with deletes, require_unchanged_tables or pending deletes",
            )
        }
        requireOperationMetadata(request.author, request.message)
        val seen = HashSet<Pair<String, String>>()
        for (end in ends) {
            val name = "${end.namespace}.${end.table}"
            if (!seen.add(end.namespace to end.table)) {
                throw HoglakeException.Validation("file_ends names $name more than once")
            }
            val partitions = end.partitions
            if (end.allFiles == (partitions != null)) {
                throw HoglakeException.Validation(
                    "file_ends for $name needs exactly one of partitions or all_files: true",
                )
            }
            if (partitions == null) continue
            if (partitions.isEmpty()) throw HoglakeException.Validation("file_ends partitions for $name is empty")
            if (partitions.size > MAX_PARTITION_TUPLES) {
                throw HoglakeException.Validation(
                    "file_ends partitions for $name has ${partitions.size} tuples, " +
                        "over the limit of $MAX_PARTITION_TUPLES",
                )
            }
            val arity = partitions.first().size
            if (arity == 0 || partitions.any { it.size != arity }) {
                throw HoglakeException.Validation(
                    "file_ends partitions for $name must be non-empty tuples of one length",
                )
            }
        }
    }

    /** Each tuple of [end] must have as many values as the live spec has fields; [fieldCount] null = no spec. */
    fun checkArity(
        end: TableFileEnds,
        fieldCount: Int?,
    ) {
        val partitions = end.partitions ?: return
        val name = "${end.namespace}.${end.table}"
        if (fieldCount == null || fieldCount == 0) {
            throw HoglakeException.Validation("$name has no partition spec: end its files with all_files")
        }
        if (partitions.any { it.size != fieldCount }) {
            throw HoglakeException.Validation(
                "file_ends partitions for $name must have $fieldCount value(s) per tuple, " +
                    "one per field of the live spec",
            )
        }
    }

    /** The unique tuples of [end], each as the compact JSON the stores compare on, in first-seen order. */
    fun tupleJson(end: TableFileEnds): List<String> =
        end.partitions.orEmpty().map { mapper.writeValueAsString(it) }.distinct()

    /** The same request with `file_ends` in an order that does not matter to a replay's fingerprint. */
    fun canonical(request: CommitRequest): CommitRequest {
        val ends = request.fileEnds ?: return request
        return request.copy(
            fileEnds =
                ends.map { end -> end.copy(partitions = end.partitions?.let { sortedTuples(it) }) }
                    .sortedWith(compareBy({ it.namespace }, { it.table })),
        )
    }

    private fun sortedTuples(tuples: List<List<String?>>): List<List<String?>> =
        tuples.map { mapper.writeValueAsString(it) to it }
            .distinctBy { it.first }
            .sortedBy { it.first }
            .map { it.second }
}
