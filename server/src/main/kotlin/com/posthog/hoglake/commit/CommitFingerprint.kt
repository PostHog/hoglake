package com.posthog.hoglake.commit

import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.wireObjectMapper

/** The process-wide wire mapper; a fresh one per fingerprint was a Kotlin-module scan per commit. */
private val mapper = wireObjectMapper()

/** Canonical full payload, not a lossy subset or hash. Partition value order is significant. */
internal fun commitFingerprint(request: CommitRequest): String {
    val appends =
        request.appends.map { append ->
            append.copy(
                files =
                    append.files.map { file ->
                        val canonical = file.copy(columnStats = file.columnStats?.sortedBy { it.fieldId })
                        mapper.writeValueAsString(canonical) to canonical
                    }.sortedBy { it.first }.map { it.second },
            ).let { mapper.writeValueAsString(it) to it }
        }.sortedBy { it.first }.map { it.second }
    return mapper.writeValueAsString(request.copy(appends = appends))
}

/**
 * The version byte every stored digest carries, and the reason the
 * digest is 33 bytes rather than 32.
 *
 * `commitFingerprint`'s output BECAME A PERSISTED FORMAT in V24, and
 * that is the whole hazard of the change. Before it, the replay check
 * re-canonicalized the STORED BODY with the reading replica's own
 * mapper, so both sides of the comparison were produced by the same
 * code at the same instant and canonicalization drift was invisible by
 * construction. A digest cannot be re-canonicalized. Any of these
 * silently invalidates every receipt inside the retention window:
 *
 *  - flipping the default of a `NON_DEFAULT` boolean on `CommitRequest`
 *    (`require_unchanged_tables`, `allow_pending_deletes`) — the value
 *    that was omitted starts being written and vice versa;
 *  - reordering `CommitRequest`'s property declarations, since Jackson
 *    serializes Kotlin properties in declaration order;
 *  - adding any non-null field (`NON_NULL` only elides the nullable ones);
 *  - a Jackson upgrade that renders a number or an escape differently.
 *
 * WITHOUT A VERSION the consequence of any of those is a **422
 * `idempotency_key reused with a different request` on a correct
 * client's legitimate replay**, for a whole retention window, which is
 * exactly the new-4xx-on-a-deployed-writer's-path that AGENT.md's
 * "stage every refusal a live client could hit" forbids. The byte does
 * not let two canonicalizations coexist — an old canonical string is
 * unrecoverable — it makes the drift DETECTABLE, and it lets the
 * comparison answer a version it does not understand with the truth it
 * does have: this key was published, here is its snapshot. See
 * `CommitService.doCommit`'s receipt arm.
 *
 * Bumping it is therefore the required companion to any change in the
 * list above. `CommitFingerprintGoldenTest` is the tripwire: it pins
 * the canonical string and this digest as LITERALS, so drift reds there
 * with a message saying what it costs.
 */
internal const val COMMIT_FINGERPRINT_VERSION: Byte = 0x01

/** Total stored length: one version byte plus a SHA-256. */
internal const val COMMIT_FINGERPRINT_LENGTH: Int = 33

/**
 * The 33 bytes a receipt stores in place of the request body:
 * [COMMIT_FINGERPRINT_VERSION] followed by SHA-256 of the canonical
 * string [commitFingerprint] already produced.
 *
 * The indirection through that string is what keeps the SEMANTICS
 * unchanged: the canonicalization, the order-insensitivity over
 * appends/files/column stats and the wire mapper's field set are the
 * same, so "same key, same request" decides what it decided when the
 * comparison was string equality on the stored jsonb. What is NOT
 * unchanged is the failure mode when that string moves, which is what
 * the version byte above is for.
 *
 * Why a digest rather than the payload: the payload is ~1.5 MB of jsonb
 * for a 270-file x 25-column prepared append and 566 KB on disk after
 * TOAST, written under the per-catalog commit lock on every commit
 * (#240). A receipt has to answer "was this idempotency key published,
 * and what snapshot did it produce" — the digest answers the first half
 * and the two id columns beside it answer the second.
 *
 * WHAT IS LOST, stated because it is a real loss: a receipt can no
 * longer be read back to see WHAT was committed. That question is
 * answered by the snapshot the receipt names — its commit message and
 * author, and the `hog_data_file` rows whose `begin_snapshot` is it —
 * for as long as the snapshot is retained. A digest cannot be inverted,
 * so after expiry the receipt says only "this key published snapshot N".
 */
internal fun commitFingerprintDigest(canonical: String): ByteArray =
    byteArrayOf(COMMIT_FINGERPRINT_VERSION) +
        java.security.MessageDigest.getInstance("SHA-256").digest(canonical.toByteArray(Charsets.UTF_8))
