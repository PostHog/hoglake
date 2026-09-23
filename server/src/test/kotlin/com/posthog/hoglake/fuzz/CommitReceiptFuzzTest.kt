package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.api.CommitRequestDto
import com.posthog.hoglake.commit.commitFingerprint
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.service.CorruptDefinitionException
import com.posthog.hoglake.wireObjectMapper
import io.ktor.serialization.JsonConvertException
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.plugins.ContentTransformationException
import java.util.Random

/**
 * Fuzz target (docs/fuzzing.md layer 4): the STORED COMMIT RECEIPT —
 * `CommitFingerprint.commitFingerprint` over a `CommitRequest` parsed
 * from the wire by the production mapper.
 *
 * The fingerprint is not a hash and not a subset: it is the canonical
 * full payload of the commit, written to the receipt row and compared
 * against on every idempotent retry. So it decides, on its own, whether
 * a retry of the guarded-DML endpoints (`/commit/prepared`,
 * `/commit/deletes/prepared`, `/commit/mutations/prepared`,
 * `/commit/transaction`) is a REPLAY or a CONFLICT. A fingerprint that
 * moves when nothing about the request did turns a retry into a 409; one
 * that does not move when the request did would replay a different
 * commit.
 *
 * Contracts under test:
 *
 *  - **phase discipline, as in [WireDtoParseFuzzTest].** `readValue`
 *    runs inside ktor's ContentNegotiation, which wraps anything it
 *    throws into a 400, so a parse failure is never a finding here.
 *    `toModel()` and the fingerprint run in the route handler, outside
 *    that wrapping, and there the exception class picks the status code.
 *    The allowed family is READ OFF api/ErrorMapping.kt and is exactly
 *    what it installs a handler for — [HoglakeException],
 *    `CorruptDefinitionException`, [BadRequestException],
 *    [JsonConvertException] and [ContentTransformationException].
 *    `JacksonException` is deliberately NOT in it: StatusPages has no
 *    handler for it, so a `JsonProcessingException` escaping
 *    `commitFingerprint`'s own `writeValueAsString` would be a 500 on a
 *    request the caller could not have known was bad. The end-to-end
 *    proof that hostile bytes really do come back 4xx lives in
 *    `api/WireParseErrorMappingTest`.
 *  - **deterministic and idempotent.** The same request fingerprints to
 *    the same string, and because the fingerprint IS a CommitRequest
 *    document, parsing it back and fingerprinting again returns it
 *    unchanged.
 *  - **invariant under permutation** of `appends`, of the `files` inside
 *    an append, and of the `column_stats` inside a file. Two writers
 *    that registered the same files in a different order submitted the
 *    same commit, and a retry must be recognised as one.
 *
 * NOT asserted: anything about the order of `deletes`.
 * `commitFingerprint` canonicalises `appends` only — the delete groups,
 * and the files within them, keep wire order — so two orderings of one
 * delete set fingerprint differently today. That gap is being closed
 * separately; asserting it here would red the nightly on a known defect
 * instead of finding new ones.
 *
 * **Input layout.** Eight bytes of permutation entropy, then the request
 * body. The prefix is a FIXED size and comes off the FRONT of the
 * provider (`consumeBytes`) rather than the back (`consumeInt`): a
 * committed seed is then a plain eight-byte header followed by readable
 * JSON, and — unlike a length prefix — a mutation anywhere in the body
 * never moves the boundary.
 */
class CommitReceiptFuzzTest {
    @FuzzTest(maxDuration = "300s")
    fun commitReceiptsAreStableUnderPermutation(data: FuzzedDataProvider) {
        val permutation = Random(commitPermutationSeed(data.consumeBytes(COMMIT_PERMUTATION_PREFIX_BYTES)))
        val body = data.consumeRemainingAsBytes()
        if (body.size > MAX_INPUT_BYTES) return

        // Parse phase: ContentNegotiation turns every throw into a 400.
        val dto =
            try {
                mapper.readValue<CommitRequestDto>(body)
            } catch (_: Exception) {
                return
            }

        // Handler phase: from here on, the exception class is the status code.
        val request =
            try {
                // allowEmptyDeletes: the prepared-DELETE and transaction
                // endpoints pass it, and it is the permissive branch, so
                // it reaches strictly more of toModel than the default.
                dto.toModel(allowEmptyDeletes = true)
            } catch (e: Exception) {
                checkMapped("CommitRequestDto.toModel", e)
                return
            }

        val fingerprint =
            try {
                commitFingerprint(request)
            } catch (e: Exception) {
                checkMapped("commitFingerprint", e)
                return
            }

        check(commitFingerprint(request) == fingerprint) {
            "commitFingerprint is not deterministic for the same request"
        }

        // Idempotence. The receipt is a CommitRequest document, so it
        // must read back as one and fingerprint to itself; a receipt the
        // server cannot re-read is a receipt it cannot compare against.
        val reparsed =
            try {
                mapper.readValue<CommitRequest>(fingerprint)
            } catch (e: Exception) {
                throw IllegalStateException(
                    "the fingerprint the server would store does not parse back as a CommitRequest: " +
                        "${e.javaClass.name}: ${e.message?.take(200)}\n  receipt: ${fingerprint.take(400)}",
                    e,
                )
            }
        check(commitFingerprint(reparsed) == fingerprint) {
            "fingerprint(parse(fingerprint(r))) != fingerprint(r)\n  first : ${fingerprint.take(400)}\n" +
                "  second: ${commitFingerprint(reparsed).take(400)}"
        }

        checkPermutationInvariance(request, fingerprint, permutation)
    }

    private fun checkPermutationInvariance(
        request: CommitRequest,
        fingerprint: String,
        permutation: Random,
    ) {
        val permuted = permuteCommitRequest(request, permutation)
        check(commitFingerprint(permuted) == fingerprint) {
            "the receipt moved under a permutation of appends/files/column_stats\n" +
                "  before: ${fingerprint.take(400)}\n  after : ${commitFingerprint(permuted).take(400)}"
        }
    }

    private fun checkMapped(
        where: String,
        e: Exception,
    ) {
        check(
            e is HoglakeException || e is CorruptDefinitionException || e is BadRequestException ||
                e is JsonConvertException || e is ContentTransformationException,
        ) {
            "$where escaped with unmapped ${e.javaClass.name}: ${e.message?.take(200)}"
        }
    }

    private companion object {
        const val MAX_INPUT_BYTES = 1 shl 20

        /** The PRODUCTION wire mapper (api/WireJson.kt), never a copy of it. */
        val mapper: ObjectMapper = wireObjectMapper()
    }
}

/** Bytes of the fixed permutation-entropy prefix every input starts with. */
internal const val COMMIT_PERMUTATION_PREFIX_BYTES = 8

/**
 * The prefix as one big-endian long, MIXED; a short prefix zero-fills.
 *
 * The mix (SplitMix64's finalizer) is not decoration. `java.util.Random`
 * derives its first output from the high bits of a barely-scrambled
 * seed, so nearby seeds shuffle a short list the same way: without this,
 * flipping one byte of the prefix usually leaves the permutation
 * unchanged, which is the one mutation the fuzzer has for reaching a
 * different ordering. Measured while choosing seed nonces — the search
 * for a non-identity two-element shuffle took 4,095 consecutive nonces
 * before the mix and one after it.
 */
internal fun commitPermutationSeed(prefix: ByteArray): Long {
    var seed = 0L
    for (i in 0 until COMMIT_PERMUTATION_PREFIX_BYTES) {
        seed = (seed shl 8) or (prefix.getOrElse(i) { 0 }.toLong() and 0xFF)
    }
    seed = (seed xor (seed ushr 30)) * -0x40a7b892e31b1a47L
    seed = (seed xor (seed ushr 27)) * -0x6b2fb644ecceee15L
    return seed xor (seed ushr 31)
}

/**
 * Reorder a commit's `appends`, the `files` inside each append, and the
 * `column_stats` inside each file — the three orderings the receipt is
 * supposed to be blind to.
 *
 * Top level and shared with `FuzzSeedGenerator` on purpose. A seed whose
 * shuffle happens to come back as the IDENTITY asserts nothing, and PR
 * CI only replays seeds, so nothing would notice: the generator proves
 * each committed seed actually permutes by running THIS function, not a
 * second copy of it that could drift from it.
 */
internal fun permuteCommitRequest(
    request: CommitRequest,
    random: Random,
): CommitRequest =
    request.copy(
        appends =
            shuffledWith(request.appends, random).map { append ->
                append.copy(
                    files =
                        shuffledWith(append.files, random).map { file ->
                            val stats = file.columnStats
                            // Column stats canonicalise by field id, and
                            // that sort is STABLE, so two rows sharing a
                            // field id keep their wire order and
                            // permuting them is visible in the receipt. A
                            // file with a duplicated field id is refused
                            // upstream (CommitService), so this target has
                            // nothing to say about it and skips it rather
                            // than reporting an input the server never
                            // accepts.
                            if (stats == null || stats.map { it.fieldId }.toSet().size != stats.size) {
                                file
                            } else {
                                file.copy(columnStats = shuffledWith(stats, random))
                            }
                        },
                )
            },
    )

private fun <T> shuffledWith(
    items: List<T>,
    random: Random,
): List<T> = items.toMutableList().also { java.util.Collections.shuffle(it, random) }
