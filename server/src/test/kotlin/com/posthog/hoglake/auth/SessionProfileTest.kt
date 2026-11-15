package com.posthog.hoglake.auth

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * The session profile gate and validations, on hand-built tokens: what the
 * vectors pin in signed form, exercised claim by claim here.
 */
class SessionProfileTest {
    private val pair = TestTokens.keyPair()
    private val keys = StaticSigningKeys(TestTokens.KID, pair.public)

    private fun verifier(on: Boolean) =
        JwtVerifier(
            TestTokens.ISSUER,
            TestTokens.AUDIENCE,
            keys,
            60,
            setOf("team_id"),
            sessionProfile = on,
        ) { 1_700_000_100L }

    private val aws =
        """"aws":{"access_key_id":"FICTIONAL_KEY","secret_access_key":"FICTIONAL_SECRET",""" +
            """"session_token":"FICTIONAL_TOKEN","region":"us-east-1","expires_at":1700001200}"""

    private fun marker(version: Int = 1) = """"profile":"storage-session","version":$version,"purpose":"user-session""""

    /** A tenant token on catalog `lake` with the profile appended; [extra] replaces the profile's tail. */
    private fun profiled(
        extra: String = """$aws,"primary_catalog":"lake","remote_schemas":{"shared":["read"]}""",
        marker: String = marker(),
        exp: Long = 1_700_000_900L,
    ): String {
        val base = TestTokens.tenantClaims(catalogs = mapOf("lake" to listOf("read")), exp = exp)
        val claims = base.removeSuffix("}") + "," + listOf(marker, extra).filter { it.isNotEmpty() }.joinToString(",") + "}"
        return TestTokens.mint(pair, claims)
    }

    private fun refusedWith(
        token: String,
        on: Boolean,
        reason: String,
    ) {
        assertThatThrownBy { verifier(on).verify(token) }
            .isInstanceOfSatisfying(JwtViolation::class.java) { assertThat(it.reason).isEqualTo(reason) }
    }

    @Test
    fun `off refuses the profile as profile_not_enabled, never as a plain token`() {
        refusedWith(profiled(), on = false, reason = JwtViolation.PROFILE_NOT_ENABLED)
        // Any one of the profile's claims alone is enough to trip the gate.
        refusedWith(profiled(extra = aws, marker = ""), on = false, reason = JwtViolation.PROFILE_NOT_ENABLED)
        refusedWith(
            profiled(extra = """"primary_catalog":"lake"""", marker = ""),
            on = false,
            reason = JwtViolation.PROFILE_NOT_ENABLED,
        )
    }

    @Test
    fun `a plain token is unaffected by the gate either way`() {
        val plain = TestTokens.mint(pair, TestTokens.tenantClaims())
        assertThat(verifier(false).verify(plain).remoteSchemas).isEmpty()
        assertThat(verifier(true).verify(plain).remoteSchemas).isEmpty()
    }

    @Test
    fun `on accepts a well-formed profile and exposes its read-only remote schemas`() {
        val caller = verifier(true).verify(profiled())
        assertThat(caller.kind).isEqualTo(Caller.Kind.TENANT)
        assertThat(caller.catalogScopes).isEqualTo(mapOf("lake" to setOf(Scope.READ)))
        assertThat(caller.remoteSchemas).containsExactly("shared")
    }

    @Test
    fun `a bundle without the marker is refused`() {
        refusedWith(profiled(marker = ""), on = true, reason = JwtViolation.MALFORMED)
    }

    @Test
    fun `an unknown profile id, version or purpose is refused`() {
        refusedWith(profiled(marker = marker(2)), on = true, reason = JwtViolation.MALFORMED)
        refusedWith(
            profiled(marker = """"profile":"other","version":1,"purpose":"user-session""""),
            on = true,
            reason = JwtViolation.MALFORMED,
        )
        refusedWith(
            profiled(marker = """"profile":"storage-session","version":1,"purpose":"task""""),
            on = true,
            reason = JwtViolation.MALFORMED,
        )
    }

    @Test
    fun `primary_catalog must be the single catalog cat names`() {
        refusedWith(
            profiled(extra = """$aws,"primary_catalog":"other","remote_schemas":{}"""),
            on = true,
            reason = JwtViolation.MALFORMED,
        )
        refusedWith(profiled(extra = """$aws,"remote_schemas":{}"""), on = true, reason = JwtViolation.MALFORMED)
    }

    @Test
    fun `remote schemas are read-only and well-named`() {
        refusedWith(
            profiled(extra = """$aws,"primary_catalog":"lake","remote_schemas":{"shared":["read","write"]}"""),
            on = true,
            reason = JwtViolation.MALFORMED,
        )
        refusedWith(
            profiled(extra = """$aws,"primary_catalog":"lake","remote_schemas":{"shared":[]}"""),
            on = true,
            reason = JwtViolation.MALFORMED,
        )
        refusedWith(
            profiled(extra = """$aws,"primary_catalog":"lake","remote_schemas":{"bad name":["read"]}"""),
            on = true,
            reason = JwtViolation.MALFORMED,
        )
        // Absent is fine: a session with no remote schemas.
        assertThat(verifier(true).verify(profiled(extra = """$aws,"primary_catalog":"lake"""")).remoteSchemas).isEmpty()
    }

    @Test
    fun `the token may not outlive its credentials`() {
        // exp 1700001201 > aws.expires_at 1700001200
        refusedWith(profiled(exp = 1_700_001_201L), on = true, reason = JwtViolation.MALFORMED)
        // ... and the credentials may not outlive the session deadline.
        refusedWith(
            profiled(extra = """$aws,"primary_catalog":"lake","session_deadline":1700001199"""),
            on = true,
            reason = JwtViolation.MALFORMED,
        )
    }

    @Test
    fun `an incomplete bundle is refused`() {
        val partial = """"aws":{"access_key_id":"FICTIONAL_KEY","region":"us-east-1","expires_at":1700001200}"""
        refusedWith(
            profiled(extra = """$partial,"primary_catalog":"lake""""),
            on = true,
            reason = JwtViolation.MALFORMED,
        )
    }

    @Test
    fun `an operator token may not carry the profile`() {
        val claims = TestTokens.operatorClaims().removeSuffix("}") + "," + marker() + "," + aws + "}"
        refusedWith(TestTokens.mint(pair, claims), on = true, reason = JwtViolation.MALFORMED)
    }

    @Test
    fun `a scoped token carries the profile like a tenant token`() {
        val claims =
            TestTokens.scopedClaims(
                catalogs = """{"lake":["write"]}""",
                extra = """,${marker()},$aws,"primary_catalog":"lake","remote_schemas":{"shared":["read"]}""",
            )
        val caller = verifier(true).verify(TestTokens.mint(pair, claims))
        assertThat(caller.kind).isEqualTo(Caller.Kind.SCOPED)
        assertThat(caller.remoteSchemas).containsExactly("shared")
    }

    @Test
    fun `no refusal and no caller ever echoes a credential value`() {
        val secrets = listOf("FICTIONAL_KEY", "FICTIONAL_SECRET", "FICTIONAL_TOKEN")
        val tokens =
            listOf(
                profiled(),
                profiled(marker = ""),
                profiled(exp = 1_700_001_201L),
                profiled(extra = """$aws,"primary_catalog":"other""""),
            )
        tokens.forEach { token ->
            listOf(true, false).forEach { on ->
                val text =
                    try {
                        verifier(on).verify(token).toString()
                    } catch (v: JwtViolation) {
                        v.reason + " " + v.message
                    }
                secrets.forEach { assertThat(text).doesNotContain(it) }
            }
        }
    }
}
