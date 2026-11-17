package com.posthog.hoglake.store

import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.PredicateGrant
import com.posthog.hoglake.auth.Routing
import com.posthog.hoglake.auth.Scope
import com.posthog.hoglake.auth.readsServedSchema
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * The caller rule of a served schema on its own (auth/StoreEntitlement.kt),
 * with callers built by hand: each condition is checked apart from the
 * verifier and the route table, which make some of them unreachable through
 * a request.
 */
class ServedSchemaRuleTest {
    private val org = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0"
    private val otherOrg = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f1"

    private fun caller(
        kind: Caller.Kind = Caller.Kind.SCOPED,
        orgId: String? = org,
        cat: Map<String, Set<Scope>> = mapOf(org to setOf(Scope.READ)),
        schemas: Set<String> = setOf("posthog"),
        routing: Routing? = null,
    ) = Caller(
        "sub",
        orgId,
        kind,
        cat,
        "jti",
        "cli",
        predicates = mapOf("team_id" to PredicateGrant.All(Scope.READ)),
        routing = routing,
        remoteSchemas = schemas,
    )

    @Test
    fun `the reader is a scoped caller of the org whose cat reads its catalog and whose claim names the schema`() {
        assertThat(caller().readsServedSchema(org, "posthog")).isTrue()
        // A write scope on the catalog reads it too.
        assertThat(caller(cat = mapOf(org to setOf(Scope.WRITE))).readsServedSchema(org, "posthog")).isTrue()
    }

    @Test
    fun `every condition is needed, and nothing is allowed by default`() {
        val none: Caller? = null
        assertThat(none.readsServedSchema(org, "posthog")).isFalse()
        // Scoped: a tenant and an operator that hold everything else are not served.
        assertThat(caller(kind = Caller.Kind.TENANT).readsServedSchema(org, "posthog")).isFalse()
        assertThat(caller(kind = Caller.Kind.OPERATOR).readsServedSchema(org, "posthog")).isFalse()
        // The claim: absent, or for another schema, or for a case variant.
        assertThat(caller(schemas = emptySet()).readsServedSchema(org, "posthog")).isFalse()
        assertThat(caller(schemas = setOf("imports")).readsServedSchema(org, "posthog")).isFalse()
        assertThat(caller(schemas = setOf("PostHog")).readsServedSchema(org, "posthog")).isFalse()
        // `cat`: another catalog, or this one with no scope that reads.
        assertThat(caller(cat = mapOf(otherOrg to setOf(Scope.READ))).readsServedSchema(org, "posthog")).isFalse()
        assertThat(caller(cat = mapOf(org to emptySet())).readsServedSchema(org, "posthog")).isFalse()
        assertThat(caller(cat = emptyMap()).readsServedSchema(org, "posthog")).isFalse()
        // `org`: another org's token whose cat names this catalog, and a token with no org.
        assertThat(caller(orgId = otherOrg).readsServedSchema(org, "posthog")).isFalse()
        assertThat(caller(orgId = null).readsServedSchema(org, "posthog")).isFalse()
        // The path's catalog is what is checked: this org's reader does not read through another's.
        assertThat(caller().readsServedSchema(otherOrg, "posthog")).isFalse()
    }

    @Test
    fun `a placement does not enter the rule`() {
        val hoglite = Routing(org, BackendKind.HOGLITE, UUID.randomUUID(), "t_1")
        assertThat(caller(routing = hoglite).readsServedSchema(org, "posthog")).isTrue()
        assertThat(caller(routing = hoglite, schemas = emptySet()).readsServedSchema(org, "posthog")).isFalse()
    }
}
