package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode

/** Signed examples for the dedicated read verifier. Use only fictional credentials. */
internal object ReadSessionVectors {
    const val PATH = "src/test/resources/oauth/read-session-vectors.json"
    const val VERIFY_AT = 2_000_000_001L
    private const val BACKEND = "11111111-1111-4111-8111-111111111111"
    private val mapper = ObjectMapper()

    private fun claims(): ObjectNode =
        mapper.readTree(
            """
            {
              "iss": "${ContractVectors.ISSUER}",
              "aud": ["trino-session", "hoglake-session", "hoglite-session"],
              "sub": "example.alice", "usr": "example.alice", "jti": "token-a",
              "profile": "example.aws-session", "version": 1, "purpose": "user-session",
              "ten": "scoped", "org": "example-org", "primary_catalog": "example-catalog",
              "sid": "session-a", "generation": 1, "session_deadline": 2000000120,
              "iat": 2000000000, "nbf": 2000000000, "exp": 2000000060,
              "cat": {"example-catalog": ["read"]}, "prd": {"team_id": {"42": "read"}},
              "rtg": {"b": "hoglite", "be": "$BACKEND", "t": "fixture_3"},
              "remote_schemas": {"team": ["read"]},
              "aws": {"access_key_id":"fictional-key", "secret_access_key":"fictional-secret",
                      "session_token":"fictional-session", "region":"us-east-1", "expires_at":2000000060},
              "grants": [
                {"catalog":"example-catalog","target":"example-catalog","bucket":"example-customer",
                 "object_key":"tables/data.parquet","region":"us-east-1","actions":["s3:GetObject"]},
                {"catalog":"example-catalog","target":"$BACKEND","bucket":"example-owner",
                 "object_key":"root/2c/t/fixture_3/manifest","region":"us-east-1","actions":["s3:GetObject"]},
                {"catalog":"example-catalog","target":"remote:events","bucket":"example-remote",
                 "prefix":"events/team_id=42/","region":"us-east-1","actions":["s3:GetObject"]},
                {"catalog":"example-catalog","target":"remote:imports","bucket":"example-remote",
                 "prefix":"imports/team_id=42/","region":"us-east-1","actions":["s3:GetObject"]}
              ]
            }
            """.trimIndent(),
        ) as ObjectNode

    fun document(): ObjectNode {
        val root = mapper.createObjectNode()
        root.put(
            "description",
            "Dedicated read verifier vectors. The key and AWS credential values are for tests only.",
        )
        root.put("issuer", ContractVectors.ISSUER)
        root.put("verify_at", VERIFY_AT)
        root.set<ObjectNode>("jwks", ContractVectors.document()["jwks"])
        root.putObject("config").apply {
            put("profile", "example.aws-session")
            put("backend_id", BACKEND)
            put("predicate_field", "team_id")
            putArray("remote_targets").add("remote:events").add("remote:imports")
        }
        val vectors = root.putArray("vectors")

        fun add(
            name: String,
            readResult: String = JwtViolation.MALFORMED,
            now: Long = VERIFY_AT,
            change: (ObjectNode) -> Unit = {},
        ) {
            val claims = claims().also(change)
            val header = mapper.createObjectNode().put("alg", "ES256").put("kid", ContractVectors.KID).put("typ", "JWT")
            vectors.addObject().apply {
                put("name", name)
                put("verify_at", now)
                set<ObjectNode>("claims", claims)
                put("token", ContractVectors.sign(header, claims))
                putObject("expect").put("read", readResult).put("ordinary", JwtViolation.WRONG_AUDIENCE)
            }
        }
        add("complete read session", "accept")
        add("mixed audiences", JwtViolation.WRONG_AUDIENCE) { (it["aud"] as ArrayNode).add(ContractVectors.AUDIENCE) }
        add("duplicate audience", JwtViolation.WRONG_AUDIENCE) {
            (it["aud"] as ArrayNode).set(1, mapper.nodeFactory.textNode("trino-session"))
        }
        add("AWS expiry without skew", JwtViolation.EXPIRED, 2_000_000_060L)
        add("missing profile") { it.remove("profile") }
        add("experimental profile marker") { it.put("profile", SessionProfile.ID) }
        add("missing session token") { (it["aws"] as ObjectNode).remove("session_token") }
        add("noncanonical incarnation") { (it["rtg"] as ObjectNode).put("t", "fixture_03") }
        add("wrong manifest suffix") {
            (it["grants"][1] as ObjectNode).put("object_key", "root/2c/t/fixture_3/manifest.json")
        }
        add("manifest prefix") {
            (it["grants"][1] as ObjectNode).apply {
                remove("object_key")
                put("prefix", "root/2c/t/fixture_3/")
            }
        }
        add("duplicate paths across remote targets") {
            (it["grants"][3] as ObjectNode).put("prefix", it["grants"][2]["prefix"].asText())
        }
        add("overlapping remote prefixes") {
            (it["grants"][3] as ObjectNode).put("prefix", "events/team_id=42/child/")
        }
        add("unbound catalog") { (it["grants"][3] as ObjectNode).put("catalog", "other") }
        add("write action") { (it["grants"][2] as ObjectNode).putArray("actions").add("s3:PutObject") }
        return root
    }
}
