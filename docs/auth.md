# Authentication and authorization

This is the canonical description of how hoglake authenticates callers. It
replaces the design history in [auth-tenancy.md](auth-tenancy.md), which is kept
for the reasoning but no longer describes the intended shape.

## The model in one paragraph

hoglake-server is a **resource server**. A client obtains a signed JWT from an
issuer of the deployment's choosing, presents it as `Authorization: Bearer`, and
hoglake verifies the signature offline against that issuer's public keys, reads
the catalog scopes the token carries, and authorizes the request. hoglake never
issues tokens on the request path, never calls the issuer, and never calls any
other service to decide a request. Everything it needs is in the token and in
its own configuration.

```
client ──(its credential)──▶ issuer ──▶ JWT             issuer: whatever the deployment runs
client ──Bearer JWT──▶ hoglake-server                   verifies offline, authorizes per catalog
client ──its own storage credentials──▶ S3              hoglake is not in the data path
```

The issuer is a deployment choice, not a hoglake dependency:

| deployment | issuer | storage credentials |
| --- | --- | --- |
| single-tenant, internal | none (`HOGLAKE_AUTH_MODE=off`), or hoglake's own local key (`local`, planned) | the server's and each client's own AWS identity |
| your own identity provider | any issuer that emits the [token contract](#the-token-contract) and publishes a JWKS | the same; or whatever your issuer hands clients |
| PostHog's managed warehouse | hogtower, PostHog's control plane, which mints tokens for PostHog's tenants and hands out scoped S3 sessions with them | vended by hogtower alongside the token |

hoglake's code carries no PostHog concepts. Catalog names, namespaces, the
`team_id` predicate and the fifteen-minute scoped-token cap are all
configuration or issuer policy. Where this document names hogtower, it is as
one issuer among possible issuers.

## Modes

`HOGLAKE_AUTH_MODE` selects how much the server trusts the network it is on.

| mode | behaviour | when |
| --- | --- | --- |
| `off` (default) | no verification; every request is accepted and the caller-supplied `author` is trusted | local development, CI, a single-tenant install on a trusted network |
| `warn` | tokens are verified and decisions are computed and counted, but nothing is refused, except a verified scoped token, which is filtered as in `enforce` | measuring before a cutover: watch `hoglake_auth_requests_total{outcome="missing"}` reach zero per client class |
| `enforce` | deny by default; a route without a verified, sufficient token gets 401 or 403 | production |
| `local` (planned) | `enforce`, with the server's own signing key as the issuer; see [Local issuer](#local-issuer-planned) | a standalone install that wants real authentication and no extra service |

Some features refuse `off` because they rely on the token: the public listener
(`HOGLAKE_PUBLIC_PORT`), hoglite backends and mounts.

Listeners:

- the **API port** serves every route and honours the mode;
- the **public port** (`HOGLAKE_PUBLIC_PORT`) always enforces, accepts tenant
  and scoped tokens and refuses operator tokens, so it can face a network the
  API port does not;
- the **metrics port** (`HOGLAKE_METRICS_PORT`) serves `/metrics` with no
  authentication, for a scraper inside the trust boundary.

`/livez`, `/healthz`, `/openapi.yaml` and `/v1/info` need no token on any
listener.

## Configuration

| variable | meaning |
| --- | --- |
| `HOGLAKE_AUTH_MODE` | `off`, `warn`, `enforce` |
| `HOGLAKE_AUTH_ISSUER` | the exact `iss` the server accepts; required unless the mode is `off` |
| `HOGLAKE_AUTH_JWKS_URL` or `HOGLAKE_AUTH_JWKS_FILE` | where the issuer's public keys come from; exactly one of the two. A key id the server has not seen triggers a rate-limited re-read, and a failed re-read keeps the old keys |
| `HOGLAKE_AUTH_AUDIENCE` | the `aud` the server accepts; default `hoglake` |
| `HOGLAKE_AUTH_CLOCK_SKEW_SECONDS` | tolerance on `exp`, `nbf` and `iat`; default 60, at most 600 |
| `HOGLAKE_AUTH_PREDICATE_FIELDS` | the fields a scoped token's `prd` may name; empty means scoped tokens are refused |

One issuer and one key set per server. A deployment that needs several issuers
runs them behind one JWKS, or waits for multi-issuer support (tracked).

## The token contract

hoglake owns this contract. The test vectors that pin it, signed tokens with
their expected verification results, are generated in this repository from the
verifier (`server/src/test/resources/oauth/contract-vectors.json`), and every
issuer conforms to them, hogtower included. An issuer that wants a new claim
changes the contract here first.

### Wire format

- A compact JWS in `Authorization: Bearer`.
- `alg` is `ES256` today. RS256 is planned so that off-the-shelf identity
  providers work unchanged (tracked).
- A `kid` header is required, and must name a key in the configured JWKS.
- `iss` must equal `HOGLAKE_AUTH_ISSUER`; `aud` must be, or contain,
  `HOGLAKE_AUTH_AUDIENCE`.
- `exp` is required. `nbf` and `iat` are optional; an `iat` in the future is
  refused.

### Claims

The core, which every issuer must emit:

| claim | required | meaning |
| --- | --- | --- |
| `iss`, `aud`, `sub`, `exp` | yes | as above; `sub` becomes the audit actor |
| `jti` | yes | token id, for correlating an audit line with a token |
| `ten` | yes today; absent will mean `tenant` (planned) | the token kind: `tenant`, `scoped` or `operator` |
| `cat` | tenant and scoped | `{"<catalog>": ["read", "write", …]}`: the catalogs the token may use and the scopes on each. An unknown scope word is dropped, not refused |

Authorization is per catalog. The scope vocabulary is ordered,
`read < write < maintenance < admin`: `write` includes `read`, `maintenance`
(expire, compact, rehydrate, cleanup, publications) includes `write`, and
`admin` (drop the catalog) includes everything. A catalog the token does not
name answers 404, not 403, so a token cannot enumerate catalogs it may not see.

Planned, so that standard identity providers can express the same thing: a
`scope` claim of space-separated `hoglake:<catalog>:<scope>` values, accepted
as equivalent to `cat` (tracked).

Profile extensions, optional and generic. An issuer uses them when its
deployment needs them; hoglake applies them without knowing what they mean:

| claim | meaning |
| --- | --- |
| `org` | the tenant the token belongs to, for the audit log only; hoglake does not authorize on it |
| `cli` | a human-readable client name, for logs |
| `dlg` | the caller may set `author` on a commit to a name other than its own `sub` |
| `prd` | on a `scoped` token: `{"<field>": {"<value>": "read" \| "write", …}}`, or `{"<field>": {"*": …}}`. The field must be one of `HOGLAKE_AUTH_PREDICATE_FIELDS`. hoglake filters metadata by `field ∈ values` and refuses commits whose files are attributed to a value the token may not write. This is how a multi-tenant issuer narrows one catalog to one tenant; PostHog's field is `team_id` |
| `usr` | on a `scoped` token: an opaque end-user name for access logs, never used for authorization |
| `rtg` | verified placement of the single catalog the token names on a configured backend (`b`, `be`, and `t` for hoglite); mismatches answer 401 `unknown_backend` |

A `scoped` token's lifetime (`exp − iat`) is capped at 900 seconds today. The
cap becomes configuration (tracked); an issuer that mints longer-lived scoped
tokens must raise it.

An `operator` token bypasses per-catalog checks and may create catalogs,
manage mounts and containment, and read operator-only routes. Operator tokens
are therefore only as safe as the keys in the JWKS: whoever can sign with them
is an operator.

### Errors

- `401` with `WWW-Authenticate: Bearer error=<reason>` for a missing, malformed,
  expired, mis-issued or mis-signed token. The reason strings are part of the
  contract; clients branch on them.
- `403` for a scope shortfall on a catalog the token can see.
- `404` for a catalog the token cannot see.

## What hoglake does not do

- **Issue tokens on the request path.** There is no `/oauth/token` here. The
  planned local issuer is an offline command, not an endpoint.
- **Call anything to authorize.** No introspection, no callback to the issuer,
  no policy service. A compromised hoglake can serve the catalogs it was already
  allowed to serve and nothing more.
- **Hold users or passwords.** Authorization is carried by the token.
- **Vend storage credentials.** See the next section.

## Storage credentials

hoglake authorizes *metadata*. Reading and writing the parquet is between the
client and the object store, and the client brings its own credentials for it:
static keys, an instance or pod identity, or short-lived credentials its issuer
handed it.

The server's own jobs, the hydrator, compaction, snapshot expiry, quarantine and
cleanup, use the server's own identity: `HOGLAKE_S3_ACCESS_KEY` and
`HOGLAKE_S3_SECRET_KEY`, or the SDK's default chain (a pod identity in
Kubernetes). hoglake does not fetch storage credentials from anyone.

A deployment that wants storage access gated by the same authority as the
tokens does it on the issuer side: the issuer returns scoped, short-lived
storage credentials alongside the token, in the same response, and the client
uses them. That is how PostHog's deployment works. Storage credentials are never
placed inside the JWT: the JWT travels on every request and is forwarded to
mount hosts, and a storage secret should not.

## Clients

Every hoglake client supports the same two shapes:

- **Client credentials.** A token URL, a client id and a client secret. The
  client performs an OAuth 2.0 client-credentials grant (`POST <token_url>`,
  HTTP Basic `client_id:client_secret`, form
  `grant_type=client_credentials&audience=hoglake`), reads `access_token` and
  `expires_in`, caches the token for the process, refreshes it sixty seconds
  before expiry, and on a 401 mints once more and retries once. The scope is
  whatever the issuer grants that client; the request carries no scope.
- **A fixed token.** A bearer token the operator minted elsewhere; the client
  sends it unchanged and a 401 is final.

| client | client credentials | fixed token |
| --- | --- | --- |
| pyhoglake | `pyhoglake.auth.ClientCredentials(token_url, client_id, client_secret)` | `pyhoglake.auth.BearerToken(token)` |
| DuckDB extension | `ATTACH … (TOKEN_URL, CLIENT_ID, CLIENT_SECRET)` | `ATTACH … (TOKEN '…')` |
| hedgerow | `auth: {token_url, client_id, client_secret}` per side | through pyhoglake |
| Trino connector (PostHog/trino) | `hoglake.auth.token-uri`, `hoglake.auth.client-id`, `hoglake.auth.client-secret` | planned (tracked) |
| millrace | planned ([#574](https://github.com/PostHog/hoglake/issues/574)) | planned ([#574](https://github.com/PostHog/hoglake/issues/574)) |
| webui | — | sign in with a pasted token (in review) |

pyhoglake accepts any `httpx.Auth`, and the Trino connector's token source is an
interface. A deployment with a different grant, a device flow, a cloud
identity provider's SDK, or an exchange of some other credential, implements
one of those and nothing else changes. hogtower appears in the clients only as
the example token URL in their READMEs.

## Local issuer (planned)

For an install that wants real authentication and no extra service:

- `HOGLAKE_AUTH_MODE=local` makes the server trust a key it holds itself,
  from `HOGLAKE_AUTH_SIGNING_KEY` or generated at first start and persisted in
  the metadata store, and publishes its public half at
  `/.well-known/jwks.json`.
- `hoglake token mint --catalog <name> --scope <read|write|maintenance|admin>
  --ttl <duration> [--operator]` signs a token offline with that key, from the
  command line, for an operator to hand to a client as a fixed token.
- Nothing listens for token requests. The runtime stays verify-only.

This is deliberately not an OAuth server inside hoglake: it holds one key and
no clients, and the earlier design that put an authorization server in hoglake
was retired for the reasons in [auth-tenancy.md §17.2](auth-tenancy.md).

## Status

Built: `off`, `warn`, `enforce`; ES256 verification against a JWKS file or URL;
the four scopes and the route table; scoped tokens with `prd`; `rtg`; the
public and metrics listeners; client credentials and fixed tokens in pyhoglake,
the DuckDB extension, hedgerow and the Trino connector.

Tracked in [#574](https://github.com/PostHog/hoglake/issues/574): hoglake-generated contract vectors;
RS256; the `scope` claim; a configurable scoped-token cap; `ten` defaulting to
`tenant`; the local issuer and `hoglake token mint`; a fixed-token option in the
Trino connector; client credentials in millrace; removing the issuer-specific
strings that remain in server comments and tests.
