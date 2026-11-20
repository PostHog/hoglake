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
| single-tenant, internal | none (`HOGLAKE_AUTH_MODE=off`), or hoglake's own key (`local`): `hoglake token keygen` once, `hoglake token mint` per client, no issuer service | each client's own AWS identity; the server's own (`ambient`) for its jobs |
| your own identity provider | any issuer that emits the [token contract](#the-token-contract) and publishes a JWKS | the same; or whatever your issuer hands clients, and a `broker` in front of your own STS for the server's jobs |
| PostHog's managed warehouse | hogtower, PostHog's control plane, which mints tokens for PostHog's tenants and hands out scoped S3 sessions with them | vended by hogtower: to clients alongside the token, to the server's jobs through the `broker` provider |

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
| `local` | `enforce`, with the server's own signing key as the issuer; see [Local issuer](#local-issuer) | a standalone install that wants real authentication and no extra service |

Under `warn` a request with no token (or one that does not verify) is counted
once, as `missing` or `invalid`, and then served as under `off`: `GET
/v1/catalogs` lists every catalog of the default PostgreSQL backend, so the
web UI keeps working through the burn-in. It lists no hoglite catalogs (an
anonymous caller has no routing claim), and
on a deployment whose default backend is not PostgreSQL it answers 404, as it
did before. The per-catalog totals (`table_count`, `live_rows`,
`live_size_bytes`, `oldest_snapshot_time`) are omitted for the anonymous
caller, as `/v1/info` omits the instance totals. A verified token gets its own
catalogs as it would under `enforce`, with their totals unless it is scoped.

Some features refuse `off` because they rely on the token: the public listener
(`HOGLAKE_PUBLIC_PORT`), hoglite backends, remote schemas and served namespaces.

Listeners:

- the **API port** serves every route and honours the mode;
- the **public port** (`HOGLAKE_PUBLIC_PORT`) always enforces, accepts tenant
  and scoped tokens and refuses operator tokens, so it can face a network the
  API port does not;
- the **metrics port** (`HOGLAKE_METRICS_PORT`) serves `/metrics` with no
  authentication, for a scraper inside the trust boundary.

`/livez`, `/healthz`, `/openapi.yaml` and `/v1/info` need no token on any
listener; nor does `/.well-known/jwks.json`, which serves the public key
under `local` and 404s otherwise.

## Configuration

| variable | meaning |
| --- | --- |
| `HOGLAKE_AUTH_MODE` | `off`, `warn`, `enforce` |
| `HOGLAKE_AUTH_ISSUER` | the exact `iss` the server accepts; required unless the mode is `off` |
| `HOGLAKE_AUTH_JWKS_URL` or `HOGLAKE_AUTH_JWKS_FILE` | where the issuer's public keys come from; exactly one of the two. A key id the server has not seen triggers a rate-limited re-read, and a failed re-read keeps the old keys |
| `HOGLAKE_AUTH_AUDIENCE` | the `aud` the server accepts; default `hoglake` |
| `HOGLAKE_AUTH_CLOCK_SKEW_SECONDS` | tolerance on `exp`, `nbf` and `iat`; default 60, at most 600 |
| `HOGLAKE_AUTH_PREDICATE_FIELDS` | the fields a scoped token's `prd` may name; empty means scoped tokens are refused |
| `HOGLAKE_AUTH_SIGNING_KEY` | `local` only, and required there: the server's own ES256 key, a private P-256 JWK base64url-encoded, as `hoglake token keygen` prints it. Env-only — no key file, nothing persisted. Refused under any other mode. The issuer defaults to `hoglake` under `local` |
| `HOGLAKE_AUTH_SCOPED_MAX_LIFETIME_SECONDS` | the longest a scoped token may live, `exp − iat`; default 900; must be positive |
| `HOGLAKE_AUTH_SESSION_PROFILE` | `off` (default) or `on`: whether tokens carrying the [session profile](#session-profile-with-embedded-storage-credentials) are accepted. Off, such a token is refused as `profile_not_enabled`, never read as a plain metadata token |
| `HOGLAKE_AUTH_CLIENT_CLASSES` | comma-separated User-Agent product tokens, each counted under its own `client` label on `hoglake_auth_requests_total` instead of `other`; at most 16, lowercase, never `none` or `other`. A deployment names its own clients here, so the server knows no issuer by name |

One issuer and one key set per server, by decision: every token's `iss` must
equal `HOGLAKE_AUTH_ISSUER`. Several signing services can share one server
only by sharing that issuer identity and publishing their keys in one JWKS
under distinct `kid`s; a deployment with genuinely distinct issuers runs one
hoglake per issuer. Multi-issuer verification is not planned: one issuer per
server is what keeps a token's authority traceable to one trust decision, and
nothing in [the deployments above](#the-model-in-one-paragraph) needs more.

## The token contract

hoglake owns this contract. The test vectors that pin it, signed tokens with
their expected verification results, live in
`server/src/test/resources/oauth/contract-vectors.json`, and every issuer
conforms to them, hogtower included; an issuer that wants a new claim changes
the contract here first. The file is generated from hoglake's verifier
(`ContractVectors.kt`, `just server contract-vectors`) under a neutral issuer,
`https://hoglake.contract.test`, and each vector carries an `expect` verdict.
An issuer's suite mints each vector's `client` or `operator` input and
compares header and claims; a vector marked `mutation` is verifier-only, a
change no conforming issuer makes, which an issuer's suite skips.

### Wire format

- A compact JWS in `Authorization: Bearer`.
- `alg` is `ES256` or `RS256`; `none`, HMAC and every other algorithm are
  refused before any key is looked up. The key the `kid` names must be of the
  type the `alg` needs (an EC P-256 key for ES256, an RSA key of at least 2048
  bits for RS256), so a header cannot steer a key into the wrong verifier.
- A `kid` header is required, and must name a key in the configured JWKS. The
  JWKS may hold EC and RSA keys together; an entry that declares another
  purpose (`use` other than `sig`, or an `alg` hoglake does not verify, as
  identity providers publish for encryption) is skipped and needs no `kid`.
- `iss` must equal `HOGLAKE_AUTH_ISSUER`; `aud` must be, or contain,
  `HOGLAKE_AUTH_AUDIENCE`. A token the server will forward to a backend that
  verifies it independently (a hoglite backend, [below](#backends-that-verify-the-token-themselves))
  carries that backend's audience as well, `["hoglake", "hoglite"]`; hoglake
  checks only that its own audience is present and applies no other audience
  policy.
- `exp` is required. `nbf` and `iat` are optional; an `iat` in the future is
  refused.

### Claims

The core, which every issuer must emit:

| claim | required | meaning |
| --- | --- | --- |
| `iss`, `aud`, `sub`, `exp` | yes | as above; `sub` becomes the audit actor |
| `jti` | yes, by decision | token id, for correlating a request with a token. Required because the access log names each request's token by it, and an audit line written while a request is handled reaches the token through its `request_id`; an identity provider that does not emit one is fronted by a small issuer that does (every provider named in this document emits it) |
| `ten` | no; absent means `tenant` | the token kind: `tenant`, `scoped` or `operator`. A token without `ten` that carries `prd` is refused rather than read as a tenant token |
| `org` | tenant and scoped, today | the tenant the token belongs to, for the audit log. On a scoped token hoglake also compares it with a namespace's org mark and refuses a namespace marked for another org as if it did not exist ([auth-tenancy.md](auth-tenancy.md) §6.2b). Required by the verifier today; making it optional is tracked |
| `cat`, or `scope` | tenant and scoped, one or both | `cat` is `{"<catalog>": ["read", "write", …]}`: the catalogs the token may use and the scopes on each. On a tenant token an unknown scope word is dropped, not refused; a scoped token is stricter, below. `scope` is the standard form, next paragraph |

Authorization is per catalog. The scope vocabulary is ordered,
`read < write < maintenance < admin`: `write` includes `read`, `maintenance`
(expire, compact, rehydrate, cleanup, publications) includes `write`, and
`admin` (drop the catalog) includes everything. A catalog the token does not
name answers 404, not 403, so a token cannot enumerate catalogs it may not see.

So that standard identity providers can express the same thing, the OAuth
`scope` claim is accepted as an alternative: a space-separated string whose
`hoglake:<catalog>:<scope>` values carry the same grants as `cat`. Values that
do not start with `hoglake:` belong to other audiences and are ignored; a
`hoglake:` value that is not exactly three parts with a valid catalog name
refuses the token. When both `cat` and `scope` are present they must describe
the same grants after each has applied its own rules, or the token is
refused. One limit: a routed (`rtg`) token still needs exactly one `cat`
entry.

Profile extensions, optional and generic. An issuer uses them when its
deployment needs them; hoglake applies them without knowing what they mean:

| claim | meaning |
| --- | --- |
| `cli` | a human-readable client name, for logs |
| `dlg` | the caller may set `author` on a commit to a name other than its own `sub` |
| `prd` | on a `scoped` token: `{"<field>": {"<value>": "read" \| "write", …}}`, or `{"<field>": {"*": …}}`. The field must be one of `HOGLAKE_AUTH_PREDICATE_FIELDS`. hoglake filters metadata by `field ∈ values` and refuses commits whose files are attributed to a value the token may not write. This is how a multi-tenant issuer narrows one catalog to one tenant; PostHog's field is `team_id` |
| `usr` | on a `scoped` token: an opaque end-user name for access logs, never used for authorization |
| `rtg` | verified placement of the single catalog the token names on a configured backend (`b`, `be`, and `t` for hoglite); mismatches answer 401 `unknown_backend`. Exception: on a Host with served namespaces, a scoped token's `rtg` naming an unknown backend is ignored; such a token can read only served namespaces and gets 404 for everything else |

### Backends that verify the token themselves

A catalog placed on a hoglite backend (`HOGLAKE_BACKENDS_FILE`; it requires
`warn` or `enforce`) is served by a separate process that verifies the token
on its own. hoglake forwards the current request's verified JWT to that
backend unchanged and nothing else: no credential of hoglake's own, and no
caller credential shared across requests. The backend verifies it offline
against the same JWKS, requires its own audience to be present, and binds the
request to the backend and tenant the `rtg` claim names.
The raw SQL routes refuse `scoped` tokens and any `prd` constraint.
Raw SQL cannot preserve that authority.
Ordinary reads and writes never provision a tenant or bootstrap a schema
there; only a catalog-bound `operator` request provisions or deletes. An
issuer that starts adding the backend audience to tokens it already mints
should let the older single-audience tokens expire before the backend
enforces.

#### Typed scoped namespace read

This operation is disabled by default.
An operator can configure one exact local namespace binding.
The binding names the catalog, backend ID, routed tenant, and namespace.
It confirms tenant isolation and support at every eligible owner.
It requires enforced authentication with mounted public JWKS.
It cannot coexist with Remote Schema or served namespace configuration.

The caller uses `GET /v1/catalogs/{catalog}/namespaces/{namespace}`.
The request has no body or query parameters.
The server sends the original verified bearer to the configured backend.
The backend operation is `GET /v1/t/{tenant}/metadata/v1/catalogs/{catalog}/namespaces/{namespace}`.
The backend verifies that bearer independently.
These routes do not accept embedded storage credentials by default.
The isolated fixture below is the only exception.

The existing owner reads the catalog identity and live namespace in one read view.
It requires catalog READ authority and permission for the stored containment field and value.
The namespace name grants no authority.
A successful JSON response contains `namespace_id`, `name`, and `containment`.
The `containment` object contains `field` and `value`.
An absent or unauthorized namespace returns `404` with `{"code":"not_found"}` on the backend route.

The backend request target and response each have a 4 KiB limit.
The compact JWT has a 64 KiB limit.
The server uses at most 5 seconds for the backend call and response validation.
A backend may forward once to a configured live owner.
An unsupported backend response does not trigger an SQL fallback.
This operation never acquires a tenant or initializes a schema.

### Session profile with embedded storage credentials

#### Dedicated read verification

A separate read verifier takes an explicit profile identifier from the receiver.
It does not accept `storage-session`.
It requires ES256 with mounted keys.
It requires exactly `trino-session`, `hoglake-session`, and `hoglite-session` in `aud`.
The ordinary verifier refuses these audiences.

Only an isolated Hoglite fixture can opt in to this verifier.
Add `read_session` to the binding in `HOGLAKE_SCOPED_NAMESPACE_FILE`.
The object contains `profile` and a `remote_targets` array of two distinct target IDs.
Configure `team_id` as the only predicate field.
The API listener then requires a dedicated read token for the exact bound namespace GET.
All other requests keep the ordinary verifier.

Hoglake sends the original token to the configured Hoglite owner.
It does not retry with another token or use legacy SQL after a refusal.
The public listener never selects the dedicated verifier.
PostgreSQL catalogs do not support this fixture.
The fixture requires a Hoglite owner that independently accepts the dedicated profile on its typed read.

The initial read scope covers one primary catalog with a Hoglite placement.
It requires one explicit team with read permission.
The token carries generation `1`, a session identifier, and a finite session deadline.
Require `nbf <= iat < exp <= aws.expires_at <= session_deadline`.
Clock skew does not extend AWS expiry.

Each storage grant binds the catalog, target, bucket, region, and `s3:GetObject`.
It carries either a literal `object_key` or a slash-terminated `prefix`.
The verifier rejects overlaps and repeated grants.
The manifest grant names one literal object for the routed tenant.
Its key ends with `/<shard>/t/<routed-tenant>/manifest`, without an extension.
The shard is the two-digit lowercase hexadecimal result of FNV-1a 32-bit modulo 256 on the full routed tenant.
Two configured remote targets bind the read-only `team` family.
The verifier does not retain or log the AWS credentials.

The [signed read session vectors](../server/src/test/resources/oauth/read-session-vectors.json)
give the expected verdict for each verifier.
Their keys and credentials are fictional.
Run `just server contract-vectors` to regenerate the file.

#### Experimental profile

**Status: PROPOSED, experimental, off by default.** A query engine that runs
one user's work across many processes (a coordinator, its workers, hoglake, a
backend that verifies the token itself) wants ONE token to carry both the
metadata grants and the storage credentials the user's work needs, so that
every process verifies the same bytes offline and none of them holds a
standing cloud identity. This profile is how such a token is spelled; hoglake
owns it the way it owns the rest of the contract, so an issuer that mints it
conforms to this section and to `server/src/test/resources/oauth/session-profile-vectors.json`
(generated here, like the contract vectors; verified with the profile on).
The verifier's half is built, and so is the Client side of remote schemas
([auth-tenancy.md §6.5c](auth-tenancy.md#65c-remote-schemas-on-the-client)),
which serves a schema the token names only to a `scoped` token.

What the server does with such a token is deliberately small. It verifies the
profile's shape, records the read-only remote schemas the token authorizes,
and forwards the exact bytes to any backend that verifies the token itself
(above). **It never uses the embedded credentials**: it does not read them
into the caller, log them, or echo them in a refusal — every refusal names a
claim, never a value. The server's own jobs keep getting storage through the
[storage credential provider](#storage-credentials).

A token carries the profile when it carries any of `profile`, `aws`,
`primary_catalog` or `remote_schemas`. Then, with `HOGLAKE_AUTH_SESSION_PROFILE=on`,
it must carry all of the following, or it is refused (`invalid_token`):

| claim | meaning |
| --- | --- |
| `profile` | the marker that selects this parser: exactly `storage-session`. A credential object without it is a token nobody declared the meaning of |
| `version` | exactly `1`; an unknown version is refused, never parsed as the one the server knows |
| `purpose` | exactly `user-session`; this profile is for a caller's own work, so an `operator` token may not carry it |
| `primary_catalog` | the one catalog the session is about; it must be the single key of the `cat` object as written — a second `cat` entry refuses the token even when a plain tenant token would have dropped its unknown scope words, and the `scope` spelling alone does not carry the profile. A routed token (`rtg`) routes only this catalog |
| `remote_schemas` | optional: `{"<schema>": ["read"], …}`, at most 16, names valid identifiers. The read-only schemas of the primary catalog this session may use, served from another hoglake deployment that verifies the same token. Read-only by construction and parsed strictly: any other word, a non-string entry, a non-array value or an empty list refuses the token; nothing is dropped. The key `team` is the **team family entry**, not a schema named `team`: `{"team": ["read"]}` grants every schema `team_<id>` (`team_` and the decimal id, no leading zeros) whose id the token's `prd.team_id` holds, at any scope, or every team with `*`. The family grants read only, counts toward the 16, and may sit beside per-name entries. Team schemas are meant to be granted by this entry; a per-name `team_<id>` entry still works and names that one schema without a `prd` check on the Client; a deployment that shows a schema under another name maps that name to the published one before it checks the claim (a team schema has no alias) |
| `aws` | the credential bundle: non-empty strings `access_key_id`, `secret_access_key`, `session_token`, `region`, and integer `expires_at`. The token may not outlive its credentials: `exp ≤ aws.expires_at` |
| `sid`, `generation`, `session_deadline`, `grants` | optional, for the engine that refreshes the session: a non-empty string, a positive integer, integer seconds with `aws.expires_at ≤ session_deadline`, and an array. The server checks their types and the ordering, nothing more |

Everything else about the token is the ordinary contract: `ten`, `org`,
`cat`/`scope`, `prd` (with its lifetime cap), `rtg` and the audiences all
apply unchanged, and the profile is checked after them. A deployment that
wants a distinct audience for session tokens sets `HOGLAKE_AUTH_AUDIENCE`
accordingly; the verifier does not special-case one.

With the profile `off`, a token carrying any of its claims is refused as
`profile_not_enabled`, and that check runs before every other claim rule so
the reason is never shadowed — never accepted as a plain metadata token, because an
older or unconfigured hoglake granting on a token whose other half it does
not understand is the same widening the strict `prd` rule exists to prevent.
`hoglake_auth_session_profile_total{outcome}` counts `accepted`,
`refused_disabled` and `refused_invalid`.

### Scoped tokens, exactly

A `scoped` token is a tenant token narrowed to some values of a field, and the
verifier is strict about it. An issuer that wants its scoped tokens accepted
must emit all of the following:

- an integer `iat`, and `exp − iat` at most `HOGLAKE_AUTH_SCOPED_MAX_LIFETIME_SECONDS`
  (default 900; an issuer that mints longer-lived scoped tokens must raise it);
- a non-empty `cat` (or `scope`) whose scopes are only `read` or `write`; any
  other scope word refuses the token rather than being dropped;
- a `prd` object naming **exactly** the server's `HOGLAKE_AUTH_PREDICATE_FIELDS`,
  no more and no fewer. A server with no predicate fields refuses every scoped
  token;
- for each field, a non-empty object of at most 200 entries whose keys are
  canonical decimal ids that fit a signed 64-bit integer, each mapped to `read`
  or `write`; or the single key `*`. `*` may not be mixed with values.

A refusal names its reason in `WWW-Authenticate` (`missing_claim`,
`invalid_predicate`, …).

An `operator` token bypasses per-catalog checks and may create catalogs,
manage containment, and read operator-only routes. Operator tokens
are therefore only as safe as the keys in the JWKS: whoever can sign with them
is an operator.

### Errors

- `401` with `WWW-Authenticate: Bearer error=<reason>` for a missing, malformed,
  expired, mis-issued or mis-signed token, or one carrying the session profile
  where it is off (`profile_not_enabled`). The reason strings are part of the
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
- **Vend storage credentials.** See the next section: it may *fetch* them for
  its own jobs, through a configured provider, but never hands any out.

## Storage credentials

hoglake authorizes *metadata*. Reading and writing the parquet is between the
client and the object store, and the client brings its own credentials for it:
static keys, an instance or pod identity, or short-lived credentials its issuer
handed it.

The server's own jobs, the hydrator, compaction, snapshot expiry, quarantine and
cleanup, get their storage credentials from a **storage credential provider**,
chosen per deployment:

| provider | how the server's jobs get S3 credentials |
| --- | --- |
| `ambient` (default) | the SDK's default chain: a pod identity in Kubernetes, an instance profile, or the environment |
| `static` | `HOGLAKE_S3_ACCESS_KEY` and `HOGLAKE_S3_SECRET_KEY`: MinIO, local development, CI, the compose quickstart |
| `broker` | an HTTP call to `HOGLAKE_STORAGE_BROKER_URL`, authenticated with hoglake's own client credential (`HOGLAKE_STORAGE_BROKER_CLIENT_ID` / `_CLIENT_SECRET`) exchanged at `HOGLAKE_STORAGE_BROKER_TOKEN_URL`: `POST <url>` with `{"catalog", "prefix", "access": "read" \| "write" \| "delete"}`, answered with `{"access_key_id", "secret_access_key", "session_token", "expiration"}`, STS's shape (`session_token` may be absent for a broker that vends expiring keys instead). One session per catalog and access, held in memory and refreshed `HOGLAKE_STORAGE_BROKER_REFRESH_MARGIN_SECONDS` (default 300) before it expires; never written to the metadata store. `HOGLAKE_STORAGE_BROKER_ALLOW_HTTP=true` accepts `http` URLs to any host |

`HOGLAKE_STORAGE_PROVIDER` selects one. Unset, it is `static` when both keys
are set and `ambient` otherwise — what hoglake did before the knob existed, so
an existing install changes nothing. The combination is checked at boot: an
explicit `static` without both keys, an explicit `ambient` or `broker` beside
keys, or a `broker` missing a setting refuses to start.

`access` has three values, each including the one before: `read` (the
hydrator's footer fetches), `write` (compaction: its inputs' reads plus the
put and multipart upload of its output — and no delete, so a leaked or buggy
compaction session cannot remove a tenant's referenced files), and `delete`
(cleanup's drain of the removal queue, the one job that deletes). A broker
grants `s3:DeleteObject` only for `delete`. The token request is the
clients' client-credentials form, `grant_type=client_credentials` with
`audience` = `HOGLAKE_STORAGE_BROKER_AUDIENCE` (default `hoglake`). Both
URLs must be `https` — the client secret and every vended key cross that
hop — with `http` accepted only for `localhost`, `127.0.0.1` or `[::1]`,
the dev stack's and the test suite's in-process brokers; anything else
refuses to start, naming the one way around it:
`HOGLAKE_STORAGE_BROKER_ALLOW_HTTP=true`, which accepts `http` to any host
and is the operator's statement that the hop to the broker runs inside a
network they already trust — a broker's in-cluster listener reached by its
Service name, for instance. It is a knob, not a hostname pattern, so that
statement lives in the values file. The refresh lead is the margin or half the credential's
lifetime, whichever is shorter, for the session and for the bearer token
alike, so a 300-second session is still cached for 150 seconds rather than
re-vended on every request. A refresh that fails is not retried by the
next request: while the held session is valid the provider waits 5 seconds
before trying again, doubling to a minute, and resets on success.
`broker` is how a deployment puts the server's own S3 access behind the same
authority as its clients' access: the server then holds no cloud identity of
its own, and every session it uses is one the broker chose to grant and can
audit. PostHog runs it, pointed at hogtower; anyone can implement the shape in
front of their own STS wrapper or a secrets engine. Under `ambient` the
server's role reaches every catalog it serves, which is the normal trade for a
single-org install.

Whatever the provider, it serves only the server's own work: the background
jobs, and the same work when a caller with `maintenance` scope triggers it
through `POST /v1/catalogs/{catalog}/maintenance/compact` or
`…/maintenance/cleanup`, which run compaction or cleanup inline and read and
rewrite S3 with the server's credentials. Every one of those paths takes its
S3 client for ONE catalog: a compaction run and a cleanup drain are per
catalog by construction, and the hydrator's sweep, which claims files from
every catalog at once, holds one read session per catalog it touches. Under
`broker` a session is cached for the life of the process and refreshed in
place; a refresh that fails keeps the session already held for as long as it
is valid, and only a job with nothing valid to use fails. A client built
without a catalog refuses to do IO under `broker` rather than reaching for a
credential it has no grant for. Ordinary metadata requests never call the
provider and never read S3, and nothing fetches credentials for a client.

A deployment that wants storage access gated by the same authority as the
tokens does it on the issuer side: the issuer returns scoped, short-lived
storage credentials alongside the token, in the same response, and the client
uses them. That is how PostHog's deployment works. Storage credentials are never
placed inside the JWT: the JWT travels on every request and is forwarded to
the hosts of remote schemas, and a storage secret should not.

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
| millrace | `HOGLAKE_AUTH_TOKEN_URL`, `HOGLAKE_AUTH_CLIENT_ID`, `HOGLAKE_AUTH_CLIENT_SECRET` (+ `HOGLAKE_AUTH_AUDIENCE`), through pyhoglake | `HOGLAKE_AUTH_TOKEN` or `HOGLAKE_AUTH_TOKEN_FILE` |
| webui | — | sign in with a pasted token (in review) |

pyhoglake accepts any `httpx.Auth`, and the Trino connector's token source is an
interface. A deployment with a different grant, a device flow, a cloud
identity provider's SDK, or an exchange of some other credential, implements
one of those and nothing else changes. hogtower appears in the clients only as
the example token URL in their READMEs.

## Local issuer

For an install that wants real authentication and no extra service, the
server is its own issuer — holding one key and no clients, verify-only at
runtime. This is deliberately not an OAuth server inside hoglake; the earlier
design that put one there was retired for the reasons in
[auth-tenancy.md §17.2](auth-tenancy.md).

- `HOGLAKE_AUTH_MODE=local` is `enforce` with the public half of
  `HOGLAKE_AUTH_SIGNING_KEY` as the only trusted key. The key is env-only: a
  private EC P-256 JWK, base64url-encoded, that `hoglake token keygen` prints
  and the server never writes anywhere. A JWKS file or URL beside it is a
  boot error, as is the mode without the key. `HOGLAKE_AUTH_ISSUER` defaults
  to `hoglake` (an explicit value wins), `HOGLAKE_AUTH_AUDIENCE` to `hoglake`.
- The public half is served at `/.well-known/jwks.json`, so a hoglite backend
  or a sibling that verifies the same tokens fetches it the way it would any
  issuer's. The `kid` is the RFC 7638 thumbprint of the key, first eight
  bytes in hex — the same rule the contract vectors use.
- `hoglake token mint` signs a token offline with that key. It is an issuer
  like any other: its claims are checked against the contract vectors in the
  test suite, so a token it mints is exactly what the verifier next door
  accepts.
- Nothing listens for token requests, and the signing key is never loaded by
  the server for anything but its public half.

### Standalone, end to end

```bash
# 1. One key for the install. Keep the line somewhere the server and your
#    shell both read it (an env file, a secret manager).
hoglake token keygen            # prints HOGLAKE_AUTH_SIGNING_KEY=<value>
export HOGLAKE_AUTH_SIGNING_KEY=<value>

# 2. Start the server as its own issuer.
HOGLAKE_AUTH_MODE=local hoglake-server

# 3. Mint a token per client, offline, from the same environment.
hoglake token mint --catalog demo --scope write --sub analytics-job --ttl 7d
hoglake token mint --operator                                  # fleet-wide admin
hoglake token mint --operator --catalog demo --org local       # bound to one catalog
```

`hoglake` here is the server binary (`bin/hoglake-server token …` from the
distribution, `just server token …` from a checkout). The token goes into the
client's fixed-token setting — `BearerToken` in pyhoglake, `TOKEN` in the
DuckDB extension, `hoglake.auth.token` in the Trino connector — and the
client brings its own storage credentials, as it does under every issuer.
`--catalog` repeats, `name=scope` sets a catalog's scope, and `--ttl` takes
`90s`, `15m`, `24h` (the default) or `7d`. Scoped tokens (`prd`) are not
minted here: a standalone install has no second authority to narrow a token
by.

## Status

The cross-repository checklist of what is left is
[auth-roadmap.md](auth-roadmap.md).

Built: `off`, `warn`, `enforce`; ES256 and RS256 verification against a JWKS
file or URL; the four scopes and the route table; the `scope` claim; scoped
tokens with `prd` and a configurable lifetime cap; `ten` defaulting to
`tenant`; `rtg`; the public and metrics listeners; contract vectors generated
here; the `ambient`, `static` and `broker` storage providers behind the
server's own jobs; the local issuer (`local`, `hoglake token keygen` and
`mint`); client credentials and fixed tokens in pyhoglake, the DuckDB
extension, hedgerow, the Trino connector and millrace; the session profile's
verifier gate and vectors (`HOGLAKE_AUTH_SESSION_PROFILE`, proposed, off by
default).

Tracked in [#574](https://github.com/PostHog/hoglake/issues/574): a fixed-token
option in the Trino connector; a token for the web UI
([#560](https://github.com/PostHog/hoglake/pull/560)).

Decided, not tracked: `jti` stays required, and the server verifies one issuer
(both above).
