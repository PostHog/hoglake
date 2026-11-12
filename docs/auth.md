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
| `HOGLAKE_AUTH_CLIENT_CLASSES` | comma-separated User-Agent product tokens, each counted under its own `client` label on `hoglake_auth_requests_total` instead of `other`; at most 16, lowercase, never `none` or `other`. A deployment names its own clients here, so the server knows no issuer by name |

One issuer and one key set per server: every token's `iss` must equal
`HOGLAKE_AUTH_ISSUER`. Several signing services can share one JWKS only if they
also share that one issuer identity; distinct issuers need separate servers
until multi-issuer verification exists (tracked).

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
| `jti` | yes | token id, for correlating an audit line with a token |
| `ten` | no; absent means `tenant` | the token kind: `tenant`, `scoped` or `operator`. A token without `ten` that carries `prd` is refused rather than read as a tenant token |
| `org` | tenant and scoped, today | the tenant the token belongs to, for the audit log only; hoglake does not authorize on it. Required by the verifier today; making it optional is tracked |
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
| `rtg` | verified placement of the single catalog the token names on a configured backend (`b`, `be`, and `t` for hoglite); mismatches answer 401 `unknown_backend` |

### Backends that verify the token themselves

A catalog placed on a hoglite backend (`HOGLAKE_BACKENDS_FILE`; it requires
`warn` or `enforce`) is served by a separate process that verifies the token
on its own. hoglake forwards the current request's verified JWT to that
backend unchanged and nothing else: no credential of hoglake's own, and no
caller credential shared across requests. The backend verifies it offline
against the same JWKS, requires its own audience to be present, and binds the
request to the backend and tenant the `rtg` claim names. It refuses `scoped`
tokens and any `prd` constraint, because raw SQL cannot preserve them.
Ordinary reads and writes never provision a tenant or bootstrap a schema
there; only a catalog-bound `operator` request provisions or deletes. An
issuer that starts adding the backend audience to tokens it already mints
should let the older single-audience tokens expire before the backend
enforces.

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
| `broker` | an HTTP call to `HOGLAKE_STORAGE_BROKER_URL`, authenticated with hoglake's own client credential (`HOGLAKE_STORAGE_BROKER_CLIENT_ID` / `_CLIENT_SECRET`) exchanged at `HOGLAKE_STORAGE_BROKER_TOKEN_URL`: `POST <url>` with `{"catalog", "prefix", "access": "read" \| "write" \| "delete"}`, answered with `{"access_key_id", "secret_access_key", "session_token", "expiration"}`, STS's shape (`session_token` may be absent for a broker that vends expiring keys instead). One session per catalog and access, held in memory and refreshed `HOGLAKE_STORAGE_BROKER_REFRESH_MARGIN_SECONDS` (default 300) before it expires; never written to the metadata store |

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
refuses to start. The refresh lead is the margin or half the credential's
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
extension, hedgerow and the Trino connector.

Tracked in [#574](https://github.com/PostHog/hoglake/issues/574): a fixed-token
option in the Trino connector; client credentials in millrace; the `jti` and
multi-issuer decisions.
