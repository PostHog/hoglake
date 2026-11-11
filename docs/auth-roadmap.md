# Auth roadmap

The one list of what is left to do on hoglake authentication, across every
repository involved. Tick a box in the PR that completes it. The design is
[auth.md](auth.md); the decisions behind it were taken on 2026-11-10 and
2026-11-11.

**The rule:** hoglake is a resource server. It verifies tokens from whichever
issuer a deployment configures, owns the token contract and its vectors, and
never calls the issuer on the request path. Clients fetch a token and present
it. The server's own jobs get storage through a pluggable provider. In
PostHog's deployment the issuer and the storage broker are hogtower; outside
it, any JWKS issuer, or auth off, with `ambient` or `static` storage.

Status is as of 2026-11-11. "Approved" means the PR has review approval and
is waiting on CI or a merge.

## hoglake

Docs

- [x] `docs/auth.md`, the canonical description, and the stale texts brought
      in line (README, AGENT.md, OpenAPI, pyhoglake, client READMEs,
      `auth-tenancy.md` banner) — [#575](https://github.com/PostHog/hoglake/pull/575)
- [x] The storage credential provider for the server's own jobs
      (`ambient` | `static` | `broker`) — [#579](https://github.com/PostHog/hoglake/pull/579)
- [ ] After the contract and identity-provider PRs below land: move RS256,
      the `scope` claim, `ten`-optional, the configurable scoped cap and
      hoglake-generated vectors from "planned" to "built" in `docs/auth.md`;
      say that `cat` and `scope` must agree when both are present, that a
      routed (`rtg`) token still needs exactly one `cat` entry, that JWKS
      entries with `use` ≠ `sig` or a foreign `alg` are skipped, and add
      `HOGLAKE_AUTH_SCOPED_MAX_LIFETIME_SECONDS` and
      `HOGLAKE_AUTH_CLIENT_CLASSES` to the environment table

Contract ownership — [#577](https://github.com/PostHog/hoglake/pull/577), approved

- [ ] Contract vectors generated here from the verifier, with a neutral
      issuer and per-vector `expect` verdicts; verifier-only vectors marked
      with `mutation`
- [ ] `ten` optional: absent means `tenant`; a `ten`-less token carrying `prd`
      is refused
- [ ] `HOGLAKE_AUTH_SCOPED_MAX_LIFETIME_SECONDS` replaces the hard-coded 900s
- [ ] No issuer name in server code or tests; `HOGLAKE_AUTH_CLIENT_CLASSES`
      names a deployment's own clients for the `client` metric label

Standard identity providers — [#576](https://github.com/PostHog/hoglake/pull/576), approved

- [ ] RS256 accepted beside ES256; RSA keys in the JWKS; key type matched to
      the token's `alg`; entries for other purposes skipped
- [ ] The `scope` claim (`hoglake:<catalog>:<scope>`) accepted as equivalent
      to `cat`; refused when the two disagree
- [ ] Decide whether `jti` stays required
- [ ] Multi-issuer verification, or document running issuers behind one `iss`

Local issuer

- [ ] `HOGLAKE_AUTH_MODE=local`: the server trusts its own signing key
      (`HOGLAKE_AUTH_SIGNING_KEY`) and serves its public half at
      `/.well-known/jwks.json`
- [ ] `hoglake token keygen` and `hoglake token mint --catalog … --scope …
      --ttl … [--operator]`, offline, no HTTP endpoint
- [ ] The standalone path documented end to end: `local`, a minted fixed
      token in each client, the client's own storage credentials

Storage provider

- [ ] The provider interface behind the hydrator, compaction, snapshot expiry,
      quarantine and cleanup, and the manual maintenance routes:
      `HOGLAKE_STORAGE_PROVIDER=ambient|static|broker`
- [ ] `broker`: `POST <url>` with `{catalog, prefix, access}` under OAuth
      client credentials, answered with an STS-shaped session; one session
      per job, one catalog, in memory, refreshed before expiry

Clients

- [ ] millrace: client credentials or a fixed token through pyhoglake
- [ ] webui: a way to present a token — [#560](https://github.com/PostHog/hoglake/pull/560)
- [x] pyhoglake, the DuckDB extension and hedgerow: issuer-neutral docs and
      the extension point described — [#575](https://github.com/PostHog/hoglake/pull/575)
- [x] Mounts: a host receives the caller's own token; no server-side exchange
      — [#565](https://github.com/PostHog/hoglake/pull/565) closed

Tracking issue: [#574](https://github.com/PostHog/hoglake/issues/574).

## hogtower (PostHog's issuer)

- [ ] The PostHog profile doc and the broker decision recorded in the design
      docs — [#185](https://github.com/PostHog/hogtower/pull/185), approved
- [ ] `storage` returned alongside the token in the `/oauth/token` response
      with `include_storage=true`, behind a flag; `exp` never later than the
      session; bounded wait — [#186](https://github.com/PostHog/hogtower/pull/186), approved
- [ ] Conform to hoglake's contract vectors instead of generating them: the
      copied file plus a test that it matches, with hoglake's issuer, kid,
      foreign audience, operator subject and `expect` fields
- [ ] Storage for hoglake's own jobs: a service credential per hoglake
      deployment bound to its id; the vend refuses a catalog that does not
      live on the calling hoglake and never returns more than one catalog
- [ ] Trino per-login tokens: PostHog-user logins on the hogtower side, with
      a membership version in the cache key (the connector half is under
      PostHog/trino below)
- [ ] Mounts: the reader presents a token scoped to the mount; the host
      forwards it — replaces [#181](https://github.com/PostHog/hogtower/pull/181), closed
- [ ] Signing-key rotation (`rotate-key`)
- [ ] Rollout: the shared hoglake from `warn` to `enforce` in dev; the Trino
      token URL everywhere; the customer hoglake service in prod; prod from
      `off` to `warn` to `enforce` once millpond, millrace and the webui
      authenticate

## PostHog/trino (the hoglake connector)

- [x] Connector docs: the `hoglake.auth.*` client-credentials properties and
      an Authentication section — [#195](https://github.com/PostHog/trino/pull/195)
- [ ] A fixed-token option (`hoglake.auth.token` / `token-file`) beside client
      credentials
- [ ] Per-session tokens: exchange the catalog credential plus the session
      user at the token URI (RFC 8693), cached per user
- [ ] Per-session storage: hand the session's vended credentials to workers as
      extra credentials instead of the catalog's static filesystem credentials

## millpond

- [x] README: millpond sends no token; hoglake authenticates when its mode is
      on — [#166](https://github.com/PostHog/millpond/pull/166)
- [ ] `HOGLAKE_AUTH_*` settings for client credentials or a fixed token, and
      the auth path in the startup probe — [#165](https://github.com/PostHog/millpond/issues/165)

## Deployment (PostHog/charts)

- [ ] `HOGLAKE_AUTH_CLIENT_CLASSES=hogtower` on every hoglake deployment, so
      the `client="hogtower"` metric series survives the issuer-neutral
      User-Agent map (needs the server release carrying hoglake#577)
- [ ] `HOGLAKE_STORAGE_PROVIDER=broker` and its settings on PostHog's hoglake
      deployments, once the provider and the vend exist
- [ ] The shared hoglake's `auth.mode` from `warn` to `enforce`, then prod
