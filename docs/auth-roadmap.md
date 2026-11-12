# Auth roadmap

The one list of what is left to do on hoglake authentication, across every
repository involved, in the order it is being done. Tick a box in the PR that
completes it. The design is [auth.md](auth.md); the decisions behind it were
taken on 2026-11-10 and 2026-11-11.

**The rule:** hoglake is a resource server. It verifies tokens from whichever
issuer a deployment configures, owns the token contract and its vectors, and
never calls the issuer on the request path. Clients fetch a token and present
it. The server's own jobs get storage through a pluggable provider. In
PostHog's deployment the issuer and the storage broker are hogtower; outside
it, any JWKS issuer, or auth off, with `ambient` or `static` storage.

Each item is tagged with its repository: **hoglake**, **hogtower** (PostHog's
issuer), **trino** (the hoglake connector in PostHog/trino), **millpond**,
**charts** (PostHog's deployment). Status is as of 2026-11-11; "approved"
means the PR has review approval and is waiting on CI or a merge.

## Phase 0 — write the design down

- [x] **hoglake** `docs/auth.md`, the canonical description, and the stale
      texts brought in line (README, AGENT.md, OpenAPI, pyhoglake, client
      READMEs, `auth-tenancy.md` banner) — [#575](https://github.com/PostHog/hoglake/pull/575)
- [x] **hoglake** The storage credential provider for the server's own jobs
      (`ambient` | `static` | `broker`) — [#579](https://github.com/PostHog/hoglake/pull/579)
- [x] **trino** Connector docs: the `hoglake.auth.*` client-credentials
      properties and an Authentication section — [PostHog/trino#195](https://github.com/PostHog/trino/pull/195)
- [x] **millpond** README: millpond sends no token; hoglake authenticates
      when its mode is on — [PostHog/millpond#166](https://github.com/PostHog/millpond/pull/166)
- [x] **hoglake**, **hogtower** The hoglake server never calls the issuer on
      the request path: [#565](https://github.com/PostHog/hoglake/pull/565)
      and [PostHog/hogtower#181](https://github.com/PostHog/hogtower/pull/181)
      closed; a mount host receives the caller's own token
- [x] **hogtower** The PostHog profile doc, and the design docs amended
      (broker kept, status refreshed, stale "no authentication" claims
      corrected) — [PostHog/hogtower#185](https://github.com/PostHog/hogtower/pull/185)
- [x] Tracking: [#574](https://github.com/PostHog/hoglake/issues/574),
      [PostHog/millpond#165](https://github.com/PostHog/millpond/issues/165),
      and this file

## Phase 1, wave 1 — hoglake owns the contract; standard issuers; storage with the token

- [x] **hoglake** Contract vectors generated here from the verifier, with a
      neutral issuer and per-vector `expect` verdicts, verifier-only vectors
      marked with `mutation`; `ten` optional (a `ten`-less token carrying
      `prd` is refused); `HOGLAKE_AUTH_SCOPED_MAX_LIFETIME_SECONDS` replaces
      the hard-coded 900s; no issuer name in server code or tests, with
      `HOGLAKE_AUTH_CLIENT_CLASSES` for the `client` metric label —
      [#577](https://github.com/PostHog/hoglake/pull/577)
- [x] **hoglake** RS256 accepted beside ES256, RSA keys in the JWKS, key type
      matched to the token's `alg`, other-purpose entries skipped; the
      `scope` claim (`hoglake:<catalog>:<scope>`) accepted as equivalent to
      `cat` and refused when the two disagree —
      [#576](https://github.com/PostHog/hoglake/pull/576)
- [x] **hogtower** `storage` returned alongside the token in the
      `/oauth/token` response with `include_storage=true`, behind a flag;
      `exp` never later than the session; bounded wait so the public listener
      always answers — [PostHog/hogtower#186](https://github.com/PostHog/hogtower/pull/186)
- [x] **hoglake** `docs/auth.md` refreshed once the two hoglake PRs land:
      RS256, `scope`, `ten`-optional, the configurable cap and
      hoglake-generated vectors from "planned" to "built"; `cat` and `scope`
      must agree; a routed (`rtg`) token still needs exactly one `cat` entry;
      JWKS entries with `use` ≠ `sig` or a foreign `alg` are skipped; the two
      new variables in the environment table
- [x] **hogtower** Conform to hoglake's contract vectors instead of
      generating them: the copied file plus a test that it matches, with
      hoglake's issuer, kid, foreign audience, operator subject and `expect`
      fields — [PostHog/hogtower#197](https://github.com/PostHog/hogtower/pull/197)
- [x] **charts** `HOGLAKE_AUTH_CLIENT_CLASSES=hogtower` on every hoglake
      deployment, so the `client="hogtower"` metric series survives the
      issuer-neutral User-Agent map — [PostHog/charts#20181](https://github.com/PostHog/charts/pull/20181)

## Phase 1, wave 2 — standalone hoglake, and storage through the broker

- [ ] **hoglake** `HOGLAKE_AUTH_MODE=local`: the server trusts its own
      signing key (`HOGLAKE_AUTH_SIGNING_KEY`) and serves its public half at
      `/.well-known/jwks.json`
- [ ] **hoglake** `hoglake token keygen` and `hoglake token mint --catalog …
      --scope … --ttl … [--operator]`, offline, no HTTP endpoint
- [ ] **hoglake** The standalone path documented end to end: `local`, a
      minted fixed token in each client, the client's own storage credentials
- [x] **hoglake** The storage provider interface behind the hydrator,
      compaction, snapshot expiry, quarantine and cleanup, and the manual
      maintenance routes: `HOGLAKE_STORAGE_PROVIDER=ambient|static|broker`
      (expiry and storage usage touch no S3; quarantine is the hydrator's)
- [x] **hoglake** `broker`: `POST <url>` with `{catalog, prefix, access}` under
      OAuth client credentials, answered with an STS-shaped session; one
      session per job, one catalog, in memory, refreshed before expiry
- [ ] **hogtower** The broker's server side: a service credential per hoglake
      deployment bound to its id; the vend refuses a catalog that does not
      live on the calling hoglake and never returns more than one catalog
- [ ] **hoglake** Decide whether `jti` stays required; multi-issuer
      verification, or document running issuers behind one `iss`

## Phase 2 — every client authenticates

- [ ] **trino** A fixed-token option (`hoglake.auth.token` / `token-file`)
      beside client credentials
- [ ] **millpond** `HOGLAKE_AUTH_*` settings for client credentials or a fixed
      token, and the auth path in the startup probe —
      [PostHog/millpond#165](https://github.com/PostHog/millpond/issues/165)
- [ ] **hoglake** millrace: client credentials or a fixed token through
      pyhoglake
- [ ] **hoglake** webui: a way to present a token —
      [#560](https://github.com/PostHog/hoglake/pull/560)
- [x] **hoglake** pyhoglake, the DuckDB extension and hedgerow: issuer-neutral
      docs and the extension point described —
      [#575](https://github.com/PostHog/hoglake/pull/575)

## Phase 3 — Trino users, and PostHog's rollout

- [ ] **hogtower** Trino per-login tokens on the hogtower side: PostHog-user
      logins, with a membership version in the cache key
- [ ] **trino** Per-session tokens: exchange the catalog credential plus the
      session user at the token URI (RFC 8693), cached per user
- [ ] **trino** Per-session storage: hand the session's vended credentials to
      workers as extra credentials instead of the catalog's static filesystem
      credentials
- [ ] **hogtower** Mounts: the reader presents a token scoped to the mount;
      the host forwards it (what replaces #181)
- [ ] **hogtower** Signing-key rotation (`rotate-key`)
- [ ] **charts** `HOGLAKE_STORAGE_PROVIDER=broker` and its settings on
      PostHog's hoglake deployments, once the provider and the vend exist
- [ ] **charts**, **hogtower** Rollout: the shared hoglake from `warn` to
      `enforce` in dev; the Trino token URL everywhere; the customer hoglake
      service in prod; prod from `off` to `warn` to `enforce` once millpond,
      millrace and the webui authenticate
