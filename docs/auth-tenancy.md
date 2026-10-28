# Auth, tenancy and S3 data-plane entitlement

**Status:** §6.1–6.5, §7 and §11 are implemented by the first PR of the stack
(the enforcement layer: JWT verification, the route-to-scope table, per-catalog
`Caller`, the `off|warn|enforce` mode flag, filtered listings, and the death of
the client-supplied `author`).

**§17 supersedes §4, §5, §6.6, §10, §14 and §15.** Reading `PostHog/hogtower`
showed that hoglake is not the right home for issuance or S3 vending: the
tenant→catalog→IAM-role→bucket mapping, an `sts:AssumeRole` broker, the
warehouse-scoped machine credentials and an ES256 verifier all already exist
there. hoglake stays a resource server, and the rest of this document is the
reasoning that got here rather than the build plan. Read §17 before §5 or §14.
Replaces the one-line position in
[AGENT.md](../AGENT.md) §Known deferrals ("Auth: out of scope for v1; audit actor
is `anonymous` until it lands") and the matching sentence in the OpenAPI
`info.description`.

**Scope:** an OAuth2-style credential plane for hoglake that (a) authenticates
a caller as a tenant, (b) authorizes catalog *metadata* reads and writes down to
the object level, and (c) vends short-lived, prefix-confined S3 credentials so
the caller can read and write the parquet itself. Data never flows through the
control plane; that constraint is the whole reason (c) is its own problem rather
than a consequence of (a) and (b).

---

## 0. The ask, restated precisely

Three separable capabilities, often conflated:

| # | Capability | What breaks without it |
|---|---|---|
| **A** | **Identity** — prove the caller is org X | Any caller can act as any tenant; the audit log says `anonymous` |
| **B** | **Catalog authorization** — org X may see the metadata for catalogs it owns and nothing else | `GET /v1/catalogs` enumerates the whole fleet; every route is `open` |
| **C** | **Data-plane entitlement** — org X may read/write only `s3://<bucket>/<org-prefix>/` for a bounded time | Today the *only* thing confining a write is a string prefix check in `CommitService`; the *client's own S3 credentials* are the real gate, and for the Trino path those are the **pod's** credentials, i.e. everyone's |

A JWT solves A and B cheaply. C is where the engineering is, because a JWT is
not an AWS principal. The rest of this document is mostly about C.

---

## 1. What exists today (verified in this tree)

This section is the baseline. Everything below is either a fact I read out of
the source or an inference I label as one.

### 1.1 The server has no authentication at all

- No `Authorization` header is read anywhere in `server/src/main/kotlin`. No
  Ktor auth plugin is installed; `App.module()` wires `RequestId`,
  `CallLogging`, `StatusPages`, `ContentNegotiation`, `MicrometerMetrics` and a
  set of flat `install*Routes` functions, and nothing else.
- `server/build.gradle.kts` has no `ktor-server-auth*` and no JWT library on the
  classpath.
- `Audit.event(actor = ...)` defaults to `"anonymous"`
  (`observability/Audit.kt`), and every caller passes a *client-supplied*
  `author` — from the create-catalog body, from a `?author=` query param on
  drop, from the commit payload. Attribution is therefore forgeable by
  construction, which also means the audit trail is not evidence today.
- `openapi/hoglake.yaml` `info.description` says "Auth is out of scope for v1."
- `README.md` §Status: "Authentication is still out of scope."

### 1.2 Tenancy exists implicitly, as the catalog name

- `hog_catalog.name` is globally unique with
  `CHECK (name ~ '^[a-z0-9][a-z0-9_-]{0,62}$')`. `V13__numeric_catalog_names.sql`
  widened that from a stricter shape specifically to admit **UUID** names.
- The duckgres Trino provisioner creates one Trino catalog per org with
  `hoglake.catalog = orgID`
  (`duckgres/controlplane/provisioner/trino_provisioner.go:1913-1920`).
- **Inference, high confidence:** catalog name == PostHog org UUID, one catalog
  per org, one hoglake instance per region (Gigahog dev / prod-us / prod-eu).
  There is no `hog_tenant`, no `org_id` column, and no ownership row — the name
  *is* the tenancy key, and nothing in the schema records who owns it.

### 1.3 S3 confinement is already enforced — for writes, at the metadata layer

This is the single most important existing building block:

- `CommitService.validatePathsUnderDataPath` (≈ line 944-975) refuses a commit
  whose registered paths fall outside the catalog's `data_path`, with a typed
  message. It is invoked on every commit path (lines ~323, ~464, ~547).
- `CatalogService.createCatalog` (lines 93-126) refuses a `data_path` that
  prefix-overlaps **any** existing catalog's, in both directions, because
  cleanup's drain-time liveness check is per-catalog — two catalogs sharing a
  prefix would let A's drain physically delete an object B still references.
- `validateDataPath` bounds length (512), refuses whitespace/control chars and
  dot segments, and requires an `s3://` URI.

So *"a catalog cannot write outside its own prefix, and prefixes are disjoint"*
is already a repository invariant. What is missing is that **`data_path` itself
is caller-supplied**, so the invariant is only as strong as the honesty of the
caller who creates the catalog.

### 1.4 Every S3 driver on the server uses one global credential

- `ObjectStore(cfg)` is constructed **once** in `App.kt:161` and shared by the
  hydrator (footer reads), compaction (rewrite input/output) and everything
  else. `cfg.s3AccessKey`/`s3SecretKey`/`s3Endpoint`/`s3Region` come from
  `HOGLAKE_S3_*` env vars.
- When those are blank (the in-cluster case) the SDK falls back to
  `DefaultCredentialsProvider`, whose IRSA link is why `software.amazon.awssdk:sts`
  is a **`runtimeOnly`** dependency (`server/build.gradle.kts:84-87` — with a
  fifteen-line comment about the failure mode when it is missing).
- Consequence: the hoglake pod's IAM role can read and write **every tenant's**
  prefix. Compaction, the hydrator and cleanup all cross tenant boundaries by
  design, on one credential.

### 1.5 The Trino path has no tenant S3 boundary for hoglake

The duckgres provisioner builds catalog properties in
`buildCatalogProperties` (`trino_provisioner.go:1908-1942`):

```go
if p.hoglakeURI != "" {
    return map[string]string{
        "connector.name":    "hoglake",
        "hoglake.uri":       p.hoglakeURI,
        "hoglake.catalog":   orgID,
        "fs.cache.enabled":  strconv.FormatBool(p.filesystemCacheEnabled),
        "hoglake.s3.region": region,
    }
}
// ducklake path below:
    "s3.auth-type":      "IAM_ROLE",
    "s3.iam-role":       d.IAMRoleARN,   // "the per-org duckling role,
                                          //  assumed per catalog — the
                                          //  tenant S3 boundary"
```

and the field's own doc comment says it plainly
(`trino_provisioner.go:369`): *"HoglakeURI selects Hoglake catalogs using the
pod's S3 credentials."*

The DuckLake path **already has** the pattern this plan needs — a per-org IAM
role assumed per catalog, deliberately never a secret in the properties map
(because "Trino logs the full CREATE CATALOG statement and renders catalog
properties in its UI"). The hoglake path has none of it: Trino reads every org's
parquet with the shared worker role, and hoglake authenticates nobody.

### 1.6 Clients

| Client | Where a credential would go | Notes |
|---|---|---|
| `pyhoglake` | one `httpx.Client(headers=…)` at `client.py:689` | Single seam. S3 handled separately via `S3Config` → pyarrow `S3FileSystem` (`client.py:495-610`) |
| DuckDB extension | `AddExtensionOption` in `hoglake_extension.cpp:20` (`hoglake_default_endpoint`, retry knobs) | Needs a token option plus a secret, or a file-based one |
| Trino connector (`PostHog/trino`) | catalog properties, no secrets allowed | Needs the secretless path; see §8.4 |
| webui | same-origin `fetch` (`api/client.ts:78`) | Today an operator console over *all* catalogs |
| `millrace` | `pyhoglake` underneath | Service-to-service writer |
| `hedgerow` | `pyhoglake` underneath | Replication daemon |

### 1.7 Non-catalog routes leak the fleet

Routes that are not under `/v1/catalogs/{catalog}` and currently return
fleet-wide data:

| Route | Leaks |
|---|---|
| `GET /v1/catalogs` | the full catalog list |
| `GET /v1/info` | instance-wide `total_rows`, `total_size_bytes` |
| `GET /v1/maintenance/status`, `/v1/maintenance/runs` | every catalog's status and run history |
| `GET /v1/database/health` | indexes, bloat, replication posture |
| `POST /v1/debug/decode-bound` | internal bound codec |
| `GET /metrics` | Prometheus, with per-catalog labels |
| `GET /openapi.yaml` | benign, but confirms the deployment |

---

## 2. Threat model

Who we are defending against, and what each gets today.

| Adversary | Today | After this plan |
|---|---|---|
| **Org X's own integration going wrong** (a misconfigured writer, a bad retry loop) | Can write into *any* catalog on the instance, because nothing checks | Confined to X's catalogs; over-scoped writes are a 403 at the commit boundary **and** an S3 `AccessDenied` |
| **Org X reading org Y's metadata** (table names, schemas, file counts, storage) | Free: `GET /v1/catalogs`, then any path | 404/403; `listCatalogs` filters |
| **Org X reading org Y's parquet** | Free if X holds any credential with object-read on the bucket — including, in the Trino case, *our own pod role*, which customers can reach through a query | Vended creds are prefix-confined per request |
| **Stolen customer credential** | N/A (none exist) | Bounded TTL (≤15 min token, ≤60 min STS), revocable credential epoch, per-tenant CloudTrail attribution |
| **Internet at large** | Full control plane, if reachable | 401 on everything; `/metrics` and `/debug/*` moved off the public listener |
| **Tenant DoS on the shared request pool** | Trivial: `RequestDispatcher` is one global bounded pool (`Config.requestThreads`) | Per-tenant concurrency + rate limits |
| **A bug in our own path guard** | Writes anywhere in the bucket | The same bug now also has to defeat the S3 policy, which is not our code |

Two things this plan deliberately does **not** claim to fix:

- **Confidentiality from the platform itself.** We operate the credentials and
the buckets. This is tenant isolation between customers, not against PostHog.
- **Data residency.** `aud` binds a token to one region's issuer, but residency
is a bucket/deployment property outside this design.

---

## 3. Decisions (with the alternatives that lost)

**D1. The tenant is the org; a catalog belongs to exactly one org.**
Keep `catalog name == org UUID` as the convention (it is what duckgres already
programs Trino with), but record ownership explicitly in a new `hog_tenant`
row rather than inferring tenancy from a naming convention. An org may own more
than one catalog (a `-dev` catalog, per-environment catalogs); the mapping is
1:N, keyed on `catalog_id`. Rationale: inferring tenancy from a name is fine
until someone renames, and the name is a `text` column with a regex CHECK, not a
foreign key.

*Rejected:* one catalog per org forever (blocks a dev catalog and forces
cross-org sharing later); a separate `hog_org` table with catalogs pointing at
it (a third entity for no benefit while catalogs are 1:1 with orgs in practice).

**D2. hoglake is its own authorization server (an OAuth2 AS), not just a
resource server.**

It mints tokens via `client_credentials` and, in-cluster, via
`urn:ietf:params:oauth:grant-type:jwt-bearer` (RFC 7523) from a Kubernetes
projected service-account token. It is also a resource server: it verifies its
own tokens, plus (phase 5) tokens from a configured external issuer (PostHog
Django) via JWKS.

*Why not "PostHog mints everything":* PostHog does not know hoglake's catalog
topology, so its token could only carry `org`, and every hoglake request would
still need a server-side `org → catalog` resolution plus a second
`catalog → S3 prefix` resolution. That is a DB round trip per request for
information the AS can bake into an `exp`-scoped token once. Also, machine
writers (millrace, hedgerow) have no PostHog identity to mint one from.

*Why not "hoglake only verifies, Keycloak/Auth0 mints":* a fourth control plane
to operate, and the S3 vending has to live somewhere anyway.

**D3. Scopes are per-catalog, not global.**
A flat `scope: "read write"` string means a token minted for the `-dev` catalog
reads production. The claim is a map `catalog → [scopes]`, so authorization is
a set-membership test on one key and a tenant never has to be trusted to pick
the right value.

**D4. S3 entitlement is vended as STS credentials, not presigned URLs and not a
proxy.**
Full analysis in §5. Summary: presigned URLs break down at scan-plan scale
(10^5 files) and cannot express a write; a proxy puts 2 PB of reads through the
control plane.

**D5. The S3 grant is derived from the same claims as the metadata grant, by one
authority.**
This is the load-bearing decision. It is what makes the token meaningful: if
Trino instead assumed the per-org role itself via `s3.iam-role` (the DuckLake
pattern), the JWT would govern metadata while an *independent* IAM grant
governed data, and the two would drift — a customer scoped to read-only in the
catalog could still overwrite parquet with their IAM role. Vending costs one
STS call per (token, catalog) per hour and buys a single authority.

**D6. No credential ever appears in a JWT, and no credential appears in a
catalog property.**
JWTs are logged, put in URLs by careless clients, and cached in browsers. STS
credentials go in a response **body** from a dedicated endpoint. Trino gets its
token from a mounted projected SA token, exchanged by the connector — never from
`CREATE CATALOG` properties (duckgres already states this rule in a comment and
it is not a coincidence it is written there).

**D7. Auth defaults to `enforce` on a fresh deployment and `off`/`warn` on an
existing one.**
See §11. A single `HOGLAKE_AUTH_MODE` with three values, not a boolean, so the
`warn` burn-in is an explicit, observable state rather than a code comment.

---

## 4. The tenancy model

### 4.1 New table: `hog_tenant`

One row per catalog, carrying the object-store entitlement. Deliberately a
**separate table from `hog_catalog`**: `hog_catalog`'s row is UPDATEd on every
single commit (allocators) and the schema's own header warns that indexing it or
widening it turns the hottest row in the catalog into a vacuum problem.

```sql
CREATE TABLE hog_tenant (
    catalog_id   bigint PRIMARY KEY REFERENCES hog_catalog ON DELETE CASCADE,
    org_id       uuid   NOT NULL,

    -- The object-store entitlement, server-side only. A client may never
    -- propose these; they are configuration, not request data (§6.6).
    s3_bucket    text   NOT NULL CHECK (s3_bucket ~ '^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$'),
    s3_prefix    text   NOT NULL CHECK (s3_prefix ~ '^[A-Za-z0-9._/-]*$'),
    s3_region    text   NOT NULL,
    iam_role_arn text   NOT NULL CHECK (iam_role_arn ~ '^arn:aws:iam::[0-9]{12}:role/'),

    -- Effective floor: a request may not ask for credentials with a
    -- later expiry than this, and a token whose iat is below
    -- revoked_before is refused without a denylist lookup.
    revoked_before        timestamptz,
    credential_epoch      bigint NOT NULL DEFAULT 1,

    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX hog_tenant_org ON hog_tenant (org_id);

-- A tenant's own data_path must equal the entitlement it is granted.
-- Enforced in the service (it needs the hog_catalog row), stated here
-- as the reason the two are not merged.
```

`org_id` is denormalized onto every catalog row rather than living in an
`hog_org` table: the only operations that need it are "list my catalogs" (index
scan) and "resolve this catalog's org" (PK lookup).

### 4.2 `data_path` becomes derived, not supplied

Today `POST /v1/catalogs` takes `dataPath` from the body. Under auth:

- The body's `dataPath` is **removed from the customer-facing contract**.
- The server composes it: `s3://{s3_bucket}/{s3_prefix}/{org_id}/`, with the
  per-tenant rows coming from the caller's provisioning request.
- `validatePathsUnderDataPath` and the disjointness check keep doing exactly
  what they do now; they simply stop depending on caller honesty.

**This closes a real hole, not a theoretical one.** The disjointness check is
against *existing* catalogs. If the first caller on a fresh instance creates its
catalog with `data_path = s3://the-bucket/` (bucket root — legal today), then
(a) it can register files anywhere in the bucket, and (b) every subsequent
catalog create is refused for overlap. One tenant can therefore own the bucket
and deny onboarding to everyone after it.

### 4.3 Backfill

`V29` cannot know each catalog's org UUID, bucket layout or role ARN. It must
therefore be paired with an operator-provided mapping (a one-shot admin endpoint
or a `--backfill-tenant` CLI taking a CSV/JSONL of `catalog,
org_id,bucket,prefix,region,role`). Two states:

- **`hog_tenant` row absent** → in `warn` mode the catalog behaves as today and
  every request emits `auth_unmapped_catalog_total`; in `enforce` mode it is
  unreachable (404 from a customer token, reachable from `platform:admin`).
- Existing `data_path` values are carried verbatim into `s3_bucket`/`s3_prefix`
  by the backfill and then asserted by a test to equal the composed value, so
the derived-path rule holds for grandfathered catalogs too.

---

## 5. The S3 data-plane entitlement (§C — the hard part)

### 5.1 Options considered

| Option | How it works | Why it wins / loses |
|---|---|---|
| **A. STS AssumeRole + session policy** | hoglake calls `sts:AssumeRole` on a per-tenant role with an inline session policy limited to `arn:aws:s3:::{bucket}/{prefix}/{org}/*`, returns `{accessKeyId, secretAccessKey, sessionToken, expiration}` | **Chosen.** Works with every client we have (boto3/pyarrow, DuckDB httpfs, Trino's native S3, the AWS SDK). Confines writes, not just reads. Per-session CloudTrail attribution. STS is *already* on the runtime classpath. |
| **B. Presigned GET per file, in the scan plan** | `ScanFile.data_file.path` becomes a time-limited URL | A scan plan is 10^4–10^5 files: response size and signing cost blow up, URLs leak into logs and caches, TTL caps long scans, and PUT/multipart has no clean presign story. Rejected for the general path; keep `/export` as the one place presigned URLs are right. |
| **C. Read-through proxy** | clients GET bytes from hoglake | 2 PB of egress and connection state through the control plane. Absurd. Rejected. |
| **D. Trino assumes the per-org role directly (`s3.auth-type=IAM_ROLE`)** | copy the DuckLake pattern | Cheapest to ship and it is already proven in this fleet — but it splits authority (D5): IAM says yes while the catalog says read-only, and drift is invisible. Keep as an emergency-rollback path, not the design. |
| **E. S3 Access Points / Access Grants** | an AP per tenant with an AP policy | Not mutually exclusive with A, and the right *phase 5 hardening*: per-tenant network origin, per-tenant AP policy, AP-level alarms, and no wildcard in a shared bucket. Costs N Access Points and a provisioning step. |
| **F. One bucket per tenant** | structural isolation | Best isolation, worst operations (N buckets, N lifecycle policies, cross-account policies, per-bucket quotas). Recommend as the **default for new large tenants**, layered under A. |

### 5.2 The vending flow

```
client                        hoglake                     AWS STS            S3
  │                              │                           │                │
  ├─ POST /v1/oauth/token ──────▶│                           │                │
  │  grant_type=client_creds     ├─ verify secret (argon2)   │                │
  │  client_id, client_secret    ├─ load tenant + scopes     │                │
  │◀── access_token (JWT, 10m) ──┤                           │                │
  │                              │                           │                │
  ├─ GET /scan ─────────────────▶│                           │                │
  │  Authorization: Bearer …     ├─ verify JWT (local)       │                │
  │                              ├─ authorize catalog:read   │                │
  │◀── plan: s3://bkt/org/…──────┤                           │                │
  │                              │                           │                │
  ├─ POST /v1/catalogs/{c}/storage-credentials ─────────────▶│                │
  │                              ├─ AssumeRole(role, policy)─▶│                │
  │                              │                           │                │
  │◀─ creds (60m, prefix-scoped)─┤                           │                │
  │                              │                           │                │
  ├─ GET s3://bkt/org/n/…  (SigV4 with vended creds) ────────────────────────▶│
  │◀───────────────────────────────────────────────────────── parquet ────────┤
```

### 5.3 The session policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:GetObjectAttributes"],
      "Resource": ["arn:aws:s3:::BUCKET/PREFIX/ORG/*"] },
    { "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
      "Resource": ["arn:aws:s3:::BUCKET/PREFIX/ORG/*"] },
    { "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:ListBucketMultipartUploads"],
      "Resource": ["arn:aws:s3:::BUCKET"],
      "Condition": { "StringLike": { "s3:prefix": ["PREFIX/ORG/*"] } } }
  ]
}
```

Notes that matter:

- **Session policies are capped at 2048 characters.** That is why `s3_prefix`
is one prefix per tenant and why the policy is generated, never accumulated.
  A tenant needing several disjoint `data_path`s gets several catalogs (or, from
  phase 4, an Access Point). Enforce a length bound on the composed policy at
  boot (`hog_tenant` insert), not at request time.
- **Read-only tokens get no `PutObject`.** The write actions are present only
  when the token carries `write` for that catalog, so an analyst token cannot
  overwrite parquet even if the metadata plane is compromised.
- **`NoSuchBucket`/`AccessDenied` and not a 403 from us** for anything outside
the prefix — the point is that S3 refuses, not that we remember to.
- The **role's identity policy** must also grant the union (session policy
intersects, never extends). The role per tenant is what makes `Resource` a
literal rather than a policy full of wildcards.

### 5.4 AssumeRole parameters

| Parameter | Value | Why |
|---|---|---|
| `RoleArn` | `hog_tenant.iam_role_arn` | per-tenant role; trust policy admits hoglake's own principal + an `ExternalId` of the hoglake instance id, to prevent the confused-deputy problem |
| `RoleSessionName` | `hl-{org[:8]}-{client[:12]}-{jti[:8]}` | `[\w+=,.@-]{2,64}`. Per-token attribution in **CloudTrail and S3 access logs** — with this, "which tenant read which object" needs no hoglake log at all |
| `Policy` | §5.3 | the prefix confinement |
| `DurationSeconds` | `min(3600, token.exp - now)` | never outlive the token |
| `Tags` | `org`, `catalog`, `client`, `jti` (+ `sts:TagSession` in the trust policy) | ABAC later, and per-tenant cost allocation |

**Credential cache.** Keyed `(catalog_id, scope-bitset)`; refresh when < 5 min
remain. One instance-wide `StsClient` (promote `software.amazon.awssdk:sts` from
`runtimeOnly` to `implementation` — the SDK is already pinned in lockstep at
`server/build.gradle.kts:84-87`, so this is one word and one jar we already
ship). Cache hit rate should approach 100% per (tenant, scope) warm pair.

### 5.5 Rotation and revocation

- **Issuer signing key**: `kid` in the JWS header; two active keys; JWKS at
  `/.well-known/jwks.json`. Recommended: **AWS KMS asymmetric sign (ES256)** so
  private key bytes never enter a pod; a mounted PEM is the dev fallback. Signing
  rate is low (token exchange and refresh only), so KMS latency is not on any
  hot path.
- **Token TTL**: 10 minutes. Refresh with the same credential. `nbf`/`iat` skew
  tolerance 60 s.
- **Revocation without a per-request DB read**: `hog_tenant.credential_epoch`
  is baked into the token as `ep`. Verification compares against an in-process
  cache of `catalog_id → epoch` refreshed every 30 s (one cheap query for the
  whole fleet). Bumping the epoch revokes every outstanding token for a tenant
  within 30 s. Deleting a client revokes its ability to *renew* immediately.
- **STS revocation**: cannot be revoked, only outlived. That is why
  `DurationSeconds` is capped at 1 h and why token TTL is 10 min — the worst-case
  blast radius of a stolen secret is one hour of one prefix, on one role, under a
  session name that identifies the token.
- **Break-glass**: `platform:admin` tokens (operator, from a projected SA token
  in-cluster, never over the internet) can act on any catalog. Every such use
  emits an audit line with `actor=operator:…` and a distinct metric.

---

## 6. The token

### 6.1 Grant types

| Grant | Client | Purpose |
|---|---|---|
| `client_credentials` | external machine clients (customer's ETL, their embedded app) | the default |
| `urn:ietf:params:oauth:grant-type:jwt-bearer` (RFC 7523) | in-cluster: Trino connector, millrace, hedgerow | **secretless**: the assertion is a Kubernetes projected service-account token, audience = hoglake, `sub = system:serviceaccount:{ns}:{sa}` |
| `refresh_token` | interactive/web | phase 5, only if a customer console ships |

RFC 7523 is the piece that makes this deployable inside the fleet: no long-lived
secret in a Trino catalog property, no secret in a Kubernetes Secret that the
provisioner has to rotate. A projected SA token is bound to a pod, expires in ≤24 h
(configurable, recommend 1 h), and its `sub` is a natural per-org identity when
the per-org Trino deployment gets its own ServiceAccount (which
`trino_provisioner.go` already effectively has, since it provisions per org).

### 6.2 Claims

```jsonc
{
  // JOSE header
  "alg": "ES256", "typ": "at+jwt", "kid": "hk-2026-11-a",

  "iss": "https://hoglake-prod-us.internal",
  "aud": "hoglake",
  "sub": "client:0193c1e2-…",           // or "sa:duckgres:trino-<org>"
  "iat": 1764547200,
  "exp": 1764547800,                    // +600s
  "jti": "0193c1e2-9f0a-7…",

  "ten": "tenant",                      // kind: "tenant" | "operator"
  "org": "1b2c3d4e-5f60-4a7b-8c9d-0e1f2a3b4c5d",
  "cli": "hedgerow-prod",
  "ep": 7,                              // carried, NOT read — see below

  // Per-catalog authorization. Present catalogs, nothing more.
  "cat": {
    "1b2c3d4e-5f60-4a7b-8c9d-0e1f2a3b4c5d":       ["read", "write", "consumer"],
    "1b2c3d4e-5f60-4a7b-8c9d-0e1f2a3b4c5d-dev":   ["read"]
  },

  "cli": "millpond-prod-us",            // display name for audit
  "rl":  { "rps": 500 },                // optional per-token rate hint
  "ten": "tenant"                       // "tenant" | "operator"
}
```

Deliberate omissions:

- **No S3 bucket/prefix/role.** Derivable server-side from `hog_tenant`; putting
  them in the token invites a verifier to trust the token's copy.
- **No AWS credentials.** D6.
- **No table-level grants** in v1. That is the phase-4 feature (§10.3) and it
  needs its own claim shape (`tab: {"<catalog>": ["ns.table", …]}`), which is
  expensive to size (a table allowlist can be thousands of entries). Design the
  verifier so the absence of `tab` means "all tables in `cat`" — which is what
  every v1 token means — and the field can be added without a version bump.

### 6.3 Scope vocabulary

| Scope | Grants |
|---|---|
| `read` | catalog get, namespaces, tables, views, files, scan, changes, partitions, stats, storage-usage, snapshots, options read, consumers read |
| `write` | commit (all three forms), uploads, table-creations, alter, truncate, drop namespace/table, options write, consumers write |
| `maintenance` | `POST …/maintenance/{expire,compact,rehydrate,cleanup}`, publications, reindex |
| `admin` | `DELETE /v1/catalogs/{catalog}` (tenant-scoped self-service drop), client management for this tenant |

`read` implies no `write`; `write` implies `read` (checked at mint time, refused
otherwise, so an operator cannot create a token that commits but cannot plan).

### 6.4 Route → scope map

Every route family gets an entry, and the test suite (§12) asserts the map is
**total** — an unlisted `/v1` route fails the build rather than silently
defaulting. This is the single most valuable test in the plan; the current tree
`501`s some routes and that must not become a bypass.

| Route | Tenant scope | Operator |
|---|---|---|
| `GET /v1/catalogs` | filtered to `cat` keys | all |
| `POST /v1/catalogs` | `admin` on the org (provisioning only; normally an operator action) | ✓ |
| `GET/DELETE /v1/catalogs/{c}` | `read` / `admin` | ✓ |
| `…/namespaces*`, `…/tables*`, `…/views*` | `read` (GET) / `write` (mutating) | ✓ |
| `…/scan`, `…/files`, `…/changes`, `…/partitions*` | `read` | ✓ |
| `…/commit*`, `…/uploads*`, `…/table-creations*`, `…/alter`, `…/truncate` | `write` | ✓ |
| `…/maintenance/*` | `maintenance` | ✓ |
| `…/consumers*` | `read` / `write` | ✓ |
| `…/storage-credentials` | `read` (+ `write` in the policy iff the token has `write`) | ✓ |
| `…/export` | `read` | ✓ |
| `…/publications` | `maintenance` | ✓ |
| `/v1/maintenance/status`, `/runs` | filtered to `cat` | ✓ |
| `/v1/database/health` | operator only | ✓ |
| `/v1/debug/*` | operator only, and only when an explicit env flag is set | ✓ |
| `/v1/info` | instance identity public; **totals per-tenant or omitted** | all |
| `/metrics`, `/livez`, `/healthz` | infra listener (see §6.7) | n/a |

### 6.5 Enforcement point

One Ktor application plugin installed in `App.module()` before any
`install*Routes`. It:

1. extracts the bearer token, verifies it locally (signature via cached JWKS,
   `iss`/`aud`/`exp`/`nbf`). **`ep` is not checked here** — see §17.3. It was
   designed against a `hog_tenant` epoch cache that §17 removed, so the verifier
   carries the claim through and acts on none of it; revocation for the metadata
   plane is the token TTL and nothing else;
2. resolves the route's required scope **and** the `{catalog}` path parameter;
3. puts a `Caller` in the call attributes, and the actor into the MDC so
   `Audit.event` stops defaulting to `anonymous`;
4. `401` with `WWW-Authenticate: Bearer error="invalid_token"` or `403
   {"error":"forbidden", …}` in the existing `ApiError` shape.

Two facts make this tractable here:

- Every catalog-scoped route already reads `call.catalog()`; the plugin's `{catalog}`
  extraction has exactly one choke point to match.
- `RequestId` (`observability/RequestId.kt`) is a hand-rolled
  `createApplicationPlugin` with a header passthrough and an `AttributeKey` — the
  precedent, the pattern and the test shape for this plugin already exist in the
  tree, and the file even explains why a Ktor plugin was hand-rolled rather than
  added.

**Defence in depth, beyond the interceptor.** An interceptor can be bypassed by a
route registered outside it, which is exactly the class of bug this repo's
invariants exist to prevent. So: every request also carries a `TenantContext`,
and the repository layer asserts `catalog_id == TenantContext.catalogId` for the
catalog it resolved — a cheap invariant, one `require`. Cost is zero on the happy
path and it turns "a new route forgot the plugin" into a 500 in tests rather than
a cross-tenant read in production.

### 6.6 Client-supplied authority fields must die

`author` arrives today from three places (create-catalog body, `?author=` on
drop, commit payload). Once a caller has an identity:

- the server **overrides** `author` with the token's `sub`, and
- a non-operator supplying a *different* `author` gets `422
  author_not_permitted` (not a silent override — silent override makes a client
  believe its attribution landed),
- **except a delegating client.** A token carrying `"dlg": true` belongs to a
  client that runs statements for end users (the Trino pool), and it may name
  the user: the server records `alice via client:trino-org1`, so the token's own
  actor is always on the record beside the claim. The claim must be printable,
  at most 200 characters, and may not contain ` via ` (no forged chains). The
  issuer sets `dlg` only on service credentials it provisions itself, never on
  a customer's own, so a customer SDK token still cannot name anybody else.

The `CommitRequest.author` / `SnapshotAuthor` fields stay in the wire contract for
operator/bulk paths and become operator-only. This also finally makes
`hog_snapshot.author` an audit-grade column.

### 6.7 Listeners

Split the process's ports:

- **public listener (8080)**: `/v1/*` with the auth plugin, plus `/livez` and
  `/openapi.yaml`.
- **infra listener (8081, cluster-internal)**: `/healthz`, `/metrics`,
  `/v1/database/health`, `/v1/debug/*`. Keeps the Prometheus surface (which
  carries per-catalog labels, i.e. tenant enumeration) off the customer path
  without contorting the authz model.

If a second listener is too much for phase 1, the fallback is: keep one port, and
require `platform:admin` for `/metrics` and `/debug/*` — but then the scrape job
needs a credential, which is its own rot. Two ports is cheaper.

---

## 7. Authorization on reads: the endpoints that must *filter*, not just gate

Gating (`403` unless entitled) is trivial. Filtering is where bugs live:

| Endpoint | Required change |
|---|---|
| `GET /v1/catalogs` | `WHERE catalog_id IN (:entitled)` — pushing the filter into SQL, not a post-hoc Kotlin `filter {}`, so paging (when added) stays correct |
| `GET /v1/maintenance/status`, `/runs` | same, and the response must not leak *counts* of other catalogs |
| `GET /v1/info` | `total_rows`/`total_size_bytes` are instance-wide sums over the metrics sampler. For a tenant token, either compute per-tenant from `catalogTotals` (already available — `installApiRoutes` receives `catalogTotals`) or omit the fields. **Recommend per-tenant**: omission is a silent lie to a customer looking at their own dashboard |
| `GET /v1/catalogs/{c}/storage-usage` | already catalog-scoped; fine |
| `GET …/partitions` | already catalog-scoped; the sampler's tier rows are per-catalog; fine |
| 404 vs 403 | A catalog the token is not entitled to must be **404**, not 403 — a 403 confirms existence and turns UUID guessing into enumeration. Use 404 for wrong-tenant, 403 only for a *known* catalog with an insufficient scope |

That last row is a design rule worth stating loudly in the code: **404 for
tenant mismatch, 403 for scope shortfall.**

### 7.1 What the enforcement PR actually did, and the two deviations

- **`GET /v1/catalogs`** filters in Kotlin (`CatalogService.listCatalogs`), not in
  SQL. Reason: the listing already reads every catalog row (one indexed scan of a
  table with one row per tenant, thousands at most), so a parameterized `IN`
  list of arbitrary length would put a statement-plan cache miss in front of the
  request every tenant makes on load. The doc's preference for SQL exists to keep
  *paging* correct, and this route does not page; that is the line to revisit if
  it ever does.
- **`GET /v1/maintenance/status` and `/runs` are OPERATOR-only, not
  tenant-filtered.** Both are keyset-paginated cross-catalog feeds (`after` +
  `limit`). A post-hoc filter returns short pages whose cursor had skipped rows
  the caller was never shown, so a client paging through it silently misses
  entries — worse than a clean 403. The tenant-facing twins
  (`…/catalogs/{c}/maintenance/status`, `…/runs`) already exist and need no
  filter at all, so no tenant loses a capability.
- **`GET /v1/info`** projects the sampler's instance totals onto the caller's own
  catalogs (`instanceTotalsFor`), matching the instance sum's own
  `unsampledCatalogs` accounting so the two numbers are comparable. As
  recommended above, the fields are not merely omitted for a tenant.
- One thing the plan did not anticipate, and it was a live bug for twenty
  minutes: **"no caller" is two different situations.** Under `off` the request
  is unauthenticated *by configuration* and every surface must behave exactly as
  it did before auth existed; under `warn`/`enforce` an absent caller is
  *anonymous* and entitled to nothing. A nullable `Caller` cannot express that,
  and the first draft of `/v1/info` omitted its totals whenever the caller was
  null — which under the default `off` mode stripped the instance totals from
  every response in production. `auth/AuthState` is therefore a three-state
  sealed type (`Off` / `Anonymous` / `Authenticated`), and every read site has to
  say which of the two caller-less cases it means.

---

## 8. Ecosystem changes

### 8.1 `pyhoglake`

- `HoglakeClient(..., auth=ClientCredentials(client_id=…, client_secret=…))` or
  `auth=Bearer("<jwt>")`, or `auth=JwtBearer(token_file=…)`.
- One seam: the `httpx.Client` at `client.py:689`. Add an auth hook that refreshes
  on `401` and at `exp - 60s`, guarded by a lock so 64 concurrent uploads do not
  each exchange.
- **New:** the client is where vended S3 credentials land. `table.scan()` / the
  writer's upload path must call `POST …/storage-credentials` and feed the
  result into the `S3Config`/`pyarrow.fs.S3FileSystem` construction at
  `client.py:495-610`. Today `S3Config` takes static `access_key`/`secret_key`;
  it needs a `RefreshableCredentials` variant (pyarrow accepts a credentials
  provider object, so the refresh can be lazy and cached).
- Cross-tenant error taxonomy: new `UnauthorizedError`/`ForbiddenError` in
  `errors.py`, mapped from `401`/`403`, so a misconfigured service fails loudly
  instead of at the first S3 403.

### 8.2 DuckDB extension

- `SET hoglake_token = '…'` (session-scoped, never persisted) or
  `hoglake_token_file`, matching the existing `hoglake_default_endpoint` option
  style at `duckdb-client/src/hoglake_extension.cpp:20`.
- `hoglake_client_id`/`hoglake_client_secret` only if the extension can hold a
  secret safely; otherwise token/CDN-file only. A `SECRET`-based spelling is the
  right DuckDB idiom and worth checking against the pinned extension version.
- The extension has its own S3 access (it reads parquet directly), so it needs
  the vended-credential path too — `SET s3_access_key_id`/`s3_session_token`
  from the vended response, refreshed on expiry.

### 8.3 `millrace`, `hedgerow`, `bench`

All sit on `pyhoglake`. Service tokens via RFC 7523 (in-cluster) or
`client_credentials` (dev/local). `millrace` creates its destination catalog at
startup from `MILLRACE_CATALOG_DATA_PATH` — with auth, that knob becomes
operator-only and the *customer* deployment path passes a tenant token instead.
`bench/` needs an `--token` flag or it stops working against an enforcing stack;
the bench harness runs in CI against a non-enforcing compose stack, so the
cheapest answer is that compose sets `HOGLAKE_AUTH_MODE=off`.

### 8.4 Trino (`PostHog/trino`, `plugin/trino-hoglake`)

This is the highest-value integration and the one with the sharpest constraint
(no secrets in catalog properties). Two properties:

```properties
connector.name=hoglake
hoglake.uri=https://hoglake-prod-us.internal
hoglake.catalog=<org-uuid>
# NEW — the secretless path:
hoglake.auth.type=jwt-bearer
hoglake.auth.token-file=/var/run/secrets/hoglake/token   # projected SA token
hoglake.auth.token-audience=hoglake
hoglake.auth.sts-endpoint=https://hoglake-prod-us.internal
```

The connector:

1. reads the projected token file (kubelet rotates it in place; the file is
   re-read on `401` / on mtime change);
2. exchanges it at `POST /v1/oauth/token` with `grant_type=…jwt-bearer` for a
   tenant JWT, cached until `exp - 60s`;
3. sends `Authorization: Bearer` on every metadata call;
4. calls `POST …/storage-credentials` and constructs its S3 client from the
   vended credentials, refreshing on a timer — **instead of** the current
   pod-role client.

Step 4 is the change that actually closes §1.5. It is also the change most
likely to be contentious, because it moves Trino's S3 access from "the worker's
IRSA role" (fast, free, already working) to "per-tenant vended creds" (one STS
call per catalog per hour, and a Trino-internal refresh path). The fallback is
documented and cheap: keep `s3.iam-role` semantics for hoglake catalogs by
teaching the connector to assume the per-tenant role directly from the ARN that
`/storage-credentials` returns *without* the session policy — which gets
tenant-confined S3 with no vending endpoint at all, at the cost of D5's single
authority. Ship vending; keep that as the rollback.

### 8.5 duckgres provisioner

The provisioner must learn the auth fields, which means:
`DUCKGRES_TRINO_HOGLAKE_URI` stays, and a new
`DUCKGRES_TRINO_HOGLAKE_AUTH=jwt-bearer` plus the mount of the org's projected
SA token into the Trino pods. It must also **append `Authorization` nothing** —
per the file's own rule, the props map carries no secret. The one new property
is a *path*.

There is also a provisioning-order consequence: the hoglake `hog_tenant` row (and
therefore the per-org IAM role and prefix) must exist **before** the Trino
catalog becomes usable, or the first query 404s. The provisioner already models
this kind of convergence (`tenantSecretNotMountedYet` treats a not-yet-visible
secret file as `Pending`, not failed). Add the analogous `hoglake tenant not yet
registered → Pending` case rather than stamping a failure.

### 8.6 webui

Today it is an operator console that lists every catalog, served same-origin. Two
honest options:

- **(i) Operator-only, forced.** Put `/v1` behind ALB OIDC or a cluster-internal
  ingress and leave the webui unauthenticated behind it. Zero code. This is the
  right answer for `platform:admin`.
- **(ii) Customer console.** OAuth2 authorization-code + PKCE, PostHog as IdP,
  server-side cookie session holding the tenant JWT, webui proxies `/v1` through
  its own backend so the token never reaches JS. This is a real project and
  should not ride along with this one.

Recommend **(i)** in this plan, **(ii)** as a separate decision.

---

## 9. Server-side background jobs

These cross tenant boundaries by design, on one credential (§1.4). Two phases:

**Phase A (ship with this plan).** The pod keeps a broad role. Nothing changes
except that the ability is *named*: a startup assertion that the configured
credential can read a canary object per tenant is a bad idea (N probes at boot);
instead, `ObjectStore` gains a per-catalog constructor, logging the caller, and
the existing `validatePathsUnderDataPath` remains the only thing keeping
compaction's output inside its own prefix.

**Phase B (hardening).** Every S3 operation is scoped by catalog:
`ObjectStore.forCatalog(catalogId)` returns a store backed by vended credentials
for that tenant, cached like the request path. Then compaction cannot write
outside the tenant's prefix even if its output-path construction is wrong — the
bug becomes a 403 instead of cross-tenant corruption. Cost: one STS call per
catalog per hour, and it must be a *distinct* role from the customer one (a
`maintenance` verb set, since compaction writes new files and cleanup deletes
them). This is the change that makes tenant isolation *provable* rather than
argued, and it is worth doing.

---

## 10. Schema

### 10.1 New tables (V29+)

Follow the repo's migration conventions exactly: one `V<n>__*.sql` per change,
`schema.sql` updated so `fold(migrations) == schema.sql`
(`SchemaEquivalenceIntegrationTest`), explicit
`SET LOCAL lock_timeout = '5s'` (or the parked-setting round trip the newer
migrations use), `CREATE INDEX CONCURRENTLY` with the accompanying `.conf`
(`executeInTransaction=false`) when the index lands on a large table, and a
header comment that states *why* rather than *what*.

```sql
-- V29__tenancy.sql
CREATE TABLE hog_tenant ( … §4.1 … );
CREATE INDEX hog_tenant_org ON hog_tenant (org_id);

-- V30__auth_clients.sql
CREATE TABLE hog_client (
    client_id     text PRIMARY KEY CHECK (client_id ~ '^hk_[a-z]+_[A-Za-z0-9]{20,40}$'),
    -- High-entropy secret: SHA-256 is sufficient and constant-time; argon2
    -- is for low-entropy human material and would put a ~50ms cost on
    -- every token exchange.
    secret_sha256 bytea NOT NULL,
    kind          text NOT NULL CHECK (kind IN ('client_credentials','jwt_bearer_subject')),
    -- For jwt_bearer_subject: the (issuer, subject) this client maps to,
    -- e.g. ('<eks-oidc>', 'system:serviceaccount:duckgres:trino-<org>')
    issuer        text,
    subject       text,
    name          text NOT NULL,
    org_id        uuid NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    created_by    text NOT NULL,
    expires_at    timestamptz,
    disabled_at   timestamptz,
    last_used_at  timestamptz
);
CREATE UNIQUE INDEX hog_client_subject ON hog_client (issuer, subject)
    WHERE issuer IS NOT NULL;

CREATE TABLE hog_client_scope (
    client_id  text NOT NULL REFERENCES hog_client ON DELETE CASCADE,
    catalog_id bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    scope      text NOT NULL CHECK (scope IN ('read','write','maintenance','admin')),
    PRIMARY KEY (client_id, catalog_id, scope)
);
CREATE INDEX hog_client_scope_catalog ON hog_client_scope (catalog_id);

-- V31__auth_signing_keys.sql — optional if keys live in KMS.
CREATE TABLE hog_signing_key (
    kid         text PRIMARY KEY,
    algorithm   text NOT NULL CHECK (algorithm IN ('ES256')),
    public_jwk  jsonb NOT NULL,
    kms_key_arn text,
    created_at  timestamptz NOT NULL DEFAULT now(),
    retired_at  timestamptz
);
```

Deliberately **not** a table: request/access auditing. `Audit` is a log stream by
explicit design ("never rows in a database" — the `duckgres` `query_log` lesson).
Auth denials are audit lines plus a counter, not rows.

Deliberately **not** a `hog_token_denylist` table in v1: the epoch column gives
sub-minute fleet-wide revocation at one query per 30 s, and a denylist adds a
per-request lookup to the hot path for a capability that mostly overlaps.

### 10.2 `hog_catalog`

No new columns. Tenancy lives in `hog_tenant` (§4.1) precisely so the hottest row
in the schema does not grow.

### 10.3 Phase-4 shape, designed now and built later

Table-level grants, if hoglake is ever to serve a *customer's end users* rather
than the customer's service account:

```sql
CREATE TABLE hog_grant (
    principal   text NOT NULL,          -- client_id or subject
    catalog_id  bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    table_uuid  uuid REFERENCES hog_table(catalog_id, table_uuid) ON DELETE CASCADE,
    privilege   text NOT NULL CHECK (privilege IN ('select','insert','delete')),
    granted_by  text NOT NULL,
    granted_at  timestamptz NOT NULL DEFAULT now()
);
```

Non-obvious requirement: `table_uuid` is the right key (it survives rename and is
what consumers key cursors to — `schema.sql` is explicit), but a grant on a table
that is dropped and recreated must **not** silently apply to the new incarnation.
`hog_table.replaced_table_id` already records the replacement edge, so the grant
must be revoked, not inherited — assert that in a test.

---

## 11. Rollout

`HOGLAKE_AUTH_MODE`:

| Mode | Behaviour |
|---|---|
| `off` | today's behaviour; the plugin is installed but anonymous |
| `warn` | the plugin resolves a caller when a token is present, authorizes, and on failure **logs + counts + allows**. Every request without a token emits `auth_missing_token_total{catalog}` |
| `enforce` | the plugin is authoritative |

Sequence:

1. Ship the plugin, `hog_tenant`, backfill tooling and `/storage-credentials` in
   `off`; ship client support in `pyhoglake` / DuckDB / Trino.
2. Flip **dev** to `warn`, then `enforce`. The `warn` metric list is the
   onboarding checklist — a single scrape answers "who is still anonymous?".
3. Run `warn` on `prod-us` and `prod-eu` for a week. `auth_missing_token_total`
   must reach zero per catalog before the flip.
4. Flip prod to `enforce`, both regions, off-peak, one region at a time.
5. Only then hand customers credentials (the fleet is already authenticated
   machine-to-machine before a single customer sees a `client_id`).

`in-cluster` clients onboard **before** customers: millrace, hedgerow, the Trino
connector, the webui's ingress. Their tokens are the `warn`-mode population.

Rollback is one env var. That is why `off` exists in the shipped binary
indefinitely rather than being deleted after the migration.

---

## 12. Testing

The repo's existing discipline (`AGENT.md` §Pre-push) already demands the shape.
The additions specific to auth:

| Test | What it pins |
|---|---|
| **Route-map totality** | Every route in `openapi/hoglake.yaml` has an entry in the auth policy map, asserted by walking the spec — an unlisted route fails the build. This is the anti-`501`-bypass test |
| **Cross-tenant matrix** | Table-driven, N=3 tenants × every route: A's token against B's catalog must be 404 (not 403); A's token with `read` against B's write routes must 403 |
| **Path-confinement property test** | A hypothesis-driven generator of hostile paths (`../`, absolute, percent-encoded, doubled slashes, unicode lookalikes, prefix-siblings like `org/abc` vs `org/abcd`) through `validatePathsUnderDataPath` *and* through the session-policy generator — the two must agree on every input. The existing `verify` on prefix comparison is a substring test; the `abc`/`abcd` sibling is the classic bug and belongs in a vector file, in the style of `pyhoglake/tests/vectors/bounds_vectors.json` |
| **Session-policy golden test** | The generated policy JSON for a matrix of (bucket, prefix, org, scope) is byte-exact, and every case is < 2048 chars |
| **`sts:AssumeRole` fault injection** | `ObjectStore` is already `open` with overridable methods *for exactly this reason* (fault injection on the rewrite path); add an `StsClient` seam in the same style — throttling, `AccessDenied`, expired cached creds, and a clock-skewed `expiration` |
| **Token verification vectors** | Cross-language vectors for the JWS: valid, bad `kid`, bad signature, `exp` skew edge, `nbf` future, wrong `aud`, `ep` stale, `cat` absent. Ship as `vectors/jwt_vectors.json` beside `bounds_vectors.json`, because the Trino connector (Java) and `pyhoglake` (Python) must agree |
| **Fuzz the token parser** | The repo already runs nightly property/fuzz suites (`docs/fuzzing.md`); a JWS parser is a parser and gets the same treatment |
| **Migration test** | `V29`/`V30` applied to a populated database, in the shape of the existing `V*MigrationIntegrationTest`s |
| **Audit attribution** | Every audited action carries a non-`anonymous` actor when a token is present, and a caller-supplied `author` is refused for tenants unless the token delegates (`dlg`), in which case it is recorded as `<user> via <actor>` |

---

## 13. Observability

New metrics (Prometheus, existing `observability/Metrics.kt` idiom):

| Metric | Why |
|---|---|
| `hoglake_auth_requests_total{mode,outcome,client}` | the `warn`-mode onboarding checklist; `client` is a closed class from `User-Agent`, and the enforce gate is zero refusals per class |
| `hoglake_auth_signing_keys` | keys the verifier holds; alert on 0 (boot refuses an empty source, this catches a rotation that never lands) |
| `hoglake_auth_missing_token_total{catalog}` | who is still anonymous |
| `hoglake_auth_denials_total{catalog,required_scope,reason}` | distinguishes "client misconfigured" from "attack" by `reason` |
| `hoglake_token_issued_total{grant_type}`, `_failures_total` | token endpoint health |
| `hoglake_sts_assume_total{catalog,outcome}`, `_duration_seconds` | the vending path is now on the critical path of a read; it needs a latency histogram |
| `hoglake_sts_cache_hit_ratio` | if this drops, every scan pays an STS round trip |
| `hoglake_auth_epoch_refresh_age_seconds` | the 30 s epoch cache; stale means revocation is not working |

Audit additions: a `tenant` field on every event, and three new actions —
`token_issue`, `token_denied`, `storage_credentials_vend` (the last one is the
one you want when a customer asks "who read my data", alongside CloudTrail).

---

## 14. Work breakdown

Estimates are calendar weeks for one engineer familiar with the codebase; they
are weather, not commitments.

### Phase 1 — Foundation (2 weeks)

| Item | Notes |
|---|---|
| `V29` `hog_tenant`, `V30` clients/scopes | §10.1 |
| `Caller` + auth plugin (verify-only, symmetric-free: JWKS + ES256), installed ahead of routes | hand-rolled in the `RequestId` idiom; no new Gradle dep |
| Route→scope map + totality test | §12 |
| `404`-not-`403` tenant mismatch; `listCatalogs` SQL filter | §7 |
| `author` override/refusal | §6.6 |
| `HOGLAKE_AUTH_MODE=off\|warn\|enforce` | §11 |
| Audit `tenant` + actor plumbing | §13 |

**Exit criterion:** in `warn` mode on the dev stack, a request with a valid token
is attributed to its tenant in the audit log, a request without one is counted,
and the route-map totality test fails if a route is added without a policy.

### Phase 2 — Issuance (2 weeks)

| Item | Notes |
|---|---|
| `POST /v1/oauth/token` — `client_credentials` | §6.1 |
| `/.well-known/jwks.json`, `/.well-known/oauth-authorization-server` | |
| Signing key handling (KMS sign + JWKS from `hog_signing_key`; PEM fallback) | §5.5 |
| `POST …/storage-credentials` + session-policy generator + STS cache | §5, §12 |
| Epoch cache + revocation | §5.5 |
| `pyhoglake` auth + refreshable S3 credentials | §8.1 |

**Exit criterion:** `pyhoglake` reads and writes a catalog end-to-end in `enforce`
mode with only a `(client_id, client_secret)` and no static AWS keys, and a
hand-rolled attempt to read another tenant's prefix gets `AccessDenied` from S3,
not a 403 from us.

### Phase 3 — In-cluster (2 weeks)

| Item | Notes |
|---|---|
| RFC 7523 `jwt-bearer` grant over Kubernetes projected SA tokens | §6.1, the secretless path |
| Trino connector: token file, exchange, cache, vended S3 client | §8.4 |
| duckgres provisioner: auth props + SA mount + `Pending` convergence | §8.5 |
| millrace / hedgerow service tokens | §8.3 |
| Two listeners; `/metrics` and `/debug/*` off the public port | §6.7 |

**Exit criterion:** Trino queries the org's catalog with zero long-lived secrets
in any manifest, and a query through org A's Trino catalog cannot read org B's
parquet even with the worker role still attached.

### Phase 4 — Customer-facing (2 weeks)

| Item | Notes |
|---|---|
| Customer credential lifecycle API (create/rotate/revoke/list, `last_used_at`) | |
| Per-tenant rate + concurrency limits in `RequestDispatcher` | §2 last row |
| `/v1/info` per-tenant totals | §7 |
| DuckDB extension token support | §8.2 |
| Onboarding runbook + customer docs | the thing that actually determines support load |

### Phase 5 — Hardening (continuous)

- Per-tenant IAM roles everywhere; the server drops its broad role (Phase B, §9).
- S3 Access Points or per-tenant buckets (§5.1 E/F) for the largest tenants.
- Table-level grants (§10.3) if hoglake ever serves end users directly.
- External issuer trust (PostHog Django JWKS) for interactive tokens.
- `DPoP` (RFC 9449) if bearer tokens on customer machines prove too stealable —
  cheap to add to a JWS verifier that already has a `jkt`-shaped slot, and it
  removes the replay window entirely.

---

## 15. Open questions (decisions I cannot make from the code)

1. **Bucket topology.** Is the current layout one shared bucket with per-org
   prefixes, or a bucket per region per tier? This decides whether `iam_role_arn`
   is per-org (clean) or shared (then the session policy is the only confinement
   and D5 becomes load-bearing rather than belt-and-braces). *Blocks Phase 2's
   policy generator.*
2. **Who creates the per-org IAM role?** duckgres's provisioner already owns
   `d.IAMRoleARN` for the DuckLake catalogs, so it likely owns this one too —
   which would make hoglake a *consumer* of provisioning rather than a
   participant. That would also mean `hog_tenant` is written by the provisioner,
   not by a customer API. *Blocks Phase 2.*
3. **Does the hoglake catalog currently exist before the Trino catalog?** If
   millpond creates it lazily, the provisioner's `Pending` state needs a third
   case. *Blocks Phase 3's exit criterion.*
4. **Is `aud` per region?** Two regions means two issuers; a token minted in
   prod-us must not be accepted in prod-eu. Cheap to do (`iss` is already in the
   claims), but it must be decided before the first key is provisioned.
5. **Do we want customers to call `/v1` directly at all,** or is every customer
   request mediated by duckgres/Trino? If the latter, Phases 2 and 4 collapse
   into Phase 3 and the entire customer-credential-lifecycle surface disappears.
   This is the single biggest scope question in the plan.

---

## 16. Summary

- The **tenancy model already exists** (catalog name = org UUID) and the
  **write-side confinement already exists** (`validatePathsUnderDataPath`,
  disjoint `data_path`s). The plan makes both explicit and stops trusting the
  caller for `data_path`.
- A JWT with **per-catalog scopes** solves metadata authorization in one plugin
  at one choke point, and the repo's own `RequestId` plugin is the pattern.
- The hard part is C: **STS AssumeRole with a generated prefix-confined session
  policy**, vended from a dedicated endpoint, never in a token and never in a
  catalog property. `sts` is already on the runtime classpath, so this adds
  capability without a dependency.
- The fleet's **secretless** path is RFC 7523 over Kubernetes projected SA
  tokens, which is what makes the Trino integration possible under duckgres's
  existing "no secret in the properties map" rule.
- The two things that will bite: **the server's own background jobs** (one
  global S3 credential crossing every tenant boundary — fixable, and worth
  fixing) and **`GET /v1/catalogs` / `/v1/info` / `/v1/maintenance/*`**, which
  must *filter*, not merely gate.

---

## 17. Revision: the authority belongs in hogtower, not here

Everything above was written from this tree alone. Reading
[`PostHog/hogtower`](https://github.com/PostHog/hogtower) — the managed-warehouse
control plane — shows that six of the seven decisions above put machinery in the
wrong repo. This section is what the stack should actually be, and it is
smaller, not larger.

The short version: **hoglake is a resource server. hogtower is the authority.**
hoglake verifies a token and authorizes catalog metadata, which is what the
first PR already does. hogtower mints the token and vends the S3 credentials,
because it already holds every input, already assumes the tenant roles, and is
already the only writer of hoglake catalogs.

### 17.1 What hogtower already has

`hogtower` describes itself as the management plane that owns "warehouses:
create, edit, deprovision; the Duckling CR behind each one (S3 bucket, IAM role,
metadata Postgres via Crossplane)" and "teams, logins and service credentials"
(`README.md`). Concretely, per warehouse:

| Input the grant needs | Where it already lives |
| --- | --- |
| tenant → hoglake catalog name | `trino_hoglake.go:111-160` creates `/v1/catalogs` with `name = orgID` and asserts `catalog.Name == orgID` on read |
| tenant → data path | same call; `data_path` is derived from `s3_path_prefix` + Duckling name, not supplied by the tenant |
| tenant → S3 bucket, region, prefix | `warehouse_ducklings.s3_bucket / s3_region / s3_path_prefix`, `00001_initial_schema.sql:63-65` |
| **tenant → IAM role ARN** | `warehouse_ducklings.identity_iam_role_arn`, `00001_initial_schema.sql:70` |
| **an `sts:AssumeRole` broker** | `internal/controlplane/sts_broker.go` — 1h sessions, singleflight, a safety margin, a refresh scheduler that pushes rotations to workers |
| a tenant-scoped S3 write probe | `trino_hoglake_storage.go:26` `TrinoHoglakeStorageCheck(roleARN, region, dataPath)` — it assumes the tenant role and put/get/delete inside the exact catalog prefix |
| machine credentials a customer holds | `service_credentials`, `00001_initial_schema.sql:126-140`; `MintServiceCredential` (`service_credentials.go:65`) mints `svc_<24 hex>` + a bcrypt-hashed secret with a TTL, warehouse-scoped and revocable |
| an ES256 JWT verifier | `internal/admin/alb_oidc.go` — verifies the ALB's `X-Amzn-Oidc-Data` against a fetched regional key |
| the tenant isolation policy | `internal/opa/policy.rego`, described as "the tenant isolation boundary" |

And it already gives the *Trino* hoglake catalogs an S3 boundary,
`s3.auth-type=IAM_ROLE` with `s3.iam-role = <tenant role>`
(`trino_hoglake.go:225`) — so §1.5 of this document is at least partly stale for
catalogs *hogtower* provisioned: the gap is not that hoglake catalogs have no IAM
identity, it is that every Trino worker ambiently holds *every* tenant's identity.

"Partly" because §1.5 was written against the duckgres provisioner
(`trino_provisioner.go:369`, "Hoglake catalogs using the pod's S3 credentials"),
and duckgres is still in service during cutover
(`hogtower/docs/cutover.md`). Whether any live catalog was published by the old
path is a question for the cutover checklist, not something to assume here. Until
it is answered, the honest statement is that the boundary depends on which control
plane published the catalog. hogtower's own security review
already frames the fix as C′, "vended short-lived S3 credentials … the best
value, do it first" (`docs/security-review-trino-isolation.md`, §"The
alternatives, re-costed").

The same review's finding M7 is this document's §1.1, filed from the other side:
"gigahog has no authentication and no NetworkPolicy. Any pod that reaches it can
name another tenant's catalog." The first PR is the fix for M7.

### 17.2 Why hoglake should not do issuance

Three reasons, in increasing order of severity.

1. **It has nothing to issue from.** No IAM role, no bucket, no STS client, no
   tenant roster. `sts` is on the classpath only as a leftover of the IRSA link.
   Every input §4 and §10 invented a table for is already a row in hogtower.
2. **Two authorities is the failure mode.** If hoglake mints tokens and hogtower
   mints credentials, revocation is two operations in two systems with two
   clocks, and the customer's entitlement is the *conjunction* — so a bug in
   either grants nothing, or worse, a missed revoke in one leaves half a grant
   alive. One authority, one `revoked_at`.
3. **Blast radius.** Making hoglake an authorization server means it holds a
   signing key and a database of clients. Today it holds no secrets at all, and
   a bug in its catalog code cannot mint authority. Keeping it a verifier keeps
   that property: the worst a compromised hoglake can do is serve the catalogs
   it was already allowed to serve.

The cost is a cross-repo contract. That is a real cost and it is the one thing
§17.5 is for.

### 17.3 The shape

One credential in, two artifacts out, each with its own lifetime and audit trail.

```
customer
  │  client_id=svc_…  client_secret=…        (already minted by hogtower)
  ▼
POST https://<hogtower>/oauth/token           client_credentials
  │  → access_token: ES256 JWT, 15 min
  │      iss=https://hogtower…   aud=hoglake
  │      sub=svc_…   ten="tenant"   org=<org>   cli=<name>   jti=<uuid>
  │      cat={"<org>":["read","write"]}
  ▼
┌───────────────────────────┬──────────────────────────────────────────────┐
│ Authorization: Bearer <jwt>│ POST /oauth/credentials  (Bearer <jwt>)      │
│ → hoglake /v1/catalogs,   │ → sts:AssumeRole(tenant role,                │
│   /v1/… metadata          │     session policy = bucket/prefix only,      │
│                           │     RoleSessionName = <org>/<svc id>)         │
│                           │ → {access_key_id, secret_access_key,          │
│                           │    session_token, expiration}                 │
└───────────────────────────┴──────────────────────────────────────────────┘
```

Two artifacts rather than one response because they are different kinds of
thing. The JWT is a claim about metadata, it is small, it is safe to log the
`jti` of, and 15 minutes is generous. The S3 credentials are the actual blast
radius: they are bearer credentials against the customer's data with no
audience restriction, so they get their own endpoint, their own rate limit,
their own audit line and their own (shorter) lifetime. Collapsing them into the
token response would make every token-caching client also a credential-caching
client, which is exactly the mistake §5.1 option D was rejected for.

**No `epoch` claim in the token.** An earlier draft carried
`hog_tenant.credential_epoch` and checked it against a 30-second cache. §17
removed the table it was keyed on, and the remaining argument for the claim does
not survive: the only consumer that could act on it is `/oauth/credentials`, which
is hogtower itself and can read `service_credentials.revoked_at` directly rather
than trust the token's copy of it. Meanwhile hoglake cannot check it at all — it
has no database, and adding one to the verification hot path is the thing §6.5
was designed to avoid. A claim carried by a token, understood by neither
consumer, is how a revocation mechanism gets believed in and never built.
Revocation for the metadata plane is `exp` and nothing else; say so in the
customer-facing docs rather than implying otherwise.

This also serves the three consumers hogtower already has. The DuckDB/httpfs
path takes vended credentials directly (no fork needed — the pinned
`PostHog/duckdb-httpfs` fork already re-resolves the secret on `ExpiredToken`).
Trino's path is C′ in its own review. hoglake's path is metadata only. One
broker, three callers, one `RoleSessionName` convention, so CloudTrail
attribution is uniform.

### 17.4 What changes in the plan above

**Superseded — §4 (the tenancy model).** `hog_tenant`, `data_path` derivation and
the backfill are not needed. hogtower is the only creator of catalogs, it derives
`data_path` from its own `s3_path_prefix`, and it verifies `Name` and `DataPath`
against its configured expectation on every reconcile (`trino_hoglake.go:156`),
refusing with "explicit recovery required" on a mismatch. What §4 was protecting
against — a *tenant* claiming a bucket root — is already prevented because
tenants cannot reach `POST /v1/catalogs`; the first PR made it operator-scoped.
The residual risk is a rogue *operator* caller, which is a different threat with
a different mitigation (audit, and eventually a `data_path` root allow-list from
config). Dropping §4 also drops the only migrations this document planned.

**Superseded — §5 (S3 vending) and §6.6 (issuance, JWKS).** Both move to
hogtower. The session-policy construction in §5.3, the `2048`-character limit,
the `RoleSessionName` argument and the rotation/revocation design transfer
unchanged; the endpoint hosting them does not.

**Superseded — §10 (schema).** No `hog_tenant`, no `hog_client`, no signing-key
table, no `hog_grant`. hoglake gains no tables from this work.

**Superseded — §14 (work breakdown).** Phases 2, 3 and 4 move to hogtower and
no longer gate anything here.

**Superseded — §15 (open questions).** Q5 ("do customers call `/v1` directly,
or is every request mediated?") is answered: they call it directly, which is
what makes hoglake the metadata service rather than an implementation detail,
and it is why the enforcement layer had to exist. Q1 (bucket topology) is
answered by hogtower's own review — "each tenant has its own bucket or prefix".
Q2 (who owns the IAM role) is answered: hogtower does, via the Duckling CR.
Q3 (catalog-vs-Trino ordering) is answered by `ensureResources`: the catalog is
created before it is published, and publication is gated on the tenant storage
probe succeeding. Q4 (per-region `aud`) remains open and is now hogtower's.

### 17.5 The contract, which is the only genuinely new interface

Two repos, two languages, one claim format. The interface is a small file, not
an RPC: a set of signed test tokens plus their expected parse results, checked
into **both** repos and asserted in both test suites, so hogtower cannot ship a
mint that hoglake misreads and hoglake cannot tighten a check hogtower violates.

It must pin, at minimum:

- the claim names and types, and which are REQUIRED. `ten` is the kind
  discriminator (`"tenant"` or `"operator"`), not the org — `org` carries the org
  and `cli` the client name. Getting this wrong is not a warning at runtime: a
  token with `ten: "<org uuid>"` is refused as malformed, so the table is
  normative and the vectors are generated from the verifier rather than from it:

  | claim | required | meaning |
  | --- | --- | --- |
  | `iss` | yes | the issuer URL |
  | `aud` | yes | `hoglake` (string, or an array containing it) |
  | `sub` | yes | the credential subject; becomes the audit actor |
  | `jti` | yes | token id, for correlating an audit line with a token |
  | `ten` | yes | `tenant` \| `operator` |
  | `org` | tenant | the org's catalog namespace |
  | `cat` | tenant | object: catalog name → array of scope names; non-empty, and an entry naming no known scope is dropped rather than carried |
  | `cli` | no | human-facing client name |
  | `exp` | yes | expiry |
  | `iat` | no | issued-at; a value in the future is refused |
  | `nbf` | no | not-before; absence means "valid now" |
  | `ep` | no | credential epoch, carried for the vendor's own use; **hoglake does not read it** |
- the scope vocabulary and its ordering (`read < write < maintenance < admin`),
  which hogtower mints and hoglake enforces — the same five words on both sides
  or neither side;
- the `alg` (`ES256`), the key id (`kid`) scheme, and the JWKS URL;
- the error contract: `401` with `WWW-Authenticate: Bearer error=…` for a bad
  token, `403` for a scope shortfall, `404` for a catalog the caller cannot see.
  The reason strings are part of the contract because a client branches on them.

hoglake's side of this is already built: `JwksSigningKeys.fromUrl` fetches, caches
per `kid`, refreshes once on a miss and keeps the old keys when a refresh fails;
`Scope.parseAll` drops unknown names rather than refusing the token, so a newer
minting side degrades to a 403 that names the scope instead of a rolling-upgrade
outage.

### 17.6 The revised stack

Six PRs across two repos, in dependency order. Only the first touches hoglake.

| # | repo | what | size |
| --- | --- | --- | --- |
| 1 | hoglake | **enforcement core** — verifier, route→scope table, `Caller`, `off\|warn\|enforce`, filtered listings, adjudicated `author`. *Open: [#460](https://github.com/PostHog/hoglake/pull/460).* | done |
| 2 | hogtower | **signing key + JWKS** — ES256 keypair sealed at rest with the existing AES-256 user-secret key machinery, a `kid`, `GET /.well-known/jwks.json`, rotation. No tokens yet, so it is inert. | 1 week |
| 3 | hogtower | **`POST /oauth/token`** — `client_credentials` against `service_credentials` (bcrypt verify, `expires_at`, `revoked_at`), scopes from service grants/OPA/tenant, mint the JWT. **No `epoch` claim** — see the review note in §17.3: the only
consumer that could act on it is the one that can read `revoked_at` directly, so
it would be a claim that implies a revocation check nobody performs. Smaller
than it reads: `ValidateTrinoServiceCredential` already verifies exactly this credential and returns the org, tier and groups. | 1 week |
| 4 | hogtower | **`POST /oauth/credentials`** — validate the JWT, resolve bucket/prefix/region from `warehouse_ducklings`, `AssumeRole` with a session policy and a per-tenant `RoleSessionName`, return the credentials. **The broker needs one real change:** `awsAssumeRole` (`sts_broker.go:247`) passes neither `Policy` nor a per-caller session name, and the cache is keyed on `roleARN` alone (`:192`) — keyed that way it would serve a prefix-scoped credential to the next caller for the same role, which is the bug that makes this whole design fail open. Cache key becomes `roleARN + hash(session policy)`. | 1.5–2 weeks |
| 5 | hoglake | **flip it on** — `HOGLAKE_AUTH_ISSUER`, `HOGLAKE_AUTH_JWKS_URL`, then `HOGLAKE_AUTH_MODE=enforce`. Config plus the contract-vector test. Optional hardening in the same PR: refuse a `POST /v1/catalogs` whose `data_path` is outside a configured root. | 2 days |
| 6 | hoglake, pyhoglake | **clients** — `pyhoglake` gains `client_credentials`, token refresh and a `/oauth/credentials` fetch; the DuckDB extension exposes it as an extension option. §8.1 and §8.2 otherwise stand. | 1 week |

Weeks 2–4 are hogtower's and are independent of hoglake's; week 1 and week 5 are
the only points where the two repos have to agree, and the contract vector file
means they agree in a test rather than in a meeting.

### 17.7 What this costs if it is wrong

The expensive part of the original §5 was STS session policies and the
credential lifecycle, and that design survives verbatim — it moves one repo
over. The expensive part of §4 was a tenant table and a backfill, and it
disappears. The part that is genuinely thrown away is §6.6's in-server issuance
and §10's schema: about two weeks of the five-phase plan, spent on a
`POST /v1/oauth/token` that would have had to reach into hogtower for every
answer anyway.

The cost of *not* doing this is concrete: two authorities, two revocations, two
clocks, and the second one owned by a service that has never held a secret.
