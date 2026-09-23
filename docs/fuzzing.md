# Fuzzing hoglake

The strategy for finding bugs by generation rather than enumeration,
and where each piece lives. Status: Python side landed; JVM
coverage-guided layer landed (layer 4 below — corpus replay runs in the
normal server suite, deep runs via `./gradlew fuzz`).

## Why fuzz this system

The bug classes that hurt us in the DuckLake era were exactly the kind
example-based tests miss: NULL-shaped rows crashing unguarded reads,
type-boundary values (UInt64 > 2^63), encoding mismatches between
components, and state machines driven into corners by concurrency. All
four are generator-friendly.

## The layers

### 1. Property-based tests (in the normal suites)

- **pyhoglake** (hypothesis, `tests/qe_*.py`): codec round-trips over
  full value domains (`decode(encode(x)) == x` per ColType — boundary
  ints, subnormal floats, ±0.0, precision-38 decimals, astral-plane
  unicode), arrow↔ColType mapping totality (every exotic arrow type
  either maps or raises `UnsupportedTypeError`, never passes silently),
  stats extraction vs independently computed ground truth, and
  wire-model parsing under mutated JSON (extra/missing/wrong-typed
  fields must fail cleanly, never leak `KeyError`).
- **server** (planned, kotest-property or jqwik): the same codec
  properties on `IcebergSingleValue`, expiry `newEarliest` math under
  generated snapshot/offset/time configurations (invariants: never >
  head, never > min consumer offset when floored, monotone across
  sweeps), commit row-id range tiling under generated concurrent
  request mixes.

### 2. Cross-language differential vectors

`pyhoglake/tests/vectors/bounds_vectors.json`: canonical
(type, value, hex) triples generated from the Python codec, consumed by
both suites. The Iceberg single-value encoding exists in Kotlin
(`stats/IcebergSingleValue.kt`) and Python (`pyhoglake/bounds.py`);
any divergence corrupts pruning silently — the vector file makes it a
test failure instead. JVM-side consumer test: planned alongside the
server property tests. When a fuzzer finds a nasty value, it gets
promoted into the vector file.

### 3. API fuzzing (live server)

Adversarial generation against a disposable stack: hostile identifiers
(unicode, injection strings, length boundaries), boundary numerics,
oversized payloads, duplicate registrations, storms of concurrent
commits/offsets/DDL. Lives in the QE test files (`qe-*` catalog
prefix); pathological but reproducible — seeds pinned on failure.

### 4. Coverage-guided fuzzing (JVM, live)

[Jazzer](https://github.com/CodeIntelligenceTesting/jazzer) via
jazzer-junit `@FuzzTest`: the targets are ordinary JUnit tests in
`server/src/test/kotlin/com/posthog/hoglake/fuzz/`, hermetic (pure JVM,
no Docker/DB), so they run in BOTH modes:

- **Corpus replay (every `./gradlew :test` run)**: without `JAZZER_FUZZ`
  set, each `@FuzzTest` deterministically replays its committed seed
  corpus plus the empty input — fast regression tests, on in CI by
  construction.
- **Fuzzing (on demand)**: `cd server && flox activate -- ./gradlew
  fuzz -PfuzzSeconds=900 -PfuzzSweepSeconds=60` runs every target under
  libFuzzer. jazzer-junit allows one fuzz test per JVM, so each target
  has its own task (`fuzzParquetFooterFuzzTest`, ...) — runnable
  individually, chained sequentially under the umbrella `fuzz` task.

**Two budget classes.** A target whose coverage has stopped moving is
not searching any more, and the nightly was spending most of its hours
on exactly those. From the 2026-09-19 run, 600 seconds each:

| Target | Executions | Coverage start → end | exec/s |
|---|---|---|---|
| `IcebergSingleValueCompareFuzzTest` | 356M | 145 → 145 | 592k |
| `IcebergSingleValueDecodeFuzzTest` | 80M | 146 → 146 | 134k |
| `BoundWireFuzzTest` | 76M | 128 → 128 | 126k |
| `PuffinDeletionVectorFuzzTest` | 57M | 142 → 142 | 94k |
| `IdentifiersFuzzTest` | 30M | 102 → 102 | 50k |
| `WireDtoParseFuzzTest` | 7.5M | 620 → 645 | 12k |
| `ParquetFooterFuzzTest` | 349k | 544 → 550 | 580 |
| `NestedAgreementFuzzTest` | 7k | 2564 → 2564 | 11 |

The first five are **saturated**: tens of millions of executions moved
the coverage counter by nothing at all, so their run is a regression
**sweep** over the accumulated corpus — it re-proves a settled contract
after a change — and it gets `-PfuzzSweepSeconds` (nightly: 60s). The
rest, plus the targets for surfaces nothing has fuzzed yet, are the
**soak** class and get `-PfuzzSeconds` (nightly: 900s). Class membership
is a judgement about the target: flat coverage across a full soak earns
a demotion to sweep, and any change to the target's surface puts it
straight back.

Targets (one class, one `@FuzzTest` each):

| Target | Budget class | Surface | Contract fuzzed |
|---|---|---|---|
| `ParquetFooterFuzzTest` | soak | the hydrator's footer parse (`FooterParse.parse`) + `FooterStats` | `IOException` refusals only — `FooterParse` translates every other escape from parquet-java into `FooterParseException`, so the contract needs no stack inspection; `FooterStats.*` total over parsed footers; 1 MiB input cap |
| `NestedAgreementFuzzTest` | soak | `FooterStats.aggregate` (read) and `ParquetRewriter.rewrite` (write) over one generated nested file | typed refusals only on both surfaces; ground-truth binding; no stats for an unowned field id; per-leaf value conservation; a leaf the rewriter copied must have produced reader stats |
| `WireDtoParseFuzzTest` | soak | every structured request body, each to the depth it has: `toModel()` on `CommitRequestDto`, the polymorphic `AlterOp` and the file registrations of `PublishTableCreationDto`; the codec round trip on `PrepareTableCreationDto`; parse-and-reserialize on `CreateCatalogRequestDto`, `ClaimUploadDto`, `UploadOwnerDto` and `AbandonUploadsDto` (none of which has a `toModel`); `parseExpectedTableUuid` / `parseLongQuery` on the guarded-DML query parameters | where a `toModel()` runs, it throws only what ErrorMapping turns into 4xx; where a DTO is reserialized, the bytes re-parse — compared for equality except on the bodies carrying `ColumnStatsDto`, whose `ByteArray` bounds keep identity equals by design |
| `TableCreationDefinitionCodecFuzzTest` | soak | `TableCreationDefinitionCodec.encode`/`decode`, stored-format versions 0-6 | `decode` refuses arbitrary bytes with `CorruptDefinitionException` and nothing else, naming the receipt; `decode(encode(d)) == d`; `encode` writes the LOWEST version that can carry the definition (a rolling deploy's older reader refuses a version it does not know) |
| `CommitReceiptFuzzTest` | soak | the stored commit receipt: `CommitFingerprint.commitFingerprint` over a `CommitRequest` parsed by the production wire mapper | handler-phase throws are only what StatusPages installs a handler for — `HoglakeException`, `CorruptDefinitionException`, `BadRequestException`, `JsonConvertException`, `ContentTransformationException` — and deliberately NOT `JacksonException`, which reaches the `Throwable` arm as a 500; the fingerprint is deterministic, idempotent (`fingerprint(parse(fingerprint(r))) == fingerprint(r)`) and invariant under permutation of `appends`, of `files` within an append and of `column_stats` within a file |
| `IcebergSingleValueDecodeFuzzTest` | sweep | `IcebergSingleValue.decode` (arbitrary bytes × every ColType) | only `IllegalArgumentException` refusals; `encode(decode(x)) == x` for canonical encodings |
| `IcebergSingleValueCompareFuzzTest` | sweep | `encode`/`compareValues` over generated typed values | round-trip; comparator sign-antisymmetry, reflexivity, transitivity, equals-consistency |
| `BoundWireFuzzTest` | sweep | `BoundWire.render` (arbitrary type × scale × bytes — the decode surface of `.../files/{fileId}/stats` and `/v1/debug/decode-bound`) | only `IllegalArgumentException` refusals (the family the endpoints map to 422/null); every produced node serializes and reparses through the production mapper; total over the full int64 temporal range |
| `PuffinDeletionVectorFuzzTest` | sweep | `PuffinDeletionVector.read` (highest value: fresh code on writer-supplied bytes) | loud typed refusals (IAE/ISE/IOException), deterministic decode, no silent mis-decode |
| `IdentifiersFuzzTest` | sweep | `Identifiers.validate` + the RequestId header shape | only `HoglakeException.Validation`; decisions stable and equal to a character-walk reference of the documented policy |

`CommitReceiptFuzzTest` deliberately says nothing about the order of
`deletes`: `commitFingerprint` canonicalises `appends` only, so two
orderings of one delete set fingerprint differently today. That gap is
being closed separately, and asserting it here would red the nightly on
a known defect instead of finding new ones.

**Keeping a target fast enough to be worth its budget.** parquet-java's
no-argument `ParquetWriter.Builder` and `ParquetFileReader.open` each
build a fresh Hadoop `Configuration`, and a fresh `Configuration`
re-parses `core-default.xml` out of the hadoop-common jar: 0.74 ms,
every time. The nested campaigns opened enough of them per iteration to
spend most of the budget parsing the same XML over and over — one
generated footer read went from 0.83 ms to 0.012 ms once `NestedFuzz`
built the parquet read options once and shared them. With that, an
in-memory `InputFile`/`OutputFile` pair in place of temp files
(`MemoryParquetFiles.kt`), one read-back of the input instead of one per
column, and an uncompressed rewrite codec (the agreement oracles judge
schema pairing, not page compression), `NestedAgreementFuzzTest` got two
speedups, which are different measurements and not one number:

- **the harness**, driven by a seeded RNG over identical seeds, went
  from 30 to 144 iterations per second;
- **the libFuzzer task** (`fuzzNestedAgreementFuzzTest`, 60s) went from
  11 to 113 exec/s, and its coverage from flat at 2564 over 600 seconds
  to 1934 → 2510 in 60.

Both were taken on one developer machine: they are ratios, not a spec.
What remains is the same `Configuration`, built inside
`FooterParse.parse` and `ParquetRewriter.rewrite` — production's call to
make, not the harness's (#165).

**Phase discipline (learned the hard way, 2026-09-06.)**
`WireDtoParseFuzzTest` originally asserted over `readValue` *and*
`toModel()` together,
and duly "found" a crash: a byte run Jackson's encoding detector reads
as UTF-32 raises `java.io.CharConversionException`, which is an
`IOException`, not a `JacksonException`. It was a harness artifact —
`readValue` runs inside ktor's ContentNegotiation, which wraps *any*
converter throw into a 400, so a parse-phase exception can never reach
the client as a 500. `toModel()` runs in the route handler, outside
that wrapping, and is the only phase where the exception class decides
the status code. The target now asserts on that phase alone; the wire
behavior it cannot observe is pinned end-to-end in
`api/WireParseErrorMappingTest` (hostile bytes -> 400 through the real
serialization + StatusPages stack). The crashing input stays in the
corpus — it is a genuinely interesting encoding trap, just not a bug.
General rule: a fuzz target that spans a framework boundary must assert
on the side of the boundary where the contract actually lives.

Corpus layout (jazzer-junit's inputs convention — the resource
directory doubles as seed corpus and replay fixture):

    server/src/test/resources/com/posthog/hoglake/fuzz/
        <TargetClass>Inputs/<fuzzMethod>/<seed files>

Seeds are generated by `./gradlew generateFuzzSeeds`
(`fuzz/FuzzSeedGenerator.kt`, manual, output committed): every vector
from `pyhoglake/tests/vectors/bounds_vectors.json` (type byte +
encoding) for the codec targets, real parquet-java-written footers
(with/without field ids, truncated), spec-conformant + CRC-corrupted
puffin DV blobs, boundary identifiers, representative wire bodies, one
stored table-creation receipt per format version (1-6 written by the
codec itself, 0 hand-written because no encoder emits it any more), and
the guarded-DML commit shapes.

`CommitReceiptFuzzTest` reads its input as a LAYOUT — eight bytes of
permutation entropy, then the request body — and a seed has to match it.
The prefix is a FIXED size and comes off the FRONT of the
`FuzzedDataProvider` (`consumeBytes`, never `consumeInt`), so a seed
stays readable and, unlike a length prefix, a mutation in the body never
moves the boundary. Its nonces are not arbitrary: the generator searches
for one whose permutation is non-identity at every level the seed can
exercise, running the target's own `permuteCommitRequest` to decide, and
fails the build if it cannot find one. A seed whose shuffle happens to
be the identity asserts nothing, and replay-only PR CI cannot tell the
difference — every committed receipt seed once passed with the
canonicalising sort deleted, for exactly that reason.

`TableCreationDefinitionCodecFuzzTest` has no layout at all, on purpose:
the whole input is the stored receipt AND the generator's tape. It did
start with a length-prefixed receipt, and that cost it the decode arm —
any mutation changing the receipt's length moved the boundary, so
libFuzzer's feedback stopped pointing at the byte it had changed
(coverage flat at 574 across 735k executions; a hand-written
`type_params: 5` receipt the oracle does catch went unfound in 30s).
Crashing inputs found while fuzzing are written back into the same
directories by jazzer-junit (instant regression tests); the growing
generated corpus lands in `server/.cifuzz-corpus/` (transient, never
committed).

Run the targets with `-XX:-OmitStackTraceInFastThrow` (wired into both
the `test` and `fuzz*` tasks). Fuzzing makes an exception site hot, and
HotSpot then throws a preallocated instance with no stack trace and no
message: the finding becomes undiagnosable, and any assertion that reads
`e.stackTrace` silently stops matching. Issue #15 was filed against the
wrong class for exactly that reason, and the erased stack was hiding two
further defects behind the first one.

Promotion rule (unchanged): a fuzzer-found nasty value becomes a
committed corpus entry here, a pinned regression test, and — when the
surface is cross-language (the bounds codec) — an entry promoted into
`bounds_vectors.json` by the Python side; the JVM side never edits the
vector file directly.

## Rules

- A fuzzer-found bug becomes: a pinned regression test (exact input,
  not a seed), an entry in the ledger if it's a design-class defect,
  and a vector-file entry when cross-language.
- Property tests run in the normal suites (`just test-all`) with
  bounded example counts; deep runs (`--hypothesis-seed`, higher
  max_examples, `./gradlew fuzz -PfuzzSeconds=...` soaks) are
  manual/periodic.
