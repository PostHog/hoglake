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
  fields must fail cleanly, never leak `KeyError`). The VARIANT codec's
  properties live in
  `tests/variant_conformance/qe_prop_variant_codec.py` (beside its other
  suites, which the `tests/qe_*.py` glob misses): JSON and Python values
  round-trip to the source value under generated declarations, encoding
  is deterministic however the rows are chunked, an invalid row becomes
  SQL NULL and nothing else moves, aware datetimes round-trip or are
  refused, and mutated Variant bytes are read or refused with
  `VariantEncodingError`, never another exception. The VARIANT footer's
  are beside them, in `qe_prop_variant_footer.py`: a file written for a
  generated declaration is, byte for byte, what a generic compact-Thrift
  re-encoding (`tests/footer_oracle.py`) makes of pyarrow's bytes with
  the annotations added, and passes the strict layout check; schema
  elements mutated from real footers get a fault or none from
  `variant_layout_fault`, never an exception; and those files, with
  footer bytes overwritten and `created_by` forged (non-UTF-8 among it),
  or with the column chunk fields pyarrow reads lazily forged (type,
  statistics, SizeStatistics, encryption), or with schema element fields
  spelled with another wire type, given twice or dropped, and the schema
  list given twice, whenever pyarrow still opens them, are accepted or
  refused with `ValidationError` by `validate_variant_file`, strict or
  not, and by `validate_column_chunks`; what the first accepts
  `extract_column_stats` reads, and what the last accepts every chunk's
  metadata and statistics read. The overwrite property found pyarrow
  (23.0.0 to 26.0.0) aborting the process when it reads the statistics
  of a chunk whose type is not its schema leaf's, or whose min or max is
  too short for the type, and review found the same abort when it builds
  a chunk's metadata (`row_group.column(i)`) with a level histogram that
  does not fit the leaf, unencoded byte array bytes on a chunk that is
  not BYTE_ARRAY, or a column-key encryption: the C++ exception is not
  translated, so no Python handler sees it. Random overwrites rarely
  spell the latter, which is why the chunk property forges fields
  instead. Every prepared file's chunks are checked from the footer's
  bytes before any is read (`parquet_schema._ChunkCheck`, run by
  `validate_variant_file` and, for a file it does not see, by
  `validate_column_chunks`), and each finding is a regression test in
  `test_variant_schema.py` and `test_append_unit.py`. The extended
  property also found a `path_in_schema` that is not UTF-8 escaping
  `extract_column_stats` as `UnicodeDecodeError`; the same check refuses
  it. Review then found the check reading a footer otherwise than
  pyarrow: Thrift's generated readers skip a field whose wire type is
  not the declared one (a repetition_type spelled as an i8 is no
  repetition_type, so the leaf is REQUIRED), merge a repeated struct
  field into the first copy, and keep the last of two schema lists. So
  each chunk is now held to pyarrow's own ColumnDescriptors, the footer
  reader skips a mistyped field as they do, and a footer with a repeated
  field is refused; the forging properties spell both. A further review
  found Python's Thrift keeping a varint whole where the generated
  readers keep its low bits: a field id 65536 + k is field k to pyarrow
  and parquet-java, which hid a checked field from the guard (and a
  delta sum wraps the same way), and an i32 or a size likewise. So a
  field id outside an i16, and an integer or a size wider than its type,
  are refused. Thrift's own writer will not spell either, so the
  properties cannot draw them; `footer_oracle.encode_unchecked` spells
  them for the regression tests. Review then found the reader checking a
  list's item type only for the lists it declared: the generated readers
  read every list's items as parquet.thrift declares them, whatever the
  header says, so an encodings list whose header named one binary hid,
  inside it, Statistics pyarrow read as the chunk's own, and aborted on.
  Every list parquet.thrift declares on the way to what the guard reads
  is now declared, and a header of another item type refused; a further
  property
  (`test_list_items_of_another_type_are_accepted_or_refused_never_raised_past`)
  spells such headers, with chunk fields hidden in an encodings item
  among them. A list, a set or a map in a field parquet.thrift does not
  define is refused outright now, wherever it is, so a list a later
  parquet.thrift declares cannot reopen that; every field it does define
  in the structs on the way is declared, read or not, so one spelled
  with another wire type is skipped by its header as the generated
  readers skip it, and the refusal names the field it is in. Review also
  found the reader holding every field it read as Python objects, some
  70 bytes a footer byte, so a 60 MB footer of empty structs pyarrow
  opened in 105 MB ran the caller out of memory: what no check reads is
  now read past and not kept, and the row groups are checked as they are
  read, so the peak is linear in the footer, a few times it for a wide
  schema, which is kept
  (`test_a_footer_is_checked_in_memory_linear_in_its_bytes`). No
  property covers memory; that test pins it. A later review found two
  disagreements the properties could not draw: a union (a logical type,
  a time unit, a column order) of two fields, which pyarrow reads as a
  struct and parquet-java's TUnion as its first field, then reads the
  rest of the footer out of step, and a group with a physical type, a
  group to pyarrow and a primitive to parquet-java. Both are refused, on
  files shared with the server's FooterStatsTest. It also found the
  skip's map branch and its double reader pinned by nothing; the chunk
  property now spells maps of keys and values of other types, and two
  regression tests hide 1-byte bounds behind a map and a double that a
  misread skip would swallow. A further review found readers parting on
  what pyarrow opens: an annotation (pyarrow reads the logical type,
  parquet-java lets a converted type that spells another type win, and
  builds what fits the element alone), a logical type of no member, a
  required enum of an undefined value, a chunk path that is not its
  leaf's, and an IEEE754 column order on an integer. Each is refused;
  the chunk property now forges codecs, paths and page encoding stats,
  and holds every accepted chunk's path to pyarrow's own leaf path, and
  the schema element property forges annotations and column orders. The
  annotation rule's ground truth is parquet-java, through a vector file
  (below).
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

`pyhoglake/tests/vectors/variant_shredding_vectors.json`: VARIANT
`type_params.shredding` declarations, each with the decision the
server's `VariantShredding.validate` reaches — accept, or the exact
path and problem of its refusal. pyhoglake's `validate_shredding`
mirrors the grammar so that a bad declaration fails before the round
trip, and both suites read the file with the count pinned on both sides
(`test_variant_ddl.py`'s `EXPECTED_VECTOR_COUNT` and
`VariantShreddingVectorFile.EXPECTED_COUNT`). Unlike the bounds file,
most of its expectations are transcribed by hand from the server's
tests, seeds and documented grammar, and verified by the JVM-side
consumer, `VariantShreddingVectorFileTest`, against the real validator.
The case-folding vectors (GREEK CAPITAL SIGMA, whose lowercase the JDK
decides by word boundaries, and case pairs of newer Unicode data) and
the fault-order witnesses were recorded from that validator, as no
independent oracle decides them; for those the JVM test is a regression
snapshot.
That test also requires every `request_*` seed of
`VariantShreddingFuzzTestInputs/declarationsFollowTheDocumentedRules`
to be in the file verbatim, as vector `fuzz_<seed>`, and every
`refusal(...)` template of `VariantShredding.kt` to have a vector. A
refusal marked `"client": "may_accept"` is a case collision the client
cannot decide exactly as the JDK does (GREEK CAPITAL SIGMA, a case pair
newer than its Unicode data) and leaves to the server; both tests refuse
the marking on any other refusal, and on a pair of ASCII names. On a
Python whose Unicode data is newer than the server's, the client leaves
every case pair that is not ASCII to the server, and the Python test
reads every such refusal as may_accept there (it runs the file under
both this Python's data and newer data).

`pyhoglake/tests/vectors/schema_annotation_vectors.json`: one schema
element of each pairing of physical type (or group), logical type,
converted type and DECIMAL scale and precision fields, each with how
parquet-java reads it: as pyarrow does (`reads`, the logical type where
there is one), with the converted type winning over a logical type that
spells another (`reads_converted`), or not at all (`refuses`). The
verdicts were recorded from parquet-java 1.18.1, and the Trino
connector's `ParquetMetadata` measured against them; the JVM-side
consumer, `SchemaAnnotationVectorFileTest`, builds each footer with
parquet-format's Thrift classes and asserts the verdict through
`FooterParse`, and `test_variant_schema.py` that pyhoglake refuses an
element exactly when it is not `reads`. The count is pinned on both
sides (`ANNOTATION_VECTOR_COUNT` and
`SchemaAnnotationVectorFile.EXPECTED_COUNT`).

The VARIANT decoder has no second implementation here to diff against,
so its external ground truth is apache/parquet-testing's `variant/` and
`shredded_variant/` vectors, vendored in
`pyhoglake/tests/data/parquet-testing` (read by
`test_parquet_testing.py`), and small files DuckDB 1.5.5 wrote
(`pyhoglake/tests/data/README.md` has their regeneration SQL). A fuzzer
finding in the codec gets promoted into the suites' hand-built vectors
(`test_encoding_vectors.py`, `DECODER_FAULTS`).

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
| `WireDtoParseFuzzTest` | soak | every structured request body, each to the depth it has: `toModel()` on `CommitRequestDto`, the polymorphic `AlterOp` and the file registrations of `PublishTableCreationDto`; the codec round trip on `PrepareTableCreationDto`; `ColumnTrees.validate` on the columns of `PrepareTableCreationDto` and of each `add_column`; parse-and-reserialize on `CreateCatalogRequestDto`, `ClaimUploadDto`, `UploadOwnerDto` and `AbandonUploadsDto` (none of which has a `toModel`); `parseExpectedTableUuid` / `parseLongQuery` on the guarded-DML query parameters | where a `toModel()` runs, it throws only what ErrorMapping turns into 4xx; where a DTO is reserialized, the bytes re-parse — compared for equality except on the bodies carrying `ColumnStatsDto`, whose `ByteArray` bounds keep identity equals by design |
| `TableCreationDefinitionCodecFuzzTest` | soak | `TableCreationDefinitionCodec.encode`/`decode`, stored-format versions 0-6 | `decode` refuses arbitrary bytes with `CorruptDefinitionException` and nothing else, naming the receipt; `decode(encode(d)) == d`; `encode` writes the LOWEST version that can carry the definition (a rolling deploy's older reader refuses a version it does not know) |
| `CommitReceiptFuzzTest` | soak | the stored commit receipt: `CommitFingerprint.commitFingerprint` over a `CommitRequest` parsed by the production wire mapper | handler-phase throws are only what StatusPages installs a handler for — `HoglakeException`, `CorruptDefinitionException`, `BadRequestException`, `JsonConvertException`, `ContentTransformationException` — and deliberately NOT `JacksonException`, which reaches the `Throwable` arm as a 500; the fingerprint is deterministic, idempotent (`fingerprint(parse(fingerprint(r))) == fingerprint(r)`) and invariant under permutation of `appends`, of `files` within an append and of `column_stats` within a file |
| `VariantShreddingFuzzTest` | soak | the `type_params.shredding` rules (`VariantShredding.validate`, through `ColumnTrees.validate` on a top-level variant): arbitrary declarations parsed by the production wire mapper, and declarations a generator builds by the documented grammar and then breaks at most one rule of, or takes to the field or depth limit | arbitrary input: `HoglakeException.Validation` refusals only, bounded messages, the same decision after a write-and-read round trip; generated input, against the generator's own ground truth: a sound declaration is accepted, a broken one refused; its `request_*` seeds are also vectors of `variant_shredding_vectors.json` (§2), which the Python client decides too |
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
vector file directly. The shredding grammar is cross-language too, the
other way round: a new `VariantShreddingFuzzTest` `request_*` seed, or a
nasty declaration the target finds, goes into
`variant_shredding_vectors.json` verbatim with the server's decision,
with `EXPECTED_COUNT` bumped on both sides, and `pyhoglake/variant.py`
changes with it if the client decides it differently.

## Rules

- A fuzzer-found bug becomes: a pinned regression test (exact input,
  not a seed), an entry in the ledger if it's a design-class defect,
  and a vector-file entry when cross-language.
- Property tests run in the normal suites (`just test-all`) with
  bounded example counts; deep runs (`--hypothesis-seed`, higher
  max_examples, `./gradlew fuzz -PfuzzSeconds=...` soaks) are
  manual/periodic.
