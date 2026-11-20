package com.posthog.hoglake.hoglite

/**
 * hoglite refused a transaction with `FOREIGN KEY constraint failed`.
 *
 * The tenant schema declares the referential integrity Postgres's does
 * (HogliteSchema, Referential integrity), and hoglite enforces it on
 * every connection, so this is what an orphan — a file row for a missing
 * table, a change row for a missing snapshot, a deleted namespace with
 * versioned history under it — becomes instead of silent corruption.
 * Nothing a client sends can produce it: every statement the store
 * writes resolves its parents under guards first, so it is a store bug,
 * surfaced as a 500 (the catch-all handler logs it with its stack) and
 * never as a client error. The transaction rolled back cleanly.
 */
class HogliteIntegrityViolation(
    catalog: String,
    detail: String,
) : IllegalStateException(
        "hoglite refused a transaction on catalog '$catalog' with a FOREIGN KEY violation " +
            "(a store bug; rolled back cleanly): $detail",
    )
