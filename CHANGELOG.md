# Changelog

Latu follows [Semantic Versioning](https://semver.org). Before 1.0, a minor version may rename or
remove; each such change is listed here with the migration in one line.

## 0.2.0 — 2026-09-05

The seam the companion ML package builds on. Everything here is additive; no migration.

**`Latu.Result.Literal` is public.** `Latu.Result.Literal.value/1` turns a literal the server
sent into an Elixir term. It was already how `observe` metrics decode; a fitted model's
attributes — a coefficient, an intercept, a vector — come back the same way, so a package built
on Latu needs it by name.

**A UDT literal decodes to `%Latu.Result.UDT{}`** rather than raising. Spark serialises `Vector`
and `Matrix` as struct literals typed by a JVM class instead of by field names, so there is
nothing to key a map by: the class and the elements come back as data, in the order that class
defines them, and the caller interprets them. PySpark raises on every UDT literal —
`docs/deviations.md`.

**`Latu.Plan.relation/1` is public**, wrapping a `rel_type` arm as a `Relation` carrying a fresh
`plan_id`. A package building relation arms Latu has no verb for needs the one allocator; a
second wrapper out of tree would be a second sequence.

**The execution latches `ml_command_result`.** The transport kept the `SqlCommand` arm and
dropped the rest, so an `MlCommand`'s answer was discarded. It is latched like the SQL result —
first one wins, so a replay after a reattach cannot clobber it.

## 0.1.1 — 2026-09-04

The README's links to the guides, `usage-rules`, deviations and contributing are absolute
hexdocs URLs. hex.pm renders the README from the package, which does not carry those files, so
the relative links 404'd there. No code change.

## 0.1.0 — 2026-09-04

First release, against Spark **4.2.0**.

A native Elixir DataFrame API over Spark Connect: session and configuration; the relational
verbs, `Latu.Column`, coercion and aggregation; a generated function library of 498 functions
with Spark's own documentation; windows and higher-order functions; readers, writers, `sql`,
views and the catalog; `create_dataframe/3` from Explorer or rows; subqueries; the whole
AnalyzePlan surface; `na`/`stat`; `observe`, checkpoint, merge, interrupt, progress and
Telemetry; results as maps, Explorer frames, a stream of frames, or raw Arrow; reattachable
execution with PySpark's retry policy; Livebook rendering behind an optional `kino` dep.

Every place the API departs from PySpark is in `docs/deviations.md`, with why.
