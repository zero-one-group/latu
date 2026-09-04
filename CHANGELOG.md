# Changelog

Latu follows [Semantic Versioning](https://semver.org). Before 1.0, a minor version may rename or
remove; each such change is listed here with the migration in one line.

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
