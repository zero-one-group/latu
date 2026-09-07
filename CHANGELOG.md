# Changelog

Latu follows [Semantic Versioning](https://semver.org). Before 1.0, a minor version may rename or
remove; each such change is listed here with the migration in one line.

## 0.4.0 — 2026-09-07

One verb. Additive; no migration.

**`Latu.add_jar/3`** puts a jar on the session, so a class in it resolves by name for the rest
of it. `AddArtifacts` under a `jars/` prefix routes to `sparkContext.addJar`, and Latu already
had the whole chunked upload path — 32 KiB chunks, CRC, batching — pointed at `cache/`; this
threads the prefix through it. Latu still ships no code of its own and does no local file IO:
the jar is bytes you hand it, under a name.

Two server rules the docs now state: re-sending identical bytes under a name the session holds
is a no-op, and different bytes under that name are refused — a jar cannot be replaced in a
live session. `Latu.sql(session, "LIST JARS")` shows what a session holds.

## 0.3.0 — 2026-09-07

Tensors out of a result, and a round of correctness fixes. Everything here is additive; no
migration.

**`Latu.to_nx/2`, `to_nx!/2` and `stream_nx/2`** turn a result into `Nx` tensors. A numeric
column with no nulls becomes a 1-D tensor whose binary *is* the Arrow buffer — no copy for a
single batch — and a column of equal-length numeric lists, or of dense MLlib `Vector`s, becomes
one `{rows, width}` tensor. Everything else is refused by name.

This is the only way to read a `Vector` column into Elixir: Spark describes one as a UDT with
no SQL type, so `collect/2` and `to_explorer/2` both refuse it, while the Arrow stream carries
its own schema and says exactly what it is. `Latu.Result.Arrow` is the reader — the IPC
streaming format, no dependency — and `Latu.Result.Nx` the mapping, behind the now-optional
`:nx`. Adding `{:nx, "~> 0.13"}` is what turns them on; without it `to_nx/2` says so.

**Every RPC retries on the session's `Latu.Retry`**, not only the result stream — PySpark's own
behaviour — except the best-effort releases. An error carrying a `RetryInfo` is retried whatever
its status, its delay a floor under the backoff capped by the new `max_server_retry_delay`
(10 min); `%Latu.Error{}` gains `retry_delay`, and a unary call's `[:latu, :retry, :attempt]`
carries `rpc` where an execution's carries `operation_id`.

**Fixed.** A result with two columns of one name is refused, naming the column, where Polars
used to panic inside its IPC reader — a join whose sides share a non-key name was the usual way
there. `select(df, "t.*")` is every column of `t`, not a column called `t.*`, and
`Latu.col(df, "*")` is every column of that frame, as PySpark's `col` and `df["*"]` read them.
`create_dataframe/3` matches a `schema:` to the data by name — the server applies it by position
and row maps sort by key, so a schema in another order put values under the wrong names; a
schema sharing no name still renames by position, a half match is refused, and a malformed one
fails as the frame is built. `error_details/2` restores a message the server abbreviated to
2048 characters. An IPv6 literal host connects (`sc://[::1]:15002` crashed inside elixir-grpc),
and a cleartext token is allowed to every loopback address. `SPARK_USER` precedes the OS user as
the default `user_id`; `lit/1` refuses a non-UTF-8 binary and names Spark's `X'…'`;
`true`/`false` are refused where a column name is taken. `disconnect/2` closes the socket within
a second — Gun waited 15 s for a close the Spark server never sends, enough to hit an open-files
limit at a few connections a second.

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
