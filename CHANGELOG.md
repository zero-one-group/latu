# Changelog

Latu follows [Semantic Versioning](https://semver.org). Before 1.0, a minor version may rename or
remove; each such change is listed here with the migration in one line.

## Unreleased

**A Spark versions page**, `docs/spark-versions.md`: what a 4.2 client does against 4.1.3 and
4.0.4, measured by the integration suite, plus 3.5, newer servers and the managed platforms'
URL shapes. Two things the runs changed in Latu. A bytes conf with a unit suffix, which is how
4.1 reports `localRelationSizeLimit`, is parsed rather than crashed on. And the two ways an
older server refuses a 4.2 client, `UNIMPLEMENTED` for an RPC it lacks and an unset-oneof
`INTERNAL_ERROR` for a plan node protobuf dropped, now carry a sentence naming the cause.

**`Latu.copy_to_fs/3`.** Bytes to a file on the cluster's filesystem, PySpark's
`copyFromLocalToFs`: `AddArtifacts` under the `forward_to_fs` prefix, written by the server with
Hadoop's `FileSystem` onto its default filesystem, overwriting. The route for handing Explorer a
file when the server's disk is not yours, and the durable alternative to `create_dataframe/3`.
A local-disk destination needs `spark.sql.artifact.copyFromLocalToFs.allowDestLocal` on the
session or the server.

**An error the status cut at 2048 characters arrives whole.** Latu fetches the detail for that
one case before returning the error, so `message` is complete and `causes` is filled; every
other error keeps `error_details/2` as the explicit call.

**A Livebook notebook**, `notebooks/quick_start.livemd`, and a "Run in Livebook" badge.

**Nested data.** `Latu.Column.get_field/2` and `get_item/2` read a struct field, an array
element or a map value out of any expression, where a dotted name only ever reached a named
column. `with_field/3` and `drop_fields/2` edit a struct in place. Two expression nodes Latu
did not build before, `UnresolvedExtractValue` and `UpdateFields`; three goldens.

**Elixir 1.18 is the floor**, down from 1.20. Nothing in the package needed the newer versions;
`JSON` needs 1.18 and so does a dependency. CI compiles and tests the floor as its own job.

**The Spark versions workflow reads in both directions.** Its goldens and its summary now run
when the integration suite is red, which against an older server it always is, and the summary
lists every failing test rather than the ten GitHub annotates. `spark_version/1`'s test asserts
the version the compose file started instead of 4.2 by name.

## 0.6.1 — 2026-09-10

**The `docker run` line in the README and the quick start now starts a server that stays up.**
It was missing `--wait`, and without it `start-connect-server.sh` daemonizes, the container's
foreground process returns and the container exits with the server it just launched. It also
passed `--packages org.apache.spark:spark-connect_2.13:...`, which the Spark 4.x assembly
already carries: `docker-compose.yml` has never passed it and the whole suite runs against
that server.

**Two guide notes on running Spark in a container.** `quick-start.md` says a container is the
fastest way to a server and the wrong one once you write files. `from-explorer.md` gains
"Handing over a file rather than a frame", because that page covered the Arrow boundary in
both directions and never the file one, where `Latu.write/2`'s path and the path Explorer
reads name different disks. A bind-mounted Spark writes as uid 185, so on Linux the process
that asked for the write cannot rename what came back.

## 0.6.0 — 2026-09-10

**Structured streaming.** `read/2` and `table/3` take `is_streaming: true`; `with_watermark/3`
and `distinct/3`'s `within_watermark:` cover the stateful side; `write_stream/2` starts a query
with the `:available_now`, `:once`, processing-time and continuous triggers and hands back a
`%Latu.StreamingQuery{}`, whose module carries every `StreamingQueryCommand` and the four
`spark.streams` verbs PySpark uses. `with_stream/3` is the bracket. `await_termination/2` is a
loop of bounded server waits rather than one call, because a Connect server cuts a silent
response stream every `senderMaxStreamDuration`; the semantics are Spark's. Progress and status
decode into snake-cased maps of Spark's own JSON. `Latu.Error` gains the kind `:query`, for a
streaming query's own failure as `exception/1` reports it. `foreach` and `foreachBatch` are not
offered at all, because both carry a serialised closure; `docs/deviations.md` says why.

**The streaming listener bus**, as `Latu.StreamingQuery.events/1`: a lazy `Stream` of every
streaming event on the session, each one Spark's own JSON snake-cased under a `:type` of
`:progress`, `:idle`, `:terminated` or `:unknown`. Opens on first enumeration, closes when the
enumeration ends. `[:latu, :streaming, :event]` is emitted per event.

**`Latu.Client.Execution`'s empty-reattach guard is now per-execution.** It was a flat 100
empty response streams for everything; a result or a command still gets that, and the listener
bus gets `silence: :expected`, where an idle stream is normal *after* the server has answered
but still fatal before it. Internal, and the reason is arithmetic: a Connect server ends a
silent stream every `senderMaxStreamDuration` whether or not it sent anything, so at 100 a
healthy bus died after eight minutes on a 5s sender.

**One breaking change, in the plan layer.** `Latu.Plan.table/2`'s second argument used to *be*
the reader options; it is now an option list, so they move under `options:` and `is_streaming:`
joins them — `Plan.table("t", merge_schema: true)` becomes
`Plan.table("t", options: [merge_schema: true])`. That is `Latu.Plan.read/1`'s shape, which the
old one gratuitously differed from. A map, previously accepted directly, is refused by name.
**`Latu.table/2,3` is unchanged** and still takes reader options flat, so this only reaches code
building plans through `Latu.Plan` itself. Everything else in this release is additive.

The 2026-09-02 decision that streaming was a separate package is reversed in
`docs/decisions.md`.

## 0.5.0 — 2026-09-08

One behavioural change, and it is the reason this is a minor rather than a patch.

**A `user_agent` in the connection URL now composes rather than replaces.**
`sc://host:15002/;user_agent=my-app` used to set `client_type` to exactly `my-app`; it now sets
`my-app latu/0.5.0 elixir/… otp/…`, which is how PySpark composes it. The `client_type:` option
still replaces the whole string, so that is the migration if you were relying on the old
behaviour. A `user_agent` longer than 2048 bytes once percent-escaped is now refused rather than
sent, matching PySpark's cap.

**The cheatsheet page was wrong in two ways and is regenerated.** The `+ !` marker that flags a
verb with a raising twin was appended before the summary was clamped to fit its cell, so 17 of
the 88 verbs that have one lost the marker. And the first-sentence split treated `e.g.` as a
sentence end, which left `spark_version/1` reading "The Spark version the server reports, e.g."
with the example eaten. Both are generator bugs, and `dev/cheatsheet.exs` now has unit tests
rather than only a diff against its own output.

**A `range_between/3` offset outside 64 bits is refused by name.** It previously escaped as a
`FunctionClauseError` on a private function, where the `rows_between/3` equivalent already
raised an `ArgumentError` saying what was wrong. Unreachable in practice.

**The documentation was rewritten.** The README, the four guides and `usage-rules.md` are the
same facts in a different voice, about a fifth shorter. MLlib now links to
[`latu_ml`](https://hexdocs.pm/latu_ml), which is published, where the prose used to describe it
as a package that might one day exist.

Internals, with no surface change: the transport grew a single request envelope in place of
twelve copies of one error, the reattach handler keeps one snapshot instead of four, and the Nx
decoder lost a triple list reversal that was correct only because two of the three cancelled.
`priv/proto/VERSION` records which Spark release the vendored protos came from.

## 0.4.0 — 2026-09-07

One verb. Additive; no migration.

**`Latu.add_jar/3`** puts a jar on the session, so a class in it resolves by name for the rest
of it. `AddArtifacts` under a `jars/` prefix routes to `sparkContext.addJar`, and Latu already
had the whole chunked upload path (32 KiB chunks, CRC, batching) pointed at `cache/`; this
threads the prefix through it. Latu still ships no code of its own and does no local file IO:
the jar is bytes you hand it, under a name.

Two server rules the docs now state: re-sending identical bytes under a name the session holds
is a no-op, and different bytes under that name are refused. A jar cannot be replaced in a
live session. `Latu.sql(session, "LIST JARS")` shows what a session holds.

## 0.3.0 — 2026-09-07

Tensors out of a result, and a round of correctness fixes. Everything here is additive; no
migration.

**`Latu.to_nx/2`, `to_nx!/2` and `stream_nx/2`** turn a result into `Nx` tensors. A numeric
column with no nulls becomes a 1-D tensor whose binary *is* the Arrow buffer, with no copy
for a single batch. A column of equal-length numeric lists, or of dense MLlib `Vector`s,
becomes one `{rows, width}` tensor. Everything else is refused by name.

This is the only way to read a `Vector` column into Elixir: Spark describes one as a UDT with
no SQL type, so `collect/2` and `to_explorer/2` both refuse it, while the Arrow stream carries
its own schema and says exactly what it is. `Latu.Result.Arrow` is the reader, covering the
IPC streaming format with no dependency, and `Latu.Result.Nx` is the mapping, behind the
now-optional
`:nx`. Adding `{:nx, "~> 0.13"}` is what turns them on; without it `to_nx/2` says so.

**Every RPC retries on the session's `Latu.Retry`**, not only the result stream, as PySpark
does, except the best-effort releases. An error carrying a `RetryInfo` is retried whatever
its status, its delay a floor under the backoff capped by the new `max_server_retry_delay`
(10 min); `%Latu.Error{}` gains `retry_delay`, and a unary call's `[:latu, :retry, :attempt]`
carries `rpc` where an execution's carries `operation_id`.

**Fixed.**

- A result with two columns of one name is refused, naming the column, where Polars used to
  panic inside its IPC reader. A join whose sides share a non-key name was the usual way there.
- `select(df, "t.*")` is every column of `t`, not a column called `t.*`.
- `Latu.col(df, "*")` is every column of that frame, as PySpark's `col` and `df["*"]` read them.
- `create_dataframe/3` matches a `schema:` to the data by name. The server applies it by
  position and row maps sort by key, so a schema in another order put values under the wrong
  names; a schema sharing no name still renames by position, a half match is refused, and a
  malformed one fails as the frame is built.
- `error_details/2` restores a message the server abbreviated to 2048 characters.
- An IPv6 literal host connects (`sc://[::1]:15002` crashed inside elixir-grpc).
- A cleartext token is allowed to every loopback address.
- `SPARK_USER` precedes the OS user as the default `user_id`.
- `lit/1` refuses a non-UTF-8 binary and names Spark's `X'…'`.
- `true`/`false` are refused where a column name is taken.
- `disconnect/2` closes the socket within a second. Gun waited 15 s for a close the Spark
  server never sends, enough to hit an open-files limit at a few connections a second.

## 0.2.0 — 2026-09-05

The seam the companion ML package builds on. Everything here is additive; no migration.

**`Latu.Result.Literal` is public.** `Latu.Result.Literal.value/1` turns a literal the server
sent into an Elixir term. It was already how `observe` metrics decode; a fitted model's
attributes (a coefficient, an intercept, a vector) come back the same way, so a package built
on Latu needs it by name.

**A UDT literal decodes to `%Latu.Result.UDT{}`** rather than raising. Spark serialises `Vector`
and `Matrix` as struct literals typed by a JVM class instead of by field names, so there is
nothing to key a map by: the class and the elements come back as data, in the order that class
defines them, and the caller interprets them. PySpark raises on every UDT literal. See
`docs/deviations.md`.

**`Latu.Plan.relation/1` is public**, wrapping a `rel_type` arm as a `Relation` carrying a fresh
`plan_id`. A package building relation arms Latu has no verb for needs the one allocator; a
second wrapper out of tree would be a second sequence.

**The execution latches `ml_command_result`.** The transport kept the `SqlCommand` arm and
dropped the rest, so an `MlCommand`'s answer was discarded. It is latched like the SQL result:
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
