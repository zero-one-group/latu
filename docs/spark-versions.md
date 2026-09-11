# Spark versions

Latu targets **Spark 4.2.0**. The vendored protos, the golden plans and the servers
`mix check.all` runs against are 4.2.0, and that is the only version the README claims.

This page is what happens against another Spark, measured rather than promised. The
integration suite ran against other `apache/spark` images with
`.github/workflows/spark-versions.yml`; every table names the image tag and the date. It is a
record of runs, not a support matrix, and a version not listed here was not run.

## How a 4.2 client fails against an older server

Spark Connect is protobuf over gRPC, and protobuf is forgiving in a way that matters here.
Three things can happen when Latu sends something the server predates. Two are refused, one is
not.

**An RPC the server lacks is refused.** gRPC answers `UNIMPLEMENTED`, and `%Latu.Error{}` has
`status: 12` with a message naming the method, such as
`Method not found: spark.connect.SparkConnectService/GetStatus`. Latu adds that the server
does not implement the RPC.

**A field the server lacks is dropped.** An unknown protobuf field is skipped on decode, not
refused. When that field was the arm of a oneof, the server sees the oneof unset and reports
that as its own error: `This oneOf field in spark.connect.Relation is not set: RELTYPE_NOT_SET`
on 4.1, `Expected Relation to be set, but is empty.` on 4.0, with `INTERNAL_ERROR` as the
class where there is one. Latu recognises these phrasings and adds that it sent a node the
server does not know. `Latu.nearest_by_join/4` and `Latu.Catalog.drop_table/3` fail this way
on both versions below.

**An enum value the server lacks is refused.** It decodes as `UNRECOGNIZED` and the server
says so: `Unknown SubqueryType UNRECOGNIZED` is what 4.0 answers an IN subquery with.

**The one that is not refused is the residual risk.** A dropped field that was *optional* on a
message the server does know leaves nothing unset, so the server runs the request without it,
with its own default in place, and reports nothing. Latu offers no knob that would fail this
way: `write_stream/2` has no `real_time` trigger for exactly this reason, since
`real_time_batch_duration` is 4.2-only and a 4.1 server would run a plain processing-time
trigger while the caller believed otherwise. The list of 4.2-only optional fields is Spark's,
and this page cannot prove it empty; it can say the suite found no case where a result differed
silently.

Beyond the wire, an **error class can move** between versions, and matching on one is the right
thing to do, so the moves are listed. And a **conf's string can change shape**: 4.1 reports
`spark.sql.session.localRelationSizeLimit` as `3221225472b` where 4.2 reports a bare count,
which is why Latu parses Spark's byte strings rather than integers.

## Spark 4.1.3

Measured 2026-09-11, `apache/spark:4.1.3`, Latu at `fb941a3` plus the fixes this run produced.
**13 failures in a suite of 1229**, by the surface each uses:

| Surface | What happens on 4.1.3 |
|---|---|
| `Latu.status/2` and its twin, and every verb that reads a status (`interrupt/2`'s tests use it to wait) | `UNIMPLEMENTED`: the `GetStatus` RPC arrives in 4.2. Nine tests. |
| `Latu.nearest_by_join/4` | `INTERNAL_ERROR`, the `Relation` oneof unset: the `NearestByJoin` arm is 4.2. |
| `Latu.Catalog.drop_table/3`, and `Latu.Catalog.drop_view/3` by the same field numbers | `INTERNAL_ERROR`, the `Catalog` oneof unset: `DropTable` and `DropView` are 4.2. `if_exists: true` fails the same way, since the message never arrives whole. |
| `Latu.StreamingQuery` verbs on a handle the server does not know, or a stale run id | The refusal comes back as `INTERNAL_ERROR` rather than `CONNECT_INVALID_PLAN.STREAMING_QUERY_NOT_FOUND` or `STREAMING_QUERY_RUN_ID_MISMATCH`, which are 4.2 classes. Matching on those classes misses on 4.1. Two tests. |

Everything else passes: `create_dataframe/3` over the cache threshold, since
`ChunkedCachedLocalRelation` is 4.1 rather than 4.2 once its byte-string conf is parsed; IN
subqueries; `clone_session/2`; structured streaming apart from the two classes above; and the
whole relational, functions, analysis, config and result surface.

## Spark 4.0.4

Measured 2026-09-11, `apache/spark:4.0.4`, same Latu, locally. **27 failures in a suite of
1230**: 4.1.3's 13 and these:

| Surface | What happens on 4.0.4 |
|---|---|
| `Latu.clone_session/2` and its twin, and `release_session/2`'s tests, which clone first | `UNIMPLEMENTED`: `CloneSession` arrives in 4.1. Eight tests. |
| `Latu.create_dataframe/3` over `spark.sql.session.localRelationCacheThreshold` | Refused by Latu before anything is sent: `spark.sql.session.localRelationSizeLimit` is not defined, and the chunked upload it governs is 4.1. Below the threshold the frame travels inline and works. Three tests. |
| `Latu.Column.isin/2` over a `%Latu.DataFrame{}` | `Unknown SubqueryType UNRECOGNIZED`: `SUBQUERY_TYPE_IN` is 4.1. `isin/2` over a list is a function call and works. Two tests. |
| `Latu.set_conf/3` on a static conf | Refused as on 4.2, under the class `CANNOT_MODIFY_CONFIG`; 4.1 renamed it `CANNOT_MODIFY_STATIC_CONFIG`. One test. |

The two streaming refusals in 4.1.3's table carry no class at all here: `error_class` is
`nil`. The two dropped-field surfaces read differently, `Expected Relation to be set, but is
empty.` and `CATTYPE_NOT_SET not supported.`, and Latu recognises both phrasings.

**Rendering is unchanged on both.** The nine result goldens in `test/sql`, which pin the
schema and the rendered table of deterministic queries over numeric widening, decimals,
temporals, nulls, complex types, casts, intervals, string functions and aggregates, are
byte-identical on 4.0.4 and on 4.1.3. Whatever 4.2 changed in how a value prints, it is not in
that set.

## Spark 3.5

Not measured. The `apache/spark:3.5.x` images ship Scala 2.12 without the Connect server in
the assembly, so the compose file cannot start one; a 3.5 measurement needs the `-scala2.13`
distribution with `--packages org.apache.spark:spark-connect_2.13:3.5.x`, or a managed cluster
that runs 3.5. Every failure above applies, and 3.5 predates more: `Latu.zip_with_index/2`,
`Latu.transpose/2`, `Latu.lateral_join/3`, `Latu.table_changes/3`, `Latu.parse/2` and the
`time` type all arrive in 4.x. Treat 3.5 as unsupported until a run says otherwise.

## Newer than 4.2

Not measured either, and the direction is different: what breaks against a newer server is
invisible to a golden that pins what Latu sends, because the bytes are unchanged and the
answer moves. The `Spark versions` workflow runs the integration suite against any tag given
as its input, and the result goldens in `test/sql` report a changed rendering into the run
summary rather than failing. `4.3.0-rc1` existed upstream on 2026-09-07 with no image
published; the first newer run happens when there is a tag to give it.

## Support calendar

From spark.apache.org's release policy: 4.0 is supported until 2026-11, 4.1 until 2027-06,
4.2 until 2028-01, and 3.5 as LTS until 2027-11. Latu re-vendors its protos at the designated
4.x LTS, 4.5.0, around January 2027.

## Managed platforms

A managed endpoint is an `sc://` URL with headers, and `Latu.Session`'s URL parser turns any
`;key=value` it does not recognise into a gRPC metadata header, so none of these needs code.
None of them has been run by Latu's suite; the URL shapes are the platforms' own.

**Databricks**, a classic cluster: `sc://<workspace-host>:443/;use_ssl=true;token=<pat>;x-databricks-cluster-id=<cluster-id>`.
The token becomes `authorization: Bearer <pat>`, and Latu refuses to send one over cleartext,
which `use_ssl=true` satisfies. Serverless compute provisions the session client-side in
Databricks' own connector and exposes no URL of this shape.

**EMR Serverless**: `GetSessionEndpoint` hands out
`sc://<endpoint-host>:443/;use_ssl=true;x-aws-proxy-auth=<token>`, which is the same
mechanism. `emr-7.13.0` runs Spark 3.5.6, so the 3.5 section applies.

**Dataproc** provisions the session client-side, like Databricks serverless, and is not
reachable by URL.

`Latu.spark_version!/1` is the first thing to run against any of them.

## Running the suite against another version

`docker-compose.yml` reads `SPARK_VERSION` for both servers, and so does the one test that
asserts the version. Locally:

```bash
export SPARK_VERSION=4.1.3
docker compose up -d --force-recreate --wait
mix test --include integration 2>&1 | tee tmp/versions-$SPARK_VERSION.log
LATU_GOLDEN=report mix test --include golden
for f in test/sql/*.actual; do diff -u "${f%.actual}.answer" "$f"; done
unset SPARK_VERSION && docker compose up -d --force-recreate --wait
```

The `.actual` files are gitignored, and a golden that differs is a rendering the other version
changed, not a Latu defect. On GitHub,
`gh workflow run spark-versions.yml -f versions='["4.1.3"]'` does the same and writes the
failing tests and the golden diffs into the run summary.
