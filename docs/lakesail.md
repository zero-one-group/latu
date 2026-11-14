# LakeSail

[LakeSail](https://github.com/lakehq/sail) (Sail) is a Spark Connect server written in Rust.
Latu connects to it like any other: Sail's default port is 50051.

```elixir
session = Latu.connect!("sc://localhost:50051")
```

Latu targets Spark 4.2.0, and Sail is a second server it works with, not one it targets. This
page is what happens against Sail, measured. `.github/workflows/lakesail.yml` runs the
integration suite against Sail's latest release every night; it blocks nothing.

## Starting Sail

```bash
pip install "pysail==0.7.3" "pyspark-client==4.2.0"
SAIL_EXECUTION__CHECKPOINT__PATH=/tmp/sail_checkpoints sail spark server --port 50051
```

Both lines matter. Sail answers `Latu.spark_version/1` by importing PySpark, so without
`pyspark-client` in Sail's own environment that call fails with `No module named 'pyspark'`.
And Sail's checkpoints are written to a path it is configured with, so `Latu.checkpoint/2` is
refused until `SAIL_EXECUTION__CHECKPOINT__PATH` is set. The message names the setting.

## What works

Measured 2026-11-14, `pysail==0.7.3`: **238 of the 366 integration tests pass**, 127 use a
surface Sail lacks or answers differently, and 1 crashes the server. What passes is most of
Latu: reading and writing files, SQL, the relational verbs, joins, windows, aggregation, the
function library, `show`, `collect`, `to_explorer`, `to_nx`, checkpoints, and reattach.

## How a missing feature fails

**An RPC Sail lacks is refused** with `UNIMPLEMENTED`, and Latu adds that the server does not
implement it. `Latu.status/2` and `Latu.clone_session/2` fail this way.

**A plan node or function Sail lacks is refused** with `INTERNAL`, a message that is only the
feature's name, and `java.lang.UnsupportedOperationException` in `classes`. Latu adds that the
server does not support it:

```elixir
{:error, %Latu.Error{message: message}} =
  df |> Latu.observe("quality", rows: F.count(:id)) |> Latu.collect_with_metrics()

message
#=> "collect metrics (the server does not support this)"
```

**No error carries a Spark error class.** `error_class`, `sql_state`, `parameters` and
`error_id` are empty on every Sail error; `classes` is set. Code that matches on an error class,
which is the right thing to do against Spark, misses on Sail. `Latu.error_details/2` returns no
causes.

## What Sail lacks

| Surface | On Sail 0.7.3 |
|---|---|
| `Latu.status/2`, `Latu.clone_session/2` | No `GetStatus` or `CloneSession` RPC. |
| `Latu.observe/3` and every `*_with_metrics` verb | No `CollectMetrics`. |
| `Latu.copy_to_fs/3`, `Latu.add_jar/3` | No `AddArtifacts`. |
| `Latu.create_dataframe/3` over the cache threshold | No `AddArtifacts` either, but Sail reports the threshold as 2147483647 bytes, so the frame travels inline unless the threshold is lowered. |
| `Latu.join_as_of/3`, `Latu.nearest_by_join/4` | Refused. |
| `Latu.approx_quantile/5`, `Latu.freq_items/3` | Refused. |
| `Latu.zip_with_index/2` | Refused: no `distributed_sequence_id`. |
| `Latu.parse/2`, `Latu.transpose/2`, `Latu.table_function/3`, `Latu.metadata_column/2` | Refused. So is a star tagged with a frame. |
| `Latu.grouping_sets/3` | Refused: Sail takes no grouping sets beside grouping expressions, and Spark requires both. |
| `Latu.same_semantics/2`, `Latu.semantic_hash/1` | Refused. |
| `Latu.persist/2`, `Latu.cache/1` | Accepted and ignored. `Latu.storage_level/1` reports one fixed level, memory and off-heap, for every frame, cached or not. |
| `Latu.Catalog.is_cached/2`, `Latu.Catalog.drop_table/3` | Refused. |
| `Latu.Functions.zip_with/3`, a Vector column | Refused: no `zip_with`, no `array_to_vector`. |
| `Latu.write_stream/2` | A file sink is refused (`streaming query must write data to a sink`), and there is no memory sink. |
| JDBC | Sail's JDBC source must first be registered as a Python data source, a pickled class Latu cannot send. |
| `CREATE FUNCTION` in SQL | Not parsed. |
| `:progress` on any action | Never called: Sail sends no progress messages. |
| `Latu.checkpoint/2` | Works once configured, but `storage_level:` and `eager: false` are refused, and `Latu.release/1` leaves the checkpoint readable. |

## Where Sail answers differently

These run and return something other than what Spark returns, which makes them the ones to know.

| Surface | Spark | Sail 0.7.3 |
|---|---|---|
| `Latu.describe/2`, `Latu.summary/2` | Rows in a fixed order: count, mean, stddev, min, the percentiles, max. | Rows in no fixed order. Select by the `summary` column, not by position. |
| `Latu.cov/3`, `Latu.corr/3` | A row with a null counts it as zero. | The row is dropped, as the SQL aggregate does. The answers differ whenever a null is present. |
| `Latu.crosstab/3` | A null key becomes the string `"null"`, as a row and as a column. | It stays `nil`. |
| `Latu.to/2` | A missing nullable target column becomes nulls. | Refused. |
| `Latu.sql/3` | An unknown table is refused by the call. | Refused only when the frame runs. |
| Analysis (`Latu.columns/1`, `Latu.schema/1`) | Never runs the plan. | Evaluates constant expressions, so `1 / 0` fails at analysis. |
| A window frame `row_number` cannot use | Refused. | Accepted. |
| `Latu.Catalog.list_tables/2` | Rows carry `:name`. | Rows carry `:tableName`. Latu passes the server's columns through. |
| `Latu.parse_ddl/2` | `char(3)` and `varchar(5)` survive. | Both become `string`. |
| `Latu.release_session/2` on a session the server has not seen | No-op. | Refused. |
| `Latu.fetch_conf/2` on a key nobody defined | `SQL_CONF_NOT_FOUND`. | `nil`. |
| `Latu.set_conf/3` on a static conf | Refused. | Accepted. |
| `Latu.confs/2` with a prefix | Keys come back whole. | The prefix is doubled: `spark.sql.spark.sql.session.timeZone`. |
| `spark.sql.session.timeZone` | Set when the server starts, `UTC` in the suite's containers. | The host's zone, `Etc/UTC` in a container. `Latu.set_conf/3` changes it per session. |

**Rendering differs too.** Six of the nine result goldens in `test/sql` change against Sail,
none of them in a value: Sail marks some columns nullable that Spark does not and the reverse,
types `1 + 1.5` as `decimal(12,1)` where Spark has `decimal(3,1)`, and prints a single-field
interval in its long form, `INTERVAL '2-0' YEAR TO MONTH` for Spark's `INTERVAL '2' YEAR`.

## A crash

Sorting the result of `Latu.to/2` kills the Sail process. PySpark's
`df.to(schema).sort("id")` does the same, so it is Sail's. The one test that does this is tagged
`sail_crash:` and never runs against Sail.

## Running the suite against Sail

The `sail` service in `docker-compose.yml` builds Sail's own quick-start image and publishes it
on both of the suite's ports, so the Spark servers come down first. `SAIL_VERSION` picks the
release; the default is the one this page measured.

```bash
docker compose down
docker compose up -d --build --wait sail
LATU_SERVER=sail mix test                      # the suite, minus the tagged gaps
LATU_SERVER=sail mix test --only sail --trace  # the gaps alone: each one should fail
docker compose --profile sail down && docker compose up -d --wait
```

`LATU_SERVER=sail` turns the integration suite on without `--include integration`, which would
override the exclusions. A test whose surface Sail lacks carries `@tag sail: "<reason>"`; when
one starts passing on a new release, the nightly summary lists it and the tag comes off.
