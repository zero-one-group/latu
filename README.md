<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/zero-one-group/latu/main/assets/latu-lockup-dark.svg">
    <img alt="Latu" src="https://raw.githubusercontent.com/zero-one-group/latu/main/assets/latu-lockup.svg" width="360">
  </picture>
</p>

A native Elixir DataFrame API for Apache Spark, over [Spark
Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html).

Latu builds a query plan on your machine and Spark runs it. There is no JVM in your project and
no cluster on your laptop — a session is a plain struct wrapping a gRPC channel.

`latu` is Javanese for *spark*; [`geni`](https://github.com/zero-one-group/geni), its Clojure
predecessor, is Javanese for *fire*.

## Install

```elixir
def deps do
  [{:latu, "~> 0.1"}]
end
```

Requires Elixir ~> 1.20 and a Spark **4.2.0** Connect server.

## A server to talk to

Point at your cluster's `sc://` URL, or run one locally:

```bash
docker run -p 15002:15002 apache/spark:4.2.0 \
  /opt/spark/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.13:4.2.0
```

## The three lines at the top of your file

`Latu` is called qualified, the way `Enum` is. `Latu.Column` is small and gets composed by hand,
so it is imported. The other two are aliased, as in PySpark, because their names collide with
the verbs on purpose — `Latu.count/1` counts a DataFrame and `F.count/1` is the aggregate:

```elixir
import Latu.Column              # operators, predicates, casts, sort keys, over/2
alias Latu.Functions, as: F     # Spark's ~500 functions, under Spark's own names
alias Latu.Window, as: W        # window specifications
```

## A first pipeline

```elixir
{:ok, session} = Latu.connect("sc://localhost:15002")

session
|> Latu.range(10)
|> Latu.filter(all([greater(:id, 2), not_equal(:id, 5)]))
|> Latu.with_columns(doubled: multiply(:id, 2))
|> Latu.distinct([:doubled])
|> Latu.rename(id: :n)
|> Latu.select([:n, :doubled])
|> Latu.limit(3)
|> Latu.show()
```

`show` prints the table Spark renders, not one Latu formats, so the output matches PySpark
byte for byte. This page shows the shape of the API; the
[quick start](https://hexdocs.pm/latu/quick-start.html) shows results, and every line of it is
executed by the test suite — which is why the numbers there can be trusted and the ones in a
README cannot.

## What is in it

**Relational verbs.** `select`, `filter`, `with_columns`, `drop`, `limit`, `offset`, `sort`,
`distinct`, `rename`, set operations, every join type plus as-of, lateral and nearest-by joins,
`group_by`/`agg` with rollup, cube, pivot and grouping sets, `unpivot`, `transpose`, sampling,
repartitioning, `zip_with_index`.

**Expressions.** Around 500 functions in `Latu.Functions` under Spark's own names, generated
from a registry derived from PySpark's Connect client with Spark's own documentation harvested
into them — so `h F.regexp_replace` tells you what Spark says. Window specifications, and
higher-order functions that take an ordinary Elixir lambda. Subqueries reach across frames:
`Latu.scalar/1`, `Latu.exists/1`, `Latu.Column.isin/2`.

**Results.** `show`, `collect` into maps, `count`, `take`/`first`/`head`/`tail`, `to_explorer`
into an `Explorer.DataFrame`, a lazy `stream` of one frame per Arrow batch, `glimpse` for a wide
frame, and raw Arrow — behind a schema guard that turns the types the decoder cannot represent
into errors naming the column.

**Asking a frame about itself**, without running it: `schema`, `columns`, `dtypes`,
`print_schema`, `explain`, `input_files`, `same_semantics`, `is_empty`, plus `cache`, `persist`
and `storage_level`.

**Reading and writing.** `read/2` and `write/2` for any format the cluster has, JDBC included;
`table`, `save_as_table`, `insert_into`, the v2 `write_v2`, and `merge_into` for a row-level
merge. `create_dataframe/3` ships rows, columns or an Explorer frame the other way as Arrow,
escalating past 64 MiB to server-cached artifacts with no change to the call. `sql/3` binds
parameters as literals rather than splicing text, and can name a DataFrame into a query without
registering anything on the server. Temp views and the catalog are there too.

**Missing data and statistics.** `drop_na`, `fill_na`, `replace`, `summary`, `describe`,
`crosstab`, `freq_items`, `sample_by`, `cov`, `corr`, `approx_quantile`.

**Running things.** `observe/3`, with the metrics coming back from the action; checkpointing;
interrupting a query by tag from another process; session config both ways; per-action progress
callbacks; errors carrying Spark's own error class and SQLSTATE; `:telemetry` events; and
Livebook rendering behind an optional `:kino` dependency.

**Not yet, or never.** MLlib and structured streaming are deferred to separate packages, for
different reasons: a streaming query is a lifecycle Latu does not own, and ML's 103-operator
parameter surface has nothing machine-readable to check it against. There are no UDFs written
in Elixir, no RDDs and no `SparkContext` — Spark Connect offers no client in any language a
path to them.

## What this is

A slim Spark Connect client with a DataFrame API designed for Elixir rather than transliterated
from PySpark. Two things are load-bearing:

1. **Ergonomics over fidelity.** Where Spark's DataFrame API and idiomatic Elixir disagree,
   Elixir wins. Aggressive coercion, no mandatory `col/1`, `show` prints to stdout, keyword
   lists for aliased projections.
2. **No runtime ownership.** Latu defines no GenServer, supervisor, registry or pool, and
   declares no application callback module — adding it to your deps starts nothing.
   `%Latu.Session{}` is a struct; you decide where it lives. The one process Latu causes to
   exist is the gRPC channel `connect/2` opens and `disconnect/2` closes, and the one
   server-side resource it allocates is a checkpoint, which is why `release/1` exists. `Latu`'s
   moduledoc states the promise exactly.

A string is a column name in `select` and SQL in `filter` — PySpark's rule — so
`Latu.filter(df, "id > 3")` works too. Inside an expression a string is a literal:
`equal(:suburb, "Reservoir")` compares a column to text.

Results come out as Elixir data:

```elixir
{:ok, rows} = df |> Latu.limit(2) |> Latu.collect()
#=> {:ok, [%{id: 0}, %{id: 1}]}

{:ok, n} = Latu.count(df)
{:ok, frame} = Latu.to_explorer(df)            # refuses past 100k rows unless told otherwise
df |> Latu.stream() |> Enum.each(&handle/1)    # lazy: one Explorer frame per Arrow batch
```

A schema comes back as data, with Spark's own name for each type — there is no client-side type
model in either direction:

```elixir
{:ok, fields} = Latu.schema(df)
#=> {:ok, [%{name: "id", type: "bigint", nullable: false}]}

Latu.dtypes!(df)       #=> [{"id", "bigint"}]
Latu.print_schema!(df) # root
                       #  |-- id: long (nullable = false)
```

Reading is one call, not a builder chain. The schema is a string the server parses; snake_case
option keys become Spark's camelCase (`infer_schema:` → `"inferSchema"`):

```elixir
Latu.read(session, format: "csv", schema: "id INT, name STRING",
  path: "/data/people.csv", header: true)

df |> Latu.write(format: "parquet", path: "/data/out", mode: :overwrite)
```

Expressions are plain functions, not macros, so one is a value you can name, pass around and
fold:

```elixir
big = greater(:price, 1_000_000)
Latu.filter(df, all([big | extra_predicates]))
```

There is no macro DSL, deliberately — a macro expression is not a value, so extracting a
fragment or folding a list of predicates would need a second construct bolted on.

Window specifications compose the same way:

```elixir
by_suburb = W.partition_by([:suburb]) |> W.order_by([desc(:price)])

df
|> Latu.with_columns(rank: over(F.rank(), by_suburb))
|> Latu.group_by(:suburb)
|> Latu.agg(avg: F.avg(:price), sold: F.count_distinct(:id))
|> Latu.show()
```

`Latu.connect/0` reads `SPARK_REMOTE`, and `Latu.connect/1` accepts Spark's URL parameters:
`sc://host:15002/;use_ssl=true;token=...;user_id=...`. Unrecognised parameters become gRPC
metadata headers, as PySpark does.

## Custom code on the cluster

Latu **calls** a user-defined function by name with `Latu.Column.fun/3`, and a SQL UDF, a Hive
UDF and a registered Java class all resolve the same way — `CREATE FUNCTION` through
`Latu.sql/3` is how you register one. Latu does **not** ship a jar from your machine; why not,
and what it would take, is in `docs/decisions.md`.

## Where to go next

  * [Quick start](https://hexdocs.pm/latu/quick-start.html) — connect, build, run; every line
    of it executed
  * [Cookbook](https://hexdocs.pm/latu/cookbook.html) — recipes for the things you actually do
  * [Coming from PySpark](https://hexdocs.pm/latu/from-pyspark.html) — the five differences,
    and a translation table for the calls you make every day
  * [Coming from Explorer](https://hexdocs.pm/latu/from-explorer.html) — where the local Elixir
    dataframe ends and the cluster begins, and how to move frames across the seam
  * [`usage-rules.md`](https://hexdocs.pm/latu/usage-rules.html) — the short set of rules that
    are not guessable from the function names, in the
    [`usage_rules`](https://github.com/ash-project/usage_rules) convention, so an agent can sync
    it into its context
  * [`docs/deviations.md`](https://hexdocs.pm/latu/deviations.html) — every place the API
    departs from PySpark, and why
  * [`CONTRIBUTING.md`](https://hexdocs.pm/latu/contributing.html) — the servers, the proto
    oracle, the golden fixtures

## SparkEx, and why Latu exists

[SparkEx](https://github.com/lukaszsamson/spark_ex) is an independent Elixir Spark Connect
client. **It got here first, it is on Hex, and it does more than Latu does** — structured
streaming, and UDF/UDTF registration, neither of which Latu ships. If you need either today,
use SparkEx.

The two made different bets. SparkEx keeps close to PySpark's shape — mandatory `col/1` and
`lit/1`, module namespaces standing in for method chains, positional arguments, string keys —
and its session is a `GenServer`. Latu's API is designed for Elixir (atoms as columns, keyword
lists for aliases and options, one namespace of verbs called the way `Enum` is), its session is
a plain struct, and it defines no process at all:

```elixir
# SparkEx
DataFrame.filter(df, Column.gt(col("salary"), lit(120)))
DataFrame.join(departments, ["dept"], :inner)
#=> {:ok, [%{"name" => "Bob", "salary" => 200}]}

# Latu
Latu.filter(df, greater(:salary, 120))
Latu.join(df, departments, on: :dept, how: :inner)
#=> {:ok, [%{name: "Bob", salary: 200}]}
```

The runtime shape is the choice everything else follows from: a session process gives you
supervision and somewhere to put shared state; doing without one is why Latu's metrics come
back from actions and its progress handler runs in your own process. Latu also promises less on
purpose — streaming and MLlib are separate packages, for the reasons in `docs/decisions.md` —
and verifies more: every plan it builds is diffed against the protobuf PySpark builds for the
same pipeline, and every documented example is executed. If SparkEx's spelling reads better to
you, that is a good reason to use SparkEx; the rules behind Latu's are in
[`docs/deviations.md`](https://hexdocs.pm/latu/deviations.html).

## Acknowledgements

Design and test suite draw heavily on [Geni](https://github.com/zero-one-group/geni)
(Apache-2.0, Copyright 2020 Zero One Group).

Studying SparkEx shaped early design decisions, and `docs/decisions.md` records where the two
projects part ways.

## How this was built

**Claude (Anthropic) wrote the overwhelming majority of the code, the tests and the
documentation.** The maintainers set the scope, made the design calls, ran every gate and
reviewed the result. That division is worth stating plainly rather than leaving to be guessed
at.

It is also worth saying what it does and does not imply. No line of this landed without passing
`mix check.all` on a maintainer's machine, and review caught real defects. But review is a small
number of people, and the thing actually holding the library up is the apparatus: the PySpark
oracle, the executed examples, and `docs/decisions.md`, which records the reasoning behind every
non-obvious choice and is unusually complete precisely because of how this was written.

Judge it the way you would judge any library — by whether the tests test the right things, and
whether the reasoning holds up when you read it.

## License

Apache License 2.0. See the `LICENSE` file. Arrow decoding uses
[Explorer](https://github.com/elixir-explorer/explorer), which is MIT.
